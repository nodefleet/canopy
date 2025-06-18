package store

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	latestStatePrefix     = "s/"         // prefix designated for the LatestStateStore where the most recent blobs of state data are held
	stateCommitmentPrefix = "c/"         // prefix designated for the StateCommitmentStore (immutable, tree DB) built of hashes of state store data
	indexerPrefix         = "i/"         // prefix designated for indexer (transactions, blocks, and quorum certificates)
	stateCommitIDPrefix   = "x/"         // prefix designated for the commit ID (height and state merkle root)
	lastCommitIDPrefix    = "a/"         // prefix designated for the latest commit ID for easy access (latest height and latest state merkle root)
	partitionExistsKey    = "e/"         // to check if partition exists
	partitionFrequency    = uint64(1000) // blocks
	maxKeyBytes           = 256          // maximum size of a key
)

var _ lib.StoreI = &Store{} // enforce the Store interface

/*
The Store struct is a high-level abstraction layer built on top of a single BadgerDB instance,
providing four main components for managing blockchain-related data.

1. StateStore: This component is responsible for storing the actual blobs of data that represent
   the state. It acts as the primary data storage layer. This store is divided into 'historical'
   partitions and 'latest' data. This separation allows efficient iteration, fast snapshot access,
   and safe pruning of older state without impacting current performance.

2. StateCommitStore: This component maintains a Sparse Merkle Tree structure, mapping keys
   (hashes) to their corresponding data hashes. It is optimized for blockchain operations,
   allowing efficient proof of existence within the tree and enabling the creation of a single
   'root' hash. This root hash represents the entire state, facilitating easy verification by
   other nodes to ensure consistency between their StateHash and the peer StateHash.

3. Indexer: This component indexes critical blockchain elements, including Quorum Certificates
   by height, Blocks by both height and hash, and Transactions by height.index, hash, sender,
   and recipient. The indexing allows for efficient querying and retrieval of these elements,
   which are essential for blockchain operation.

4. CommitIDStore: This is a smaller abstraction that isolates the 'CommitID' structures, which
   consist of two fields: Version, representing the height or version number, and Root, the root
   hash of the StateCommitStore corresponding to that version. This separation aids in managing
   the state versioning process.

The Store leverages BadgerDB in Managed Mode to maintain historical versions of the state,
allowing for time-travel operations and historical state queries. It uses BadgerDB Transactions
to ensure that all writes to the StateStore, StateCommitStore, Indexer, and CommitIDStore are
performed atomically in a single commit operation per height. Additionally, the Store uses
lexicographically ordered prefix keys to facilitate easy and efficient iteration over stored data.
*/

type Store struct {
	version             uint64          // version of the store
	root                []byte          // root associated with the CommitID at this version
	db                  *pebble.DB      // underlying database
	reader              *VersionedStore // reader to view committed data
	writer              *VersionedStore // the batch writer that allows committing it all at once
	lss                 *Txn            // reference to the 'latest' state store
	sc                  *SMT            // reference to the state commitment store
	*Indexer                            // reference to the indexer store
	isGarbageCollecting atomic.Bool     // protect garbage collector (only 1 at a time)
	metrics             *lib.Metrics    // telemetry
	log                 lib.LoggerI     // logger
}

// New() creates a new instance of a StoreI either in memory or an actual disk DB
func New(config lib.Config, metrics *lib.Metrics, l lib.LoggerI) (lib.StoreI, lib.ErrorI) {
	if config.StoreConfig.InMemory {
		return NewStoreInMemory(l)
	}
	return NewStore(filepath.Join(config.DataDirPath, config.DBName), metrics, l)
}

// NewStore() creates a new instance of a disk DB
func NewStore(path string, metrics *lib.Metrics, log lib.LoggerI) (lib.StoreI, lib.ErrorI) {
	db, err := pebble.Open(path, &pebble.Options{
		FormatMajorVersion: pebble.FormatColumnarBlocks,
		FS:                 vfs.Default,
		MaxOpenFiles:       500,
	})
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	return NewStoreWithDB(db, metrics, log, true)
}

// NewStoreInMemory() creates a new instance of a mem DB
func NewStoreInMemory(log lib.LoggerI) (lib.StoreI, lib.ErrorI) {
	db, err := pebble.Open("", &pebble.Options{
		FS:                 vfs.NewMem(),
		DisableWAL:         true,
		FormatMajorVersion: pebble.FormatColumnarBlocks,
	})
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	return NewStoreWithDB(db, nil, log, true)
}

// NewStoreWithDB() returns a Store object given a DB and a logger
func NewStoreWithDB(db *pebble.DB, metrics *lib.Metrics, log lib.LoggerI, write bool) (*Store, lib.ErrorI) {
	// get the latest CommitID (height and hash)
	id := getLatestCommitID(db, log)
	// create a new versioned store at the latest height
	vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), id.Height, false)
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	// return the store object
	return &Store{
		version: id.Height,
		log:     log,
		db:      db,
		reader:  vs,
		writer:  vs,
		lss:     NewTxn(vs, vs, []byte(latestStatePrefix), true, log),
		sc:      NewDefaultSMT(NewTxn(vs, vs, []byte(stateCommitmentPrefix), false, log)),
		Indexer: &Indexer{NewTxn(vs, vs, []byte(indexerPrefix), true, log)},
		metrics: metrics,
		root:    id.Root,
	}, nil
}

// NewReadOnly() returns a store without a writer - meant for historical read only queries
func (s *Store) NewReadOnly(queryVersion uint64) (lib.StoreI, lib.ErrorI) {
	// create a new versioned store at the specified version
	vs, err := NewVersionedStore(s.db.NewSnapshot(), s.db.NewBatch(), queryVersion, false)
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	// commit the versioned so no writes can be made to it
	err = vs.Commit()
	if err != nil {
		return nil, ErrCommitDB(err)
	}
	return &Store{
		version: queryVersion,
		log:     s.log,
		db:      s.db,
		reader:  vs,
		writer:  vs,
		lss:     NewTxn(vs, vs, []byte(latestStatePrefix), true, s.log),
		sc:      NewDefaultSMT(NewTxn(vs, vs, []byte(stateCommitmentPrefix), false, s.log)),
		Indexer: &Indexer{NewTxn(vs, vs, []byte(indexerPrefix), true, s.log)},
		metrics: s.metrics,
		root:    bytes.Clone(s.root),
	}, nil
}

// Copy() make a copy of the store with a new read/write transaction
// this can be useful for having two simultaneous copies of the store
// ex: Mempool state and FSM state
func (s *Store) Copy() (lib.StoreI, lib.ErrorI) {
	vs, err := NewVersionedStore(s.db.NewSnapshot(), s.db.NewBatch(), s.version, false)
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	return &Store{
		version: s.version,
		log:     s.log,
		db:      s.db,
		reader:  vs,
		writer:  vs,
		lss:     NewTxn(vs, vs, []byte(latestStatePrefix), true, s.log),
		sc:      NewDefaultSMT(NewTxn(vs, vs, []byte(stateCommitmentPrefix), false, s.log)),
		Indexer: &Indexer{NewTxn(vs, vs, []byte(indexerPrefix), true, s.log)},
		metrics: s.metrics,
		root:    bytes.Clone(s.root),
	}, nil
}

// Commit() performs a single atomic write of the current state to all stores.
func (s *Store) Commit() (root []byte, err lib.ErrorI) {
	// update the version (height) number
	s.version++
	// execute operations over the tree and hash upwards to get the new root
	if e := s.sc.Commit(); e != nil {
		return nil, e
	}
	// get the root from the sparse merkle tree at the current state
	s.root = s.sc.Root()
	// set the new CommitID (to the Transaction not the actual DB)
	if err = s.setCommitID(s.version, s.root); err != nil {
		return nil, err
	}
	// extract the internal metrics from the badger Txn
	// size, entries := getSizeAndCountFromBatch(s.writer)
	// update the metrics once complete
	// defer s.metrics.UpdateStoreMetrics(size, entries, time.Time{}, time.Now())
	// finally commit the entire Transaction to the actual DB under the proper version (height) number
	if e := s.Write(); e != nil {
		return nil, e
	}
	if e := s.writer.Commit(); e != nil {
		return nil, ErrCommitDB(e)
	}
	// reset the writer for the next height
	s.resetWriter()
	// return the root
	return bytes.Clone(s.root), nil
}

// Write() writes the current state to the batch writer without committing it.
func (s *Store) Write() lib.ErrorI {
	if er := s.sc.store.(TxnWriterI).Commit(); er != nil {
		return ErrCommitDB(er)
	}
	if e := s.lss.Commit(); e != nil {
		return ErrCommitDB(e)
	}
	if e := s.Indexer.db.Commit(); e != nil {
		return ErrCommitDB(e)
	}
	return nil
}

// PARTITIONING CODE BELOW

// ShouldPartition() determines if it is time to partition
func (s *Store) ShouldPartition() (timeToPartition bool) {

	return
}

// Partition()
//  1. SNAPSHOT: for each key in the state store, copy them in the new historical partition to ensure
//     each partition has a complete version of the state at the border -- allowing older partitions to
//     safely be pruned
//  2. PRUNE: Drop all versions in the LSS older than the latest keys @ partition height
func (s *Store) Partition() {

}

// Get() returns the value bytes blob from the State Store
func (s *Store) Get(key []byte) ([]byte, lib.ErrorI) {
	return s.lss.Get(key)
}

// Set() sets the value bytes blob in the LatestStateStore and the HistoricalStateStore
// as well as the value hash in the StateCommitStore referenced by the 'key' and hash('key') respectively
func (s *Store) Set(k, v []byte) lib.ErrorI {
	// set in the state store @ latest
	if err := s.lss.Set(k, v); err != nil {
		return err
	}
	// set in the state commit store
	return s.sc.Set(k, v)
}

// Delete() removes the key-value pair from both the LatestStateStore, HistoricalStateStore, and CommitStore
func (s *Store) Delete(k []byte) lib.ErrorI {
	// delete from the state store @ latest
	if err := s.lss.Delete(k); err != nil {
		return err
	}
	// delete from the state commit store
	return s.sc.Delete(k)
}

// GetProof() uses the StateCommitStore to prove membership and non-membership
func (s *Store) GetProof(key []byte) ([]*lib.Node, lib.ErrorI) {
	return s.sc.GetMerkleProof(key)
}

// VerifyProof() checks the validity of a member or non-member proof from the StateCommitStore
// by verifying the proof against the provided key, value, and proof data.
func (s *Store) VerifyProof(key, value []byte, validateMembership bool, root []byte, proof []*lib.Node) (bool, lib.ErrorI) {
	return s.sc.VerifyProof(key, value, validateMembership, root, proof)
}

// Iterator() returns an object for scanning the StateStore starting from the provided prefix.
// The iterator allows forward traversal of key-value pairs that match the prefix.
func (s *Store) Iterator(p []byte) (lib.IteratorI, lib.ErrorI) {
	return s.lss.Iterator(p)
}

// RevIterator() returns an object for scanning the StateStore starting from the provided prefix.
// The iterator allows backward traversal of key-value pairs that match the prefix.
func (s *Store) RevIterator(p []byte) (lib.IteratorI, lib.ErrorI) {
	return s.lss.RevIterator(p)
}

// Version() returns the current version number of the Store, representing the height or version
// number of the state. This is used to track the versioning of the state data.
func (s *Store) Version() uint64 { return s.version }

// NewTxn() creates and returns a new transaction for the Store, allowing atomic operations
// on the StateStore, StateCommitStore, Indexer, and CommitIDStore.
func (s *Store) NewTxn() lib.StoreI {
	return &Store{
		version: s.version,
		log:     s.log,
		db:      s.db,
		reader:  s.reader,
		writer:  s.writer,
		lss:     NewTxn(s.lss, s.lss, nil, true, s.log),
		// the current implementation uses Txn as the reader and writer for the SMT. so this won't
		// fail, should be revised if the SMT store is ever changed
		sc:      NewDefaultSMT(NewTxn(s.sc.store.(TxnReaderI), s.sc.store.(TxnWriterI), nil, false, s.log)),
		Indexer: &Indexer{NewTxn(s.Indexer.db, s.Indexer.db, nil, true, s.log)},
		metrics: s.metrics,
		root:    bytes.Clone(s.root),
	}
}

// DB() returns the underlying PebbleDB instance associated with the Store, providing access
// to the database for direct operations and management.
func (s *Store) DB() *pebble.DB { return s.db }

// Root() retrieves the root hash of the StateCommitStore, representing the current root of the
// Sparse Merkle Tree. This hash is used for verifying the integrity and consistency of the state.
func (s *Store) Root() (root []byte, err lib.ErrorI) {
	if err = s.sc.Commit(); err != nil {
		return nil, err
	}
	return s.sc.Root(), nil
}

// Reset() discard and re-sets the stores writer
func (s *Store) Reset() {
	s.resetWriter()
}

// Discard() closes the reader and writer
func (s *Store) Discard() {
	s.reader.Discard()
	s.writer.Cancel()
}

// Close() discards the writer and closes the database connection
func (s *Store) Close() lib.ErrorI {
	s.Discard()
	if err := s.db.Close(); err != nil {
		return ErrCloseDB(s.db.Close())
	}
	return nil
}

// resetWriter() closes the writer, and creates a new writer, and sets the writer to the 3 main abstractions
func (s *Store) resetWriter() {
	// TODO: handle error
	newVS, _ := NewVersionedStore(s.db.NewSnapshot(), s.db.NewBatch(), s.version, false)
	// create all new transaction-dependent objects
	newLSS := NewTxn(newVS, newVS, []byte(latestStatePrefix), true, s.log)
	newSC := NewDefaultSMT(NewTxn(newVS, newVS, []byte(stateCommitmentPrefix), false, s.log))
	newIndexer := NewTxn(newVS, newVS, []byte(indexerPrefix), true, s.log)
	// only after creating all new objects, discard old transactions
	s.reader.Discard()
	s.writer.Cancel()
	// update all references
	s.reader = newVS
	s.writer = newVS
	s.lss = newLSS
	s.sc = newSC
	s.Indexer.setDB(newIndexer)
}

// commitIDKey() returns the key for the commitID at a specific version
func (s *Store) commitIDKey(version uint64) []byte {
	return fmt.Appendf(nil, "%s/%d", stateCommitIDPrefix, version)
}

// getCommitID() retrieves the CommitID value for the specified version from the database
func (s *Store) getCommitID(version uint64) (id lib.CommitID, err lib.ErrorI) {
	var bz []byte
	bz, err = NewTxn(s.reader, s.writer, nil, false, s.log).Get(s.commitIDKey(version))
	if err != nil {
		return
	}
	if err = lib.Unmarshal(bz, &id); err != nil {
		return
	}
	return
}

// setCommitID() stores the CommitID for the specified version and root in the database
func (s *Store) setCommitID(version uint64, root []byte) lib.ErrorI {
	w := NewTxn(s.reader, s.writer, nil, false, s.log)
	value, err := lib.Marshal(&lib.CommitID{
		Height: version,
		Root:   root,
	})
	if err != nil {
		return err
	}
	if err = w.Set([]byte(lastCommitIDPrefix), value); err != nil {
		return err
	}
	k := s.commitIDKey(version)

	if err = w.Set(k, value); err != nil {
		return err
	}

	return w.Commit()
}

// getLatestCommitID() retrieves the latest CommitID from the database
func getLatestCommitID(db *pebble.DB, log lib.LoggerI) (id *lib.CommitID) {
	vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), math.MaxUint64, false)
	if err != nil {
		log.Fatalf("getLatestCommitID() failed with err: %s", err.Error())
	}
	tx := NewTxn(vs, vs, nil, false, log)
	defer tx.Close()
	id = new(lib.CommitID)
	bz, err := tx.Get([]byte(lastCommitIDPrefix))
	if err != nil {
		log.Fatalf("getLatestCommitID() failed with err: %s", err.Error())
	}
	if err = lib.Unmarshal(bz, id); err != nil {
		log.Fatalf("unmarshalCommitID() failed with err: %s", err.Error())
	}
	return
}
