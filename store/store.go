package store

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/alecthomas/units"
	"github.com/canopy-network/canopy/lib"
	"github.com/dgraph-io/badger/v4"
)

const (
	latestStatePrefix     = "s/"           // prefix designated for the LatestStateStore where the most recent blobs of state data are held
	historicStatePrefix   = "h/"           // prefix designated for the HistoricalStateStore where the historical blobs of state data are held
	stateCommitmentPrefix = "c/"           // prefix designated for the StateCommitmentStore (immutable, tree DB) built of hashes of state store data
	indexerPrefix         = "i/"           // prefix designated for indexer (transactions, blocks, and quorum certificates)
	stateCommitIDPrefix   = "x/"           // prefix designated for the commit ID (height and state merkle root)
	lastCommitIDPrefix    = "a/"           // prefix designated for the latest commit ID for easy access (latest height and latest state merkle root)
	maxKeyBytes           = 256            // maximum size of a key
	lssVersion            = math.MaxUint64 // the arbitrary version the latest state is written to for optimized queries
)

// maximum size of the database (batch) transaction
var maxTransactionSize = int64(
	math.Ceil(float64(5*units.GB) / badgerDBMaxBatchScalingFactor),
)

var _ lib.StoreI = &Store{} // enforce the Store interface

/*
The Store struct is a high-level abstraction layer built on top of a single BadgerDB instance,
providing four main components for managing blockchain-related data.

1. StateStore: This component is responsible for storing the actual blobs of data that represent
   the state. It acts as the primary data storage layer. This store is divided into 'historical'
   partitions and 'latest' data. This separation allows efficient block processing time while
   minimizing storage de-duplication.

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

type StoreStat struct {
	store    string
	duration time.Duration
}

func (s *Store) logData() {
	logger := lib.NewDefaultLogger()
	quit := make(chan struct{})
	stats := make([]StoreStat, 0)

	go func() {
		for stat := range s.statsChan {
			stats = append(stats, stat)
		}
		close(quit)
	}()

	go func() {
		interval := 5 * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if len(stats) == 0 {
					continue
				}

				type statType struct {
					duration time.Duration
					total    int
				}

				st := map[string]statType{}
				st[stateCommitIDPrefix] = statType{}
				st[latestStatePrefix] = statType{}
				st[historicStatePrefix] = statType{}
				for _, stat := range stats {
					st[stat.store] = statType{
						duration: st[stat.store].duration + stat.duration,
						total:    st[stat.store].total + 1,
					}
				}

				lssAvg := st[latestStatePrefix].duration / time.Duration(st[latestStatePrefix].total)
				hssAvg := st[historicStatePrefix].duration / time.Duration(st[historicStatePrefix].total)
				scAvg := st[stateCommitmentPrefix].duration / time.Duration(st[stateCommitmentPrefix].total)

				logger.Infof("[Store] Avg Set Time: LSS: %s | HSS: %s | SC: %s",
					lssAvg, hssAvg, scAvg)
				stats = make([]StoreStat, 0)
			case <-quit:
				return
			}
		}
	}()
}

type Store struct {
	statsChan     chan StoreStat
	version       uint64             // version of the store
	root          []byte             // root associated with the CommitID at this version
	db            *badger.DB         // underlying database
	writer        *badger.WriteBatch // the shared batch writer that allows committing it all at once
	lss           *Txn               // reference to the 'latest' state store
	hss           *Txn               // references the 'historical' state store (non-latest)
	sc            *SMT               // reference to the state commitment store
	*Indexer                         // reference to the indexer store
	useHistorical bool               // signals to use the historical state store for query
	metrics       *lib.Metrics       // telemetry
	log           lib.LoggerI        // logger
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
	// use badger DB in managed mode to allow efficient versioning
	// memTableSize is set to 1.28GB (max) to allow 128MB (10%) of writes in a
	// single batch. It is seemingly unknown why the 10% limit is set
	now := time.Now()
	log.Info("opening new store DB")
	// https://discuss.dgraph.io/t/discussion-badgerdb-should-offer-arbitrarily-sized-atomic-transactions/8736
	db, err := badger.OpenManaged(badger.DefaultOptions(path).WithNumVersionsToKeep(math.MaxInt64).
		WithLoggingLevel(badger.ERROR).WithMemTableSize(maxTransactionSize),
	)
	log.Infof("opened new store DB, took: %s", time.Since(now).String())
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	return NewStoreWithDB(db, metrics, log)
}

// NewStoreInMemory() creates a new instance of a mem DB
func NewStoreInMemory(log lib.LoggerI) (lib.StoreI, lib.ErrorI) {
	db, err := badger.OpenManaged(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	if err != nil {
		return nil, ErrOpenDB(err)
	}
	return NewStoreWithDB(db, nil, log)
}

// NewStoreWithDB() returns a Store object given a DB and a logger
func NewStoreWithDB(db *badger.DB, metrics *lib.Metrics, log lib.LoggerI) (*Store, lib.ErrorI) {
	// get the latest CommitID (height and hash)
	id := getLatestCommitID(db, log)
	// set the version
	nextVersion, version := id.Height+1, id.Height
	// make a reader from the current height
	reader := db.NewTransactionAt(version, false)
	// create a new batch writer for the next version
	// note: version for WriteBatch may be overridden by the setEntryAt(version) code
	writer := db.NewWriteBatchAt(nextVersion)
	// return the store object
	st := &Store{
		version:   version,
		log:       log,
		db:        db,
		writer:    writer,
		lss:       NewBadgerTxn(db.NewTransactionAt(lssVersion, false), writer, []byte(latestStatePrefix), true, lssVersion, true, log),
		hss:       NewBadgerTxn(reader, writer, []byte(historicStatePrefix), false, nextVersion, true, log),
		sc:        NewDefaultSMT(NewBadgerTxn(reader, writer, []byte(stateCommitmentPrefix), false, nextVersion, true, log)),
		Indexer:   &Indexer{NewBadgerTxn(reader, writer, []byte(indexerPrefix), true, nextVersion, true, log)},
		metrics:   metrics,
		root:      id.Root,
		statsChan: make(chan StoreStat, 5000),
	}
	st.logData()
	return st, nil
}

// NewReadOnly() returns a store without a writer - meant for historical read only queries
func (s *Store) NewReadOnly(queryVersion uint64) (lib.StoreI, lib.ErrorI) {
	var useHistorical bool
	// if the query is for the latest version use the HSS over the LSS
	if s.version != queryVersion {
		useHistorical = true
	}
	// make a reader for the specified version
	reader := s.db.NewTransactionAt(queryVersion, false)
	// return the store object
	st := &Store{
		version:       queryVersion,
		log:           s.log,
		db:            s.db,
		writer:        nil,
		lss:           NewBadgerTxn(s.db.NewTransactionAt(lssVersion, false), nil, []byte(latestStatePrefix), true, 0, false, s.log),
		hss:           NewBadgerTxn(reader, nil, []byte(historicStatePrefix), false, 0, false, s.log),
		sc:            NewDefaultSMT(NewBadgerTxn(reader, nil, []byte(stateCommitmentPrefix), false, 0, false, s.log)),
		Indexer:       &Indexer{NewBadgerTxn(reader, nil, []byte(indexerPrefix), true, 0, false, s.log)},
		useHistorical: useHistorical,
		metrics:       s.metrics,
		root:          bytes.Clone(s.root),
		statsChan:     make(chan StoreStat, 5000),
	}
	st.logData()
	return st, nil
}

// Copy() make a copy of the store with a new read/write transaction
// this can be useful for having two simultaneous copies of the store
// ex: Mempool state and FSM state
func (s *Store) Copy() (lib.StoreI, lib.ErrorI) {
	nextVersion := s.version + 1
	// create a comparable writer and reader
	writer, reader := s.db.NewWriteBatchAt(nextVersion), s.db.NewTransactionAt(s.version, false)
	// return the store oebject
	st := &Store{
		version:   s.version,
		log:       s.log,
		db:        s.db,
		writer:    writer,
		lss:       NewBadgerTxn(s.db.NewTransactionAt(lssVersion, false), writer, []byte(latestStatePrefix), true, lssVersion, true, s.log),
		hss:       NewBadgerTxn(reader, writer, []byte(historicStatePrefix), false, nextVersion, true, s.log),
		sc:        NewDefaultSMT(NewBadgerTxn(reader, writer, []byte(stateCommitmentPrefix), false, nextVersion, true, s.log)),
		Indexer:   &Indexer{NewBadgerTxn(reader, writer, []byte(indexerPrefix), true, nextVersion, true, s.log)},
		metrics:   s.metrics,
		root:      bytes.Clone(s.root),
		statsChan: make(chan StoreStat, 5000),
	}
	st.logData()
	return st, nil
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
	size, entries := getSizeAndCountFromBatch(s.writer)
	// update the metrics once complete
	defer s.metrics.UpdateStoreMetrics(size, entries, time.Time{}, time.Now())
	// finally commit the entire Transaction to the actual DB under the proper version (height) number
	if e := s.Flush(); e != nil {
		return nil, e
	}
	now := time.Now()
	s.log.Infof("flushing batch writer to disk, version: %d size %d count %d", s.version, size, entries)
	if e := s.writer.Flush(); e != nil {
		return nil, ErrCommitDB(e)
	}
	s.log.Infof("flushed batch writer to disk, took: %s", time.Since(now).String())
	// reset the writer for the next height
	s.resetWriter()
	// return the root
	s.log.Info("resetting store")
	return bytes.Clone(s.root), nil
}

// Flush() writes the current state to the batch writer without committing it.
func (s *Store) Flush() lib.ErrorI {
	s.log.Infof("writing to sc writer, len: %d", len(s.sc.store.(*Txn).cache.ops))
	if e := s.sc.store.(TxnWriterI).Flush(); e != nil {
		return ErrCommitDB(e)
	}
	s.log.Infof("writing to lss writer, len: %d", len(s.lss.cache.ops))
	if e := s.lss.Flush(); e != nil {
		return ErrCommitDB(e)
	}
	s.log.Infof("writing to hss writer, len: %d", len(s.hss.cache.ops))
	if e := s.hss.Flush(); e != nil {
		return ErrCommitDB(e)
	}
	s.log.Infof("writing to indexer writer, len: %d", len(s.Indexer.db.cache.ops))
	if e := s.Indexer.db.Flush(); e != nil {
		return ErrCommitDB(e)
	}
	return nil
}

// Set() sets the value bytes blob in the LatestStateStore and the HistoricalStateStore
// as well as the value hash in the StateCommitStore referenced by the 'key' and hash('key') respectively
func (s *Store) Set(k, v []byte) lib.ErrorI {
	// set in the state store @ latest
	now := time.Now()
	if err := s.lss.Set(k, v); err != nil {
		return err
	}
	s.statsChan <- StoreStat{
		store:    latestStatePrefix,
		duration: time.Since(now),
	}
	now = time.Now()
	// set in the state store @ historical partition
	if err := s.hss.Set(k, v); err != nil {
		return err
	}
	s.statsChan <- StoreStat{
		store:    historicStatePrefix,
		duration: time.Since(now),
	}
	now = time.Now()
	// set in the state commit store
	if err := s.sc.Set(k, v); err != nil {
		return err
	}
	s.statsChan <- StoreStat{
		store:    stateCommitmentPrefix,
		duration: time.Since(now),
	}
	return nil
}

// Delete() removes the key-value pair from both the LatestStateStore, HistoricalStateStore, and CommitStore
func (s *Store) Delete(k []byte) lib.ErrorI {
	// delete from the state store @ latest
	if err := s.lss.Delete(k); err != nil {
		return err
	}
	// delete from the state store @ historical partition
	if err := s.hss.Delete(k); err != nil {
		return err
	}
	// delete from the state commit store
	return s.sc.Delete(k)
}

// Get() returns the value bytes blob from the State Store
func (s *Store) Get(key []byte) ([]byte, lib.ErrorI) {
	// if reading from a historical partition
	if s.useHistorical {
		return s.hss.Get(key)
	}
	// if reading from the latest
	return s.lss.Get(key)
}

// Iterator() returns an object for scanning the StateStore starting from the provided prefix.
// The iterator allows forward traversal of key-value pairs that match the prefix.
func (s *Store) Iterator(p []byte) (lib.IteratorI, lib.ErrorI) {
	// if reading from a historical partition
	if s.useHistorical {
		return s.hss.Iterator(p)
	}
	// if reading from latest
	return s.lss.Iterator(p)
}

// RevIterator() returns an object for scanning the StateStore starting from the provided prefix.
// The iterator allows backward traversal of key-value pairs that match the prefix.
func (s *Store) RevIterator(p []byte) (lib.IteratorI, lib.ErrorI) {
	// if reading from a historical partition
	if s.useHistorical {
		return s.hss.RevIterator(p)
	}
	// if reading from latest
	return s.lss.RevIterator(p)
}

// GetProof() uses the StateCommitStore to prove membership and non-membership
func (s *Store) GetProof(key []byte) ([]*lib.Node, lib.ErrorI) { return s.sc.GetMerkleProof(key) }

// VerifyProof() checks the validity of a member or non-member proof from the StateCommitStore
// by verifying the proof against the provided key, value, and proof data.
func (s *Store) VerifyProof(key, value []byte, validateMembership bool, root []byte, proof []*lib.Node) (bool, lib.ErrorI) {
	return s.sc.VerifyProof(key, value, validateMembership, root, proof)
}

// Version() returns the current version number of the Store, representing the height or version
// number of the state. This is used to track the versioning of the state data.
func (s *Store) Version() uint64 { return s.version }

// NewTxn() creates and returns a new transaction for the Store, allowing atomic operations
// on the StateStore, StateCommitStore, Indexer, and CommitIDStore.
func (s *Store) NewTxn() lib.StoreI {
	nextVersion := s.version + 1
	st := &Store{
		version:   s.version,
		log:       s.log,
		db:        s.db,
		writer:    s.writer,
		lss:       NewTxn(s.lss, s.lss, nil, true, lssVersion, true, s.log),
		hss:       NewTxn(s.hss, s.hss, nil, false, nextVersion, true, s.log),
		sc:        NewDefaultSMT(NewTxn(s.sc.store.(TxnReaderI), s.sc.store.(TxnWriterI), nil, false, nextVersion, true, s.log)),
		Indexer:   &Indexer{NewTxn(s.Indexer.db, s.Indexer.db, nil, true, nextVersion, true, s.log)},
		metrics:   s.metrics,
		root:      bytes.Clone(s.root),
		statsChan: make(chan StoreStat, 5000),
	}
	st.logData()
	return st
}

// DB() returns the underlying BadgerDB instance associated with the Store, providing access
// to the database for direct operations and management.
func (s *Store) DB() *badger.DB { return s.db }

// Root() retrieves the root hash of the StateCommitStore, representing the current root of the
// Sparse Merkle Tree. This hash is used for verifying the integrity and consistency of the state.
func (s *Store) Root() (root []byte, err lib.ErrorI) {
	if err = s.sc.Commit(); err != nil {
		return nil, err
	}
	return s.sc.Root(), nil
}

// Reset() discard and re-sets the stores writer
func (s *Store) Reset() { s.resetWriter() }

// Discard() closes the reader and writer
func (s *Store) Discard() {
	s.lss.reader.Discard()
	s.hss.reader.Discard()
	if s.writer != nil {
		s.writer.Cancel()
	}
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
	// create new transactions first before discarding old ones
	newReader := s.db.NewTransactionAt(s.version, false)
	newLSSReader := s.db.NewTransactionAt(lssVersion, false)
	// create a new batch writer for the next version as the version cannot
	// be set at the commit time
	newWriter := s.db.NewWriteBatchAt(s.version + 1)
	// create all new transaction-dependent objects
	newLSS := NewBadgerTxn(newLSSReader, newWriter, []byte(latestStatePrefix), true, lssVersion, true, s.log)
	newHSS := NewBadgerTxn(newReader, newWriter, []byte(historicStatePrefix), false, s.version+1, true, s.log)
	newSC := NewDefaultSMT(NewBadgerTxn(newReader, newWriter, []byte(stateCommitmentPrefix), false, s.version+1, true, s.log))
	newIndexer := NewBadgerTxn(newReader, newWriter, []byte(indexerPrefix), true, s.version+1, true, s.log)
	// only after creating all new objects, discard old transactions
	s.lss.reader.Discard()
	s.hss.reader.Discard()
	s.writer.Cancel()
	// update all references
	s.writer = newWriter
	s.lss = newLSS
	s.hss = newHSS
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
	bz, err = NewTxn(s.hss.reader, nil, nil, false, 0, false, s.log).Get(s.commitIDKey(version))
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
	w := NewTxn(s.hss.reader, s.writer, nil, false, version, false, s.log)
	value, err := lib.Marshal(&lib.CommitID{Height: version, Root: root})
	if err != nil {
		return err
	}
	if err = w.Set([]byte(lastCommitIDPrefix), value); err != nil {
		return err
	}
	if err = w.Set(s.commitIDKey(version), value); err != nil {
		return err
	}
	if e := w.Flush(); e != nil {
		return ErrCommitDB(e)
	}
	return nil
}

// getLatestCommitID() retrieves the latest CommitID from the database
func getLatestCommitID(db *badger.DB, log lib.LoggerI) (id *lib.CommitID) {
	reader := db.NewTransactionAt(math.MaxUint64, false)
	tx := NewBadgerTxn(reader, nil, nil, false, 0, false, log)
	defer reader.Discard()
	bz, err := tx.Get([]byte(lastCommitIDPrefix))
	if err != nil {
		log.Fatalf("getLatestCommitID() failed with err: %s", err.Error())
	}
	id = new(lib.CommitID)
	if err = lib.Unmarshal(bz, id); err != nil {
		log.Fatalf("unmarshalCommitID() failed with err: %s", err.Error())
	}
	return
}
