package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble"
)

// enforce interface compliance
var _ lib.RStoreI = &VersionedStore{}

const (
	// VersionSize of the version field in bytes
	VersionSize     = 8
	TombstoneMarker = byte(0x01) // marker for deleted keys
	LiveMarker      = byte(0x00) // marker for live keys
)

// VersionedStore represents a versioned key-value store using Pebble.
type VersionedStore struct {
	reader *pebble.Snapshot
	// writer is a Pebble batch that allows writing multiple operations atomically.
	// an IndexedBatch is needed in order to read uncommitted keys
	writer         *pebble.Batch
	version        uint64
	readUncomitted bool // whether to read keys that are not yet committed
	// whether the writer has been committed. PebbleDB will panic if any
	// operation is performed on a batch that has been committed. this prevents
	// the panic to replace it with a more graceful error handling.
	committed atomic.Bool
}

// NewVersionedStore creates a new VersionedStore with the given database and initial version.
func NewVersionedStore(reader *pebble.Snapshot, writer *pebble.Batch, version uint64, readUncommitted bool) *VersionedStore {
	return &VersionedStore{
		reader:         reader,
		writer:         writer,
		version:        version,
		readUncomitted: readUncommitted,
	}
}

func (vs *VersionedStore) Get(key []byte) (value []byte, err lib.ErrorI) {
	// perform basic validation on the key
	if err := validateKey(key); err != nil {
		return nil, err
	}
	// if readCache is enabled, try to get the value from the writer first
	if vs.readUncomitted {
		// do not read from the writer if it has been committed
		if !vs.committed.Load() {
			value, err = vs.get(vs.writer, key, vs.nextVersion())
			if err != nil || value != nil {
				return value, err
			}
		}
	}
	// look for the existing key at a lower version or with a tombstone marker
	iterOpts := &pebble.IterOptions{
		// lowest possible version (live)
		LowerBound: vs.makeVersionedKey(key, 0, false),
		// highest possible version (tombstone), endBytes added as is not inclusive
		UpperBound: lib.Append(vs.makeVersionedKey(key, vs.Version(), true), endBytes),
	}
	iter, iterErr := vs.reader.NewIter(iterOpts)
	if iterErr != nil {
		return nil, ErrStoreGet(iterErr)
	}
	// ensure the iterator is closed after use
	defer iter.Close()
	// iterate through the keys to find the latest version or tombstone
	for iter.Last(); iter.Valid(); iter.Prev() {
		// retrieve the versioned key
		iterKey, _, tombstone, err := vs.getVersionedKey(iter.Key())
		if err != nil || !bytes.Equal(key, iterKey) {
			// if key cannot be parsed or doesn't match the requested key, skip it
			continue
		}
		// we have a matching key
		if !tombstone {
			// if not a tombstone, get the value
			value = iter.Value()
		}
		// in either case (tombstone or valid value), the key is found, break
		break
	}
	// check if the iterator encountered an error
	if iterErr := iter.Error(); iterErr != nil {
		return nil, ErrStoreGet(iterErr)
	}
	// exit
	return value, err
}

// Set stores a value for a key at the next version to the underlying batch, note that values are
// not yet committed and will not be visible to readers until the batch is committed. (or if readCache is enabled)
func (vs *VersionedStore) Set(key, value []byte) lib.ErrorI {
	return vs.set(key, value, false)
}

// Delete marks a key as deleted (tombstoned) at the next version in the underlying batch, note that values are
// not yet committed and will not be visible to readers until the batch is committed. (or if readCache is enabled)
func (vs *VersionedStore) Delete(key []byte) lib.ErrorI {
	// actual deletion of live key if any. This is to prevent both a live a tombstone marker
	// existing at the same time
	vs.writer.Delete(vs.makeVersionedKey(key, vs.nextVersion(), false), nil)
	// set the value to nil to mark as deleted (tombstoned)
	return vs.set(key, nil, true)
}

func (vs *VersionedStore) set(key []byte, value []byte, tombstone bool) lib.ErrorI {
	// check if the store is already committed
	if vs.committed.Load() {
		return ErrStoreCommitted()
	}
	// perform basic validation on the key
	if err := validateKey(key); err != nil {
		return err
	}
	// create a composite key with the next version and no tombstone marker
	versionedKey := vs.makeVersionedKey(key, vs.nextVersion(), tombstone)
	// set the value in the database
	if err := vs.writer.Set(versionedKey, value, nil); err != nil {
		return ErrStoreSet(err)
	}
	return nil
}

// get retrieves the value for a key at a specific version
func (vs *VersionedStore) get(reader pebble.Reader, key []byte, version uint64) ([]byte, lib.ErrorI) {
	// perform basic validation on the key
	if err := validateKey(key); err != nil {
		return nil, err
	}
	// create a composite key with the current version and no tombstone marker
	versionedKey := vs.makeVersionedKey(key, version, false)
	// retrieve the value from the database
	value, closer, err := reader.Get(versionedKey)
	// if the key is found, return the value
	if err != nil {
		// check for errors not related to key not found
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrStoreGet(err)
		}
		// key not found, return nil
		return nil, nil
	}
	// ensure the closer is closed to release resources
	defer closer.Close()
	// copy the value before returning (since the original may get overwritten)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	// exit
	return valueCopy, nil
}

// Commit commits the underlying batch to the database, making all changes visible.
func (vs *VersionedStore) Commit() lib.ErrorI {
	// check if the store is already committed
	if vs.committed.Load() {
		return ErrStoreCommitted()
	}
	// commit the underlying batch
	if err := vs.writer.Commit(pebble.Sync); err != nil {
		return ErrCommitDB(err)
	}
	// mark the store as committed
	vs.committed.Store(true)
	return nil
}

func (vs *VersionedStore) Iterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	//TODO implement me
	panic("implement me")
}

func (vs *VersionedStore) RevIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	//TODO implement me
	panic("implement me")
}

// makeVersionedKey sets a composite key with the current version
// Format: [ActualKey][8-byte Version][1-byte TombstoneMarker]
func (vs *VersionedStore) makeVersionedKey(key []byte, version uint64, tombstone bool) []byte {
	var marker byte = LiveMarker
	if tombstone {
		marker = TombstoneMarker
	}
	encodedVersion := vs.encodeBigEndian(version)
	return lib.Append(key, lib.Append(encodedVersion, []byte{marker}))
}

// getVersionedKey extracts the actual key, version, and tombstone marker from a versioned key.
func (vs *VersionedStore) getVersionedKey(key []byte) (actualKey []byte, version uint64, tombstone bool, err error) {
	keyLen := len(key)
	if keyLen < VersionSize+1 {
		return nil, 0, false, ErrInvalidKey()
	}
	tombstoneIdx := keyLen - 1
	versionIdx := tombstoneIdx - VersionSize
	// extract the version and tombstone marker from the key
	version = binary.BigEndian.Uint64(key[versionIdx:tombstoneIdx])
	tombstone = key[tombstoneIdx] == TombstoneMarker
	// extract the actual key part
	actualKey = key[:versionIdx]
	return actualKey, version, tombstone, nil
}

// encodeBigEndian encodes a uint64 value into a byte slice in big-endian order using VersionSize bytes.
func (vs *VersionedStore) encodeBigEndian(i uint64) []byte {
	b := make([]byte, VersionSize)
	binary.BigEndian.PutUint64(b, i)
	return b
}

// Version returns the current version of the store.
func (vs *VersionedStore) Version() uint64 {
	return vs.version
}

// nextVersion returns the next version number for the store.
func (vs *VersionedStore) nextVersion() uint64 {
	return vs.version + 1
}

// validateKey performs basic validation on the key.
func validateKey(key []byte) lib.ErrorI {
	// sanity check for empty key
	if len(key) == 0 {
		return ErrInvalidKey()
	}
	// exit
	return nil
}
