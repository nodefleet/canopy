package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble/v2"
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
		LowerBound: makeVersionedKey(key, 0, false),
		// highest possible version (tombstone), endBytes added as is not inclusive
		UpperBound: lib.Append(makeVersionedKey(key, vs.Version(), true), endBytes),
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
		iterKey, _, tombstone, err := getVersionedKey(iter.Key())
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
	vs.writer.Delete(makeVersionedKey(key, vs.nextVersion(), false), nil)
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
	versionedKey := makeVersionedKey(key, vs.nextVersion(), tombstone)
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
	versionedKey := makeVersionedKey(key, version, false)
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

func (vs *VersionedStore) Iterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, false)
}

func (vs *VersionedStore) RevIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, true)
}

func (vs *VersionedStore) iterator(prefix []byte, reverse bool) (lib.IteratorI, lib.ErrorI) {
	it, err := vs.reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: endPrefix(prefix),
	})
	if err != nil {
		return nil, ErrStoreIterator(err)
	}
	return NewVersionedIterator(prefix, it, vs.version, reverse), nil
}

// VersionedIterator CODE BELOW

// VersionedIterator is an iterator that allows iterating over versioned keys in a Pebble database.
type VersionedIterator struct {
	iter    *pebble.Iterator
	version uint64
	prefix  []byte
	reverse bool
	started bool

	next func() bool
	prev func() bool
}

// NewVersionedIterator creates a new VersionedIterator
func NewVersionedIterator(prefix []byte, iter *pebble.Iterator, version uint64, reverse bool) *VersionedIterator {
	if reverse {
		// set the next and prev functions for reverse iteration
		iter.Last()
		return &VersionedIterator{
			iter:    iter,
			reverse: true,
			version: version,
			prefix:  prefix,
			next:    iter.Prev,
			prev:    iter.Next,
		}
	}

	vi := &VersionedIterator{
		iter:    iter,
		reverse: reverse,
		version: version,
		prefix:  prefix,
		next:    iter.Next,
		prev:    iter.Prev,
	}
	iter.First()
	vi.Next()
	return vi
}

func (vi *VersionedIterator) Valid() bool {
	return vi.iter.Valid()
}

// Next moves the iterator to the next item in the versioned key space.
func (vi *VersionedIterator) Next() {
	// only move to the next item if the iteration started
	if vi.started {
		if !vi.moveToNextLogicalKey() {
			return
		}
	} else {
		vi.started = true
	}
	// iterately find the next logical key, skipping any tombstones
	for {
		// get the current key
		key, _, _, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return
		}
		// append the next version than desired into the key
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, vi.version+1)
		keyEnd := append(key, buf...)
		// seek the key at the highest version lower than the version being iterated
		if !vi.iter.SeekLT(keyEnd) {
			return
		}
		// check if the current key is a tombstone
		_, _, tombstone, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return
		}
		// key is valid
		if !tombstone {
			break
		}
		// otherwise, continue to the next item
		if !vi.moveToNextLogicalKey() {
			return
		}
	}
}

func (vi *VersionedIterator) moveToNextLogicalKey() bool {
	// move the iterator to the next item in the versioned key space
	key, _, _, err := getVersionedKey(vi.iter.Key())
	if err != nil {
		return false
	}
	return vi.iter.SeekGE(endPrefix(key))
}

// Prev moves the iterator to the previous item in the versioned key space.
func (vi *VersionedIterator) Prev() {
	// only move if iteration started
	if vi.started {
		if !vi.moveToPrevLogicalKey() {
			return
		}
	} else {
		vi.started = true
	}
	// iteratively find the previous logical key, skipping tombstones
	for {
		// get the current key
		key, _, _, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return
		}
		// append the version just above the current version
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, vi.version+1)
		keyStart := append(key, buf...)
		// seek to the highest version <= desired version (in reverse, use SeekLE)
		// since we want the largest version <= vi.version, SeekLE(keyStart)
		if !vi.iter.SeekLT(keyStart) {
			return
		}
		// check if the current key is a tombstone
		_, _, tombstone, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return
		}
		if !tombstone {
			break
		}
		// otherwise, continue to the previous logical key
		if !vi.moveToPrevLogicalKey() {
			return
		}
	}
}

// moveToPrevLogicalKey moves the iterator to the previous logical key
func (vi *VersionedIterator) moveToPrevLogicalKey() bool {
	// get current logical key
	key, _, _, err := getVersionedKey(vi.iter.Key())
	if err != nil {
		return false
	}
	// Seek to the key just lower than the current logical key
	return vi.iter.SeekLT(key)
}

// Key returns the key of the current item in the iterator.
func (vi *VersionedIterator) Key() []byte {
	// valid check
	if !vi.Valid() {
		return nil
	}
	// extract the actual key
	actualKey, _, _, err := getVersionedKey(vi.iter.Key())
	if err != nil {
		return nil
	}
	// return the cleaned key without the prefix
	return removePrefix(actualKey, vi.prefix)
}

// Value returns the value associated with the current key in the iterator.
func (vi *VersionedIterator) Value() []byte {
	// valid check
	if !vi.Valid() {
		return nil
	}
	// retrieve the value from the iterator
	value := vi.iter.Value()
	if value == nil {
		return nil
	}
	// copy the value to return it safely
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy
}

// Close closes the iterator and releases any resources it holds.
func (vi *VersionedIterator) Close() {
	vi.iter.Close()
}

// makeVersionedKey sets a composite key with the current version
// Format: [ActualKey][8-byte Version][1-byte TombstoneMarker]
func makeVersionedKey(key []byte, version uint64, tombstone bool) []byte {
	// pre-allocate a buffer with the exact size needed
	buff := make([]byte, len(key)+VersionSize+1)
	// copy the key directly into the beginning of the buffer
	copy(buff, key)
	// encode version directly into the buffer at the appropriate offset
	binary.BigEndian.PutUint64(buff[len(key):], version)
	// set the tombstone marker at the end
	if tombstone {
		buff[len(buff)-1] = TombstoneMarker
	} else {
		buff[len(buff)-1] = LiveMarker
	}
	return buff
}

// getVersionedKey extracts the actual key, version, and tombstone marker from a versioned key.
func getVersionedKey(key []byte) (actualKey []byte, version uint64, tombstone bool, err error) {
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

// endPrefix constructs the end bound for a prefix by incrementing the last
// byte less than 0xFF.
func endPrefix(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // can't construct bound
}
