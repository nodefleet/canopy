package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
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
//
// Key Layout: [UserKey][8-byte Version][1-byte TombstoneMarker]
// - Versions are stored in big-endian format for lexicographical ordering
// - Version 0 and math.MaxUint64 are reserved (invalid)
// - TombstoneMarker: 0x00 = live, 0x01 = deleted
//
// The store supports:
// - MVCC (Multi-Version Concurrency Control) reads at any version ≤ current version
// - Uncommitted read capability via readUncommitted flag
// - Atomic batch operations via the underlying Pebble batch
type VersionedStore struct {
	reader *pebble.Snapshot
	// writer is a Pebble batch that allows writing multiple operations atomically.
	// a pebble.NewIndexedBatch() is needed in order to read uncommitted keys
	writer          *pebble.Batch
	version         uint64
	readUncommitted bool // whether to read keys that are not yet committed
	// whether the writer has been committed. Prevents PebbleDB panics on
	// committed batches by returning an error instead.
	committed atomic.Bool
}

// NewVersionedStore creates a new VersionedStore with the given database and initial version.
func NewVersionedStore(reader *pebble.Snapshot, writer *pebble.Batch, version uint64, readUncommitted bool) (*VersionedStore, lib.ErrorI) {
	// maximum version is math.MaxUint64 - 1 due to basic type limit
	if version == math.MaxUint64 {
		version = math.MaxUint64 - 1
	}
	return &VersionedStore{
		reader:          reader,
		writer:          writer,
		version:         version,
		readUncommitted: readUncommitted,
	}, nil
}

// Get() reads the highest version of a value stored for a key that is less than or equal to store version
func (vs *VersionedStore) Get(key []byte) (value []byte, err lib.ErrorI) {
	// if readUncommitted is enabled, read from the batch
	if vs.readUncommitted && !vs.committed.Load() {
		return vs.get(key, vs.writer, vs.nextVersion()+1)
	}
	// else get the value from the reader
	return vs.get(key, vs.reader, vs.nextVersion())
}

// get() performs a reverse seek operation on a reader to find
// the highest version of a value stored for a key that is less than or equal to store version
func (vs *VersionedStore) get(key []byte, reader pebble.Reader, version uint64) (value []byte, err lib.ErrorI) {
	// create an iterator for the reader
	iter, e := reader.NewIter(&pebble.IterOptions{
		// lowest possible version (live)
		LowerBound: makeVersionedKey(key, 0, false),
		// highest possible version with (tombstone)
		UpperBound: makeVersionedKey(key, math.MaxUint64, true),
	})
	if e != nil {
		return nil, ErrStoreGet(e)
	}
	// ensure the iterator is closed when done
	defer iter.Close()
	// seek the key lower than the next version
	if !iter.SeekLT(makeVersionedKey(key, version, false)) {
		// if no key is found, return nil
		return
	}
	// extract the logical key from the iterator
	logicalKey, _, tombstone, err := getVersionedKey(iter.Key())
	// if an error occurred
	if err != nil {
		return
	}
	// if the key is deleted
	if tombstone {
		return
	}
	// the key isn't expected
	if !bytes.Equal(key, logicalKey) {
		return
	}
	// exit
	return iter.Value(), nil
}

// Set stores a value for a key at the next version to the underlying batch, note that values are
// not yet committed and will not be visible to readers until the batch is committed. (or readUncommitted is enabled)
func (vs *VersionedStore) Set(key, value []byte) lib.ErrorI { return vs.update(key, value, false) }

// Delete marks a key as deleted (tombstoned) at the next version in the underlying batch, note that values are
// not yet committed and will not be visible to readers until the batch is committed. (or readUncommitted is enabled)
func (vs *VersionedStore) Delete(key []byte) lib.ErrorI {
	// prevent a live version and a tombstone marker existing at the same time
	if err := vs.writer.Delete(makeVersionedKey(key, vs.nextVersion(), false), nil); err != nil {
		return ErrStoreDelete(err)
	}
	// set the value to nil to mark as deleted (tombstoned)
	return vs.update(key, nil, true)
}

// set() sets the value for a key at the next version
func (vs *VersionedStore) update(key []byte, value []byte, tombstone bool) lib.ErrorI {
	// check if the store is already committed
	if vs.committed.Load() {
		return ErrStoreCommitted()
	}
	// create a composite key with the next version and no tombstone marker
	versionedKey := makeVersionedKey(key, vs.nextVersion(), tombstone)
	// set the value in the database
	if err := vs.writer.Set(versionedKey, value, nil); err != nil {
		return ErrStoreSet(err)
	}
	return nil
}

// Commit commits the underlying batch to the database, making all changes visible.
func (vs *VersionedStore) Commit() lib.ErrorI {
	// mark the store as committed
	if vs.committed.Swap(true) {
		// if store already committed
		return ErrStoreCommitted()
	}
	// commit the underlying batch
	if err := vs.writer.Commit(pebble.Sync); err != nil {
		return ErrCommitDB(err)
	}
	return nil
}

// nextVersion returns the next version number for the store.
func (vs *VersionedStore) nextVersion() uint64 {
	if vs.version == math.MaxUint64 {
		return math.MaxUint64
	}
	return vs.version + 1
}

// NewIterator creates a new iterator for the versioned store
func (vs *VersionedStore) NewIterator(prefix []byte, reverse, allVersions bool) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, reverse, allVersions)
}

// Iterator creates a new iterator that iterates over all the keys in the store up to the current version.
func (vs *VersionedStore) Iterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, false, false)
}

// RevIterator creates a new iterator that iterates over all versions of keys in the store in reverse order.
func (vs *VersionedStore) RevIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, true, false)
}

// ArchiveIterator creates a new iterator that iterates over all versions of keys in the store.
func (vs *VersionedStore) ArchiveIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return vs.iterator(prefix, false, true)
}

// Discard closes the reader of the versioned store.
func (vs *VersionedStore) Discard() {
	vs.reader.Close()
}

// Cancel closes the writer of the versioned store.
func (vs *VersionedStore) Cancel() {
	// close the writer
	vs.writer.Close()
	// mark the store as committed
	vs.committed.Store(true)
}

// iterator creates a new iterator for the versioned store.
func (vs *VersionedStore) iterator(prefix []byte, reverse bool, allVersions bool) (lib.IteratorI, lib.ErrorI) {
	reader := (pebble.Reader)(vs.reader)
	// if readUncommitted is enabled use the batch to read
	if vs.readUncommitted && !vs.committed.Load() {
		reader = vs.writer
	}
	it, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: endPrefix(prefix),
	})
	if err != nil {
		return nil, ErrStoreIterator(err)
	}
	return NewVersionedIterator(prefix, it, vs.version, reverse, allVersions), nil
}

// VersionedIterator CODE BELOW

// VersionedIterator is an iterator that allows iterating over versioned keys in a Pebble database.
type VersionedIterator struct {
	iter        *pebble.Iterator
	version     uint64
	allVersions bool
	prefix      []byte
	reverse     bool
	valid       bool
	once        sync.Once
}

// NewVersionedIterator creates a new VersionedIterator
func NewVersionedIterator(prefix []byte, iter *pebble.Iterator, version uint64, reverse,
	allVersions bool) *VersionedIterator {
	vi := &VersionedIterator{
		iter:        iter,
		version:     version,
		allVersions: allVersions,
		prefix:      prefix,
		reverse:     reverse,
		once:        sync.Once{},
	}

	if reverse {
		iter.Last()
	} else {
		iter.First()
	}

	vi.valid = vi.iter.Valid()

	if !allVersions {
		// position iterator at the first valid entry
		// (handles version filtering and tombstone skipping)
		vi.Next()
	}

	return vi
}

// Valid returns true if the iterator is positioned at a valid key/value pair based on the options
func (vi *VersionedIterator) Valid() bool { return vi.valid }

// Key returns the key of the current key in the iterator.
func (vi *VersionedIterator) Key() []byte {
	// extract the actual key
	actualKey, _, _, err := getVersionedKey(vi.iter.Key())
	if err != nil {
		return nil
	}
	return actualKey
}

// Value returns the value associated with the current key in the iterator.
func (vi *VersionedIterator) Value() []byte { return bytes.Clone(vi.iter.Value()) }

// Close closes the iterator and releases any resources it holds.
func (vi *VersionedIterator) Close() { vi.iter.Close() }

// Next moves the iterator to the next key in the versioned key space.
func (vi *VersionedIterator) Next() { vi.valid = vi.next() }

// Next moves the iterator to the next key in the versioned key space.
// Allows Next to be called directly without needing to check if the iterator is reverse or not.
func (vi *VersionedIterator) next() bool {
	// if using an archive iterator
	if vi.allVersions {
		if vi.reverse {
			return vi.iter.Prev()
		}
		return vi.iter.Next()
	}
	// only move to the next key if passed the first iteration
	if vi.started() {
		// get current logical key
		k, _, _, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return false
		}
		// move until a key with a valid version (lower or equal than the set version) is found
		if !vi.moveToNextLogicalKey(k) {
			return false
		}
	}
	// find the next valid key, skipping any tombstones
	return vi.seekNextValidKey()
}

// seekNextValidKey seeks to the next valid (non-tombstone) key within the version bounds
func (vi *VersionedIterator) seekNextValidKey() bool {
	for {
		// get the logical 'start' key
		startKey, _, _, err := getVersionedKey(vi.iter.Key())
		if err != nil {
			return false
		}
		// seek to the highest version under 'next version'
		if !vi.iter.SeekLT(makeVersionedKey(startKey, vi.nextVersion(), false)) {
			return false
		}
		seekedKey, version, tombstone, err := getVersionedKey(vi.iter.Key())
		// the iterator is invalid if forward and the seeked key is different than the start key
		if err != nil || (!vi.reverse && !bytes.Equal(seekedKey, startKey)) {
			return false
		}
		// valid key found, break
		if version <= vi.version && !tombstone {
			return true
		}
		// otherwise, continue to the next key
		if !vi.moveToNextLogicalKey(seekedKey) {
			return false
		}
	}
}

// moveToNextLogicalKey moves the iterator to the next logical key
func (vi *VersionedIterator) moveToNextLogicalKey(current []byte) bool {
	// seek to the key just after the current logical key
	if vi.reverse {
		return vi.iter.SeekLT(current)
	}
	return vi.iter.SeekGE(endPrefix(current))
}

// started returns if the iterator is on the first cycle
func (vi *VersionedIterator) started() (started bool) {
	started = true
	vi.once.Do(func() { started = false })
	return
}

// nextVersion returns the next version number for the iterator.
func (vi *VersionedIterator) nextVersion() uint64 {
	if vi.version == math.MaxUint64 {
		return math.MaxUint64
	}
	return vi.version + 1
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
func getVersionedKey(key []byte) (actualKey []byte, version uint64, tombstone bool, err lib.ErrorI) {
	if len(key) < VersionSize+1 {
		// set error
		err = ErrInvalidKey()
		// exit with error
		return
	}
	// extract the indices of version and tombstone
	tombstoneIdx := len(key) - 1
	versionIdx := len(key) - (VersionSize + 1)
	// extract the version and tombstone marker from the key
	version = binary.BigEndian.Uint64(key[versionIdx:tombstoneIdx])
	tombstone = key[tombstoneIdx] == TombstoneMarker
	// extract the actual key part
	actualKey = make([]byte, versionIdx)
	copy(actualKey, key[:versionIdx])
	// exit
	return
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
	return nil
}
