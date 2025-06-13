package store

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble"
)

// enforce interface compliance
// var _ lib.RStoreI = &VersionedStore{}

const (
	// VersionSize of the version field in bytes
	VersionSize     = 8
	TombstoneMarker = byte(0x01) // marker for deleted keys
	LiveMarker      = byte(0x00) // marker for live keys
)

// VersionedStore represents a versioned key-value store using Pebble.
type VersionedStore struct {
	reader    *pebble.Snapshot
	version   uint64
	readCache bool // whether to read keys that are not yet committed
}

// NewVersionedStore creates a new VersionedStore with the given database and initial version.
func NewVersionedStore(db *pebble.Snapshot, version uint64, readCache bool) *VersionedStore {
	return &VersionedStore{
		reader:    db,
		version:   version,
		readCache: readCache,
	}
}

func (vs *VersionedStore) Get(key []byte) (value []byte, err lib.ErrorI) {
	// sanity check for empty key
	if len(key) == 0 {
		return nil, ErrInvalidKey()
	}
	// look for the existing key at a lower version or with a tombstone marker
	iterOpts := &pebble.IterOptions{
		// lowest possible version (live)
		LowerBound: vs.makeVersionedKey(key, 0, false),
		// highest possible version (tombstone), endBytes added as is not inclusive
		UpperBound: lib.Append(vs.makeVersionedKey(key, vs.Version(), true), endBytes),
	}
	iter, IterErr := vs.reader.NewIter(iterOpts)
	if IterErr != nil {
		return nil, ErrStoreGet(IterErr)
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
	// exit
	return value, err
}

// get retrieves the value for a key at a specific version
func (vs *VersionedStore) get(key []byte, version uint64) ([]byte, lib.ErrorI) {
	// sanity check for empty key
	if len(key) == 0 {
		return nil, ErrInvalidKey()
	}
	// create a composite key with the current version and no tombstone marker
	versionedKey := vs.makeVersionedKey(key, version, false)
	// retrieve the value from the database
	value, closer, err := vs.reader.Get(versionedKey)
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

func (vs *VersionedStore) Iterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	//TODO implement me
	panic("implement me")
}

func (vs *VersionedStore) RevIterator(prefix []byte) (*pebble.Iterator, lib.ErrorI) {
	//TODO implement me
	panic("implement me")
}

func (vs *VersionedStore) Set(key, value []byte) lib.ErrorI {
	//TODO implement me
	panic("implement me")
}

func (vs *VersionedStore) Delete(key []byte) lib.ErrorI {
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
