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
	db        *pebble.DB
	version   uint64
	readCache bool // whether to read keys that are not yet committed
}

// NewVersionedStore creates a new VersionedStore with the given database and initial version.
func NewVersionedStore(db *pebble.DB, version uint64, readCache bool) *VersionedStore {
	return &VersionedStore{
		db:        db,
		version:   version,
		readCache: readCache,
	}
}

// Get retrieves the value for a key at the latest version available.
func (vs *VersionedStore) Get(key []byte) ([]byte, lib.ErrorI) {
	// helper function to get the value for a key at given version
	value, err := vs.get(key, vs.Version())
	if err != nil && value == nil {
		return nil, err
	}
	// at this point, err == pebble.ErrNotFound
	// look for the existing key at a lower version or with a tombstone marker
	iterOpts := &pebble.IterOptions{
		// lowest possible version (live)
		LowerBound: vs.makeVersionedKey(key, 0, false),
		// highest possible version (tombstone), endBytes added as is not inclusive
		UpperBound: lib.Append(vs.makeVersionedKey(key, vs.Version(), true), endBytes),
	}
	iter, iterErr := vs.db.NewIter(iterOpts)
	if iterErr != nil {
		return nil, ErrStoreGet(iterErr)
	}
	// ensure the iterator is closed after use
	defer iter.Close()
	// iterate through the keys to find the latest version or tombstone
	for iter.Last(); iter.Valid(); iter.Prev() {
		iterKey, _, tombstone, err := vs.getVersionedKey(iter.Key())
		if err != nil {
			// if key cannot be parsed, skip it
			continue
		}
		// valid key found, check if it matches the requested key
		if bytes.Equal(key, iterKey) {
			if tombstone {
				// return nil to indicate that the key does not exist
				return nil, nil
			}
			// if the key matches, return the value
			return iter.Value(), nil
		}
	}
	// if no valid key is found, return nil to indicate the key does not exist
	return nil, nil
}

func (vs *VersionedStore) GetIter(key []byte) ([]byte, lib.ErrorI) {
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
	iter, err := vs.db.NewIter(iterOpts)
	if err != nil {
		return nil, ErrStoreGet(err)
	}
	// ensure the iterator is closed after use
	defer iter.Close()
	// iterate through the keys to find the latest version or tombstone
	for iter.Last(); iter.Valid(); iter.Prev() {
		iterKey, _, tombstone, err := vs.getVersionedKey(iter.Key())
		if err != nil {
			// if key cannot be parsed, skip it
			continue
		}
		// valid key found, check if it matches the requested key
		if bytes.Equal(key, iterKey) {
			if tombstone {
				// return nil to indicate that the key does not exist
				return nil, nil
			}
			// if the key matches, return the value
			return iter.Value(), nil
		}
	}
	// if no valid key is found, return nil to indicate the key does not exist
	return nil, nil
}

// get retrieves the value for a key at a specific version
func (vs *VersionedStore) get(key []byte, version uint64) ([]byte, lib.ErrorI) {
	// sanity check for empty key
	if len(key) == 0 {
		return nil, ErrInvalidKey()
	}
	// create a composite key with the current version and no tombstone marker
	compositeKey := vs.makeVersionedKey(key, version, false)
	// retrieve the value from the database
	value, closer, err := vs.db.Get(compositeKey)
	// if the key is found, return the value
	if err != nil {
		// check for errors not related to key not found
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrStoreGet(err)
		}
		return nil, nil // key not found, return nil
	}
	// ensure the closer is closed to release resources
	defer closer.Close()
	// copy the value before returning (since the original may get overwritten)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
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
