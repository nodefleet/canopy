package store

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionedStoreGet(t *testing.T) {
	tests := []struct {
		name string
		// function to prepare the store state (e.g., prepopulate data)
		storeState    func(t *testing.T, vs *VersionedStore, db *pebble.DB)
		key           []byte
		expectedValue []byte
		expectedError lib.ErrorI
		version       uint64
	}{
		{
			name:          "empty key returns ErrInvalidKey",
			storeState:    func(t *testing.T, vs *VersionedStore, db *pebble.DB) {},
			key:           nil,
			expectedValue: nil,
			expectedError: ErrInvalidKey(),
		},
		{
			name:          "key not found returns nil value",
			storeState:    func(t *testing.T, vs *VersionedStore, db *pebble.DB) {},
			key:           []byte("non-existent-key"),
			expectedValue: nil,
			expectedError: nil,
		},
		{
			name: "fetch existing key",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("existing-key")
				value := []byte("value")
				err := db.Set(vs.makeVersionedKey(key,
					vs.version, false), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("existing-key"),
			expectedValue: []byte("value"),
			expectedError: nil,
		},
		{
			name: "tombstoned key returns nil value",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("tombstoned-key")
				value := []byte("value")
				err := db.Set(vs.makeVersionedKey(key,
					vs.nextVersion(), true), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("tombstoned-key"),
			expectedValue: nil,
			expectedError: nil,
		},
		{
			name: "fetch existing key at a lower version",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("lower-version-key")
				err := db.Set(vs.makeVersionedKey(key, 0, false), []byte("value0"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(vs.makeVersionedKey(key, 1, false), []byte("value1"), pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("lower-version-key"),
			expectedValue: []byte("value1"),
			expectedError: nil,
			version:       10,
		},
		{
			name: "fetch existing key at specific version",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("specific-version-key")
				err := db.Set(vs.makeVersionedKey(key, 0, false), []byte("value0"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(vs.makeVersionedKey(key, 1, false), []byte("value1"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(vs.makeVersionedKey(key, 2, false), []byte("value2"), pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("specific-version-key"),
			expectedValue: []byte("value1"),
			expectedError: nil,
			version:       1,
		},
		{
			name: "tombstoned key at a lower version",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("lower-version-key")
				value := []byte("value")
				err := db.Set(vs.makeVersionedKey(key, 0, false), value, pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(vs.makeVersionedKey(key, 1, true), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("lower-version-key"),
			expectedValue: nil,
			expectedError: nil,
			version:       10,
		},
		{
			name: "tombstoned key at a current version",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("current-version-key")
				value := []byte("value")
				err := db.Set(vs.makeVersionedKey(key, 1, false), value, pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(vs.makeVersionedKey(key, 10, true), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("current-version-key"),
			expectedValue: nil,
			expectedError: nil,
			version:       10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup pebble DB
			db := newTestDb(t)
			defer db.Close()
			// initialize VersionedStore
			store := NewVersionedStore(db.NewSnapshot(), tt.version, false)
			// apply the store state setup function
			tt.storeState(t, store, db)
			// update the store snapshot to ensure access to the latest data
			store.reader = db.NewSnapshot()
			// invoke the method being tested
			result, err := store.Get(tt.key)
			// assert results
			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
				assert.True(t, bytes.Equal(tt.expectedValue, result))
			}
		})
	}
}

func TestKeyVersioning(t *testing.T) {
	tests := []struct {
		name      string
		key       []byte
		version   uint64
		tombstone bool
	}{
		{
			name:      "simple key with no tombstone",
			key:       []byte("testkey"),
			version:   42,
			tombstone: false,
		},
		{
			name:      "empty key with no tombstone",
			key:       []byte{},
			version:   100,
			tombstone: false,
		},
		{
			name:      "binary key with no tombstone",
			key:       []byte{0x01, 0x02, 0x03, 0x04},
			version:   9999,
			tombstone: false,
		},
		{
			name:      "simple key with tombstone",
			key:       []byte("deletedkey"),
			version:   7654321,
			tombstone: true,
		},
		{
			name:      "binary key with tombstone",
			key:       []byte{0xFF, 0xEE, 0xDD, 0xCC},
			version:   0,
			tombstone: true,
		},
		{
			name:      "max version value",
			key:       []byte("maxversion"),
			version:   18446744073709551615, // max uint64 value
			tombstone: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create versioned store instance
			vs := &VersionedStore{version: tt.version}
			// test round-trip: make versioned key and then extract the parts
			versionedKey := vs.makeVersionedKey(tt.key, tt.version, tt.tombstone)
			// extract the original parts using getVersionedKey
			extractedKey, extractedVersion, extractedTombstone, err := vs.getVersionedKey(versionedKey)
			// Verify no errors occurred
			require.NoError(t, err)
			// check that the extracted key matches the original
			require.Equal(t, tt.key, extractedKey)
			// check that the extracted version matches the original
			require.Equal(t, tt.version, extractedVersion)
			// check that the extracted tombstone flag matches the original
			require.Equal(t, tt.tombstone, extractedTombstone)
		})
	}
}

func newTestDb(t *testing.T) *pebble.DB {
	db, err := pebble.Open("", &pebble.Options{
		// Set options as needed for testing
		FS: vfs.NewMem(),
	})
	assert.NoError(t, err)
	return db
}

func BenchmarkGet(b *testing.B) {
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	require.NoError(b, err)
	defer db.Close()
	vs := NewVersionedStore(db.NewSnapshot(), 1, false)

	// amount of keys per version
	version0 := 500
	version1 := 500
	// pre populate the store with some data at version 0
	for i := range version0 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		err := db.Set(vs.makeVersionedKey(key, 0, false), value, pebble.Sync)
		require.NoError(b, err)
	}
	// pre populate the store with some data at version 1
	for i := version0; i < version1; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		err := db.Set(vs.makeVersionedKey(key, 1, false), value, pebble.Sync)
		require.NoError(b, err)
	}
	// update the versioned store to read from the latest snapshot
	vs.reader = db.NewSnapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + strconv.Itoa(i%version1))
		value, err := vs.Get(key)
		require.NoError(b, err)
		require.NotNil(b, value, "Expected value to be non-nil for idx %d", i)
	}
}
