package store

import (
	"bytes"
	"fmt"
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
			store := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version, false)
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

func TestVersionedStoreSet(t *testing.T) {
	tests := []struct {
		name string
		// operations to test over
		operations      func(t *testing.T, vs *VersionedStore) error
		key             []byte
		expectedValue   []byte
		expectedError   lib.ErrorI
		version         uint64
		readUncommitted bool
	}{
		{
			name: "empty key returns ErrInvalidKey",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Set(nil, nil)
			},
			key:           nil,
			expectedValue: nil,
			expectedError: ErrInvalidKey(),
		},
		{
			name: "basic set operation",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Set([]byte("key"), []byte("value"))
			},
			key:           []byte("key"),
			expectedValue: []byte("value"),
			expectedError: nil,
		},
		{
			name: "set empty value",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Set([]byte("key"), nil)
			},
			key:           []byte("key"),
			expectedValue: nil,
			expectedError: nil,
		},
		{
			name: "update existing uncommitted key",
			operations: func(t *testing.T, vs *VersionedStore) error {
				require.NoError(t, vs.Set([]byte("key"), []byte("value")))
				return vs.Set([]byte("key"), []byte("new_value"))
			},
			key:           []byte("key"),
			expectedValue: []byte("new_value"),
			expectedError: nil,
		},
		{
			name: "set after commit",
			operations: func(t *testing.T, vs *VersionedStore) error {
				require.NoError(t, vs.Set([]byte("key"), []byte("value")))
				require.NoError(t, vs.Commit())
				return vs.Set([]byte("key"), []byte("new_value"))
			},
			key:           []byte("key"),
			expectedValue: nil,
			expectedError: ErrStoreCommitted(),
		},
		{
			name: "cannot read uncommitted changes",
			operations: func(t *testing.T, vs *VersionedStore) error {
				require.NoError(t, vs.Set([]byte("key"), []byte("value")))
				value, err := vs.Get([]byte("key"))
				require.NoError(t, err)
				require.Nil(t, value)
				return nil
			},
			key:           []byte("key"),
			expectedValue: []byte("value"),
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup pebble DB
			db := newTestDb(t)
			defer db.Close()
			// initialize VersionedStore
			var batch *pebble.Batch
			// set batch type based on readUncommitted flag
			batch = db.NewBatch()
			if tt.readUncommitted {
				batch = db.NewIndexedBatch()
			}
			store := NewVersionedStore(db.NewSnapshot(), batch, tt.version, tt.readUncommitted)
			// apply the store state setup function
			err := tt.operations(t, store)
			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
				return
			}
			// commit the store to persist changes
			require.NoError(t, store.Commit())
			// update the store snapshot to ensure access to the latest data
			store = NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version+1, false)
			// get the latest value for the key
			result, err := store.Get(tt.key)
			// assert results
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(tt.expectedValue, result))
		})
	}
}

func TestVersionedStoreDelete(t *testing.T) {
	tests := []struct {
		name string
		// operations to test over
		operations      func(t *testing.T, vs *VersionedStore) error
		key             []byte
		expectedValue   []byte
		expectedError   lib.ErrorI
		version         uint64
		readUncommitted bool
	}{
		{
			name: "empty key returns ErrInvalidKey",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Delete(nil)
			},
			expectedError: ErrInvalidKey(),
		},
		{
			name: "set then delete should return nil",
			operations: func(t *testing.T, vs *VersionedStore) error {
				require.NoError(t, vs.Set([]byte("key"), []byte("value")))
				val, err := vs.Get([]byte("key"))
				require.NoError(t, err)
				require.True(t, bytes.Equal(val, []byte("value")))
				return vs.Delete([]byte("key"))
			},
			expectedValue:   nil,
			key:             []byte("key"),
			readUncommitted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup pebble DB
			db := newTestDb(t)
			defer db.Close()
			// initialize VersionedStore
			var batch *pebble.Batch
			// set batch type based on readUncommitted flag
			batch = db.NewBatch()
			if tt.readUncommitted {
				batch = db.NewIndexedBatch()
			}
			store := NewVersionedStore(db.NewSnapshot(), batch, tt.version, tt.readUncommitted)
			// apply the store state setup function
			err := tt.operations(t, store)
			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
				return
			}
			// commit the store to persist changes
			require.NoError(t, store.Commit())
			// update the store snapshot to ensure access to the latest data
			store = NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version+1, false)
			// get the latest value for the key
			result, err := store.Get(tt.key)
			// assert results
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(tt.expectedValue, result))
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

func TestGetSetDeleteVersioned(t *testing.T) {
	// setup pebble DB
	db := newTestDb(t)
	defer db.Close()
	version := uint64(1)
	// initialize VersionedStore
	vs := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), version, false)
	// set a key
	key := []byte("key")
	require.NoError(t, vs.Set(key, []byte("value")))
	// get the key when readUncommitted is false
	value, err := vs.Get(key)
	require.NoError(t, err)
	assert.Nil(t, value)
	// commit
	require.NoError(t, vs.Commit())
	// update the store snapshot to ensure access to the latest data
	version++
	vs = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	// get the key when readUncommitted is true
	value, err = vs.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(value, []byte("value")))
	// set a new value for the key
	require.NoError(t, vs.Set(key, []byte("new_value")))
	value, err = vs.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(value, []byte("new_value")))
	// delete the key
	err = vs.Delete(key)
	require.NoError(t, err)
	// commit
	require.NoError(t, vs.Commit())
	// update the store snapshot to ensure access to the latest data
	version++
	vs = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	// get the key after deletion
	value, err = vs.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(value, nil))
	// lower the store version
	version--
	vs = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	// get the key after deletion
	value, err = vs.Get(key)
	require.NoError(t, err)
	fmt.Printf("a: %s b: %s\n", value, []byte("value"))
	assert.True(t, bytes.Equal(value, []byte("value")))
}

func BenchmarkGet(b *testing.B) {
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	require.NoError(b, err)
	defer db.Close()
	vs := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), 1, false)

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

func newTestDb(t *testing.T) *pebble.DB {
	db, err := pebble.Open("", &pebble.Options{
		// Set options as needed for testing
		FS: vfs.NewMem(),
	})
	assert.NoError(t, err)
	return db
}
