package store

import (
	"bytes"
	"math"
	"strconv"
	"testing"

	"github.com/canopy-network/canopy/lib"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
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
			version:       1,
		},
		{
			name:          "key not found returns nil value",
			storeState:    func(t *testing.T, vs *VersionedStore, db *pebble.DB) {},
			key:           []byte("non-existent-key"),
			expectedValue: nil,
			expectedError: nil,
			version:       1,
		},
		{
			name: "fetch existing key",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("existing-key")
				value := []byte("value")
				err := db.Set(makeVersionedKey(key,
					vs.version, false), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("existing-key"),
			expectedValue: []byte("value"),
			expectedError: nil,
			version:       1,
		},
		{
			name: "tombstoned key returns nil value",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("tombstoned-key")
				value := []byte("value")
				err := db.Set(makeVersionedKey(key,
					vs.nextVersion(), true), value, pebble.Sync)
				assert.NoError(t, err)
			},
			key:           []byte("tombstoned-key"),
			expectedValue: nil,
			expectedError: nil,
			version:       1,
		},
		{
			name: "fetch existing key at a lower version",
			storeState: func(t *testing.T, vs *VersionedStore, db *pebble.DB) {
				key := []byte("lower-version-key")
				err := db.Set(makeVersionedKey(key, 0, false), []byte("value0"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(makeVersionedKey(key, 1, false), []byte("value1"), pebble.Sync)
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
				err := db.Set(makeVersionedKey(key, 0, false), []byte("value0"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(makeVersionedKey(key, 1, false), []byte("value1"), pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(makeVersionedKey(key, 2, false), []byte("value2"), pebble.Sync)
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
				err := db.Set(makeVersionedKey(key, 0, false), value, pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(makeVersionedKey(key, 1, true), value, pebble.Sync)
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
				err := db.Set(makeVersionedKey(key, 1, false), value, pebble.Sync)
				assert.NoError(t, err)
				err = db.Set(makeVersionedKey(key, 10, true), value, pebble.Sync)
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
			vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version, false)
			require.NoError(t, err)
			// apply the store state setup function
			tt.storeState(t, vs, db)
			// update the store snapshot to ensure access to the latest data
			vs.reader = db.NewSnapshot()
			// invoke the method being tested
			result, err := vs.Get(tt.key)
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
			version:       1,
		},
		{
			name: "basic set operation",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Set([]byte("key"), []byte("value"))
			},
			key:           []byte("key"),
			expectedValue: []byte("value"),
			expectedError: nil,
			version:       1,
		},
		{
			name: "set empty value",
			operations: func(t *testing.T, vs *VersionedStore) error {
				return vs.Set([]byte("key"), nil)
			},
			key:           []byte("key"),
			expectedValue: nil,
			expectedError: nil,
			version:       1,
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
			version:       1,
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
			version:       1,
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
			version:       1,
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
			vs, libErr := NewVersionedStore(db.NewSnapshot(), batch, tt.version, tt.readUncommitted)
			require.NoError(t, libErr)
			// apply the store state setup function
			err := tt.operations(t, vs)
			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
				return
			}
			// commit the store to persist changes
			require.NoError(t, vs.Commit())
			// update the store snapshot to ensure access to the latest data
			vs, err = NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version+1, false)
			require.NoError(t, err)
			// get the latest value for the key
			result, err := vs.Get(tt.key)
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
			version:       1,
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
			version:         1,
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
			vs, libErr := NewVersionedStore(db.NewSnapshot(), batch, tt.version, tt.readUncommitted)
			require.NoError(t, libErr)
			// apply the store state setup function
			err := tt.operations(t, vs)
			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
				return
			}
			// commit the store to persist changes
			require.NoError(t, vs.Commit())
			// update the store snapshot to ensure access to the latest data
			vs, err = NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version+1, false)
			require.NoError(t, err)
			// get the latest value for the key
			result, err := vs.Get(tt.key)
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
			version:   math.MaxUint64,
			tombstone: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// test round-trip: make versioned key and then extract the parts
			versionedKey := makeVersionedKey(tt.key, tt.version, tt.tombstone)
			// extract the original parts using getVersionedKey
			extractedKey, extractedVersion, extractedTombstone, err := getVersionedKey(versionedKey)
			// verify no errors occurred
			require.NoError(t, err)
			// check that the extracted key matches the original
			require.True(t, bytes.Equal(tt.key, extractedKey))
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
	vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), version, false)
	require.NoError(t, err)
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
	vs, err = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	require.NoError(t, err)
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
	vs, err = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	require.NoError(t, err)
	// get the key after deletion
	value, err = vs.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(value, nil))
	// lower the store version
	version--
	vs, err = NewVersionedStore(db.NewSnapshot(), db.NewIndexedBatch(), version, true)
	require.NoError(t, err)
	// get the key after deletion
	value, err = vs.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(value, []byte("value")))
}

func TestVersionedIterator(t *testing.T) {
	type kvPair struct {
		key       []byte
		value     []byte
		version   uint64
		tombstone bool
	}
	// test cases
	tests := []struct {
		name        string
		testData    []kvPair
		prefix      []byte
		version     uint64
		reverse     bool
		allVersions bool
		expected    []kvPair
	}{
		{
			name:    "forward iteration over multiple keys",
			version: 1,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 1, false},
				{[]byte("key3"), []byte("value3"), 1, false},
			},
			expected: []kvPair{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
			},
		},
		{
			name:    "reverse iteration over multiple keys",
			version: 1,
			reverse: true,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 1, false},
				{[]byte("key3"), []byte("value3"), 1, false},
			},
			expected: []kvPair{
				{[]byte("key3"), []byte("value3"), 1, false},
				{[]byte("key2"), []byte("value2"), 1, false},
				{[]byte("key1"), []byte("value1"), 1, false},
			},
		},
		{
			name:    "iteration with prefix filtering",
			prefix:  []byte("prefix1:"),
			version: 1,
			testData: []kvPair{
				{[]byte("prefix1:key1"), []byte("value1"), 1, false},
				{[]byte("prefix1:key2"), []byte("value2"), 1, false},
				{[]byte("prefix2:key1"), []byte("value3"), 1, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 1, false},
			},
		},
		{
			name:        "archive iterates all versions",
			version:     1,
			allVersions: true,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value3"), 3, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value3"), 3, false},
			},
		},
		{
			name:    "get only keys with specific version",
			version: 2,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value2"), 2, false},
			},
		},
		{
			name:    "skip tombstoned keys",
			version: 2,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key3"), []byte("value2"), 2, true},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value2"), 2, false},
			},
		},
		{
			name:    "skip multiple tombstoned keys",
			version: 2,
			testData: []kvPair{
				{[]byte("key0"), []byte("value0"), 2, true},
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key4"), []byte("value2"), 2, true},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value2"), 2, false},
			},
		},
		{
			name:    "reverse skip multiple tombstoned keys",
			version: 2,
			reverse: true,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, false},
				{[]byte("key2"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key3"), []byte("value2"), 2, true},
				{[]byte("key4"), []byte("value2"), 2, true},
			},
			expected: []kvPair{
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key1"), []byte("value2"), 2, false},
			},
		},
		{
			name:    "get key readded in a later version",
			version: 10,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value1"), 3, true},
				{[]byte("key1"), []byte("value1"), 10, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
			},
		},
		{
			name:    "don't get key readded in later version",
			version: 9,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value1"), 3, true},
				{[]byte("key1"), []byte("value1"), 10, false},
			},
			expected: []kvPair{},
		},
		{
			name:     "empty database returns no results",
			version:  1,
			testData: []kvPair{},
			expected: []kvPair{},
		},
		{
			name:    "single key with multiple versions shows latest only",
			version: 10,
			testData: []kvPair{
				{[]byte("key"), []byte("value1"), 1, false},
				{[]byte("key"), []byte("value5"), 5, false},
				{[]byte("key"), []byte("value10"), 10, false},
			},
			expected: []kvPair{
				{[]byte("key"), []byte("value10"), 10, false},
			},
		},
		{
			name:    "complex version history with interleaved tombstones",
			version: 10,
			testData: []kvPair{
				{[]byte("key1"), []byte("v1"), 1, false},
				{[]byte("key1"), []byte("v3"), 3, false},
				{[]byte("key1"), []byte("v5"), 5, true},  // tombstone
				{[]byte("key1"), []byte("v7"), 7, false}, // readded
				{[]byte("key1"), []byte("v9"), 9, true},  // tombstone again
				{[]byte("key2"), []byte("v2"), 2, false},
				{[]byte("key2"), []byte("v4"), 4, true},  // tombstone
				{[]byte("key2"), []byte("v8"), 8, false}, // readded
			},
			expected: []kvPair{
				{[]byte("key2"), []byte("v8"), 8, false},
			},
		},
		{
			name:    "keys with special characters and UTF-8",
			version: 5,
			testData: []kvPair{
				{[]byte("key\x00with\x00nulls"), []byte("value1"), 1, false},
				{[]byte("keyüñicode"), []byte("value2"), 2, false},
				{[]byte("key-with-dashes"), []byte("value3"), 3, false},
			},
			expected: []kvPair{
				{[]byte("key\x00with\x00nulls"), []byte("value1"), 1, false},
				{[]byte("key-with-dashes"), []byte("value3"), 3, false},
				{[]byte("keyüñicode"), []byte("value2"), 2, false},
			},
		},
		{
			name:    "version boundary conditions",
			version: 2,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key3"), []byte("value3"), 3, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
			},
		},
		{
			name:        "archive iterator with tombstones",
			version:     5,
			allVersions: true,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, true},
				{[]byte("key1"), []byte("value3"), 3, false},
				{[]byte("key2"), []byte("value4"), 4, false},
				{[]byte("key2"), []byte("value5"), 5, true},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 2, true},
				{[]byte("key1"), []byte("value3"), 3, false},
				{[]byte("key2"), []byte("value4"), 4, false},
				{[]byte("key2"), []byte("value5"), 5, true},
			},
		},
		{
			name:    "reverse iteration with prefix at version boundaries",
			version: 5,
			prefix:  []byte("prefix:"),
			reverse: true,
			testData: []kvPair{
				{[]byte("prefix:key1"), []byte("value1"), 3, false},
				{[]byte("prefix:key2"), []byte("value2"), 5, false},
				{[]byte("prefix:key3"), []byte("value3"), 7, false},
				{[]byte("otherprefix:key"), []byte("value4"), 4, false},
			},
			expected: []kvPair{
				{[]byte("key2"), []byte("value2"), 5, false},
				{[]byte("key1"), []byte("value1"), 3, false},
			},
		},
		{
			name:    "key reuse across different versions",
			version: 10,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 2, false},
				{[]byte("key1"), []byte("value3"), 3, true}, // tombstone key1
				{[]byte("key3"), []byte("value4"), 4, false},
				{[]byte("key1"), []byte("value5"), 5, false}, // reintroduce key1
				{[]byte("key2"), []byte("value6"), 6, true},  // tombstone key2
				{[]byte("key2"), []byte("value7"), 7, false}, // reintroduce key2
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value5"), 5, false},
				{[]byte("key2"), []byte("value7"), 7, false},
				{[]byte("key3"), []byte("value4"), 4, false},
			},
		},
		{
			name:    "handle large version gaps",
			version: 10000,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 100, false},
				{[]byte("key3"), []byte("value3"), 1000, false},
				{[]byte("key4"), []byte("value4"), 10000, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key2"), []byte("value2"), 100, false},
				{[]byte("key3"), []byte("value3"), 1000, false},
				{[]byte("key4"), []byte("value4"), 10000, false},
			},
		},
		{
			name:    "zero-length key values",
			version: 5,
			testData: []kvPair{
				{[]byte("key1"), []byte{}, 1, false},
				{[]byte("key2"), []byte{}, 2, false},
				{[]byte("key3"), []byte("value"), 3, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte{}, 1, false},
				{[]byte("key2"), []byte{}, 2, false},
				{[]byte("key3"), []byte("value"), 3, false},
			},
		},
		{
			name:    "only tombstones no valid keys",
			version: 5,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, true},
				{[]byte("key2"), []byte("value2"), 2, true},
				{[]byte("key3"), []byte("value3"), 3, true},
			},
			expected: []kvPair{},
		},
		{
			name:    "binary keys with various version patterns",
			version: 5,
			testData: []kvPair{
				{[]byte{0x01, 0x02}, []byte("value1"), 1, false},
				{[]byte{0x01, 0x03}, []byte("value2"), 2, false},
				{[]byte{0x01, 0x02}, []byte("value3"), 3, true}, // tombstone earlier key
				{[]byte{0x01, 0x04}, []byte("value4"), 4, false},
			},
			expected: []kvPair{
				{[]byte{0x01, 0x03}, []byte("value2"), 2, false},
				{[]byte{0x01, 0x04}, []byte("value4"), 4, false},
			},
		},
		{
			name:    "version lower than all stored versions",
			version: 1,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 2, false},
				{[]byte("key2"), []byte("value2"), 3, false},
			},
			expected: []kvPair{},
		},
		{
			name:    "version exactly at boundary with multiple versions",
			version: 5,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 4, false},
				{[]byte("key1"), []byte("value2"), 5, false},
				{[]byte("key1"), []byte("value3"), 6, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("value2"), 5, false},
			},
		},
		{
			name:    "version gaps with tombstones in between",
			version: 100,
			testData: []kvPair{
				{[]byte("key1"), []byte("value1"), 1, false},
				{[]byte("key1"), []byte("value2"), 50, true},   // tombstone
				{[]byte("key1"), []byte("value3"), 200, false}, // beyond version
			},
			expected: []kvPair{},
		},
		{
			name:    "keys with overlapping prefixes",
			version: 1,
			prefix:  []byte("s/"),
			testData: []kvPair{
				{append([]byte("s/"), []byte{1, '1'}...), []byte("validator1"), 1, false},
				{append([]byte("s/"), []byte{1, '1'}...), []byte("validator1v2"), 2, true},
				{append([]byte("s/"), []byte{1, '2'}...), []byte("validator2"), 1, false},
				{append([]byte("s/"), []byte{1, '3', '3'}...), []byte("validator3"), 1, false},
				{append([]byte("s/"), []byte{1, '4', '0', '1'}...), []byte("validator3"), 1, false},
				{append([]byte("s/"), []byte{1, '5'}...), []byte("validator3"), 1, false},
				{append([]byte("s/"), []byte{2, '1'}...), []byte("account1"), 1, false},
				{append([]byte("s/"), []byte{2, '1'}...), []byte("account1"), 2, true},
			},
			expected: []kvPair{
				{[]byte{1, '1'}, []byte("validator1"), 1, false},
				{[]byte{1, '2'}, []byte("validator2"), 1, false},
				{[]byte{1, '3', '3'}, []byte("validator3"), 1, false},
				{[]byte{1, '4', '0', '1'}, []byte("validator3"), 1, false},
				{[]byte{1, '5'}, []byte("validator3"), 1, false},
				{[]byte{2, '1'}, []byte("account1"), 1, false},
			},
		},
		{
			name:        "archive iterator with prefix and tombstones",
			prefix:      []byte("test:"),
			version:     10,
			allVersions: true,
			testData: []kvPair{
				{[]byte("test:key1"), []byte("v1"), 1, false},
				{[]byte("test:key1"), []byte("v3"), 3, true},
				{[]byte("test:key1"), []byte("v5"), 5, false},
				{[]byte("test:key2"), []byte("v2"), 2, false},
				{[]byte("test:key2"), []byte("v4"), 4, true},
				{[]byte("test:key2"), []byte("v6"), 6, false},
				// should be filtered out
				{[]byte("other:key"), []byte("v7"), 7, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("v1"), 1, false},
				{[]byte("key1"), []byte("v3"), 3, true},
				{[]byte("key1"), []byte("v5"), 5, false},
				{[]byte("key2"), []byte("v2"), 2, false},
				{[]byte("key2"), []byte("v4"), 4, true},
				{[]byte("key2"), []byte("v6"), 6, false},
			},
		},
		{
			name:    "alternating live tombstone pattern",
			version: 20,
			testData: []kvPair{
				{[]byte("key1"), []byte("v1"), 1, false},
				{[]byte("key1"), []byte("v2"), 2, true},
				{[]byte("key1"), []byte("v3"), 3, false},
				{[]byte("key1"), []byte("v4"), 4, true},
				{[]byte("key1"), []byte("v5"), 5, false},
				{[]byte("key1"), []byte("v6"), 6, true},
				{[]byte("key1"), []byte("v7"), 7, false},
			},
			expected: []kvPair{
				{[]byte("key1"), []byte("v7"), 7, false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup test database
			db := newTestDb(t)
			defer db.Close()
			// set up test data
			for _, v := range tt.testData {
				// create versioned key and store in database
				vk := makeVersionedKey(v.key, v.version, v.tombstone)
				err := db.Set(vk, v.value, pebble.Sync)
				if err != nil {
					t.Fatalf("Failed to set up test data: %v", err)
				}
			}
			// setup versioned store
			vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), tt.version, false)
			require.NoError(t, err)
			// create iterator
			testIter, err := vs.NewIterator([]byte(tt.prefix), tt.reverse, tt.allVersions)
			require.NoError(t, err)
			iter := testIter.(*VersionedIterator)
			defer iter.Close()
			// validate the iterator
			i := 0
			for ; iter.Valid(); iter.Next() {
				if i >= len(tt.expected) {
					t.Fatalf("iterator returned more keys than expected")
				}
				assert.True(t, bytes.Equal(tt.expected[i].key, iter.Key()))
				assert.True(t, bytes.Equal(tt.expected[i].value, iter.Value()))
				i++
			}
			assert.Equal(t, len(tt.expected), i, "iterator returned fewer keys than expected")
			// check reverse iteration
			for {
				iter.Prev()
				if !iter.Valid() {
					break
				}
				i--
				assert.True(t, bytes.Equal(tt.expected[i].key, iter.Key()))
				assert.True(t, bytes.Equal(tt.expected[i].value, iter.Value()))
			}
			require.Equal(t, 0, i, "reverse iterator did not return to the start")
		})
	}
}

func FuzzKeyVersioning(f *testing.F) {
	// seed corpus
	tests := []struct {
		key       []byte
		version   uint64
		tombstone bool
	}{
		// seed input and testing format comes from TestKeyVersioning
		{
			key:       []byte("testkey"),
			version:   42,
			tombstone: false,
		},
		{
			key:       []byte{},
			version:   100,
			tombstone: false,
		},
		{
			key:       []byte{0x01, 0x02, 0x03, 0x04},
			version:   9999,
			tombstone: true,
		},
		{
			key:       []byte("maxversion"),
			version:   math.MaxUint64,
			tombstone: false,
		},
	}
	for _, test := range tests {
		f.Add(test.key, test.version, test.tombstone)
	}
	f.Fuzz(func(t *testing.T, key []byte, version uint64, tombstone bool) {
		// test round-trip: make versioned key and then extract the parts
		versionedKey := makeVersionedKey(key, version, tombstone)
		extractedKey, extractedVersion, extractedTombstone, err := getVersionedKey(versionedKey)
		// verify no errors occurred
		require.NoError(t, err)
		// check that the extracted key matches the original
		require.True(t, bytes.Equal(key, extractedKey))
		// check that the extracted version matches the original
		require.Equal(t, version, extractedVersion)
		// check that the extracted tombstone flag matches the original
		require.Equal(t, tombstone, extractedTombstone)
	})
}

func BenchmarkGet(b *testing.B) {
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	require.NoError(b, err)
	defer db.Close()
	vs, err := NewVersionedStore(db.NewSnapshot(), db.NewBatch(), 1, false)
	require.NoError(b, err)
	// amount of keys per version
	version0 := 500
	version1 := 500
	// pre populate the store with some data at version 0
	for i := range version0 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		err := db.Set(makeVersionedKey(key, 0, false), value, pebble.Sync)
		require.NoError(b, err)
	}
	// pre populate the store with some data at version 1
	for i := version0; i < version1; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		err := db.Set(makeVersionedKey(key, 1, false), value, pebble.Sync)
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

// newTestDb creates a new pebble in memory database
func newTestDb(t *testing.T) *pebble.DB {
	db, err := pebble.Open("", &pebble.Options{
		FS:                 vfs.NewMem(),
		FormatMajorVersion: pebble.FormatColumnarBlocks,
	})
	assert.NoError(t, err)
	return db
}
