package vm

import (
	"fmt"
	"log"
	"sync"

	"github.com/canopy-network/canopy/lib"
)

// InMemoryCache provides a simple in-memory cache for WASM bytecode
type InMemoryCache struct {
	data    map[uint64][]byte
	maxSize uint32
	mu      sync.RWMutex
}

// NewInMemoryCache creates a new in-memory cache with the specified maximum size
func NewInMemoryCache(maxSize uint32) (*InMemoryCache, lib.ErrorI) {
	log.Printf("[DEBUG] NewInMemoryCache called with maxSize: %d", maxSize)
	if maxSize == 0 {
		log.Printf("[DEBUG] NewInMemoryCache error: maxSize is 0")
		return nil, ErrCacheMaxSize()
	}

	cache := &InMemoryCache{
		data:    make(map[uint64][]byte),
		maxSize: maxSize,
	}
	log.Printf("[DEBUG] NewInMemoryCache created successfully")
	return cache, nil
}

// GetCode retrieves WASM bytecode from the cache
func (c *InMemoryCache) GetCode(codeID uint64) ([]byte, lib.ErrorI) {
	log.Printf("[DEBUG] InMemoryCache.GetCode called with codeID: %d", codeID)
	c.mu.RLock()
	defer c.mu.RUnlock()

	code, exists := c.data[codeID]
	if !exists {
		log.Printf("[DEBUG] InMemoryCache.GetCode: code not found for codeID: %d", codeID)
		return nil, ErrCacheNotFound()
	}

	// Return a copy to prevent modification
	result := make([]byte, len(code))
	copy(result, code)
	log.Printf("[DEBUG] InMemoryCache.GetCode: returning %d bytes for codeID: %d", len(result), codeID)
	return result, nil
}

// SetCode stores WASM bytecode in the cache
func (c *InMemoryCache) SetCode(codeID uint64, code []byte) lib.ErrorI {
	log.Printf("[DEBUG] InMemoryCache.SetCode called with codeID: %d, code size: %d bytes", codeID, len(code))
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict old entries
	if uint32(len(c.data)) >= c.maxSize {
		log.Printf("[DEBUG] InMemoryCache.SetCode: cache full (size: %d, max: %d), evicting oldest", len(c.data), c.maxSize)
		c.evictOldest()
	}

	// Store a copy to prevent external modification
	codeCopy := make([]byte, len(code))
	copy(codeCopy, code)
	c.data[codeID] = codeCopy

	log.Printf("[DEBUG] InMemoryCache.SetCode: successfully stored codeID: %d, cache size now: %d", codeID, len(c.data))
	return nil
}

// HasCode checks if WASM bytecode exists in the cache
func (c *InMemoryCache) HasCode(codeID uint64) bool {
	log.Printf("[DEBUG] InMemoryCache.HasCode called with codeID: %d", codeID)
	c.mu.RLock()
	defer c.mu.RUnlock()

	d, exists := c.data[codeID]
	log.Printf("[DEBUG] InMemoryCache.HasCode: codeID %d exists: %t %d bytes", codeID, exists, len(d))
	return exists
}

// RemoveCode removes WASM bytecode from the cache
func (c *InMemoryCache) RemoveCode(codeID uint64) lib.ErrorI {
	log.Printf("[DEBUG] InMemoryCache.RemoveCode called with codeID: %d", codeID)
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, codeID)
	log.Printf("[DEBUG] InMemoryCache.RemoveCode: removed codeID: %d, cache size now: %d", codeID, len(c.data))
	return nil
}

// Size returns the current number of cached entries
func (c *InMemoryCache) Size() uint32 {
	log.Printf("[DEBUG] InMemoryCache.Size called")
	c.mu.RLock()
	defer c.mu.RUnlock()

	size := uint32(len(c.data))
	log.Printf("[DEBUG] InMemoryCache.Size: returning %d", size)
	return size
}

// Clear removes all entries from the cache
func (c *InMemoryCache) Clear() {
	log.Printf("[DEBUG] InMemoryCache.Clear called")
	c.mu.Lock()
	defer c.mu.Unlock()

	oldSize := len(c.data)
	c.data = make(map[uint64][]byte)
	log.Printf("[DEBUG] InMemoryCache.Clear: cleared %d entries", oldSize)
}

// evictOldest removes one entry from the cache (simple eviction strategy)
// In a production system, you might want a more sophisticated LRU strategy
func (c *InMemoryCache) evictOldest() {
	log.Printf("[DEBUG] InMemoryCache.evictOldest called")
	if len(c.data) == 0 {
		log.Printf("[DEBUG] InMemoryCache.evictOldest: cache is empty, nothing to evict")
		return
	}

	// Remove an arbitrary entry (maps iterate in random order in Go)
	for codeID := range c.data {
		delete(c.data, codeID)
		log.Printf("[DEBUG] InMemoryCache.evictOldest: evicted codeID: %d, cache size now: %d", codeID, len(c.data))
		break
	}
}

// PersistentCache provides a disk-based cache for WASM bytecode
// This would integrate with the existing BadgerDB storage system
type PersistentCache struct {
	store     lib.RWStoreI
	keyPrefix string
}

// NewPersistentCache creates a new persistent cache using the provided store
func NewPersistentCache(store lib.RWStoreI, keyPrefix string) *PersistentCache {
	log.Printf("[DEBUG] NewPersistentCache called with keyPrefix: %s", keyPrefix)
	cache := &PersistentCache{
		store:     store,
		keyPrefix: keyPrefix,
	}
	log.Printf("[DEBUG] NewPersistentCache created successfully")
	return cache
}

// GetCode retrieves WASM bytecode from persistent storage
func (c *PersistentCache) GetCode(codeID uint64) ([]byte, lib.ErrorI) {
	log.Printf("[DEBUG] PersistentCache.GetCode called with codeID: %d", codeID)
	key := c.makeKey(codeID)
	log.Printf("[DEBUG] PersistentCache.GetCode: using key: %s", string(key))
	code, err := c.store.Get(key)
	if err != nil {
		log.Printf("[DEBUG] PersistentCache.GetCode: store.Get error: %v", err)
		return nil, ErrCacheStoreGet(err)
	}
	if len(code) == 0 {
		log.Printf("[DEBUG] PersistentCache.GetCode: code not found for codeID: %d", codeID)
		return nil, ErrCacheNotFound()
	}
	log.Printf("[DEBUG] PersistentCache.GetCode: returning %d bytes for codeID: %d", len(code), codeID)
	return code, nil
}

// SetCode stores WASM bytecode in persistent storage
func (c *PersistentCache) SetCode(codeID uint64, code []byte) lib.ErrorI {
	log.Printf("[DEBUG] PersistentCache.SetCode called with codeID: %d, code size: %d bytes", codeID, len(code))
	key := c.makeKey(codeID)
	log.Printf("[DEBUG] PersistentCache.SetCode: using key: %s", string(key))
	if err := c.store.Set(key, code); err != nil {
		log.Printf("[DEBUG] PersistentCache.SetCode: store.Set error: %v", err)
		return ErrCacheStoreSet(err)
	}
	log.Printf("[DEBUG] PersistentCache.SetCode: successfully stored codeID: %d", codeID)
	return nil
}

// HasCode checks if WASM bytecode exists in persistent storage
func (c *PersistentCache) HasCode(codeID uint64) bool {
	log.Printf("[DEBUG] PersistentCache.HasCode called with codeID: %d", codeID)
	key := c.makeKey(codeID)
	log.Printf("[DEBUG] PersistentCache.HasCode: using key: %s", string(key))
	_, err := c.store.Get(key)
	exists := err == nil
	log.Printf("[DEBUG] PersistentCache.HasCode: codeID %d exists: %t", codeID, exists)
	return exists
}

// RemoveCode removes WASM bytecode from persistent storage
func (c *PersistentCache) RemoveCode(codeID uint64) lib.ErrorI {
	log.Printf("[DEBUG] PersistentCache.RemoveCode called with codeID: %d", codeID)
	key := c.makeKey(codeID)
	log.Printf("[DEBUG] PersistentCache.RemoveCode: using key: %s", string(key))
	if err := c.store.Delete(key); err != nil {
		log.Printf("[DEBUG] PersistentCache.RemoveCode: store.Delete error: %v", err)
		return ErrCacheStoreSet(err)
	}
	log.Printf("[DEBUG] PersistentCache.RemoveCode: successfully removed codeID: %d", codeID)
	return nil
}

// makeKey creates a storage key for the given code ID
func (c *PersistentCache) makeKey(codeID uint64) []byte {
	log.Printf("[DEBUG] PersistentCache.makeKey called with codeID: %d", codeID)
	key := fmt.Sprintf("%s%d", c.keyPrefix, codeID)
	log.Printf("[DEBUG] PersistentCache.makeKey: generated key: %s", key)
	return []byte(key)
}

// HybridCache combines in-memory and persistent caching
type HybridCache struct {
	memory     *InMemoryCache
	persistent *PersistentCache
}

// NewHybridCache creates a cache that uses both memory and persistent storage
func NewHybridCache(memoryMaxSize uint32, store lib.RWStoreI, keyPrefix string) (*HybridCache, lib.ErrorI) {
	log.Printf("[DEBUG] NewHybridCache called with memoryMaxSize: %d, keyPrefix: %s", memoryMaxSize, keyPrefix)
	memory, err := NewInMemoryCache(memoryMaxSize)
	if err != nil {
		log.Printf("[DEBUG] NewHybridCache: NewInMemoryCache error: %v", err)
		return nil, err
	}

	persistent := NewPersistentCache(store, keyPrefix)

	cache := &HybridCache{
		memory:     memory,
		persistent: persistent,
	}
	log.Printf("[DEBUG] NewHybridCache created successfully")
	return cache, nil
}

// GetCode retrieves code from memory cache first, then persistent cache
func (c *HybridCache) GetCode(codeID uint64) ([]byte, lib.ErrorI) {
	log.Printf("[DEBUG] HybridCache.GetCode called with codeID: %d", codeID)
	// Try memory cache first
	if code, err := c.memory.GetCode(codeID); err == nil {
		log.Printf("[DEBUG] HybridCache.GetCode: found in memory cache for codeID: %d", codeID)
		return code, nil
	}

	log.Printf("[DEBUG] HybridCache.GetCode: not in memory cache, trying persistent cache for codeID: %d", codeID)
	// Try persistent cache
	code, err := c.persistent.GetCode(codeID)
	if err != nil {
		log.Printf("[DEBUG] HybridCache.GetCode: not found in persistent cache for codeID: %d, error: %v", codeID, err)
		return nil, err
	}

	// Cache in memory for future access
	log.Printf("[DEBUG] HybridCache.GetCode: found in persistent cache, caching in memory for codeID: %d", codeID)
	c.memory.SetCode(codeID, code)
	return code, nil
}

// SetCode stores code in both memory and persistent cache
func (c *HybridCache) SetCode(codeID uint64, code []byte) lib.ErrorI {
	log.Printf("[DEBUG] HybridCache.SetCode called with codeID: %d, code size: %d bytes", codeID, len(code))
	// Store in persistent cache first
	if err := c.persistent.SetCode(codeID, code); err != nil {
		log.Printf("[DEBUG] HybridCache.SetCode: persistent cache error: %v", err)
		return err
	}

	// Then cache in memory
	if err := c.memory.SetCode(codeID, code); err != nil {
		log.Printf("[DEBUG] HybridCache.SetCode: memory cache error: %v", err)
		return err
	}

	log.Printf("[DEBUG] HybridCache.SetCode: successfully stored codeID: %d in both caches", codeID)
	return nil
}

// HasCode checks both memory and persistent cache
func (c *HybridCache) HasCode(codeID uint64) bool {
	log.Printf("[DEBUG] HybridCache.HasCode called with codeID: %d", codeID)
	memoryHas := c.memory.HasCode(codeID)
	log.Printf("[DEBUG] HybridCache.HasCode: memory cache has codeID %d: %t", codeID, memoryHas)
	if memoryHas {
		return true
	}
	persistentHas := c.persistent.HasCode(codeID)
	log.Printf("[DEBUG] HybridCache.HasCode: persistent cache has codeID %d: %t", codeID, persistentHas)
	return persistentHas
}

// RemoveCode removes code from both caches
func (c *HybridCache) RemoveCode(codeID uint64) lib.ErrorI {
	log.Printf("[DEBUG] HybridCache.RemoveCode called with codeID: %d", codeID)
	// Remove from memory (ignore errors)
	if err := c.memory.RemoveCode(codeID); err != nil {
		log.Printf("[DEBUG] HybridCache.RemoveCode: memory cache error (ignored): %v", err)
	}

	// Remove from persistent storage
	if err := c.persistent.RemoveCode(codeID); err != nil {
		log.Printf("[DEBUG] HybridCache.RemoveCode: persistent cache error: %v", err)
		return err
	}

	log.Printf("[DEBUG] HybridCache.RemoveCode: successfully removed codeID: %d from both caches", codeID)
	return nil
}
