package lib

import (
	"container/list"
	"github.com/canopy-network/canopy/lib/crypto"
	"slices"
	"sync"
	"time"
)

/* This file defines and implements a mempool that maintains an ordered list of 'valid, pending to be included' transactions in memory */

var _ Mempool = &FeeMempool{} // Mempool interface enforcement for FeeMempool implementation

// Mempool interface is a model for a pre-block, in-memory, Transaction store
type Mempool interface {
	Contains(hash string) bool                                       // whether the mempool has this transaction already (de-duplicated by hash)
	AddTransaction(tx []byte, fee uint64) (recheck bool, err ErrorI) // insert new unconfirmed transaction
	DeleteTransaction(tx []byte)                                     // delete unconfirmed transaction
	GetTransactions(maxBytes uint64) [][]byte                        // retrieve transactions from the highest fee to lowest

	Clear()              // reset the entire store
	TxCount() int        // number of Transactions in the pool
	TxsBytes() int       // collective number of bytes in the pool
	Iterator() IteratorI // loop through each transaction in the pool
}

// FeeMempool is a Mempool implementation that prioritizes transactions with the highest fees
type FeeMempool struct {
	l        sync.RWMutex        // for thread safety // TODO evaluate the need for this since the controller locks
	hashMap  map[string]struct{} // O(1) de-duplication
	pool     MempoolTxs          // the actual pool of transactions
	count    int                 // the number of Transactions in the pool
	txsBytes int                 // collective number of bytes in the pool
	config   MempoolConfig       // user configuration of the pool
}

// MempoolTx is a wrapper over Transaction bytes that maintains the fee associated with the bytes
type MempoolTx struct {
	Tx  []byte // transaction bytes
	Fee uint64 // fee associated with the transaction
}

// NewMempool() creates a new FeeMempool instance of a Mempool
func NewMempool(config MempoolConfig) Mempool {
	// if the config drop percentage is set to 0
	if config.DropPercentage == 0 {
		// set the drop percentage to the default mempool config
		config.DropPercentage = DefaultMempoolConfig().DropPercentage
	}
	// return the default mempool
	return &FeeMempool{
		l:       sync.RWMutex{},
		hashMap: make(map[string]struct{}),
		pool: MempoolTxs{
			count: 0,
			l:     list.New(),
			m:     make(map[string]*list.Element),
		},
		config: config,
	}
}

// AddTransaction() inserts a new unconfirmed Transaction to the Pool and returns if this addition
// requires a recheck of the Mempool due to dropping or re-ordering of the Transactions
func (f *FeeMempool) AddTransaction(tx []byte, fee uint64) (recheck bool, err ErrorI) {
	// lock the mempool for thread safety
	f.l.Lock()
	// when the function finishes unlock the mempool
	defer f.l.Unlock()
	// ensure the size of the Transaction doesn't exceed the individual limit
	txBytes := len(tx)
	// if the transaction bytes is larger than the max size
	if uint32(txBytes) > f.config.IndividualMaxTxSize {
		// exit with error
		return false, ErrMaxTxSize()
	}
	// create quick hash of the transaction for de-duplication;
	// note that hash may not equal Transaction Hash based on the implementation
	hash := crypto.HashString(tx)
	// check for a duplicate
	if _, alreadyFound := f.hashMap[hash]; alreadyFound {
		// exit with 'already found' error
		return false, ErrTxFoundInMempool(hash)
	}
	// insert the transaction into the pool
	recheck = f.pool.insert(MempoolTx{Tx: tx, Fee: fee})
	// insert into de-duplication hash map
	f.hashMap[hash] = struct{}{}
	// increment the count
	f.count++
	// update the number of bytes
	f.txsBytes += txBytes
	// assess if limits are exceeded - if so, drop from the bottom
	var dropped []MempoolTx
	// loop until the conditions are satisfied
	for uint32(f.count) > f.config.MaxTransactionCount || uint64(f.txsBytes) > f.config.MaxTotalBytes {
		// drop percentage is configurable
		dropped = f.pool.drop(f.config.DropPercentage)
		// for each dropped transaction
		for _, d := range dropped {
			// decrement count
			f.count--
			// subtract the txsBytes
			f.txsBytes -= len(d.Tx)
			// delete from teh de-duplication hash map
			delete(f.hashMap, crypto.HashString(d.Tx))
		}
	}
	// if any are dropped or re-order happened
	return len(dropped) != 0 || recheck, nil
}

// GetTransactions() returns a list of the Transactions from the pool up to 'max collective Transaction bytes'
func (f *FeeMempool) GetTransactions(maxBytes uint64) (txs [][]byte) {
	// lock for thread safety
	f.l.RLock()
	// unlock when the function completes
	defer f.l.RUnlock()
	// create a variable to track the total transaction byte count
	totalBytes := uint64(0)
	// for each transaction in the pool
	for e := f.pool.l.Front(); e != nil; e = e.Next() {
		// cast the item
		item := e.Value.(MempoolTx)
		// get the size of the transaction in bytes
		txSize := len(item.Tx)
		// add to the total bytes
		totalBytes += uint64(txSize)
		// check to see if the addition of this transaction
		// exceeds the maxBytes limit
		if totalBytes > maxBytes {
			// exit without adding the tx
			return
		}
		// add the tx to the list and increment totalTxs
		txs = append(txs, item.Tx)
	}
	// exit
	return
}

// Contains() checks if a transaction with the given hash exists in the mempool
func (f *FeeMempool) Contains(hash string) (contains bool) {
	// lock for thread safety
	f.l.RLock()
	// unlock when the function completes
	defer f.l.RUnlock()
	// check if the hash map contains the transaction hash
	_, contains = f.hashMap[hash]
	// exit
	return
}

// DeleteTransaction() removes the specified transaction from the mempool
func (f *FeeMempool) DeleteTransaction(tx []byte) {
	// lock for thread safety
	f.l.Lock()
	// unlock when the function completes
	defer f.l.Unlock()
	// delete the transaction from the pool
	deleted := f.pool.delete(tx)
	// if the attempted deleted tx is nil
	if deleted.Tx == nil {
		// exit
		return
	}
	// delete from the hash map
	delete(f.hashMap, crypto.HashString(deleted.Tx))
	// reduce the mempool count
	f.count--
	// subtract the from the tx bytes count
	f.txsBytes -= len(deleted.Tx)
}

// Clear() empties the mempool and resets its state
func (f *FeeMempool) Clear() {
	// lock the mempool for thread safety
	f.l.Lock()
	// unlock when the function completes
	defer f.l.Unlock()
	// reset the memory pool of transactions
	f.pool = MempoolTxs{
		count: 0,
		l:     list.New(),
		m:     make(map[string]*list.Element),
	}
	// reset the hash map
	f.hashMap = make(map[string]struct{})
	// reset the count
	f.count = 0
	// reset the bytes count
	f.txsBytes = 0
}

// TxCount() returns the current number of transactions in the mempool
func (f *FeeMempool) TxCount() int {
	// lock for thread safety
	f.l.RLock()
	// unlock when function completes
	defer f.l.RUnlock()
	// return the count
	return f.count
}

// TxsBytes() returns the total size in bytes of all transactions in the mempool
func (f *FeeMempool) TxsBytes() int {
	// lock for thread safety
	f.l.RLock()
	// unlock when function completes
	defer f.l.RUnlock()
	// return the number of bytes in the memory pool
	return f.txsBytes
}

// Iterator() creates a new iterator for traversing the transactions in the mempool
func (f *FeeMempool) Iterator() IteratorI {
	// exit with a new mempool iterator
	return NewMempoolIterator(f.pool)
}

var _ IteratorI = &mempoolIterator{} // enforce

// mempoolIterator implements IteratorI using the list of Transactions the index and if the position is valid
type mempoolIterator struct {
	pool    *MempoolTxs   // reference to list of Transactions
	current *list.Element // the current element
}

// NewMempoolIterator() initializes a new iterator for the mempool transactions
func NewMempoolIterator(p MempoolTxs) *mempoolIterator {
	pool := p.copy() // copy the pool for safe iteration during a parallel
	current := pool.l.Front()
	return &mempoolIterator{pool: pool, current: current}
}

// Valid() checks if the iterator is positioned on a valid element
func (m *mempoolIterator) Valid() bool { return m.current != nil }

// Next() advances the iterator to the next transaction in the pool
func (m *mempoolIterator) Next() { m.current = m.current.Next() }

// Key() returns the transaction at the current iterator position
func (m *mempoolIterator) Key() (key []byte) { return m.current.Value.(MempoolTx).Tx }

// Value() returns same as key
func (m *mempoolIterator) Value() (value []byte) { return m.Key() }

// Error() always returns nil, as no errors are tracked by this iterator
func (m *mempoolIterator) Error() error { return nil }

// Close() is a no-op in this iterator, as no resources need to be released
func (m *mempoolIterator) Close() {}

// MempoolTxs is a list of MempoolTxs with a count
type MempoolTxs struct {
	count int
	l     *list.List               // Doubly linked list
	m     map[string]*list.Element // txHash -> list element
}

// insert() inserts a new tx into the list sorted by the highest fee to the lowest fee
func (t *MempoolTxs) insert(tx MempoolTx) (recheck bool) {
	// initialization sanity check
	if t.l == nil {
		t.l = list.New()
		t.m = make(map[string]*list.Element)
	}
	// increment count
	t.count++
	// get a key for the tx
	k := string(tx.Tx)
	// start from the back and scan backwards
	for e := t.l.Back(); e != nil; e = e.Prev() {
		if e.Value.(MempoolTx).Fee >= tx.Fee {
			// insert after this element
			t.m[k] = t.l.InsertAfter(tx, e)
			return t.m[k] != t.l.Back() && t.count != 1
		}
	}
	// if we got here, tx has the highest fee: insert at front
	t.m[k] = t.l.PushFront(tx)
	// return if recheck required
	return t.m[k] != t.l.Back() && t.count != 1
}

func (t *MempoolTxs) delete(tx []byte) (deleted MempoolTx) {
	// check if exists
	elem, exists := t.m[string(tx)]
	if !exists {
		return
	}
	// delete the element from the list
	t.l.Remove(elem)
	// remove from map
	delete(t.m, string(tx))
	// decrement the count
	t.count--
	// return the element
	return elem.Value.(MempoolTx)
}

// drop() removes the bottom (the lowest fee) X percent of Transactions
func (t *MempoolTxs) drop(percent int) (dropped []MempoolTx) {
	if t.count == 0 || percent <= 0 {
		return nil
	}
	// calculate the percent using integer division
	numDrop := (t.count*percent)/100 + 1
	// decrement count by number evicted
	t.count -= numDrop
	// start at the back
	current := t.l.Back()
	// reverse iterate 'num to drop'
	for i := 0; i < numDrop && current != nil; i++ {
		// maintain a pointer to the next item (previous)
		prev := current.Prev()
		// cast the transaction
		tx := current.Value.(MempoolTx)
		// remove from the list
		t.l.Remove(current)
		// delete from the map
		delete(t.m, string(tx.Tx))
		// decrement the count
		t.count--
		// save dropped
		dropped = append(dropped, tx)
		// set previous to current
		current = prev
	}
	return
}

// copy() returns a shallow copy of the MempoolTxs
func (t *MempoolTxs) copy() *MempoolTxs {
	newList := list.New()
	newMap := make(map[string]*list.Element)
	// iterate through all the items
	for e := t.l.Front(); e != nil; e = e.Next() {
		// cast it
		tx := e.Value.(MempoolTx)
		// push it to the new list
		elem := newList.PushBack(tx)
		// add to the map
		newMap[string(tx.Tx)] = elem
	}
	// exit with new structure
	return &MempoolTxs{
		l:     newList,
		m:     newMap,
		count: t.count,
	}
}

// FAILED TX CACHE CODE BELOW

// FailedTxCache is a cache of failed transactions that is used to inform the user of the failure
type FailedTxCache struct {
	cache                  map[string]*FailedTx // map tx hashes to errors
	disallowedMessageTypes []string             // reject all transactions that are of these types
	l                      sync.Mutex           // a lock for thread safety
}

// NewFailedTxCache returns a new FailedTxCache
func NewFailedTxCache(disallowedMessageTypes ...string) (cache *FailedTxCache) {
	// initialize the failed transactions cache
	cache = &FailedTxCache{
		cache:                  map[string]*FailedTx{},
		l:                      sync.Mutex{},
		disallowedMessageTypes: disallowedMessageTypes,
	}
	// start the cleaning service
	go cache.StartCleanService()
	// exit with the cache
	return
}

// Add() adds a failed transaction with its error to the cache
func (f *FailedTxCache) Add(txBytes []byte, hash string, txErr error) (added bool) {
	// lock for thread safety
	f.l.Lock()
	// unlock when the function completes
	defer f.l.Unlock()
	// create a new transaction object reference to ensure a non nil result
	tx := new(Transaction)
	// populate the new object reference using the transaction bytes
	if err := Unmarshal(txBytes, tx); err != nil {
		// exit with 'not added'
		return
	}
	// if the message is on the 'disallowed' list
	if slices.Contains(f.disallowedMessageTypes, tx.MessageType) {
		// exit with 'not added'
		return
	}
	// if the signature is empty
	if tx.Signature == nil {
		// exit with 'not added'
		return
	}
	// get the public key object from the bytes of the signature
	pubKey, err := crypto.NewPublicKeyFromBytes(tx.Signature.PublicKey)
	// if an error occurred during the conversion
	if err != nil {
		// exit with 'not added'
		return
	}
	// add a new 'failed tx' type to the cache
	f.cache[hash] = &FailedTx{
		Transaction: tx,
		Hash:        hash,
		Address:     pubKey.Address().String(),
		Error:       txErr,
		timestamp:   time.Now(),
	}
	// exit with 'added'
	return true
}

// Get() returns the failed transaction associated with its hash
func (f *FailedTxCache) Get(txHash string) (failedTx *FailedTx, found bool) {
	// lock for thread safety
	f.l.Lock()
	// unlock when the function completes
	defer f.l.Unlock()
	// get the failed tx from the cache
	failedTx, found = f.cache[txHash]
	// if not found in the cache
	if !found {
		// exit with not found
		return
	}
	// exit
	return
}

// GetFailedForAddress() returns all the failed transactions in the cache for a given address
func (f *FailedTxCache) GetFailedForAddress(address string) (failedTxs []*FailedTx) {
	// lock for thread safety
	f.l.Lock()
	// unlock when the function completes
	defer f.l.Unlock()
	// for each failed transaction in the cache
	for _, failed := range f.cache {
		// if the address matches
		if failed.Address == address {
			// add to the list
			failedTxs = append(failedTxs, failed)
		}
	}
	// exit
	return
}

// Remove() removes a transaction hash from the cache
func (f *FailedTxCache) Remove(txHashes ...string) {
	// lock for thread safety
	f.l.Lock()
	// unlock when function completes
	defer f.l.Unlock()
	// for each transaction hash
	for _, hash := range txHashes {
		// remove it from the memory cache
		delete(f.cache, hash)
	}
}

// StartCleanService() periodically removes transactions from the cache that are older than 5 minutes
func (f *FailedTxCache) StartCleanService() {
	// every minute until app stops
	for range time.Tick(time.Minute) {
		// wrap in a function to use 'defer'
		func() {
			// lock for thread safety
			f.l.Lock()
			// unlock when iteration completes
			defer f.l.Unlock()
			// for each in the cache
			for hash, tx := range f.cache {
				// if the 'time since' is greater than 5 minutes
				if time.Since(tx.timestamp) >= 5*time.Minute {
					// remove it from the cache
					delete(f.cache, hash)
				}
			}
		}()
	}
}

// FailedTx contains a failed transaction and its error
type FailedTx struct {
	Transaction *Transaction `json:"transaction,omitempty"` // the transaction object that failed
	Hash        string       `json:"txHash,omitempty"`      // the hash of the transaction object
	Address     string       `json:"address,omitempty"`     // the address that sent the transaction
	Error       error        `json:"error,omitempty"`       // the error that occurred
	timestamp   time.Time    // the time when the failure was recorded
}

type FailedTxs []*FailedTx // a list of failed transactions

// ensure failed txs implements the pageable interface
var _ Pageable = &FailedTxs{}

// implement pageable interface
func (t *FailedTxs) Len() int      { return len(*t) }
func (t *FailedTxs) New() Pageable { return &FailedTxs{} }
