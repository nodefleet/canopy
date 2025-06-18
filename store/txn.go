package store

import (
	"bytes"
	"strings"

	"github.com/canopy-network/canopy/lib"
	"github.com/dgraph-io/badger/v4"

	"maps"

	"github.com/google/btree"
)

// TxReaderI() defines the interface to read a TxnTransaction
// Txn implements this itself to allow for nested transactions
type TxnReaderI interface {
	Get(key []byte) ([]byte, lib.ErrorI)
	NewIterator(prefix []byte, reverse bool, allVersions bool) (lib.IteratorI, lib.ErrorI)
	Discard()
}

// TxnWriterI() defines the interface to write a TxnTransaction
// Txn implements this itself to allow for nested transactions
type TxnWriterI interface {
	Set(key, value []byte) lib.ErrorI
	Delete(key []byte) lib.ErrorI
	Commit() lib.ErrorI
	Cancel()
}

// enforce the necessary interfaces over Txn
var _ lib.RWStoreI = &Txn{}
var _ TxnReaderI = &Txn{}
var _ TxnWriterI = &Txn{}

/*
	Txn acts like a database transaction
	It saves set/del operations in memory and allows the caller to Write() to the parent or Discard()
	When read from, it merges with the parent as if Write() had already been called

	Txn abstraction is necessary due to the inability of BadgerDB to have nested transactions.
	Txns allow an easy rollback of write operations within a single Transaction object, which is necessary
	for ephemeral states and testing the validity of a proposal block / transactions.

	CONTRACT:
	- only safe when writing to another memory store like a badger.Txn() as Write() is not atomic.
	- not thread safe (can't use 1 txn across multiple threads)
	- nil values are supported; deleted values are also set to nil
	- keys must be smaller than 128 bytes
	- Nested txns are supported, but iteration becomes increasingly inefficient
*/

type Txn struct {
	reader TxnReaderI  // memory store to Read() from
	writer TxnWriterI  // memory store to Write() to
	prefix []byte      // prefix for keys in this txn
	logger lib.LoggerI // logger for this txn
	sort   bool        // whether to sort the keys in the cache; used for iteration
	cache  txn
}

type cache struct {
	Value string
}

// txn internal structure maintains the write operations sorted lexicographically by keys
type txn struct {
	ops map[string]valueOp // [string(key)] -> set/del operations saved in memory
	// sorted   []string           // ops keys sorted lexicographically; needed for iteration
	sorted    *btree.BTreeG[*CacheItem] // sorted btree of keys for fast iteration
	sortedLen int                       // len(sorted)
}

// txn() returns a copy of the current transaction cache
func (t txn) copy() txn {
	ops := make(map[string]valueOp, t.sortedLen)
	maps.Copy(ops, t.ops)
	return txn{
		ops:       ops,
		sorted:    t.sorted.Clone(),
		sortedLen: t.sortedLen,
	}
}

// op is the type of operation to be performed on the key
type op uint8

const (
	opDelete    op = iota // delete the key
	opSet                 // set the key
	opTombstone           // tombstone the key (false delete)
	opEntry               // custom badger entry
)

// valueOp has the value portion of the operation and the corresponding operation to perform
type valueOp struct {
	key   []byte // the key of the key value pair
	value []byte // value of key value pair
	op    op     // is operation delete
}

// NewTxn() creates a new instance of Txn with the specified reader and writer
func NewTxn(reader TxnReaderI, writer TxnWriterI, prefix []byte, sort bool, logger lib.LoggerI) *Txn {
	return &Txn{
		reader: reader,
		writer: writer,
		prefix: prefix,
		logger: logger,
		sort:   sort,
		cache: txn{
			ops: make(map[string]valueOp),
			sorted: btree.NewG(32, func(a, b *CacheItem) bool {
				return a.Less(b)
			}), // need to benchmark this value
		},
	}
}

// Get() retrieves the value for a given key from either the cache operations or the reader store
func (t *Txn) Get(key []byte) ([]byte, lib.ErrorI) {
	// append the prefix to the key
	prefixedKey := lib.Append(t.prefix, key)
	// first retrieve from the in-memory cache
	if v, found := t.cache.ops[string(prefixedKey)]; found {
		return v.value, nil
	}
	// if not found, retrieve from the parent reader
	return t.reader.Get(prefixedKey)
}

// Set() adds or updates the value for a key in the cache operations
func (t *Txn) Set(key, value []byte) lib.ErrorI {
	t.update(lib.Append(t.prefix, key), value, opSet)
	return nil
}

// Delete() marks a key for deletion in the cache operations
func (t *Txn) Delete(key []byte) lib.ErrorI {
	t.update(lib.Append(t.prefix, key), nil, opDelete)
	return nil
}

// Tombstone() removes the key-value pair from the BadgerDB transaction but prevents it from being garbage collected
func (t *Txn) Tombstone(key []byte) lib.ErrorI {
	t.update(lib.Append(t.prefix, key), nil, opTombstone)
	return nil
}

// update() modifies or adds an operation for a key in the cache operations and maintains the
// lexicographical order.
// NOTE: update() won't modify the key itself, any key prefixing must be done before calling this
func (t *Txn) update(key []byte, v []byte, opAction op) {
	k := string(key)
	if t.sort {
		if _, found := t.cache.ops[k]; !found && t.sort {
			t.addToSorted(string(key))
		}
	}
	t.cache.ops[k] = valueOp{key: key, value: v, op: opAction}
}

// addToSorted() inserts a key into the sorted list of operations maintaining lexicographical order
func (t *Txn) addToSorted(key string) {
	t.cache.sortedLen++
	t.cache.sorted.ReplaceOrInsert(&CacheItem{Key: key, Exists: true})
}

// Iterator() returns a new iterator for merged iteration of both the in-memory operations and parent store with the given prefix
func (t *Txn) Iterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	it, err := t.reader.NewIterator(lib.Append(t.prefix, prefix), false, false)
	if err != nil {
		return nil, err
	}
	return newTxnIterator(it, t.cache.copy(), t.prefix, prefix, false), nil
}

// RevIterator() returns a new reverse iterator for merged iteration of both the in-memory operations and parent store with the given prefix
func (t *Txn) RevIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	it, err := t.reader.NewIterator(lib.Append(t.prefix, prefix), true, false)
	if err != nil {
		return nil, err
	}
	return newTxnIterator(it, t.cache.copy(), t.prefix, prefix, true), nil
}

// ArchiveIterator() creates a new iterator for all versions under the given prefix in the BadgerDB transaction
func (t *Txn) ArchiveIterator(prefix []byte) (lib.IteratorI, lib.ErrorI) {
	return t.reader.NewIterator(prefix, false, true)
}

// Discard() clears all in-memory operations and resets the sorted key list
func (t *Txn) Discard() {
	t.cache.sorted.Clear(false)
	t.cache.ops, t.cache.sortedLen = make(map[string]valueOp), 0
}

// Cancel() cancels the current transaction. Any new writes won't be committed
func (t *Txn) Cancel() {
	t.writer.Cancel()
}

// Commit() flushes the in-memory operations to the batch writer and clears in-memory changes
func (t *Txn) Commit() lib.ErrorI {
	for _, v := range t.cache.ops {
		sk := v.key
		switch v.op {
		case opSet:
			if err := t.writer.Set(sk, v.value); err != nil {
				return ErrStoreSet(err)
			}
		case opDelete:
			if err := t.writer.Delete(sk); err != nil {
				return ErrStoreDelete(err)
			}
		}
	}
	t.Discard() // clear the in-memory operations after writing

	return nil
}

func (t *Txn) NewIterator(prefix []byte, reverse bool, allVersions bool) (lib.IteratorI, lib.ErrorI) {
	// Combine the current in-memory cache and parent reader (recursively)
	combinedParentIterator, err := t.reader.NewIterator(lib.Append(t.prefix, prefix), reverse, allVersions)
	if err != nil {
		return nil, ErrStoreIterator(err)
	}

	// Create a merged iterator for the parent and in-memory cache
	return newTxnIterator(combinedParentIterator, t.cache, t.prefix, prefix, reverse), nil
}

// Close() cancels the current transaction. Any new writes will result in an error and a new
// WriteBatch() must be created to write new entries.
func (t *Txn) Close() {
	t.reader.Discard()
	t.writer.Cancel()
}

// TXN ITERATOR CODE BELOW

// enforce the Iterator interface
var _ lib.IteratorI = &TxnIterator{}

// TxnIterator is a reversible, merged iterator of the parent and the in-memory operations
type TxnIterator struct {
	parent lib.IteratorI
	tree   *BTreeIterator
	txn
	hasNext      bool
	prefix       string
	parentPrefix string
	index        int
	reverse      bool
	invalid      bool
	useTxn       bool
}

// newTxnIterator() initializes a new merged iterator for traversing both the in-memory operations and parent store
func newTxnIterator(parent lib.IteratorI, t txn, parentPrefix, prefix []byte, reverse bool) *TxnIterator {
	tree := NewBTreeIterator(t.sorted.Clone(),
		&CacheItem{
			Key: string(lib.Append(parentPrefix, prefix)),
		},
		reverse)

	return (&TxnIterator{
		parent:       parent,
		tree:         tree,
		txn:          t,
		parentPrefix: string(parentPrefix),
		prefix:       string(prefix),
		reverse:      reverse,
	}).First()
}

// First() positions the iterator at the first valid entry based on the traversal direction
func (ti *TxnIterator) First() *TxnIterator {
	if ti.reverse {
		return ti.revSeek() // seek to the end
	}
	return ti.seek() // seek to the beginning
}

// Close() closes the merged iterator
func (ti *TxnIterator) Close() { ti.parent.Close() }

// Next() advances the iterator to the next entry, choosing between in-memory and parent store entries
func (ti *TxnIterator) Next() {
	// if parent is not usable any more then txn.Next()
	// if txn is not usable any more then parent.Next()
	if !ti.parent.Valid() {
		ti.txnNext()
		return
	}
	if ti.txnInvalid() {
		ti.parent.Next()
		return
	}
	// compare the keys of the in memory option and the parent option
	switch ti.compare(ti.txnKey(), ti.parent.Key()) {
	case 1: // use parent
		ti.parent.Next()
	case 0: // use both
		ti.parent.Next()
		ti.txnNext()
	case -1: // use txn
		ti.txnNext()
	}
}

// Key() returns the current key from either the in-memory operations or the parent store
func (ti *TxnIterator) Key() []byte {
	if ti.useTxn {
		return bytes.TrimPrefix(ti.txnKey(), []byte(ti.parentPrefix))
	}
	return bytes.TrimPrefix(ti.parent.Key(), []byte(ti.parentPrefix))
}

// Value() returns the current value from either the in-memory operations or the parent store
func (ti *TxnIterator) Value() []byte {
	if ti.useTxn {
		return ti.txnValue().value
	}
	return ti.parent.Value()
}

// Valid() checks if the current position of the iterator is valid, considering both the parent and in-memory entries
func (ti *TxnIterator) Valid() bool {
	for {
		if !ti.parent.Valid() {
			// only using cache; call txn.next until invalid or !deleted
			ti.txnFastForward()
			ti.useTxn = true
			break
		}
		if ti.txnInvalid() {
			// parent is valid; txn is not
			ti.useTxn = false
			break
		}
		// both are valid; key comparison matters
		cKey, pKey := ti.txnKey(), ti.parent.Key()
		switch ti.compare(cKey, pKey) {
		case 1: // use parent
			ti.useTxn = false
		case 0: // when equal txn shadows parent
			if ti.txnValue().op == opDelete || ti.txnValue().op == opTombstone {
				ti.parent.Next()
				ti.txnNext()
				continue
			}
			ti.useTxn = true
		case -1: // use txn
			if ti.txnValue().op == opDelete || ti.txnValue().op == opTombstone {
				ti.txnNext()
				continue
			}
			ti.useTxn = true
		}
		break
	}
	return !ti.txnInvalid() || ti.parent.Valid()
}

// txnFastForward() skips over deleted entries in the in-memory operations
// return when invalid or !deleted
func (ti *TxnIterator) txnFastForward() {
	for {
		if ti.txnInvalid() || !(ti.txnValue().op == opDelete || ti.txnValue().op == opTombstone) {
			return
		}
		ti.txnNext()
	}
}

// txnInvalid() determines if the current in-memory entry is invalid
func (ti *TxnIterator) txnInvalid() bool {
	if ti.invalid {
		return ti.invalid
	}
	ti.invalid = true
	current := ti.tree.Current()
	if current == nil || current.Key == "" {
		ti.invalid = true
		return ti.invalid
	}
	if !strings.HasPrefix(current.Key, ti.parentPrefix+ti.prefix) {
		return ti.invalid
	}
	ti.invalid = false
	return ti.invalid
}

// txnKey() returns the key of the current in-memory operation
func (ti *TxnIterator) txnKey() []byte {
	return []byte(ti.tree.Current().Key)
}

// txnValue() returns the value of the current in-memory operation
func (ti *TxnIterator) txnValue() valueOp {
	return ti.ops[ti.tree.Current().Key]
}

// compare() compares two byte slices, adjusting for reverse iteration if needed
func (ti *TxnIterator) compare(a, b []byte) int {
	if ti.reverse {
		return bytes.Compare(a, b) * -1
	}
	return bytes.Compare(a, b)
}

// txnNext() advances the index of the in-memory operations based on the iteration direction
func (ti *TxnIterator) txnNext() {
	ti.hasNext = ti.tree.HasNext()
	ti.tree.Next()
}

// seek() positions the iterator at the first entry that matches or exceeds the prefix.
func (ti *TxnIterator) seek() *TxnIterator {
	ti.tree.Move(&CacheItem{
		Key: ti.parentPrefix + ti.prefix,
	})
	return ti
}

// revSeek() positions the iterator at the last entry that matches the prefix in reverse order.
func (ti *TxnIterator) revSeek() *TxnIterator {
	bz := []byte(ti.parentPrefix + ti.prefix)
	endPrefix := string(prefixEnd(bz))
	ti.tree.Move(&CacheItem{
		Key: endPrefix,
	})
	return ti
}

var (
	endBytes = bytes.Repeat([]byte{0xFF}, maxKeyBytes+1)
)

// removePrefix() removes the prefix from the key
func removePrefix(b, prefix []byte) []byte { return b[len(prefix):] }

// prefixEnd() returns the end key for a given prefix by appending max possible bytes
func prefixEnd(prefix []byte) []byte {
	return lib.Append(prefix, endBytes)
}

// seekLast() positions the iterator at the last key for the given prefix
func seekLast(it *badger.Iterator, prefix []byte) {
	it.Seek(prefixEnd(prefix))
}

// BTREE ITERATOR CODE BELOW

type CacheItem struct {
	Key    string
	Exists bool
}

func (ti CacheItem) Less(than *CacheItem) bool {
	// compare the keys lexicographically
	return ti.Key < than.Key
}

// BTreeIterator provides external iteration over a btree
type BTreeIterator struct {
	tree    *btree.BTreeG[*CacheItem] // the btree to iterate over
	current *CacheItem                // current item in the iteration
	reverse bool                      // whether the iteration is in reverse order
}

// NewBTreeIterator() creates a new iterator starting at the closest item to the given key
func NewBTreeIterator(tree *btree.BTreeG[*CacheItem], start *CacheItem, reverse bool) *BTreeIterator {
	// create a new BTreeIterator
	bt := &BTreeIterator{
		tree:    tree,
		reverse: reverse,
	}
	// if no start item is provided, set the iterator to the first or last item based on the direction
	if start == nil || start.Key == "" {
		if reverse {
			val, _ := tree.Max()
			bt.current = val
		} else {
			val, _ := tree.Min()
			bt.current = val
		}
		return bt
	}
	// otherwise, move the iterator to that item
	bt.Move(start)
	return bt
}

// Move() moves the iterator to the given key or the closest item if the key is not found
func (bi *BTreeIterator) Move(item *CacheItem) {
	// reset the current item
	bi.current = nil
	// try to get an exact match
	if exactMatch, ok := bi.tree.Get(item); ok {
		bi.current = exactMatch
		return
	}
	// if no exact match, find the closest item based on the direction of iteration
	if bi.reverse {
		bi.current = &CacheItem{Key: item.Key + string(endBytes)}
		bi.current = bi.prev()
	} else {
		bi.current = &CacheItem{Key: item.Key}
		bi.current = bi.next()
	}
}

// Current() returns the current item in the iteration
func (bi *BTreeIterator) Current() *CacheItem {
	// if current is nil, return an empty Item to avoid nil pointer dereference
	if bi.current == nil {
		return &CacheItem{Key: "", Exists: false}
	}
	return bi.current
}

// Next() advances to the next item in the tree
func (bi *BTreeIterator) Next() *CacheItem {
	// check if current exist, otherwise the iterator is invalid
	if bi.current == nil {
		return nil
	}
	// go to the next item based on the direction of iteration
	if bi.reverse {
		bi.current = bi.prev()
	} else {
		bi.current = bi.next()
	}
	// return the current item which is the possible next item in the iteration
	return bi.Current()
}

// next() finds the next item in the tree based on the current item
func (bi *BTreeIterator) next() *CacheItem {
	var nextItem *CacheItem
	var found bool
	// find the next item
	bi.tree.AscendGreaterOrEqual(bi.current, func(item *CacheItem) bool {
		nextItem = item
		if nextItem.Key != bi.current.Key {
			found = true
			return false
		}
		return true
	})
	// if the item found, return it
	if found {
		return nextItem
	}
	// no next item
	return nil
}

// Prev() back towards the previous item in the tree
func (bi *BTreeIterator) Prev() *CacheItem {
	// check if current exist, otherwise the iterator is invalid
	if bi.current == nil {
		return nil
	}
	// go to the previous item based on the direction of iteration
	if bi.reverse {
		bi.current = bi.next()
	} else {
		bi.current = bi.prev()
	}
	// return the current item which is the possible previous item in the iteration
	return bi.Current()
}

// next() finds the previous item in the tree based on the current item
func (bi *BTreeIterator) prev() *CacheItem {
	var prevItem *CacheItem
	var found bool
	// find the previous item
	bi.tree.DescendLessOrEqual(bi.current, func(item *CacheItem) bool {
		prevItem = item
		if prevItem.Less(bi.current) {
			found = true
			return false
		}
		return true
	})
	// if the item found, return it
	if found {
		return prevItem
	}
	// no previous item
	return nil
}

// HasNext() returns true if there are more items after current
func (bi *BTreeIterator) HasNext() bool {
	if bi.reverse {
		return bi.hasPrev()
	}
	return bi.hasNext()
}

// hasNext() checks if there is a next item in the iteration
func (bi *BTreeIterator) hasNext() bool {
	if bi.current == nil {
		return false
	}
	return bi.next() != nil
}

// HasPrev() returns true if there are items before current
func (bi *BTreeIterator) HasPrev() bool {
	if bi.reverse {
		return bi.hasNext()
	}
	return bi.hasPrev()
}

// hasPrev() checks if there is a previous item in the iteration
func (bi *BTreeIterator) hasPrev() bool {
	if bi.current == nil {
		return false
	}
	return bi.prev() != nil
}
