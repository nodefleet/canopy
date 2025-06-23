package lib

import (
	"container/list"
	"errors"
	"github.com/canopy-network/canopy/lib/crypto"
	"github.com/stretchr/testify/require"
	"math"
	"sync"
	"testing"
	"time"
)

func TestAddTransactionFeeOrdering(t *testing.T) {
	// pre-define a mempool with the default config
	mempool := NewMempool(DefaultMempoolConfig())
	// add a transaction
	recheck, err := mempool.AddTransaction([]byte("b"), 1000)
	require.NoError(t, err)
	require.False(t, recheck)
	// add another transaction with the same fee
	recheck, err = mempool.AddTransaction([]byte("c"), 1000)
	require.NoError(t, err)
	require.False(t, recheck)
	// add another transaction with a higher fee
	recheck, err = mempool.AddTransaction([]byte("a"), 1001)
	require.NoError(t, err)
	// ensure recheck on non-append insert
	require.True(t, recheck)
	// add another transaction with the lowest fee
	recheck, err = mempool.AddTransaction([]byte("e"), 1)
	require.NoError(t, err)
	require.False(t, recheck)
	// add another transaction with the same fee
	recheck, err = mempool.AddTransaction([]byte("d"), 1000)
	require.NoError(t, err)
	// ensure recheck on non-append insert
	require.True(t, recheck)
	it := mempool.Iterator()
	defer it.Close()
	// iterate through each
	result := ""
	for ; it.Valid(); it.Next() {
		result += string(it.Key())
	}
	// compare got vs expected
	require.Equal(t, "abcde", result)
}

func TestAddTransaction(t *testing.T) {
	// pre-define a transaction to add
	transaction := MempoolTx{
		Tx:  []byte("bytes"),
		Fee: 1000,
	}
	tests := []struct {
		name    string
		detail  string
		mempool FeeMempool
		toAdd   MempoolTx
		// expected
		transactions [][]byte
		recheck      bool
		count        int
		error        string
	}{
		{
			name:   "max tx size",
			detail: "the tx size exceeds max (config)",
			mempool: FeeMempool{
				l: sync.RWMutex{},
			},
			toAdd: transaction,
			error: "max tx size",
		},
		{
			name:   "already exists",
			detail: "transaction not added because it already exists",
			mempool: FeeMempool{
				l:       sync.RWMutex{},
				hashMap: map[string]struct{}{crypto.HashString(transaction.Tx): {}},
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: 0,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      10,
				},
			},
			toAdd: transaction,
			error: "already found in mempool",
		},
		{
			name:   "recheck max tx count",
			detail: "max tx count causes a recheck",
			mempool: FeeMempool{
				l:        sync.RWMutex{},
				hashMap:  make(map[string]struct{}),
				pool:     MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)},
				count:    0,
				txsBytes: 0,
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: 0,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      10,
				},
			},
			recheck: true,
			toAdd:   transaction,
		},
		{
			name:   "recheck max total bytes",
			detail: "max total bytes",
			mempool: FeeMempool{
				l:        sync.RWMutex{},
				hashMap:  make(map[string]struct{}),
				pool:     MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)},
				count:    0,
				txsBytes: 0,
				config: MempoolConfig{
					MaxTotalBytes:       0,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      10,
				},
			},
			recheck: true,
			toAdd:   transaction,
		},
		{
			name:   "no recheck",
			detail: "there's no recheck as the transaction is added without exceeding limits",
			mempool: FeeMempool{
				l:       sync.RWMutex{},
				hashMap: make(map[string]struct{}),
				pool:    MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)},
				count:   0,
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      10,
				},
			},
			count: 1,
			toAdd: transaction,
			transactions: [][]byte{
				transaction.Tx,
			},
		},
		{
			name:   "multi-transaction",
			detail: "test transaction ordering with multi-transaction",
			mempool: FeeMempool{
				l:       sync.RWMutex{},
				hashMap: make(map[string]struct{}),
				pool:    MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)},
				count:   0,
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      10,
				},
			},
			count: 1,
			toAdd: transaction,
			transactions: [][]byte{
				transaction.Tx,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// execute function call
			gotRecheck, err := test.mempool.AddTransaction(test.toAdd.Tx, test.toAdd.Fee)
			// validate if an error is expected
			require.Equal(t, err != nil, test.error != "", err)
			// validate actual error if any
			if err != nil {
				require.ErrorContains(t, err, test.error, err)
				return
			}
			// compare got vs expected
			require.Equal(t, test.recheck, gotRecheck)
			require.Equal(t, test.count, test.mempool.count)
			// call get transaction
			gotTxs := test.mempool.GetTransactions(math.MaxUint64)
			require.Equal(t, test.transactions, gotTxs)
			// test mempool.Contains
			for _, txn := range test.transactions {
				require.True(t, test.mempool.Contains(crypto.HashString(txn)))
			}
		})
	}
}

func TestGetAndContainsTransaction(t *testing.T) {
	// define test cases
	tests := []struct {
		name          string
		detail        string
		txs           []MempoolTx
		mempool       Mempool
		expectedCount uint64
		expectedTxs   []MempoolTx
		maxBytes      uint64
	}{
		{
			name:   "reap top 3 transactions",
			detail: "get the top 3 transactions only based on the max bytes ",
			txs: []MempoolTx{
				{
					Tx:  []byte("a"),
					Fee: 1000,
				},
				{
					Tx:  []byte("b"),
					Fee: 1001,
				},
				{
					Tx:  []byte("c"),
					Fee: 999,
				},
				{
					Tx:  []byte("d"),
					Fee: 1,
				},
			},
			mempool: &FeeMempool{
				l:       sync.RWMutex{},
				hashMap: make(map[string]struct{}),
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      30,
				},
			},
			expectedCount: 3,
			expectedTxs: []MempoolTx{
				{
					Tx:  []byte("b"),
					Fee: 1001,
				},
				{
					Tx:  []byte("a"),
					Fee: 1000,
				},
				{
					Tx:  []byte("c"),
					Fee: 999,
				},
			},
			maxBytes: 3,
		},
		{
			name:   "reap top 2 transactions",
			detail: "get the top 2 transactions only based on the max bytes",
			txs: []MempoolTx{
				{
					Tx:  []byte("a"),
					Fee: 1000,
				},
				{
					Tx:  []byte("b"),
					Fee: 1001,
				},
				{
					Tx:  []byte("c"),
					Fee: 999,
				},
				{
					Tx:  []byte("d"),
					Fee: 1,
				},
			},
			mempool: &FeeMempool{
				l:       sync.RWMutex{},
				hashMap: make(map[string]struct{}),
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      30,
				},
			},
			expectedCount: 2,
			expectedTxs: []MempoolTx{
				{
					Tx:  []byte("b"),
					Fee: 1001,
				},
				{
					Tx:  []byte("a"),
					Fee: 1000,
				},
			},
			maxBytes: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// pre-add the transactions
			for _, txn := range test.txs {
				_, err := test.mempool.AddTransaction(txn.Tx, txn.Fee)
				require.NoError(t, err)
			}
			// get the transactions
			got := test.mempool.GetTransactions(test.maxBytes)
			// ensure the count is correct
			require.EqualValues(t, test.expectedCount, len(got))
			require.Equal(t, len(test.expectedTxs), len(got))
			// compare got vs expected
			for i := 0; i < len(got); i++ {
				require.Equal(t, test.expectedTxs[i].Tx, got[i])
				require.True(t, test.mempool.Contains(crypto.HashString(test.txs[i].Tx)))
			}
		})
	}
}

func TestDeleteTransaction(t *testing.T) {
	// define test cases
	tests := []struct {
		name          string
		detail        string
		mempool       Mempool
		delete        [][]byte
		expectedTxs   []MempoolTx
		expectedCount uint64
	}{
		{
			name:   "delete the first transaction",
			detail: "delete the transaction with the highest fee",
			mempool: &FeeMempool{
				l: sync.RWMutex{},
				pool: func() MempoolTxs {
					m := MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)}
					m.insert(MempoolTx{
						Tx:  []byte("b"),
						Fee: 1001,
					})
					m.insert(MempoolTx{
						Tx:  []byte("a"),
						Fee: 1000,
					})
					m.insert(MempoolTx{
						Tx:  []byte("c"),
						Fee: 999,
					})
					return m
				}(),
				hashMap: map[string]struct{}{
					crypto.HashString([]byte("a")): {},
					crypto.HashString([]byte("b")): {},
					crypto.HashString([]byte("c")): {},
				},
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      30,
				},
			},
			delete: [][]byte{
				[]byte("b"),
			},
			expectedCount: 2,
			expectedTxs: []MempoolTx{
				{
					Tx:  []byte("a"),
					Fee: 1000,
				},
				{
					Tx:  []byte("c"),
					Fee: 999,
				},
			},
		},
		{
			name:   "delete the second two transactions",
			detail: "delete the 2 transactions with the lowest fees",
			mempool: &FeeMempool{
				l: sync.RWMutex{},
				pool: func() MempoolTxs {
					m := MempoolTxs{count: 0, l: list.New(), m: make(map[string]*list.Element)}
					m.insert(MempoolTx{
						Tx:  []byte("b"),
						Fee: 1001,
					})
					m.insert(MempoolTx{
						Tx:  []byte("a"),
						Fee: 1000,
					})
					m.insert(MempoolTx{
						Tx:  []byte("c"),
						Fee: 999,
					})
					return m
				}(),
				hashMap: map[string]struct{}{
					crypto.HashString([]byte("a")): {},
					crypto.HashString([]byte("b")): {},
					crypto.HashString([]byte("c")): {},
				},
				config: MempoolConfig{
					MaxTotalBytes:       math.MaxUint64,
					MaxTransactionCount: math.MaxUint32,
					IndividualMaxTxSize: math.MaxUint32,
					DropPercentage:      30,
				},
			},
			delete: [][]byte{
				[]byte("a"),
				[]byte("c"),
			},
			expectedCount: 1,
			expectedTxs: []MempoolTx{
				{
					Tx:  []byte("b"),
					Fee: 1001,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// delete the transactions
			for _, toDelete := range test.delete {
				test.mempool.DeleteTransaction(toDelete)
			}
			// get the transactions left
			got := test.mempool.GetTransactions(math.MaxUint64)
			// ensure the count is correct
			require.EqualValues(t, test.expectedCount, len(got))
			require.Equal(t, len(test.expectedTxs), len(got))
			// compare got vs expected
			for i := 0; i < len(got); i++ {
				require.Equal(t, test.expectedTxs[i].Tx, got[i])
				require.True(t, test.mempool.Contains(crypto.HashString(test.expectedTxs[i].Tx)))
			}
		})
	}
}

func TestFailedTxCache(t *testing.T) {
	rawPubKey := newTestPublicKeyBytes(t)
	pubKey, err := crypto.NewPublicKeyFromBytes(rawPubKey)
	require.NoError(t, err)

	// pre-define a test message
	sig := &Signature{
		PublicKey: newTestPublicKeyBytes(t),
		Signature: rawPubKey,
	}
	// pre-define an any for testing
	a, e := NewAny(sig)
	require.NoError(t, e)
	// pre-define a transaction
	tx := &Transaction{
		MessageType: testMessageName,
		Msg:         a,
		Signature:   sig,
		Time:        uint64(time.Now().UnixMicro()),
		Fee:         1,
		Memo:        "memo",
	}
	// marshal transaction to bytes
	txBytes, err := Marshal(tx)
	require.NoError(t, err)

	// define test cases
	tests := []struct {
		name                    string
		dissallowedMessageTypes []string
		txBytes                 []byte
		hash                    string
		err                     error
		expectedResult          bool
		address                 string
	}{
		{
			name:                    "valid transaction",
			dissallowedMessageTypes: []string{},
			txBytes:                 txBytes,
			hash:                    "validHash",
			err:                     nil,
			expectedResult:          true,
			address:                 pubKey.Address().String(),
		},
		{
			name:                    "invalid message type",
			dissallowedMessageTypes: []string{testMessageName},
			txBytes:                 txBytes,
			hash:                    "invalidHash",
			err:                     nil,
			expectedResult:          false,
			address:                 pubKey.Address().String(),
		},
		{
			name:                    "unmarshal error",
			dissallowedMessageTypes: []string{},
			txBytes:                 []byte("invalidBytes"),
			hash:                    "unmarshalErrorHash",
			err:                     ErrUnmarshal(errors.New("unmarshal error")),
			expectedResult:          false,
			address:                 pubKey.Address().String(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create a new failed tx cache
			cache := NewFailedTxCache(test.dissallowedMessageTypes...)
			// add transaction to cache
			result := cache.Add(test.txBytes, test.hash, test.err)
			// validate result
			require.Equal(t, test.expectedResult, result)
			if test.expectedResult {
				// validate cache
				failedTx, ok := cache.Get(test.hash)
				require.True(t, ok)
				require.Equal(t, test.err, failedTx.Error)
				require.EqualExportedValues(t, tx, failedTx.Transaction)

				// validate get all
				failedTxs := cache.GetFailedForAddress(test.address)
				require.Len(t, failedTxs, 1)
				require.Equal(t, failedTx, failedTxs[0])

				// validate removal
				cache.Remove(test.hash)
				_, ok = cache.Get(test.hash)
				require.False(t, ok)
			} else {
				// validate cache
				tx, ok := cache.Get(test.hash)
				require.False(t, ok)
				require.Nil(t, tx)
			}
		})
	}
}
