package lsm

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"mythdb/internal/iterator"
)

// ErrSerialization is returned by Commit when a serialization conflict is found.
var ErrSerialization = errors.New("lsm: transaction aborted due to serialization conflict")

// Txn is an optimistic, serializable-snapshot-isolation transaction. Reads see a
// snapshot at readTs merged with the transaction's own buffered writes; writes are
// staged locally and applied atomically at Commit.
type Txn struct {
	engine *Storage
	readTs uint64

	mu        sync.Mutex
	local     map[string][]byte   // user key -> value; empty value means delete
	accessSet map[uint64]struct{} // hashes of keys read or written
	done      bool
}

// Put stages an insert/overwrite.
func (t *Txn) Put(key, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.local[string(key)] = append([]byte(nil), value...)
	t.accessSet[hashKey(key)] = struct{}{}
}

// Delete stages a tombstone.
func (t *Txn) Delete(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.local[string(key)] = []byte{}
	t.accessSet[hashKey(key)] = struct{}{}
}

// Get returns the value for key in the transaction's view (local writes first,
// then the snapshot), recording the access for conflict detection.
func (t *Txn) Get(key []byte) ([]byte, bool, error) {
	t.mu.Lock()
	t.accessSet[hashKey(key)] = struct{}{}
	if v, ok := t.local[string(key)]; ok {
		t.mu.Unlock()
		if len(v) == 0 {
			return nil, false, nil
		}
		return append([]byte(nil), v...), true, nil
	}
	t.mu.Unlock()

	it, err := t.engine.buildMvccScan(key, nil, t.readTs)
	if err != nil {
		return nil, false, err
	}
	if it.IsValid() && bytes.Equal(it.Key(), key) {
		return append([]byte(nil), it.Value()...), true, nil
	}
	return nil, false, nil
}

// Scan returns an iterator over [lower, upper) in the transaction's view.
func (t *Txn) Scan(lower, upper []byte) (iterator.StorageIterator, error) {
	eng, err := t.engine.buildMvccScan(lower, upper, t.readTs)
	if err != nil {
		return nil, err
	}
	local := t.localIterator(lower, upper)
	return newTxnIterator(local, eng, t), nil
}

// localIterator snapshots the local buffer into a sorted, range-bounded iterator
// over user keys (including deletes, so they can shadow snapshot results).
func (t *Txn) localIterator(lower, upper []byte) *txnLocalIterator {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]string, 0, len(t.local))
	for k := range t.local {
		if lower != nil && k < string(lower) {
			continue
		}
		if upper != nil && k >= string(upper) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]txnLocalEntry, len(keys))
	for i, k := range keys {
		entries[i] = txnLocalEntry{key: []byte(k), value: append([]byte(nil), t.local[k]...)}
	}
	return &txnLocalIterator{entries: entries}
}

// recordRead notes a user key the transaction observed during a scan.
func (t *Txn) recordRead(userKey []byte) {
	t.mu.Lock()
	t.accessSet[hashKey(userKey)] = struct{}{}
	t.mu.Unlock()
}

// Rollback abandons the transaction.
func (t *Txn) Rollback() {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.done = true
	t.mu.Unlock()
	t.engine.mvcc.removeReader(t.readTs)
}

type txnLocalEntry struct {
	key, value []byte
}

// txnLocalIterator iterates a sorted slice of local buffer entries (user keys),
// including deletes (empty value).
type txnLocalIterator struct {
	entries []txnLocalEntry
	i       int
}

func (it *txnLocalIterator) IsValid() bool { return it.i < len(it.entries) }
func (it *txnLocalIterator) Key() []byte   { return it.entries[it.i].key }
func (it *txnLocalIterator) Value() []byte { return it.entries[it.i].value }
func (it *txnLocalIterator) Next() error   { it.i++; return nil }

// txnIterator merges the local buffer (user keys, includes deletes) with the
// engine snapshot scan (user keys, live only). Local wins ties; entries whose
// chosen value is empty (a local delete) are skipped. Yielded keys are recorded.
type txnIterator struct {
	local iterator.StorageIterator
	eng   iterator.StorageIterator
	txn   *Txn
	key   []byte
	value []byte
	valid bool
	err   error
}

func newTxnIterator(local, eng iterator.StorageIterator, txn *Txn) *txnIterator {
	it := &txnIterator{local: local, eng: eng, txn: txn}
	it.advance()
	return it
}

func (it *txnIterator) advance() {
	for {
		lv := it.local.IsValid()
		ev := it.eng.IsValid()
		if !lv && !ev {
			it.valid = false
			return
		}
		useLocal := false
		switch {
		case lv && ev:
			c := bytes.Compare(it.local.Key(), it.eng.Key())
			if c < 0 {
				useLocal = true
			} else if c > 0 {
				useLocal = false
			} else {
				// Same key: local overrides; advance the engine past the shadowed version.
				useLocal = true
				if err := it.eng.Next(); err != nil {
					it.err = err
					it.valid = false
					return
				}
			}
		case lv:
			useLocal = true
		default:
			useLocal = false
		}

		var k, v []byte
		if useLocal {
			k = it.local.Key()
			v = it.local.Value()
			if err := it.local.Next(); err != nil {
				it.err = err
				it.valid = false
				return
			}
		} else {
			k = it.eng.Key()
			v = it.eng.Value()
			if err := it.eng.Next(); err != nil {
				it.err = err
				it.valid = false
				return
			}
		}
		if len(v) == 0 {
			continue // local delete; skip
		}
		it.key = append([]byte(nil), k...)
		it.value = append([]byte(nil), v...)
		it.valid = true
		it.txn.recordRead(it.key)
		return
	}
}

func (it *txnIterator) IsValid() bool { return it.err == nil && it.valid }
func (it *txnIterator) Key() []byte   { return it.key }
func (it *txnIterator) Value() []byte { return it.value }

func (it *txnIterator) Next() error {
	if !it.valid {
		return it.err
	}
	it.advance()
	return it.err
}
