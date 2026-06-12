package memtable

import (
	"sync"

	"mythdb/internal/key"
)

// Memtable is a sorted in-memory table backed by a skiplist. A tombstone is a
// key stored with a zero-length value.
type Memtable struct {
	mu         sync.RWMutex
	list       *skiplist
	id         int
	approxSize int64
}

// New creates an empty memtable with the given id.
func New(id int) *Memtable {
	return &Memtable{list: newSkiplist(), id: id}
}

func (m *Memtable) ID() int { return m.id }

// Put inserts or overwrites key with value.
func (m *Memtable) Put(k, v []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inserted, oldLen := m.list.put(k, v)
	if inserted {
		m.approxSize += int64(len(k) + len(v))
	} else {
		m.approxSize += int64(len(v) - oldLen)
	}
}

// Get returns the stored value and whether the key exists in this memtable.
// A returned empty value means a tombstone (the engine treats it as deleted).
func (m *Memtable) Get(k []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.get(k)
}

// ApproximateSize is the running estimate of stored key+value bytes.
func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.approxSize
}

// IsEmpty reports whether the memtable has no entries.
func (m *Memtable) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.head.next[0] == nil
}

// Iter returns an iterator over [lower, upper). nil lower means from start,
// nil upper means to end.
func (m *Memtable) Iter(lower, upper []byte) *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &Iterator{cur: m.list.seek(lower), upper: upper}
}

// Iterator walks memtable entries in key order, bounded above by upper (exclusive).
type Iterator struct {
	cur   *node
	upper []byte
}

func (it *Iterator) IsValid() bool {
	if it.cur == nil {
		return false
	}
	if it.upper != nil && key.Compare(it.cur.key, it.upper) >= 0 {
		return false
	}
	return true
}

func (it *Iterator) Key() []byte   { return it.cur.key }
func (it *Iterator) Value() []byte { return it.cur.value }

func (it *Iterator) Next() error {
	if it.cur != nil {
		it.cur = it.cur.next[0]
	}
	return nil
}
