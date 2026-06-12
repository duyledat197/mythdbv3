package memtable

import (
	"math/rand"

	"mythdb/internal/key"
)

const (
	maxLevel = 16
	pValue   = 0.5
)

type node struct {
	key, value []byte
	next       []*node
}

type skiplist struct {
	head  *node
	level int
	rng   *rand.Rand
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &node{next: make([]*node, maxLevel)},
		level: 1,
		rng:   rand.New(rand.NewSource(1)),
	}
}

func (s *skiplist) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rng.Float64() < pValue {
		lvl++
	}
	return lvl
}

// put inserts or overwrites key. Returns (insertedNew, oldValueLen).
func (s *skiplist) put(k, v []byte) (bool, int) {
	update := make([]*node, maxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && key.Compare(x.next[i].key, k) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	x = x.next[0]
	if x != nil && key.Compare(x.key, k) == 0 {
		oldLen := len(x.value)
		x.value = append([]byte(nil), v...)
		return false, oldLen
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &node{
		key:   append([]byte(nil), k...),
		value: append([]byte(nil), v...),
		next:  make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	return true, 0
}

func (s *skiplist) get(k []byte) ([]byte, bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && key.Compare(x.next[i].key, k) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && key.Compare(x.key, k) == 0 {
		return x.value, true
	}
	return nil, false
}

// seek returns the first node with key >= k (or nil). k==nil means first node.
func (s *skiplist) seek(k []byte) *node {
	x := s.head
	if k == nil {
		return x.next[0]
	}
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && key.Compare(x.next[i].key, k) < 0 {
			x = x.next[i]
		}
	}
	return x.next[0]
}
