package iterator

import (
	"container/heap"

	"mythdb/internal/key"
)

type mergeItem struct {
	idx  int
	iter StorageIterator
}

type iterHeap []*mergeItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	c := key.Compare(h[i].iter.Key(), h[j].iter.Key())
	if c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx // lower index == newer source wins
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)   { *h = append(*h, x.(*mergeItem)) }
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// MergeIterator merges N iterators. On duplicate keys the source with the
// lowest index (newest) wins; equal keys in older sources are skipped.
type MergeIterator struct {
	h       iterHeap
	current *mergeItem
}

// NewMergeIterator builds a merge over iters. iters[0] is treated as newest.
func NewMergeIterator(iters []StorageIterator) *MergeIterator {
	m := &MergeIterator{}
	for i, it := range iters {
		if it != nil && it.IsValid() {
			m.h = append(m.h, &mergeItem{idx: i, iter: it})
		}
	}
	heap.Init(&m.h)
	if m.h.Len() > 0 {
		m.current = heap.Pop(&m.h).(*mergeItem)
	}
	return m
}

func (m *MergeIterator) IsValid() bool { return m.current != nil && m.current.iter.IsValid() }
func (m *MergeIterator) Key() []byte   { return m.current.iter.Key() }
func (m *MergeIterator) Value() []byte { return m.current.iter.Value() }

func (m *MergeIterator) Next() error {
	cur := m.current
	// Copy the key: advancing other iterators must not be affected by buffer reuse.
	curKey := append([]byte(nil), cur.iter.Key()...)

	// Advance every other iterator still positioned on the same key.
	for m.h.Len() > 0 && key.Compare(m.h[0].iter.Key(), curKey) == 0 {
		top := heap.Pop(&m.h).(*mergeItem)
		if err := top.iter.Next(); err != nil {
			return err
		}
		if top.iter.IsValid() {
			heap.Push(&m.h, top)
		}
	}

	// Advance the current iterator.
	if err := cur.iter.Next(); err != nil {
		return err
	}
	if cur.iter.IsValid() {
		heap.Push(&m.h, cur)
	}

	if m.h.Len() > 0 {
		m.current = heap.Pop(&m.h).(*mergeItem)
	} else {
		m.current = nil
	}
	return nil
}
