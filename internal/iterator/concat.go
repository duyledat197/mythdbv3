package iterator

import (
	"sort"

	"mythdb/internal/key"
	"mythdb/internal/sstable"
)

// SstConcatIterator iterates a sorted run of non-overlapping SSTs as a single
// stream. The tables must be sorted by key with disjoint ranges.
type SstConcatIterator struct {
	tables []*sstable.SsTable
	idx    int
	cur    *sstable.Iterator
}

// NewConcatIterAndSeekToFirst positions on the first key of the run.
func NewConcatIterAndSeekToFirst(tables []*sstable.SsTable) (*SstConcatIterator, error) {
	it := &SstConcatIterator{tables: tables}
	if len(tables) == 0 {
		it.idx = 0
		return it, nil
	}
	cur, err := sstable.NewIterAndSeekToFirst(tables[0])
	if err != nil {
		return nil, err
	}
	it.idx = 0
	it.cur = cur
	if err := it.skipExhausted(); err != nil {
		return nil, err
	}
	return it, nil
}

// NewConcatIterAndSeekToKey positions on the first key >= target across the run.
func NewConcatIterAndSeekToKey(tables []*sstable.SsTable, target []byte) (*SstConcatIterator, error) {
	it := &SstConcatIterator{tables: tables}
	// First table whose LastKey >= target may contain it.
	i := sort.Search(len(tables), func(i int) bool {
		return key.Compare(tables[i].LastKey(), target) >= 0
	})
	if i >= len(tables) {
		it.idx = len(tables)
		return it, nil
	}
	cur, err := sstable.NewIterAndSeekToKey(tables[i], target)
	if err != nil {
		return nil, err
	}
	it.idx = i
	it.cur = cur
	if err := it.skipExhausted(); err != nil {
		return nil, err
	}
	return it, nil
}

// skipExhausted advances to the next table when the current one is exhausted.
func (it *SstConcatIterator) skipExhausted() error {
	for it.cur != nil && !it.cur.IsValid() {
		it.idx++
		if it.idx >= len(it.tables) {
			it.cur = nil
			return nil
		}
		cur, err := sstable.NewIterAndSeekToFirst(it.tables[it.idx])
		if err != nil {
			return err
		}
		it.cur = cur
	}
	return nil
}

func (it *SstConcatIterator) IsValid() bool { return it.cur != nil && it.cur.IsValid() }
func (it *SstConcatIterator) Key() []byte   { return it.cur.Key() }
func (it *SstConcatIterator) Value() []byte { return it.cur.Value() }

func (it *SstConcatIterator) Next() error {
	if err := it.cur.Next(); err != nil {
		return err
	}
	return it.skipExhausted()
}
