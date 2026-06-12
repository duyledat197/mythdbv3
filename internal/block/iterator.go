package block

import (
	"sort"

	"mythdb/internal/key"
)

// Iterator walks a decoded block in key order.
type Iterator struct {
	block      *Block
	idx        int
	key, value []byte
}

// NewIterAndSeekToFirst returns an iterator positioned on the first entry.
func NewIterAndSeekToFirst(b *Block) *Iterator {
	it := &Iterator{block: b}
	it.seekTo(0)
	return it
}

// NewIterAndSeekToKey returns an iterator on the first key >= target.
func NewIterAndSeekToKey(b *Block, target []byte) *Iterator {
	it := &Iterator{block: b}
	it.SeekToKey(target)
	return it
}

func (it *Iterator) seekTo(i int) {
	if i < 0 || i >= len(it.block.Offsets) {
		it.idx = len(it.block.Offsets)
		it.key, it.value = nil, nil
		return
	}
	it.idx = i
	it.key, it.value = it.block.entryAt(i)
}

// SeekToKey positions on the first entry whose key is >= target.
func (it *Iterator) SeekToKey(target []byte) {
	n := len(it.block.Offsets)
	i := sort.Search(n, func(i int) bool {
		k, _ := it.block.entryAt(i)
		return key.Compare(k, target) >= 0
	})
	it.seekTo(i)
}

func (it *Iterator) IsValid() bool { return it.idx < len(it.block.Offsets) }
func (it *Iterator) Key() []byte   { return it.key }
func (it *Iterator) Value() []byte { return it.value }

func (it *Iterator) Next() error {
	it.seekTo(it.idx + 1)
	return nil
}
