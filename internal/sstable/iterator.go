package sstable

import "mythdb/internal/block"

// Iterator walks an SST across its data blocks in key order.
type Iterator struct {
	table   *SsTable
	blkIdx  int
	blk     *block.Block
	blkIter *block.Iterator
}

// NewIterAndSeekToFirst positions on the first key in the table.
func NewIterAndSeekToFirst(t *SsTable) (*Iterator, error) {
	it := &Iterator{table: t}
	if err := it.loadBlock(0); err != nil {
		return nil, err
	}
	if it.blk != nil {
		it.blkIter = block.NewIterAndSeekToFirst(it.blk)
	}
	return it, nil
}

// NewIterAndSeekToKey positions on the first key >= target.
func NewIterAndSeekToKey(t *SsTable, target []byte) (*Iterator, error) {
	it := &Iterator{table: t}
	if err := it.SeekToKey(target); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *Iterator) loadBlock(idx int) error {
	if idx >= it.table.NumBlocks() {
		it.blkIdx = idx
		it.blk = nil
		it.blkIter = nil
		return nil
	}
	blk, err := it.table.ReadBlock(idx)
	if err != nil {
		return err
	}
	it.blkIdx = idx
	it.blk = blk
	return nil
}

// SeekToKey positions on the first key >= target, descending into the right block.
func (it *Iterator) SeekToKey(target []byte) error {
	idx := it.table.FindBlockIdx(target)
	if err := it.loadBlock(idx); err != nil {
		return err
	}
	if it.blk == nil {
		return nil
	}
	it.blkIter = block.NewIterAndSeekToKey(it.blk, target)
	for !it.blkIter.IsValid() {
		if err := it.loadBlock(it.blkIdx + 1); err != nil {
			return err
		}
		if it.blk == nil {
			return nil
		}
		it.blkIter = block.NewIterAndSeekToFirst(it.blk)
	}
	return nil
}

func (it *Iterator) IsValid() bool { return it.blkIter != nil && it.blkIter.IsValid() }
func (it *Iterator) Key() []byte   { return it.blkIter.Key() }
func (it *Iterator) Value() []byte { return it.blkIter.Value() }

func (it *Iterator) Next() error {
	if err := it.blkIter.Next(); err != nil {
		return err
	}
	if it.blkIter.IsValid() {
		return nil
	}
	if err := it.loadBlock(it.blkIdx + 1); err != nil {
		return err
	}
	if it.blk == nil {
		return nil
	}
	it.blkIter = block.NewIterAndSeekToFirst(it.blk)
	return nil
}
