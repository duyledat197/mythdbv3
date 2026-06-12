package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"mythdb/internal/block"
	"mythdb/internal/bloom"
	"mythdb/internal/key"
)

// SsTable is a read handle over an on-disk sorted string table.
type SsTable struct {
	file      *os.File
	path      string
	id        int
	blockMeta []BlockMeta
	bloom     *bloom.Bloom
	firstKey  []byte
	lastKey   []byte
}

// Open reads an SST file's metadata and bloom filter into memory.
func Open(id int, path string) (*SsTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := info.Size()
	if size < 8 {
		f.Close()
		return nil, fmt.Errorf("sstable: file too small")
	}

	read := func(off, n int64) ([]byte, error) {
		b := make([]byte, n)
		if _, err := f.ReadAt(b, off); err != nil {
			return nil, err
		}
		return b, nil
	}

	bloomOffBuf, err := read(size-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	bloomOff := int64(binary.LittleEndian.Uint32(bloomOffBuf))
	bloomBuf, err := read(bloomOff, size-4-bloomOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	bl, err := bloom.Decode(bloomBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	metaOffBuf, err := read(bloomOff-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	metaOff := int64(binary.LittleEndian.Uint32(metaOffBuf))
	metaBuf, err := read(metaOff, (bloomOff-4)-metaOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	metas, err := decodeBlockMeta(metaBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	t := &SsTable{file: f, path: path, id: id, blockMeta: metas, bloom: bl}
	if len(metas) > 0 {
		t.firstKey = metas[0].FirstKey
		t.lastKey = metas[len(metas)-1].LastKey
	}
	return t, nil
}

// ID returns the SST id.
func (t *SsTable) ID() int { return t.id }

// NumBlocks returns the number of data blocks.
func (t *SsTable) NumBlocks() int { return len(t.blockMeta) }

// FirstKey/LastKey bound the table's key range.
func (t *SsTable) FirstKey() []byte { return t.firstKey }
func (t *SsTable) LastKey() []byte  { return t.lastKey }

// MayContain consults the bloom filter for a key.
func (t *SsTable) MayContain(k []byte) bool { return t.bloom.MayContain(bloom.Hash(k)) }

// Close releases the underlying file handle.
func (t *SsTable) Close() error { return t.file.Close() }

// ReadBlock loads and decodes data block idx.
func (t *SsTable) ReadBlock(idx int) (*block.Block, error) {
	if idx < 0 || idx >= len(t.blockMeta) {
		return nil, fmt.Errorf("sstable: block index %d out of range", idx)
	}
	start := int64(t.blockMeta[idx].Offset)
	var end int64
	if idx+1 < len(t.blockMeta) {
		end = int64(t.blockMeta[idx+1].Offset)
	} else {
		info, err := t.file.Stat()
		if err != nil {
			return nil, err
		}
		end = t.metaSectionStart(info.Size())
	}
	raw := make([]byte, end-start)
	if _, err := t.file.ReadAt(raw, start); err != nil {
		return nil, err
	}
	return block.Decode(raw)
}

// metaSectionStart returns the byte offset where the data section ends and the
// meta section begins, derived from the file footer.
func (t *SsTable) metaSectionStart(size int64) int64 {
	buf := make([]byte, 4)
	if _, err := t.file.ReadAt(buf, size-4); err != nil {
		return size
	}
	bloomOff := int64(binary.LittleEndian.Uint32(buf))
	if _, err := t.file.ReadAt(buf, bloomOff-4); err != nil {
		return size
	}
	return int64(binary.LittleEndian.Uint32(buf))
}

// FindBlockIdx returns the index of the block that may contain key k:
// the last block whose FirstKey <= k, or 0.
func (t *SsTable) FindBlockIdx(k []byte) int {
	idx := sort.Search(len(t.blockMeta), func(i int) bool {
		return key.Compare(t.blockMeta[i].FirstKey, k) > 0
	})
	if idx == 0 {
		return 0
	}
	return idx - 1
}
