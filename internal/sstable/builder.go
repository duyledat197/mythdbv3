package sstable

import (
	"encoding/binary"
	"os"

	"mythdb/internal/block"
	"mythdb/internal/bloom"
)

// Builder accumulates sorted key-value pairs and writes an SST file.
type Builder struct {
	blockBuilder *block.Builder
	blockSize    int
	data         []byte
	meta         []BlockMeta
	firstKey     []byte
	lastKey      []byte
	keyHashes    []uint32
}

// NewBuilder creates an SST builder targeting blockSize-byte data blocks.
func NewBuilder(blockSize int) *Builder {
	return &Builder{
		blockBuilder: block.NewBuilder(blockSize),
		blockSize:    blockSize,
	}
}

// Add appends a key-value pair. Keys must arrive in ascending order.
func (b *Builder) Add(k, v []byte) {
	b.keyHashes = append(b.keyHashes, bloom.Hash(k))
	if b.firstKey == nil {
		b.firstKey = append([]byte(nil), k...)
	}
	if b.blockBuilder.Add(k, v) {
		b.lastKey = append(b.lastKey[:0], k...)
		return
	}
	// Current block is full: finish it and start a new one.
	b.finishBlock()
	b.blockBuilder.Add(k, v)
	b.lastKey = append(b.lastKey[:0], k...)
}

func (b *Builder) finishBlock() {
	if b.blockBuilder.IsEmpty() {
		return
	}
	blk := b.blockBuilder.Build()
	encoded := blk.Encode()
	first := append([]byte(nil), blk.FirstKey()...)
	last := append([]byte(nil), b.lastKey...)
	b.meta = append(b.meta, BlockMeta{
		Offset:   len(b.data),
		FirstKey: first,
		LastKey:  last,
	})
	b.data = append(b.data, encoded...)
	b.blockBuilder = block.NewBuilder(b.blockSize)
}

// Build flushes any pending block and writes the SST file to path.
func (b *Builder) Build(id int, path string) (*SsTable, error) {
	b.finishBlock()

	buf := append([]byte(nil), b.data...)

	metaOffset := len(buf)
	buf = append(buf, encodeBlockMeta(b.meta)...)
	off4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(off4, uint32(metaOffset))
	buf = append(buf, off4...)

	bl := bloom.Build(b.keyHashes, bloom.BitsPerKey(len(b.keyHashes), 0.01))
	bloomOffset := len(buf)
	buf = append(buf, bl.Encode()...)
	binary.LittleEndian.PutUint32(off4, uint32(bloomOffset))
	buf = append(buf, off4...)

	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &SsTable{
		file:      f,
		path:      path,
		id:        id,
		blockMeta: b.meta,
		bloom:     bl,
		firstKey:  b.firstKey,
		lastKey:   append([]byte(nil), b.lastKey...),
	}, nil
}
