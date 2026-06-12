package block

import "encoding/binary"

// Builder accumulates entries into a single block up to a target size.
type Builder struct {
	offsets   []uint16
	data      []byte
	blockSize int
	firstKey  []byte
}

// NewBuilder creates a builder targeting blockSize bytes.
func NewBuilder(blockSize int) *Builder {
	return &Builder{blockSize: blockSize}
}

func (b *Builder) estimatedSize() int {
	// data + offsets + num(2) + crc(4)
	return len(b.data) + len(b.offsets)*2 + 2 + 4
}

// Add appends key/value. It returns false (without adding) when the block is
// non-empty and adding would exceed the target size. The first entry always
// succeeds so any single entry fits.
func (b *Builder) Add(k, v []byte) bool {
	entrySize := 2 + len(k) + 2 + len(v)
	if !b.IsEmpty() && b.estimatedSize()+entrySize+2 > b.blockSize {
		return false
	}
	b.offsets = append(b.offsets, uint16(len(b.data)))
	tmp := make([]byte, 2)
	binary.LittleEndian.PutUint16(tmp, uint16(len(k)))
	b.data = append(b.data, tmp...)
	b.data = append(b.data, k...)
	binary.LittleEndian.PutUint16(tmp, uint16(len(v)))
	b.data = append(b.data, tmp...)
	b.data = append(b.data, v...)
	if b.firstKey == nil {
		b.firstKey = append([]byte(nil), k...)
	}
	return true
}

// IsEmpty reports whether no entries have been added.
func (b *Builder) IsEmpty() bool { return len(b.offsets) == 0 }

// Build finalizes the accumulated entries into a Block.
func (b *Builder) Build() *Block {
	return &Block{Data: b.data, Offsets: b.offsets}
}
