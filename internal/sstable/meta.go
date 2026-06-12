package sstable

import (
	"encoding/binary"
	"fmt"
)

// BlockMeta describes one data block's location and key range.
type BlockMeta struct {
	Offset   int    // byte offset of the block within the file
	FirstKey []byte // first key in the block
	LastKey  []byte // last key in the block
}

// encodeBlockMeta serializes a slice of BlockMeta:
// [count u32] then per entry: offset u32, firstLen u16, first, lastLen u16, last.
func encodeBlockMeta(metas []BlockMeta) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(metas)))
	tmp4 := make([]byte, 4)
	tmp2 := make([]byte, 2)
	for _, m := range metas {
		binary.LittleEndian.PutUint32(tmp4, uint32(m.Offset))
		buf = append(buf, tmp4...)
		binary.LittleEndian.PutUint16(tmp2, uint16(len(m.FirstKey)))
		buf = append(buf, tmp2...)
		buf = append(buf, m.FirstKey...)
		binary.LittleEndian.PutUint16(tmp2, uint16(len(m.LastKey)))
		buf = append(buf, tmp2...)
		buf = append(buf, m.LastKey...)
	}
	return buf
}

func decodeBlockMeta(buf []byte) ([]BlockMeta, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("sstable: meta too short")
	}
	count := int(binary.LittleEndian.Uint32(buf))
	pos := 4
	metas := make([]BlockMeta, 0, count)
	readBytes := func() ([]byte, error) {
		if pos+2 > len(buf) {
			return nil, fmt.Errorf("sstable: meta truncated")
		}
		n := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+n > len(buf) {
			return nil, fmt.Errorf("sstable: meta truncated")
		}
		b := append([]byte(nil), buf[pos:pos+n]...)
		pos += n
		return b, nil
	}
	for i := 0; i < count; i++ {
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("sstable: meta truncated")
		}
		off := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		first, err := readBytes()
		if err != nil {
			return nil, err
		}
		last, err := readBytes()
		if err != nil {
			return nil, err
		}
		metas = append(metas, BlockMeta{Offset: off, FirstKey: first, LastKey: last})
	}
	return metas, nil
}
