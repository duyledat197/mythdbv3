// Package block implements the smallest LSM I/O unit: a checksum-guarded run of
// sorted key-value entries with an offset index.
package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Block is a decoded run of entries plus their start offsets.
type Block struct {
	Data    []byte   // concatenated entries
	Offsets []uint16 // start offset of each entry within Data
}

// Encode serializes the block to its on-disk representation.
func (b *Block) Encode() []byte {
	buf := make([]byte, 0, len(b.Data)+len(b.Offsets)*2+2+4)
	buf = append(buf, b.Data...)
	tmp := make([]byte, 2)
	for _, off := range b.Offsets {
		binary.LittleEndian.PutUint16(tmp, off)
		buf = append(buf, tmp...)
	}
	binary.LittleEndian.PutUint16(tmp, uint16(len(b.Offsets)))
	buf = append(buf, tmp...)
	crc := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	return append(buf, crcBuf...)
}

// Decode parses a block and verifies its checksum.
func Decode(raw []byte) (*Block, error) {
	if len(raw) < 6 {
		return nil, fmt.Errorf("block: too short (%d bytes)", len(raw))
	}
	body := raw[:len(raw)-4]
	wantCRC := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.ChecksumIEEE(body) != wantCRC {
		return nil, fmt.Errorf("block: checksum mismatch")
	}
	num := int(binary.LittleEndian.Uint16(body[len(body)-2:]))
	offStart := len(body) - 2 - num*2
	if offStart < 0 {
		return nil, fmt.Errorf("block: corrupt offset section")
	}
	offsets := make([]uint16, num)
	for i := 0; i < num; i++ {
		offsets[i] = binary.LittleEndian.Uint16(body[offStart+i*2:])
	}
	return &Block{Data: body[:offStart], Offsets: offsets}, nil
}

// entryAt decodes the key and value of entry i.
func (b *Block) entryAt(i int) (k, v []byte) {
	off := int(b.Offsets[i])
	keyLen := int(binary.LittleEndian.Uint16(b.Data[off:]))
	off += 2
	k = b.Data[off : off+keyLen]
	off += keyLen
	valLen := int(binary.LittleEndian.Uint16(b.Data[off:]))
	off += 2
	v = b.Data[off : off+valLen]
	return k, v
}

// FirstKey returns the first key in the block.
func (b *Block) FirstKey() []byte {
	k, _ := b.entryAt(0)
	return k
}
