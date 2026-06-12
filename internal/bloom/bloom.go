// Package bloom implements a LevelDB-style bloom filter using double hashing.
package bloom

import (
	"fmt"
	"hash/fnv"
	"math"
)

// Bloom is an immutable bloom filter: a bitset plus the number of probes k.
type Bloom struct {
	filter []byte
	k      uint8
}

// Hash maps a key to a 32-bit hash used for both building and querying.
func Hash(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

// BitsPerKey computes a bits-per-key budget for a target false-positive rate.
func BitsPerKey(numEntries int, fpr float64) int {
	if numEntries == 0 {
		return 10
	}
	size := -1 * float64(numEntries) * math.Log(fpr) / (math.Ln2 * math.Ln2)
	bits := int(math.Ceil(size / float64(numEntries)))
	if bits < 1 {
		bits = 1
	}
	return bits
}

// Build constructs a filter from key hashes with the given bits-per-key budget.
func Build(hashes []uint32, bitsPerKey int) *Bloom {
	// Optimal number of probes: k = bitsPerKey * ln2.
	k := uint8(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	nbits := len(hashes) * bitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	filter := make([]byte, nbytes)
	for _, h := range hashes {
		delta := (h >> 17) | (h << 15) // rotate right 17 bits
		bitpos := h
		for j := uint8(0); j < k; j++ {
			pos := bitpos % uint32(nbits)
			filter[pos/8] |= 1 << (pos % 8)
			bitpos += delta
		}
	}
	return &Bloom{filter: filter, k: k}
}

// MayContain reports whether the key hash might be present (no false negatives).
func (b *Bloom) MayContain(h uint32) bool {
	if len(b.filter) == 0 {
		return true
	}
	nbits := uint32(len(b.filter) * 8)
	delta := (h >> 17) | (h << 15)
	bitpos := h
	for j := uint8(0); j < b.k; j++ {
		pos := bitpos % nbits
		if b.filter[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		bitpos += delta
	}
	return true
}

// Encode serializes the filter as [k(1 byte)][bitset].
func (b *Bloom) Encode() []byte {
	out := make([]byte, 0, 1+len(b.filter))
	out = append(out, b.k)
	return append(out, b.filter...)
}

// Decode parses an encoded filter.
func Decode(buf []byte) (*Bloom, error) {
	if len(buf) < 1 {
		return nil, fmt.Errorf("bloom: empty buffer")
	}
	return &Bloom{k: buf[0], filter: append([]byte(nil), buf[1:]...)}, nil
}
