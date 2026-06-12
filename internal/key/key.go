// Package key centralizes key ordering and timestamped (MVCC) key encoding.
//
// A stored key is userKey || bigEndian(^ts): an 8-byte timestamp suffix, bit-
// complemented so bytes.Compare orders equal user keys by timestamp descending
// (newest version first).
package key

import (
	"bytes"
	"encoding/binary"
)

// tsLen is the size of the encoded timestamp suffix in bytes.
const tsLen = 8

const (
	// TsRangeBegin encodes the newest possible version of a user key (used as a
	// lower scan bound to include all versions).
	TsRangeBegin = ^uint64(0)
	// TsRangeEnd encodes the oldest possible version.
	TsRangeEnd = uint64(0)
)

// Encode returns userKey || bigEndian(^ts).
func Encode(userKey []byte, ts uint64) []byte {
	out := make([]byte, len(userKey)+tsLen)
	copy(out, userKey)
	binary.BigEndian.PutUint64(out[len(userKey):], ^ts)
	return out
}

// UserKey returns the user-key portion of an encoded key.
// If the key is shorter than tsLen bytes it is treated as a raw (unencoded)
// user key and returned as-is — this supports the pre-MVCC flush path where
// keys have not yet been encoded by the write path.
func UserKey(encoded []byte) []byte {
	if len(encoded) < tsLen {
		return encoded
	}
	return encoded[:len(encoded)-tsLen]
}

// Timestamp returns the decoded timestamp of an encoded key.
// Returns 0 for keys shorter than tsLen bytes (unencoded keys).
func Timestamp(encoded []byte) uint64 {
	if len(encoded) < tsLen {
		return 0
	}
	return ^binary.BigEndian.Uint64(encoded[len(encoded)-tsLen:])
}

// Compare orders encoded keys: user key ascending, then timestamp descending.
//
// It must NOT be a plain bytes.Compare of the concatenation: a shorter user key
// followed by the timestamp suffix can otherwise compare greater than a longer
// user key (e.g. "b"||suffix vs "bc"||suffix, where the suffix's high byte beats
// 'c'), which would corrupt scan ordering and bounds. So compare the user-key
// portions first, then break ties by timestamp descending (newest first).
func Compare(a, b []byte) int {
	if c := bytes.Compare(UserKey(a), UserKey(b)); c != 0 {
		return c
	}
	ta, tb := Timestamp(a), Timestamp(b)
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	default:
		return 0
	}
}

// CompareUserKey compares only the user-key portions of two encoded keys.
func CompareUserKey(a, b []byte) int {
	return bytes.Compare(UserKey(a), UserKey(b))
}
