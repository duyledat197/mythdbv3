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
func UserKey(encoded []byte) []byte {
	return encoded[:len(encoded)-tsLen]
}

// Timestamp returns the decoded timestamp of an encoded key.
func Timestamp(encoded []byte) uint64 {
	return ^binary.BigEndian.Uint64(encoded[len(encoded)-tsLen:])
}

// Compare orders encoded keys: user key ascending, then timestamp descending.
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// CompareUserKey compares only the user-key portions of two encoded keys.
func CompareUserKey(a, b []byte) int {
	return bytes.Compare(UserKey(a), UserKey(b))
}
