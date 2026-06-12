// Package key centralizes key ordering. Week 1 keys are plain byte slices;
// Week 3 (MVCC) will extend Compare to account for timestamps without touching
// every call site.
package key

import "bytes"

// Compare returns -1, 0, or +1 comparing a and b lexicographically.
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
