package lsm

import (
	"testing"

	"mythdb/internal/key"
	"mythdb/internal/sstable"
)

// countVersionsInSST counts how many stored versions of userKey exist in SST id.
func countVersionsInSST(t *testing.T, s *Storage, id int, userKey string) int {
	t.Helper()
	sst := s.snapshot().sstables[id]
	it, err := sstable.NewIterAndSeekToFirst(sst)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for it.IsValid() {
		if string(key.UserKey(it.Key())) == userKey {
			n++
		}
		it.Next()
	}
	return n
}
