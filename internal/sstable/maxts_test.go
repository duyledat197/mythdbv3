package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"mythdb/internal/bloom"
	"mythdb/internal/key"
)

func TestMaxTsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	// Encoded keys with increasing timestamps.
	for i := 0; i < 30; i++ {
		b.Add(key.Encode([]byte(fmt.Sprintf("k%03d", i)), uint64(i+1)), []byte("v"))
	}
	sst, err := b.Build(1, path)
	if err != nil {
		t.Fatal(err)
	}
	if sst.MaxTs() != 30 {
		t.Fatalf("built MaxTs = %d want 30", sst.MaxTs())
	}
	sst.Close()

	reopened, err := Open(1, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.MaxTs() != 30 {
		t.Fatalf("reopened MaxTs = %d want 30", reopened.MaxTs())
	}
}

func TestBloomIsOnUserKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	b.Add(key.Encode([]byte("apple"), 5), []byte("v"))
	sst, err := b.Build(1, path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()
	// MayContain takes a USER-key hash; "apple" must be reported present.
	if !sst.bloom.MayContain(bloom.Hash([]byte("apple"))) {
		t.Fatal("bloom should contain user key 'apple'")
	}
}
