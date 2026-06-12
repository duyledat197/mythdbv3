package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestSizeReflectsFileLength(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(4096)
	for i := 0; i < 100; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	sst, err := b.Build(1, filepath.Join(dir, "1.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if sst.Size() <= 0 {
		t.Fatalf("size should be positive, got %d", sst.Size())
	}
	reopened, err := Open(1, filepath.Join(dir, "1.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Size() != sst.Size() {
		t.Fatalf("reopened size %d != built size %d", reopened.Size(), sst.Size())
	}
}

func TestBuilderEstimatedSizeGrows(t *testing.T) {
	b := NewBuilder(64)
	for i := 0; i < 50; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if b.EstimatedSize() == 0 {
		t.Fatal("estimated size should grow after many entries flush blocks")
	}
}
