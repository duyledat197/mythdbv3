package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
)

func buildSST(t *testing.T, n int) *SsTable {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(64) // small blocks to force multiple blocks
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		v := []byte(fmt.Sprintf("val%05d", i))
		b.Add(k, v)
	}
	sst, err := b.Build(1, path)
	if err != nil {
		t.Fatal(err)
	}
	return sst
}

func TestSSTBuildAndScan(t *testing.T) {
	sst := buildSST(t, 100)
	defer sst.Close()
	it, err := NewIterAndSeekToFirst(sst)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for it.IsValid() {
		want := fmt.Sprintf("key%05d", count)
		if string(it.Key()) != want {
			t.Fatalf("at %d got %q want %q", count, it.Key(), want)
		}
		count++
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if count != 100 {
		t.Fatalf("scanned %d want 100", count)
	}
}

func TestSSTSeekAcrossBlocks(t *testing.T) {
	sst := buildSST(t, 100)
	defer sst.Close()
	it, err := NewIterAndSeekToKey(sst, []byte("key00050"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00050" {
		t.Fatalf("seek -> %q", it.Key())
	}
	if string(it.Value()) != "val00050" {
		t.Fatalf("value=%q", it.Value())
	}
}

func TestSSTReopenFromDisk(t *testing.T) {
	sst := buildSST(t, 50)
	path := sst.path
	sst.Close()
	reopened, err := Open(1, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.MayContain([]byte("key00010")) {
		t.Fatal("bloom lost a present key after reopen")
	}
	it, err := NewIterAndSeekToKey(reopened, []byte("key00049"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00049" {
		t.Fatalf("reopened seek -> %q", it.Key())
	}
}

func TestSSTBloomAbsentKey(t *testing.T) {
	sst := buildSST(t, 50)
	defer sst.Close()
	_ = sst.MayContain([]byte("definitely-not-present-zzz"))
}
