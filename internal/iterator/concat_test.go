package iterator

import (
	"fmt"
	"path/filepath"
	"testing"

	"mythdb/internal/sstable"
)

// makeSST writes keys [startInclusive, endExclusive) as key%05d/val%05d.
func makeSST(t *testing.T, dir string, id, start, end int) *sstable.SsTable {
	t.Helper()
	b := sstable.NewBuilder(4096)
	for i := start; i < end; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	sst, err := b.Build(id, filepath.Join(dir, fmt.Sprintf("%d.sst", id)))
	if err != nil {
		t.Fatal(err)
	}
	return sst
}

func TestConcatSeekToFirst(t *testing.T) {
	dir := t.TempDir()
	tables := []*sstable.SsTable{
		makeSST(t, dir, 1, 0, 10),
		makeSST(t, dir, 2, 10, 20),
		makeSST(t, dir, 3, 20, 30),
	}
	it, err := NewConcatIterAndSeekToFirst(tables)
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
		it.Next()
	}
	if count != 30 {
		t.Fatalf("count=%d want 30", count)
	}
}

func TestConcatSeekToKeyCrossesSST(t *testing.T) {
	dir := t.TempDir()
	tables := []*sstable.SsTable{
		makeSST(t, dir, 1, 0, 10),
		makeSST(t, dir, 2, 10, 20),
		makeSST(t, dir, 3, 20, 30),
	}
	// key00015 lands in the middle table
	it, err := NewConcatIterAndSeekToKey(tables, []byte("key00015"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00015" {
		t.Fatalf("seek -> %q valid=%v", it.Key(), it.IsValid())
	}
	// key00009 is the last key of the first table
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key00009"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00009" {
		t.Fatalf("seek boundary -> %q", it.Key())
	}
	// past everything
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key99999"))
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatal("seek past end should be invalid")
	}
	// gap key between tables: key00010 exists (start of table 2); use a gap by
	// seeking to a key just after a table's range that does not exist but lands
	// on next table's first key.
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key00010"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00010" {
		t.Fatalf("seek next-table-first -> %q", it.Key())
	}
}

func TestConcatEmpty(t *testing.T) {
	it, err := NewConcatIterAndSeekToFirst(nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatal("empty concat should be invalid")
	}
}
