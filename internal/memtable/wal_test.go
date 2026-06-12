package memtable

import (
	"path/filepath"
	"testing"
)

func TestWALBackedPutRecovers(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "7.wal")
	m, err := NewWithWAL(7, walPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("b"), []byte{}); err != nil { // tombstone
		t.Fatal(err)
	}
	if m.WALPath() != walPath {
		t.Fatalf("WALPath = %q", m.WALPath())
	}
	if err := m.CloseWAL(); err != nil {
		t.Fatal(err)
	}

	r, err := RecoverWAL(7, walPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.CloseWAL()
	if r.ID() != 7 {
		t.Fatalf("id = %d", r.ID())
	}
	if v, ok := r.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("recovered a = %q ok=%v", v, ok)
	}
	if v, ok := r.Get([]byte("b")); !ok || len(v) != 0 {
		t.Fatalf("recovered tombstone b = %q ok=%v", v, ok)
	}
}

func TestNewWithoutWALStillWorks(t *testing.T) {
	m := New(0)
	if err := m.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if m.WALPath() != "" {
		t.Fatalf("expected empty WALPath, got %q", m.WALPath())
	}
	if v, ok := m.Get([]byte("k")); !ok || string(v) != "v" {
		t.Fatalf("get = %q ok=%v", v, ok)
	}
}
