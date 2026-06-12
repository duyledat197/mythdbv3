package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, err := Create(path, false)
	if err != nil {
		t.Fatal(err)
	}
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte{}) // tombstone
	w.Put([]byte("c"), []byte("3"))
	w.Close()

	recs, w2, err := Recover(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if string(recs[0].Key) != "a" || string(recs[0].Value) != "1" {
		t.Fatalf("rec0 = %q/%q", recs[0].Key, recs[0].Value)
	}
	if string(recs[1].Key) != "b" || len(recs[1].Value) != 0 {
		t.Fatalf("rec1 tombstone = %q/%q", recs[1].Key, recs[1].Value)
	}
}

func TestRecoverMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.wal")
	recs, w, err := Recover(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(recs) != 0 {
		t.Fatalf("missing file should recover 0 records, got %d", len(recs))
	}
}

func TestRecoverDropsTruncatedTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, _ := Create(path, false)
	w.Put([]byte("a"), []byte("1"))
	w.Close()
	// Append a partial (truncated) record: a keyLen header with no body.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, 99) // claims a 99-byte key that isn't there
	f.Write(hdr)
	f.Close()

	recs, w2, err := Recover(path, false)
	if err != nil {
		t.Fatalf("truncated trailing record should be dropped, got err %v", err)
	}
	defer w2.Close()
	if len(recs) != 1 || string(recs[0].Key) != "a" {
		t.Fatalf("expected 1 intact record, got %d", len(recs))
	}
}

func TestRecoverDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, _ := Create(path, false)
	w.Put([]byte("aaaa"), []byte("bbbb"))
	w.Close()
	// Flip a byte inside the (complete) record's value.
	// Layout: keyLen(4) + key(4) + valLen(4) + value(4) + crc(4) = 20 bytes.
	// Value starts at byte 12; data[12] is the first byte of the value.
	data, _ := os.ReadFile(path)
	data[12] ^= 0xff
	os.WriteFile(path, data, 0o644)

	if _, _, err := Recover(path, false); err == nil {
		t.Fatal("expected checksum error for corrupted complete record")
	}
}
