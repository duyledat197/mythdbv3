package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaChecksumRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	for i := 0; i < 40; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte("v"))
	}
	if _, err := b.Build(1, path); err != nil {
		t.Fatal(err)
	}
	sst, err := Open(1, path)
	if err != nil {
		t.Fatalf("clean open failed: %v", err)
	}
	sst.Close()
}

func TestMetaChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	for i := 0; i < 40; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte("v"))
	}
	sst, _ := b.Build(1, path)
	sst.Close()

	data, _ := os.ReadFile(path)
	size := int64(len(data))
	bloomOff := int64(binary.LittleEndian.Uint32(data[size-4:]))
	metaOff := int64(binary.LittleEndian.Uint32(data[bloomOff-4:]))
	data[metaOff] ^= 0xff // corrupt the first meta byte
	os.WriteFile(path, data, 0o644)

	if _, err := Open(1, path); err == nil {
		t.Fatal("expected meta checksum error")
	}
}
