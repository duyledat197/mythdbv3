// Package wal implements a write-ahead log: a sequence of CRC-checked
// (key, value) records appended before a memtable mutation.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// WAL appends records to a file.
type WAL struct {
	f          *os.File
	syncWrites bool
}

// Record is one recovered key/value pair. An empty Value is a tombstone.
type Record struct {
	Key, Value []byte
}

// Create starts a fresh (truncated) WAL at path.
func Create(path string, syncWrites bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, syncWrites: syncWrites}, nil
}

// Recover reads all intact records from path and returns a WAL positioned to
// append. A truncated/incomplete trailing record is dropped (the file is
// truncated to the last intact record). A CRC mismatch on a complete record
// returns an error. A missing file recovers zero records.
func Recover(path string, syncWrites bool) ([]Record, *WAL, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	recs, consumed, perr := parseRecords(data)
	if perr != nil {
		return nil, nil, perr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	// Drop any partial trailing bytes so future appends stay well-formed.
	if err := f.Truncate(consumed); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(consumed, 0); err != nil {
		f.Close()
		return nil, nil, err
	}
	return recs, &WAL{f: f, syncWrites: syncWrites}, nil
}

// parseRecords returns intact records and the byte offset up to which the file
// is well-formed. An incomplete trailing record stops parsing (no error); a
// complete record with a bad CRC returns an error.
func parseRecords(data []byte) ([]Record, int64, error) {
	var recs []Record
	pos := 0
	for {
		if pos+4 > len(data) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos:]))
		keyStart := pos + 4
		if keyStart+keyLen+4 > len(data) {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(data[keyStart+keyLen:]))
		valStart := keyStart + keyLen + 4
		if valStart+valLen+4 > len(data) {
			break
		}
		crcPos := valStart + valLen
		want := binary.LittleEndian.Uint32(data[crcPos:])
		if crc32.ChecksumIEEE(data[pos:crcPos]) != want {
			return recs, int64(pos), fmt.Errorf("wal: checksum mismatch at offset %d", pos)
		}
		k := append([]byte(nil), data[keyStart:keyStart+keyLen]...)
		v := append([]byte(nil), data[valStart:valStart+valLen]...)
		recs = append(recs, Record{Key: k, Value: v})
		pos = crcPos + 4
	}
	return recs, int64(pos), nil
}

// Put appends one record, writing through to the file. fsync if syncWrites.
func (w *WAL) Put(key, value []byte) error {
	buf := make([]byte, 0, 4+len(key)+4+len(value)+4)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(value)))
	buf = append(buf, value...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.syncWrites {
		return w.f.Sync()
	}
	return nil
}

// Sync flushes buffered data to stable storage.
func (w *WAL) Sync() error { return w.f.Sync() }

// Close closes the underlying file.
func (w *WAL) Close() error { return w.f.Close() }
