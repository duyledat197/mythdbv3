// Package manifest is an append-only log of structural LSM changes used to
// rebuild the engine's level layout on restart.
package manifest

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
)

// RecordKind identifies a manifest record.
type RecordKind string

const (
	KindNewMemtable RecordKind = "new_memtable"
	KindFlush       RecordKind = "flush"
	KindCompaction  RecordKind = "compaction"
)

// Record is one structural change. Fields not relevant to a kind stay zero.
type Record struct {
	Kind RecordKind `json:"kind"`
	ID   int        `json:"id,omitempty"`
	// Level fields are NOT omitempty: 0 is a valid level index (an L0 source),
	// so it must be serialized explicitly rather than dropped.
	UpperLevel int   `json:"upper_level"`
	UpperIDs   []int `json:"upper_ids,omitempty"`
	LowerLevel int   `json:"lower_level"`
	LowerIDs   []int `json:"lower_ids,omitempty"`
	NewIDs     []int `json:"new_ids,omitempty"`
}

// Manifest appends records to a file.
type Manifest struct {
	f *os.File
}

// Create starts a fresh (truncated) manifest at path.
func Create(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Manifest{f: f}, nil
}

// Recover reads all records and returns a manifest positioned to append.
func Recover(path string) ([]Record, *Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	var recs []Record
	pos := 0
	for {
		if pos+4 > len(data) {
			break
		}
		n := int(binary.LittleEndian.Uint32(data[pos:]))
		jsonStart := pos + 4
		if jsonStart+n+4 > len(data) {
			break // incomplete trailing record
		}
		payload := data[jsonStart : jsonStart+n]
		want := binary.LittleEndian.Uint32(data[jsonStart+n:])
		if crc32.ChecksumIEEE(payload) != want {
			return nil, nil, fmt.Errorf("manifest: checksum mismatch at offset %d", pos)
		}
		var r Record
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, nil, fmt.Errorf("manifest: bad record at offset %d: %w", pos, err)
		}
		recs = append(recs, r)
		pos = jsonStart + n + 4
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if _, err := f.Seek(int64(pos), 0); err != nil {
		f.Close()
		return nil, nil, err
	}
	return recs, &Manifest{f: f}, nil
}

// AddRecord appends one record and fsyncs.
func (m *Manifest) AddRecord(r Record) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	buf := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
	buf = append(buf, payload...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(payload))
	if _, err := m.f.Write(buf); err != nil {
		return err
	}
	return m.f.Sync()
}

// Close closes the underlying file.
func (m *Manifest) Close() error { return m.f.Close() }
