package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAddAndRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Kind: KindNewMemtable, ID: 0},
		{Kind: KindFlush, ID: 0},
		{Kind: KindNewMemtable, ID: 1},
		{Kind: KindCompaction, UpperLevel: 0, UpperIDs: []int{0}, LowerLevel: 1, LowerIDs: nil, NewIDs: []int{5}},
	}
	for _, r := range want {
		if err := m.AddRecord(r); err != nil {
			t.Fatal(err)
		}
	}
	m.Close()

	got, m2, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if len(got) != len(want) {
		t.Fatalf("got %d records want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Kind != want[i].Kind || got[i].ID != want[i].ID {
			t.Fatalf("record %d = %+v want %+v", i, got[i], want[i])
		}
	}
	if !reflect.DeepEqual(got[3].NewIDs, []int{5}) || !reflect.DeepEqual(got[3].UpperIDs, []int{0}) {
		t.Fatalf("compaction record = %+v", got[3])
	}
}

func TestRecoverDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, _ := Create(path)
	m.AddRecord(Record{Kind: KindNewMemtable, ID: 7})
	m.Close()
	data, _ := os.ReadFile(path)
	data[6] ^= 0xff // flip a byte inside the json payload
	os.WriteFile(path, data, 0o644)
	if _, _, err := Recover(path); err == nil {
		t.Fatal("expected checksum error")
	}
}
