// Command mythdb is a tiny demo driving the LSM engine end to end.
package main

import (
	"fmt"
	"log"
	"os"

	"mythdb/internal/lsm"
)

func main() {
	dir, err := os.MkdirTemp("", "mythdb-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := lsm.Open(lsm.Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		v := []byte(fmt.Sprintf("value%02d", i))
		if err := s.Put(k, v); err != nil {
			log.Fatal(err)
		}
	}

	// Force the data through freeze + flush so reads come from an SST.
	if err := s.ForceFreezeMemtable(); err != nil {
		log.Fatal(err)
	}
	if err := s.ForceFlushNextImmMemtable(); err != nil {
		log.Fatal(err)
	}

	s.Delete([]byte("key03"))
	s.Put([]byte("key05"), []byte("UPDATED"))

	fmt.Println("== Get ==")
	for _, k := range []string{"key00", "key03", "key05", "key09", "missing"} {
		v, ok, err := s.Get([]byte(k))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => found=%v value=%q\n", k, ok, v)
	}

	fmt.Println("== Scan [key02, key08) ==")
	it, err := s.Scan([]byte("key02"), []byte("key08"))
	if err != nil {
		log.Fatal(err)
	}
	for it.IsValid() {
		fmt.Printf("%s => %s\n", it.Key(), it.Value())
		it.Next()
	}
}
