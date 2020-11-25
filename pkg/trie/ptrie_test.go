package trie

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

func TestPTrie_Put(t *testing.T) {
	trie := NewTrie()

	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, 1234567)
	trie.Put(buf, 0x0001, 1234567)

	binary.BigEndian.PutUint64(buf, 1234567)
	trie.Put(buf, 0x0001, 7654321)

	binary.BigEndian.PutUint64(buf, 1236789)
	trie.Put(buf, 0x0001, 1234568)

	binary.BigEndian.PutUint64(buf, 1256789)
	trie.Put(buf, 0x0001, 8654321)

	// Get 1
	binary.BigEndian.PutUint64(buf, 1234567)
	ret := trie.Get(buf)
	fmt.Println("##########Get 1############")
	for _, r := range ret {
		fmt.Println(r)
	}

	if len(ret) != 2 || ret[0] != 1234567 || ret[1] != 7654321 {
		t.Error("No Pass")
	}

	// Get 2
	binary.BigEndian.PutUint64(buf, 1236789)
	ret = trie.Get(buf)

	fmt.Println("##########Get 2############")
	for _, r := range ret {
		fmt.Println(r)
	}

	if len(ret) != 1 || ret[0] != 1234568 {
		t.Error("No Pass")
	}

	// Get 3
	binary.BigEndian.PutUint64(buf, 1256789)
	ret = trie.Get(buf)
	fmt.Println("##########Get 3############")
	for _, r := range ret {
		fmt.Println(r)
	}

	if len(ret) != 1 || ret[0] != 8654321 {
		t.Error("No Pass")
	}
}

func TestPTrie_RangeQuery(t *testing.T) {
	trie := NewTrie()
	var numbers uint64 = 20000
	var i uint64 = 0

	for ; i < numbers; i++ {
		ip := uint64(rand.Uint32())
		//fmt.Println(ip)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, ip)
		trie.Put(buf, 1, i+1)
	}

	fmt.Println("Put all key/value")

	buf1 := make([]byte, 8)
	buf2 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf1, 0x0)
	binary.BigEndian.PutUint64(buf2, 0xffffffffffffffff)

	ret, err := trie.RangeQuery(buf1, buf2)
	if err != nil || len(ret) != int(numbers) {
		t.Error("No Pass")
	}

	fmt.Println("RangeQuery = ", len(ret))
	//for _, r := range ret {
	//	fmt.Println(r)
	//}

}
