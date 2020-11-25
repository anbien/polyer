package vpack

import (
	"fmt"
	"testing"
)

func TestPack(t *testing.T) {
	var v1 uint64 = 1234567

	pv := Pack(v1)

	ret := pv.UnPack()
	if len(ret) != 1 || ret[0] != v1 {
		t.Error("No Pass")
	}
}

func TestMerge(t *testing.T) {
	var v1 uint64 = 1234567
	var v2 uint64 = 7654321
	var v3 uint64 = 1234568
	var v4 uint64 = 8654321

	p1 := NewValuePack(1, 0)
	p2 := NewValuePack(1, 0)

	p1.Add(v1)
	p1.Add(v2)
	p2.Add(v3)
	p2.Add(v4)

	m1, err := Merge(p1, p2)
	if err != nil {
		t.Error("No Pass")
		return
	}

	ret := m1.Unpack()
	for _, tmp := range ret {
		fmt.Println(tmp)
	}
	if len(ret) != 4 || ret[0] != v1 || ret[1] != v3 || ret[2] != v2 || ret[3] != v4 {
		t.Error("No Pass")
		return
	}
}
