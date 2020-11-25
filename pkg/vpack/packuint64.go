// Package vpack implement value packing function
package vpack

import (
	"errors"
	"math"
)

type PackUint64 uint64

type VPack struct {
	vType uint64
	data  []PackUint64
}

const (
	DefaultCapcity = 16
	ValueBitNum    = 32
	BlockBitNum    = 64 - ValueBitNum
)

func NewValuePack(vType uint64, capacity uint32) *VPack {
	if capacity == 0 {
		capacity = DefaultCapcity
	}

	return &VPack{vType: vType, data: make([]PackUint64, 0, capacity)}
}

func (vp VPack) Size() int {
	return len(vp.data)
}

func Pack(value uint64) PackUint64 {
	blockId := value / ValueBitNum
	bitOffset := value % ValueBitNum

	return PackUint64(blockId<<ValueBitNum | 1<<bitOffset)
}

func (v PackUint64) block() uint64 {
	return uint64(v >> ValueBitNum)
}

func (v PackUint64) bitmap() uint64 {
	return uint64((v << BlockBitNum) >> BlockBitNum)
}

func (v PackUint64) UnPack() []uint64 {
	values := make([]uint64, 0, 32)

	prefix := v.block() * ValueBitNum
	bits := v.bitmap()

	for i := 0; i < ValueBitNum; i++ {
		if bits&(1<<i) != 0 {
			values = append(values, uint64(prefix)+uint64(i))
		}
	}

	return values
}

func (vp *VPack) Add(value uint64) {
	// 先计算 value 存放的 blockId 和 value bit
	pv := Pack(value)

	if len(vp.data) == 0 {
		vp.data = append(vp.data, pv)
		return
	}

	loc := vp.location(pv)
	if loc >= 0 {
		vp.data[loc] = vp.data[loc] | pv
	} else {
		offset := uint32(math.Abs(float64(loc))) - 1
		vp.insert(offset, pv)
	}
}

func (vp *VPack) Unpack() []uint64 {
	var vList []uint64

	if len(vp.data) == 0 {
		return vList
	}

	for _, vp1 := range vp.data {
		vl := vp1.UnPack()
		if len(vl) > 0 {
			vList = append(vList, vl...)
		}
	}

	return vList
}

func (vp *VPack) location(v PackUint64) int {
	if len(vp.data) == 0 {
		return -1
	}

	block := v.block()
	low := 0
	high := len(vp.data) - 1

	for low <= high {
		mid := low + (high-low)>>2
		tmp := vp.data[mid].block()
		if tmp == block {
			return mid
		}

		if tmp < block {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return -(low + 1)
}

func (vp *VPack) insert(offset uint32, v PackUint64) {
	if offset >= uint32(len(vp.data)) {
		vp.data = append(vp.data, v)
		return
	}

	// 插入数据到offset位置
	rear := append([]PackUint64{}, vp.data[offset:]...)
	vp.data = append(vp.data[0:offset], v)
	vp.data = append(vp.data, rear...)
}

func (vp *VPack) Merge(vp1 *VPack) {
	if vp1 == nil {
		return
	}

	if vp.vType == 0 {
		vp.vType = vp1.vType
	}

	vp.data = merge(vp.data, vp1.data)
}

// 合并两个VPack为一个新的VPack
func Merge(vp1, vp2 *VPack) (*VPack, error) {
	if vp1 == nil || vp2 == nil {
		return nil, errors.New("Unsupprt merge nil vpack")
	}

	if vp1.vType != 0 && vp2.vType != 0 && vp1.vType != vp2.vType {
		return nil, errors.New("Unsupport merge two different vpack")
	}

	newPack := NewValuePack(vp1.vType, uint32(len(vp1.data)+len(vp2.data)))
	if len(vp1.data) == 0 {
		newPack.data = append(newPack.data, vp2.data...)
	} else if len(vp2.data) == 0 {
		newPack.data = append(newPack.data, vp1.data...)
	} else {
		newPack.data = merge(vp1.data, vp2.data)
	}

	return newPack, nil
}

func merge(s1, s2 []PackUint64) []PackUint64 {
	l1 := len(s1)
	l2 := len(s2)

	dest := make([]PackUint64, 0, l1+l2)

	var i, j int
	for ; i < l1 && j < l2; i++ {
		p1 := s1[i]
		merge := false
		for ; j < l2; j++ {
			p2 := s2[j]
			cmp := int64(p1.block()) - int64(p2.block())

			if cmp > 0 {
				dest = append(dest, p2)
				continue
			}

			if cmp == 0 {
				dest = append(dest, p1|p2)
				j++

				merge = true
			}

			break
		}

		if !merge {
			dest = append(dest, p1)
		}
	}

	if i < l1 {
		dest = append(dest, s1[i:]...)
	} else if j < l2 {
		dest = append(dest, s2[j:]...)
	}

	return dest
}
