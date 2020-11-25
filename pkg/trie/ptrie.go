package trie

import (
	"errors"
	"math"
	"polyer/pkg/vpack"
)

type PTrie struct {
	root PTrieNode
}

func NewTrie() *PTrie {
	return &PTrie{}
}

func (pt *PTrie) Put(key []byte, vType uint64, value uint64) error {
	// 找到 value 插入的位置
	// 1. 如果offset >= 0，说明找到了具体的插入节点(offset指向的节点), chunk/parent都不为空
	//    1.1 如果 remainKey 不为空，则需要分裂插入节点(offset指向的节点)
	//    1.2 如果 remainKey 为空，则直接value加入到插入节点(offset指向的节点)
	// 2. 如果offset < 0, 说明找到了插入位置， 需要新建节点插入到该位置, parent不为空，chunk 可能存在或不存在
	//    2.1 如果chunk不存在，则新建chunk，然后新建节点加入到chunk
	//    2.2 如果chunk存在，然后新建节点加入到chunk
	parent, chunk, offset, remainKey := pt.location(key)
	if parent == nil {
		return errors.New("未找到可插入的位置")
	}

	if offset >= 0 {
		node := chunk.nodes[offset]
		if len(remainKey) == 0 {
			// 将之插入到node中
			node.Add(vType, value)
		} else { // 需要进行分裂处理
			commOffset := node.PrefixOffset(remainKey)
			splitChunk := splitNode(node, commOffset)

			newNode := NewPTrieNode()
			newNode.SetKey(remainKey[commOffset+1:])
			newNode.Add(vType, value)
			splitChunk.AddNode(newNode)
		}
	} else {
		if chunk == nil {
			chunk = NewTrieChunk()
			parent.next = chunk
			chunk.parent = parent
			offset = -1
		}

		newNode := NewPTrieNode()
		newNode.SetKey(remainKey)
		newNode.Add(vType, value)

		index := int(math.Abs(float64(offset)) - 1)
		chunk.InsertNode(index, newNode)
	}
	return nil
}

func (pt *PTrie) Get(key []byte) []uint64 {
	parent, chunk, offset, remainKey := pt.location(key)
	if parent == nil || offset < 0 || chunk == nil || len(remainKey) > 0 {
		return []uint64{}
	}

	node := chunk.nodes[offset]

	return node.vPack.Unpack()
}

func compare(start, end []byte) int {
	var i = 0
	for ; i < len(start) && i < len(end); i++ {
		if start[i] < end[i] {
			return -1
		} else if start[i] > end[i] {
			return 1
		}
	}

	if i < len(start) {
		return 1
	}

	if i < len(end) {
		return -1
	}

	return 0
}

func recordBoundNode(chunk *PTrieChunk, index int, pack *vpack.VPack) {
	node := chunk.nodes[index]

	if node.vPack == nil || node.vPack.Size() == 0 {
		return
	}

	pack.Merge(node.vPack)

	return
}

func recordNode(chunk *PTrieChunk, index int, pack *vpack.VPack) {
	recordBoundNode(chunk, index, pack)

	if chunk.nodes[index].next == nil {
		return
	}

	chunk = chunk.nodes[index].next
	for i := 0; i < len(chunk.nodes); i++ {
		recordRangeNode(chunk, 0, len(chunk.nodes)-1, pack)
	}
}

func recordRangeNode(chunk *PTrieChunk, start, end int, pack *vpack.VPack) {
	for i := start; i <= end; i++ {
		recordNode(chunk, i, pack)
	}
}

func (pt *PTrie) RangeQuery(start, end []byte) ([]uint64, error) {
	// 确保阐述的是范围
	ret := compare(start, end)
	if ret > 0 {
		return nil, errors.New("不是合法的范围")
	}

	if ret == 0 {
		return pt.Get(start), nil
	}

	// 先找左边界
	leftBound := pt.LeftBound(start)
	rightBound := pt.RightBound(end)

	return pt.rangeQuery(leftBound, rightBound), nil
}

func (pt *PTrie) rangeQuery(left, right []*BoundNode) []uint64 {
	newPack := &vpack.VPack{}

	var i = 0

	for ; i < len(left) && i < len(right); i++ {
		lNode := left[i]
		rNode := right[i]

		if lNode.chunk != rNode.chunk {
			break
		}

		// 记录左节点
		recordBoundNode(lNode.chunk, lNode.offset, newPack)

		if lNode.offset != rNode.offset {
			// 记录右节点
			recordBoundNode(rNode.chunk, rNode.offset, newPack)
			// 记录中间节点
			if lNode.offset+1 <= rNode.offset-1 {
				recordRangeNode(lNode.chunk, lNode.offset+1, rNode.offset-1, newPack)
			}
		}
	}

	if i < len(left) {
		for _, lNode := range left[i:] {
			// 记录左节点
			recordBoundNode(lNode.chunk, lNode.offset, newPack)
			// 记录中间节点
			if lNode.offset+1 <= len(lNode.chunk.nodes)-1 {
				recordRangeNode(lNode.chunk, lNode.offset+1, len(lNode.chunk.nodes)-1, newPack)
			}
		}
	}

	if i < len(right) {
		for _, rNode := range right[i:] {
			// 记录右节点
			recordBoundNode(rNode.chunk, rNode.offset, newPack)
			// 记录中间节点
			if rNode.offset-1 >= 0 {
				recordRangeNode(rNode.chunk, 0, rNode.offset-1, newPack)
			}
		}
	}

	return newPack.Unpack()
}

type BoundNode struct {
	chunk  *PTrieChunk
	offset int
}

func LeftBoundNext(chunk *PTrieChunk, index int, bounds []*BoundNode) []*BoundNode {
	if len(bounds) == 0 {
		return bounds[0:0]
	}

	// 回退到上一层的下一个节点,记录下来
	for j := len(bounds) - 1; j >= 0; j-- {
		node := bounds[j]
		if node.offset+1 < len(chunk.nodes) {
			node.offset += 1
			return bounds
		}

		bounds = bounds[0:j]
	}

	return bounds
}

func RightBoundPrev(chunk *PTrieChunk, index int, bounds []*BoundNode) []*BoundNode {
	if len(bounds) == 0 {
		return bounds[0:0]
	}

	// 回退到上一层的上一个节点,记录下来
	for j := len(bounds) - 1; j >= 0; j-- {
		node := bounds[j]
		if node.offset-1 >= 0 {
			node.offset -= 1
			return bounds
		}

		bounds = bounds[0:j]
	}

	return bounds
}

func (pt *PTrie) LeftBound(key []byte) []*BoundNode {
	var bounds = make([]*BoundNode, 0, len(key))

	parent := &pt.root
	chunk := parent.next

	remainKey := key
	for chunk != nil {
		offset := chunk.location(remainKey)
		if offset < 0 {
			index := int(math.Abs(float64(offset)) - 1)
			if index >= len(chunk.nodes)-1 {
				bounds = LeftBoundNext(chunk, index, bounds)
			} else {
				bounds = leftBoundAll(chunk, index, bounds)
			}
			return bounds
		} else {
			currNode := chunk.nodes[offset]
			commOffset := currNode.PrefixOffset(remainKey)
			if len(currNode.key) > commOffset+1 {
				ret := compare(remainKey[commOffset+1:], currNode.key[commOffset+1:])
				if ret > 0 && offset >= len(chunk.nodes)-1 {
					bounds = LeftBoundNext(chunk, offset, bounds)
				} else {
					bounds = leftBoundAll(chunk, offset, bounds)
				}
				return bounds
			}

			bounds = append(bounds, &BoundNode{
				chunk:  chunk,
				offset: offset,
			})

			remainKey = remainKey[commOffset+1:]
			if len(remainKey) == 0 {
				break
			}

			parent = currNode
			chunk = currNode.next
		}
	}

	return bounds
}

func leftBoundAll(chunk *PTrieChunk, index int, bounds []*BoundNode) []*BoundNode {
	bounds = append(bounds, &BoundNode{
		chunk:  chunk,
		offset: index,
	})

	node := chunk.nodes[index]
	for node.next != nil {
		c := node.next
		bounds = append(bounds, &BoundNode{
			chunk:  c,
			offset: 0,
		})

		node = c.nodes[0]
	}

	return bounds
}

func rightBoundAll(chunk *PTrieChunk, index int, bounds []*BoundNode) []*BoundNode {
	bounds = append(bounds, &BoundNode{
		chunk:  chunk,
		offset: index,
	})

	node := chunk.nodes[index]
	for node.next != nil {
		c := node.next
		bounds = append(bounds, &BoundNode{
			chunk:  c,
			offset: len(c.nodes) - 1,
		})

		node = c.nodes[len(c.nodes)-1]
	}

	return bounds
}

func (pt *PTrie) RightBound(key []byte) []*BoundNode {
	var bounds = make([]*BoundNode, 0, len(key))

	parent := &pt.root
	chunk := parent.next

	remainKey := key
	for chunk != nil {
		offset := chunk.location(remainKey)
		if offset < 0 {
			index := int(math.Abs(float64(offset)) - 1)
			if index == 0 {
				bounds = RightBoundPrev(chunk, index, bounds)
			} else {
				bounds = rightBoundAll(chunk, index-1, bounds)
			}
			return bounds
		} else {
			currNode := chunk.nodes[offset]
			commOffset := currNode.PrefixOffset(remainKey)
			if len(currNode.key) > commOffset+1 {
				ret := compare(remainKey[commOffset+1:], currNode.key[commOffset+1:])
				if ret < 0 && offset == 0 {
					bounds = RightBoundPrev(chunk, offset, bounds)
				} else {
					bounds = rightBoundAll(chunk, offset, bounds)
				}
				return bounds
			}

			bounds = append(bounds, &BoundNode{
				chunk:  chunk,
				offset: offset,
			})

			remainKey = remainKey[commOffset+1:]
			if len(remainKey) == 0 {
				break
			}
			parent = currNode
			chunk = currNode.next
		}
	}

	return bounds
}

func splitNode(node *PTrieNode, splitOffset int) *PTrieChunk {
	oldChunk := node.next

	// 分新节点，并加入到新的chunk中
	splitNode := NewPTrieNode()
	splitNode.SetKey(node.key[splitOffset+1:])
	splitNode.vPack = node.vPack
	if oldChunk != nil {
		splitNode.next = oldChunk
		oldChunk.parent = splitNode
	}

	splitChunk := NewTrieChunk()
	splitChunk.AddNode(splitNode)

	node.next = splitChunk
	node.next.parent = node
	node.SetKey(node.key[0 : splitOffset+1])
	node.vPack = nil

	return splitChunk
}

func (pt *PTrie) location(key []byte) (*PTrieNode, *PTrieChunk, int, []byte) {
	parent := &pt.root
	chunk := parent.next

	remainKey := key
	for chunk != nil {
		offset := chunk.location(remainKey)
		if offset < 0 {
			return parent, chunk, offset, remainKey
		}

		currNode := chunk.nodes[offset]
		commOffset := currNode.PrefixOffset(remainKey)
		if len(currNode.key) > commOffset+1 {
			return parent, chunk, offset, remainKey
		}

		remainKey = remainKey[commOffset+1:]
		if len(remainKey) == 0 {
			return parent, chunk, offset, remainKey
		}
		parent = currNode
		chunk = currNode.next
	}

	return parent, nil, -1, remainKey
}
