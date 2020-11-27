package trie

import (
	"errors"
	"log"
	"math"

	"github.com/anbien/polyer/pkg/vpack"
)

type PTrie struct {
	root PTrieNode
}

func NewTrie() *PTrie {
	trie := &PTrie{}
	trie.root.next = NewTrieChunk()
	return trie
}

func (pt *PTrie) Put(key []byte, tag uint32, value uint64) error {
	// 找到 value 插入的位置
	// 1. 如果offset >= 0，说明找到了具体的插入节点(offset指向的节点), chunk都不为空
	//    1.1 如果 remainKey 不为空，则需要分裂插入节点(offset指向的节点)
	//    1.2 如果 remainKey 为空，则直接value加入到插入节点(offset指向的节点)
	// 2. 如果offset < 0, 说明需要新建节点插入到该位置
	//    2.1 新建节点加入到chunk
	chunk, offset, remainKey := pt.location2(key)
	if chunk == nil {
		return errors.New("the trie is error")
	}

	if offset >= 0 {
		node := chunk.nodes[offset]
		if remainKey == nil {
			// 将之插入到node中
			node.Add(tag, value)
			return nil
		}

		// 需要进行分裂处理
		prefixOffset := node.PrefixOffset(remainKey)
		splitChunk := splitNode(node, prefixOffset)

		newNode := NewPTrieNode()
		newNode.SetKey(remainKey[prefixOffset+1:])
		newNode.Add(tag, value)
		splitChunk.AddNode(newNode)
	} else {
		newNode := NewPTrieNode()
		newNode.SetKey(remainKey)
		newNode.Add(tag, value)

		index := int(math.Abs(float64(offset)) - 1)
		chunk.InsertNode(index, newNode)
	}
	return nil
}

// Get 根据key查找
func (pt *PTrie) Get(key []byte) []uint64 {
	chunk, offset, remainKey := pt.location2(key)
	if remainKey != nil || offset < 0 || chunk == nil {
		return nil
	}

	node := chunk.nodes[offset]

	return node.vPack.Unpack()
}

// RangeQuery 根据key范围查找
func (pt *PTrie) RangeQuery(start, end []byte) ([]uint64, error) {
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

func (pt *PTrie) rangeQuery(left, right []*BoundNode) []uint64 {
	newPack := &vpack.VPack{}

	var depth = 0

	for ; depth < len(left) && depth < len(right); depth++ {
		lNode := left[depth]
		rNode := right[depth]

		// 检查是否分叉了
		if lNode.chunk != rNode.chunk {
			break
		}

		// 记录左节点
		recordBoundNode(lNode.chunk, lNode.offset, newPack)
		if lNode.offset == rNode.offset {
			continue
		}

		// 记录右节点
		recordBoundNode(rNode.chunk, rNode.offset, newPack)

		// 记录中间节点
		if lNode.offset+1 <= rNode.offset-1 {
			recordRangeNode(lNode.chunk, lNode.offset+1, rNode.offset-1, newPack)
		}
	}

	// 左分叉遍历
	if depth < len(left) {
		for _, lNode := range left[depth:] {
			// 记录左节点
			recordBoundNode(lNode.chunk, lNode.offset, newPack)

			// 记录中间节点
			if lNode.offset+1 <= len(lNode.chunk.nodes)-1 {
				recordRangeNode(lNode.chunk, lNode.offset+1, len(lNode.chunk.nodes)-1, newPack)
			}
		}
	}

	// 有分叉遍历
	if depth < len(right) {
		for _, rNode := range right[depth:] {
			// 记录右节点
			recordBoundNode(rNode.chunk, rNode.offset, newPack)
			// 记录中间节点
			if rNode.offset >= 1 {
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
		prefixOffset := currNode.PrefixOffset(remainKey)
		if len(currNode.key) > prefixOffset+1 {
			return parent, chunk, offset, remainKey
		}

		remainKey = remainKey[prefixOffset+1:]

		// 找到精确的节点
		if len(remainKey) == 0 {
			return parent, chunk, offset, nil
		}

		parent = currNode
		chunk = currNode.next
	}

	// 理论上不应该出现这种场景
	log.Println("error tree , need check the bug")
	return parent, nil, -1, remainKey
}

func (pt *PTrie) location2(key []byte) (*PTrieChunk, int, []byte) {
	chunk := pt.root.next

	remainKey := key
	for chunk != nil {
		offset := chunk.location(remainKey)
		if offset < 0 {
			return chunk, offset, remainKey
		}

		currNode := chunk.nodes[offset]
		prefixOffset := currNode.PrefixOffset(remainKey)
		if len(currNode.key) > prefixOffset+1 {
			return chunk, offset, remainKey
		}

		remainKey = remainKey[prefixOffset+1:]

		// 找到精确的节点
		if len(remainKey) == 0 {
			return chunk, offset, nil
		}

		chunk = currNode.next
	}

	return chunk, -1, remainKey
}
