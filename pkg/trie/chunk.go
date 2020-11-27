package trie

import (
	"math"

	"github.com/anbien/polyer/pkg/vpack"
)

type PTrieChunk struct {
	nodes []*PTrieNode

	parent *PTrieNode
}

func NewTrieChunk() *PTrieChunk {
	return &PTrieChunk{}
}

// AddNode 添加结点
func (tc *PTrieChunk) AddNode(node *PTrieNode) error {
	// 先查找Node的插入位置
	offset := tc.location(node.key)
	if offset >= 0 {
		tmp := tc.nodes[offset]
		pack, err := vpack.Merge(node.vPack, tmp.vPack)
		if err != nil {
			return err
		}

		tmp.vPack = pack
	}

	index := int(math.Abs(float64(offset)) - 1)
	tc.InsertNode(index, node)

	return nil
}

// 指定位置插入节点
func (tc *PTrieChunk) InsertNode(offset int, node *PTrieNode) {
	if len(tc.nodes) == 0 || len(tc.nodes)-1 <= offset {
		tc.nodes = append(tc.nodes, node)
		return
	}

	rear := append([]*PTrieNode{}, tc.nodes[offset:]...)
	tc.nodes = append(tc.nodes[:offset], node)
	tc.nodes = append(tc.nodes, rear...)
}

func (tc *PTrieChunk) location(key []byte) int {
	if len(tc.nodes) == 0 {
		return -1
	}

	low := 0
	high := len(tc.nodes) - 1
	for low <= high {
		mid := low + (high-low) >> 1
		tmp := tc.nodes[mid]
		if key[0] == tmp.key[0] {
			return mid
		}
		if key[0] < tmp.key[0] {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return -(low + 1)
}

// 合并为新的 Chunk
func Merge(c1, c2 *PTrieChunk) *PTrieChunk {
	return nil
}
