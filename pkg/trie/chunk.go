package trie

import (
	"math"
	"polyer/pkg/vpack"
)

type PTrieChunk struct {
	nodes []*PTrieNode

	parent *PTrieNode
}

func NewTrieChunk() *PTrieChunk {
	return &PTrieChunk{}
}

// AddNode 添加结点
func (pc *PTrieChunk) AddNode(node *PTrieNode) error {
	// 先查找Node的插入位置
	offset := pc.location(node.key)
	if offset >= 0 {
		tmp := pc.nodes[offset]
		pack, err := vpack.Merge(node.vPack, tmp.vPack)
		if err != nil {
			return err
		}

		tmp.vPack = pack
	}

	index := int(math.Abs(float64(offset)) - 1)
	pc.InsertNode(index, node)

	return nil
}

// 指定位置插入节点
func (pc *PTrieChunk) InsertNode(offset int, node *PTrieNode) {
	if len(pc.nodes) == 0 || len(pc.nodes)-1 <= offset {
		pc.nodes = append(pc.nodes, node)
		return
	}

	rear := append([]*PTrieNode{}, pc.nodes[offset:]...)
	pc.nodes = append(pc.nodes[:offset], node)
	pc.nodes = append(pc.nodes, rear...)
}

func (pc *PTrieChunk) location(key []byte) int {
	if len(pc.nodes) == 0 {
		return -1
	}

	low := 0
	high := len(pc.nodes) - 1
	for low <= high {
		mid := low + (high-low)>>2
		tmp := pc.nodes[mid]

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
