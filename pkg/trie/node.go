package trie

import "github.com/anbien/polyer/pkg/vpack"

type PTrieNode struct {
	key []byte

	next *PTrieChunk

	vPack *vpack.VPack
}

func NewPTrieNode() *PTrieNode {
	return &PTrieNode{}
}

func (pn *PTrieNode) SetKey(key []byte) {
	pn.key = make([]byte, 0, len(key))
	pn.key = append(pn.key, key...)
}

// 将value存储到结点中
func (pn *PTrieNode) Add(tag uint32, value uint64) {
	if pn.vPack == nil {
		pn.vPack = vpack.NewValuePack(tag, 0)
	}

	pn.vPack.Add(value)
}

func (pn *PTrieNode) PrefixOffset(key []byte) int {
	var offset = -1

	nodeKey := pn.key
	for i := 0; i < len(nodeKey) && i < len(key); i++ {
		if nodeKey[i] != key[i] {
			break
		}
		offset = i
	}

	return offset
}
