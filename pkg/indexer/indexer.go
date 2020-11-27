package pkg

import (
	"errors"
	"fmt"

	"github.com/anbien/polyer/pkg/trie"
)

type Metadata struct {
}

type attrItem struct {
	byteLen uint32
	tag     uint32
	trie    *trie.PTrie
}

type Indexer struct {
	attrItems map[string]*attrItem

	metadataTable map[int64]*Metadata
}

func newIndexer() *Indexer {
	return &Indexer{
		attrItems:     make(map[string]*attrItem),
		metadataTable: make(map[int64]*Metadata),
	}
}

type builder struct {
	indexer *Indexer
}

func Builder() *builder {
	return &builder{
		indexer: newIndexer(),
	}
}

func (b *builder) Build() (*Indexer, error) {
	if b.indexer == nil {
		return nil, errors.New("indexer is nil")
	}

	// 先检查是否符合规范
	for attrName, item := range b.indexer.attrItems {
		if item.trie == nil {
			return nil, fmt.Errorf("attribute %s trie is nil", attrName)
		}

		if isIllegalLen(item.byteLen) {
			return nil, fmt.Errorf("attribut %s bytelen(%d) is illegal", attrName, item.byteLen)
		}
	}

	return b.indexer, nil
}

func isIllegalLen(byteLen uint32) bool {
	if byteLen == 0 || byteLen > 64 || byteLen%8 != 0 {
		return true
	}

	return false
}

func (b *builder) AddAttrItem(attr string, byteLen uint32, tag uint32) *builder {
	indexer := b.indexer
	if _, ok := indexer.attrItems[attr]; ok {
		return b
	}

	attrItem := &attrItem{
		byteLen: byteLen,
		tag:     tag,
		trie:    trie.NewTrie(),
	}

	indexer.attrItems[attr] = attrItem

	return b
}

func IntXXToBytes(v int64, intLen uint32) []byte {
	l := int(intLen / 4)

	var buf = make([]byte, l)
	for i := 0; i < l; i++ {
		buf[i] = byte(v >> i)
	}

	return buf
}

func (indexer *Indexer) AddAttrKeyValue(attr string, key int64, value uint64) error {
	item, ok := indexer.attrItems[attr]
	if !ok || item == nil {
		return errors.New("not exsit the attr item in the tree")
	}
	keys := IntXXToBytes(key, item.byteLen)

	return item.trie.Put(keys, item.tag, value)
}
