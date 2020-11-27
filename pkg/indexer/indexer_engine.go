package pkg

import "sync/atomic"

type SearchRule interface {
	Attr(key string) (uint64, error)
	Filter()
}

type IndexRule interface {
	Attr(key string) (int64, uint64, error)
}

type Analyzer interface {
	Search(SearchRule) ([]uint64, error)
}

type dispatcher struct {
}

type storer struct {
}

type sequencer struct {
	seq uint64
}

func (s *sequencer) Get() uint64 {
	return atomic.AddUint64(&s.seq, 1)
}

func (s *sequencer) InitSequence(initSeq uint64) {
	atomic.StoreUint64(&s.seq, initSeq)
}

type engine struct {
	dispatcher dispatcher
	storer     storer

	sequencer sequencer

	indexerNum uint8
	indexer    *Indexer
}

func NewIndexerEngine() (Analyzer, error) {
	e := &engine{}

	indexer, err := Builder().
		AddAttrItem("sip", 32, 0).
		AddAttrItem("dip", 32, 0).
		AddAttrItem("svc", 32, 0).
		Build()

	if err != nil {
		return nil, err
	}

	e.indexer = indexer

	e.sequencer.InitSequence(10000)

	return e, nil
}

func (e *engine) Search(r SearchRule) ([]uint64, error) {

	return nil, nil
}

func (e *engine) Index(r IndexRule) ([]uint64, error) {
	indexer := e.indexer

	for attrName := range indexer.attrItems {
		k, v, err := r.Attr(attrName)
		if err != nil {
			return nil, err
		}

		indexer.AddAttrKeyValue(attrName, k, v)
	}

	return nil, nil
}

func (e *engine) Start() {

}

func (e *engine) Stop() {

}
