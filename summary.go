package tdigest

import (
	"math"

	"github.com/petar/GoLLRB/llrb"
)

type summary struct {
	tree *llrb.LLRB
}

func (c centroid) Less(than llrb.Item) bool {
	return c.mean < than.(centroid).mean
}

func newSummary() *summary {
	s := summary{tree: llrb.New()}
	return &s
}

func (s summary) Len() int {
	return s.tree.Len()
}

func (s summary) Min() centroid {
	return s.tree.Min().(centroid)
}

func (s summary) Max() centroid {
	return s.tree.Max().(centroid)
}

func (s *summary) Add(c centroid) {
	s.tree.InsertNoReplace(c)
}

func (s summary) Data() []centroid {
	data := make([]centroid, 0, s.tree.Len())
	s.IterInOrderWith(func(item llrb.Item) bool {
		data = append(data, item.(centroid))
		return true
	})

	return data
}

func (s summary) Find(c centroid) *centroid {
	f := s.tree.Get(c)
	if f != nil {
		fAsCentroid := f.(centroid)
		return &fAsCentroid
	}
	return nil
}

func (s *summary) Delete(c centroid) *centroid {
	removed := s.tree.Delete(c)
	if removed != nil {
		removedAsCentroid := removed.(centroid)
		return &removedAsCentroid
	}
	return nil
}

func (s summary) IterInOrderWith(f llrb.ItemIterator) {
	s.tree.AscendGreaterOrEqual(centroid{math.Inf(-1), 0}, f)
}
