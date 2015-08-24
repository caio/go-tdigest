package tdigest

import (
	"github.com/ancientlore/go-avltree"
)

type Summary struct {
	tree *avltree.Tree
}

func newSummary() *Summary {
	s := Summary{tree: avltree.New(compareCentroids, 0)}
	return &s
}

func (s Summary) Len() int {
	return s.tree.Len()
}

func (s Summary) Min() Centroid {
	return s.tree.At(0).(Centroid)
}

func (s Summary) Max() Centroid {
	return s.tree.At(s.tree.Len() - 1).(Centroid)
}

func (s *Summary) Add(c Centroid) {
	s.tree.Add(c)
}

func (s Summary) Data() []interface{} {
	return s.tree.Data()
}

func (s Summary) Find(c Centroid) *Centroid {
	f := s.tree.Find(c)
	if f != nil {
		fAsCentroid := f.(Centroid)
		return &fAsCentroid
	}
	return nil
}

func (s *Summary) Delete(c Centroid) *Centroid {
	removed := s.tree.Remove(c)
	if removed != nil {
		removedAsCentroid := removed.(Centroid)
		return &removedAsCentroid
	}
	return nil
}

func (s Summary) IterInOrder() <-chan interface{} {
	return s.tree.Iter()
}
