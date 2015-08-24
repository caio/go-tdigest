package tdigest

import (
	"math"

	"github.com/petar/GoLLRB/llrb"
)

type Summary struct {
	tree *llrb.LLRB
}

func (c Centroid) Less(than llrb.Item) bool {
	return c.mean < than.(Centroid).mean
}

func newSummary() *Summary {
	s := Summary{tree: llrb.New()}
	return &s
}

func (s Summary) Len() int {
	return s.tree.Len()
}

func (s Summary) Min() Centroid {
	return s.tree.Min().(Centroid)
}

func (s Summary) Max() Centroid {
	return s.tree.Max().(Centroid)
}

func (s *Summary) Add(c Centroid) {
	s.tree.InsertNoReplace(c)
}

func (s Summary) Data() []Centroid {
	data := make([]Centroid, s.tree.Len())
	i := 0
	for item := range s.iterInOrder() {
		data[i] = item.(Centroid)
		i++
	}
	return data
}

func (s Summary) Find(c Centroid) *Centroid {
	f := s.tree.Get(c)
	if f != nil {
		fAsCentroid := f.(Centroid)
		return &fAsCentroid
	}
	return nil
}

func (s *Summary) Delete(c Centroid) *Centroid {
	removed := s.tree.Delete(c)
	if removed != nil {
		removedAsCentroid := removed.(Centroid)
		return &removedAsCentroid
	}
	return nil
}

func (s Summary) iterInOrder() <-chan interface{} {
	channel := make(chan interface{})

	go func() {
		s.tree.AscendGreaterOrEqual(Centroid{math.Inf(-1), 0}, func(i llrb.Item) bool {
			channel <- i
			return true
		})
		close(channel)
	}()
	return channel
}

func (s Summary) IterInOrderWith(f llrb.ItemIterator) {
	s.tree.AscendGreaterOrEqual(Centroid{math.Inf(-1), 0}, f)
}
