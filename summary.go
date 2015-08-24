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
	data := make([]centroid, s.tree.Len())
	i := 0
	for item := range s.iterInOrder() {
		data[i] = item.(centroid)
		i++
	}
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

func (s summary) iterInOrder() <-chan interface{} {
	channel := make(chan interface{})

	go func() {
		s.tree.AscendGreaterOrEqual(centroid{math.Inf(-1), 0}, func(i llrb.Item) bool {
			channel <- i
			return true
		})
		close(channel)
	}()
	return channel
}

func (s summary) IterInOrderWith(f llrb.ItemIterator) {
	s.tree.AscendGreaterOrEqual(centroid{math.Inf(-1), 0}, f)
}
