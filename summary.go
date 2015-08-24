package tdigest

import (
	"math"

	"github.com/petar/GoLLRB/llrb"
)

type Summary struct {
	tree *llrb.LLRB
}

func (c centroid) Less(than llrb.Item) bool {
	return c.mean < than.(centroid).mean
}

func newSummary() *Summary {
	s := Summary{tree: llrb.New()}
	return &s
}

func (s Summary) Len() int {
	return s.tree.Len()
}

func (s Summary) Min() centroid {
	return s.tree.Min().(centroid)
}

func (s Summary) Max() centroid {
	return s.tree.Max().(centroid)
}

func (s *Summary) Add(c centroid) {
	s.tree.InsertNoReplace(c)
}

func (s Summary) Data() []centroid {
	data := make([]centroid, s.tree.Len())
	i := 0
	for item := range s.iterInOrder() {
		data[i] = item.(centroid)
		i++
	}
	return data
}

func (s Summary) Find(c centroid) *centroid {
	f := s.tree.Get(c)
	if f != nil {
		fAsCentroid := f.(centroid)
		return &fAsCentroid
	}
	return nil
}

func (s *Summary) Delete(c centroid) *centroid {
	removed := s.tree.Delete(c)
	if removed != nil {
		removedAsCentroid := removed.(centroid)
		return &removedAsCentroid
	}
	return nil
}

func (s Summary) iterInOrder() <-chan interface{} {
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

func (s Summary) IterInOrderWith(f llrb.ItemIterator) {
	s.tree.AscendGreaterOrEqual(centroid{math.Inf(-1), 0}, f)
}
