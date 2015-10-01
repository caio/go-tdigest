package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type Centroid struct {
	Mean  float64
	Count uint32
	index int
}

func (c Centroid) isValid() bool {
	return !math.IsNaN(c.Mean) && c.Count > 0
}

func (c *Centroid) Update(x float64, weight uint32) {
	c.Count += weight
	c.Mean += float64(weight) * (x - c.Mean) / float64(c.Count)
}

var invalidCentroid = Centroid{Mean: math.NaN(), Count: 0}

type summary struct {
	keys   []float64
	counts []uint32
}

func newSummary(initialCapacity uint) *summary {
	return &summary{
		keys:   make([]float64, 0, initialCapacity),
		counts: make([]uint32, 0, initialCapacity),
	}
}

func (s summary) Len() int {
	return len(s.keys)
}

func (s *summary) Add(key float64, value uint32) error {

	if math.IsNaN(key) {
		return fmt.Errorf("Key must not be NaN")
	}

	if value == 0 {
		return fmt.Errorf("Count must be >0")
	}

	idx := s.FindIndex(key)

	if s.meanAtIndexIs(idx, key) {
		s.updateAt(idx, key, value)
		return nil
	}

	s.keys = append(s.keys, math.NaN())
	s.counts = append(s.counts, 0)

	copy(s.keys[idx+1:], s.keys[idx:])
	copy(s.counts[idx+1:], s.counts[idx:])

	s.keys[idx] = key
	s.counts[idx] = value

	return nil
}

func (s summary) Find(x float64) Centroid {
	idx := s.FindIndex(x)

	if idx < s.Len() && s.keys[idx] == x {
		return Centroid{x, s.counts[idx], idx}
	}

	return invalidCentroid
}

func (s summary) FindIndex(x float64) int {
	// FIXME When is linear scan better than binsearch()?
	//       should I even bother?
	if len(s.keys) < 30 {
		for i, item := range s.keys {
			if item >= x {
				return i
			}
		}
		return len(s.keys)
	}

	return sort.Search(len(s.keys), func(i int) bool {
		return s.keys[i] >= x
	})
}

func (s summary) At(index int) Centroid {
	if s.Len()-1 < index || index < 0 {
		return invalidCentroid
	}

	return Centroid{s.keys[index], s.counts[index], index}
}

func (s summary) Iterate(f func(c Centroid) bool) {
	for i := 0; i < s.Len(); i++ {
		if !f(Centroid{s.keys[i], s.counts[i], i}) {
			break
		}
	}
}

func (s summary) Min() Centroid {
	return s.At(0)
}

func (s summary) Max() Centroid {
	return s.At(s.Len() - 1)
}

func (s summary) Data() []Centroid {
	data := make([]Centroid, 0, s.Len())
	s.Iterate(func(c Centroid) bool {
		data = append(data, c)
		return true
	})
	return data
}

func (s summary) successorAndPredecessorItems(mean float64) (Centroid, Centroid) {
	idx := s.FindIndex(mean)
	return s.At(idx + 1), s.At(idx - 1)
}

func (s summary) ceilingAndFloorItems(mean float64) (Centroid, Centroid) {
	idx := s.FindIndex(mean)

	// Case 1: item is greater than all items in the summary
	if idx == s.Len() {
		return invalidCentroid, s.Max()
	}

	item := s.At(idx)

	// Case 2: item exists in the summary
	if item.isValid() && mean == item.Mean {
		return item, item
	}

	// Case 3: item is smaller than all items in the summary
	if idx == 0 {
		return s.Min(), invalidCentroid
	}

	return item, s.At(idx - 1)
}

func (s summary) sumUntilMean(mean float64) uint32 {
	var cumSum uint32
	for i := 0; i < len(s.keys); i++ {
		if s.keys[i] < mean {
			cumSum += s.counts[i]
		} else {
			break
		}
	}
	return cumSum
}

func (s *summary) updateAt(index int, mean float64, count uint32) {
	c := Centroid{s.keys[index], s.counts[index], index}
	c.Update(mean, count)

	oldMean := s.keys[index]
	s.keys[index] = c.Mean
	s.counts[index] = c.Count

	if c.Mean > oldMean {
		s.adjustRight(index)
	} else if c.Mean < oldMean {
		s.adjustLeft(index)
	}
}

func (s *summary) adjustRight(index int) {
	for i := index + 1; i < len(s.keys) && s.keys[i-1] > s.keys[i]; i++ {
		s.keys[i-1], s.keys[i] = s.keys[i], s.keys[i-1]
		s.counts[i-1], s.counts[i] = s.counts[i], s.counts[i-1]
	}
}

func (s *summary) adjustLeft(index int) {
	for i := index - 1; i >= 0 && s.keys[i] > s.keys[i+1]; i-- {
		s.keys[i], s.keys[i+1] = s.keys[i+1], s.keys[i]
		s.counts[i], s.counts[i+1] = s.counts[i+1], s.counts[i]
	}
}

func (s summary) meanAtIndexIs(index int, mean float64) bool {
	return index < len(s.keys) && s.keys[index] == mean
}
