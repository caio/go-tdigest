package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type Centroid struct {
	Mean  float64
	Count uint32
	Index int
}

func (c Centroid) isValid() bool {
	return !math.IsNaN(c.Mean) && c.Count > 0
}

func (c *Centroid) Update(x float64, weight uint32) {
	c.Count += weight
	c.Mean += float64(weight) * (x - c.Mean) / float64(c.Count)
}

var invalidCentroid = Centroid{Mean: math.NaN(), Count: 0}

type Summary struct {
	Keys   []float64
	Counts []uint32
}

func newSummary(initialCapacity uint) *Summary {
	return &Summary{
		Keys:   make([]float64, 0, initialCapacity),
		Counts: make([]uint32, 0, initialCapacity),
	}
}

func (s Summary) Len() int {
	return len(s.Keys)
}

func (s *Summary) Add(key float64, value uint32) error {

	if math.IsNaN(key) {
		return fmt.Errorf("Key must not be NaN")
	}

	if value == 0 {
		return fmt.Errorf("Count must be >0")
	}

	idx := s.FindIndex(key)

	if s.MeanAtIndexIs(idx, key) {
		s.updateAt(idx, key, value)
		return nil
	}

	s.Keys = append(s.Keys, math.NaN())
	s.Counts = append(s.Counts, 0)

	copy(s.Keys[idx+1:], s.Keys[idx:])
	copy(s.Counts[idx+1:], s.Counts[idx:])

	s.Keys[idx] = key
	s.Counts[idx] = value

	return nil
}

func (s Summary) Find(x float64) Centroid {
	idx := s.FindIndex(x)

	if idx < s.Len() && s.Keys[idx] == x {
		return Centroid{x, s.Counts[idx], idx}
	}

	return invalidCentroid
}

func (s Summary) FindIndex(x float64) int {
	// FIXME When is linear scan better than binsearch()?
	//       should I even bother?
	if len(s.Keys) < 30 {
		for i, item := range s.Keys {
			if item >= x {
				return i
			}
		}
		return len(s.Keys)
	}

	return sort.Search(len(s.Keys), func(i int) bool {
		return s.Keys[i] >= x
	})
}

func (s Summary) At(index int) Centroid {
	if s.Len()-1 < index || index < 0 {
		return invalidCentroid
	}

	return Centroid{s.Keys[index], s.Counts[index], index}
}

func (s Summary) Iterate(f func(c Centroid) bool) {
	for i := 0; i < s.Len(); i++ {
		if !f(Centroid{s.Keys[i], s.Counts[i], i}) {
			break
		}
	}
}

func (s Summary) Min() Centroid {
	return s.At(0)
}

func (s Summary) Max() Centroid {
	return s.At(s.Len() - 1)
}

func (s Summary) Data() []Centroid {
	data := make([]Centroid, 0, s.Len())
	s.Iterate(func(c Centroid) bool {
		data = append(data, c)
		return true
	})
	return data
}

func (s Summary) successorAndPredecessorItems(mean float64) (Centroid, Centroid) {
	idx := s.FindIndex(mean)
	return s.At(idx + 1), s.At(idx - 1)
}

func (s Summary) CeilingAndFloorItems(mean float64) (Centroid, Centroid) {
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

func (s Summary) sumUntilMean(mean float64) uint32 {
	var cumSum uint32
	for i := 0; i < len(s.Keys); i++ {
		if s.Keys[i] < mean {
			cumSum += s.Counts[i]
		} else {
			break
		}
	}
	return cumSum
}

func (s *Summary) updateAt(index int, mean float64, count uint32) {
	c := Centroid{s.Keys[index], s.Counts[index], index}
	c.Update(mean, count)

	oldMean := s.Keys[index]
	s.Keys[index] = c.Mean
	s.Counts[index] = c.Count

	if c.Mean > oldMean {
		s.adjustRight(index)
	} else if c.Mean < oldMean {
		s.adjustLeft(index)
	}
}

func (s *Summary) adjustRight(index int) {
	for i := index + 1; i < len(s.Keys) && s.Keys[i-1] > s.Keys[i]; i++ {
		s.Keys[i-1], s.Keys[i] = s.Keys[i], s.Keys[i-1]
		s.Counts[i-1], s.Counts[i] = s.Counts[i], s.Counts[i-1]
	}
}

func (s *Summary) adjustLeft(index int) {
	for i := index - 1; i >= 0 && s.Keys[i] > s.Keys[i+1]; i-- {
		s.Keys[i], s.Keys[i+1] = s.Keys[i+1], s.Keys[i]
		s.Counts[i], s.Counts[i+1] = s.Counts[i+1], s.Counts[i]
	}
}

func (s Summary) MeanAtIndexIs(index int, mean float64) bool {
	return index < len(s.Keys) && s.Keys[index] == mean
}
