package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type centroid struct {
	mean  float64
	count uint32
	index int
}

func (c centroid) isValid() bool {
	return !math.IsNaN(c.mean) && c.count > 0
}

var invalidCentroid = centroid{mean: math.NaN(), count: 0}

type summary struct {
	means  []float64
	counts []uint32
}

func newSummary(initialCapacity uint) *summary {
	return &summary{
		means:  make([]float64, 0, initialCapacity),
		counts: make([]uint32, 0, initialCapacity),
	}
}

func (s summary) Len() int {
	return len(s.means)
}

func (s *summary) Add(key float64, value uint32) error {

	if math.IsNaN(key) {
		return fmt.Errorf("Key must not be NaN")
	}

	if value == 0 {
		return fmt.Errorf("Count must be >0")
	}

	idx := s.FindInsertionIndex(key)

	s.means = append(s.means, math.NaN())
	s.counts = append(s.counts, 0)

	copy(s.means[idx+1:], s.means[idx:])
	copy(s.counts[idx+1:], s.counts[idx:])

	s.means[idx] = key
	s.counts[idx] = value

	return nil
}

func (s summary) Floor(x float64) int {
	return sort.Search(len(s.means), func(i int) bool {
		return s.means[i] >= x
	}) - 1
}

// Always insert to the right
func (s summary) FindInsertionIndex(x float64) int {
	return sort.Search(len(s.means), func(i int) bool {
		return s.means[i] > x
	})
}

func (s summary) HeadSum(index int) (sum float64) {
	for i := 0; i < index; i++ {
		sum += float64(s.counts[i])
	}
	return sum
}

func (s summary) FindIndex(x float64) int {
	idx := sort.Search(len(s.means), func(i int) bool {
		return s.means[i] >= x
	})
	if idx < s.Len() && s.means[idx] == x {
		return idx
	}
	return s.Len()
}

func (s summary) Iterate(f func(c centroid) bool) {
	for i := 0; i < s.Len(); i++ {
		if !f(centroid{s.means[i], s.counts[i], i}) {
			break
		}
	}
}

func (s summary) Data() []centroid {
	data := make([]centroid, 0, s.Len())
	s.Iterate(func(c centroid) bool {
		data = append(data, c)
		return true
	})
	return data
}

// return the index of the last item which the sum of counts
// of items before it is less than or equal to `sum`. -1 in
// case no centroid satisfies the requirement.
// Since it's cheap, this also returns the `HeadSum` until
// the found index (i.e. cumSum = HeadSum(FloorSum(x)))
func (s summary) FloorSum(sum float64) (index int, cumSum float64) {
	index = -1
	for i := 0; i < s.Len(); i++ {
		if cumSum <= sum {
			index = i
		} else {
			break
		}
		cumSum += float64(s.counts[i])
	}
	if index != -1 {
		cumSum -= float64(s.counts[index])
	}
	return index, cumSum
}

func (s *summary) setAt(index int, mean float64, count uint32) {
	s.means[index] = mean
	s.counts[index] = count
	s.adjustRight(index)
	s.adjustLeft(index)
}

func (s *summary) adjustRight(index int) {
	for i := index + 1; i < len(s.means) && s.means[i-1] > s.means[i]; i++ {
		s.means[i-1], s.means[i] = s.means[i], s.means[i-1]
		s.counts[i-1], s.counts[i] = s.counts[i], s.counts[i-1]
	}
}

func (s *summary) adjustLeft(index int) {
	for i := index - 1; i >= 0 && s.means[i] > s.means[i+1]; i-- {
		s.means[i], s.means[i+1] = s.means[i+1], s.means[i]
		s.counts[i], s.counts[i+1] = s.counts[i+1], s.counts[i]
	}
}
