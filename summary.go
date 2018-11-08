package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type summary struct {
	means  []float64
	counts []uint32
}

func newSummary(initialCapacity int) *summary {
	s := &summary{
		means:  make([]float64, 0, initialCapacity),
		counts: make([]uint32, 0, initialCapacity),
	}
	return s
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

// This method is the hotspot when calling Add(), which in turn is called by
// Compress() and Merge().
func (s summary) HeadSum(idx int) (sum float64) {
	return float64(sumUntilIndex(s.counts, idx))
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

func (s summary) Mean(uncheckedIndex int) float64 {
	return s.means[uncheckedIndex]
}

func (s summary) Count(uncheckedIndex int) uint32 {
	return s.counts[uncheckedIndex]
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

func (s summary) ForEach(f func(float64, uint32) bool) {
	for i := 0; i < len(s.means); i++ {
		if !f(s.means[i], s.counts[i]) {
			break
		}
	}
}

func (s summary) Clone() *summary {
	return &summary{
		means:  append([]float64{}, s.means...),
		counts: append([]uint32{}, s.counts...),
	}
}

// A simple loop unroll saves a surprising amount of time.
func sumUntilIndex(s []uint32, idx int) uint64 {
	var cumSum uint64
	var i int
	for i = idx - 1; i >= 3; i -= 4 {
		cumSum += uint64(s[i])
		cumSum += uint64(s[i-1])
		cumSum += uint64(s[i-2])
		cumSum += uint64(s[i-3])
	}
	for ; i >= 0; i-- {
		cumSum += uint64(s[i])
	}
	return cumSum
}
