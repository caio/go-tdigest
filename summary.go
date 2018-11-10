package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type sumCache struct {
	sums  []uint64
	valid int
}

func newSumCache(n int) *sumCache {
	return &sumCache{
		sums:  make([]uint64, n>>2),
		valid: -1,
	}
}

func (s *sumCache) Clone() *sumCache {
	if s == nil {
		return nil
	}
	return &sumCache{
		sums:  append([]uint64(nil), s.sums...),
		valid: s.valid,
	}
}

func (s *sumCache) Set(idx int, sum uint64) {
	if s == nil || idx < 4 {
		return
	}
	idx = idx>>2 - 1
	if idx == len(s.sums) {
		s.sums = append(s.sums, sum)
	} else {
		s.sums[idx] = sum
	}
	s.valid = idx
}

func (s *sumCache) Invalidate(idx int) {
	if s == nil {
		return
	}
	idx = idx>>2 - 1
	if idx-1 < s.valid {
		s.valid = idx - 1
	}
}

func (s *sumCache) Get(idx int) (int, uint64) {
	if s == nil || idx < 4 || s.valid < 0 {
		return 0, 0
	}
	idx = idx>>2 - 1
	if idx <= s.valid {
		return (idx + 1) << 2, s.sums[idx]
	}
	return (s.valid + 1) << 2, s.sums[s.valid]
}

type summary struct {
	means    []float64
	counts   []uint32
	sumCache *sumCache
}

func newSummary(initialCapacity int) *summary {
	s := &summary{
		means:  make([]float64, 0, initialCapacity),
		counts: make([]uint32, 0, initialCapacity),
	}
	return s
}

func (s *summary) Len() int {
	return len(s.means)
}

func (s *summary) Add(key float64, value uint32) error {
	if math.IsNaN(key) {
		return fmt.Errorf("Key must not be NaN")
	}
	if value == 0 {
		return fmt.Errorf("Count must be >0")
	}

	idx := s.findInsertionIndex(key)

	s.means = append(s.means, math.NaN())
	s.counts = append(s.counts, 0)

	copy(s.means[idx+1:], s.means[idx:])
	copy(s.counts[idx+1:], s.counts[idx:])

	s.means[idx] = key
	s.counts[idx] = value

	if s.sumCache != nil {
		s.sumCache.Invalidate(idx)
	} else if len(s.means) > 100 {
		s.sumCache = newSumCache(cap(s.means))
	}

	return nil
}

// Always insert to the right
func (s *summary) findInsertionIndex(x float64) int {
	// Binary search is only worthwhile if we have a lot of keys.
	if len(s.means) < 250 {
		for i, mean := range s.means {
			if mean > x {
				return i
			}
		}
		return len(s.means)
	}

	return sort.Search(len(s.means), func(i int) bool {
		return s.means[i] > x
	})
}

// This method is the hotspot when calling Add(), which in turn is called by
// Compress() and Merge().
func (s *summary) HeadSum(end int) float64 {
	i, sum := s.sumCache.Get(end)
	if i == end {
		return float64(sum)
	}

	// A simple loop unroll saves a surprising amount of time.
	for ; i < end-3; i += 4 {
		s.sumCache.Set(i, sum)
		sum += uint64(s.counts[i])
		sum += uint64(s.counts[i+1])
		sum += uint64(s.counts[i+2])
		sum += uint64(s.counts[i+3])
	}
	for ; i < end; i++ {
		sum += uint64(s.counts[i])
	}

	return float64(sum)
}

func (s *summary) Floor(x float64) int {
	return s.findIndex(x) - 1
}

func (s *summary) findIndex(x float64) int {
	// Binary search is only worthwhile if we have a lot of keys.
	if len(s.means) < 250 {
		for i, mean := range s.means {
			if mean >= x {
				return i
			}
		}
		return len(s.means)
	}

	return sort.Search(len(s.means), func(i int) bool {
		return s.means[i] >= x
	})
}

func (s *summary) Mean(uncheckedIndex int) float64 {
	return s.means[uncheckedIndex]
}

func (s *summary) Count(uncheckedIndex int) uint32 {
	return s.counts[uncheckedIndex]
}

// return the index of the last item which the sum of counts
// of items before it is less than or equal to `sum`. -1 in
// case no centroid satisfies the requirement.
// Since it's cheap, this also returns the `HeadSum` until
// the found index (i.e. cumSum = HeadSum(FloorSum(x)))
func (s *summary) FloorSum(sum float64) (index int, cumSum float64) {
	index = -1
	for i, count := range s.counts {
		if cumSum <= sum {
			index = i
		} else {
			break
		}
		cumSum += float64(count)
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

func (s *summary) ForEach(f func(float64, uint32) bool) {
	for i, mean := range s.means {
		if !f(mean, s.counts[i]) {
			break
		}
	}
}

func (s *summary) Perm(rng RNG, f func(float64, uint32) bool) {
	for _, i := range perm(rng, s.Len()) {
		if !f(s.means[i], s.counts[i]) {
			break
		}
	}
}

func (s *summary) Clone() *summary {
	return &summary{
		means:    append([]float64{}, s.means...),
		counts:   append([]uint32{}, s.counts...),
		sumCache: s.sumCache.Clone(),
	}
}

// Randomly shuffles summary contents, so they can be added to another summary
// with being pathological. Renders summary invalid.
func (s *summary) shuffle(rng RNG) {
	for i := len(s.means) - 1; i > 1; i-- {
		s.Swap(i, rng.Intn(i+1))
	}
}

// for sort.Interface
func (s *summary) Swap(i, j int) {
	s.means[i], s.means[j] = s.means[j], s.means[i]
	s.counts[i], s.counts[j] = s.counts[j], s.counts[i]
}

func (s *summary) Less(i, j int) bool {
	return s.means[i] < s.means[j]
}

func perm(rng RNG, n int) []int {
	m := make([]int, n)
	for i := 1; i < n; i++ {
		j := rng.Intn(i + 1)
		m[i] = m[j]
		m[j] = i
	}
	return m
}
