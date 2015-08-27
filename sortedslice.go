package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type sortedSlice struct {
	keys   []float64
	values []interface{}
}

func newSortedSlice(initialCapacity uint) *sortedSlice {
	return &sortedSlice{
		keys:   make([]float64, 0, initialCapacity),
		values: make([]interface{}, 0, initialCapacity),
	}
}

func (s sortedSlice) Len() int {
	return len(s.keys)
}

func (s sortedSlice) String() string {
	return fmt.Sprintf("SortedSlice(size=%d, keys=%v)", len(s.keys), s.keys)
}

func (s *sortedSlice) Add(key float64, value interface{}) error {

	if math.IsNaN(key) {
		return fmt.Errorf("Key must not be NaN")
	}

	idx := s.FindIndex(key)

	if idx < len(s.keys) && s.keys[idx] == key {
		return fmt.Errorf("Duplicate key %f", key)
	}

	s.keys = append(s.keys, math.NaN())
	s.values = append(s.values, nil)

	copy(s.keys[idx+1:], s.keys[idx:])
	copy(s.values[idx+1:], s.values[idx:])

	s.keys[idx] = key
	s.values[idx] = value

	return nil
}

func (s sortedSlice) Find(x float64) interface{} {
	idx := s.FindIndex(x)

	if idx < s.Len() && s.keys[idx] == x {
		return s.values[idx]
	}

	return nil
}

func (s sortedSlice) FindIndex(x float64) int {
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

func (s *sortedSlice) Remove(x float64) interface{} {
	idx := s.FindIndex(x)

	if idx >= s.Len() || s.keys[idx] != x {
		return nil
	}

	removed := s.values[idx]

	s.keys = append(s.keys[:idx], s.keys[idx+1:]...)
	s.values = append(s.values[:idx], s.values[idx+1:]...)

	return removed
}

func (s sortedSlice) At(index int) (interface{}, error) {
	if s.Len()-1 < index {
		return nil, fmt.Errorf("Offset (%d) past slice length (%d)", index, s.Len())
	}
	if index < 0 {
		return nil, fmt.Errorf("Invalid offset: %d", index)
	}

	return s.values[index], nil
}

func (s sortedSlice) Iterate(f func(item interface{}) bool) {
	for i := 0; i < s.Len(); i++ {
		if !f(s.values[i]) {
			break
		}
	}
}
