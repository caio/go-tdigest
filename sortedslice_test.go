package tdigest

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestBasics(t *testing.T) {
	s := newSortedSlice(2)

	for _, n := range []float64{12, 13, 14, 15} {
		item := s.Find(n)

		if item != nil {
			t.Errorf("Found something for non existing key %.0f: %f", n, item)
		}

		item = s.Remove(n)

		if item != nil {
			t.Errorf("Delete on empty structure returned something: %s", item)
		}
	}

	err := s.Add(1, 1)

	if err != nil {
		t.Errorf("Failed to add simple item")
	}

	if s.Add(math.NaN(), 1) == nil {
		t.Errorf("Adding math.NaN() shouldn't be allowed")
	}

	if s.Add(1, 2) == nil {
		t.Errorf("Shouldn't allow duplicate keys")
	}
}

func checkSorted(s *sortedSlice, t *testing.T) {
	if !sort.Float64sAreSorted(s.keys) {
		t.Fatalf("Keys are not sorted! %s", s.keys)
	}
}

func TestCore(t *testing.T) {

	testData := make(map[float64]float64)

	const maxDataSize = 10000
	s := newSortedSlice(maxDataSize)
	checkSorted(s, t)

	if s.Len() != 0 {
		t.Errorf("Initial size should be zero regardless of capacity. Got %d", s.Len())
	}

	for i := 0; i < maxDataSize; i++ {
		k := rand.Float64()
		v := rand.Float64()

		err := s.Add(k, v)

		if err != nil {
			_, exists := testData[k]
			if !exists {
				t.Errorf("Failed to insert %.2f even though it doesn't exist yet")
			}
		}

		testData[k] = v
	}

	checkSorted(s, t)

	if s.Len() != len(testData) {
		t.Errorf("Got Len() == %d. Expected %d", s.Len(), len(testData))
	}

	for k, v := range testData {
		if s.Find(k) != v {
			t.Errorf("Find(%.0f) returned %.0f, expected %.0f", k, s.Find(k), v)
		}
	}

	for k, v := range testData {
		deleted := s.Remove(k)
		if deleted == nil || deleted != v {
			t.Errorf("Delete(%f) returned %f, expected %f", k, deleted, v)
		}
		checkSorted(s, t)
	}

	checkSorted(s, t)

	if s.Len() != 0 {
		t.Errorf("Still have some items after attempting to remove all. %s", s)
	}
}

func TestGetAt(t *testing.T) {
	data := make(map[int]float64)
	const maxDataSize = 1000

	s := newSortedSlice(maxDataSize)

	_, err := s.At(0)

	if err == nil {
		t.Errorf("At() on an empty structure should give an error")
	}

	for i := 0; i < maxDataSize; i++ {
		data[i] = rand.Float64()
		s.Add(float64(i), data[i])
	}

	for i, v := range data {
		item, err := s.At(i)
		if item == nil || err != nil || item != v {
			t.Errorf("At(%d) = %.2f. Should've been %.2f", i, item, v)
		}
	}

	_, err = s.At(s.Len())

	if err == nil {
		t.Errorf("At() past the slice length should give an error")
	}

	_, err = s.At(-10)

	if err == nil {
		t.Errorf("At() with negative index should give an error")
	}
}

func TestIterate(t *testing.T) {

	s := newSortedSlice(10)
	for _, i := range []float64{1, 2, 3, 4, 5, 6} {
		s.Add(i, i*10)
	}

	c := 0
	s.Iterate(func(i interface{}) bool {
		c++
		return false
	})

	if c != 1 {
		t.Errorf("Iterate must exit early if the closure returns false")
	}

	var tot float64
	s.Iterate(func(i interface{}) bool {
		tot += i.(float64)
		return true
	})

	if tot != 210 {
		t.Errorf("Iterate must walk through the whole data if it always returns true")
	}
}
