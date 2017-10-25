package tdigest

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestBasics(t *testing.T) {
	s := newSummary(2)

	err := s.Add(1, 1)

	if err != nil {
		t.Errorf("Failed to add simple item")
	}

	if s.Add(math.NaN(), 1) == nil {
		t.Errorf("Adding math.NaN() shouldn't be allowed")
	}

	if s.Add(1, 0) == nil {
		t.Errorf("Adding count=0 shouldn't be allowed")
	}
}

func checkSorted(s *summary, t *testing.T) {
	if !sort.Float64sAreSorted(s.keys) {
		t.Fatalf("Keys are not sorted! %v", s.keys)
	}
}

func TestCore(t *testing.T) {

	testData := make(map[float64]uint32)

	const maxDataSize = 10000
	s := newSummary(maxDataSize)
	checkSorted(s, t)

	if s.Len() != 0 {
		t.Errorf("Initial size should be zero regardless of capacity. Got %d", s.Len())
	}

	// construct a summary made of unique items only
	for i := 0; i < maxDataSize; i++ {
		k := rand.Float64()
		v := rand.Uint32()

		_, exists := testData[k]
		if !exists {
			_ = s.Add(k, v)
			testData[k] = v
		}
	}

	checkSorted(s, t)

	if s.Len() != len(testData) {
		t.Errorf("Got Len() == %d. Expected %d", s.Len(), len(testData))
	}

	for k, v := range testData {
		i := s.FindIndex(k)

		if i == s.Len() {
			t.Errorf("Couldn't find previously added key on summary")
			continue
		}

		if s.keys[i] != k || s.counts[i] != v {
			t.Errorf("Wanted to find {%.4f,%d}, but found {%.4f,%d} instead", k, v, s.keys[i], s.counts[i])
		}
	}
}

func TestSetAtNeverBreaksSorting(t *testing.T) {
	s := newSummary(10)

	for _, i := range []float64{10, 10, 10, 10, 10} {
		s.Add(i, 1)
	}

	s.setAt(0, 30, 1)
	checkSorted(s, t)

	s.setAt(s.Len()-1, 0, 1)
	checkSorted(s, t)

	s.setAt(3, 10.1, 1)
	checkSorted(s, t)

	s.setAt(3, 9.9, 1)
	checkSorted(s, t)

}

func TestIterate(t *testing.T) {

	s := newSummary(10)
	for _, i := range []uint32{1, 2, 3, 4, 5, 6} {
		_ = s.Add(float64(i), i*10)
	}

	c := 0
	s.Iterate(func(i centroid) bool {
		c++
		return false
	})

	if c != 1 {
		t.Errorf("Iterate must exit early if the closure returns false")
	}

	var tot uint32
	s.Iterate(func(i centroid) bool {
		tot += i.count
		return true
	})

	if tot != 210 {
		t.Errorf("Iterate must walk through the whole data if it always returns true")
	}
}

func TestFloorSum(t *testing.T) {
	s := newSummary(100)
	var total uint32
	for i := 0; i < 100; i++ {
		count := uint32(rand.Intn(10) + 1)
		s.Add(rand.Float64(), count)
		total += count
	}

	idx, _ := s.FloorSum(-1)
	if idx != -1 {
		t.Errorf("Expected no centroid to satisfy -1 but got index=%d", idx)
	}

	for i := float64(0); i < float64(total)+10; i++ {
		node, _ := s.FloorSum(i)
		if s.HeadSum(node) > i {
			t.Errorf("headSum(%d)=%.0f (>%.0f)", node, s.HeadSum(node), i)
		}
		if node+1 < s.Len() && s.HeadSum(node+1) <= i {
			t.Errorf("headSum(%d)=%.0f (>%.0f)", node+1, s.HeadSum(node+1), i)
		}
	}
}

func TestFloor(t *testing.T) {
	s := newSummary(200)
	for i := float64(0); i < 101; i++ {
		s.Add(i/2.0, 1)
	}

	if s.Floor(-30) != -1 {
		t.Errorf("Shouldn't have found a floor index. Got %d", s.Floor(-30))
	}

	for i := 0; i < s.Len(); i++ {
		m := s.keys[i]
		f := s.keys[s.Floor(m+0.1)]
		if m != f {
			t.Errorf("Erm, %.4f != %.4f", m, f)
		}
	}
}

func TestAdjustLeftRight(t *testing.T) {

	keys := []float64{1, 2, 3, 4, 9, 5, 6, 7, 8}
	counts := []uint32{1, 2, 3, 4, 9, 5, 6, 7, 8}

	s := summary{keys: keys, counts: counts}

	s.adjustRight(4)

	if !sort.Float64sAreSorted(s.keys) || s.counts[4] != 5 {
		t.Errorf("adjustRight should have fixed the keys/counts state. %v %v", s.keys, s.counts)
	}

	keys = []float64{1, 2, 3, 4, 0, 5, 6, 7, 8}
	counts = []uint32{1, 2, 3, 4, 0, 5, 6, 7, 8}

	s = summary{keys: keys, counts: counts}
	s.adjustLeft(4)

	if !sort.Float64sAreSorted(s.keys) || s.counts[4] != 4 {
		t.Errorf("adjustLeft should have fixed the keys/counts state. %v %v", s.keys, s.counts)
	}
}
