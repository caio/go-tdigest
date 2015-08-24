package tdigest

import (
	"bytes"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"testing"
)

func TestCentroid(t *testing.T) {
	t.Parallel()

	c1 := Centroid{mean: 0.4, count: 1}
	c2 := Centroid{mean: 0.4, count: 1}
	c3 := Centroid{mean: 0.4, count: 2}

	if c1.Equals(c2) != c2.Equals(c1) {
		t.Errorf("Equality is not commutative: c1=%s c2=%s", c1, c2)
	}

	if !c1.Equals(c2) {
		t.Errorf("C1 (%s) should be equals to C2 (%s)", c1, c2)
	}

	if c1.Equals(c3) != false {
		t.Errorf("C1 (%s) should NOT be equals to C2 (%s)", c1, c3)
	}

	countBefore := c1.count
	c1.Update(1, 1)

	if c1.count <= countBefore || c1.count != countBefore+1 {
		t.Errorf("Update didn't do what was expected to C1 (%s)", c1)
	}
}

func TestCeilingAndFloor(t *testing.T) {
	t.Parallel()
	tdigest := New(100)

	ceil, floor := tdigest.ceilingAndFloorItems(Centroid{1, 1})

	if ceil != InvalidCentroid || floor != InvalidCentroid {
		t.Errorf("Empty centroids must return invalid ceiling and floor items")
	}

	c1 := Centroid{mean: 0.4, count: 1}
	tdigest.addCentroid(c1)

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.3, 1})

	if floor != InvalidCentroid || !c1.Equals(ceil) {
		t.Errorf("Expected to find a floor and NOT find a ceiling. ceil=%s, floor=%s", ceil, floor)
	}

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.5, 1})

	if ceil != InvalidCentroid || !c1.Equals(floor) {
		t.Errorf("Expected to find a ceiling and NOT find a floor. ceil=%s, floor=%s", ceil, floor)
	}

	c2 := Centroid{mean: 0.1, count: 2}
	tdigest.addCentroid(c2)

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.2, 1})

	if !c1.Equals(ceil) || !c2.Equals(floor) {
		t.Errorf("Expected to find a ceiling and a floor. ceil=%s, floor=%s", ceil, floor)
	}

	c3 := Centroid{mean: 0.21, count: 3}
	tdigest.addCentroid(c3)

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.2, 1})

	if !c3.Equals(ceil) || !c2.Equals(floor) {
		t.Errorf("Ceil should've shrunk. ceil=%s, floor=%s", ceil, floor)
	}

	c4 := Centroid{mean: 0.1999, count: 1}
	tdigest.addCentroid(c4)

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.2, 1})

	if !c3.Equals(ceil) || !c4.Equals(floor) {
		t.Errorf("Floor should've shrunk. ceil=%s, floor=%s", ceil, floor)
	}

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{10, 1})

	if ceil != InvalidCentroid {
		t.Errorf("Expected an invalid ceil. Got %s", ceil)
	}

	ceil, floor = tdigest.ceilingAndFloorItems(Centroid{0.0001, 12})

	if floor != InvalidCentroid {
		t.Errorf("Expected an invalid floor. Got %s", floor)
	}

	ceil, floor = tdigest.ceilingAndFloorItems(c4)

	if !floor.Equals(ceil) || floor == InvalidCentroid {
		t.Errorf("ceiling and floor of an existing item should be the item itself")
	}
}

func TestTInternals(t *testing.T) {
	t.Parallel()

	tdigest := New(100)

	tdigest.addCentroid(Centroid{mean: 0.4, count: 1})
	tdigest.addCentroid(Centroid{mean: 0.5, count: 1})

	if tdigest.summary.Len() != 2 {
		t.Errorf("Expected size 2, got %d", tdigest.summary.Len())
	}

	tdigest.addCentroid(Centroid{mean: 0.4, count: 2})
	tdigest.addCentroid(Centroid{mean: 0.4, count: 3})

	if tdigest.summary.Len() != 2 {
		t.Errorf("Adding centroids of same mean shouldn't change size")
	}

	y := tdigest.summary.Find(Centroid{mean: 0.4})

	if y.count != 6 || y.mean != 0.4 {
		t.Errorf("Adding centroids with same mean should increment the count only. Got %s", y)
	}

}

func assertDifferenceSmallerThan(tdigest *TDigest, p float64, m float64, t *testing.T) {
	tp := tdigest.Percentile(p)
	if math.Abs(tp-p) >= m {
		t.Errorf("T-Digest.Percentile(%.4f) = %.4f. Diff (%.4f) >= %.4f", p, tp, math.Abs(tp-p), m)
	}
}

func TestUniformDistribution(t *testing.T) {
	t.Parallel()

	tdigest := New(10)

	for i := 0; i < 10000; i++ {
		tdigest.Update(rand.Float64(), 1)
	}

	assertDifferenceSmallerThan(tdigest, 0.5, 0.02, t)
	assertDifferenceSmallerThan(tdigest, 0.1, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.9, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.01, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.99, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.001, 0.001, t)
	assertDifferenceSmallerThan(tdigest, 0.999, 0.001, t)
}

func TestGoRoutineLeak(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping goroutine leak test. Short flag is on")
	}

	startGoroutines := runtime.NumGoroutine()

	tdigest := New(10)

	for i := 0; i < 10000; i++ {
		tdigest.Update(rand.Float64(), 1)
	}

	endGoroutines := runtime.NumGoroutine()
	if startGoroutines < endGoroutines {
		t.Errorf("Number of goroutines increased after a series of Update calls. Started with %d, Ended with %d", startGoroutines, endGoroutines)
	}

	startGoroutines = endGoroutines

	for _, p := range []float64{0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999} {
		tdigest.Percentile(p)
	}

	endGoroutines = runtime.NumGoroutine()
	if startGoroutines < endGoroutines {
		t.Errorf("Number of goroutines increased after a series of Percentile calls. Started with %d, Ended with %d", startGoroutines, endGoroutines)
	}
}

func TestSequentialInsertion(t *testing.T) {
	t.Parallel()
	tdigest := New(10)

	// FIXME Timeout after X seconds of something?
	for i := 0; i < 10000; i++ {
		tdigest.Update(float64(i), 1)
	}
}

func TestIntegers(t *testing.T) {
	t.Parallel()
	tdigest := New(100)

	tdigest.Update(1, 1)
	tdigest.Update(2, 1)
	tdigest.Update(3, 1)

	if tdigest.Percentile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Percentile(0.5))
	}

	tdigest = New(100)

	for _, i := range []float64{1, 2, 2, 2, 2, 2, 2, 2, 3} {
		tdigest.Update(i, 1)
	}

	if tdigest.Percentile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Percentile(0.5))
	}

	var tot uint32 = 0
	for i := range tdigest.summary.iterInOrder() {
		tot += i.(Centroid).count
	}

	if tot != 9 {
		t.Errorf("Expected the centroid count to be 9, Got %d instead", tot)
	}
}

func quantile(q float64, data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}

	if q == 1 || len(data) == 1 {
		return data[len(data)-1]
	}

	index := q * (float64(len(data)) - 1)

	return data[int(index)+1]*(index-float64(int(index))) + data[int(index)]*(float64(int(index)+1)-index)
}

func TestMerge(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skipf("Skipping merge test. Short flag is on")
	}

	const numItems = 10000
	const numSubs = 5

	data := make([]float64, numItems)
	var subs [numSubs]*TDigest

	dist1 := New(10)

	for i := 0; i < numSubs; i++ {
		subs[i] = New(10)
	}

	for i := 0; i < numItems; i++ {
		num := rand.Float64()

		data[i] = num
		dist1.Update(num, 1)
		for j := 0; j < numSubs; j++ {
			subs[j].Update(num, 1)
		}
	}

	dist2 := New(10)
	for i := 0; i < numSubs; i++ {
		dist2.Merge(subs[i])
	}

	// Merge empty. Should be no-op
	dist2.Merge(New(10))

	sort.Float64s(data)

	for _, p := range []float64{0.001, 0.01, 0.1, 0.2, 0.3, 0.5} {
		q := quantile(p, data)
		p1 := dist1.Percentile(p)
		p2 := dist2.Percentile(p)

		e1 := math.Abs(p1 - q)
		e2 := math.Abs(p1 - q)

		if e2/p >= 0.3 {
			t.Errorf("Relative error for %f above threshold. q=%f p1=%f p2=%f e1=%f e2=%f", p, q, p1, p2, e1, e2)
		}
		if e2 >= 0.015 {
			t.Errorf("Absolute error for %f above threshold. q=%f p1=%f p2=%f e1=%f e2=%f", p, q, p1, p2, e1, e2)
		}
	}
}

func TestEncodeDecode(t *testing.T) {
	testUints := []uint32{0, 10, 100, 1000, 10000, 65535, 2147483647}
	buf := new(bytes.Buffer)

	for _, i := range testUints {
		encodeUint(buf, i)
	}

	readBuf := bytes.NewReader(buf.Bytes())
	for _, i := range testUints {
		j, _ := decodeUint(readBuf)

		if i != j {
			t.Errorf("Basic encode/decode failed. Got %d, wanted %d", j, i)
		}
	}
}

func TestSerialization(t *testing.T) {
	// NOTE Using a high compression value and adding few items
	//      so we don't end up compressing automatically
	t1 := New(100)
	for i := 0; i < 100; i++ {
		t1.Update(rand.Float64(), 1)
	}

	serialized, _ := t1.AsBytes()

	t2, _ := FromBytes(bytes.NewReader(serialized))

	if t1.count != t2.count || t1.summary.Len() != t2.summary.Len() || t1.compression != t2.compression {
		t.Errorf("Deserialized to something different. t1=%s t2=%s serialized=%x", t1, t2, serialized)
	}
}
