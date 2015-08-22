package tdigest

import (
	"math"
	"math/rand"
	"testing"
)

func TestCentroid(t *testing.T) {

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

	if y.(Centroid).count != 6 || y.(Centroid).mean != 0.4 {
		t.Errorf("Adding centroids with same mean should increment the count only. Got %s", y.(Centroid))
	}

}

func assertDifferenceSmallerThan(tdigest *TDigest, p float64, m float64, t *testing.T) {
	tp := tdigest.Percentile(p)
	if math.Abs(tp-p) >= m {
		t.Errorf("T-Digest.Percentile(%.4f) = %.4f. Diff (%.4f) >= %.4f", p, tp, math.Abs(tp-p), m)
	}
}

func TestUniformDistribution(t *testing.T) {
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

func TestSequentialInsertion(t *testing.T) {
	t.Parallel()
	tdigest := New(10)

	// FIXME Timeout after X seconds of something?
	for i := 0; i < 10000; i++ {
		tdigest.Update(float64(i), 1)
	}
}

func TestIntegers(t *testing.T) {
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

	var tot float64 = 0
	for i := range tdigest.summary.Iter() {
		tot += i.(Centroid).count
	}

	if tot != 9 {
		t.Errorf("Expected the centroid count to be 9, Got %.2f instead", tot)
	}
}
