package tdigest

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// Test of tdigest internals and accuracy. Note no t.Parallel():
// during tests the default random seed is consistent, but varying
// concurrency scheduling mixes up the random values used in each test.
// Since there's a random number call inside tdigest this breaks repeatability
// for all tests. So, no test concurrency here.

func TestTInternals(t *testing.T) {
	tdigest := New(100)

	if !math.IsNaN(tdigest.Quantile(0.1)) {
		t.Errorf("Quantile() on an empty digest should return NaN. Got: %.4f", tdigest.Quantile(0.1))
	}

	_ = tdigest.Add(0.4, 1)

	if tdigest.Quantile(0.1) != 0.4 {
		t.Errorf("Quantile() on a single-sample digest should return the samples's mean. Got %.4f", tdigest.Quantile(0.1))
	}

	_ = tdigest.Add(0.5, 1)

	if tdigest.summary.Len() != 2 {
		t.Errorf("Expected size 2, got %d", tdigest.summary.Len())
	}

	if tdigest.summary.Min().mean != 0.4 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	if tdigest.summary.Max().mean != 0.5 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	_ = tdigest.Add(0.4, 2)
	_ = tdigest.Add(0.4, 3)

	if tdigest.summary.Len() != 2 {
		t.Errorf("Adding centroids of same mean shouldn't change size")
	}

	y := tdigest.summary.Find(0.4)

	if y.count != 6 || y.mean != 0.4 {
		t.Errorf("Adding centroids with same mean should increment the count only. Got %v", y)
	}

	err := tdigest.Add(0, 0)

	if err == nil {
		t.Errorf("Expected Add() to error out with input (0,0)")
	}

	if tdigest.Quantile(0.9999999) != tdigest.summary.Max().mean {
		t.Errorf("High quantiles with little data should give out the MAX recorded mean")
	}

	if tdigest.Quantile(0.0000001) != tdigest.summary.Min().mean {
		t.Errorf("Low quantiles with little data should give out the MIN recorded mean")
	}
}

func assertDifferenceSmallerThan(tdigest *TDigest, p float64, m float64, t *testing.T) {
	tp := tdigest.Quantile(p)
	if math.Abs(tp-p) >= m {
		t.Errorf("T-Digest.Quantile(%.4f) = %.4f. Diff (%.4f) >= %.4f", p, tp, math.Abs(tp-p), m)
	}
}

func TestUniformDistribution(t *testing.T) {
	tdigest := New(100)

	for i := 0; i < 10000; i++ {
		_ = tdigest.Add(rand.Float64(), 1)
	}

	assertDifferenceSmallerThan(tdigest, 0.5, 0.02, t)
	assertDifferenceSmallerThan(tdigest, 0.1, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.9, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.01, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.99, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.001, 0.001, t)
	assertDifferenceSmallerThan(tdigest, 0.999, 0.001, t)
}

// Asserts quantile p is no greater than absolute m off from "true"
// fractional quantile for supplied data. So m must be scaled
// appropriately for source data range.
func assertDifferenceFromQuantile(data []float64, tdigest *TDigest, p float64, m float64, t *testing.T) {
	q := quantile(p, data)
	tp := tdigest.Quantile(p)

	if math.Abs(tp-q) >= m {
		t.Fatalf("T-Digest.Quantile(%.4f) = %.4f vs actual %.4f. Diff (%.4f) >= %.4f", p, tp, q, math.Abs(tp-q), m)
	}
}

func TestSequentialInsertion(t *testing.T) {
	tdigest := New(10)

	rand.Seed(0xDEADBEEF)

	data := make([]float64, 10000)
	for i := 0; i < len(data); i++ {
		data[i] = float64(i)
	}

	for i := 0; i < len(data); i++ {
		_ = tdigest.Add(data[i], 1)

		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.001, 1.0+0.001*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.01, 1.0+0.005*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.05, 1.0+0.01*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.25, 1.0+0.03*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.5, 1.0+0.03*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.75, 1.0+0.03*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.95, 1.0+0.01*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.99, 1.0+0.005*float64(i), t)
		assertDifferenceFromQuantile(data[:i+1], tdigest, 0.999, 1.0+0.001*float64(i), t)
	}
}

func TestNonSequentialInsertion(t *testing.T) {
	tdigest := New(10)

	// Not quite a uniform distribution, but close.
	data := make([]float64, 1000)
	for i := 0; i < len(data); i++ {
		tmp := (i * 1627) % len(data)
		data[i] = float64(tmp)
	}

	sorted := make([]float64, 0, len(data))

	for i := 0; i < len(data); i++ {
		_ = tdigest.Add(data[i], 1)
		sorted = append(sorted, data[i])

		// Estimated quantiles are all over the place for low counts, which is
		// OK given that something like P99 is not very meaningful when there are
		// 25 samples. To account for this, increase the error tolerance for
		// smaller counts.
		if i == 0 {
			continue
		}

		max := float64(len(data))
		fac := 1.0 + max/float64(i)

		sort.Float64s(sorted)
		assertDifferenceFromQuantile(sorted, tdigest, 0.001, fac+0.001*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.01, fac+0.005*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.05, fac+0.01*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.25, fac+0.01*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.5, fac+0.02*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.75, fac+0.01*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.95, fac+0.01*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.99, fac+0.005*max, t)
		assertDifferenceFromQuantile(sorted, tdigest, 0.999, fac+0.001*max, t)
	}
}

func TestSingletonInACrowd(t *testing.T) {
	tdigest := New(100)
	for i := 0; i < 10000; i++ {
		tdigest.Add(10, 1)
	}
	tdigest.Add(20, 1)
	tdigest.Compress()

	for _, q := range []float64{0, 0.5, 0.8, 0.9, 0.99, 0.999} {
		if q == 0.999 {
			// Test for 0.999 disabled since it doesn't
			// pass in the reference implementation
			continue
		}
		result := tdigest.Quantile(q)
		if !closeEnough(result, 10) {
			t.Errorf("Expected Quantile(%.3f) = 10, but got %.4f (size=%d)", q, result, tdigest.Len())
		}
	}

	result := tdigest.Quantile(1)
	if result != 20 {
		t.Errorf("Expected Quantile(1) = 20, but got %.4f (size=%d)", result, tdigest.Len())
	}
}

func TestRespectBounds(t *testing.T) {
	tdigest := New(10)

	data := []float64{0, 279, 2, 281}
	for _, f := range data {
		tdigest.Add(f, 1)
	}

	quantiles := []float64{0.01, 0.25, 0.5, 0.75, 0.999}
	for _, q := range quantiles {
		result := tdigest.Quantile(q)
		if result < 0 {
			t.Errorf("q(%.3f) = %.4f < 0", q, result)
		}
		if tdigest.Quantile(q) > 281 {
			t.Errorf("q(%.3f) = %.4f > 281", q, result)
		}
	}
}

func TestWeights(t *testing.T) {
	tdigest := New(10)

	// Create data slice with repeats matching weights we gave to tdigest
	data := []float64{}
	for i := 0; i < 100; i++ {
		_ = tdigest.Add(float64(i), uint32(i))

		for j := 0; j < i; j++ {
			data = append(data, float64(i))
		}
	}

	assertDifferenceFromQuantile(data, tdigest, 0.001, 1.0+0.001*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.01, 1.0+0.005*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.05, 1.0+0.01*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.25, 1.0+0.01*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.5, 1.0+0.02*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.75, 1.0+0.01*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.95, 1.0+0.01*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.99, 1.0+0.005*100.0, t)
	assertDifferenceFromQuantile(data, tdigest, 0.999, 1.0+0.001*100.0, t)
}

func TestIntegers(t *testing.T) {
	tdigest := New(100)

	_ = tdigest.Add(1, 1)
	_ = tdigest.Add(2, 1)
	_ = tdigest.Add(3, 1)

	if tdigest.Quantile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Quantile(0.5))
	}

	tdigest = New(100)

	for _, i := range []float64{1, 2, 2, 2, 2, 2, 2, 2, 3} {
		_ = tdigest.Add(i, 1)
	}

	if tdigest.Quantile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Quantile(0.5))
	}

	var tot uint32
	tdigest.summary.Iterate(func(item centroid) bool {
		tot += item.count
		return true
	})

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
		_ = dist1.Add(num, 1)
		for j := 0; j < numSubs; j++ {
			_ = subs[j].Add(num, 1)
		}
	}

	dist2 := New(10)
	for i := 0; i < numSubs; i++ {
		_ = dist2.Merge(subs[i])
	}

	// Merge empty. Should be no-op
	err := dist2.Merge(New(10))
	if err != nil {
		t.Errorf("Merge() with an empty digest should be a noop. Got %s", err)
	}

	sort.Float64s(data)

	for _, p := range []float64{0.001, 0.01, 0.1, 0.2, 0.3, 0.5} {
		q := quantile(p, data)
		p1 := dist1.Quantile(p)
		p2 := dist2.Quantile(p)

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

func TestCompressDoesntChangeCount(t *testing.T) {
	tdigest := New(100)

	for i := 0; i < 1000; i++ {
		_ = tdigest.Add(rand.Float64(), 1)
	}

	initialCount := tdigest.count

	err := tdigest.Compress()
	if err != nil {
		t.Errorf("Compress() triggered an unexpected error: %s", err)
	}

	if tdigest.count != initialCount {
		t.Errorf("Compress() should not change count. Wanted %d, got %d", initialCount, tdigest.count)
	}
}

func shouldPanic(f func(), t *testing.T, message string) {
	defer func() {
		tryRecover := recover()
		if tryRecover == nil {
			t.Errorf(message)
		}
	}()
	f()
}

func TestPanic(t *testing.T) {
	shouldPanic(func() {
		New(0.5)
	}, t, "Compression < 1 should panic!")

	tdigest := New(100)

	shouldPanic(func() {
		tdigest.Quantile(-42)
	}, t, "Quantile < 0 should panic!")

	shouldPanic(func() {
		tdigest.Quantile(42)
	}, t, "Quantile > 1 should panic!")

	shouldPanic(func() {
		tdigest.findNearestCentroids(0.2)
	}, t, "findNearestCentroids on empty summary should panic!")
}

func TestForEachCentroid(t *testing.T) {
	tdigest := New(10)

	for i := 0; i < 100; i++ {
		_ = tdigest.Add(float64(i), 1)
	}

	// Iterate limited number.
	means := []float64{}
	tdigest.ForEachCentroid(func(mean float64, count uint32) bool {
		means = append(means, mean)
		return len(means) != 3
	})
	if len(means) != 3 {
		t.Errorf("ForEachCentroid handled incorrect number of data items")
	}

	// Iterate all datapoints.
	means = []float64{}
	tdigest.ForEachCentroid(func(mean float64, count uint32) bool {
		means = append(means, mean)
		return true
	})
	if len(means) != tdigest.Len() {
		t.Errorf("ForEachCentroid did not handle all data")
	}
}

func benchmarkAdd(compression float64, b *testing.B) {
	t := New(compression)

	data := make([]float64, b.N)
	for n := 0; n < b.N; n++ {
		data[n] = rand.Float64()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := t.Add(data[n], 1)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func BenchmarkAdd1(b *testing.B) {
	benchmarkAdd(1, b)
}

func BenchmarkAdd10(b *testing.B) {
	benchmarkAdd(10, b)
}

func BenchmarkAdd100(b *testing.B) {
	benchmarkAdd(100, b)
}
