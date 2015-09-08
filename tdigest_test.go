package tdigest

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestTInternals(t *testing.T) {
	t.Parallel()

	tdigest := New(100)

	if !math.IsNaN(tdigest.Quantile(0.1)) {
		t.Errorf("Quantile() on an empty digest should return NaN. Got: %.4f", tdigest.Quantile(0.1))
	}

	tdigest.Add(0.4, 1)

	if tdigest.Quantile(0.1) != 0.4 {
		t.Errorf("Quantile() on a single-sample digest should return the samples's mean. Got %.4f", tdigest.Quantile(0.1))
	}

	tdigest.Add(0.5, 1)

	if tdigest.summary.Len() != 2 {
		t.Errorf("Expected size 2, got %d", tdigest.summary.Len())
	}

	if tdigest.summary.Min().mean != 0.4 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	if tdigest.summary.Max().mean != 0.5 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	tdigest.Add(0.4, 2)
	tdigest.Add(0.4, 3)

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
	t.Parallel()

	rand.Seed(0xDEADBEEF)

	tdigest := New(100)

	for i := 0; i < 10000; i++ {
		tdigest.Add(rand.Float64(), 1)
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
		tdigest.Add(float64(i), 1)
	}
}

func TestIntegers(t *testing.T) {
	t.Parallel()
	tdigest := New(100)

	tdigest.Add(1, 1)
	tdigest.Add(2, 1)
	tdigest.Add(3, 1)

	if tdigest.Quantile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Quantile(0.5))
	}

	tdigest = New(100)

	for _, i := range []float64{1, 2, 2, 2, 2, 2, 2, 2, 3} {
		tdigest.Add(i, 1)
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
		dist1.Add(num, 1)
		for j := 0; j < numSubs; j++ {
			subs[j].Add(num, 1)
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
