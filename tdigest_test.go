package tdigest

import (
	"bytes"
	"encoding/base64"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestTInternals(t *testing.T) {
	t.Parallel()

	tdigest := New(100)

	if !math.IsNaN(tdigest.Percentile(0.1)) {
		t.Errorf("Percentile() on an empty digest should return NaN. Got: %.4f", tdigest.Percentile(0.1))
	}

	tdigest.addCentroid(0.4, 1)

	if tdigest.Percentile(0.1) != 0.4 {
		t.Errorf("Percentile() on a single-sample digest should return the samples's mean. Got %.4f", tdigest.Percentile(0.1))
	}

	tdigest.addCentroid(0.5, 1)

	if tdigest.summary.Len() != 2 {
		t.Errorf("Expected size 2, got %d", tdigest.summary.Len())
	}

	if tdigest.summary.Min().mean != 0.4 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	if tdigest.summary.Max().mean != 0.5 {
		t.Errorf("Min() returned an unexpected centroid: %v", tdigest.summary.Min())
	}

	deleted := tdigest.summary.Remove(0.6)
	if deleted.isValid() {
		t.Errorf("Remove() on non-existant centroid should give an invalid return, go this instead: %v", deleted)
	}

	tdigest.addCentroid(0.4, 2)
	tdigest.addCentroid(0.4, 3)

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
}

func assertDifferenceSmallerThan(tdigest *TDigest, p float64, m float64, t *testing.T) {
	tp := tdigest.Percentile(p)
	if math.Abs(tp-p) >= m {
		t.Errorf("T-Digest.Percentile(%.4f) = %.4f. Diff (%.4f) >= %.4f", p, tp, math.Abs(tp-p), m)
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

	if tdigest.Percentile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Percentile(0.5))
	}

	tdigest = New(100)

	for _, i := range []float64{1, 2, 2, 2, 2, 2, 2, 2, 3} {
		tdigest.Add(i, 1)
	}

	if tdigest.Percentile(0.5) != 2 {
		t.Errorf("Expected p(0.5) = 2, Got %.2f instead", tdigest.Percentile(0.5))
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
		t1.Add(rand.Float64(), 1)
	}

	serialized, _ := t1.AsBytes()

	t2, _ := FromBytes(bytes.NewReader(serialized))

	if t1.count != t2.count || t1.summary.Len() != t2.summary.Len() || t1.compression != t2.compression {
		t.Errorf("Deserialized to something different. t1=%v t2=%v serialized=%v", t1, t2, serialized)
	}
}

func TestJavaSmallBytesCompat(t *testing.T) {
	// Base64 string generated via (<3 clojure):
	// (def t (com.tdunning.math.stats.AVLTreeDigest. 100))
	// (def r (java.util.Random.))
	// (.setSeed r 0xDEADBEEF)
	//
	// (dotimes [x 100000]
	// (.add t (.nextDouble r)))
	//
	// (def buf (java.nio.ByteBuffer/allocate (.smallByteSize t)))
	// (.asSmallBytes t buf)
	// (.flip buf)
	// (.compress t)
	// (def serialized-tdigest (.encodeToString (java.util.Base64/getEncoder) (.array buf)))
	//
	// (println serialized-tdigest)

	serializedJavaTDigestB64 := "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"

	tdigestAsBytes, err := base64.StdEncoding.DecodeString(serializedJavaTDigestB64)

	if err != nil {
		t.Fatalf(err.Error())
	}

	tdigest, err := FromBytes(bytes.NewReader(tdigestAsBytes))

	if err != nil {
		t.Fatalf(err.Error())
	}

	if tdigest.count != 100000 {
		t.Fatalf("Expected deserialized t-digest to have a count of 100_000. Got %d", tdigest.count)
	}

	assertDifferenceSmallerThan(tdigest, 0.5, 0.02, t)
	assertDifferenceSmallerThan(tdigest, 0.1, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.9, 0.01, t)
	assertDifferenceSmallerThan(tdigest, 0.01, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.99, 0.005, t)
	assertDifferenceSmallerThan(tdigest, 0.001, 0.001, t)
	assertDifferenceSmallerThan(tdigest, 0.999, 0.001, t)
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
