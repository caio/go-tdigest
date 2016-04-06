// Package tdigest provides a highly accurate mergeable data-structure
// for quantile estimation.
package tdigest

import (
	"fmt"
	"math"
	"math/rand"
)

// TDigest is a quantile approximation data structure.
// Typical T-Digest use cases involve accumulating metrics on several
// distinct nodes of a cluster and then merging them together to get
// a system-wide quantile overview. Things such as: sensory data from
// IoT devices, quantiles over enormous document datasets (think
// ElasticSearch), performance metrics for distributed systems, etc.
type TDigest struct {
	Summary     *Summary
	Compression float64
	Count       uint32
}

// New creates a new digest.
// The compression parameter rules the threshold in which samples are
// merged together - the more often distinct samples are merged the more
// precision is lost. Compression should be tuned according to your data
// distribution, but a value of 100 is often good enough. A higher
// compression value means holding more centroids in memory (thus: better
// precision), which means a bigger serialization payload and higher
// memory footprint.
// Compression must be a value greater of equal to 1, will panic
// otherwise.
func New(compression float64) *TDigest {
	if compression < 1 {
		panic("Compression must be >= 1.0")
	}
	return &TDigest{
		Compression: compression,
		Summary:     newSummary(estimateCapacity(compression)),
		Count:       0,
	}
}

// Quantile returns the desired percentile estimation.
// Values of p must be between 0 and 1 (inclusive), will panic otherwise.
func (t *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		panic("q must be between 0 and 1 (inclusive)")
	}

	if t.Summary.Len() == 0 {
		return math.NaN()
	} else if t.Summary.Len() == 1 {
		return t.Summary.Min().Mean
	}

	q *= float64(t.Count)
	var total float64
	i := 0

	found := false
	var result float64

	t.Summary.Iterate(func(item Centroid) bool {
		k := float64(item.Count)

		if q < total+k {
			if i == 0 || i+1 == t.Summary.Len() {
				result = item.Mean
				found = true
				return false
			}
			succ, pred := t.Summary.successorAndPredecessorItems(item.Mean)
			delta := (succ.Mean - pred.Mean) / 2
			result = item.Mean + ((q-total)/k-0.5)*delta
			found = true
			return false
		}

		i++
		total += k
		return true
	})

	if found {
		return result
	}
	return t.Summary.Max().Mean
}

// Returns value of CDF evaluated at x
func (t *TDigest) CDF(x float64) float64 {
	N := float64(t.Count)
	tt := float64(0.0)
	done := false
	var result float64
	i := 0
	t.Summary.Iterate(func(item Centroid) bool {
		succ, pred := t.Summary.successorAndPredecessorItems(item.Mean)
		delta := float64(0.0)
		k := float64(item.Count)
		if i < t.Summary.Len() {
			delta = (succ.Mean - item.Mean)/2
		} else {
			delta = (item.Mean - pred.Mean)/2
		}
		if delta == float64(0.0) { // This should never happen as all centroids are distinct
			done = true
			return false
		}
		z := math.Max(-1.0, (x - item.Mean)/delta)
		if z < 1.0 {
			result = tt/N + ((k/N)*(z + 1.0)*0.5)
			done = true
			return false
		}
		tt += k
		return true
	})
	if done {
		return result
	}
	return float64(1.0)
}

// Add registers a new sample in the digest.
// It's the main entry point for the digest and very likely the only
// method to be used for collecting samples. The count parameter is for
// when you are registering a sample that occurred multiple times - the
// most common value for this is 1.
func (t *TDigest) Add(value float64, count uint32) error {

	if count == 0 {
		return fmt.Errorf("Illegal datapoint <value: %.4f, count: %d>", value, count)
	}

	t.Count += count

	if t.Summary.Len() == 0 {
		t.Summary.Add(value, count)
		return nil
	}

	candidates := t.FindNearestCentroids(value)

	for len(candidates) > 0 && count > 0 {
		j := rand.Intn(len(candidates))
		chosen := candidates[j]

		quantile := t.computeCentroidQuantile(chosen)

		if float64(chosen.Count +count) > t.threshold(quantile) {
			candidates = append(candidates[:j], candidates[j+1:]...)
			continue
		}

		deltaW := math.Min(t.threshold(quantile)-float64(chosen.Count), float64(count))
		t.Summary.updateAt(chosen.Index, value, uint32(deltaW))
		count -= uint32(deltaW)

		candidates = append(candidates[:j], candidates[j+1:]...)
	}

	if count > 0 {
		t.Summary.Add(value, count)
	}

	if float64(t.Summary.Len()) > 20*t.Compression {
		t.Compress()
	}

	return nil
}

// Compress tries to reduce the number of individual centroids stored
// in the digest.
// Compression trades off accuracy for performance and happens
// automatically after a certain amount of distinct samples have been
// stored.
func (t *TDigest) Compress() {
	if t.Summary.Len() <= 1 {
		return
	}

	oldTree := t.Summary
	t.Summary = newSummary(estimateCapacity(t.Compression))

	nodes := oldTree.Data()
	shuffle(nodes)

	for _, item := range nodes {
		t.Add(item.Mean, item.Count)
	}
}

// Merge joins a given digest into itself.
// Merging is useful when you have multiple TDigest instances running
// in separate threads and you want to compute quantiles over all the
// samples. This is particularly important on a scatter-gather/map-reduce
// scenario.
func (t *TDigest) Merge(other *TDigest) {
	if other.Summary.Len() == 0 {
		return
	}

	nodes := other.Summary.Data()
	shuffle(nodes)

	for _, item := range nodes {
		t.Add(item.Mean, item.Count)
	}
}

// Len returns the number of centroids in the TDigest.
func (t *TDigest) Len() int { return t.Summary.Len() }

// ForEachCentroid calls the specified function for each centroid.
// Iteration stops when the supplied function returns false, or when all
// centroids have been iterated.
func (t *TDigest) ForEachCentroid(f func(mean float64, count uint32) bool) {
	s := t.Summary
	for i := 0; i < s.Len(); i++ {
		if !f(s.Keys[i], s.Counts[i]) {
			break
		}
	}
}

func shuffle(data []Centroid) {
	for i := len(data) - 1; i > 1; i-- {
		other := rand.Intn(i + 1)
		tmp := data[other]
		data[other] = data[i]
		data[i] = tmp
	}
}

func estimateCapacity(compression float64) uint {
	return uint(compression) * 10
}

func (t *TDigest) threshold(q float64) float64 {
	return (4 * float64(t.Count) * q * (1 - q)) / t.Compression
}

func (t *TDigest) computeCentroidQuantile(c *Centroid) float64 {
	cumSum := t.Summary.sumUntilMean(c.Mean)
	return (float64(c.Count)/2.0 + float64(cumSum)) / float64(t.Count)
}

func (t *TDigest) FindNearestCentroids(mean float64) []*Centroid {
	ceil, floor := t.Summary.CeilingAndFloorItems(mean)

	if !ceil.isValid() && !floor.isValid() {
		panic("findNearestCentroids called on an empty tree")
	}

	if !ceil.isValid() {
		return []*Centroid{&floor}
	}

	if !floor.isValid() {
		return []*Centroid{&ceil}
	}

	if math.Abs(floor.Mean -mean) < math.Abs(ceil.Mean -mean) {
		return []*Centroid{&floor}
	} else if math.Abs(floor.Mean -mean) == math.Abs(ceil.Mean -mean) && floor.Mean != ceil.Mean {
		return []*Centroid{&floor, &ceil}
	} else {
		return []*Centroid{&ceil}
	}
}
