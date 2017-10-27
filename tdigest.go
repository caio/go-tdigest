// Package tdigest provides a highly accurate mergeable data-structure
// for quantile estimation.
//
// Typical T-Digest use cases involve accumulating metrics on several
// distinct nodes of a cluster and then merging them together to get
// a system-wide quantile overview. Things such as: sensory data from
// IoT devices, quantiles over enormous document datasets (think
// ElasticSearch), performance metrics for distributed systems, etc.
//
// After you create (and configure, if desired) the digest:
//     digest := tdigest.New(tdigest.Compression(100))
//
// You can then use it for registering measurements:
//     digest.Add(number)
//
// Estimating quantiles:
//     digest.Quantile(0.99)
//
// And merging with another digest:
//     digest.Merge(otherDigest)
package tdigest

import (
	"fmt"
	"math"
)

// TDigest is a quantile approximation data structure.
type TDigest struct {
	summary     *summary
	compression float64
	count       uint64
	rng         TDigestRNG
}

// New creates a new digest.
//
// By default the digest is constructed with a configuration that
// should be useful for most use-cases. It comes with compression
// set to 100 and uses the global random number generator (same
// as using math/rand top-level functions).
func New(options ...tdigestOption) (*TDigest, error) {
	tdigest := &TDigest{
		compression: 100,
		count:       0,
		rng:         &globalRNG{},
	}

	for _, option := range options {
		err := option(tdigest)
		if err != nil {
			return nil, err
		}
	}

	tdigest.summary = newSummary(estimateCapacity(tdigest.compression))
	return tdigest, nil
}

func _quantile(index float64, previousIndex float64, nextIndex float64, previousMean float64, nextMean float64) float64 {
	delta := nextIndex - previousIndex
	previousWeight := (nextIndex - index) / delta
	nextWeight := (index - previousIndex) / delta
	return previousMean*previousWeight + nextMean*nextWeight
}

// Quantile returns the desired percentile estimation.
//
// Values of p must be between 0 and 1 (inclusive), will panic otherwise.
func (t *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		panic("q must be between 0 and 1 (inclusive)")
	}

	if t.Len() == 0 {
		return math.NaN()
	} else if t.Len() == 1 {
		return t.summary.Mean(0)
	}

	index := q * float64(t.count-1)
	previousMean := math.NaN()
	previousIndex := float64(0)
	next, total := t.summary.FloorSum(index)

	if next > 0 {
		previousMean = t.summary.Mean(next - 1)
		previousIndex = total - float64(t.summary.Count(next-1)+1)/2
	}

	for {
		nextIndex := total + float64(t.summary.Count(next)-1)/2
		if nextIndex >= index {
			if math.IsNaN(previousMean) {
				// the index is before the 1st centroid
				if nextIndex == previousIndex {
					return t.summary.Mean(next)
				}
				// assume linear growth
				nextIndex2 := total + float64(t.summary.Count(next)) + float64(t.summary.Count(next+1)-1)/2
				previousMean = (nextIndex2*t.summary.Mean(next) - nextIndex*t.summary.Mean(next+1)) / (nextIndex2 - nextIndex)
			}
			// common case: two centroids found, the result in inbetween
			return _quantile(index, previousIndex, nextIndex, previousMean, t.summary.Mean(next))
		} else if next+1 == t.Len() {
			// the index is after the last centroid
			nextIndex2 := float64(t.count - 1)
			nextMean2 := (t.summary.Mean(next)*(nextIndex2-previousIndex) - previousMean*(nextIndex2-nextIndex)) / (nextIndex - previousIndex)
			return _quantile(index, nextIndex, nextIndex2, t.summary.Mean(next), nextMean2)
		}
		total += float64(t.summary.Count(next))
		previousMean = t.summary.Mean(next)
		previousIndex = nextIndex
		next++
	}
	// unreachable
}

func weightedAverage(x1 float64, w1 float64, x2 float64, w2 float64) float64 {
	if x1 > x2 {
		x1, x2, w1, w2 = x2, x1, w2, w1
	}
	return x1*w1/(w1+w2) + x2*w2/(w1+w2)
}

// AddWeighted registers a new sample in the digest.
//
// It's the main entry point for the digest and very likely the only
// method to be used for collecting samples. The count parameter is for
// when you are registering a sample that occurred multiple times - the
// most common value for this is 1.
//
// This will emit an error if `value` is NaN of if `count` is zero.
func (t *TDigest) AddWeighted(value float64, count uint32) (err error) {

	if count == 0 {
		return fmt.Errorf("Illegal datapoint <value: %.4f, count: %d>", value, count)
	}

	if t.Len() == 0 {
		err = t.summary.Add(value, count)
		t.count = uint64(count)
		return err
	}

	start := t.summary.Floor(value)
	if start == -1 {
		start = 0
	}

	minDistance := math.MaxFloat64
	lastNeighbor := t.Len()
	for neighbor := start; neighbor < t.Len(); neighbor++ {
		z := math.Abs(t.summary.Mean(neighbor) - value)
		if z < minDistance {
			start = neighbor
			minDistance = z
		} else if z > minDistance {
			lastNeighbor = neighbor
			break
		}
	}

	closest := t.Len()
	sum := t.summary.HeadSum(start)
	var n float32

	for neighbor := start; neighbor != lastNeighbor; neighbor++ {
		c := float64(t.summary.Count(neighbor))
		var q float64
		if t.count == 1 {
			q = 0.5
		} else {
			q = (sum + (c-1)/2) / float64(t.count-1)
		}
		k := 4 * float64(t.count) * q * (1 - q) / t.compression

		if c+float64(count) <= k {
			n++
			if t.rng.Float32() < 1/n {
				closest = neighbor
			}
		}
		sum += c
	}

	if closest == t.Len() {
		t.summary.Add(value, count)
	} else {
		c := float64(t.summary.Count(closest))
		newMean := weightedAverage(t.summary.Mean(closest), c, value, float64(count))
		t.summary.setAt(closest, newMean, uint32(c)+count)
	}
	t.count += uint64(count)

	if float64(t.Len()) > 20*t.compression {
		err = t.Compress()
	}

	return err
}

// Count returns the total number of samples this digest represents
//
// The result represents how many times Add() was called on a digest
// plus how many samples the digests it has been merged with had.
// This is useful mainly for two scenarios:
//
// - Knowing if there is enough data so you can trust the quantiles
//
// - Knowing if you've registered too many samples already and
// deciding what to do about it.
//
// For the second case one approach would be to create a side empty
// digest and start registering samples on it as well as on the old
// (big) one and then discard the bigger one after a certain criterion
// is reached (say, minimum number of samples or a small relative
// error between new and old digests).
func (t TDigest) Count() uint64 {
	return t.count
}

// Add(x) is an alias for AddWeighted(x,1)
// Read the documentation for AddWeighted for more details.
func (t *TDigest) Add(value float64) error {
	return t.AddWeighted(value, 1)
}

// Compress tries to reduce the number of individual centroids stored
// in the digest.
//
// Compression trades off accuracy for performance and happens
// automatically after a certain amount of distinct samples have been
// stored.
//
// At any point in time you may call Compress on a digest, but you
// may completely ignore this and it will compress itself automatically
// after it grows too much. If you are minimizing network traffic
// it might be a good idea to compress before serializing.
func (t *TDigest) Compress() (err error) {
	if t.Len() <= 1 {
		return nil
	}

	oldTree := t.summary
	t.summary = newSummary(uint(t.Len()))
	t.count = 0

	shuffle(oldTree.means, oldTree.counts, t.rng)
	oldTree.ForEach(func(mean float64, count uint32) bool {
		err = t.AddWeighted(mean, count)
		return err == nil
	})

	return err
}

// Merge joins a given digest into itself.
//
// Merging is useful when you have multiple TDigest instances running
// in separate threads and you want to compute quantiles over all the
// samples. This is particularly important on a scatter-gather/map-reduce
// scenario.
func (t *TDigest) Merge(other *TDigest) (err error) {
	if other.Len() == 0 {
		return nil
	}

	// We must keep the other digest intact
	data := other.summary.Clone()
	shuffle(data.means, data.counts, t.rng)

	data.ForEach(func(mean float64, count uint32) bool {
		err = t.AddWeighted(mean, count)
		return err == nil
	})
	return err
}

// Len returns the number of centroids in the TDigest.
func (t *TDigest) Len() int { return t.summary.Len() }

// ForEachCentroid calls the specified function for each centroid.
//
// Iteration stops when the supplied function returns false, or when all
// centroids have been iterated.
func (t *TDigest) ForEachCentroid(f func(mean float64, count uint32) bool) {
	t.summary.ForEach(f)
}

func shuffle(means []float64, counts []uint32, rng TDigestRNG) {
	for i := len(means) - 1; i > 1; i-- {
		j := rng.Intn(i + 1)
		means[i], means[j], counts[i], counts[j] = means[j], means[i], counts[j], counts[i]
	}
}

func estimateCapacity(compression float64) uint {
	return uint(compression) * 10
}
