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
	summary     *summary
	compression float64
	count       uint32
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
		compression: compression,
		summary:     newSummary(estimateCapacity(compression)),
		count:       0,
	}
}

func _quantile(index float64, previousIndex float64, nextIndex float64, previousMean float64, nextMean float64) float64 {
	delta := nextIndex - previousIndex
	previousWeight := (nextIndex - index) / delta
	nextWeight := (index - previousIndex) / delta
	return previousMean*previousWeight + nextMean*nextWeight
}

// Quantile returns the desired percentile estimation.
// Values of p must be between 0 and 1 (inclusive), will panic otherwise.
func (t *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		panic("q must be between 0 and 1 (inclusive)")
	}

	if t.Len() == 0 {
		return math.NaN()
	} else if t.Len() == 1 {
		return t._mean(0)
	}

	index := q * float64(t.count-1)
	previousMean := math.NaN()
	previousIndex := float64(0)
	next, total := t.summary.FloorSum(index)

	if next > 0 {
		previousMean = t._mean(next - 1)
		previousIndex = total - (t._count(next-1)+1)/2
	}

	for {
		nextIndex := total + (t._count(next)-1)/2
		if nextIndex >= index {
			if math.IsNaN(previousMean) {
				// the index is before the 1st centroid
				if nextIndex == previousIndex {
					return t._mean(next)
				}
				// assume linear growth
				nextIndex2 := total + t._count(next) + (t._count(next+1)-1)/2
				previousMean = (nextIndex2*t._mean(next) - nextIndex*t._mean(next+1)) / (nextIndex2 - nextIndex)
			}
			// common case: two centroids found, the result in inbetween
			return _quantile(index, previousIndex, nextIndex, previousMean, t._mean(next))
		} else if next+1 == t.Len() {
			// the index is after the last centroid
			nextIndex2 := float64(t.count - 1)
			nextMean2 := (t._mean(next)*(nextIndex2-previousIndex) - previousMean*(nextIndex2-nextIndex)) / (nextIndex - previousIndex)
			return _quantile(index, nextIndex, nextIndex2, t._mean(next), nextMean2)
		}
		total += t._count(next)
		previousMean = t._mean(next)
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

func (t TDigest) _mean(index int) float64  { return t.summary.keys[index] }
func (t TDigest) _count(index int) float64 { return float64(t.summary.counts[index]) }

// Add registers a new sample in the digest.
// It's the main entry point for the digest and very likely the only
// method to be used for collecting samples. The count parameter is for
// when you are registering a sample that occurred multiple times - the
// most common value for this is 1.
func (t *TDigest) Add(value float64, count uint32) (err error) {

	if count == 0 {
		return fmt.Errorf("Illegal datapoint <value: %.4f, count: %d>", value, count)
	}

	if t.Len() == 0 {
		err = t.summary.Add(value, count)
		t.count = count
		return err
	}

	x := value
	w := count

	start := t.summary.Floor(x)
	if start == -1 {
		start = 0
	}

	minDistance := math.MaxFloat64
	lastNeighbor := t.Len()
	for neighbor := start; neighbor < t.Len(); neighbor++ {
		z := math.Abs(t._mean(neighbor) - x)
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
		c := t._count(neighbor)
		var q float64
		if t.count == 1 {
			q = 0.5
		} else {
			q = (sum + (c-1)/2) / float64(t.count-1)
		}
		k := 4 * float64(t.count) * q * (1 - q) / t.compression

		if c+float64(w) <= k {
			n++
			if rand.Float32() < 1/n {
				closest = neighbor
			}
		}
		sum += c
	}

	if closest == t.Len() {
		t.summary.Add(x, w)
	} else {
		c := t._count(closest)
		newMean := weightedAverage(t._mean(closest), c, x, float64(w))
		t.summary.setAt(closest, newMean, uint32(c)+w)
	}
	t.count += w

	if float64(t.Len()) > 20*t.compression {
		err = t.Compress()
	}

	return err
}

// Compress tries to reduce the number of individual centroids stored
// in the digest.
// Compression trades off accuracy for performance and happens
// automatically after a certain amount of distinct samples have been
// stored.
func (t *TDigest) Compress() error {
	if t.Len() <= 1 {
		return nil
	}

	oldTree := t.summary
	t.summary = newSummary(uint(t.Len()))
	t.count = 0

	nodes := oldTree.Data()
	shuffle(nodes)

	for _, item := range nodes {
		err := t.Add(item.mean, item.count)
		if err != nil {
			return err
		}
	}

	return nil
}

// Merge joins a given digest into itself.
// Merging is useful when you have multiple TDigest instances running
// in separate threads and you want to compute quantiles over all the
// samples. This is particularly important on a scatter-gather/map-reduce
// scenario.
func (t *TDigest) Merge(other *TDigest) error {
	if other.Len() == 0 {
		return nil
	}

	nodes := other.summary.Data()
	shuffle(nodes)

	for _, item := range nodes {
		err := t.Add(item.mean, item.count)
		if err != nil {
			return err
		}
	}

	return nil
}

// Len returns the number of centroids in the TDigest.
func (t *TDigest) Len() int { return t.summary.Len() }

// ForEachCentroid calls the specified function for each centroid.
// Iteration stops when the supplied function returns false, or when all
// centroids have been iterated.
func (t *TDigest) ForEachCentroid(f func(mean float64, count uint32) bool) {
	s := t.summary
	for i := 0; i < s.Len(); i++ {
		if !f(s.keys[i], s.counts[i]) {
			break
		}
	}
}

func shuffle(data []centroid) {
	for i := len(data) - 1; i > 1; i-- {
		j := rand.Intn(i + 1)
		data[i], data[j] = data[j], data[i]
	}
}

func estimateCapacity(compression float64) uint {
	return uint(compression) * 10
}
