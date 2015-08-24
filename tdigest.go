package tdigest

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/petar/GoLLRB/llrb"
)

type Centroid struct {
	mean  float64
	count uint32
}

func (c Centroid) String() string {
	return fmt.Sprintf("C<m=%.6f,c=%d>", c.mean, c.count)
}

func (c Centroid) Equals(other Centroid) bool {
	return c.mean == other.mean && c.count == other.count
}

func (c *Centroid) Update(x float64, weight uint32) {
	c.count += weight
	c.mean += float64(weight) * (x - c.mean) / float64(c.count)
}

var InvalidCentroid Centroid = Centroid{mean: 0.0, count: 0}

func compareCentroids(p, q interface{}) int {
	a := p.(Centroid).mean
	b := q.(Centroid).mean

	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}

func centroidLess(p, q interface{}) bool {
	res := p.(Centroid).mean < q.(Centroid).mean
	return res
}

func centroidLessOrEquals(p, q interface{}) bool {
	a := p.(Centroid).mean
	b := q.(Centroid).mean
	return a <= b
}

type TDigest struct {
	summary     *Summary
	compression float64
	count       uint32
}

func New(compression float64) *TDigest {
	tdigest := TDigest{compression: compression, summary: newSummary(), count: 0}
	return &tdigest
}

func (t *TDigest) Percentile(p float64) float64 {
	if p < 0 || p > 1 {
		panic("Percentiles must be between 0 and 1 (inclusive)")
	}

	if t.summary.Len() == 0 {
		return math.NaN()
	} else if t.summary.Len() == 1 {
		return t.summary.Min().mean
	}

	p *= float64(t.count)
	var total float64 = 0
	i := 0

	found := false
	var result float64

	t.summary.IterInOrderWith(func(item llrb.Item) bool {
		k := float64(item.(Centroid).count)

		if p < total+k {
			if i == 0 || i+1 == t.summary.Len() {
				result = item.(Centroid).mean
				found = true
				return false
			}
			succ, pred := t.successorAndPredecessorItems(item.(Centroid))
			delta := (succ.mean - pred.mean) / 2
			result = item.(Centroid).mean + ((p-total)/k-0.5)*delta
			found = true
			return false
		}

		i++
		total += k
		return true
	})

	if found {
		return result
	} else {
		return t.summary.Max().mean
	}
}

func (t *TDigest) Update(value float64, weight uint32) {
	t.count += weight

	newCentroid := Centroid{value, weight}

	if t.summary.Len() == 0 {
		t.addCentroid(newCentroid)
		return
	}

	candidates := t.findNearestCentroids(newCentroid)

	for len(candidates) > 0 && weight > 0 {
		j := rand.Intn(len(candidates))
		chosen := candidates[j]

		quantile := t.computeCentroidQuantile(chosen)

		if float64(chosen.count+weight) > t.threshold(quantile) {
			candidates = append(candidates[:j], candidates[j+1:]...)
			continue
		}

		delta_w := math.Min(t.threshold(quantile)-float64(chosen.count), float64(weight))
		t.updateCentroid(chosen, value, uint32(delta_w))
		weight -= uint32(delta_w)

		candidates = append(candidates[:j], candidates[j+1:]...)
	}

	if weight > 0 {
		t.addCentroid(Centroid{value, weight})
	}

	if float64(t.summary.Len()) > 20*t.compression {
		t.Compress()
	}
}

func (t *TDigest) Compress() {
	if t.summary.Len() <= 1 {
		return
	}

	oldTree := t.summary
	t.summary = newSummary()

	nodes := oldTree.Data()
	shuffle(nodes)

	for _, item := range nodes {
		t.Update(item.mean, item.count)
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

func (t *TDigest) Merge(other *TDigest) {
	if other.summary.Len() == 0 {
		return
	}

	nodes := other.summary.Data()
	shuffle(nodes)

	for _, item := range nodes {
		t.Update(item.mean, item.count)
	}
}

func (t TDigest) String() string {
	return fmt.Sprintf("TD<compression=%.2f, count=%d, centroids=%d>", t.compression, t.count, t.summary.Len())
}

func (t *TDigest) updateCentroid(c Centroid, mean float64, weight uint32) {
	if t.summary.Find(c) == nil {
		panic(fmt.Sprintf("Trying to update a centroid that doesn't exist: %s. %s", c, t))
	}

	t.summary.Delete(c)
	c.Update(mean, weight)
	t.addCentroid(c)
}

func (t *TDigest) threshold(q float64) float64 {
	return (4 * float64(t.count) * q * (1 - q)) / t.compression
}

func (t *TDigest) computeCentroidQuantile(c Centroid) float64 {
	var cumSum uint32 = 0

	t.summary.IterInOrderWith(func(i llrb.Item) bool {
		if !centroidLess(i.(Centroid), c) {
			return false
		}

		cumSum += i.(Centroid).count

		return true
	})

	return (float64(c.count)/2.0 + float64(cumSum)) / float64(t.count)
}

func (t *TDigest) addCentroid(c Centroid) {
	current := t.summary.Find(c)

	if current != nil {
		t.summary.Delete(*current)
		c.Update(current.mean, current.count)
	}

	t.summary.Add(c)
}

func (t *TDigest) findNearestCentroids(c Centroid) []Centroid {
	ceil, floor := t.ceilingAndFloorItems(c)

	if ceil == InvalidCentroid && floor == InvalidCentroid {
		panic("findNearestCentroids called on an empty tree")
	}

	if ceil == InvalidCentroid {
		return []Centroid{floor}
	}

	if floor == InvalidCentroid {
		return []Centroid{ceil}
	}

	if math.Abs(floor.mean-c.mean) < math.Abs(ceil.mean-c.mean) {
		return []Centroid{floor}
	} else if math.Abs(floor.mean-c.mean) == math.Abs(ceil.mean-c.mean) && !floor.Equals(ceil) {
		return []Centroid{floor, ceil}
	} else {
		return []Centroid{ceil}
	}
}

func (t *TDigest) getSurroundingWith(c Centroid, cmp func(a, b interface{}) bool) (Centroid, Centroid) {
	ceiling, floor := InvalidCentroid, InvalidCentroid
	for item := range t.summary.iterInOrder() {
		if ceiling == InvalidCentroid && cmp(c, item) {
			ceiling = item.(Centroid)
		}
		if cmp(item, c) {
			floor = item.(Centroid)
		}
	}
	return ceiling, floor
}

func (t *TDigest) ceilingAndFloorItems(c Centroid) (Centroid, Centroid) {
	// ceiling => smallest key greater than or equals to key
	// floor   => greatest key less than or equals to key
	return t.getSurroundingWith(c, centroidLessOrEquals)
}

func (t *TDigest) successorAndPredecessorItems(c Centroid) (Centroid, Centroid) {
	// FIXME This can be way cheaper if done directly on the tree nodes
	return t.getSurroundingWith(c, centroidLess)
}
