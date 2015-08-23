package tdigest

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/ancientlore/go-avltree"
)

type Centroid struct {
	mean  float64
	count uint
}

func (c Centroid) String() string {
	return fmt.Sprintf("C<m=%.6f,c=%d>", c.mean, c.count)
}

func (c Centroid) Equals(other Centroid) bool {
	return c.mean == other.mean && c.count == other.count
}

func (c *Centroid) Update(x float64, weight uint) {
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
	summary     *avltree.Tree
	compression uint
	count       uint
}

func New(compression uint) *TDigest {
	tree := avltree.New(compareCentroids, 0)
	tdigest := TDigest{compression: compression, summary: tree, count: 0}
	return &tdigest
}

func (t *TDigest) Percentile(p float64) float64 {
	if p < 0 || p > 1 {
		panic("Percentiles must be between 0 and 1 (inclusive)")
	}

	if t.summary.Len() == 0 {
		return math.NaN()
	} else if t.summary.Len() == 1 {
		return t.summary.At(0).(Centroid).mean
	}

	p *= float64(t.count)
	var total float64 = 0
	i := 0

	for item := range t.summary.Iter() {
		k := float64(item.(Centroid).count)

		if p < total+k {
			if i == 0 || i+1 == t.summary.Len() {
				return item.(Centroid).mean
			}
			succ, pred := t.successorAndPredecessorItems(item.(Centroid))
			delta := (succ.mean - pred.mean) / 2
			return item.(Centroid).mean + ((p-total)/k-0.5)*delta
		}

		i++
		total += k
	}

	return t.summary.At(t.summary.Len() - 1).(Centroid).mean
}

func (t *TDigest) Update(value float64, weight uint) {
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
		t.updateCentroid(chosen, value, uint(delta_w))
		weight -= uint(delta_w)

		candidates = append(candidates[:j], candidates[j+1:]...)
	}

	if weight > 0 {
		t.addCentroid(Centroid{value, weight})
	}

	if float64(t.summary.Len()) > float64(20*t.compression) {
		t.Compress()
	}
}

func (t *TDigest) Compress() {
	if t.summary.Len() <= 1 {
		return
	}

	oldTree := t.summary
	t.summary = avltree.New(compareCentroids, 0)

	nodes := oldTree.Data()
	shuffle(nodes)

	for _, item := range nodes {
		t.Update(item.(Centroid).mean, item.(Centroid).count)
	}
}

func shuffle(data []interface{}) {
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
		t.Update(item.(Centroid).mean, item.(Centroid).count)
	}
}

func (t TDigest) String() string {
	return fmt.Sprintf("TD<compression=%d, count=%d, centroids=%d>", t.compression, t.count, t.summary.Len())
}

func (t *TDigest) updateCentroid(c Centroid, mean float64, weight uint) {
	if t.summary.Find(c) == nil {
		panic(fmt.Sprintf("Trying to update a centroid that doesn't exist: %s. %s", c, t))
	}

	t.summary.Remove(c)
	c.Update(mean, weight)
	t.addCentroid(c)
}

func (t *TDigest) threshold(q float64) float64 {
	return (4 * float64(t.count) * q * (1 - q)) / float64(t.compression)
}

func (t *TDigest) computeCentroidQuantile(c Centroid) float64 {
	var cumSum uint = 0
	channel := t.exclusiveSliceUntilMean(c)
	for item := range channel {
		cumSum += item.count
	}

	return (float64(c.count)/2.0 + float64(cumSum)) / float64(t.count)
}

func (t *TDigest) addCentroid(c Centroid) {
	current := t.summary.Find(c)

	if current != nil {
		t.summary.Remove(current)
		c.Update(current.(Centroid).mean, current.(Centroid).count)
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
	for item := range t.summary.Iter() {
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

func (t *TDigest) exclusiveSliceUntilMean(c Centroid) <-chan Centroid {
	channel := make(chan Centroid)
	go func() {
		for item := range t.summary.Iter() {
			if centroidLess(item, c) {
				channel <- item.(Centroid)
			} else {
				break
			}
		}
		close(channel)
	}()
	return channel
}
