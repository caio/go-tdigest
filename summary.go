package tdigest

type summary struct {
	tree *sortedSlice
}

func newSummary(initialCapacity uint) *summary {
	return &summary{
		tree: newSortedSlice(initialCapacity),
	}
}

func (s summary) Len() int {
	return s.tree.Len()
}

func (s summary) Min() *centroid {
	value, _ := s.tree.At(0)
	if value != nil {
		return value.(*centroid)
	}
	return nil
}

func (s summary) Max() *centroid {
	value, _ := s.tree.At(s.tree.Len() - 1)
	if value != nil {
		return value.(*centroid)
	}
	return nil
}

func (s *summary) Add(c *centroid) {
	s.tree.Add(c.mean, c)
}

func (s summary) Data() []*centroid {
	data := make([]*centroid, 0, s.tree.Len())
	s.tree.Iterate(func(item interface{}) bool {
		data = append(data, item.(*centroid))
		return true
	})

	return data
}

func (s summary) Find(c *centroid) *centroid {
	f := s.tree.Find(c.mean)
	if f != nil {
		return f.(*centroid)
	}
	return nil
}

func (s *summary) Delete(c *centroid) *centroid {
	removed := s.tree.Remove(c.mean)
	if removed != nil {
		return removed.(*centroid)
	}
	return nil
}

func (s summary) IterInOrderWith(f func(item interface{}) bool) {
	s.tree.Iterate(f)
}

func (s summary) successorAndPredecessorItems(c *centroid) (*centroid, *centroid) {
	idx := s.tree.FindIndex(c.mean)

	succ, _ := s.tree.At(idx + 1)
	pred, _ := s.tree.At(idx - 1)

	return succ.(*centroid), pred.(*centroid)
}

func (s summary) ceilingAndFloorItems(c *centroid) (*centroid, *centroid) {
	idx := s.tree.FindIndex(c.mean)

	// Case 1: item is greater than all items in the summary
	if idx == s.tree.Len() {
		return nil, s.Max()
	}

	item, _ := s.tree.At(idx)

	// Case 2: item exists in the summary
	if item != nil && c.Equals(item.(*centroid)) {
		return item.(*centroid), item.(*centroid)
	}

	// Case 3: item is smaller than all items in the summary
	if idx == 0 {
		return s.Min(), nil
	}

	var ceil, floor *centroid

	ceilP := item
	floorP, _ := s.tree.At(idx - 1)

	if ceilP != nil {
		ceil = ceilP.(*centroid)
	}
	if floorP != nil {
		floor = floorP.(*centroid)
	}

	return ceil, floor

}
