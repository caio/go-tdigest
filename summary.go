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
	return value.(*centroid)
}

func (s summary) Max() *centroid {
	value, _ := s.tree.At(s.tree.Len() - 1)
	return value.(*centroid)
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
