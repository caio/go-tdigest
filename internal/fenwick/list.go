// Package fenwick provides a list data structure supporting prefix sums.
//
// A Fenwick tree, or binary indexed tree, is a space-efficient list
// data structure that can efficiently update elements and calculate
// prefix sums in a list of numbers.
//
// Compared to a common array, a Fenwick tree achieves better balance
// between element update and prefix sum calculation – both operations
// run in O(log n) time – while using the same amount of memory.
// This is achieved by representing the list as an implicit tree,
// where the value of each node is the sum of the numbers in that
// subtree.
//
package fenwick

// List represents a list of numbers with support for efficient
// prefix sum computation. The zero value is an empty list.
type List struct {
	// The tree slice stores range sums of an underlying array t.
	// To compute the prefix sum t[0] + t[1] + t[k-1], add elements
	// which correspond to each 1 bit in the binary expansion of k.
	//
	// For example, this is how the sum of the 13 first elements
	// in t is computed: 13 is 1101₂ in binary, so the elements
	// at indices 1101₂ - 1, 1100₂ - 1, and 1000₂ - 1  are added;
	// they contain the range sums t[12], t[8] + … t[11], and
	// t[0] + … + t[7], respectively.
	//
	tree []uint32
}

// New creates a new list with the given elements.
func New(n ...uint32) *List {
	len := len(n)
	t := make([]uint32, len)
	copy(t, n)
	for i := range t {
		if j := i | (i + 1); j < len {
			t[j] += t[i]
		}
	}
	return &List{
		tree: t,
	}
}

// Len returns the number of elements in the list.
func (l *List) Len() int {
	return len(l.tree)
}

// Get returns the element at index i.
func (l *List) Get(i int) uint32 {
	sum := l.tree[i]
	j := i + 1
	j -= j & -j
	for i > j {
		sum -= l.tree[i-1]
		i -= i & -i
	}
	return sum
}

// Set sets the element at index i to n.
func (l *List) Set(i int, n uint32) {
	n -= l.Get(i)
	for len := len(l.tree); i < len; i |= i + 1 {
		l.tree[i] += n
	}
}

// Add adds n to the element at index i.
func (l *List) Add(i int, n uint32) {
	for len := len(l.tree); i < len; i |= i + 1 {
		l.tree[i] += n
	}
}

// Sum returns the sum of the elements from index 0 to index i-1.
func (l *List) Sum(i int) uint32 {
	var sum uint32
	for i > 0 {
		sum += l.tree[i-1]
		i -= i & -i
	}
	return sum
}

// SumRange returns the sum of the elements from index i to index j-1.
func (l *List) SumRange(i, j int) uint32 {
	var sum uint32
	for j > i {
		sum += l.tree[j-1]
		j -= j & -j
	}
	for i > j {
		sum -= l.tree[i-1]
		i -= i & -i
	}
	return sum
}

// Append appends a new element to the end of the list.
func (l *List) Append(n uint32) {
	i := len(l.tree)
	l.tree = append(l.tree, 0)
	l.tree[i] = n - l.Get(i)
}
