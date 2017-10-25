package tdigest

type tdigestOption func(*TDigest)

// Compression sets the digest compression
//
// The compression parameter rules the threshold in which samples are
// merged together - the more often distinct samples are merged the more
// precision is lost. Compression should be tuned according to your data
// distribution, but a value of 100 (the default) is often good enough.
//
// A higher compression value means holding more centroids in memory
// (thus: better precision), which means a bigger serialization payload,
// higher memory footprint and slower addition of new samples.
//
// Compression must be a value greater of equal to 1, will panic
// otherwise.
func Compression(compression uint32) tdigestOption {
	if compression < 1 {
		panic("Compression should be >= 1")
	}
	return func(t *TDigest) {
		t.compression = float64(compression)
	}
}
