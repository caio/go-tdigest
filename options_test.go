package tdigest

import "testing"

func TestDefaults(t *testing.T) {
	digest := New()

	if digest.compression != 100 {
		t.Errorf("The default compression should be 100")
	}
}

func TestCompression(t *testing.T) {
	if New(Compression(40)).compression != 40 {
		t.Errorf("The compression option should change the new digest compression")
	}

	shouldPanic(func() { Compression(0) }, t, "Compression < 1 should panic")
}
