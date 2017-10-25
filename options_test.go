package tdigest

import "testing"

func TestDefaults(t *testing.T) {
	digest, err := New()

	if err != nil {
		t.Errorf("Creating a default TDigest should never error out. Got %s", err)
	}

	if digest.compression != 100 {
		t.Errorf("The default compression should be 100")
	}
}

func TestCompression(t *testing.T) {
	digest, _ := New(Compression(40))
	if digest.compression != 40 {
		t.Errorf("The compression option should change the new digest compression")
	}

	digest, err := New(Compression(0))
	if err == nil || digest != nil {
		t.Errorf("Trying to create a digest with bad compression should give an error")
	}
}
