package tdigest

import (
	"math/rand"
)

type TDigestRNG interface {
	Float32() float32
	Intn(int) int
}

type globalRNG struct{}

func (r *globalRNG) Float32() float32 {
	return rand.Float32()
}

func (r *globalRNG) Intn(i int) int {
	return rand.Intn(i)
}

type localRNG struct {
	localRand *rand.Rand
}

func newLocalRNG(seed int64) *localRNG {
	return &localRNG{
		localRand: rand.New(rand.NewSource(seed)),
	}
}

func (r *localRNG) Float32() float32 {
	return r.localRand.Float32()
}

func (r *localRNG) Intn(i int) int {
	return r.localRand.Intn(i)
}
