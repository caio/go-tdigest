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
