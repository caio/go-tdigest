# T-Digest

A map-reduce and parallel streaming friendly data-structure for accurate
quantile approximation.

This package provides a very crude implementation of Ted Dunning's t-digest
data structure in Go.

[![Build Status](https://travis-ci.org/caio/go-tdigest.svg?branch=master)](https://travis-ci.org/caio/go-tdigest)

## Installation

    go get github.com/caio/go-tdigest

## Usage

    import ("github.com/caio/go-tdigest" "math/rand" "fmt")

    compression := 10
    t := tdigest.New(compression)

    for i := 0; i < 10000; i++ {
        t.Update(rand.Float64(), 1)
    }

    fmt.Printf("p(.5) = %.6f\n", t.Percentile(0.5))

## Disclaimer

I've written this solely with the purpose of understanding how the
data-structure works, it hasn't been throughly verified nor have I bothered with
optimizations for now.

## References

This is a very simple port of the [reference][1] implementation with some
ideas borrowed from the [python version][2]. If you wanna get a quick grasp of
how it works and why it's useful, [this video and companion article is pretty
helpful][3].

[1]: https://github.com/tdunning/t-digest
[2]: https://github.com/CamDavidsonPilon/tdigest
[3]: https://www.mapr.com/blog/better-anomaly-detection-t-digest-whiteboard-walkthrough

