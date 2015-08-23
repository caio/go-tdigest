package tdigest

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const SMALL_ENCODING int32 = 2

func (t TDigest) AsBytes() []byte {
	buffer := new(bytes.Buffer)

	binary.Write(buffer, binary.BigEndian, SMALL_ENCODING)
	binary.Write(buffer, binary.BigEndian, t.compression)
	binary.Write(buffer, binary.BigEndian, int32(t.summary.Len()))

	var x float64 = 0
	for item := range t.summary.Iter() {
		delta := item.(Centroid).mean - x
		x = item.(Centroid).mean
		binary.Write(buffer, binary.BigEndian, float32(delta))
	}

	for item := range t.summary.Iter() {
		encodeUint(buffer, item.(Centroid).count)
	}

	return buffer.Bytes()
}

func FromBytes(buf *bytes.Reader) *TDigest {
	var encoding int32
	binary.Read(buf, binary.BigEndian, &encoding)

	if encoding != SMALL_ENCODING {
		panic(fmt.Sprintf("Unsupported encoding version: %d", encoding))
	}

	var compression float64
	binary.Read(buf, binary.BigEndian, &compression)

	t := New(compression)

	var numCentroids int32
	binary.Read(buf, binary.BigEndian, &numCentroids)

	means := make([]float32, numCentroids)
	var i int32
	for i = 0; i < numCentroids; i++ {
		binary.Read(buf, binary.BigEndian, &means[i])
	}

	var x float64 = 0
	for i = 0; i < numCentroids; i++ {
		t.Update(float64(means[i])+x, decodeUint(buf))
		x = float64(means[i])
	}

	return t
}

func encodeUint(buf *bytes.Buffer, n uint32) {
	var k uint32 = 0
	for n < 0 || n > 0x7f {
		b := byte(0x80 | (0x7f & n))
		buf.WriteByte(b)
		n = n >> 7
		k++
		if k >= 6 {
			panic("Tried encoding a number that's too big")
		}
	}
	buf.WriteByte(byte(n))
}

func decodeUint(buf *bytes.Reader) uint32 {
	v, _ := buf.ReadByte()
	var z uint32 = 0x7f & uint32(v)
	var shift uint32 = 7
	for v&0x80 != 0 {
		if shift > 28 {
			panic("Something wrong, this number looks too big")
		}
		v, _ = buf.ReadByte()
		z += uint32((v & 0x7f)) << shift
		shift += 7
	}
	return z
}
