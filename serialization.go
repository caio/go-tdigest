package tdigest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/petar/GoLLRB/llrb"
)

const smallEncoding int32 = 2

var endianess = binary.BigEndian

// AsBytes serializes the digest into a byte array so it can be
// saved to disk or sent over the wire.
func (t TDigest) AsBytes() ([]byte, error) {
	buffer := new(bytes.Buffer)

	err := binary.Write(buffer, endianess, smallEncoding)

	if err != nil {
		return nil, err
	}

	err = binary.Write(buffer, endianess, t.compression)

	if err != nil {
		return nil, err
	}

	err = binary.Write(buffer, endianess, int32(t.summary.Len()))

	if err != nil {
		return nil, err
	}

	var x float64 = 0
	t.summary.IterInOrderWith(func(item llrb.Item) bool {
		delta := item.(centroid).mean - x
		x = item.(centroid).mean
		err = binary.Write(buffer, endianess, float32(delta))

		return err == nil
	})
	if err != nil {
		return nil, err
	}

	t.summary.IterInOrderWith(func(item llrb.Item) bool {
		err = encodeUint(buffer, item.(centroid).count)
		return err == nil
	})
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// FromBytes reads a byte buffer with a serialized digest (from AsBytes)
// and deserializes it.
func FromBytes(buf *bytes.Reader) (*TDigest, error) {
	var encoding int32
	err := binary.Read(buf, endianess, &encoding)
	if err != nil {
		return nil, err
	}

	if encoding != smallEncoding {
		return nil, errors.New(fmt.Sprintf("Unsupported encoding version: %d", encoding))
	}

	var compression float64
	err = binary.Read(buf, endianess, &compression)
	if err != nil {
		return nil, err
	}

	t := New(compression)

	var numCentroids int32
	err = binary.Read(buf, endianess, &numCentroids)
	if err != nil {
		return nil, err
	}

	means := make([]float32, numCentroids)
	var i int32
	for i = 0; i < numCentroids; i++ {
		err = binary.Read(buf, endianess, &means[i])
		if err != nil {
			return nil, err
		}
	}

	var x float64 = 0
	for i = 0; i < numCentroids; i++ {
		decUint, err := decodeUint(buf)
		if err != nil {
			return nil, err
		}

		t.Update(float64(means[i])+x, decUint)
		x = float64(means[i])
	}

	return t, nil
}

func encodeUint(buf *bytes.Buffer, n uint32) error {
	var k uint32 = 0
	for n < 0 || n > 0x7f {
		b := byte(0x80 | (0x7f & n))

		err := buf.WriteByte(b)
		if err != nil {
			return err
		}

		n = n >> 7
		k++
		if k >= 6 {
			return errors.New("Tried encoding a number that's too big")
		}
	}

	err := buf.WriteByte(byte(n))
	if err != nil {
		return err
	}

	return nil
}

func decodeUint(buf *bytes.Reader) (uint32, error) {
	v, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}

	var z uint32 = 0x7f & uint32(v)
	var shift uint32 = 7
	for v&0x80 != 0 {
		if shift > 28 {
			return 0, errors.New("Something wrong, this number looks too big")
		}

		v, err = buf.ReadByte()
		if err != nil {
			return 0, err
		}

		z += uint32((v & 0x7f)) << shift
		shift += 7
	}

	return z, nil
}
