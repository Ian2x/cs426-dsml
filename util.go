package util

import (
    "encoding/binary"
    "math"
)

// Float64SliceToByteArray converts a slice of float64 to a byte array
func Float64SliceToByteArray(floats []float64) []byte {
    bytes := make([]byte, len(floats)*8)
    for i, f := range floats {
        binary.LittleEndian.PutUint64(bytes[i*8:], math.Float64bits(f))
    }
    return bytes
}

// ByteArrayToFloat64Slice converts a byte array to a slice of float64
func ByteArrayToFloat64Slice(data []byte) []float64 {
    floats := make([]float64, len(data)/8)
    for i := 0; i < len(floats); i++ {
        bits := binary.LittleEndian.Uint64(data[i*8:])
        floats[i] = math.Float64frombits(bits)
    }
    return floats
}