package util

import (
    "encoding/binary"
    "math"
    "fmt"
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

// AddFloat64Slices adds two float64 slices element-wise
func AddFloat64Slices(a, b []float64) []float64 {
    if len(a) != len(b) {
        fmt.Println("Input slices must have the same length")
        return nil
    }
    result := make([]float64, len(a))
    for i := range a {
        result[i] = a[i] + b[i]
    }
    return result
}

// MultiplyFloat64Slices multiplies two float64 slices element-wise
func MultiplyFloat64Slices(a, b []float64) []float64 {
    if len(a) != len(b) {
        fmt.Println("Input slices must have the same length")
        return nil
    }
    result := make([]float64, len(a))
    for i := range a {
        result[i] = a[i] * b[i]
    }
    return result
}

// MinFloat64Slices computes the element-wise minimum of two float64 slices
func MinFloat64Slices(a, b []float64) []float64 {
    if len(a) != len(b) {
        fmt.Println("Input slices must have the same length")
        return nil
    }
    result := make([]float64, len(a))
    for i := range a {
        result[i] = math.Min(a[i], b[i])
    }
    return result
}

// MaxFloat64Slices computes the element-wise maximum of two float64 slices
func MaxFloat64Slices(a, b []float64) []float64 {
    if len(a) != len(b) {
        fmt.Println("Input slices must have the same length")
        return nil
    }
    result := make([]float64, len(a))
    for i := range a {
        result[i] = math.Max(a[i], b[i])
    }
    return result
}

type DeviceConfig struct {
    DeviceID   uint64 `json:"deviceId"`
    IPAddress  string `json:"ipAddress"`
    Port       uint64 `json:"port"`
    MinMemAddr uint64 `json:"minMemAddr,omitempty"` // Optional, omitempty if not set
    MaxMemAddr uint64 `json:"maxMemAddr,omitempty"` // Optional, omitempty if not set
}

type CoordinatorConfig struct {
    IPAddress  string `json:"ipAddress"`
    Port       uint64 `json:"port"`
}

// CustomError represents an error that carries a number
type DeviceError struct {
    Msg   string
    Rank    uint32
}

// Error implements the error interface
func (e *DeviceError) Error() string {
    return fmt.Sprintf("%s (Rank: %d)", e.Msg, e.Rank)
}

// WrapError creates a new CustomError
func DeviceErrorf(msg string, rank uint32) *DeviceError {
    return &DeviceError{
        Msg:  msg,
        Rank: rank,
    }
}
