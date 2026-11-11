//go:build tinygo

package zenoh

// gzipBytes: no compressor on microcontrollers (its tables need more memory
// than they have); metrics go out uncompressed.
func gzipBytes([]byte) ([]byte, bool) { return nil, false }
