package span

// hexDigits is the lookup table for hex conversion
const hexDigits = "0123456789abcdef"

// Uint64ToHex converts a uint64 to a 16-character hex string without allocations.
// This is much faster than fmt.Sprintf("%016x", val).
// The buffer must be at least 16 bytes long.
func Uint64ToHex(val uint64, buf []byte) {
	_ = buf[15] // bounds check hint to eliminate bounds checks in loop
	for i := 15; i >= 0; i-- {
		buf[i] = hexDigits[val&0xf]
		val >>= 4
	}
}

// FormatSpanID formats a span ID from uint64 to 16-character hex string.
func FormatSpanID(spanID uint64) string {
	var buf [16]byte
	Uint64ToHex(spanID, buf[:])
	return string(buf[:])
}

// FormatTraceID formats a trace ID from hi/lo uint64 components to 32-character hex string.
func FormatTraceID(hi, lo uint64) string {
	var buf [32]byte
	Uint64ToHex(hi, buf[:16])
	Uint64ToHex(lo, buf[16:])
	return string(buf[:])
}

// Note: ParseTraceID and ParseSpanID functions already exist in binary.go
// and use strconv.ParseUint for parsing hex strings.
