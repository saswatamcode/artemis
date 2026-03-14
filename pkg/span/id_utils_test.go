package span

import (
	"testing"
)

func TestUint64ToHex(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{"zero", 0, "0000000000000000"},
		{"small", 42, "000000000000002a"},
		{"medium", 0xdeadbeef, "00000000deadbeef"},
		{"large", 0x123456789abcdef0, "123456789abcdef0"},
		{"max", 0xffffffffffffffff, "ffffffffffffffff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf [16]byte
			Uint64ToHex(tt.input, buf[:])
			result := string(buf[:])
			if result != tt.expected {
				t.Errorf("Uint64ToHex(%x) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatSpanID(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{"zero", 0, "0000000000000000"},
		{"small", 42, "000000000000002a"},
		{"hex_digits", 0x123456789abcdef0, "123456789abcdef0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatSpanID(tt.input)
			if result != tt.expected {
				t.Errorf("FormatSpanID(%x) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatTraceID(t *testing.T) {
	tests := []struct {
		name     string
		hi       uint64
		lo       uint64
		expected string
	}{
		{"zero", 0, 0, "00000000000000000000000000000000"},
		{"hi_only", 0x123456789abcdef0, 0, "123456789abcdef00000000000000000"},
		{"lo_only", 0, 0xfedcba9876543210, "0000000000000000fedcba9876543210"},
		{"both", 0x123456789abcdef0, 0xfedcba9876543210, "123456789abcdef0fedcba9876543210"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatTraceID(tt.hi, tt.lo)
			if result != tt.expected {
				t.Errorf("FormatTraceID(%x, %x) = %s, want %s", tt.hi, tt.lo, result, tt.expected)
			}
		})
	}
}

// Benchmark to ensure no performance regression
func BenchmarkUint64ToHex(b *testing.B) {
	var buf [16]byte
	val := uint64(0x123456789abcdef0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Uint64ToHex(val, buf[:])
	}
}

func BenchmarkFormatSpanID(b *testing.B) {
	val := uint64(0x123456789abcdef0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatSpanID(val)
	}
}

func BenchmarkFormatTraceID(b *testing.B) {
	hi := uint64(0x123456789abcdef0)
	lo := uint64(0xfedcba9876543210)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatTraceID(hi, lo)
	}
}
