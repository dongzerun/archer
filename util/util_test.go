package util

import (
	"testing"
)

func Benchmark_Itob(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Itob(i)
	}
}

func Benchmark_Iu32tob(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Iu32tob(i)
	}
}

func Benchmark_Iu32tob2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Iu32tob2(i)
	}
}
