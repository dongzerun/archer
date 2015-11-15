package archer

import (
	"bufio"
	"bytes"
	"testing"
)

func Benchmark_ReadProtocol(b *testing.B) {
	res := []byte("*4\r\n$5\r\nhello\r\n$5\r\nworld\r\n$12\r\nwocao\r\nzhaha\r\n$-1\r\n")
	r := bytes.NewBuffer(res)
	for i := 0; i < b.N; i++ {
		ReadProtocol(bufio.NewReader(r))
	}
}
