package util

import (
	"errors"
	"strconv"
)

// var scratchPool = sync.Pool{
// 	New: func() interface{} { return make([]byte, 12) },
// }

func Itob(i int) []byte {
	return []byte(strconv.Itoa(i))
}

func Iu32tob(i int) []byte {
	return strconv.AppendUint(nil, uint64(i), 10)
}

func Iu32tob2(i int) []byte {
	buf := make([]byte, 10)
	idx := len(buf) - 1
	for i >= 10 {
		buf[idx] = byte('0' + i%10)
		i = i / 10
		idx--
	}
	buf[idx] = byte('0' + i)
	return buf[idx:]
}

func ParseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, errors.New("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, errors.New("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func LowerSlice(buf []byte) []byte {
	for i, r := range buf {
		if 'A' <= r && r <= 'Z' {
			r += 'a' - 'A'
		}

		buf[i] = r
	}
	return buf
}

func UpperSlice(buf []byte) []byte {
	for i, r := range buf {
		if 'a' <= r && r <= 'z' {
			r -= 'a' - 'A'
		}

		buf[i] = r
	}
	return buf
}
