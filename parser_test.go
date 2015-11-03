package simpleproxy

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestSimpleResp(t *testing.T) {
	sr := &SimpleResp{}
	sr.Args = [][]byte{[]byte("ok")}
	sr.Rtype = SimpleType

	if sr.String() != "ok" {
		t.Errorf("SimpleResp String Error %s  expected ok", sr.String())
	}

	res := []byte("+ok\r\n")
	if !bytes.Equal(res, sr.Encode()) {
		t.Errorf("SimpleResp Encode Error %s expected %s", sr.Encode(), res)
	}
}

func TestErrorResp(t *testing.T) {
	er := &ErrorResp{}
	er.Args = [][]byte{[]byte("test error not support")}
	er.Rtype = ErrorType

	if er.String() != "test error not support" {
		t.Errorf("ErrorResp String Error %s expected test error not support", er.String())
	}

	res := []byte("-test error not support\r\n")
	if !bytes.Equal(res, er.Encode()) {
		t.Errorf("ErrorResp Encode Error %s expected %s", er.Encode(), res)
	}
}

func TestIntResp(t *testing.T) {
	ir := &IntResp{}
	ir.Args = [][]byte{[]byte("10")}
	ir.Rtype = IntType

	if ir.String() != "10" {
		t.Errorf("IntResp String Error %s expected 10", ir.String())
	}

	res := []byte(":10\r\n")
	if !bytes.Equal(res, ir.Encode()) {
		t.Errorf("IntResp Encode Error %s expected %s", ir.Encode(), res)
	}
}

func TestBulkResp(t *testing.T) {
	br := &BulkResp{}
	br.Args = [][]byte{[]byte("hello world")}
	br.Rtype = BulkType

	if br.String() != "hello world" {
		t.Errorf("BulkResp String Error %s expected hello world", br.String())
	}

	res := []byte("$11\r\nhello world\r\n")
	if !bytes.Equal(res, br.Encode()) {
		t.Errorf("BulkResp Encode Error %s expected %s", br.Encode(), res)
	}

	br1 := &BulkResp{}
	br1.Rtype = BulkType
	br1.Empty = true

	if br1.String() != "" {
		t.Errorf("BulkResp Empty String Error %s expected EMPTY", br1.String())
	}

	if !bytes.Equal(br1.Encode(), []byte("$-1\r\n")) {
		t.Errorf("BulkResp Encode Empty Error %s expected $-1\r\n", br.Encode())
	}
}

func TestArrayResp(t *testing.T) {
	br := &BulkResp{}
	br.Args = [][]byte{[]byte("hello")}
	br.Rtype = BulkType

	br1 := &BulkResp{}
	br1.Args = [][]byte{[]byte("world")}
	br1.Rtype = BulkType

	br2 := &BulkResp{}
	br2.Args = [][]byte{[]byte("wocao\r\nzhaha")}
	br2.Rtype = BulkType

	br3 := &BulkResp{}
	br3.Rtype = BulkType
	br3.Empty = true

	ar := &ArrayResp{}
	ar.Rtype = ArrayType
	ar.Args = append(ar.Args, br, br1, br2, br3)

	res := []byte("*4\r\n$5\r\nhello\r\n$5\r\nworld\r\n$12\r\nwocao\r\nzhaha\r\n$-1\r\n")
	if !bytes.Equal(res, ar.Encode()) {
		t.Errorf("ArrayResp Encode  Error %s expected %s", ar.Encode(), res)
	}
}

func TestReadProtocol(t *testing.T) {
	data := []byte("$4\r\nabcd\r\n$4\r\na\r\na\r\n:1234\r\n+ok\r\n-error not found\r\n*4\r\n$1\r\na\r\n$-1\r\n$4\r\nb\r\nb\r\naaa")
	b := bytes.NewBuffer(data)
	for {
		r, err := ReadProtocol(bufio.NewReader(b))
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf("TestReadProtocol read error %s", err)
		}
		t.Log("TestReadProtocol read bytes ", string(r.Encode()))
	}
}
