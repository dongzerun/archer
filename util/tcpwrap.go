package util

import (
	"net"
	"time"
)

// Conn wraps a net.Conn, and sets a deadline for every read
// and write operation.
type Conn struct {
	net.Conn
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// 重写 Read 方法
func (c *Conn) Read(b []byte) (count int, e error) {
	if c.ReadTimeout > 0 {
		err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout))

		if err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(b)
}

// 重写 Write 方法
func (c *Conn) Write(b []byte) (count int, e error) {
	if c.ReadTimeout > 0 {
		err := c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
		if err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(b)

}

// 重写 Close
func (c *Conn) Close() error {
	return c.Conn.Close()
}
