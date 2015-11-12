package archer

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	log "github.com/ngaut/logging"
)

var (
	ClusterNodes = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n") // cluster nodes
	Ping         = []byte("*1\r\n$4\r\nPING\r\n")
)

type RedisConn struct {
	id string
	c  net.Conn
	w  *bufio.Writer
	r  *bufio.Reader

	lastUsed time.Time

	readTimeout  time.Duration
	writeTimeout time.Duration

	closed bool
}

func NewRedisConn(host string, port int) (*RedisConn, error) {
	conn := &RedisConn{}
	c, err := net.Dial("tcp4", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("Backend Dial  %s:%d failed %s", host, port, err)
	}

	conn.w = bufio.NewWriter(c)
	conn.r = bufio.NewReader(c)
	return conn, nil
}

func RedisConnDialer(host string, port int, id string, pc *ProxyConfig) func() (Conn, error) {
	return func() (Conn, error) {
		var c net.Conn
		var err error

		if pc.dialTimeout > 0 {
			c, err = net.DialTimeout("tcp4", fmt.Sprintf("%s:%d", host, port), pc.dialTimeout)
		} else {
			c, err = net.Dial("tcp4", fmt.Sprintf("%s:%d", host, port))
		}
		if err != nil {
			return nil, err
		}

		conn := &RedisConn{
			id:           id,
			c:            c,
			w:            bufio.NewWriter(c),
			r:            bufio.NewReader(c),
			readTimeout:  pc.readTimeout,
			writeTimeout: pc.writeTimeout,
		}
		return conn, nil
	}
}

func (c *RedisConn) Close() error {
	if c.closed {
		return nil
	}

	if c.c != nil {
		return c.c.Close()
	}
	return nil
}

func (c *RedisConn) Discard() error {
	return nil
}

func (c *RedisConn) LastUsed() time.Time {
	return c.lastUsed
}

func (c *RedisConn) SetLastUsed(t time.Time) {
	c.lastUsed = t
}

func (c *RedisConn) ID() string {
	return c.id
}

func (c *RedisConn) Alive() bool {
	if c.Ping() {
		return true
	}
	return false
}

func (c *RedisConn) Ping() bool {
	_, err := c.w.Write(ClusterNodes)
	if err != nil {
		return false
	}

	err = c.w.Flush()
	if err != nil {
		return false
	}
	r, e := ReadProtocol(c.r)

	if e != nil {
		return false
	}

	sr, ok := r.(*SimpleResp)
	if !ok {
		return false
	}

	if bytes.Equal(sr.Args[0], PONG) {
		return true
	}
	return false
}

func GetClusterNodes(host string, port int) ([]*Node, error) {
	c, err := NewRedisConn(host, port)
	if err != nil {
		return nil, err
	}

	_, err = c.w.Write(ClusterNodes)
	if err != nil {
		return nil, err
	}

	err = c.w.Flush()
	if err != nil {
		return nil, err
	}
	r, e := ReadProtocol(c.r)

	if e != nil {
		return nil, e
	}

	br, ok := r.(*BulkResp)
	if !ok {
		return nil, errors.New("wrong resp type, respect BulkResp in GetClusterNodes")
	}

	if br.Empty {
		return nil, errors.New("Cluster nodes Command failed")
	}

	lines := strings.Split(string(br.Args[0]), "\r\n")
	// 96ea3677b33334fb27382a08e475571a48342db0 10.10.10.86:6592 slave 4382646a92a3949bb9fdcfdc5a383e5e4b20a849 0 1447149668244 57 connected
	// 219dfcf127e995244a43a5d57d95ea5f55b69c07 10.10.10.96:6595 master - 0 1447149668743 44 connected 3736-3939
	ns := make([]*Node, 0)
	for _, l := range lines {
		n := &Node{}
		fields := strings.Fields(l)
		log.Info("GetClusterNodes fields: ", fields)
		if len(fields) != 8 && len(fields) != 9 {
			return nil, errors.New("Cluster nodes fileds wrong")
		}

		n.id = fields[1]

		url := strings.Split(fields[1], ":")

		if len(url) != 2 {
			return nil, errors.New("cluster nodes url wrong")
		}

		n.host = url[0]
		var err error
		n.port, err = strconv.Atoi(url[1])
		if err != nil {
			return nil, errors.New("cluster nodes port wrong")
		}

		if strings.Contains(fields[2], "slave") {
			n.role = "slave"
			n.slaveOf = fields[3]
		} else {
			n.role = "master"
			slot := strings.Split(fields[8], "-")

			var err error
			n.serveSlot.start, err = strconv.Atoi(slot[0])
			if err != nil {
				return nil, errors.New("cluster nodes serve slots wrong")
			}

			n.serveSlot.stop, err = strconv.Atoi(slot[1])
			if err != nil {
				return nil, errors.New("cluster nodes serve slots wrong")
			}
		}

		ns = append(ns, n)
	}
	return ns, nil
}
