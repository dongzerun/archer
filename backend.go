package archer

import (
	"bufio"
	"fmt"
	"net"

	log "github.com/ngaut/logging"
)

type Backend struct {
	c net.Conn
	w *bufio.Writer
	r *bufio.Reader
}

func NewBackend(host string, port int) *Backend {
	b := &Backend{}
	c, err := net.Dial("tcp4", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("Backend Dial  %s:%d failed %s", host, port, err)
	}

	tcpc, ok := c.(*net.TCPConn)
	if ok {
		tcpc.SetNoDelay(false)
		b.c = tcpc
	} else {
		b.c = c
	}
	b.w = bufio.NewWriter(b.c)
	b.r = bufio.NewReader(b.c)
	return b
}
