package simpleproxy

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	log "github.com/ngaut/logging"
)

type Proxy struct {
	l net.Listener
	h string
	p int
}

func NewProxy(port int, host string, pport int) *Proxy {
	p := &Proxy{}
	p.h = host
	p.p = port

	l, err := net.Listen("tcp4", fmt.Sprintf(":%d", pport))
	if err != nil {
		log.Fatalf("Proxy Listen  %d failed %s", pport, err)
	}
	p.l = l
	return p
}

func (p *Proxy) Start() {
	for {
		c, err := p.l.Accept()
		if err != nil {
			log.Warning("got error when Accept network connect ", err)
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Warningf("NOTICE: temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Warningf("ERROR: listener.Accept() - %s", err)
			}
			break
		}

		go HandleConn(p, c)
	}
}

func HandleConn(p *Proxy, c net.Conn) {
	s := NewSession(p, c)
	s.Serve()
	log.Warning("Close client ", c.RemoteAddr().String())
	c.Close()
}
