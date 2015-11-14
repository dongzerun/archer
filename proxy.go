package archer

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	log "github.com/ngaut/logging"
)

type Proxy struct {
	l net.Listener // 监听 Listener

	filter Filter // Redis 有效协议检测过滤器

	pc *ProxyConfig // 全局配置文件

	sm *SessMana // Session 管理

	cluster *Cluster // 集群实现
}

func NewProxy(pc *ProxyConfig) *Proxy {
	p := &Proxy{
		sm:      newSessMana(pc.idleTimeout),
		cluster: NewCluster(pc),
		filter:  &StrFilter{},
		pc:      pc,
	}

	// listen 放到最后
	l, err := net.Listen("tcp4", fmt.Sprintf(":%d", pc.port))
	if err != nil {
		log.Fatalf("Proxy Listen  %d failed %s", pc.port, err.Error())
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
	p.sm.Put(c.RemoteAddr().String(), s)
	s.Serve()
	log.Warning("Close client ", c.RemoteAddr().String())
	c.Close()
}
