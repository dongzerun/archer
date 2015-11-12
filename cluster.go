package archer

import (
	"fmt"

	log "github.com/ngaut/logging"
)

type Cluster struct {
	pc    *ProxyConfig
	pools map[string]*ConnPool //key: node id host:port
	opts  map[string]*Options

	topo *Topology
}

func NewCluster(pc *ProxyConfig) *Cluster {
	c := &Cluster{
		pc:    pc,
		pools: make(map[string]*ConnPool),
		opts:  make(map[string]*Options),
		topo:  NewTopo(pc),
	}
	c.initializePool()
	return c
}

func (c *Cluster) GetConn(key []byte, slave bool) (Conn, error) {
	id := c.topo.GetNodeID(key, slave)

	pool, ok := c.pools[id]
	if !ok {
		// opt一定存在要做个判断
		opt := c.opts[id]
		if opt == nil {
			n := c.topo.GetNode(id)
			if n == nil {
				return nil, fmt.Errorf("Cluster GetConn ID %s not exists ", id)
			}

			opt := &Options{
				Network:      "tcp",
				Addr:         fmt.Sprintf("%s:%d", n.host, n.port),
				Dialer:       RedisConnDialer(n.host, n.port, n.id, c.pc),
				DialTimeout:  c.pc.dialTimeout,
				ReadTimeout:  c.pc.readTimeout,
				WriteTimeout: c.pc.writeTimeout,
				PoolSize:     c.pc.poolSize,
				IdleTimeout:  c.pc.idleTimeout,
			}
			c.opts[id] = opt
		}
		pool = NewConnPool(opt)
		c.pools[id] = pool
	}

	return pool.Get()
}

func (c *Cluster) PutConn(cn Conn) {
	pool, ok := c.pools[cn.ID()]
	if !ok {
		log.Warningf("Cluster PutConn %s, belong no pool", cn.ID())
		return
	}
	pool.Put(cn)
}

// initialize conn Pool before Serve
func (c *Cluster) initializePool() {
	log.Info("Cluster start initializePool")
	nodes := make([]*Node, len(c.pc.nodes))
	for _, s := range c.topo.slots {
		if s.master != nil {
			nodes = append(nodes, s.master)
		}
		for _, n := range s.slaves {
			nodes = append(nodes, n)
		}
	}

	for _, n := range nodes {
		_, ok := c.pools[n.id]
		if ok {
			log.Fatalf("Cluster initializePool duplicate %s %s:%d", n.id, n.host, n.port)
		}

		opt := &Options{
			Network:      "tcp",
			Addr:         fmt.Sprintf("%s:%d", n.host, n.port),
			Dialer:       RedisConnDialer(n.host, n.port, n.id, c.pc),
			DialTimeout:  c.pc.dialTimeout,
			ReadTimeout:  c.pc.readTimeout,
			WriteTimeout: c.pc.writeTimeout,
			PoolSize:     c.pc.poolSize,
			IdleTimeout:  c.pc.idleTimeout,
		}

		c.pools[n.id] = NewConnPool(opt)
		c.opts[n.id] = opt
	}
	log.Info("Cluster initializePool done")
}
