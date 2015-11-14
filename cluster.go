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
		pools: make(map[string]*ConnPool, 1),
		opts:  make(map[string]*Options, 1),
		topo:  NewTopo(pc),
	}
	c.initializePool()
	return c
}

func (c *Cluster) GetConn(key []byte, slave bool) (Conn, error) {
	id := c.topo.GetNodeID(key, slave)
	log.Infof("GetConn %s for key: %s", id, string(key))

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
	log.Info("Cluster start initializePool ", len(c.pc.nodes))
	nodes := make(map[string]*Node, len(c.pc.nodes))
	for _, s := range c.topo.slots {
		if s.master != nil {
			nodes[s.master.id] = s.master
		}
		for _, n := range s.slaves {
			nodes[n.id] = n
		}
	}

	log.Info("initializePool nodes len ", len(nodes))
	for _, n := range nodes {
		log.Info("Cluster nodes ", n.id)
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
		//test
		testConn, err := c.pools[n.id].Get()
		if err != nil {
			log.Warning("test pool failed ", err)
			continue
		}
		c.pools[n.id].Put(testConn)
	}
	log.Info("Cluster initializePool done")
}
