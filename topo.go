package archer

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dongzerun/archer/util"
	log "github.com/ngaut/logging"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Node struct {
	id        string // 0799a0354fe57b79b32122c2b1231ba43f09832d
	host      string
	port      int
	role      string
	serveSlot *SlotRange // 0-16384
	slaveOf   string
}

type SlotRange struct {
	start int
	stop  int
}

type Slot struct {
	id     int     // slot id integer
	master *Node   // master
	slaves []*Node // slaves
}

type Topology struct {
	conf  *ProxyConfig
	rw    sync.RWMutex
	slots []*Slot

	reloadChan chan int
}

func NewTopo(pc *ProxyConfig) *Topology {
	t := &Topology{
		conf:       pc,
		reloadChan: make(chan int, 1),
		slots:      make([]*Slot, 1),
	}

	t.reloadSlots()
	go t.ReloadLoop()
	return t
}

func (t *Topology) ReloadLoop() {
	ticker := time.NewTicker(t.conf.reloadSlot * time.Second)
	for {
		select {
		case <-ticker.C:
			t.reloadSlots()
		case <-t.reloadChan:
			t.reloadSlots()
		}
	}

}

func (t *Topology) reloadSlots() {
	ss, err := t.getSlots()
	if err != nil {
		log.Warningf("ReloadLoop failed ", err)
		return
	}

	t.rw.Lock()
	t.slots = ss
	t.rw.Unlock()
}

// 从配置中随机挑选一个节点，调用 cluster nodes 命令获取slot信息
// 失败的话应该有重试，并且将失败的节点踢出掉
func (t *Topology) getSlots() ([]*Slot, error) {
	var nodes []*Node
	for i := 0; i < 3; i++ {
		idx := rand.Intn(len(t.conf.nodes))

		url := strings.Split(t.conf.nodes[idx], ":")
		if len(url) != 2 {
			return nil, errors.New("loadSlots read nodes failed")
		}
		var (
			err  error
			port int
		)
		port, err = strconv.Atoi(url[1])
		if err != nil {
			return nil, fmt.Errorf("Topology getSlots port failed %s ", err.Error())
		}
		nodes, err = GetClusterNodes(url[0], port)
		if err == nil {
			break
		}

		log.Warningf("getSlots failed, kick off url %s for reason %s", t.conf.nodes[idx], err.Error())
		t.conf.kickOff = append(t.conf.kickOff, t.conf.nodes[idx])
		if idx == 0 {
			t.conf.nodes = t.conf.nodes[1:]
		} else if idx == len(t.conf.nodes)-1 {
			t.conf.nodes = t.conf.nodes[:len(t.conf.nodes)-2]
		} else {
			t.conf.nodes = append(t.conf.nodes[:idx-1], t.conf.nodes[idx+1:]...)
		}

	}

	slots := make([]*Slot, 16384)
	slaves := make([]*Node, 0)

	// range master node
	for _, n := range nodes {
		s := &Slot{}
		for i := n.serveSlot.start; i <= n.serveSlot.stop; i++ {
			s.id = i
			if n.role == "master" {
				s.master = n
				slots[i] = s
			} else {
				slaves = append(slaves, n)
			}
		}
	}

	// range slave nodes
	for _, n := range slaves {
		for _, s := range slots {
			if n.slaveOf == s.master.id {
				s.slaves = append(s.slaves, n)
			}
		}
	}

	return slots, nil
}

func (t *Topology) GetNodeID(key []byte, slave bool) string {
	id := util.Crc16sum(key) % 16384

	s := t.slots[id]

	if !slave && s.master != nil {
		return s.master.id
	}

	// default we have only 1 slave
	// TODO:: add more slave RR read
	if slave && len(s.slaves) == 1 {
		return s.slaves[0].id
	}

	return ""
}

func (t *Topology) GetNode(id string) *Node {
	for _, s := range t.slots {
		if s.master != nil && s.master.id == id {
			return s.master
		}

		for _, slave := range s.slaves {
			if slave.id == id {
				return slave
			}
		}
	}

	log.Warning("Topology GetNode Empty, Notify to Reload Topology ")
	t.reloadChan <- 1
	return nil
}
