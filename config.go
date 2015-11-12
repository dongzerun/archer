package archer

import (
	"runtime"
	"strings"
	"time"

	"github.com/astaxie/beego/config"
	log "github.com/ngaut/logging"
)

type ProxyConfig struct {
	//proxy
	name        string
	port        int
	cpu         int
	slaveOk     bool
	maxConn     int
	conCurrency int
	pipeLength  int

	//redis
	nodes      []string
	kickOff    []string //TODO: handle kickOff nodes
	poolSize   int
	reloadSlot time.Duration

	//common
	idleTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialTimeout  time.Duration

	//log
	logLevel string
	logFile  string

	//debug
	cpuFile string
	memFile string
}

func NewProxyConfig(file string) *ProxyConfig {
	c, err := config.NewConfig("ini", file)
	if err != nil {
		log.Fatal("read config file failed ", err)
	}

	pc := &ProxyConfig{}
	// proxy
	pc.name = c.DefaultString("proxy::name", "")
	pc.port = c.DefaultInt("proxy::port", 0)
	pc.cpu = c.DefaultInt("proxy::cpu", 0)
	pc.slaveOk = c.DefaultBool("proxy::slaveok", false)
	pc.maxConn = c.DefaultInt("proxy::maxconn", 4000)
	pc.conCurrency = c.DefaultInt("proxy::concurrency", 5)
	pc.pipeLength = c.DefaultInt("proxy::pipelength", 4096)

	// redis
	pc.poolSize = c.DefaultInt("redis::poolsize", 10)
	pc.nodes = strings.Fields(c.DefaultString("redis::nodes", ""))

	//common
	pc.idleTimeout = time.Duration(c.DefaultInt("common::idletimeout", 30))
	pc.writeTimeout = time.Duration(c.DefaultInt("common::writetimeout", 5))
	pc.readTimeout = time.Duration(c.DefaultInt("common::readtimeout", 5))
	pc.dialTimeout = time.Duration(c.DefaultInt("common::dialtimeout", 3))

	//log
	pc.logFile = c.DefaultString("log::logfile", "")
	pc.logLevel = c.DefaultString("log::loglevel", "info")

	//debug
	pc.cpuFile = c.DefaultString("debug::cpufile", "/tmp/cpufile")
	pc.memFile = c.DefaultString("debug::memfile", "/tmp/memfile")

	pc.apply()
	return pc
}

func (pc *ProxyConfig) apply() {
	log.SetLevelByString(pc.logLevel)

	if pc.logFile != "" {
		err := log.SetOutputByName(pc.logFile)
		if err != nil {
			log.Fatalf("ProxyConfig SetOutputByName %s failed %s ", pc.logFile, err.Error())
		}
		log.SetRotateByDay()
	}

	if pc.name == "" {
		log.Fatal("ProxyConfig name must not empty")
	}

	if pc.port == 0 {
		log.Fatal("ProxyConfig port  must not 0")
	}

	if pc.cpu > runtime.NumCPU() {
		log.Warningf("ProxyConfig cpu  %d exceed %d, adjust to %d ", pc.cpu, runtime.NumCPU(), runtime.NumCPU())
		pc.cpu = runtime.NumCPU()
	}

	if pc.maxConn > 10000 {
		log.Warningf("ProxyConfig maxconn %d exceed 10000, adjust to 10000", pc.maxConn)
		pc.maxConn = 10000
	}

	runtime.GOMAXPROCS(pc.cpu)

	if pc.poolSize <= 0 || pc.poolSize > 30 {
		log.Warning("ProxyConfig poolSize %d , adjust to 10 ", pc.poolSize)
		pc.poolSize = 10
	}

}
