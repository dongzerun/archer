package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/dongzerun/simpleproxy"
	log "github.com/ngaut/logging"
)

var (
	host       = flag.String("redis_host", "127.0.0.1", "single redis instance hostname, default localhost")
	port       = flag.Int("redis_port", 6379, "single redis instance port, default 6379")
	pport      = flag.Int("proxy_port", 6000, "proxy port listned on")
	cpuprofile = flag.String("cpuprofile", "/tmp/cpuprofile", "write cpu profile to file")
	memprofile = flag.String("memprofile", "/tmp/memprofile", "write cpu profile to file")
)

func main() {
	flag.Parse()
	log.Info("flag parse: ", *host, *port, *pport)

	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		for {
			log.Info("Got goroutine ", runtime.NumGoroutine())
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	// if *cpuprofile != "" {
	// 	f, err := os.Create(*cpuprofile)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	pprof.StartCPUProfile(f)
	// 	defer pprof.StopCPUProfile()
	// }

	// if *memprofile != "" {
	// 	f, err := os.Create(*memprofile)
	// 	if err != nil {
	// 		pprof.WriteHeapProfile(f)
	// 	}
	// }
	go func() {
		log.Info(http.ListenAndServe(":6061", nil))
	}()
	p := simpleproxy.NewProxy(*port, *host, *pport)
	p.Start()
}
