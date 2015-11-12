package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/dongzerun/archer"
	log "github.com/ngaut/logging"
)

var (
	cfg = flag.String("config_file", "example.conf", "archer proxy config file")
)

func main() {
	flag.Parse()

	pc := archer.NewProxyConfig(*cfg)

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
	p := archer.NewProxy(pc)
	p.Start()
}
