package main

import (
	"flag"

	"github.com/dongzerun/archer"
)

var (
	cfg = flag.String("config_file", "example.conf", "archer proxy config file")
)

func main() {
	flag.Parse()
	pc := archer.NewProxyConfig(*cfg)
	p := archer.NewProxy(pc)
	p.Start()
}
