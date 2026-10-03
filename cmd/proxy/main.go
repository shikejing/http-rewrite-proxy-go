package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	proxy "github.com/shikejing/http-rewrite-proxy-go/proxy"
)

func loadConfig(path string) (*proxy.Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg proxy.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	confPath := flag.String("config", "config.json", "config file path")
	flag.Parse()

	cfg, err := loadConfig(*confPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	p, err := proxy.NewProxy(cfg)
	if err != nil {
		log.Fatalf("new proxy: %v", err)
	}
	defer p.Close()

	// SIGHUP 触发热更新；失败时保留旧配置。
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	go func() {
		for range sig {
			cfg, err := loadConfig(*confPath)
			if err != nil {
				log.Printf("reload: load config failed, keep old: %v", err)
				continue
			}
			if err := p.UpdateConfig(cfg); err != nil {
				log.Printf("reload: apply failed, keep old: %v", err)
				continue
			}
			log.Printf("reload: config applied")
		}
	}()

	log.Printf("listening on %s", *addr)
	srv := &http.Server{Addr: *addr, Handler: p}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
