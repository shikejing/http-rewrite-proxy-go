// Command http-rewrite-proxy-go 是可配置的请求转发与改写层。
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/runtime"
)

func main() {
	addr := flag.String("addr", ":8080", "监听地址")
	cfgPath := flag.String("config", "config.json", "配置文件路径")
	flag.Parse()

	rt, err := runtime.New(*cfgPath)
	if err != nil {
		log.Printf("初始配置加载失败，将以空配置启动: %v", err)
	}
	defer rt.Close()

	mux := http.NewServeMux()
	mux.Handle("/admin/", rt.AdminHandler())
	mux.Handle("/", rt.Handler())

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("转发代理监听 %s", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("服务退出: %v", err)
	}
}
