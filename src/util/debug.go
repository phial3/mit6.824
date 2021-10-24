package util

import (
	"net/http"
	"net/http/pprof"
)

const (
	pprofAddr string = ":7890"
)

//启动一个pprof的端口
//访问http://localhost:7890/debug/pprof/

type Debugger struct {
}

func (d *Debugger) StartHTTPDebugger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}
