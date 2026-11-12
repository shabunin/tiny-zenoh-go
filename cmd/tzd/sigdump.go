//go:build !tinygo && unix

package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

// SIGUSR1 dumps every goroutine's stack to stderr, to see what a router
// that seems stuck is doing (kill -USR1 <pid>).
func init() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		buf := make([]byte, 8<<20)
		for range ch {
			n := runtime.Stack(buf, true)
			_, _ = os.Stderr.Write(buf[:n])
		}
	}()
}
