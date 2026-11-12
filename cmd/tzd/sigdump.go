//go:build !tinygo && unix

package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

// SIGUSR1 dumps every goroutine's stack to stderr, to see what a router
// that seems stuck is doing (kill -USR1 <pid>). SIGUSR2 prints the Go
// runtime's memory figures to stderr and writes a heap profile to
// $TMPDIR/tzd-heap-<pid>.pprof (go tool pprof reads it).
func init() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for sig := range ch {
			if sig == syscall.SIGUSR2 {
				memDump()
				continue
			}
			buf := make([]byte, 8<<20)
			n := runtime.Stack(buf, true)
			_, _ = os.Stderr.Write(buf[:n])
		}
	}()
}

func memDump() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	const mi = 1 << 20
	f := func(v uint64) string { return fmt.Sprintf("%.1f", float64(v)/mi) }
	fmt.Fprintf(os.Stderr, "mem: sys %s MiB; heap alloc %s inuse %s idle %s released %s; stacks %s; mspan %s mcache %s; gc meta %s other %s buckhash %s; next gc %s; goroutines %d; gcs %d\n",
		f(m.Sys), f(m.HeapAlloc), f(m.HeapInuse), f(m.HeapIdle), f(m.HeapReleased), f(m.StackInuse),
		f(m.MSpanInuse), f(m.MCacheInuse), f(m.GCSys), f(m.OtherSys), f(m.BuckHashSys), f(m.NextGC),
		runtime.NumGoroutine(), m.NumGC)
	path := fmt.Sprintf("%s/tzd-heap-%d.pprof", os.TempDir(), os.Getpid())
	out, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mem:", err)
		return
	}
	defer out.Close()
	if err := pprof.Lookup("heap").WriteTo(out, 0); err != nil {
		fmt.Fprintln(os.Stderr, "mem:", err)
		return
	}
	fmt.Fprintln(os.Stderr, "mem: heap profile in", path)
}
