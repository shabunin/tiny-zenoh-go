package main

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// console is where tzd prints: status lines to out, warnings and errors to
// err. run points it at its writers (tests capture them).
var console = struct {
	sync.Mutex
	out, err io.Writer
}{out: os.Stdout, err: os.Stderr}

// setConsole points the console at out and err and returns the previous
// writers.
func setConsole(out, err io.Writer) (prevOut, prevErr io.Writer) {
	console.Lock()
	defer console.Unlock()
	prevOut, prevErr = console.out, console.err
	console.out, console.err = out, err
	return prevOut, prevErr
}

// printf writes a status line.
func printf(f string, a ...any) {
	console.Lock()
	defer console.Unlock()
	fmt.Fprintf(console.out, f, a...)
}

// errorf writes an error line prefixed with "tzd: ".
func errorf(f string, a ...any) {
	console.Lock()
	defer console.Unlock()
	fmt.Fprintf(console.err, "tzd: "+f+"\n", a...)
}

// warnf reports a configuration problem tzd works around.
func warnf(f string, a ...any) { errorf("config: "+f, a...) }
