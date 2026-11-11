//go:build !linux || tinygo || !(amd64 || arm64 || 386 || arm || riscv64 || loong64)

package link

import (
	"errors"
	"io"
)

// openSerialPort is only implemented for Linux ttys; elsewhere (and on
// TinyGo) wrap your port with NewSerial and supply it via a custom Dial.
func openSerialPort(string, int, bool) (io.ReadWriteCloser, error) {
	return nil, errors.New("link: serial devices can't be opened by name on this platform; use NewSerial")
}
