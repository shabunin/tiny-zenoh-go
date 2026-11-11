//go:build linux && !tinygo && (amd64 || arm64 || 386 || arm || riscv64 || loong64)

package link

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

// Linux tty setup with the asm-generic termios ioctls and flag values (the
// syscall package doesn't export them on every architecture).
const (
	ioTCGETS   = 0x5401
	ioTCSETS   = 0x5402
	ioTCFLSH   = 0x540b
	ioTIOCEXCL = 0x540c
	ioTIOCNXCL = 0x540d
	tcIOFLUSH  = 2

	tIGNBRK = 0x1
	tBRKINT = 0x2
	tPARMRK = 0x8
	tISTRIP = 0x20
	tINLCR  = 0x40
	tIGNCR  = 0x80
	tICRNL  = 0x100
	tIXON   = 0x400
	tIXOFF  = 0x1000
	tOPOST  = 0x1
	tISIG   = 0x1
	tICANON = 0x2
	tECHO   = 0x8
	tECHONL = 0x40
	tIEXTEN = 0x8000
	tCSIZE  = 0x30
	tCS8    = 0x30
	tCSTOPB = 0x40
	tCREAD  = 0x80
	tPARENB = 0x100
	tCLOCAL = 0x800
	tCBAUD  = 0x100f
	tCRTS   = 0x80000000
	tVTIME  = 5
	tVMIN   = 6
)

var ttyBauds = map[int]uint32{
	1200: 0x9, 2400: 0xb, 4800: 0xc, 9600: 0xd, 19200: 0xe, 38400: 0xf,
	57600: 0x1001, 115200: 0x1002, 230400: 0x1003, 460800: 0x1004,
	500000: 0x1005, 576000: 0x1006, 921600: 0x1007, 1000000: 0x1008,
	1152000: 0x1009, 1500000: 0x100a, 2000000: 0x100b, 2500000: 0x100c,
	3000000: 0x100d, 3500000: 0x100e, 4000000: 0x100f,
}

func ioctl(fd uintptr, req uintptr, arg unsafe.Pointer) error {
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, req, uintptr(arg)); e != 0 {
		return e
	}
	return nil
}

// openSerialPort opens a tty in raw 8N1 mode at baud, flushing stale input.
func openSerialPort(path string, baud int, exclusive bool) (io.ReadWriteCloser, error) {
	speed, ok := ttyBauds[baud]
	if !ok {
		return nil, fmt.Errorf("link: unsupported serial baudrate %d", baud)
	}
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOCTTY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return nil, err
	}
	var ierr error
	err = rc.Control(func(fd uintptr) {
		var t syscall.Termios
		if ierr = ioctl(fd, ioTCGETS, unsafe.Pointer(&t)); ierr != nil {
			return
		}
		t.Iflag &^= tIGNBRK | tBRKINT | tPARMRK | tISTRIP | tINLCR | tIGNCR | tICRNL | tIXON | tIXOFF
		t.Oflag &^= tOPOST
		t.Lflag &^= tECHO | tECHONL | tICANON | tISIG | tIEXTEN
		t.Cflag &^= tCSIZE | tPARENB | tCSTOPB | tCBAUD | tCRTS
		t.Cflag |= tCS8 | tCREAD | tCLOCAL | speed
		t.Cc[tVMIN], t.Cc[tVTIME] = 1, 0
		if ierr = ioctl(fd, ioTCSETS, unsafe.Pointer(&t)); ierr != nil {
			return
		}
		if exclusive {
			if ierr = ioctl(fd, ioTIOCEXCL, nil); ierr != nil {
				return
			}
		}
		_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, fd, ioTCFLSH, tcIOFLUSH)
	})
	if err == nil {
		err = ierr
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("link: configure %s: %w", path, err)
	}
	if exclusive {
		return &ttyFile{File: f, rc: rc}, nil
	}
	return f, nil
}

// ttyFile drops exclusive mode when closed: it belongs to the tty, not to
// our descriptor, and would otherwise lock everyone out until the device's
// last close (as serialport-rs does).
type ttyFile struct {
	*os.File
	rc syscall.RawConn
}

func (t *ttyFile) Close() error {
	_ = t.rc.Control(func(fd uintptr) { _ = ioctl(fd, ioTIOCNXCL, nil) })
	return t.File.Close()
}
