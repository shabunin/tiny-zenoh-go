//go:build tinygo

// Command stickc runs the board tests (examples/boardtest/agent) on an
// M5StickC over its UART, with LZ4 compression and user/password
// authentication. The host side:
//
//	gozenohd -l 'serial//dev/ttyUSB0#baudrate=115200' -e tcp/127.0.0.1:7447 \
//	    --compression --usrpwd-dict users.txt      # users.txt: board:board-test-pw
//	python host.py tcp/127.0.0.1:7447 board/stickc/serial
//
// The UART carries zenoh frames, so the run reports only through zenoh; the
// red LED lights once all steps passed.
package main

import (
	"machine"
	"time"

	"github.com/shabunin/gozenoh/examples/boardtest/agent"
	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/transport"
	"github.com/shabunin/gozenoh/zenoh"
)

type uartPort struct{ u *machine.UART }

func (p uartPort) Read(b []byte) (int, error) {
	for {
		if n, _ := p.u.Read(b); n > 0 {
			return n, nil
		}
		time.Sleep(500 * time.Microsecond)
	}
}

func (p uartPort) Write(b []byte) (int, error) { return p.u.Write(b) }
func (p uartPort) Close() error                { return nil }

func main() {
	uart := machine.UART0
	_ = uart.Configure(machine.UARTConfig{BaudRate: 115200})
	led := machine.Pin(10) // red LED, active low
	led.Configure(machine.PinConfig{Mode: machine.PinOutput})
	led.High()
	for {
		s, err := zenoh.Open(zenoh.Config{
			Connect:     []string{"serial/uart0"},
			BatchSize:   link.SerialMTU,
			Lease:       10 * time.Second,
			Compression: true,
			Auth:        &transport.Auth{User: "board", Password: "board-test-pw"},
			Dial: func(string, time.Duration) (transport.Link, error) {
				l := link.NewSerial(uartPort{uart})
				return l, l.Connect(0)
			},
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_, err = agent.Run(s, "board/stickc/serial", func(string) {})
		_ = s.Close()
		if err == nil {
			led.Low()
			break
		}
		time.Sleep(2 * time.Second)
	}
	for {
		time.Sleep(time.Hour)
	}
}
