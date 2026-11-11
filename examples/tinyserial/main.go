//go:build tinygo

// tinyserial runs a gozenoh client on a microcontroller whose only link is
// its UART (e.g. the USB-serial bridge of an ESP32 board), using zenoh's
// serial framing. On the host, a router listens on the serial port:
//
//	gozenohd -l 'serial//dev/ttyUSB0#baudrate=115200' -e tcp/127.0.0.1:7447
//
// The device publishes a counter on demo/<name>/counter every second,
// answers queries on demo/<name>/info, and blinks its LED on demo/<name>/led
// ("on"/"off"). Nothing may be printed: the UART carries zenoh frames.
package main

import (
	"machine"
	"runtime"
	"strconv"
	"time"

	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/transport"
	"github.com/shabunin/gozenoh/zenoh"
)

const name = "stickc"

// uartPort makes a machine.UART a blocking io.ReadWriteCloser.
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
	led := machine.Pin(10) // M5StickC red LED, active low
	led.Configure(machine.PinConfig{Mode: machine.PinOutput})
	led.High()

	start := time.Now()
	for {
		s, err := zenoh.Open(zenoh.Config{
			Connect:   []string{"serial/uart0"},
			BatchSize: link.SerialMTU,
			Lease:     10 * time.Second,
			Dial: func(string, time.Duration) (transport.Link, error) {
				l := link.NewSerial(uartPort{uart})
				return l, l.Connect(0)
			},
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_, _ = s.DeclareSubscriber("demo/"+name+"/led", func(smp zenoh.Sample) {
			if string(smp.Payload) == "on" {
				led.Low()
			} else {
				led.High()
			}
		})
		_, _ = s.DeclareQueryable("demo/"+name+"/info", func(q *zenoh.Query) {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			info := "tinygo " + runtime.Version() + " " + runtime.GOARCH +
				" uptime=" + time.Since(start).Round(time.Second).String() +
				" heap_inuse=" + strconv.FormatUint(ms.HeapInuse, 10) +
				" heap_sys=" + strconv.FormatUint(ms.HeapSys, 10)
			_ = q.Reply(q.KeyExpr(), []byte(info))
		})
		pub, _ := s.DeclarePublisher("demo/" + name + "/counter")
		for i := 0; ; i++ {
			if pub.Put([]byte(strconv.Itoa(i))) != nil {
				break
			}
			select {
			case <-s.Done():
			case <-time.After(time.Second):
				continue
			}
			break
		}
		_ = s.Close()
	}
}
