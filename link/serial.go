package link

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

// Serial links, "serial/<device>#baudrate=<n>" (zenoh-rust's z-serial crate
// and zenoh-pico's serial link, which share the format). Each batch travels
// as one frame:
//
//	COBS( flags u8 | len u16 LE | payload | CRC-32 u32 LE ) 0x00
//
// The CRC is the reflected table algorithm seeded with the *non*-reflected
// polynomial 0x04C11DB7 (init and final xor 0xFFFFFFFF), which is what both
// implementations compute. Payloads are at most 1500 bytes; the link is a
// lossy datagram link to the transport.
//
// Before a session, the connecting side sends empty frames with the I flag
// until the other answers I|A; a side that receives I on an established link
// answers R (reset) and drops the link, and an initiator that gets R retries
// after 250 ms.
//
// NewSerial wraps any byte stream (a TinyGo machine.UART, a USB CDC port, a
// Bluetooth SPP socket...); on Linux, Dial and Listen open tty devices.

// SerialMTU is the largest batch a serial frame carries.
const SerialMTU = 1500

const (
	serialMaxFrame = 1517 // COBS-encoded MTU-sized frame, without the 0x00
	serialOverhead = 1 + 2 + 4

	serialInit  = 0x01
	serialAck   = 0x02
	serialReset = 0x04

	serialRetry       = 250 * time.Millisecond
	serialDefaultBaud = 9600 // zenoh-rust's default
)

var serialCRC = crc32.MakeTable(0x04C11DB7)

// Errors from serial links.
var (
	ErrSerialReset = errors.New("link: serial peer restarted the session")
	ErrSerialFrame = errors.New("link: bad serial frame")
)

// cobsEncode appends the COBS encoding of src to dst.
func cobsEncode(dst, src []byte) []byte {
	codeAt := len(dst)
	dst = append(dst, 0)
	code := byte(1)
	for _, b := range src {
		if b != 0 {
			dst = append(dst, b)
			code++
		}
		if b == 0 || code == 0xff {
			dst[codeAt] = code
			code = 1
			codeAt = len(dst)
			dst = append(dst, 0)
		}
	}
	dst[codeAt] = code
	return dst
}

// cobsDecode decodes src (without the trailing 0x00) into dst[:0].
func cobsDecode(dst, src []byte) ([]byte, error) {
	dst = dst[:0]
	for i := 0; i < len(src); {
		code := int(src[i])
		if code == 0 {
			return nil, ErrSerialFrame
		}
		i++
		end := i + code - 1
		if end > len(src) {
			return nil, ErrSerialFrame
		}
		dst = append(dst, src[i:end]...)
		i = end
		if code != 0xff && i < len(src) {
			dst = append(dst, 0)
		}
	}
	return dst, nil
}

// appendSerialFrame appends one complete frame (COBS + delimiter).
func appendSerialFrame(dst []byte, flags byte, payload []byte) []byte {
	raw := make([]byte, 0, len(payload)+serialOverhead)
	raw = append(raw, flags)
	raw = binary.LittleEndian.AppendUint16(raw, uint16(len(payload)))
	raw = append(raw, payload...)
	raw = binary.LittleEndian.AppendUint32(raw, crc32.Checksum(payload, serialCRC))
	return append(cobsEncode(dst, raw), 0)
}

// parseSerialFrame checks a decoded frame and returns its flags and payload.
func parseSerialFrame(raw []byte) (byte, []byte, error) {
	if len(raw) < serialOverhead {
		return 0, nil, ErrSerialFrame
	}
	n := int(binary.LittleEndian.Uint16(raw[1:3]))
	if len(raw) != n+serialOverhead {
		return 0, nil, ErrSerialFrame
	}
	payload := raw[3 : 3+n]
	if binary.LittleEndian.Uint32(raw[3+n:]) != crc32.Checksum(payload, serialCRC) {
		return 0, nil, ErrSerialFrame
	}
	return raw[0], payload, nil
}

type serialFrame struct {
	flags   byte
	payload []byte
}

// Serial is a zenoh serial link over a byte stream. It is a
// transport.PacketLink once Connect or Accept succeeded.
type Serial struct {
	rw   io.ReadWriteCloser
	rx   chan serialFrame
	done chan struct{}

	wmu  sync.Mutex
	wbuf []byte

	mu          sync.Mutex
	established bool
	rxErr       error
	closeOnce   sync.Once
}

var _ transport.PacketLink = (*Serial)(nil)

// NewSerial starts reading frames from rw. Call Connect or Accept before
// handing it to the transport.
func NewSerial(rw io.ReadWriteCloser) *Serial {
	s := &Serial{rw: rw, rx: make(chan serialFrame, 8), done: make(chan struct{})}
	go s.readLoop()
	return s
}

func (s *Serial) readLoop() {
	defer close(s.rx)
	br := bufio.NewReaderSize(s.rw, 512)
	enc := make([]byte, 0, serialMaxFrame+1)
	var dec []byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			s.mu.Lock()
			s.rxErr = err
			s.mu.Unlock()
			return
		}
		if b != 0 {
			if len(enc) < serialMaxFrame+1 {
				enc = append(enc, b)
			} else {
				enc = enc[:serialMaxFrame+1] // too long: dropped at the delimiter
			}
			continue
		}
		frame := enc
		enc = enc[:0]
		if len(frame) == 0 || len(frame) > serialMaxFrame {
			continue
		}
		var derr error
		if dec, derr = cobsDecode(dec, frame); derr != nil {
			continue // line noise, boot messages...: skip
		}
		flags, payload, perr := parseSerialFrame(dec)
		if perr != nil {
			continue
		}
		select {
		case s.rx <- serialFrame{flags, append([]byte(nil), payload...)}:
		case <-s.done:
			return
		}
	}
}

func (s *Serial) writeFrame(flags byte, payload []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.wbuf = appendSerialFrame(s.wbuf[:0], flags, payload)
	_, err := s.rw.Write(s.wbuf)
	return err
}

func (s *Serial) next(timeout <-chan time.Time) (serialFrame, error) {
	select {
	case f, ok := <-s.rx:
		if !ok {
			s.mu.Lock()
			err := s.rxErr
			s.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return serialFrame{}, err
		}
		return f, nil
	case <-timeout:
		return serialFrame{}, errors.New("link: serial handshake timed out")
	}
}

// Connect runs the initiator side of the serial handshake: it sends I until
// the peer answers I|A, or timeout (0 = wait forever) expires.
func (s *Serial) Connect(timeout time.Duration) error {
	var deadline <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		deadline = t.C
	}
	for {
		if err := s.writeFrame(serialInit, nil); err != nil {
			return err
		}
		retry := time.NewTimer(serialRetry)
		for waiting := true; waiting; {
			select {
			case f, ok := <-s.rx:
				if !ok {
					retry.Stop()
					_, err := s.next(nil)
					return err
				}
				switch {
				case f.flags&(serialInit|serialAck) == serialInit|serialAck:
					retry.Stop()
					s.mu.Lock()
					s.established = true
					s.mu.Unlock()
					return nil
				case f.flags&serialReset != 0:
					<-retry.C // throttle, then ask again
					waiting = false
				}
				// anything else (e.g. data from a previous session): ignore
			case <-retry.C:
				waiting = false // no answer yet (peer booting?): ask again
			case <-deadline:
				retry.Stop()
				return errors.New("link: serial connect timed out")
			}
		}
	}
}

// Accept runs the responder side: it waits for an I frame and answers I|A.
func (s *Serial) Accept() error {
	for {
		f, err := s.next(nil)
		if err != nil {
			return err
		}
		if f.flags&serialInit != 0 {
			if err := s.writeFrame(serialInit|serialAck, nil); err != nil {
				return err
			}
			s.mu.Lock()
			s.established = true
			s.mu.Unlock()
			return nil
		}
	}
}

// MTU is the largest batch: 1500 bytes.
func (s *Serial) MTU() int { return SerialMTU }

// Read returns the payload of the next data frame. An I frame on an
// established link means the peer restarted: it is answered with R and the
// link fails, as zenoh-rust does.
func (s *Serial) Read(b []byte) (int, error) {
	for {
		f, err := s.next(nil)
		if err != nil {
			return 0, err
		}
		if f.flags&serialInit != 0 {
			_ = s.writeFrame(serialReset, nil)
			s.mu.Lock()
			s.established = false
			s.mu.Unlock()
			return 0, ErrSerialReset
		}
		if len(f.payload) == 0 {
			continue
		}
		if len(f.payload) > len(b) {
			return 0, transport.ErrBatchLength
		}
		return copy(b, f.payload), nil
	}
}

// Write sends b as one data frame.
func (s *Serial) Write(b []byte) (int, error) {
	if len(b) > SerialMTU {
		return 0, errors.New("link: batch larger than the serial MTU")
	}
	if err := s.writeFrame(0, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close closes the underlying stream.
func (s *Serial) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		err = s.rw.Close()
	})
	return err
}

func serialBaud(l Locator) (int, error) {
	v, ok := l.ConfigValues()["baudrate"]
	if !ok {
		return serialDefaultBaud, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, errors.New("link: bad serial baudrate " + strconv.Quote(v))
	}
	return n, nil
}

func dialSerial(l Locator, timeout time.Duration) (transport.Link, error) {
	baud, err := serialBaud(l)
	if err != nil {
		return nil, err
	}
	port, err := openSerialPort(l.Address, baud, l.ConfigValues()["exclusive"] != "false")
	if err != nil {
		return nil, err
	}
	s := NewSerial(port)
	if err := s.Connect(timeout); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// serialListener serves one device: one session at a time, accepted again
// once the previous one ended.
type serialListener struct {
	s      *Serial
	loc    Locator
	free   chan struct{} // holds a token while no session uses the port
	closed chan struct{}
	once   sync.Once
}

func listenSerial(l Locator, bound Locator) (Listener, error) {
	baud, err := serialBaud(l)
	if err != nil {
		return nil, err
	}
	port, err := openSerialPort(l.Address, baud, l.ConfigValues()["exclusive"] != "false")
	if err != nil {
		return nil, err
	}
	bound.Address = l.Address
	ln := &serialListener{s: NewSerial(port), loc: bound, free: make(chan struct{}, 1), closed: make(chan struct{})}
	ln.free <- struct{}{}
	return ln, nil
}

func (ln *serialListener) Accept() (transport.Link, error) {
	select {
	case <-ln.free:
	case <-ln.closed:
		return nil, io.EOF
	}
	if err := ln.s.Accept(); err != nil {
		ln.free <- struct{}{}
		return nil, err
	}
	return &serialSession{Serial: ln.s, ln: ln}, nil
}

func (ln *serialListener) Close() error {
	ln.once.Do(func() { close(ln.closed) })
	return ln.s.Close()
}

func (ln *serialListener) Locator() Locator { return ln.loc }

// serialSession is an accepted session; closing it frees the port for the
// next one instead of closing the device.
type serialSession struct {
	*Serial
	ln   *serialListener
	once sync.Once
}

func (c *serialSession) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.established = false
		c.mu.Unlock()
		c.ln.free <- struct{}{}
	})
	return nil
}
