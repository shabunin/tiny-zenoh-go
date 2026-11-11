package fs

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
	"github.com/shabunin/tiny-zenoh-go/zext"
)

// The metadata of the files (their values' timestamps and encodings).
// zenoh-rust keeps it in a RocksDB database in the .zenoh_datainfo
// directory; here it is an append-only log in that directory (a different
// file, so a directory written by zenoh-rust is read without its metadata,
// as zenoh-rust reads files it did not write: timestamp from the file's
// modification time, encoding from its extension).
//
// Each record is the file's path relative to the storage directory and its
// metadata (zenoh-rust's value: the z_serialize'd tuple (u64 time,
// [u8; 16] timestamp ID, u16 encoding ID, Vec<u8> schema)), both prefixed
// by their varint length; an empty value deletes the path's metadata. The
// log is compacted when it is opened and when it holds more dead records
// than live ones.

const (
	dataInfoDir  = ".zenoh_datainfo"
	dataInfoFile = "gozenoh.log"
)

type dataInfo struct {
	ts  zenoh.Timestamp
	enc zenoh.Encoding
}

func (d dataInfo) marshal() []byte {
	s := zext.NewSerializer()
	s.WriteUint64(d.ts.Time)
	var id [16]byte
	copy(id[:], d.ts.ID.Bytes())
	for _, b := range id {
		s.WriteUint8(b)
	}
	s.WriteUint16(d.enc.ID)
	s.WriteBytes([]byte(d.enc.Schema))
	return s.Bytes()
}

func unmarshalDataInfo(b []byte) (d dataInfo, err error) {
	r := zext.NewDeserializer(b)
	if d.ts.Time, err = r.ReadUint64(); err != nil {
		return d, err
	}
	var id [16]byte
	for i := range id {
		if id[i], err = r.ReadUint8(); err != nil {
			return d, err
		}
	}
	if d.ts.ID, err = protocol.NewZID(id[:]); err != nil {
		return d, err
	}
	if d.enc.ID, err = r.ReadUint16(); err != nil {
		return d, err
	}
	schema, err := r.ReadBytes()
	if err != nil {
		return d, err
	}
	d.enc.Schema = string(schema)
	if !r.Done() {
		return d, errors.New("trailing bytes")
	}
	return d, nil
}

type dataInfos struct {
	path string
	f    *os.File
	m    map[string]dataInfo
	dead int // records in the log that no longer count
}

// openDataInfos reads (and compacts) the metadata log of dir.
func openDataInfos(dir string, readOnly bool) (*dataInfos, error) {
	d := &dataInfos{path: filepath.Join(dir, dataInfoDir, dataInfoFile), m: map[string]dataInfo{}}
	if f, err := os.Open(d.path); err == nil {
		err = d.load(bufio.NewReader(f))
		f.Close()
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if readOnly {
		return d, nil
	}
	if err := os.MkdirAll(filepath.Dir(d.path), 0o755); err != nil {
		return nil, err
	}
	return d, d.compact()
}

// load reads the records; a torn last record is dropped.
func (d *dataInfos) load(r *bufio.Reader) error {
	for {
		k, err := readChunk(r)
		if err != nil {
			return nil
		}
		v, err := readChunk(r)
		if err != nil {
			return nil
		}
		if _, ok := d.m[string(k)]; ok {
			d.dead++
		}
		if len(v) == 0 {
			delete(d.m, string(k))
			d.dead++
			continue
		}
		if di, err := unmarshalDataInfo(v); err == nil {
			d.m[string(k)] = di
		} else {
			d.dead++
		}
	}
}

func readChunk(r *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n > 1<<20 {
		return nil, errors.New("record too long")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func appendRecord(b []byte, k string, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(k)))
	b = append(b, k...)
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

// compact rewrites the log with the live records only.
func (d *dataInfos) compact() error {
	if d.f != nil {
		d.f.Close()
		d.f = nil
	}
	var b []byte
	for k, v := range d.m {
		b = appendRecord(b, k, v.marshal())
	}
	tmp := d.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, d.path); err != nil {
		return err
	}
	d.dead = 0
	f, err := os.OpenFile(d.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	d.f = f
	return nil
}

func (d *dataInfos) write(k string, v []byte) error {
	if d.f == nil {
		return errors.New("read-only metadata")
	}
	_, err := d.f.Write(appendRecord(nil, k, v))
	if err == nil && d.dead > 64 && d.dead > len(d.m) {
		err = d.compact()
	}
	return err
}

func (d *dataInfos) get(k string) (dataInfo, bool) {
	v, ok := d.m[k]
	return v, ok
}

func (d *dataInfos) put(k string, v dataInfo) error {
	if _, ok := d.m[k]; ok {
		d.dead++
	}
	d.m[k] = v
	return d.write(k, v.marshal())
}

func (d *dataInfos) del(k string) error {
	if _, ok := d.m[k]; !ok {
		return nil
	}
	delete(d.m, k)
	d.dead += 2
	return d.write(k, nil)
}

func (d *dataInfos) rename(from, to string) bool {
	v, ok := d.m[from]
	if !ok {
		return false
	}
	_ = d.del(from)
	_ = d.put(to, v)
	return true
}

func (d *dataInfos) close() error {
	if d.f == nil {
		return nil
	}
	err := d.f.Close()
	d.f = nil
	return err
}
