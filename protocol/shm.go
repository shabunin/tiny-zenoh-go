package protocol

import (
	"errors"

	"github.com/shabunin/gozenoh/internal/codec"
)

// Shared-memory payloads (zenoh-rust commons/zenoh-codec/src/core/shm.rs and
// core/zbuf.rs "sliced" codec). When a PUT, ERR or query body carries the SHM
// extension, its payload is a list of parts, each either raw bytes or a
// reference to a buffer in a shared-memory segment.

// Extension keys for the SHM marker (unit extensions).
const (
	extPutShm   byte = 0x12 // unit, id 2, mandatory (PUT, ERR)
	extQueryShm byte = 0x04 // unit, id 4: must directly precede the query body
)

const (
	partRaw    = 0
	partShmPtr = 1
)

// ShmInfo references a buffer in shared memory (zenoh-shm ShmBufInfo).
type ShmInfo struct {
	DataLen    uint64 // bytes of payload
	MetaID     uint16 // metadata segment id
	MetaIndex  uint16 // header index in that segment
	Generation uint32 // must match the header's generation
}

// PayloadPart is one slice of a payload sent with the SHM extension: Raw
// bytes, or (when Shm is set) a shared-memory reference.
type PayloadPart struct {
	Raw []byte
	Shm *ShmInfo
}

var errBadSlice = errors.New("protocol: bad payload slice")

func (i *ShmInfo) encode(w *codec.Writer) {
	w.WriteZInt(i.DataLen)
	w.WriteZInt(uint64(i.MetaID))
	w.WriteZInt(uint64(i.MetaIndex))
	w.WriteZInt(uint64(i.Generation))
}

func decodeShmInfo(r *codec.Reader) (*ShmInfo, error) {
	var i ShmInfo
	var err error
	if i.DataLen, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if i.DataLen == 0 {
		return nil, errBadSlice
	}
	if i.MetaID, err = readU16(r); err != nil {
		return nil, err
	}
	if i.MetaIndex, err = readU16(r); err != nil {
		return nil, err
	}
	g, err := r.ReadZInt()
	if err != nil {
		return nil, err
	}
	if g > 0xffffffff {
		return nil, ErrOutOfRange
	}
	i.Generation = uint32(g)
	return &i, nil
}

// encodeParts writes the sliced payload format.
func encodeParts(w *codec.Writer, parts []PayloadPart) {
	w.WriteZInt(uint64(len(parts)))
	for _, p := range parts {
		if p.Shm != nil {
			w.WriteZInt(partShmPtr)
			w.WriteZBytes(encodeTo(p.Shm.encode))
		} else {
			w.WriteZInt(partRaw)
			w.WriteZBytes(p.Raw)
		}
	}
}

func decodeParts(r *codec.Reader) ([]PayloadPart, error) {
	n, err := r.ReadZInt()
	if err != nil {
		return nil, err
	}
	if n > uint64(r.Remaining()) {
		return nil, errBadSlice
	}
	parts := make([]PayloadPart, 0, n)
	for range n {
		kind, err := r.ReadZInt()
		if err != nil {
			return nil, err
		}
		b, err := r.ReadZBytes()
		if err != nil {
			return nil, err
		}
		switch kind {
		case partRaw:
			parts = append(parts, PayloadPart{Raw: append([]byte{}, b...)})
		case partShmPtr:
			info, err := decodeShmInfo(codec.NewReader(b))
			if err != nil {
				return nil, err
			}
			parts = append(parts, PayloadPart{Shm: info})
		default:
			return nil, errBadSlice
		}
	}
	return parts, nil
}
