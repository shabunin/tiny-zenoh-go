package storage

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// Backends: where a storage keeps its values (zenoh-rust's
// zenoh-backend-traits). A Volume is a configured backend (zenoh's
// plugins/storage_manager/volumes entry, or the built-in "memory"); each
// storage on it gets its own Backend instance.
//
// Keys given to a Backend are stripped of the storage's strip_prefix; ""
// stands for the strip prefix itself (zenoh's None key).

// Value is a stored value (zenoh's StoredData).
type Value struct {
	Payload   []byte
	Encoding  zenoh.Encoding
	Timestamp zenoh.Timestamp
}

// Entry is a stored key with its value's timestamp.
type Entry struct {
	Key       string
	Timestamp zenoh.Timestamp
}

// Result is the outcome of a Put or Delete (zenoh's
// StorageInsertionResult).
type Result int

const (
	Outdated Result = iota
	Inserted
	Replaced
	Deleted
)

// History is what a backend keeps of a key: its latest value only, or
// every value (then queries' parameters can select among them, and the
// storage cannot be replicated).
type History int

const (
	HistoryLatest History = iota
	HistoryAll
)

// Capability is zenoh's backend Capability.
type Capability struct {
	// Durable: the values outlive the storage (and the process).
	Durable bool
	History History
}

// Backend is the store of one storage (zenoh's Storage trait). The
// storage serializes its calls.
type Backend interface {
	// Put stores v under key.
	Put(key string, v Value) (Result, error)
	// Delete removes key's value, as of ts.
	Delete(key string, ts zenoh.Timestamp) (Result, error)
	// Get returns key's value(s); parameters are the query's (selector
	// parameters), "" when the storage itself asks.
	Get(key, parameters string) ([]Value, error)
	// Entries lists the stored keys.
	Entries() ([]Entry, error)
	// Close releases the backend; what happens to its values is the
	// backend's (and its on_closure setting's) business.
	Close() error
}

// Volume makes backends for storages (zenoh's Volume trait).
type Volume interface {
	Capability() Capability
	// CreateStorage makes the backend of a storage: cfg is the storage's
	// configuration and params the storage's volume settings (the entries
	// of its "volume" object besides "id"; nil if none).
	CreateStorage(cfg Config, params map[string]any) (Backend, error)
	// Status is what the admin space shows of the volume (zenoh's
	// get_admin_status), nil for nothing more than its name.
	Status() map[string]any
	// Close releases the volume's resources.
	Close() error
}

// VolumeFactory makes a volume from its configuration (the volume's entry
// of plugins/storage_manager/volumes; nil if it has none).
type VolumeFactory func(name string, cfg map[string]any) (Volume, error)

var (
	volumesMu sync.Mutex
	volumes   = map[string]VolumeFactory{"memory": func(string, map[string]any) (Volume, error) { return MemoryVolume{}, nil }}
)

// RegisterBackend makes a backend available to volume configurations by
// name (zenoh-rust loads zenoh_backend_<name> libraries instead).
func RegisterBackend(name string, f VolumeFactory) {
	volumesMu.Lock()
	volumes[name] = f
	volumesMu.Unlock()
}

// Backends lists the registered backend names.
func Backends() []string {
	volumesMu.Lock()
	defer volumesMu.Unlock()
	var out []string
	for k := range volumes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ErrUnknownBackend: no backend has the name.
var ErrUnknownBackend = errors.New("storage: unknown backend")

// OpenVolume makes a volume of the named backend.
func OpenVolume(backend, name string, cfg map[string]any) (Volume, error) {
	volumesMu.Lock()
	f := volumes[backend]
	volumesMu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownBackend, backend)
	}
	return f(name, cfg)
}

// MemoryVolume is the built-in "memory" volume.
type MemoryVolume struct{}

func (MemoryVolume) Capability() Capability { return Capability{History: HistoryLatest} }
func (MemoryVolume) CreateStorage(Config, map[string]any) (Backend, error) {
	return NewMemoryBackend(), nil
}
func (MemoryVolume) Status() map[string]any { return nil }
func (MemoryVolume) Close() error           { return nil }

// MemoryBackend keeps values in a map.
type MemoryBackend struct {
	data map[string]Value
}

// NewMemoryBackend makes an empty memory backend.
func NewMemoryBackend() *MemoryBackend { return &MemoryBackend{data: map[string]Value{}} }

func (b *MemoryBackend) Put(key string, v Value) (Result, error) {
	_, had := b.data[key]
	b.data[key] = v
	if had {
		return Replaced, nil
	}
	return Inserted, nil
}

func (b *MemoryBackend) Delete(key string, _ zenoh.Timestamp) (Result, error) {
	delete(b.data, key)
	return Deleted, nil
}

func (b *MemoryBackend) Get(key, _ string) ([]Value, error) {
	if v, ok := b.data[key]; ok {
		return []Value{v}, nil
	}
	return nil, nil
}

func (b *MemoryBackend) Entries() ([]Entry, error) {
	out := make([]Entry, 0, len(b.data))
	for k, v := range b.data {
		out = append(out, Entry{k, v.Timestamp})
	}
	return out, nil
}

func (b *MemoryBackend) Close() error { return nil }

// Params reads storage and volume settings.
type Params map[string]any

// String is the string setting k, or def if unset.
func (p Params) String(k, def string) (string, error) {
	switch v := p[k].(type) {
	case nil:
		return def, nil
	case string:
		return v, nil
	}
	return "", fmt.Errorf("%s: not a string: %v", k, p[k])
}

// Bool is the boolean setting k (true or false, or their strings), or def.
func (p Params) Bool(k string, def bool) (bool, error) {
	switch v := p[k].(type) {
	case nil:
		return def, nil
	case bool:
		return v, nil
	case string:
		switch v {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	}
	return false, fmt.Errorf("%s: not a boolean: %v", k, p[k])
}

// Map is the object setting k (nil if unset).
func (p Params) Map(k string) (Params, error) {
	switch v := p[k].(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return v, nil
	}
	return nil, fmt.Errorf("%s: not an object: %v", k, p[k])
}

// Private is the setting k, looked for first in the "private" object (where
// zenoh keeps secrets that the admin space hides), then at the top.
func (p Params) Private(k string) (string, error) {
	priv, err := p.Map("private")
	if err != nil {
		return "", err
	}
	if v, ok := priv[k]; ok && v != nil {
		return priv.String(k, "")
	}
	return p.String(k, "")
}

// OnClosure is the on_closure setting of a storage: "do_nothing" (the
// default) or "delete_all".
func (p Params) OnClosure() (deleteAll bool, err error) {
	s, err := p.String("on_closure", "do_nothing")
	if err != nil {
		return false, err
	}
	switch s {
	case "do_nothing":
		return false, nil
	case "delete_all":
		return true, nil
	}
	return false, fmt.Errorf("on_closure: %q is neither \"do_nothing\" nor \"delete_all\"", s)
}

// FormatTimestamp renders ts as uhlc does: "<NTP64 time>/<ID in hex>",
// the form backends store timestamps in.
func FormatTimestamp(ts zenoh.Timestamp) string {
	return strconv.FormatUint(ts.Time, 10) + "/" + ts.ID.String()
}

// ParseTimestamp parses FormatTimestamp's form.
func ParseTimestamp(s string) (zenoh.Timestamp, error) {
	t, id, ok := strings.Cut(s, "/")
	if !ok {
		return zenoh.Timestamp{}, fmt.Errorf("timestamp %q: no '/'", s)
	}
	n, err := strconv.ParseUint(t, 10, 64)
	if err != nil {
		return zenoh.Timestamp{}, fmt.Errorf("timestamp %q: %w", s, err)
	}
	z, err := protocol.ParseZID(id)
	if err != nil {
		return zenoh.Timestamp{}, fmt.Errorf("timestamp %q: %w", s, err)
	}
	return zenoh.Timestamp{Time: n, ID: z}, nil
}
