// Package fs is zenoh's filesystem storage backend (zenoh-rust's
// zenoh-backend-filesystem): each key's value is a file under the storage's
// directory, at the key's path. Importing the package registers the "fs"
// backend.
//
// Volume settings: none (the root directory is $ZENOH_BACKEND_FS_ROOT, by
// default $ZENOH_HOME/zenoh_backend_fs with ZENOH_HOME defaulting to
// ~/.zenoh). Storage settings (the storage's "volume" object):
//   - dir (required): the storage's directory, relative to the root,
//     without "..";
//   - read_only (false): serve the files, refuse updates;
//   - on_closure ("do_nothing"): "delete_all" removes the directory when the
//     storage stops;
//   - follow_links (false): serve files reached through symbolic links;
//   - keep_mime_types (true): files without metadata (written by something
//     else) get the encoding their extension suggests, rather than
//     application/octet-stream.
//
// As zenoh-rust: a key that is both a value and a prefix of other keys
// (a file where a directory must be, or the reverse) is stored in
// <path>.##z; the strip prefix itself is the file "@root"; files without
// metadata are timestamped by their modification time, with ID 0x01.
package fs

import (
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

const (
	// RootEnv names the environment variable with the volume's root.
	RootEnv = "ZENOH_BACKEND_FS_ROOT"
	// DefaultRootDir is the root's name in zenoh's home directory.
	DefaultRootDir = "zenoh_backend_fs"
	rootKey        = "@root"
	conflictSuffix = ".##z"
)

func init() {
	storage.RegisterBackend("fs", func(string, map[string]any) (storage.Volume, error) {
		return NewVolume("")
	})
}

// Volume is a filesystem volume.
type Volume struct {
	root string
}

// DefaultRoot is the root of a volume without one set: $ZENOH_BACKEND_FS_ROOT
// or <zenoh home>/zenoh_backend_fs.
func DefaultRoot() string {
	if d := os.Getenv(RootEnv); d != "" {
		return d
	}
	home := os.Getenv("ZENOH_HOME")
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			h = "."
		}
		home = filepath.Join(h, ".zenoh")
	}
	return filepath.Join(home, DefaultRootDir)
}

// NewVolume makes a volume rooted at root ("": DefaultRoot), creating it.
func NewVolume(root string) (*Volume, error) {
	if root == "" {
		root = DefaultRoot()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("fs: creating root %s: %w", root, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if abs, err = filepath.EvalSymlinks(abs); err != nil {
		return nil, fmt.Errorf("fs: root %s: %w", root, err)
	}
	return &Volume{root: abs}, nil
}

// Root is the volume's root directory.
func (v *Volume) Root() string { return v.root }

func (v *Volume) Capability() storage.Capability {
	return storage.Capability{Durable: true, History: storage.HistoryLatest}
}

func (v *Volume) Status() map[string]any { return map[string]any{"root": v.root} }

func (v *Volume) Close() error { return nil }

// CreateStorage opens (creating it) the storage's directory.
func (v *Volume) CreateStorage(cfg storage.Config, params map[string]any) (storage.Backend, error) {
	p := storage.Params(params)
	if p == nil {
		return nil, errors.New("fs: storages need volume settings (at least \"dir\")")
	}
	s := &Storage{}
	var err error
	if s.readOnly, err = p.Bool("read_only", false); err != nil {
		return nil, fmt.Errorf("fs: %w", err)
	}
	if s.followLinks, err = p.Bool("follow_links", false); err != nil {
		return nil, fmt.Errorf("fs: %w", err)
	}
	if s.keepMime, err = p.Bool("keep_mime_types", true); err != nil {
		return nil, fmt.Errorf("fs: %w", err)
	}
	if s.deleteAll, err = p.OnClosure(); err != nil {
		return nil, fmt.Errorf("fs: %w", err)
	}
	dir, err := p.String("dir", "")
	switch {
	case err != nil:
		return nil, fmt.Errorf("fs: %w", err)
	case dir == "":
		return nil, errors.New(`fs: missing required property "dir"`)
	case filepath.IsAbs(dir):
		return nil, fmt.Errorf(`fs: "dir"=%q: the path must be relative`, dir)
	case slices.Contains(strings.Split(filepath.ToSlash(dir), "/"), ".."):
		return nil, fmt.Errorf(`fs: "dir"=%q: the path must not contain any '..'`, dir)
	}
	s.base = filepath.Join(v.root, dir)
	switch st, err := os.Stat(s.base); {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(s.base, 0o755); err != nil {
			return nil, fmt.Errorf("fs: cannot create storage on %s: %w", s.base, err)
		}
	case err != nil:
		return nil, fmt.Errorf("fs: cannot create storage on %s: %w", s.base, err)
	case !st.IsDir():
		return nil, fmt.Errorf("fs: cannot create storage on %s: this is not a directory", s.base)
	}
	if !s.readOnly {
		f, err := os.CreateTemp(s.base, ".probe")
		if err != nil {
			return nil, fmt.Errorf("fs: cannot create writeable storage on %s: %w", s.base, err)
		}
		f.Close()
		os.Remove(f.Name())
	}
	if s.info, err = openDataInfos(s.base, s.readOnly); err != nil {
		return nil, fmt.Errorf("fs: metadata of %s: %w", s.base, err)
	}
	return s, nil
}

// Storage is a filesystem storage.
type Storage struct {
	base        string
	readOnly    bool
	followLinks bool
	keepMime    bool
	deleteAll   bool
	info        *dataInfos
}

// Dir is the storage's directory.
func (s *Storage) Dir() string { return s.base }

func (s *Storage) fspath(key string) string {
	if key == "" {
		key = rootKey
	}
	return filepath.Join(s.base, filepath.FromSlash(key))
}

// rel is a file's path relative to the storage directory, the metadata key.
func (s *Storage) rel(file string) string {
	r, err := filepath.Rel(s.base, file)
	if err != nil {
		return file
	}
	return filepath.ToSlash(r)
}

var errReadOnly = errors.New("fs: update for a read-only storage")

func (s *Storage) Put(key string, v storage.Value) (storage.Result, error) {
	if s.readOnly {
		return storage.Outdated, errReadOnly
	}
	file := s.fspath(key)
	parent := filepath.Dir(file)
	// A file where a directory must go moves aside to its conflict name.
	for a := parent; len(a) > len(s.base); a = filepath.Dir(a) {
		st, err := os.Lstat(a)
		if err != nil {
			continue
		}
		if st.IsDir() {
			break
		}
		if st.Mode().IsRegular() {
			cf := a + conflictSuffix
			if err := os.Rename(a, cf); err != nil {
				return storage.Outdated, fmt.Errorf("fs: moving %s aside: %w", a, err)
			}
			if !s.info.rename(s.rel(a), s.rel(cf)) {
				_ = s.info.put(s.rel(cf), s.fallbackInfo(cf, v.Timestamp))
			}
		}
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return storage.Outdated, fmt.Errorf("fs: creating directories for %s: %w", file, err)
	}
	if st, err := os.Stat(file); err == nil && st.IsDir() {
		file += conflictSuffix
	}
	if err := os.WriteFile(file, v.Payload, 0o644); err != nil {
		return storage.Outdated, fmt.Errorf("fs: writing %s: %w", file, err)
	}
	if err := s.info.put(s.rel(file), dataInfo{v.Timestamp, v.Encoding}); err != nil {
		return storage.Outdated, fmt.Errorf("fs: metadata of %s: %w", file, err)
	}
	return storage.Inserted, nil
}

func isFile(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func (s *Storage) Delete(key string, _ zenoh.Timestamp) (storage.Result, error) {
	if s.readOnly {
		return storage.Outdated, errReadOnly
	}
	file := s.fspath(key)
	if !isFile(file) {
		file += conflictSuffix
	}
	if _, err := os.Lstat(file); err == nil {
		if err := os.Remove(file); err != nil {
			return storage.Outdated, fmt.Errorf("fs: deleting %s: %w", file, err)
		}
		for d := filepath.Dir(file); d != s.base && len(d) > len(s.base); d = filepath.Dir(d) {
			if os.Remove(d) != nil {
				break
			}
		}
	}
	if err := s.info.del(s.rel(file)); err != nil {
		return storage.Outdated, err
	}
	return storage.Deleted, nil
}

// readable reports whether file is a regular file the storage serves.
func (s *Storage) readable(file string) bool {
	return isFile(file) && (s.followLinks || !s.containsSymlink(file))
}

func (s *Storage) containsSymlink(p string) bool {
	for ; len(p) > len(s.base); p = filepath.Dir(p) {
		if st, err := os.Lstat(p); err == nil && st.Mode()&fs.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

// valueFile is the file holding key's value, "" if none.
func (s *Storage) valueFile(key string) string {
	file := s.fspath(key)
	if s.readable(file) {
		return file
	}
	if file += conflictSuffix; s.readable(file) {
		return file
	}
	return ""
}

func (s *Storage) Get(key, _ string) ([]storage.Value, error) {
	file := s.valueFile(key)
	if file == "" {
		return nil, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("fs: reading %s: %w", file, err)
	}
	di, err := s.dataInfo(file)
	if err != nil {
		return nil, err
	}
	return []storage.Value{{Payload: b, Encoding: di.enc, Timestamp: di.ts}}, nil
}

func (s *Storage) dataInfo(file string) (dataInfo, error) {
	if di, ok := s.info.get(s.rel(file)); ok {
		return di, nil
	}
	st, err := os.Stat(file)
	if err != nil {
		return dataInfo{}, fmt.Errorf("fs: metadata of %s: %w", file, err)
	}
	return s.fallbackInfo(file, zenoh.Timestamp{Time: protocol.NTP64FromTime(st.ModTime()), ID: fileID}), nil
}

// fileID is the timestamp ID of files without metadata (zenoh-rust's [1]).
var fileID, _ = protocol.NewZID([]byte{1})

// fallbackInfo is the metadata of a file written by something else.
func (s *Storage) fallbackInfo(file string, def zenoh.Timestamp) dataInfo {
	di := dataInfo{ts: def, enc: zenoh.EncodingApplicationOctetStream}
	if st, err := os.Stat(file); err == nil {
		di.ts = zenoh.Timestamp{Time: protocol.NTP64FromTime(st.ModTime()), ID: fileID}
	}
	if s.keepMime {
		name := strings.TrimSuffix(file, conflictSuffix)
		if t := mime.TypeByExtension(filepath.Ext(name)); t != "" {
			if essence, _, _ := strings.Cut(t, ";"); essence != "" {
				di.enc = zenoh.ParseEncoding(strings.TrimSpace(essence))
			}
		}
	}
	return di
}

func (s *Storage) Entries() ([]storage.Entry, error) {
	var out []storage.Entry
	add := func(key string) {
		file := s.valueFile(key)
		if file == "" {
			return
		}
		if di, err := s.dataInfo(file); err == nil {
			out = append(out, storage.Entry{Key: key, Timestamp: di.ts})
		}
	}
	add("")
	seen := map[string]bool{}
	err := s.walk(s.base, func(file string) {
		key := strings.TrimSuffix(s.rel(file), conflictSuffix)
		if key == rootKey || seen[key] || keyexpr.Validate(key) != nil {
			return
		}
		seen[key] = true
		add(key)
	})
	return out, err
}

// walk calls fn on the regular files under dir, skipping the metadata
// directory and, without follow_links, symbolic links.
func (s *Storage) walk(dir string, fn func(string)) error {
	des, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, de := range des {
		p := filepath.Join(dir, de.Name())
		t := de.Type()
		if t&fs.ModeSymlink != 0 {
			if !s.followLinks {
				continue
			}
			st, err := os.Stat(p)
			if err != nil {
				continue
			}
			t = st.Mode().Type()
		}
		switch {
		case t.IsDir():
			if de.Name() == dataInfoDir {
				continue
			}
			_ = s.walk(p, fn)
		case t.IsRegular():
			fn(p)
		}
	}
	return nil
}

// Close keeps the files, or removes the directory with on_closure
// "delete_all".
func (s *Storage) Close() error {
	err := s.info.close()
	if s.deleteAll {
		err = errors.Join(err, os.RemoveAll(s.base))
	}
	return err
}
