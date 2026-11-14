package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// pluginRunner runs the built-in plugins and applies changes to their
// configuration made through the admin space (adminspace/permissions/write:
// PUT/DELETE on @/<zid>/router/config/plugins/...), as zenohd's plugin
// manager does: storages are started and stopped as they come and go
// (restarted when their configuration changes), the REST server follows
// plugins/rest/http_port.
type pluginRunner struct {
	mu       sync.Mutex
	s        *zenoh.Session
	tree     map[string]any // the plugins section
	volumes  map[string]runningVolume
	storages map[string]runningStorage
	restAddr string
	restStop func() error
}

type runningStorage struct {
	raw    string
	volume string
	stop   func() error
}

type runningVolume struct {
	raw string
	vol storage.Volume
}

func newPluginRunner(tree map[string]any) *pluginRunner {
	if tree == nil {
		tree = map[string]any{}
	}
	return &pluginRunner{tree: tree, volumes: map[string]runningVolume{}, storages: map[string]runningStorage{}}
}

// start runs the plugins of p on s.
func (r *pluginRunner) start(s *zenoh.Session, p *pluginsConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s = s
	if err := r.applyLocked(p); err != nil {
		r.stopLocked()
		return err
	}
	return nil
}

// startPlugins starts the configured storages and REST server on s; the
// returned function stops them.
func startPlugins(s *zenoh.Session, p *pluginsConfig) (func(), error) {
	r := newPluginRunner(nil)
	if err := r.start(s, p); err != nil {
		return nil, err
	}
	return r.stop, nil
}

// applyLocked makes the running plugins match p.
func (r *pluginRunner) applyLocked(p *pluginsConfig) error {
	wantVol := map[string]namedVolume{}
	for _, nv := range p.volumes {
		wantVol[nv.name] = nv
	}
	want := map[string]namedStorage{}
	for _, ns := range p.storages {
		want[ns.name] = ns
	}
	// Storages go before their volume does.
	goneVol := map[string]bool{}
	for name, rv := range r.volumes {
		if nv, ok := wantVol[name]; !ok || nv.raw != rv.raw {
			goneVol[name] = true
		}
	}
	for name, rs := range r.storages {
		if ns, ok := want[name]; !ok || ns.raw != rs.raw || goneVol[rs.volume] {
			_ = rs.stop()
			delete(r.storages, name)
			printf("storage %s stopped\n", name)
		}
	}
	for name := range goneVol {
		_ = r.volumes[name].vol.Close()
		delete(r.volumes, name)
		printf("volume %s closed\n", name)
	}
	var errs []error
	for _, nv := range p.volumes {
		if _, ok := r.volumes[nv.name]; ok {
			continue
		}
		vol, err := storage.OpenVolume(nv.backend, nv.name, nv.cfg)
		if err != nil {
			errs = append(errs, fmt.Errorf("volume %s: %w", nv.name, err))
			continue
		}
		r.volumes[nv.name] = runningVolume{nv.raw, vol}
		printf("volume %s (backend %s)\n", nv.name, nv.backend)
	}
	for _, ns := range p.storages {
		if _, ok := r.storages[ns.name]; ok {
			continue
		}
		var vol storage.Volume = storage.MemoryVolume{}
		if ns.volume != "memory" {
			rv, ok := r.volumes[ns.volume]
			if !ok {
				errs = append(errs, fmt.Errorf("storage %s: no volume %q (configured: %s)", ns.name, ns.volume,
					strings.Join(volumeNames(p), ", ")))
				continue
			}
			vol = rv.vol
		}
		cfg := ns.cfg
		name := ns.name
		cfg.OnError = func(err error) { warnf("storage %s: %v", name, err) }
		m, err := storage.New(r.s, cfg, vol, ns.params)
		if err != nil {
			errs = append(errs, fmt.Errorf("storage %s: %w", ns.name, err))
			continue
		}
		r.storages[ns.name] = runningStorage{ns.raw, ns.volume, m.Close}
		printf("storage %s on %s (volume %s)\n", ns.name, ns.cfg.KeyExpr, ns.volume)
	}
	if p.restAddr != r.restAddr {
		if r.restStop != nil {
			_ = r.restStop()
			r.restStop, r.restAddr = nil, ""
			printf("REST API stopped\n")
		}
		if p.restAddr != "" {
			closeREST, addr, err := startREST(r.s, p.restAddr)
			if err != nil {
				errs = append(errs, err)
			} else {
				r.restStop, r.restAddr = closeREST, p.restAddr
				printf("REST API on http://%s\n", addr)
			}
		}
	}
	return errors.Join(errs...)
}

func (r *pluginRunner) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopLocked()
}

func (r *pluginRunner) stopLocked() {
	if r.restStop != nil {
		_ = r.restStop()
		r.restStop, r.restAddr = nil, ""
		printf("REST API stopped\n")
	}
	for name, rs := range r.storages {
		_ = rs.stop()
		delete(r.storages, name)
		printf("storage %s stopped\n", name)
	}
	for name, rv := range r.volumes {
		_ = rv.vol.Close()
		delete(r.volumes, name)
		printf("volume %s closed\n", name)
	}
}

func volumeNames(p *pluginsConfig) []string {
	out := []string{"memory"}
	for _, nv := range p.volumes {
		out = append(out, nv.name)
	}
	return out
}

// onConfig applies a config write (zenoh.AdminSpaceConfig.OnConfig): key
// is plugins/<path>, value the JSON5 value of a PUT or nil for a DELETE.
// A last segment <field>=<value> addresses the objects of an array whose
// <field> is <value>, as zenoh-rust's try_insert_json5_array_item.
func (r *pluginRunner) onConfig(key string, value []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.s == nil {
		return errors.New("plugins not started yet")
	}
	path := strings.TrimPrefix(key, "plugins/")
	if path == "" {
		return errors.New("empty config key")
	}
	// Work on a copy: a change that does not decode leaves things as they are.
	b, err := json.Marshal(r.tree)
	if err != nil {
		return err
	}
	var tree map[string]any
	if err := json.Unmarshal(b, &tree); err != nil {
		return err
	}
	if value == nil {
		err = removePath(tree, path)
	} else {
		var v any
		if v, err = parseJSON5(value); err == nil {
			err = insertConfig(tree, path, v)
		}
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(tree)
	if err != nil {
		return err
	}
	var p pluginsConfig
	if err := decodePlugins(raw, &p); err != nil {
		return err
	}
	r.tree = tree
	return r.applyLocked(&p)
}

// splitFilter splits path ".../<array>/<field>=<value>" into the array's
// path, the field and the value; ok is false without a filter.
func splitFilter(path string) (array, field, value string, ok bool, err error) {
	prefix, value, ok := strings.Cut(path, "=")
	if !ok {
		return "", "", "", false, nil
	}
	i := strings.LastIndexByte(prefix, '/')
	if i < 0 {
		return "", "", "", true, errors.New("missing field filter")
	}
	return prefix[:i], prefix[i+1:], value, true, nil
}

// lookupPath returns the value at path in tree.
func lookupPath(tree map[string]any, path string) (any, bool) {
	var cur any = tree
	for _, p := range strings.Split(path, "/") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func insertConfig(tree map[string]any, path string, v any) error {
	array, field, fv, filtered, err := splitFilter(path)
	if err != nil {
		return err
	}
	if !filtered {
		return insertPath(tree, path, v)
	}
	item, ok := v.(map[string]any)
	if !ok || item[field] != fv {
		return fmt.Errorf("field filter mismatch: value must be an object containing %s=%q", field, fv)
	}
	cur, _ := lookupPath(tree, array)
	list, ok := cur.([]any)
	if !ok && cur != nil {
		return fmt.Errorf("%s is not an array", array)
	}
	replaced := false
	for i, x := range list {
		m, ok := x.(map[string]any)
		if !ok {
			return errors.New("array item is not an object")
		}
		if m[field] == fv {
			list[i], replaced = item, true
			break
		}
	}
	if !replaced {
		list = append(list, item)
	}
	return insertPath(tree, array, list)
}

func removePath(tree map[string]any, path string) error {
	array, field, fv, filtered, err := splitFilter(path)
	if err != nil {
		return err
	}
	if filtered {
		cur, _ := lookupPath(tree, array)
		list, ok := cur.([]any)
		if !ok {
			return fmt.Errorf("%s is not an array", array)
		}
		kept := list[:0]
		for _, x := range list {
			if m, ok := x.(map[string]any); !ok || m[field] != fv {
				kept = append(kept, x)
			}
		}
		return insertPath(tree, array, kept)
	}
	parent, last := "", path
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		parent, last = path[:i], path[i+1:]
	}
	m := tree
	if parent != "" {
		cur, ok := lookupPath(tree, parent)
		if m, ok = cur.(map[string]any); !ok {
			return fmt.Errorf("plugins/%s: no such key", path)
		}
	}
	if _, ok := m[last]; !ok {
		return fmt.Errorf("plugins/%s: no such key", path)
	}
	delete(m, last)
	return nil
}
