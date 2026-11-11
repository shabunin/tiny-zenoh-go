package link

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
)

// ErrTLSConfig reports missing or unreadable TLS material.
var ErrTLSConfig = errors.New("link: tls configuration")

// tlsMaterial resolves one PEM item: locator config (_raw, _base64, _file)
// first, then the file named in the TLSConfig, then its PEM bytes.
func tlsMaterial(cfg map[string]string, key, file string, data []byte) ([]byte, error) {
	if v, ok := cfg[key+"_raw"]; ok {
		return []byte(v), nil
	}
	if v, ok := cfg[key+"_base64"]; ok {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %s_base64: %v", ErrTLSConfig, key, err)
		}
		return b, nil
	}
	if v, ok := cfg[key+"_file"]; ok {
		file = v
	}
	if file == "" {
		return data, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrTLSConfig, key, err)
	}
	return b, nil
}

func tlsBool(cfg map[string]string, key string, def bool) bool {
	switch cfg[key] {
	case "true":
		return true
	case "false":
		return false
	}
	return def
}
