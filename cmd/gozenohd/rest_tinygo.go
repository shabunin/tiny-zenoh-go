//go:build tinygo

package main

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func startREST(*zenoh.Session, string) (func() error, string, error) {
	return nil, "", errors.New("the REST plugin is not available in TinyGo builds")
}
