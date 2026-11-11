//go:build !tinygo

package main

import (
	"github.com/shabunin/tiny-zenoh-go/plugins/rest"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func startREST(s *zenoh.Session, addr string) (func() error, string, error) {
	srv, err := rest.Serve(s, addr)
	if err != nil {
		return nil, "", err
	}
	return srv.Close, srv.Addr(), nil
}
