//go:build !tinygo

package main

// The storage backends that talk HTTP (not in TinyGo builds).
import (
	_ "github.com/shabunin/tiny-zenoh-go/plugins/storage/influxdb"
	_ "github.com/shabunin/tiny-zenoh-go/plugins/storage/s3"
)
