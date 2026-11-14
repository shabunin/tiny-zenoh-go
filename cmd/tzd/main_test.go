package main

import (
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
)

// teardown runs after the tests (binary_test.go removes the built tzd).
var teardown []func()

func TestMain(m *testing.M) { leaktest.Main(m, teardown...) }
