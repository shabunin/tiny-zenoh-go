package zenoh

import (
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
)

func TestMain(m *testing.M) {
	leaktest.Main(m, func() {
		if goRouter.s != nil { // ZENOH_ROUTER=go: the shared in-process router
			_ = goRouter.s.Close()
		}
	})
}
