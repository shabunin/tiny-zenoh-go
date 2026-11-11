//go:build tinygo && !zenoh_stats

package zenoh

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// TinyGo builds leave traffic statistics out (about 30 KB of code) unless
// built with -tags zenoh_stats; these stand in for them.

const statsDefault = false

type statsRegistry struct{ filters []string }

type statsTransport struct{}

type adminStats struct{ root, rootFiltered string }

func newStatsRegistry(StatsConfig) (*statsRegistry, error) {
	return nil, errors.New("not in this build (TinyGo: build with -tags zenoh_stats)")
}

func (s *Session) statsUp(*face)                              {}
func (s *Session) statsLinks(*face, []adminLink, []adminLink) {}
func (s *Session) statsDown(*face, []adminLink)               {}
func (s *Session) statsGroupUp(*mcastGroup)                   {}
func (s *Session) statsGroupDown(*mcastGroup)                 {}
func (f *face) observeRx(protocol.Message, bool)              {}
func (f *face) observeTx([]transport.Encoder)                 {}
func (f *face) statsDrop(int, uint8, any)                     {}
func (s *Session) adminStats() *adminStats                    { return nil }

func (a *adminStats) sessionFields([]string, string) []jsonField { return nil }
func (a *adminStats) linkField(_, _, _ string) jsonField         { return jsonField{} }

// Stats returns zero: statistics are not in this build.
func (s *Session) Stats() Stats { return Stats{} }

// Metrics shows the build information only: statistics are not in this
// build.
func (s *Session) Metrics(MetricsOptions) string { return s.buildInfoMetrics() }

func (s *Session) metrics(MetricsOptions) string { return s.buildInfoMetrics() }
