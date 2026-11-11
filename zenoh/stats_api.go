package zenoh

import "strings"

// StatsConfig configures traffic statistics.
type StatsConfig struct {
	// Enabled keeps statistics. Unset: on. Like zenoh-rust's "stats"
	// feature they are compiled in, except on TinyGo, where they need
	// -tags zenoh_stats (without it, enabling them fails Open).
	Enabled *bool
	// Filters are key expressions with payload statistics of their own
	// (zenoh-rust stats/filters): a message counts for each filter its key
	// intersects.
	Filters []string
}

// TrafficStats is traffic in one direction.
type TrafficStats struct {
	Bytes             uint64 // on the wire, framing included
	TransportMessages uint64 // FRAMEs, FRAGMENTs, KEEP_ALIVEs...
	NetworkMessages   uint64 // all kinds
	// Messages and PayloadBytes count PUT, DELETE, query and reply
	// messages and their payload (plus attachment) sizes, all key spaces.
	Messages, PayloadBytes uint64
	// Dropped counts the data messages the node dropped (access control,
	// downsampling, low-pass filter, no link).
	Dropped uint64
}

// LinkStats is the traffic of one link (Messages, PayloadBytes and Dropped
// are only counted per transport).
type LinkStats struct {
	Link   Link
	Tx, Rx TrafficStats
}

// TransportStats is the traffic of one transport: its links' and those it
// had. A multicast group is a transport with Transport.Multicast set and a
// zero ZID.
type TransportStats struct {
	Transport TransportInfo
	Tx, Rx    TrafficStats
	Links     []LinkStats
}

// Stats is a node's traffic.
type Stats struct {
	// Tx and Rx include closed transports.
	Tx, Rx     TrafficStats
	Transports []TransportStats // open ones
}

// MetricsOptions select what Session.Metrics shows; the zero value is
// zenohd's default.
type MetricsOptions struct {
	NoPerTransport bool // no per-transport series (?per_transport=false)
	NoPerLink      bool // no per-link series (?per_link=false)
	NoPerKey       bool // no per-filter series (?per_key=false)
	Disconnected   bool // include transports closed less than a minute ago (?disconnected=true)
	NoDescriptors  bool // no # HELP/TYPE/UNIT lines (?descriptors=false)
}

// Drop reasons (zenoh-rust ReasonLabel).
const (
	dropACL = iota
	dropCongestion
	dropDownsampling
	dropLowPass
	dropNoLink
)

var dropReasonNames = [...]string{"access-control", "congestion", "downsampling", "low-pass", "no-link"}

// buildInfoMetrics is what zenoh-rust's <root>/metrics shows without its
// "stats" feature (with its stray space after "# EOF").
func (s *Session) buildInfoMetrics() string {
	var b strings.Builder
	b.WriteString("# HELP zenoh_build Zenoh build version.\n# TYPE zenoh_build info\n")
	b.WriteString(`zenoh_build_info{local_id="` + s.zid.String() + `",local_whatami="` + s.cfg.Mode.String() +
		`",version="` + versionString() + "\"} 1\n# EOF\n ")
	return b.String()
}
