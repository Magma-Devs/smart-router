package redisstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
)

// Topology selects how the client reaches the backend.
type Topology string

const (
	// TopologyStandalone dials a single node (or a single endpoint fronting
	// one, e.g. a Global Datastore primary/reader endpoint).
	TopologyStandalone Topology = "standalone"
	// TopologySentinel discovers the primary through a sentinel quorum and
	// follows failovers transparently.
	TopologySentinel Topology = "sentinel"
	// TopologyCluster joins a sharded cluster through a configuration
	// endpoint; the client discovers topology (nodes, slots, replicas) itself,
	// so Addresses holds seed endpoint(s), never the full node list.
	TopologyCluster Topology = "cluster"
)

const DefaultCredentialRefreshInterval = 10 * time.Second

// Config is the full connection surface of the RESP backend. The mapstructure
// tags are the operator-facing YAML keys of the router's `resp-cache:` block.
type Config struct {
	// Topology defaults to standalone when empty.
	Topology Topology `mapstructure:"topology"`
	// Addresses: standalone = the node address; sentinel = the sentinel
	// addresses; cluster = the configuration endpoint(s) used as discovery
	// seeds. Never empty.
	Addresses []string `mapstructure:"addresses"`
	// ReadAddresses optionally builds a SECOND client of the same topology for
	// reads (reader endpoints in multi-region deployments). Empty = reads and
	// writes share one client. This selects an ENDPOINT, not a replica role:
	// under sentinel/cluster the read client discovers and resolves to the
	// master(s) those seeds front, so it only yields replica reads when it
	// points at a separate replicated deployment (warned about in New).
	ReadAddresses []string `mapstructure:"read-addresses"`
	// MasterName is the sentinel-monitored master set name (sentinel only).
	MasterName string `mapstructure:"master-name"`

	// Data-node credentials. Password and PasswordFile are mutually
	// exclusive; the file variant is watched and rotations are pushed to live
	// connections through the streaming provider (see credentials.go). A file
	// containing "username:password" also rotates the username.
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	PasswordFile string `mapstructure:"password-file"`
	// CredentialRefreshInterval is the PasswordFile poll cadence
	// (DefaultCredentialRefreshInterval when zero).
	CredentialRefreshInterval time.Duration `mapstructure:"credential-refresh-interval"`

	// Sentinel control-plane credentials — distinct from data-node
	// credentials: sentinels authenticate independently, and hardened
	// deployments fail discovery without them. Read at construction time.
	SentinelUsername     string `mapstructure:"sentinel-username"`
	SentinelPassword     string `mapstructure:"sentinel-password"`
	SentinelPasswordFile string `mapstructure:"sentinel-password-file"`

	// DB selects the logical database (standalone/sentinel only; cluster has
	// exactly one).
	DB int `mapstructure:"db"`

	KeyPrefix string    `mapstructure:"key-prefix"`
	TLS       TLSConfig `mapstructure:"tls"`

	// Expiration is the TTL table the router's in-process cache engine applies
	// over this backend. It mirrors the cache sidecar's expiration flags, which
	// the chart sets; without it a router that moved its cache to a RESP
	// backend silently lost the chart's settings — the shipped default
	// multiplier of 1.5 on settled answers among them — with no way to set
	// them back (MAG-3631). Unset fields keep the engine's built-in defaults.
	Expiration ExpirationConfig `mapstructure:"expiration"`

	DialTimeout  time.Duration `mapstructure:"dial-timeout"`
	ReadTimeout  time.Duration `mapstructure:"read-timeout"`
	WriteTimeout time.Duration `mapstructure:"write-timeout"`
	PoolSize     int           `mapstructure:"pool-size"`
}

// TLSConfig is the file-based TLS surface (config-file friendly).
//
// Enabled is the switch, and it is the ONLY key that turns TLS on: build reads
// nothing else while it is false. A block that carries any other tls.* key
// without it is therefore refused by Config.Validate rather than accepted —
// otherwise the router starts, opens a plaintext connection, and its first
// write puts the configured username and password on the wire readable, while
// the operator who wrote three certificate paths believes the opposite.
type TLSConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// CAFile roots server verification; empty falls back to the system pool.
	CAFile string `mapstructure:"ca-file"`
	// CertFile/KeyFile enable mTLS; both or neither.
	CertFile string `mapstructure:"cert-file"`
	KeyFile  string `mapstructure:"key-file"`
	// ServerName overrides SNI/verification name (endpoints fronted by DNS
	// that differs from the certificate).
	ServerName         string `mapstructure:"server-name"`
	InsecureSkipVerify bool   `mapstructure:"insecure-skip-verify"`
}

// ExpirationConfig is the operator-facing form of core.Policy: one key per
// cache-sidecar expiration flag, with the same meaning and the same defaults,
// so a value moved from the chart's sidecar settings to this block yields the
// same lifetimes. Zero means "the default" for every field.
type ExpirationConfig struct {
	// Finalized is the lifetime of a settled (finalized) answer; the sidecar's
	// --expiration. Default one hour.
	Finalized time.Duration `mapstructure:"finalized"`
	// FinalizedMultiplier scales Finalized; the sidecar's
	// --expiration-multiplier, which the published chart sets to 1.5.
	FinalizedMultiplier float64 `mapstructure:"finalized-multiplier"`
	// NonFinalized is the floor for a recent (non-finalized) answer — the
	// effective TTL is max(averageBlockTime/8, NonFinalized); the sidecar's
	// --expiration-non-finalized. Default 500ms.
	NonFinalized time.Duration `mapstructure:"non-finalized"`
	// NonFinalizedMultiplier scales NonFinalized; the sidecar's
	// --expiration-non-finalized-multiplier.
	NonFinalizedMultiplier float64 `mapstructure:"non-finalized-multiplier"`
	// NodeErrors caps a cached node error on a finalized block; the sidecar's
	// --expiration-finalized-node-errors. Default 250ms.
	NodeErrors time.Duration `mapstructure:"node-errors"`
	// BlocksHashesToHeights is the lifetime of a block-hash→height mapping; the
	// sidecar's --expiration-blocks-hashes-to-heights. Default 48h.
	BlocksHashesToHeights time.Duration `mapstructure:"blocks-hashes-to-heights"`
}

// minLifetime is the shortest lifetime the store can express. A RESP expiry is
// set with PX, whole milliseconds, and go-redis rounds anything shorter up to
// one millisecond on every write while printing a warning through its own
// logger (formatMs), so a lifetime below it is refused at startup instead.
// It is also where a value written without a unit shows itself: mapstructure
// maps a bare YAML integer onto the duration as nanoseconds, so `finalized:
// 3600` is 3.6µs rather than an hour, and the sidecar's flag would have
// refused it outright ("missing unit").
const minLifetime = time.Millisecond

// Policy builds the engine's TTL table from the block, the way the sidecar
// builds its own from flags: each unset duration keeps its default, and each
// multiplier (default 1) scales the duration it belongs to.
//
// The table comes from resolve, the same computation validate checks, so a
// block that passed validation yields exactly what it promised and one that
// bypassed it is clamped to the nearest lifetime the store can express — it
// still never yields a zero, which to the store is a key with no expiry.
func (e ExpirationConfig) Policy() core.Policy {
	policy, _ := e.resolve()
	return policy
}

// resolve computes every row of the TTL table and reports the first row that
// is no lifetime, naming its keys. Rows are visited in a fixed order so the
// same block always yields the same message. Every row is resolved even after
// an error, clamped, so Policy has a complete table to hand out.
func (e ExpirationConfig) resolve() (core.Policy, error) {
	policy := core.DefaultPolicy()
	rows := []struct {
		name           string
		multiplierName string
		configured     time.Duration
		multiplier     float64
		target         *time.Duration
	}{
		{"expiration.finalized", "expiration.finalized-multiplier", e.Finalized, e.FinalizedMultiplier, &policy.Finalized},
		{"expiration.non-finalized", "expiration.non-finalized-multiplier", e.NonFinalized, e.NonFinalizedMultiplier, &policy.NonFinalized},
		{"expiration.node-errors", "", e.NodeErrors, 0, &policy.NodeErrors},
		{"expiration.blocks-hashes-to-heights", "", e.BlocksHashesToHeights, 0, &policy.BlocksHashesToHeights},
	}
	var firstErr error
	for _, row := range rows {
		base := *row.target
		if row.configured > 0 {
			base = row.configured
		}
		lifetime, err := effectiveLifetime(base, row.multiplier)
		*row.target = lifetime
		if err == nil || firstErr != nil {
			continue
		}
		what := row.name
		if row.multiplier > 0 {
			what = fmt.Sprintf("%s (%s) with %s (%g)", row.name, base, row.multiplierName, row.multiplier)
		}
		firstErr = fmt.Errorf("resp-cache: %s %w", what, err)
		if row.configured > 0 && row.configured < minLifetime {
			// The configured value itself is below the floor, whatever the
			// multiplier did to it: almost always a number written without a unit.
			firstErr = fmt.Errorf("%w; a bare number is read as nanoseconds (%d is %s), so write the value with a unit such as %ds",
				firstErr, int64(row.configured), row.configured, int64(row.configured))
		}
	}
	return policy, firstErr
}

// effectiveLifetime is the lifetime a duration and its multiplier produce, as
// Policy applies it, and the reason it must not be applied when there is one.
// A multiplier of zero means "unset" and leaves the base alone.
//
// The product is checked before it becomes a duration, because the conversion
// hides two failures. A product past the range of a duration wraps. A product
// below minLifetime is one the store cannot express: it would be rounded up to
// one millisecond by the client on every write, and the shape that matters
// most, a product under one nanosecond, would truncate to a zero TTL first —
// and a zero TTL is not "expire at once" to the store but "no expiry at all",
// since SetEntry writes a plain SET, so a setting meant to shorten retention
// created a permanent key that no volatile-* eviction policy can reclaim
// (Codex review of #405). The value returned alongside an error is clamped to
// the nearest lifetime the store can express.
func effectiveLifetime(base time.Duration, multiplier float64) (time.Duration, error) {
	product := float64(base)
	if multiplier > 0 {
		product *= multiplier
	}
	if product >= math.MaxInt64 {
		return time.Duration(math.MaxInt64), fmt.Errorf("is longer than a duration can hold")
	}
	if product < float64(minLifetime) {
		return minLifetime, fmt.Errorf("is %s, shorter than the %s a RESP expiry can express", time.Duration(product), minLifetime)
	}
	return time.Duration(product), nil
}

// validate rejects what no lifetime can mean: a negative duration, a negative
// multiplier, or a resolved lifetime the store cannot express (see resolve and
// effectiveLifetime). Zero is "the default" everywhere and is accepted.
func (e ExpirationConfig) validate() error {
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{"expiration.finalized", e.Finalized},
		{"expiration.non-finalized", e.NonFinalized},
		{"expiration.node-errors", e.NodeErrors},
		{"expiration.blocks-hashes-to-heights", e.BlocksHashesToHeights},
	} {
		if field.value < 0 {
			return fmt.Errorf("resp-cache: %s must not be negative (got %s; leave it unset for the default)", field.name, field.value)
		}
	}
	for _, field := range []struct {
		name  string
		value float64
	}{
		{"expiration.finalized-multiplier", e.FinalizedMultiplier},
		{"expiration.non-finalized-multiplier", e.NonFinalizedMultiplier},
	} {
		if field.value < 0 {
			return fmt.Errorf("resp-cache: %s must not be negative (got %g; leave it unset for 1)", field.name, field.value)
		}
	}
	_, err := e.resolve()
	return err
}

// hasMaterial reports whether the block carries any setting other than the
// switch itself — the operator wrote a tls section, whatever Enabled says.
//
// It compares the whole struct with the switch cleared rather than naming the
// keys, so a key added to TLSConfig is covered by the rule without anyone
// remembering to list it here; it stops compiling if a non-comparable field is
// ever added, which is the moment to revisit it.
func (c TLSConfig) hasMaterial() bool {
	c.Enabled = false
	return c != TLSConfig{}
}

// build materialises the tls.Config, nil when disabled. Files are read
// eagerly so misconfiguration fails at startup, not first dial.
func (c TLSConfig) build() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // operator opt-in, validated config
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("resp-cache tls: reading ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("resp-cache tls: ca-file %q contains no valid certificates", c.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, fmt.Errorf("resp-cache tls: cert-file and key-file must be set together")
	}
	if c.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("resp-cache tls: loading client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// Validate applies the fail-fast startup rules for the connection config.
// (The key prefix has its own validation in NewWithClient.)
func (cfg Config) Validate() error {
	switch cfg.EffectiveTopology() {
	case TopologyStandalone, TopologySentinel, TopologyCluster:
	default:
		return fmt.Errorf("resp-cache: unknown topology %q (want standalone, sentinel, or cluster)", cfg.Topology)
	}
	if len(cfg.Addresses) == 0 {
		return fmt.Errorf("resp-cache: no addresses configured")
	}
	if cfg.EffectiveTopology() == TopologySentinel && cfg.MasterName == "" {
		return fmt.Errorf("resp-cache: sentinel topology requires master-name")
	}
	if cfg.EffectiveTopology() != TopologySentinel && (cfg.SentinelUsername != "" || cfg.SentinelPassword != "" || cfg.SentinelPasswordFile != "") {
		return fmt.Errorf("resp-cache: sentinel-* credentials are set but topology is %q — dangling configuration", cfg.EffectiveTopology())
	}
	// The other half of the check above. A master-name is read by nothing
	// except the sentinel client, so under any other topology the operator
	// wrote a sentinel configuration and left out the line that says so: the
	// router then dialled the first sentinel address as an ordinary data node,
	// every cache operation failed, and the startup line printed a blank
	// topology — byte for byte what a configuration with no master-name at all
	// produced (MAG-3671).
	if cfg.EffectiveTopology() != TopologySentinel && cfg.MasterName != "" {
		return fmt.Errorf("resp-cache: master-name %q is set but topology is %q — dangling configuration: master-name is only read under topology: sentinel (set it, or remove master-name)", cfg.MasterName, cfg.EffectiveTopology())
	}
	// Standalone dials exactly one address (standaloneOptions takes the first
	// element), where sentinel and cluster take the whole list. A longer list
	// here used to be truncated in silence while the startup line echoed every
	// address back at the operator, so a "spare" written for redundancy — or a
	// list left behind when moving a block from a multi-node topology — read as
	// confirmed and did nothing (MAG-3672). Refused rather than warned: there is
	// no reading of a two-address standalone block under which the operator
	// wanted the second one ignored. Checked after the master-name rule on
	// purpose: a sentinel block that forgot its topology line trips both, and
	// the master-name message is the precise diagnosis of that mistake.
	if cfg.EffectiveTopology() == TopologyStandalone {
		if len(cfg.Addresses) > 1 {
			return fmt.Errorf("resp-cache: %d addresses are configured but topology is standalone, which dials exactly one — %s would be ignored (dangling configuration: set topology: sentinel or cluster, or configure a single address)",
				len(cfg.Addresses), strings.Join(cfg.Addresses[1:], ", "))
		}
		if len(cfg.ReadAddresses) > 1 {
			return fmt.Errorf("resp-cache: %d read-addresses are configured but topology is standalone, which dials exactly one — %s would be ignored (dangling configuration: set topology: sentinel or cluster, or configure a single read address)",
				len(cfg.ReadAddresses), strings.Join(cfg.ReadAddresses[1:], ", "))
		}
	}
	if cfg.EffectiveTopology() == TopologyCluster && cfg.DB != 0 {
		return fmt.Errorf("resp-cache: db selection is not available in cluster topology")
	}
	if cfg.Password != "" && cfg.PasswordFile != "" {
		return fmt.Errorf("resp-cache: password and password-file are mutually exclusive")
	}
	if cfg.SentinelPassword != "" && cfg.SentinelPasswordFile != "" {
		return fmt.Errorf("resp-cache: sentinel-password and sentinel-password-file are mutually exclusive")
	}
	// The same rule as the sentinel-* credentials above: a half-written section
	// is a deployment mistake, not a configuration. Here the cost of accepting
	// it is a credential crossing the network in the clear, so this is the one
	// combination that must not be able to start quietly. The message keeps to
	// the shape of its siblings; the reasoning lives on TLSConfig.
	if !cfg.TLS.Enabled && cfg.TLS.hasMaterial() {
		return fmt.Errorf("resp-cache: tls.* options are set but tls.enabled is not true — dangling configuration (set tls.enabled: true, or remove the other tls.* keys)")
	}
	return cfg.Expiration.validate()
}

// EffectiveTopology is the topology the client is actually built with:
// standalone when the field is empty. Every decision in this package goes
// through it, and so must anything that reports the configuration back to an
// operator — the startup line used to print the raw field, so an omitted
// topology showed up as a blank, and a sentinel configuration missing its
// topology line left no trace that it had been resolved as standalone
// (MAG-3671).
func (cfg Config) EffectiveTopology() Topology {
	if cfg.Topology == "" {
		return TopologyStandalone
	}
	return cfg.Topology
}

func (cfg Config) refreshInterval() time.Duration {
	if cfg.CredentialRefreshInterval <= 0 {
		return DefaultCredentialRefreshInterval
	}
	return cfg.CredentialRefreshInterval
}

// configuredCredentialKeys names every credential key the block sets, data
// node and sentinel control plane alike — the keys, never the values. Every
// one of them is sent to the backend on each new connection, so together they
// are what crosses the network readable when TLS is off.
func (cfg Config) configuredCredentialKeys() []string {
	var keys []string
	for _, key := range []struct {
		name string
		set  bool
	}{
		{"username", cfg.Username != ""},
		{"password", cfg.Password != ""},
		{"password-file", cfg.PasswordFile != ""},
		{"sentinel-username", cfg.SentinelUsername != ""},
		{"sentinel-password", cfg.SentinelPassword != ""},
		{"sentinel-password-file", cfg.SentinelPasswordFile != ""},
	} {
		if key.set {
			keys = append(keys, key.name)
		}
	}
	return keys
}

// credentialsSource picks the data-node credential source: file-backed when
// PasswordFile is set (rotation-capable), static otherwise.
func (cfg Config) credentialsSource() CredentialsSource {
	if cfg.PasswordFile != "" {
		return &FileCredentials{Username: cfg.Username, Path: cfg.PasswordFile}
	}
	return StaticCredentials{Username: cfg.Username, Password: cfg.Password}
}

// sentinelPassword resolves the control-plane password (file wins when set).
//
// ROTATION REQUIRES A RESTART. The file is read once here and the resolved
// string is captured in FailoverOptions.SentinelPassword, so go-redis reuses
// that same value for every subsequent discovery — rewriting the file has no
// effect on a running router. This differs from the DATA-node credentials,
// which are resolved per connection attempt (sentinel) or refreshed in place
// (standalone/cluster). Documented in docs/RESP-CACHE.md.
//
// The file is read by the same reader as the data-node file, so the same
// rules hold: a leading byte order mark and the four ASCII whitespace bytes
// are trimmed, an empty file is refused, and a credential beginning with an
// invisible rune is reported (MAG-3685 review).
func (cfg Config) sentinelPassword() (string, error) {
	if cfg.SentinelPasswordFile == "" {
		return cfg.SentinelPassword, nil
	}
	pw, err := readCredentialFile(cfg.SentinelPasswordFile)
	if err != nil {
		return "", fmt.Errorf("resp-cache: reading sentinel-password-file: %w", err)
	}
	if field, r, ok := invisibleStart("", pw); ok {
		warnCredentialBeginsInvisibly(cfg.SentinelPasswordFile, "sentinel-"+field, r)
	}
	return pw, nil
}

// clientCredentials is what a client build takes from the credential
// settings, resolved once per store: one source for the data-node
// credentials, the provider that pushes a file-backed one to live
// connections, and the sentinel control-plane password, read once. One of
// each however many clients the store builds, so a file is read once at
// startup and the once-only lines about it fire once — New used to build a
// FileCredentials for its fail-fast, another for the provider and a third for
// the sentinel client, and the "read as username:password" line fired for
// each (MAG-3685 review).
type clientCredentials struct {
	source           CredentialsSource
	provider         *StreamingProvider
	sentinelPassword string
}

func (cfg Config) resolveClientCredentials() (clientCredentials, error) {
	creds := clientCredentials{source: cfg.credentialsSource()}
	// The streaming provider exists to push a rotated file-backed credential
	// to live connections. Static credentials used to ride it too, for
	// uniformity, which put every connection of every RESP client on the path
	// that kept abandoned connections alive (MAG-3728); they now go straight
	// into the client options, and the provider is built only when it has a
	// file to watch and a client that subscribes (sentinel resolves the file
	// per connection attempt instead, see failoverOptions).
	if cfg.PasswordFile != "" && cfg.EffectiveTopology() != TopologySentinel {
		creds.provider = NewStreamingProvider(creds.source)
	}
	if cfg.EffectiveTopology() == TopologySentinel {
		pw, err := cfg.sentinelPassword()
		if err != nil {
			return clientCredentials{}, err
		}
		creds.sentinelPassword = pw
	}
	return creds, nil
}

// ---------------------------------------------------------------------------
// Config → go-redis options mapping (pure, so tests assert it directly)
// ---------------------------------------------------------------------------

// DefaultDialTimeout bounds a fresh connection's dial and handshake when the
// config leaves dial-timeout unset. go-redis's own default is 5 seconds —
// sized for batch clients, not for a dialer on the relay path, where every
// cold lookup against a black-holed backend pays the full dial budget (one
// per pool slot) before degrading to a miss.
const DefaultDialTimeout = 500 * time.Millisecond

func (cfg Config) dialTimeout() time.Duration {
	if cfg.DialTimeout <= 0 {
		return DefaultDialTimeout
	}
	return cfg.DialTimeout
}

// baseDialer is the transport dialer every client variant shares. It exists
// instead of redis.NewDialer for two reasons.
//
// go-redis's TLS branch is the ctx-less tls.DialWithDialer, so a caller's
// context deadline bounds a plaintext dial but never a TLS handshake — a
// black-holed (SYN-dropped) TLS endpoint then costs every cold lookup the full
// DialTimeout instead of the relay's cache budget. tls.Dialer.DialContext
// threads the context through both the TCP dial and the handshake; whichever
// bound (context deadline or DialTimeout) is sooner applies.
//
// And a dial is where the store learns an endpoint is GONE, so it is where that
// has to be recorded: by the time the operation returns, its own error no longer
// says (see ErrEndpointUnreachable). The tracker may be nil for a dialer built
// outside a store.
func baseDialer(tlsCfg *tls.Config, dialTimeout time.Duration, tracker *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
	netDialer := &net.Dialer{Timeout: dialTimeout}
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return netDialer.DialContext(ctx, network, addr)
	}
	if tlsCfg != nil {
		tlsDialer := &tls.Dialer{NetDialer: netDialer, Config: tlsCfg}
		dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return tlsDialer.DialContext(ctx, network, addr)
		}
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		started := time.Now()
		conn, err := dial(ctx, network, addr)
		if err == nil {
			return conn, nil
		}
		// Which failures prove the endpoint is not there, and which prove
		// nothing. An attempt that used up its OWN budget was answered by
		// nobody: a black hole. One that failed with budget to spare was
		// answered — refused, no route, no such name. But one the CONTEXT cut
		// short proves neither: go-redis abandons a queued dial whose caller
		// has gone, and a healthy cache a network away cannot finish a
		// handshake inside a same-zone read budget either. Calling that an
		// outage would send an operator hunting a cache that is up — the same
		// wrong turn as the bug this exists to fix, in the other direction (see
		// common.CacheTimeout, and DefaultDialTimeout above).
		spentItsOwnBudget := dialTimeout > 0 && time.Since(started) >= dialTimeout
		if spentItsOwnBudget || ctx.Err() == nil {
			tracker.noteFault(err)
		}
		return conn, err
	}
}

// trackingDialer records every successful dial so the store learns which
// address it actually connected to — the standalone/cluster tracker, where
// every dial is a data dial (cluster discovery dials ARE cluster nodes).
//
// This is the only way to name the serving node: the configured address is a
// discovery seed (not the shard) under cluster.
func trackingDialer(tlsCfg *tls.Config, dialTimeout time.Duration, tracker *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
	base := baseDialer(tlsCfg, dialTimeout, tracker)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := base(ctx, network, addr)
		if err == nil {
			tracker.note(addr)
		}
		return conn, err
	}
}

// dataDialMarkKey is the context key markDataDialsHook stamps on every dial
// that traverses the failover client's own hook chain.
type dataDialMarkKey struct{}

// markDataDialsHook marks data-path dials so trackingDialerMarkedOnly can tell
// them apart from sentinel control-plane dials.
//
// Why a hook and not an address list: go-redis copies FailoverOptions.Dialer
// verbatim into the options it builds for the SENTINEL control-plane
// connections (sentinelOptions), and discoverSentinels APPENDS every peer the
// quorum reports — as IPs, while configs seed hostnames — so a static exclude
// list built from the configured addresses always misses the discovered peers,
// and the tracker records a sentinel after all (last write wins; sentinels
// re-dial at arbitrary times). The tracker feeds the Lava-Cache-Backend
// header, which is how a failover is observed, so that pollution turns the
// signal into a coin flip.
//
// The hook discriminates by WHICH CLIENT dials instead of by how an address is
// spelled: NewFailoverClient builds its data pool through the returned
// client's own hook chain (rdb.dialHook wraps the pool's dials), while the
// internal sentinel clients are separate *Clients built from sentinelOptions
// that never traverse that chain. A dial carries the mark iff it is a data
// dial — by construction, not by heuristic. buildClient installs the hook;
// trackingDialerMarkedOnly is the other half of the pair.
type markDataDialsHook struct{}

func (markDataDialsHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(context.WithValue(ctx, dataDialMarkKey{}, struct{}{}), network, addr)
	}
}

func (markDataDialsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (markDataDialsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// trackingDialerMarkedOnly records only dials carrying the data-dial mark —
// the sentinel-topology tracker (see markDataDialsHook for why sentinel cannot
// use the plain trackingDialer).
func trackingDialerMarkedOnly(tlsCfg *tls.Config, dialTimeout time.Duration, tracker *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
	base := baseDialer(tlsCfg, dialTimeout, tracker)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := base(ctx, network, addr)
		if err == nil && ctx.Value(dataDialMarkKey{}) != nil {
			tracker.note(addr)
		}
		return conn, err
	}
}

// standaloneOptions builds the single-node client options. provider is nil
// for static credentials, which then travel in the options themselves; only a
// file-backed credential rides the streaming provider (see New).
func (cfg Config) standaloneOptions(addrs []string, tlsCfg *tls.Config, provider *StreamingProvider, tracker *endpointTracker) *redis.Options {
	opts := &redis.Options{
		Dialer:       trackingDialer(tlsCfg, cfg.dialTimeout(), tracker),
		Addr:         addrs[0],
		DB:           cfg.DB,
		TLSConfig:    tlsCfg,
		DialTimeout:  cfg.dialTimeout(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		// The caller's context deadline must bound socket I/O: the router
		// gives cache lookups a tight per-relay budget, and without this
		// go-redis uses only Read/WriteTimeout (seconds) for socket deadlines,
		// letting a slow backend inject latency far past that budget.
		ContextTimeoutEnabled: true,
	}
	// A typed-nil provider must never reach the interface field: go-redis
	// would call it and nil-panic on the first connection.
	if provider != nil {
		opts.StreamingCredentialsProvider = provider
	} else {
		opts.Username, opts.Password = cfg.Username, cfg.Password
	}
	return opts
}

// failoverOptions carries data-node credentials through
// CredentialsProviderContext (fresh resolution on every connection attempt)
// rather than the streaming provider: go-redis v9.22's NewFailoverClient
// accepts StreamingCredentialsProvider in its options but never initializes
// the streaming re-auth manager, so the first operation nil-panics. With the
// context provider, rotated credentials apply on reconnects and failovers —
// in-place re-auth of idle connections needs the upstream gap fixed first.
func (cfg Config) failoverOptions(addrs []string, tlsCfg *tls.Config, source CredentialsSource, sentinelPassword string, tracker *endpointTracker) *redis.FailoverOptions {
	return &redis.FailoverOptions{
		// Records the DATA-node address — whichever node currently holds the
		// master role, which changes on every failover. This same dialer is
		// reused verbatim for the sentinel control-plane connections (including
		// runtime-DISCOVERED sentinels no config list can name), so it records
		// only dials carrying the data-dial mark stamped by markDataDialsHook —
		// which buildClient installs on the failover client's hook chain.
		Dialer:           trackingDialerMarkedOnly(tlsCfg, cfg.dialTimeout(), tracker),
		MasterName:       cfg.MasterName,
		SentinelAddrs:    addrs,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: sentinelPassword,
		DB:               cfg.DB,
		CredentialsProviderContext: func(ctx context.Context) (string, string, error) {
			return source.Credentials()
		},
		TLSConfig:    tlsCfg,
		DialTimeout:  cfg.dialTimeout(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		// See standaloneOptions: the caller's deadline must bound socket I/O.
		ContextTimeoutEnabled: true,
	}
}

// clusterOptions builds the cluster client options; the credential rule is
// standaloneOptions's.
func (cfg Config) clusterOptions(addrs []string, tlsCfg *tls.Config, provider *StreamingProvider, tracker *endpointTracker) *redis.ClusterOptions {
	opts := &redis.ClusterOptions{
		Dialer:       trackingDialer(tlsCfg, cfg.dialTimeout(), tracker),
		Addrs:        addrs,
		TLSConfig:    tlsCfg,
		DialTimeout:  cfg.dialTimeout(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		// See standaloneOptions: the caller's deadline must bound socket I/O.
		ContextTimeoutEnabled: true,
	}
	if provider != nil {
		opts.StreamingCredentialsProvider = provider
	} else {
		opts.Username, opts.Password = cfg.Username, cfg.Password
	}
	return opts
}

// buildClient constructs one client for the given address set, from the
// credentials New resolved once for every client of the store.
func (cfg Config) buildClient(addrs []string, tlsCfg *tls.Config, creds clientCredentials, tracker *endpointTracker) (redis.UniversalClient, error) {
	switch cfg.EffectiveTopology() {
	case TopologySentinel:
		client := redis.NewFailoverClient(cfg.failoverOptions(addrs, tlsCfg, creds.source, creds.sentinelPassword, tracker))
		// The mark half of the tracker pair (trackingDialerMarkedOnly is the
		// other): this hook chain wraps only the returned client's own data-pool
		// dials, never the internal sentinel clients' — see markDataDialsHook.
		client.AddHook(markDataDialsHook{})
		return client, nil
	case TopologyCluster:
		return redis.NewClusterClient(cfg.clusterOptions(addrs, tlsCfg, creds.provider, tracker)), nil
	default:
		return redis.NewClient(cfg.standaloneOptions(addrs, tlsCfg, creds.provider, tracker)), nil
	}
}
