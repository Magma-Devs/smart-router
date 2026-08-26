package redisstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

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

	DialTimeout  time.Duration `mapstructure:"dial-timeout"`
	ReadTimeout  time.Duration `mapstructure:"read-timeout"`
	WriteTimeout time.Duration `mapstructure:"write-timeout"`
	PoolSize     int           `mapstructure:"pool-size"`
}

// TLSConfig is the file-based TLS surface (config-file friendly).
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
	switch cfg.topology() {
	case TopologyStandalone, TopologySentinel, TopologyCluster:
	default:
		return fmt.Errorf("resp-cache: unknown topology %q (want standalone, sentinel, or cluster)", cfg.Topology)
	}
	if len(cfg.Addresses) == 0 {
		return fmt.Errorf("resp-cache: no addresses configured")
	}
	if cfg.topology() == TopologySentinel && cfg.MasterName == "" {
		return fmt.Errorf("resp-cache: sentinel topology requires master-name")
	}
	if cfg.topology() != TopologySentinel && (cfg.SentinelUsername != "" || cfg.SentinelPassword != "" || cfg.SentinelPasswordFile != "") {
		return fmt.Errorf("resp-cache: sentinel-* credentials are set but topology is %q — dangling configuration", cfg.topology())
	}
	if cfg.topology() == TopologyCluster && cfg.DB != 0 {
		return fmt.Errorf("resp-cache: db selection is not available in cluster topology")
	}
	if cfg.Password != "" && cfg.PasswordFile != "" {
		return fmt.Errorf("resp-cache: password and password-file are mutually exclusive")
	}
	if cfg.SentinelPassword != "" && cfg.SentinelPasswordFile != "" {
		return fmt.Errorf("resp-cache: sentinel-password and sentinel-password-file are mutually exclusive")
	}
	return nil
}

func (cfg Config) topology() Topology {
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
func (cfg Config) sentinelPassword() (string, error) {
	if cfg.SentinelPasswordFile == "" {
		return cfg.SentinelPassword, nil
	}
	pw, err := os.ReadFile(cfg.SentinelPasswordFile)
	if err != nil {
		return "", fmt.Errorf("resp-cache: reading sentinel-password-file: %w", err)
	}
	return trimCredential(string(pw)), nil
}

// ---------------------------------------------------------------------------
// Config → go-redis options mapping (pure, so tests assert it directly)
// ---------------------------------------------------------------------------

func (cfg Config) standaloneOptions(addrs []string, tlsCfg *tls.Config, provider *StreamingProvider) *redis.Options {
	return &redis.Options{
		Addr:                         addrs[0],
		DB:                           cfg.DB,
		StreamingCredentialsProvider: provider,
		TLSConfig:                    tlsCfg,
		DialTimeout:                  cfg.DialTimeout,
		ReadTimeout:                  cfg.ReadTimeout,
		WriteTimeout:                 cfg.WriteTimeout,
		PoolSize:                     cfg.PoolSize,
		// The caller's context deadline must bound socket I/O: the router
		// gives cache lookups a tight per-relay budget, and without this
		// go-redis uses only Read/WriteTimeout (seconds) for socket deadlines,
		// letting a slow backend inject latency far past that budget.
		ContextTimeoutEnabled: true,
	}
}

// failoverOptions carries data-node credentials through
// CredentialsProviderContext (fresh resolution on every connection attempt)
// rather than the streaming provider: go-redis v9.22's NewFailoverClient
// accepts StreamingCredentialsProvider in its options but never initializes
// the streaming re-auth manager, so the first operation nil-panics. With the
// context provider, rotated credentials apply on reconnects and failovers —
// in-place re-auth of idle connections needs the upstream gap fixed first.
func (cfg Config) failoverOptions(addrs []string, tlsCfg *tls.Config, source CredentialsSource, sentinelPassword string) *redis.FailoverOptions {
	return &redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    addrs,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: sentinelPassword,
		DB:               cfg.DB,
		CredentialsProviderContext: func(ctx context.Context) (string, string, error) {
			return source.Credentials()
		},
		TLSConfig:    tlsCfg,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		// See standaloneOptions: the caller's deadline must bound socket I/O.
		ContextTimeoutEnabled: true,
	}
}

func (cfg Config) clusterOptions(addrs []string, tlsCfg *tls.Config, provider *StreamingProvider) *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:                        addrs,
		StreamingCredentialsProvider: provider,
		TLSConfig:                    tlsCfg,
		DialTimeout:                  cfg.DialTimeout,
		ReadTimeout:                  cfg.ReadTimeout,
		WriteTimeout:                 cfg.WriteTimeout,
		PoolSize:                     cfg.PoolSize,
		// See standaloneOptions: the caller's deadline must bound socket I/O.
		ContextTimeoutEnabled: true,
	}
}

// buildClient constructs one client for the given address set.
func (cfg Config) buildClient(addrs []string, tlsCfg *tls.Config, provider *StreamingProvider) (redis.UniversalClient, error) {
	switch cfg.topology() {
	case TopologySentinel:
		sentinelPassword, err := cfg.sentinelPassword()
		if err != nil {
			return nil, err
		}
		return redis.NewFailoverClient(cfg.failoverOptions(addrs, tlsCfg, cfg.credentialsSource(), sentinelPassword)), nil
	case TopologyCluster:
		return redis.NewClusterClient(cfg.clusterOptions(addrs, tlsCfg, provider)), nil
	default:
		return redis.NewClient(cfg.standaloneOptions(addrs, tlsCfg, provider)), nil
	}
}
