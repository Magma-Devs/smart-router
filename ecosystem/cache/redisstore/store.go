// Package redisstore adapts a RESP-compatible backend (Redis/Valkey) onto
// core.KVStore, so the cache Engine — the same one the gRPC cache server
// runs over ristretto — executes against a remote, persistent, shareable
// store. The adapter owns representation and atomicity only; every cache
// semantic (lookup precedence, validity rules, TTL selection) stays in core.
package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultKeyPrefix = "sr"

	// chainTipRetention is how long the chain-tip KEY persists. Reader
	// freshness is the embedded deadline (core.DefaultExpirationForNonFinalized,
	// much shorter); the key outliving it keeps the monotonic write guard
	// fencing lower writes long after the tip goes stale for readers —
	// mirroring the in-memory adapter, where the stored block fences forever.
	// Bounded (instead of no TTL) so every key stays volatile and
	// maxmemory-policy volatile-* strategies see the whole keyspace.
	chainTipRetention = 24 * time.Hour

	scanBatchSize = 512

	// envelopeVersion tags the stored JSON so future fields (or a binary
	// codec) can be introduced additively.
	envelopeVersion = 1
)

// keyPrefixPattern restricts prefixes to characters with no glob meaning:
// Purge feeds the prefix into SCAN MATCH, whose patterns are globs, so an
// unsafe prefix could silently match and delete unrelated keys on a shared
// backend.
var keyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Store implements core.KVStore over a RESP backend. Reads and writes may be
// routed to distinct clients (D8: replica/reader endpoints in multi-region
// deployments); with no read addresses both fields hold the same client.
type Store struct {
	write  redis.UniversalClient
	read   redis.UniversalClient
	prefix string

	// stopWatcher terminates the credential poll loop (nil when the
	// credentials are static).
	stopWatcher chan struct{}
}

var _ core.KVStore = (*Store)(nil)

// New validates the config and builds the client(s): topology-appropriate
// construction, TLS, streaming credentials (with a poll-driven rotation
// watcher for file-backed passwords), and the optional read/write split.
func New(cfg Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tlsCfg, err := cfg.TLS.build()
	if err != nil {
		return nil, err
	}
	provider := NewStreamingProvider(cfg.credentialsSource())
	// Fail fast on an unreadable credential source (e.g. missing file) before
	// any client exists.
	if _, _, err := cfg.credentialsSource().Credentials(); err != nil {
		return nil, fmt.Errorf("resp-cache: reading credentials: %w", err)
	}

	writeClient, err := cfg.buildClient(cfg.Addresses, tlsCfg, provider)
	if err != nil {
		return nil, err
	}
	readClient := writeClient
	if len(cfg.ReadAddresses) > 0 {
		readClient, err = cfg.buildClient(cfg.ReadAddresses, tlsCfg, provider)
		if err != nil {
			_ = writeClient.Close()
			return nil, err
		}
	}

	store, err := newStore(writeClient, readClient, cfg.KeyPrefix)
	if err != nil {
		_ = writeClient.Close()
		if readClient != writeClient {
			_ = readClient.Close()
		}
		return nil, err
	}
	if cfg.PasswordFile != "" {
		store.stopWatcher = make(chan struct{})
		go watchCredentials(provider, cfg.refreshInterval(), store.stopWatcher)
	}
	return store, nil
}

// NewWithClient wraps an externally constructed client for both reads and
// writes (any topology; tests inject miniredis-backed clients here).
func NewWithClient(client redis.UniversalClient, keyPrefix string) (*Store, error) {
	return newStore(client, client, keyPrefix)
}

// NewWithClients wraps externally constructed write and read clients.
func NewWithClients(writeClient, readClient redis.UniversalClient, keyPrefix string) (*Store, error) {
	return newStore(writeClient, readClient, keyPrefix)
}

func newStore(writeClient, readClient redis.UniversalClient, keyPrefix string) (*Store, error) {
	if keyPrefix == "" {
		keyPrefix = DefaultKeyPrefix
	}
	if !keyPrefixPattern.MatchString(keyPrefix) {
		return nil, fmt.Errorf("invalid resp-cache key prefix %q: must match %s — SCAN MATCH patterns are globs, so glob characters could purge unrelated keys", keyPrefix, keyPrefixPattern.String())
	}
	return &Store{write: writeClient, read: readClient, prefix: keyPrefix}, nil
}

func (s *Store) key(k string) string {
	return s.prefix + ":" + k
}

// Ping probes backend connectivity — both endpoints when reads are split.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.write.Ping(ctx).Err(); err != nil {
		return err
	}
	if s.read != s.write {
		return s.read.Ping(ctx).Err()
	}
	return nil
}

// Close stops the credential watcher and releases both clients.
func (s *Store) Close() error {
	if s.stopWatcher != nil {
		close(s.stopWatcher)
		s.stopWatcher = nil
	}
	err := s.write.Close()
	if s.read != s.write {
		if readErr := s.read.Close(); err == nil {
			err = readErr
		}
	}
	return err
}

// ---------------------------------------------------------------------------
// Relay envelopes
// ---------------------------------------------------------------------------

// storedEnvelope is the versioned JSON wire form of core.Envelope. JSON keeps
// it consistent with the repo's cache wire codec and additive-fields
// compatibility rules; the version field leaves room for a binary codec.
type storedEnvelope struct {
	V                int                   `json:"v"`
	Response         relaytypes.RelayReply `json:"response"`
	Hash             []byte                `json:"hash,omitempty"`
	OptionalMetadata []relaytypes.Metadata `json:"optional_metadata,omitempty"`
	SeenBlock        int64                 `json:"seen_block"`
	IsCompressed     bool                  `json:"is_compressed"`
}

func encodeEnvelope(env *core.Envelope) ([]byte, error) {
	return json.Marshal(storedEnvelope{
		V:                envelopeVersion,
		Response:         env.Response,
		Hash:             env.Hash,
		OptionalMetadata: env.OptionalMetadata,
		SeenBlock:        env.SeenBlock,
		IsCompressed:     env.IsCompressed,
	})
}

func decodeEnvelope(data []byte) (*core.Envelope, error) {
	var stored storedEnvelope
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}
	return &core.Envelope{
		Response:         stored.Response,
		Hash:             stored.Hash,
		OptionalMetadata: stored.OptionalMetadata,
		SeenBlock:        stored.SeenBlock,
		IsCompressed:     stored.IsCompressed,
	}, nil
}

// GetEntries fetches all keys in one pipeline execution (in Cluster mode one
// round trip per involved shard, issued concurrently). A corrupt entry reads
// as a miss — on a shared backend foreign writers can never crash lookups.
func (s *Store) GetEntries(ctx context.Context, keys []string) ([]*core.Envelope, error) {
	out := make([]*core.Envelope, len(keys))
	pipe := s.read.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(ctx, s.key(k))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		env, decErr := decodeEnvelope(data)
		if decErr != nil {
			utils.LavaFormatError("corrupt cache entry in RESP backend, treating as miss", decErr, utils.LogAttr("key", s.key(keys[i])))
			continue
		}
		out[i] = env
	}
	return out, nil
}

func (s *Store) SetEntry(ctx context.Context, key string, env *core.Envelope, ttl time.Duration) error {
	data, err := encodeEnvelope(env)
	if err != nil {
		return err
	}
	// ttl <= 0 means "never expires" in the ristretto adapter; mirror it with
	// a plain SET rather than erroring on a non-positive PX.
	if ttl < 0 {
		ttl = 0
	}
	return s.write.Set(ctx, s.key(key), data, ttl).Err()
}

// ---------------------------------------------------------------------------
// Monotonic int64 (shared-state tip)
// ---------------------------------------------------------------------------

// setInt64GEScript implements greater-OR-EQUAL compare-and-set on a single
// key: equal observations rewrite, refreshing the TTL — a stalled-but-alive
// chain must not lose its tip. Single-key scripts are Cluster-safe.
var setInt64GEScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur and tonumber(ARGV[1]) < tonumber(cur) then
	return 0
end
if tonumber(ARGV[2]) > 0 then
	redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
else
	redis.call('SET', KEYS[1], ARGV[1])
end
return 1
`)

func (s *Store) GetInt64(ctx context.Context, key string) (int64, bool, error) {
	raw, err := s.read.Get(ctx, s.key(key)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	value, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil {
		// The in-memory adapter fatals on a corrupt shared-tip entry; a
		// router-embedded backend on a SHARED store must not be crashable by a
		// foreign writer, so corruption reads as a miss instead.
		utils.LavaFormatError("corrupt int64 entry in RESP backend, treating as miss", parseErr, utils.LogAttr("key", s.key(key)), utils.LogAttr("value", raw))
		return 0, false, nil
	}
	return value, true, nil
}

func (s *Store) SetInt64IfGreaterOrEqual(ctx context.Context, key string, value int64, ttl time.Duration) error {
	if ttl < 0 {
		ttl = 0
	}
	return setInt64GEScript.Run(ctx, s.write, []string{s.key(key)}, value, ttl.Milliseconds()).Err()
}

// ---------------------------------------------------------------------------
// Chain tip
// ---------------------------------------------------------------------------

// The chain tip stores "block:freshnessDeadlineUnixMs". Readers honour the
// embedded deadline; the monotonic guard compares against the raw stored
// block for as long as the key is retained (chainTipRetention), stale or not.
var setChainTipGEScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur then
	local curb = tonumber(string.match(cur, '^(-?%d+)'))
	if curb and tonumber(ARGV[1]) < curb then
		return 0
	end
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
`)

func encodeChainTip(block int64, deadlineUnixMs int64) string {
	return strconv.FormatInt(block, 10) + ":" + strconv.FormatInt(deadlineUnixMs, 10)
}

func decodeChainTip(raw string) (block int64, deadlineUnixMs int64, ok bool) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	block, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	deadlineUnixMs, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return block, deadlineUnixMs, true
}

func (s *Store) GetChainTip(ctx context.Context, key string) (int64, bool, error) {
	raw, err := s.read.Get(ctx, s.key(key)).Result()
	if err == redis.Nil {
		return spectypes.NOT_APPLICABLE, false, nil
	}
	if err != nil {
		return spectypes.NOT_APPLICABLE, false, err
	}
	block, deadlineUnixMs, ok := decodeChainTip(raw)
	if !ok {
		utils.LavaFormatError("corrupt chain-tip entry in RESP backend, treating as unknown", nil, utils.LogAttr("key", s.key(key)), utils.LogAttr("value", raw))
		return spectypes.NOT_APPLICABLE, false, nil
	}
	if time.Now().UnixMilli() < deadlineUnixMs {
		return block, true, nil
	}
	return spectypes.NOT_APPLICABLE, false, nil
}

func (s *Store) SetChainTipIfGreaterOrEqual(ctx context.Context, key string, block int64) error {
	deadline := time.Now().Add(core.DefaultExpirationForNonFinalized).UnixMilli()
	encoded := encodeChainTip(block, deadline)
	return setChainTipGEScript.Run(ctx, s.write, []string{s.key(key)}, block, encoded, chainTipRetention.Milliseconds()).Err()
}

// ---------------------------------------------------------------------------
// Block-hash → height
// ---------------------------------------------------------------------------

func (s *Store) GetHeight(ctx context.Context, key string) (int64, bool, error) {
	raw, err := s.read.Get(ctx, s.key(key)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	value, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil {
		return 0, false, nil
	}
	return value, true, nil
}

func (s *Store) SetHeight(ctx context.Context, key string, height int64, ttl time.Duration) error {
	if ttl < 0 {
		ttl = 0
	}
	return s.write.Set(ctx, s.key(key), height, ttl).Err()
}

// ---------------------------------------------------------------------------
// Purge
// ---------------------------------------------------------------------------

// Purge drops every key under this store's prefix — never FLUSHDB, the
// backend may be shared. UNLINK is issued as pipelined SINGLE-KEY commands: a
// multi-key UNLINK over a scan page would fail with CROSSSLOT in Cluster,
// where multi-key operations require all keys in one hash slot. In Cluster
// mode each master is scanned individually.
func (s *Store) Purge(ctx context.Context) error {
	match := s.prefix + ":*"
	if clusterClient, ok := s.write.(*redis.ClusterClient); ok {
		return clusterClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return purgeByScan(ctx, master, match)
		})
	}
	return purgeByScan(ctx, s.write, match)
}

func purgeByScan(ctx context.Context, client redis.UniversalClient, match string) error {
	iter := client.Scan(ctx, 0, match, scanBatchSize).Iterator()
	pipe := client.Pipeline()
	queued := 0
	for iter.Next(ctx) {
		pipe.Unlink(ctx, iter.Val())
		queued++
		if queued%scanBatchSize == 0 {
			if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
				return err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	return nil
}
