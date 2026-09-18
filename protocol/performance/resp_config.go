package performance

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/viper"
)

const (
	// RespCacheViperKey is the config-file block holding the full RESP backend
	// surface (see redisstore.Config for the keys).
	RespCacheViperKey = "resp-cache"

	// Flags for the common path; the full surface lives in the YAML block. An
	// explicitly passed flag outranks the YAML value, same as every other
	// cache flag. Environment variables are intentionally not bound.
	RespCacheAddressesFlagName = "resp-cache-addresses"
	RespCacheTopologyFlagName  = "resp-cache-topology"
	// RespCacheKeyPrefixFlagName is the flag form of resp-cache.key-prefix. It
	// exists because the keyspace is the one setting that MUST differ between
	// deployments sharing a backend, and a deployment that can only pass flags
	// (a chart rendering a global flag list) had no way to set it — so every
	// such router landed on the shared default (MAG-3687).
	RespCacheKeyPrefixFlagName = "resp-cache-key-prefix"
)

// LoadRespCacheConfig reads the resp-cache configuration: the `resp-cache:`
// YAML block overlaid by the flat flags. enabled reports whether a RESP
// backend is configured at all (addresses present). A block or flag that sets
// options WITHOUT addresses is a deployment mistake, not a configuration —
// fail-fast, mirroring the repo's cache-config validation style.
func LoadRespCacheConfig(v *viper.Viper) (cfg redisstore.Config, enabled bool, err error) {
	blockPresent := v.IsSet(RespCacheViperKey)
	if blockPresent {
		// Strict: a key the block does not define is an error, not a line
		// silently ignored. What lives here decides which keyspace the router
		// occupies and how it authenticates, so a misspelled key-prefix that
		// fell back to the shared default without a word put the router in
		// another deployment's keyspace (MAG-3677). Nested blocks (tls) are
		// held to the same rule.
		strict := func(dc *mapstructure.DecoderConfig) { dc.ErrorUnused = true }
		if unmarshalErr := v.UnmarshalKey(RespCacheViperKey, &cfg, strict); unmarshalErr != nil {
			return cfg, false, fmt.Errorf("invalid %s block (unknown keys are rejected so a misspelled setting cannot fall back to a default unnoticed): %w", RespCacheViperKey, unmarshalErr)
		}
	}

	// Flag overlay. BindPFlags places flag values at flat keys, which viper's
	// nested UnmarshalKey does not merge — overlay explicitly, flags first.
	flagged := false
	if addresses := strings.TrimSpace(v.GetString(RespCacheAddressesFlagName)); addresses != "" {
		cfg.Addresses = nil
		for _, address := range strings.Split(addresses, ",") {
			if address = strings.TrimSpace(address); address != "" {
				cfg.Addresses = append(cfg.Addresses, address)
			}
		}
		flagged = true
	}
	if topology := strings.TrimSpace(v.GetString(RespCacheTopologyFlagName)); topology != "" {
		cfg.Topology = redisstore.Topology(topology)
		flagged = true
	}
	if keyPrefix := strings.TrimSpace(v.GetString(RespCacheKeyPrefixFlagName)); keyPrefix != "" {
		cfg.KeyPrefix = keyPrefix
		flagged = true
	}

	enabled = len(cfg.Addresses) > 0
	if !enabled {
		if blockPresent || flagged {
			return cfg, false, fmt.Errorf("%s options are set without addresses — dangling configuration (set %s.addresses or --%s, or remove the block)",
				RespCacheViperKey, RespCacheViperKey, RespCacheAddressesFlagName)
		}
		return cfg, false, nil
	}
	if err := cfg.Validate(); err != nil {
		return cfg, false, err
	}
	return cfg, true, nil
}

// SelectCacheBackend picks the router's primary cache backend from
// configuration:
//
//   - resp-cache configured → the RESP backend, INCLUDING when cache-be is
//     also set: coexistence is deliberate, the preserved cache-be is the
//     rollback path (delete the resp-cache block and the gRPC cache takes
//     over on the next start).
//   - only cache-be → the gRPC cache client, byte-for-byte today's behavior.
//   - neither → an inert typed-nil *Cache inside the interface (never a nil
//     interface): call sites probe CacheActive() and rely on the
//     nil-receiver-safe methods.
//
// Configuration errors abort startup; the gRPC path's connection failures do
// not (it reconnects in the background), and the RESP path does not dial at
// construction — its failures degrade per-operation.
func SelectCacheBackend(ctx context.Context, v *viper.Viper) (CacheBackend, error) {
	respConfig, respEnabled, err := LoadRespCacheConfig(v)
	if err != nil {
		return nil, err
	}
	cacheAddr := v.GetString(CacheFlagName)
	// The gRPC client's keyspace. Validated HERE and not in the constructor,
	// because the constructor's error means "the first dial failed, the client
	// reconnects in the background" and the call below deliberately carries on
	// through it — a configuration error returned the same way would start the
	// router without a cache and log it as a connection problem.
	cacheKeyPrefix := strings.TrimSpace(v.GetString(CacheKeyPrefixFlagName))
	if err := core.ValidateKeyPrefix(cacheKeyPrefix); err != nil {
		return nil, fmt.Errorf("%s: %w", CacheKeyPrefixFlagName, err)
	}
	// A gRPC prefix that scopes nothing is said out loud, naming the prefix: the
	// two shapes are a prefix with no cache-be to apply it to, and a prefix
	// beside a resp-cache block, which outranks cache-be entirely so the gRPC
	// client is never built — the keyspace in force is the RESP block's.
	if cacheKeyPrefix != "" {
		switch {
		case respEnabled:
			utils.LavaFormatWarning(CacheKeyPrefixFlagName+" is set but the RESP backend is configured and takes precedence, so the gRPC prefix scopes nothing; the keyspace in force is resp-cache.key-prefix", nil,
				utils.LogAttr("key-prefix", cacheKeyPrefix),
				utils.LogAttr("resp-cache-key-prefix", respConfig.KeyPrefix))
		case cacheAddr == "":
			utils.LavaFormatWarning(CacheKeyPrefixFlagName+" is set while "+CacheFlagName+" is empty — dangling configuration, it scopes nothing (set "+CacheFlagName+" or drop the prefix; the RESP backend's keyspace is resp-cache.key-prefix)", nil,
				utils.LogAttr("key-prefix", cacheKeyPrefix))
		}
	}

	if respEnabled {
		if cacheAddr != "" {
			utils.LavaFormatWarning("both resp-cache and cache-be are configured — the RESP backend takes precedence; cache-be is preserved as the rollback path (remove the resp-cache configuration to revert)", nil,
				utils.LogAttr("cache-be", cacheAddr))
		}
		store, storeErr := redisstore.New(respConfig)
		if storeErr != nil {
			return nil, storeErr
		}
		// The RESOLVED topology, never the raw field: an omitted topology printed
		// as a blank here, which is how a sentinel configuration missing its
		// topology line ran as standalone without a trace (MAG-3671).
		utils.LavaFormatInfo("resp-cache backend configured",
			utils.LogAttr("topology", respConfig.EffectiveTopology()),
			utils.LogAttr("master-name", respConfig.MasterName),
			utils.LogAttr("addresses", respConfig.Addresses),
			utils.LogAttr("read-addresses", respConfig.ReadAddresses),
			utils.LogAttr("key-prefix", respConfig.KeyPrefix),
			utils.LogAttr("tls", respConfig.TLS.Enabled),
		)
		// The TTL table is the operator's when the block carries an expiration
		// section and the engine's defaults otherwise — the same table the cache
		// sidecar builds from its flags, reachable here for the first time
		// (MAG-3631). GET /debug/cache-state reports what is in force.
		return NewRespCache(store, respConfig.Expiration.Policy()), nil
	}

	var cache CacheBackend = (*Cache)(nil)
	if cacheAddr != "" {
		grpcCache, initErr := InitCacheWithKeyPrefix(ctx, cacheAddr, cacheKeyPrefix)
		if initErr != nil {
			utils.LavaFormatError("Failed To Connect to cache at address", initErr, utils.Attribute{Key: "address", Value: cacheAddr})
		} else {
			utils.LavaFormatInfo("cache service connected", utils.Attribute{Key: "address", Value: cacheAddr}, utils.LogAttr("key-prefix", cacheKeyPrefix))
		}
		cache = grpcCache
	}
	return cache, nil
}
