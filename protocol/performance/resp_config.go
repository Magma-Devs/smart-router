package performance

import (
	"context"
	"fmt"
	"strings"

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
)

// LoadRespCacheConfig reads the resp-cache configuration: the `resp-cache:`
// YAML block overlaid by the flat flags. enabled reports whether a RESP
// backend is configured at all (addresses present). A block or flag that sets
// options WITHOUT addresses is a deployment mistake, not a configuration —
// fail-fast, mirroring the repo's cache-config validation style.
func LoadRespCacheConfig(v *viper.Viper) (cfg redisstore.Config, enabled bool, err error) {
	blockPresent := v.IsSet(RespCacheViperKey)
	if blockPresent {
		if unmarshalErr := v.UnmarshalKey(RespCacheViperKey, &cfg); unmarshalErr != nil {
			return cfg, false, fmt.Errorf("invalid %s block: %w", RespCacheViperKey, unmarshalErr)
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

	if respEnabled {
		if cacheAddr != "" {
			utils.LavaFormatWarning("both resp-cache and cache-be are configured — the RESP backend takes precedence; cache-be is preserved as the rollback path (remove the resp-cache configuration to revert)", nil,
				utils.LogAttr("cache-be", cacheAddr))
		}
		store, storeErr := redisstore.New(respConfig)
		if storeErr != nil {
			return nil, storeErr
		}
		utils.LavaFormatInfo("resp-cache backend configured",
			utils.LogAttr("topology", respConfig.Topology),
			utils.LogAttr("addresses", respConfig.Addresses),
			utils.LogAttr("read-addresses", respConfig.ReadAddresses),
			utils.LogAttr("key-prefix", respConfig.KeyPrefix),
			utils.LogAttr("tls", respConfig.TLS.Enabled),
		)
		// TTL policy mirrors the cache server's defaults; router-side TTL
		// tuning is a deliberate non-goal for now.
		return NewRespCache(store, core.DefaultPolicy()), nil
	}

	var cache CacheBackend = (*Cache)(nil)
	if cacheAddr != "" {
		grpcCache, initErr := InitCache(ctx, cacheAddr)
		if initErr != nil {
			utils.LavaFormatError("Failed To Connect to cache at address", initErr, utils.Attribute{Key: "address", Value: cacheAddr})
		} else {
			utils.LavaFormatInfo("cache service connected", utils.Attribute{Key: "address", Value: cacheAddr})
		}
		cache = grpcCache
	}
	return cache, nil
}
