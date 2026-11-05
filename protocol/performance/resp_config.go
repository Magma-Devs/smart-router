package performance

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	// SecondaryRespCacheViperKey is the read-only second tier's RESP block
	// (MAG-4778): the same surface as resp-cache, minus what a tier that only
	// reads cannot use (read-addresses, expiration). Its flags mirror the
	// resp-cache ones for the same reason those exist.
	SecondaryRespCacheViperKey          = "secondary-resp-cache"
	SecondaryRespCacheAddressesFlagName = "secondary-resp-cache-addresses"
	SecondaryRespCacheTopologyFlagName  = "secondary-resp-cache-topology"
	SecondaryRespCacheKeyPrefixFlagName = "secondary-resp-cache-key-prefix"
)

// respBlockKeys names one RESP configuration block and its three flags.
type respBlockKeys struct {
	block, addressesFlag, topologyFlag, keyPrefixFlag string
	// name is the store's log name (redisstore.Config.Name), set before Validate
	// so the block's own errors name it.
	name string
	// refuse, when set, rejects keys the block's tier cannot use. It runs before
	// Validate, so a refused key is reported as refused rather than through a
	// validation rule written for the block that does use it.
	refuse func(redisstore.Config) error
}

var (
	primaryRespBlock   = respBlockKeys{RespCacheViperKey, RespCacheAddressesFlagName, RespCacheTopologyFlagName, RespCacheKeyPrefixFlagName, "", nil}
	secondaryRespBlock = respBlockKeys{SecondaryRespCacheViperKey, SecondaryRespCacheAddressesFlagName, SecondaryRespCacheTopologyFlagName, SecondaryRespCacheKeyPrefixFlagName, secondaryRespCacheLogName, refuseReadOnlyTierKeys}
)

// LoadRespCacheConfig reads the resp-cache configuration: the `resp-cache:`
// YAML block overlaid by the flat flags. enabled reports whether a RESP
// backend is configured at all (addresses present). A block or flag that sets
// options WITHOUT addresses is a deployment mistake, not a configuration —
// fail-fast, mirroring the repo's cache-config validation style.
func LoadRespCacheConfig(v *viper.Viper) (cfg redisstore.Config, enabled bool, err error) {
	return loadRespBlock(v, primaryRespBlock)
}

// LoadSecondaryRespCacheConfig reads the secondary-resp-cache block and flags, the
// read-only second tier on a RESP backend (MAG-4778), under the same rules as
// LoadRespCacheConfig. Two keys that block would accept are refused here because a
// tier that only reads cannot use them: read-addresses (there are no writes to
// split reads from; give the reader endpoint as addresses) and expiration (the
// zone that writes the entries owns their lifetimes).
func LoadSecondaryRespCacheConfig(v *viper.Viper) (cfg redisstore.Config, enabled bool, err error) {
	return loadRespBlock(v, secondaryRespBlock)
}

// refuseReadOnlyTierKeys rejects the two keys LoadSecondaryRespCacheConfig names.
// Checked before Validate: a refused read-addresses list used to be reported by
// the standalone rule, which advised switching topology, and a negative
// expiration by an error naming resp-cache.
func refuseReadOnlyTierKeys(cfg redisstore.Config) error {
	if len(cfg.ReadAddresses) > 0 {
		return fmt.Errorf("%s.read-addresses is not used by a read-only tier: every operation is a read, so set the reader endpoint as %s.addresses", SecondaryRespCacheViperKey, SecondaryRespCacheViperKey)
	}
	if cfg.Expiration != (redisstore.ExpirationConfig{}) {
		return fmt.Errorf("%s.expiration is not used by a read-only tier: the zone that writes the entries sets their lifetimes", SecondaryRespCacheViperKey)
	}
	return nil
}

func loadRespBlock(v *viper.Viper, keys respBlockKeys) (cfg redisstore.Config, enabled bool, err error) {
	blockPresent := v.IsSet(keys.block)
	if blockPresent {
		// Strict: a key the block does not define is an error, not a line
		// silently ignored. What lives here decides which keyspace the router
		// occupies and how it authenticates, so a misspelled key-prefix that
		// fell back to the shared default without a word put the router in
		// another deployment's keyspace (MAG-3677). Nested blocks (tls) are
		// held to the same rule.
		strict := func(dc *mapstructure.DecoderConfig) { dc.ErrorUnused = true }
		if unmarshalErr := v.UnmarshalKey(keys.block, &cfg, strict); unmarshalErr != nil {
			return cfg, false, fmt.Errorf("invalid %s block (unknown keys are rejected so a misspelled setting cannot fall back to a default unnoticed): %w", keys.block, unmarshalErr)
		}
	}

	// Flag overlay. BindPFlags places flag values at flat keys, which viper's
	// nested UnmarshalKey does not merge — overlay explicitly, flags first.
	flagged := false
	if addresses := strings.TrimSpace(v.GetString(keys.addressesFlag)); addresses != "" {
		cfg.Addresses = nil
		for _, address := range strings.Split(addresses, ",") {
			if address = strings.TrimSpace(address); address != "" {
				cfg.Addresses = append(cfg.Addresses, address)
			}
		}
		flagged = true
	}
	if topology := strings.TrimSpace(v.GetString(keys.topologyFlag)); topology != "" {
		cfg.Topology = redisstore.Topology(topology)
		flagged = true
	}
	if keyPrefix := strings.TrimSpace(v.GetString(keys.keyPrefixFlag)); keyPrefix != "" {
		cfg.KeyPrefix = keyPrefix
		flagged = true
	}

	enabled = len(cfg.Addresses) > 0
	if !enabled {
		if blockPresent || flagged {
			return cfg, false, fmt.Errorf("%s options are set without addresses — dangling configuration (set %s.addresses or --%s, or remove the block)",
				keys.block, keys.block, keys.addressesFlag)
		}
		return cfg, false, nil
	}
	cfg.Name = keys.name
	if keys.refuse != nil {
		if err := keys.refuse(cfg); err != nil {
			return cfg, false, err
		}
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
			utils.LogAttr("tls-insecure-skip-verify", respConfig.TLS.InsecureSkipVerify),
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

// OpenSecondaryRespCache builds the read-only RESP second tier from a configuration
// LoadSecondaryRespCacheConfig accepted. Like the primary, it does not dial here:
// an unreachable backend degrades each lookup to a miss and is reported by the
// health probe, while a configuration the store refuses (an unreadable credential
// file) fails startup. lookupBudget is secondary-cache-timeout (see
// NewSecondaryRespCache).
func OpenSecondaryRespCache(cfg redisstore.Config, lookupBudget time.Duration) (*RespCache, error) {
	store, err := redisstore.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewSecondaryRespCache(store, lookupBudget), nil
}

// RespKeyPrefixInForce is the keyspace a RESP store occupies: its configured
// prefix, or the store's default when none is set.
func RespKeyPrefixInForce(configured string) string {
	if configured == "" {
		return redisstore.DefaultKeyPrefix
	}
	return configured
}
