package cache

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/spf13/pflag"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	grpc "google.golang.org/grpc"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sliceutil"
)

const (
	ExpirationFlagName                           = "expiration"
	ExpirationTimeFinalizedMultiplierFlagName    = "expiration-multiplier"
	ExpirationNonFinalizedFlagName               = "expiration-non-finalized"
	ExpirationTimeNonFinalizedMultiplierFlagName = "expiration-non-finalized-multiplier"
	ExpirationBlocksHashesToHeightsFlagName      = "expiration-blocks-hashes-to-heights"
	ExpirationNodeErrorsOnFinalizedFlagName      = "expiration-finalized-node-errors"
	FlagCacheSizeName                            = "max-items"
	DefaultExpirationForNonFinalized             = 500 * time.Millisecond
	// SharedStateTipBlockMultiplier bounds how long a pod's published chain tip lives in the
	// shared cache: SharedStateTipBlockMultiplier * averageBlockTime. It mirrors the smart
	// router's own tip-staleness horizon (chainstate.StalenessWindow) so a dead pod's tip
	// evaporates on roughly the same timescale the router considers a tip stale, rather than
	// lingering for the full finalized (~1h) TTL.
	SharedStateTipBlockMultiplier = 10
	// MinSharedStateTipExpiration is the absolute floor for that TTL, mirroring the router-side
	// floor (chainstate.minStalenessWindow) rather than importing it — ecosystem/cache is a
	// standalone binary and must not depend on protocol/. It is a hard-coded constant, NOT an
	// operator-tunable duration, because it underwrites two guarantees the tunables cannot:
	//   - non-zero: ristretto treats a zero TTL as "never expires", so a dead pod's tip would
	//     linger forever. ExpirationNonFinalized alone cannot prevent this — it is
	//     flag * multiplier, both operator-settable, and either being 0 makes it 0.
	//   - long enough to be useful: a chain tip must outlive the gap between requests. At the
	//     500ms ExpirationNonFinalized default a tip published for a spec with no
	//     average_block_time would evaporate between relays, silently disabling shared state.
	MinSharedStateTipExpiration                 = 2 * time.Second
	DefaultExpirationTimeFinalizedMultiplier    = 1.0
	DefaultExpirationTimeNonFinalizedMultiplier = 1.0
	DefaultExpirationBlocksHashesToHeights      = 48 * time.Hour
	DefaultExpirationTimeFinalized              = time.Hour
	DefaultExpirationNodeErrors                 = 250 * time.Millisecond
	CacheNumCounters                            = 100000000 // expect 10M items
	unixPrefix                                  = "unix:"
)

type CacheServer struct {
	finalizedCache             *ristretto.Cache[string, any]
	tempCache                  *ristretto.Cache[string, any] // cache for temporary inputs, such as latest blocks
	blocksHashesToHeightsCache *ristretto.Cache[string, any]

	ExpirationFinalized             time.Duration
	ExpirationNonFinalized          time.Duration
	ExpirationNodeErrors            time.Duration
	ExpirationBlocksHashesToHeights time.Duration

	CacheMetrics *CacheMetrics
	CacheMaxCost int64
}

func (cs *CacheServer) InitCache(
	ctx context.Context,
	expiration time.Duration,
	expirationNonFinalized time.Duration,
	expirationNodeErrorsOnFinalized time.Duration,
	expirationBlocksHashesToHeights time.Duration,
	metricsAddr string,
	expirationFinalizedMultiplier float64,
	expirationNonFinalizedMultiplier float64,
) {
	cs.ExpirationFinalized = time.Duration(float64(expiration) * expirationFinalizedMultiplier)
	cs.ExpirationNonFinalized = time.Duration(float64(expirationNonFinalized) * expirationNonFinalizedMultiplier)
	cs.ExpirationNodeErrors = expirationNodeErrorsOnFinalized
	cs.ExpirationBlocksHashesToHeights = time.Duration(float64(expirationBlocksHashesToHeights))

	var err error
	// initialize prometheus first so we can use it in OnEvict callbacks
	cs.CacheMetrics = NewCacheMetricsServer(metricsAddr)

	onEvictHandler := func(item *ristretto.Item[any]) {
		cs.CacheMetrics.AddExpired()
	}

	cs.tempCache, err = ristretto.NewCache(&ristretto.Config[string, any]{
		NumCounters: CacheNumCounters,
		MaxCost:     cs.CacheMaxCost,
		BufferItems: 64,
		Metrics:     true,
		OnEvict:     onEvictHandler,
	})
	if err != nil {
		utils.FormatFatal("could not create cache", err)
	}

	cs.finalizedCache, err = ristretto.NewCache(&ristretto.Config[string, any]{
		NumCounters: CacheNumCounters,
		MaxCost:     cs.CacheMaxCost,
		BufferItems: 64,
		Metrics:     true,
		OnEvict:     onEvictHandler,
	})
	if err != nil {
		utils.FormatFatal("could not create finalized cache", err)
	}

	cs.blocksHashesToHeightsCache, err = ristretto.NewCache(&ristretto.Config[string, any]{
		NumCounters: CacheNumCounters,
		MaxCost:     cs.CacheMaxCost,
		BufferItems: 64,
		Metrics:     true,
		OnEvict:     onEvictHandler,
	})
	if err != nil {
		utils.FormatFatal("could not create blocks hashes to heights cache", err)
	}

	go cs.periodicCacheSizeUpdate(ctx)
}

func (cs *CacheServer) Serve(ctx context.Context, listenAddr string) {
	ctx, cancel := context.WithCancel(ctx)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	var lis net.Listener
	var err error
	if strings.HasPrefix(listenAddr, unixPrefix) {
		host, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			utils.FormatFatal("Failed to parse unix socket, provide address in this format unix:/tmp/example.sock", err)
			return
		}

		syscall.Unlink(port)

		addr, err := net.ResolveUnixAddr(host, port)
		if err != nil {
			utils.FormatFatal("Failed to resolve unix socket address", err)
			return
		}

		lis, err = net.ListenUnix(host, addr)
		if err != nil {
			utils.FormatFatal("Failed to listen to unix socket listener", err)
			return
		}

		err = os.Chmod(port, 0o600)
		if err != nil {
			utils.FormatFatal("Failed to set permissions for Unix socket", err)
			return
		}
	} else {
		lis, err = net.Listen("tcp", listenAddr)
		if err != nil {
			utils.FormatFatal("Cache server failure setting up TCP listener", err)
			return
		}
	}

	serverReceiveMaxMessageSize := grpc.MaxRecvMsgSize(chainproxy.MaxCallRecvMsgSize)
	s := grpc.NewServer(serverReceiveMaxMessageSize)

	wrappedServer := grpcweb.WrapServer(s)
	handler := func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.Header().Set("Access-Control-Allow-Headers", "Content-Type,x-grpc-web")
		wrappedServer.ServeHTTP(resp, req)
	}

	httpServer := http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(handler), &http2.Server{}),
	}

	go func() {
		select {
		case <-ctx.Done():
			_ = utils.FormatInfo("Cache Server ctx.Done")
		case <-signalChan:
			_ = utils.FormatInfo("Cache Server signalChan")
		}

		shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownRelease()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			utils.FormatFatal("Cache failed to shutdown", err)
		}
	}()

	cacheServer := &RelayerCacheServer{CacheServer: cs}
	relaytypes.RegisterRelayerCacheServer(s, cacheServer)

	_ = utils.FormatInfo("Cache Server listening", utils.Attribute{Key: "Address", Value: lis.Addr().String()})
	if err := httpServer.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
		utils.FormatFatal("cache failed to serve", err, utils.Attribute{Key: "Address", Value: lis.Addr().String()})
	}
}

func (cs *CacheServer) ExpirationForChain(averageBlockTimeForChain time.Duration) time.Duration {
	eighthBlock := averageBlockTimeForChain / 8
	return sliceutil.Max([]time.Duration{eighthBlock, cs.ExpirationNonFinalized})
}

// SharedStateTipExpiration returns the TTL for a pod's published chain tip in shared-state
// mode: SharedStateTipBlockMultiplier * averageBlockTime, floored at ExpirationNonFinalized and
// then at the hard MinSharedStateTipExpiration.
//
// The second floor is the load-bearing one. ExpirationNonFinalized is derived from two operator
// flags (duration * multiplier), so it is neither guaranteed non-zero — a zero TTL means "never
// expires" in ristretto, so a dead pod's tip would linger forever — nor guaranteed long enough:
// at its 500ms default a tip for a spec that omits average_block_time (0 * multiplier = 0) would
// evaporate between relays and silently disable shared state. MinSharedStateTipExpiration is a
// constant precisely so no flag combination can defeat either guarantee.
func (cs *CacheServer) SharedStateTipExpiration(averageBlockTimeForChain time.Duration) time.Duration {
	ttl := averageBlockTimeForChain * SharedStateTipBlockMultiplier
	return sliceutil.Max([]time.Duration{ttl, cs.ExpirationNonFinalized, MinSharedStateTipExpiration})
}

func (cs *CacheServer) GetTotalCacheSize() int64 {
	var total int64
	if cs.finalizedCache != nil {
		if m := cs.finalizedCache.Metrics; m != nil {
			total += int64(m.KeysAdded() - m.KeysEvicted())
		}
	}
	if cs.tempCache != nil {
		if m := cs.tempCache.Metrics; m != nil {
			total += int64(m.KeysAdded() - m.KeysEvicted())
		}
	}
	if cs.blocksHashesToHeightsCache != nil {
		if m := cs.blocksHashesToHeightsCache.Metrics; m != nil {
			total += int64(m.KeysAdded() - m.KeysEvicted())
		}
	}
	return total
}

func (cs *CacheServer) periodicCacheSizeUpdate(ctx context.Context) {
	if cs.finalizedCache == nil || cs.tempCache == nil || cs.blocksHashesToHeightsCache == nil {
		return
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size := cs.GetTotalCacheSize()
			cs.CacheMetrics.SetCacheSize(size)
		}
	}
}

func Server(
	ctx context.Context,
	listenAddr string,
	metricsAddr string,
	flags *pflag.FlagSet,
) {
	expiration, err := flags.GetDuration(ExpirationFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationFlagName})
	}

	expirationNonFinalized, err := flags.GetDuration(ExpirationNonFinalizedFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationNonFinalizedFlagName})
	}

	expirationNodeErrors, err := flags.GetDuration(ExpirationNodeErrorsOnFinalizedFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationNodeErrorsOnFinalizedFlagName})
	}

	expirationBlocksHashesToHeights, err := flags.GetDuration(ExpirationBlocksHashesToHeightsFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationBlocksHashesToHeightsFlagName})
	}

	expirationFinalizedMultiplier, err := flags.GetFloat64(ExpirationTimeFinalizedMultiplierFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationTimeFinalizedMultiplierFlagName})
	}

	expirationNonFinalizedMultiplier, err := flags.GetFloat64(ExpirationTimeNonFinalizedMultiplierFlagName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: ExpirationTimeNonFinalizedMultiplierFlagName})
	}

	cacheMaxCost, err := flags.GetInt64(FlagCacheSizeName)
	if err != nil {
		utils.FormatFatal("failed to read flag", err, utils.Attribute{Key: "flag", Value: FlagCacheSizeName})
	}

	cs := CacheServer{CacheMaxCost: cacheMaxCost}
	cs.InitCache(ctx, expiration, expirationNonFinalized, expirationNodeErrors, expirationBlocksHashesToHeights, metricsAddr, expirationFinalizedMultiplier, expirationNonFinalizedMultiplier)
	cs.Serve(ctx, listenAddr)
}
