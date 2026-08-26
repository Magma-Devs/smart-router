// Package rpcsmartrouter provides the RPC routing solution for the Lava protocol.
//
// # Architecture Overview
//
// The smart router routes RPC requests to statically configured provider endpoints.
//
//   - Uses pre-configured static providers from configuration files
//   - Provider selection based on configured weights (static providers get 10x multiplier)
//   - Direct RPC connections to provider nodes
//
// # Provider Selection
//
// Static providers are configured in YAML files and automatically receive a 10x weight
// multiplier. This ensures static providers are preferred in routing decisions.
// See StaticProviderDummyCoin for implementation details.
package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/statetracker"
	"github.com/magma-Devs/smart-router/protocol/tracing"
	epochstoragetypes "github.com/magma-Devs/smart-router/types/epoch"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	scoreutils "github.com/magma-Devs/smart-router/utils/score"
	"github.com/magma-Devs/smart-router/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultRPCSmartRouterFileName = "rpcsmartrouter.yml"
	DebugRelaysFlagName           = "debug-relays"
	DebugProbesFlagName           = "debug-probes"

	// lavaAppName is the application name, previously app.Name.
	lavaAppName = "lava"
	// lavaDefaultNodeHome is the default home directory, previously lavaDefaultNodeHome (~/.lava).
	lavaDefaultNodeHome = "$HOME/." + lavaAppName
)

var (
	Yaml_config_properties         = []string{"network-address", "chain-id", "api-interface"}
	RelaysHealthEnableFlagDefault  = true
	RelayHealthIntervalFlagDefault = 5 * time.Minute

	// StaticProviderDummyStake is used for stake-based provider selection weighting.
	// For static providers that do NOT specify an explicit stake, we keep this at 0 so CalcWeightsByStake
	// can apply the legacy "static provider boost" behavior (see lavasession package).
	StaticProviderDummyStake = int64(0)
)

// staticPolicy is a simple implementation of chainlib.PolicyInf
// used to configure the chain parser with allowed extensions and addons
// derived from static provider configurations.
type staticPolicy struct {
	addons       []string
	extensions   []string
	apiInterface string
}

func (p staticPolicy) GetSupportedAddons(string) ([]string, error) {
	return p.addons, nil
}

func (p staticPolicy) GetSupportedExtensions(string) ([]epochstoragetypes.EndpointService, error) {
	services := make([]epochstoragetypes.EndpointService, 0, len(p.extensions))
	for _, ext := range p.extensions {
		services = append(services, epochstoragetypes.EndpointService{
			Extension:    ext,
			ApiInterface: p.apiInterface,
		})
	}
	return services, nil
}

type strategyValue struct {
	provideroptimizer.Strategy
}

var strategyNames = []string{
	"balanced",
	"latency",
	"sync-freshness",
	"cost",
	"privacy",
	"accuracy",
	"distributed",
}

var strategyFlag strategyValue = strategyValue{Strategy: provideroptimizer.StrategyBalanced}

func (s *strategyValue) String() string {
	return strategyNames[int(s.Strategy)]
}

func (s *strategyValue) Set(str string) error {
	for i, name := range strategyNames {
		if strings.EqualFold(str, name) {
			s.Strategy = provideroptimizer.Strategy(i)
			return nil
		}
	}
	return fmt.Errorf("invalid strategy: %s", str)
}

func (s *strategyValue) Type() string {
	return "string"
}

// newUsageSinkFromOptions builds the UsageEventSink for the run. Returns a
// NoopUsageSink (inlinable no-op on every Emit) when usage-otel-enabled is
// false or the OTel sink fails to construct, so the relay/QoS hot paths never
// pay for telemetry that isn't configured.
func newUsageSinkFromOptions(options *rpcSmartRouterStartOptions) metrics.UsageEventSink {
	a := options.analyticsServerAddresses
	if !a.UsageOTelEnabled {
		return metrics.NoopUsageSink{}
	}
	if otelSink := metrics.NewOTelUsageSink(metrics.OTelUsageSinkConfig{
		Endpoint:          a.UsageOTelEndpoint,
		Insecure:          a.UsageOTelInsecure,
		QueueSize:         a.UsageOTelQueueSize,
		BatchSize:         a.UsageOTelBatchSize,
		FlushInterval:     a.UsageOTelFlushInterval,
		ExportTimeout:     a.UsageOTelExportTimeout,
		ServiceName:       a.UsageOTelServiceName,
		ServiceInstanceID: a.UsageOTelInstanceID,
	}); otelSink != nil {
		return otelSink
	}
	utils.LavaFormatWarning("usage-otel-enabled but OTel sink failed to construct; falling back to no-op sink", nil)
	return metrics.NoopUsageSink{}
}

type AnalyticsServerAddresses struct {
	MetricsListenAddress string
	// Usage telemetry (OTel). Off by default via UsageOTelEnabled=false.
	UsageOTelEnabled       bool
	UsageOTelEndpoint      string
	UsageOTelInsecure      bool
	UsageOTelQueueSize     int
	UsageOTelBatchSize     int
	UsageOTelFlushInterval time.Duration
	UsageOTelExportTimeout time.Duration
	UsageOTelServiceName   string
	UsageOTelInstanceID    string
}
type RPCSmartRouter struct {
	// Smart router doesn't need blockchain state tracking
	epochTimer             *common.EpochTimer
	mu                     sync.Mutex                                                      // protects the maps below during parallel endpoint setup and retry
	sessionManagers        map[string]*lavasession.ConsumerSessionManager                  // key: chainID-apiInterface
	providerSessions       map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider // key: chainID-apiInterface
	backupProviderSessions map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider // key: chainID-apiInterface

	// failedStaticProviders holds providers that failed verification at startup,
	// keyed by sessionManagerKey (chainID-apiInterface). The retry loop reads this
	// to periodically re-validate and re-register recovered providers.
	failedStaticProviders map[string][]*lavasession.RPCStaticProviderEndpoint

	// failedBackupProviders is the backup-tier counterpart. Backups used to recover
	// only on the 15m epoch reverification, five times slower than the static tier's
	// retry loop. That gap did not matter while a chain could never boot on backups
	// alone; since MAG-2525 it can, so a failed backup may be the only thing standing
	// between the chain and serving traffic.
	failedBackupProviders map[string][]*lavasession.RPCStaticProviderEndpoint

	// Server references for per-endpoint ChainTracker cleanup on epoch updates
	rpcServers map[string]*RPCSmartRouterServer // key: chainID-apiInterface

	// smartRouterMetricsManager is held here, not just passed down, because the
	// serving-tier gauge has to be republished from every path that mutates the
	// session maps — including the epoch tick and the retry loop, which run long
	// after CreateSmartRouterEndpoint has returned. Nil when metrics are disabled.
	smartRouterMetricsManager *metrics.SmartRouterMetricsManager

	// reverifyInputs holds the per-chain inputs applyReverification needs
	// (chain parser, configured static/backup lists, the convertProvidersToSessions
	// closure built in CreateSmartRouterEndpoint). Populated under rpsr.mu in
	// CreateSmartRouterEndpoint (parallel goroutines per endpoint); after Start's
	// wg.Wait the map is read-only, so updateEpoch reads it without locking. Absent
	// for tests that build RPCSmartRouter directly — updateEpoch then runs the
	// original freshen-from-old loops without a reverification post-pass.
	reverifyInputs map[string]*chainReverifyInputs // key: chainID-apiInterface

	// usageSink is the OTel usage emitter (or NoopUsageSink when telemetry is
	// off). It must outlive Start() — Start returns once endpoints are wired,
	// but the router keeps serving until ctx is cancelled — so Close() is
	// deferred to Stop()'s drain phase, not to Start(). Closing it in Start
	// would shut the BatchProcessor down while relays are still emitting.
	usageSink metrics.UsageEventSink
}

type rpcSmartRouterStartOptions struct {
	rpcEndpoints             []*lavasession.RPCEndpoint
	cache                    *performance.Cache
	strategy                 provideroptimizer.Strategy
	analyticsServerAddresses AnalyticsServerAddresses
	cmdFlags                 common.ConsumerCmdFlags
	stateShare               bool
	staticProvidersList      []*lavasession.RPCStaticProviderEndpoint // define static providers as primary providers
	backupProvidersList      []*lavasession.RPCStaticProviderEndpoint // define backup providers as emergency fallback when no providers available
	upstreamSelectorConfig   provideroptimizer.UpstreamSelectorConfig
}

// Start sets up the RPCSmartRouter and all its processes, then returns once
// every endpoint is ready for traffic. Internal goroutines (chain listeners,
// the debug HTTP server, the WS subscription managers, etc.) are bound to the
// passed-in ctx — they run until the caller cancels it. The caller is expected
// to wait on <-ctx.Done() and then call Stop(gracePeriod) to drain in-flight
// requests gracefully before the process exits.
func (rpsr *RPCSmartRouter) Start(ctx context.Context, options *rpcSmartRouterStartOptions) (err error) {
	if common.IsTestMode(ctx) {
		testModeWarn("RPCSmartRouter running tests")
	}

	// Initialize session managers and provider sessions maps for epoch timer callbacks
	rpsr.sessionManagers = make(map[string]*lavasession.ConsumerSessionManager)
	rpsr.providerSessions = make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider)
	rpsr.backupProviderSessions = make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider)
	rpsr.failedStaticProviders = make(map[string][]*lavasession.RPCStaticProviderEndpoint)
	rpsr.failedBackupProviders = make(map[string][]*lavasession.RPCStaticProviderEndpoint)
	rpsr.rpcServers = make(map[string]*RPCSmartRouterServer)
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)

	// RPCSmartRouter always runs in standalone mode with time-based epochs
	epochDuration := options.cmdFlags.EpochDuration
	if epochDuration == 0 {
		epochDuration = common.StandaloneEpochDuration // 15 minutes default for standalone
	}

	rpsr.epochTimer = common.NewEpochTimer(epochDuration)
	currentEpoch := rpsr.epochTimer.GetCurrentEpoch()
	timeUntilNext := rpsr.epochTimer.GetTimeUntilNextEpoch()

	utils.LavaFormatInfo("RPCSmartRouter: using time-based epochs (standalone mode)",
		utils.LogAttr("epochDuration", epochDuration),
		utils.LogAttr("currentEpoch", currentEpoch),
		utils.LogAttr("timeUntilNextEpoch", timeUntilNext),
		utils.LogAttr("nextEpochTime", time.Now().Add(timeUntilNext).Format("15:04:05 MST")),
	)

	metrics.InitErrorMetrics()

	// Smart router doesn't need consumer address from blockchain
	// Using a static identifier for metrics and logging
	smartRouterIdentifier := "smart-router-" + strconv.FormatUint(rand.Uint64(), 10)

	// usageSink is the single fan-out for both relay_usage and optimizer_qos
	// events. With --usage-otel-enabled=false (default) it's a NoopUsageSink:
	// every Emit / EmitOptimizerQoS is an inlinable no-op, so the relay and
	// QoS paths pay nothing. Flip the flag on to ship OTLP/HTTP logs to a
	// local collector. It's stored on the struct (not deferred here) because
	// Start returns once endpoints are wired while the router keeps serving;
	// Stop() closes it so the BatchProcessor drains on real shutdown.
	usageSink := newUsageSinkFromOptions(options)
	rpsr.usageSink = usageSink

	// Always collect optimizer QoS reports so the rpc_optimizer_selection_score
	// metric is exposed on /metrics regardless of OTel. Each sampling tick also
	// fires the reports at usageSink (Noop when OTel is off).
	smartRouterOptimizerQoSClient := metrics.NewConsumerOptimizerQoSClient(smartRouterIdentifier, usageSink)
	smartRouterOptimizerQoSClient.StartOptimizersQoSReportsCollecting(ctx, metrics.OptimizerQosServerSamplingInterval)
	// SmartRouterMetricsManager is the single metrics owner for the smart router.
	// It serves its own HTTP endpoint and implements ConsumerMetricsManagerInf so it
	// can be passed to RPCConsumerLogs, ConsumerSessionManager, etc. — the interface
	// decouples those consumers from the concrete metrics sink.
	smartRouterMetricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{
		NetworkAddress:     options.analyticsServerAddresses.MetricsListenAddress,
		OptimizerQoSClient: smartRouterOptimizerQoSClient,
	})

	rpsr.smartRouterMetricsManager = smartRouterMetricsManager

	rpcSmartRouterMetrics, err := metrics.NewRPCConsumerLogs(smartRouterMetricsManager, usageSink, smartRouterOptimizerQoSClient)
	if err != nil {
		utils.LavaFormatFatal("failed creating RPCSmartRouter logs", err)
	}

	smartRouterMetricsManager.SetVersion(version.Version)
	smartRouterMetricsManager.StartSelectionStatsUpdater(ctx, metrics.OptimizerQosServerSamplingInterval)

	// we want one provider optimizer per chain so we will store them for reuse across rpcEndpoints
	chainMutexes := map[string]*sync.Mutex{}
	for _, endpoint := range options.rpcEndpoints {
		chainMutexes[endpoint.ChainID] = &sync.Mutex{} // create a mutex per chain for shared resources
	}

	optimizers := &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{}

	var wg sync.WaitGroup
	parallelJobs := len(options.rpcEndpoints)
	wg.Add(parallelJobs)

	errCh := make(chan error, parallelJobs)

	utils.LavaFormatInfo("RPCSmartRouter identifier: " + smartRouterIdentifier)
	utils.LavaFormatInfo("RPCSmartRouter setting up endpoints", utils.Attribute{Key: "length", Value: strconv.Itoa(parallelJobs)})

	relaysMonitorAggregator := metrics.NewRelaysMonitorAggregator(options.cmdFlags.RelaysHealthIntervalFlag, smartRouterMetricsManager)
	for _, rpcEndpoint := range options.rpcEndpoints {
		go func(rpcEndpoint *lavasession.RPCEndpoint) error {
			defer wg.Done()
			err := rpsr.CreateSmartRouterEndpoint(ctx, rpcEndpoint, errCh,
				optimizers, chainMutexes,
				options, smartRouterIdentifier, rpcSmartRouterMetrics, smartRouterOptimizerQoSClient,
				smartRouterMetricsManager, relaysMonitorAggregator)
			return err
		}(rpcEndpoint)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}

	// Start epoch timer after all endpoints are set up
	// Register ONE global epoch callback that updates ALL session managers
	// This prevents multiple UpdateAllProviders calls with the same epoch to the same session manager.
	// Capture ctx in the closure rather than stashing it on rpsr (storing context.Context in a struct is
	// idiomatically discouraged); the EpochTimer's callback signature is fixed at func(uint64).
	//
	// Skip the very first synchronous callback. EpochTimer.Start fires
	// notifyCallbacks(currentEpoch) inline before returning (see
	// protocol/common/epoch_timer.go), so without this guard updateEpoch — and
	// the applyReverification it drives — runs while chain listeners, direct
	// RPC connection pools, and chain trackers are all still completing their
	// initial dials against the same upstreams. The contention amplifies the
	// race window in addClientsAsynchronouslyGrpc; defense-in-depth alongside
	// the connector fix in chainproxy/connector.go. All subsequent ticks come
	// from the timer goroutine after the system is steady-state and run
	// normally.
	var firstTick atomic.Bool
	rpsr.epochTimer.RegisterCallback(func(epoch uint64) {
		if firstTick.CompareAndSwap(false, true) {
			return
		}
		rpsr.updateEpoch(ctx, epoch)
	})

	// Log that epoch timer is configured for all session managers
	utils.LavaFormatInfo("RPCSmartRouter: Registered epoch timer callback for all session managers",
		utils.LogAttr("sessionManagerCount", len(rpsr.sessionManagers)),
	)

	// Start the epoch timer
	rpsr.epochTimer.Start(ctx)

	relaysMonitorAggregator.StartMonitoring(ctx)

	// Start optional debug HTTP server for integration tests.
	// Only starts when --debug-address flag is provided. Off by default.
	if options.cmdFlags.DebugAddress != "" {
		// Debug-mode-only: enable the in-memory ring-buffer log sink so the
		// /debug/logs endpoint can serve recent logs to the test harness.
		utils.EnableDebugLogBuffer(50000)
		// Debug-mode-only, same reasoning: start recording cross-validation dissent so
		// /debug/cross-validation-events can serve it (MAG-2772). Until this call the recorder is a
		// nil check on the relay path, so a production router stores nothing.
		enableCrossValidationEventRing(crossValidationEventRingCapacity)
		var currentOffsetNano atomic.Int64
		debugMux := buildDebugMux(debugMuxDeps{
			optimizers: optimizers,
			offsetNano: &currentOffsetNano,
			router:     rpsr,
			cache:      options.cache,
			qosClient:  smartRouterOptimizerQoSClient,
		})
		srv := &http.Server{Addr: options.cmdFlags.DebugAddress, Handler: debugMux}
		// Watcher goroutine: shuts the server down gracefully when ctx is cancelled
		// (i.e. when the caller cancels — typically on SIGINT/SIGTERM via NotifyContext).
		go func() {
			<-ctx.Done()
			srv.Shutdown(context.Background()) //nolint:errcheck
		}()
		go func() {
			utils.LavaFormatInfo("Debug HTTP server started", utils.LogAttr("address", options.cmdFlags.DebugAddress))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				utils.LavaFormatError("Debug HTTP server stopped", err)
			}
		}()
	}

	utils.LavaFormatInfo("RPCSmartRouter done setting up all endpoints, ready for requests")

	return nil
}

func (rpsr *RPCSmartRouter) Stop(shutdownGracePeriod time.Duration) {
	utils.LavaFormatInfo("RPCSmartRouter: shutdown signal received, draining",
		utils.LogAttr("gracePeriod", shutdownGracePeriod),
	)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancelShutdown()

	// Phase 1: drain client-facing layer in parallel.
	// WS goroutines have already started reacting to the cancelled Serve ctx
	// (sending 1001 close frames via ListenToMessages); Shutdown waits for them
	// to drain (via wsWG) and then drains in-flight HTTP via app.ShutdownWithContext.
	var drainWG sync.WaitGroup
	for key, server := range rpsr.rpcServers {
		drainWG.Add(1)
		go func(k string, s *RPCSmartRouterServer) {
			defer drainWG.Done()
			if s.chainListener == nil {
				return
			}
			if err := s.chainListener.Shutdown(shutdownCtx); err != nil {
				utils.LavaFormatWarning("listener shutdown returned error", err, utils.LogAttr("endpoint", k))
			}
		}(key, server)
	}
	drainWG.Wait()

	// Phase 2: close upstream connections (provider WS pools, gRPC streaming pools).
	// This must run AFTER Phase 1 so in-flight relays don't lose their pools mid-call.
	for _, server := range rpsr.rpcServers {
		if server.wsSubscriptionManager != nil {
			if dwsm, ok := server.wsSubscriptionManager.(*DirectWSSubscriptionManager); ok {
				dwsm.Close()
			}
		}
		if server.grpcSubscriptionManager != nil {
			server.grpcSubscriptionManager.Stop()
		}
	}

	// Phase 3: drain the usage sink last — after every relay has finished, so
	// the BatchProcessor flushes its pending OTLP records (bounded by the
	// export timeout) instead of dropping them. NoopUsageSink.Close is a no-op.
	if rpsr.usageSink != nil {
		rpsr.usageSink.Close()
	}

	utils.LavaFormatInfo("RPCSmartRouter: graceful shutdown complete")
}

// debugMuxDeps bundles the state the debug HTTP handlers reach into. Bundling
// rather than positional args lets us add stores (router-wide retry caches,
// session managers, etc.) without breaking the existing test fixtures, which
// can leave router=nil and exercise just the optimizer+offset surface.
type debugMuxDeps struct {
	optimizers *common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]
	offsetNano *atomic.Int64
	// router is optional. When provided, /debug/reset-all also flushes
	// per-server RelayRetriesManagers and per-CSM transient failure state.
	router *RPCSmartRouter
	// qosClient is the optional optimizer-QoS sampler. When provided,
	// GET /debug/provider-scores reads the live per-provider quality scores
	// through it (MAG-2707). Without it that endpoint reports the scores as
	// unavailable rather than answering with an empty list.
	qosClient *metrics.ConsumerOptimizerQoSClient
	// cache is the optional external cache-be client. When non-nil and
	// CacheActive(), /debug/reset-all also flushes the cache-be pod via
	// RelayerCache.FlushCache — without it the in-process Ristretto reset is a
	// lie on any deployment running with --cache-be (MAG-1764). Reached
	// through cacheFlusher rather than the concrete *performance.Cache so
	// tests can inject a fake without standing up a gRPC client.
	cache cacheFlusher
}

// debugPollNowTimeout caps how long POST /debug/poll-now waits per request (MAG-2649). A single
// poll is already bounded by the tracker's own fetch timeout (max(10s, MinimumTimePerRelayDelay)),
// but a trigger arriving mid-cycle queues behind the in-flight poll first — so the ceiling is
// roughly two fetch timeouts plus slack. Reaching it returns 504 rather than hanging the caller;
// the poll itself, once started, still completes and records — the response simply cannot report
// what it recorded, which is why that case answers Polled=false rather than claiming the poll.
const debugPollNowTimeout = 25 * time.Second

// cacheFlusher is the minimal surface /debug/reset-all needs from the cache-be
// client. *performance.Cache satisfies it; tests substitute a fake that
// records the Flush call.
type cacheFlusher interface {
	CacheActive() bool
	Flush(ctx context.Context) error
}

// resetAllChainTrackers walks the router's per-server EndpointMonitors
// and zeroes every cached latest-block, so consistency pre-validation skips the
// lag check until the next poll repopulates the value. Returns the total number
// of trackers reset across all servers. Nil-safe at every level so test fixtures
// without a fully-wired router still get a useful no-op (count == 0).
func resetAllChainTrackers(deps debugMuxDeps) int {
	if deps.router == nil {
		return 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	count := 0
	for _, server := range deps.router.rpcServers {
		if server == nil || server.endpointChainTrackerManager == nil {
			continue
		}
		count += server.endpointChainTrackerManager.ResetAllLatestBlocks()
	}
	return count
}

// resetAllProbeBackoff walks every server's EndpointMonitor and clears the per-endpoint poll
// back-off so each provider returns to its base poll cadence (MAG-2395). Probe back-off (the
// fetchFails streak) is otherwise unreachable by any reset endpoint, so a provider that failed
// before a reset keeps its stretched schedule (up to BACKOFF_MAX_TIME) after it. Nil-safe at every
// level; returns the total number of trackers signalled across all servers.
func resetAllProbeBackoff(deps debugMuxDeps) int {
	if deps.router == nil {
		return 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	count := 0
	for _, server := range deps.router.rpcServers {
		if server == nil || server.endpointChainTrackerManager == nil {
			continue
		}
		count += server.endpointChainTrackerManager.ResetAllBackoff()
	}
	return count
}

// reregisterChainTrackerRows re-registers every currently-configured direct-RPC endpoint that lost
// its ChainTracker row — the recovery tool for MAG-2445, where the epoch cleanup (cleanupStaleTrackers)
// can delete the row of a provider that was only briefly unhealthy, after which no reset recreates it
// and the provider returns only on a later ~15-minute rebuild.
//
// Since MAG-2622 the server's own reconcile loop (initializeChainTrackers) re-registers a missing
// row within chainTrackerReconcileInterval from this SAME source, so this handler is no longer the
// only way back — its remaining value is being on-demand and auditable: an operator gets the row
// back immediately (e.g. right after /debug/reset-pairing re-admits a demoted provider) instead of
// waiting out a tick, and the action is explicit in the support record.
//
// Neither path shortens MAG-2445's EXCLUSION window, because both read the LIVE PAIRING: there the
// epoch re-verify demotes the endpoint OUT of the pairing (applyReverification sees a 503) before
// cleanupStaleTrackers drops its row, so until a later epoch promotes it back it is absent from
// GetAllDirectRPCEndpoints entirely. Shortening that means keeping the endpoint paired, not
// re-registering its tracker. What both paths DO fix is what happens after the promote: the row
// comes back instead of waiting on a relay to trigger lazy creation.
//
// The source is the config/pairing set
// (GetAllDirectRPCEndpoints, the same source initializeChainTrackers uses at startup), NOT the live
// health-filtered set cleanupStaleTrackers deletes from — so a temporarily-disabled provider is
// restored while a genuinely config-removed one is correctly left out. GetOrCreateTracker is
// idempotent (no-op for a live row) and non-blocking (it registers synchronously and starts the poll
// goroutine via startTrackerWithRetry), so the handler never blocks on a down endpoint's init fetch.
//
// Holding router.mu does NOT exclude the epoch cleanup: cleanupStaleTrackers deliberately runs
// outside that lock, so a cleanup pass that already built its keep-set can delete a row this handler
// just recreated. Acceptable for a recovery tool — the operator re-POSTs, and MAG-2445 is the root
// fix — but do not assume the lock provides that exclusion.
// Returns (ensured, created): endpoints processed, and rows that did not previously exist.
func reregisterChainTrackerRows(deps debugMuxDeps) (ensured, created int) {
	if deps.router == nil {
		return 0, 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	for chainKey, server := range deps.router.rpcServers {
		if server == nil || server.endpointChainTrackerManager == nil {
			continue
		}
		csm := deps.router.sessionManagers[chainKey]
		if csm == nil {
			continue
		}
		// Count creations against a snapshot of the pre-existing rows rather than a
		// GetEndpointCount before/after delta: a concurrent cleanupStaleTrackers removal between
		// the two reads would skew the delta (even negative).
		existing := make(map[string]bool)
		for _, url := range server.endpointChainTrackerManager.GetAllEndpoints() {
			existing[url] = true
		}
		for _, ep := range csm.GetAllDirectRPCEndpoints() {
			if ep == nil || ep.Endpoint == nil {
				continue
			}
			ensured++
			if _, err := server.endpointChainTrackerManager.GetOrCreateTracker(ep.Endpoint, ep.DirectConnection); err != nil {
				utils.LavaFormatWarning("reset-chaintracker-rows: failed to re-register endpoint", err,
					utils.LogAttr("endpoint", ep.Endpoint.NetworkAddress),
					utils.LogAttr("chainKey", chainKey),
				)
				continue
			}
			if !existing[ep.Endpoint.NetworkAddress] {
				created++
				existing[ep.Endpoint.NetworkAddress] = true
			}
		}
	}
	return ensured, created
}

// pollNowTarget is one endpoint /debug/poll-now will poll: the EndpointMonitor that owns its
// ChainTracker, plus the chain identity used to label the response row.
type pollNowTarget struct {
	chainID      string
	apiInterface string
	monitor      *endpointstate.EndpointMonitor
}

// resolvePollNowTargets finds every registered ChainTracker for networkAddress, optionally narrowed
// by chainID / apiInterface (both optional; empty means "any", matched case-insensitively). The
// same URL can legitimately be registered on more than one chain or interface — the endpointtip
// store keys on all three — so this returns a slice rather than a single hit and the handler polls
// each match.
//
// It resolves ONLY: the router lock is taken to walk the servers and released before any polling
// happens. A poll can block for up to the tracker's fetch timeout, and holding router.mu across it
// would stall epoch updates, relay bookkeeping and every other /debug endpoint for that long.
// Nil-safe at every level so test fixtures without a wired router simply match nothing.
func resolvePollNowTargets(deps debugMuxDeps, networkAddress, chainID, apiInterface string) []pollNowTarget {
	if deps.router == nil || networkAddress == "" {
		return nil
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	targets := []pollNowTarget{}
	for _, server := range deps.router.rpcServers {
		if server == nil || server.endpointChainTrackerManager == nil || server.listenEndpoint == nil {
			continue
		}
		if chainID != "" && !strings.EqualFold(chainID, server.listenEndpoint.ChainID) {
			continue
		}
		if apiInterface != "" && !strings.EqualFold(apiInterface, server.listenEndpoint.ApiInterface) {
			continue
		}
		// Only servers that actually track this URL — otherwise a multi-chain router would answer
		// with a row of "no tracker" noise per unrelated chain.
		if _, ok := server.endpointChainTrackerManager.GetTracker(networkAddress); !ok {
			continue
		}
		targets = append(targets, pollNowTarget{
			chainID:      server.listenEndpoint.ChainID,
			apiInterface: server.listenEndpoint.ApiInterface,
			monitor:      server.endpointChainTrackerManager,
		})
	}
	return targets
}

// providerScoresResponse is the body of GET /debug/provider-scores (MAG-2707).
//
// It is an ENVELOPE rather than the bare array the other /debug state endpoints return, because a
// partial answer has to be able to say what is missing. On a router serving several chains, one
// chain can have providers while another has none: the rows alone would then be a 200 that looks
// complete, and a caller reading the empty chain would conclude "this provider has no score" rather
// than "this chain produced nothing" — the same silent pass this endpoint exists to end, narrowed
// from the whole router to a single chain.
//
// chains_unavailable names every matched chain that produced no rows; it is empty (never null) when
// the answer is complete. Rows keep the bare Go-identifier keys the other debug rows use; the
// envelope keys are snake_case like the other non-row debug responses.
type providerScoresResponse struct {
	Rows              []map[string]any `json:"rows"`
	ChainsUnavailable []string         `json:"chains_unavailable"`
}

// providerEndpointURLs maps each provider address to the direct-RPC endpoint URLs configured for it,
// so a /debug/provider-scores row can be joined to /debug/endpoint-state (MAG-2707).
//
// The two views key on different identities and neither can be derived from the other: the optimizer
// — and therefore every score — is keyed by provider address (PublicLavaAddress, the same identity
// /debug/provider-routing reports), while per-endpoint health is keyed by NetworkAddress. Carrying
// the URLs on the score row is what lets the automation ask "this provider's score moved, was its
// endpoint healthy?" from two reads instead of needing a third mapping call.
//
// Nil-safe at every level; returns an empty map when no router or session managers are wired.
func providerEndpointURLs(deps debugMuxDeps) map[string][]string {
	urls := map[string][]string{}
	if deps.router == nil {
		return urls
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	for _, csm := range deps.router.sessionManagers {
		if csm == nil {
			continue
		}
		for _, ep := range csm.GetAllDirectRPCEndpoints() {
			if ep == nil || ep.Endpoint == nil || ep.ProviderAddress == "" {
				continue
			}
			urls[ep.ProviderAddress] = append(urls[ep.ProviderAddress], ep.Endpoint.NetworkAddress)
		}
	}
	for _, list := range urls {
		sort.Strings(list) // stable output; the map iteration above is unordered
	}
	return urls
}

// setAllChainStateDebugOffset shifts every per-server ChainState's effective clock by offset, aging
// its TTL/staleness/consensus windows without waiting real time; offset 0 clears the warp. It backs
// /debug/chain-state-time-warp — deliberately SEPARATE from /debug/time-warp (the optimizer/QoS
// clock) so a routine QoS warp-then-reset never disturbs ChainState (MAG-2307 review). Only freshness
// EVALUATION is warped; stored observation timestamps stay on the real clock (see
// ChainState.effectiveNow), so the warp ages state without leaving future-dated timestamps. Nil-safe
// at every level (test fixtures without a wired router are a no-op). Returns the number of
// ChainStates the offset was applied to.
func setAllChainStateDebugOffset(deps debugMuxDeps, offset time.Duration) int {
	if deps.router == nil {
		return 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	count := 0
	for _, server := range deps.router.rpcServers {
		if server == nil || server.chainState == nil {
			continue
		}
		server.chainState.SetDebugClockOffset(offset)
		count++
	}
	return count
}

// resetAllChainStates clears every server's ChainState tip and consensus baseline back to cold
// start. This is the operator recovery for a poisoned tip that cannot self-heal — a within-band
// lie kept fresh by the liar's own traffic, or a cold-start lie before any peer baseline forms.
// Recompute and stale-tip re-adoption bound most poisoning to ~one TTL, but neither can drop a
// within-band value while the liar keeps reporting it, so /debug/reset-* is the only immediate
// path down. Returns the number of ChainStates reset. Nil-safe at every level.
func resetAllChainStates(deps debugMuxDeps) int {
	if deps.router == nil {
		return 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	count := 0
	for _, server := range deps.router.rpcServers {
		if server == nil || server.chainState == nil {
			continue
		}
		server.chainState.Reset()
		count++
	}
	return count
}

// resetEndpointHealthAndGauge re-enables every endpoint across all session managers
// (ConsumerSessionManager.ResetEndpointHealth — clears ConnectionRefusals, sets
// Enabled=true) and mirrors the reset onto the per-endpoint Prometheus health gauge for
// every provider (primary + backup), matching what the epoch tick does in updateEpoch.
// Returns the number of endpoints actually re-enabled.
//
// Endpoints disabled after MaxConsecutiveConnectionAttempts consecutive failures (e.g. an
// all-providers-down stress burst) otherwise stay disabled until the next epoch tick, the
// only other ResetHealth caller — contaminating later tests. Shared by /debug/reset-all
// and /debug/reset-endpoint-health.
func resetEndpointHealthAndGauge(deps debugMuxDeps) int {
	if deps.router == nil {
		return 0
	}
	deps.router.mu.Lock()
	defer deps.router.mu.Unlock()
	total := 0
	for chainKey, csm := range deps.router.sessionManagers {
		if csm == nil {
			continue
		}
		total += csm.ResetEndpointHealth()

		// Mirror the struct reset onto the Prometheus health gauge so operators see
		// providers recover immediately rather than at the next epoch tick — without it
		// the gauge stays stuck at 0 until a successful relay, one a rarely-used backup
		// may never receive.
		server := deps.router.rpcServers[chainKey]
		if server == nil || server.smartRouterEndpointMetrics == nil || server.listenEndpoint == nil {
			continue
		}
		for _, sessions := range []map[uint64]*lavasession.ConsumerSessionsWithProvider{
			deps.router.providerSessions[chainKey],
			deps.router.backupProviderSessions[chainKey],
		} {
			for _, cswp := range sessions {
				if cswp != nil {
					server.smartRouterEndpointMetrics.SetEndpointOverallHealth(
						server.listenEndpoint.ChainID, server.listenEndpoint.ApiInterface, cswp.PublicLavaAddress, true)
				}
			}
		}
	}
	return total
}

// resolveSelectionWeights turns --qos-selection-priority plus the individual
// --qos-*-weight flags into the four QoS weights the endpoint selector scores on.
//
// Precedence: the priority preset sets the weights, then any weight the operator set by
// hand overrides it — so the preset is a starting point, never a cage.
//
// "Set by hand" deliberately checks the config file as well as the command line.
// Changed() only reports flags typed on the CLI, so a weight set in config.yml would
// otherwise lose silently to the preset; viper.InConfig covers that half.
//
// Only the four weights are decided here. Every other field is left at its default for
// the caller to fill in, so a priority can never move something it does not own.
//
// The weights are normalised to sum to 1.0 before returning. A preset sums to 1.0 by
// construction, so ANY partial override breaks the sum — --qos-selection-priority fastest
// with --qos-latency-weight 0.5 gives 0.80 — and NewUpstreamSelector would then rescale it
// while logging "weights do not sum to 1.0, normalizing" at WARN. That reads as the
// operator's mistake on a combination this flag explicitly advertises, and it left three
// disagreeing accounts of one config: this Info line reported the typed 0.5, the WARN said
// the config was wrong, and the selector ran 0.625.
//
// Normalising here does the identical arithmetic one step earlier, in the code that knows
// the override was deliberate, so the selector receives a set that already sums to 1.0 and
// stays quiet. Nothing about routing changes: scaling preserves ratios, so 0.5/0.8 and
// 0.625/1.0 are the same share. Reported by @avitenzer.
//
// Genuinely broken input is deliberately NOT handled here — negative, NaN, Inf, or an
// all-zero set falls through untouched so NewUpstreamSelector's existing validation still
// rejects it loudly and falls back to defaults. This only quiets the case we decided is
// legitimate: correct proportions that happen not to total 1.
func resolveSelectionWeights(flags *pflag.FlagSet) (provideroptimizer.UpstreamSelectorConfig, error) {
	config := provideroptimizer.DefaultUpstreamSelectorConfig()

	priority, err := provideroptimizer.ParseSelectionPriority(viper.GetString(common.ProviderOptimizerSelectionPriority))
	if err != nil {
		return config, err
	}
	config = priority.ApplyTo(config)

	overridden := []string{}
	for _, w := range []struct {
		flagName string
		target   *float64
	}{
		{common.ProviderOptimizerAvailabilityWeight, &config.AvailabilityWeight},
		{common.ProviderOptimizerLatencyWeight, &config.LatencyWeight},
		{common.ProviderOptimizerSyncWeight, &config.SyncWeight},
		{common.ProviderOptimizerStakeWeight, &config.StakeWeight},
	} {
		if (flags != nil && flags.Changed(w.flagName)) || viper.InConfig(w.flagName) {
			*w.target = viper.GetFloat64(w.flagName)
			overridden = append(overridden, w.flagName)
		}
	}

	rescaled := normalizeSelectionWeights(&config)

	// Stay quiet on the default path so an untouched deployment logs nothing new. The
	// weights logged are the EFFECTIVE ones — what the selector will actually score on,
	// after any rescale — because the typed number is not what runs and an operator who
	// reads this line should not have to work that out.
	if priority != provideroptimizer.SelectionPriorityBalanced || len(overridden) > 0 {
		utils.LavaFormatInfo("Working with provider selection priority: "+priority.String(),
			utils.LogAttr("availabilityWeight", config.AvailabilityWeight),
			utils.LogAttr("latencyWeight", config.LatencyWeight),
			utils.LogAttr("syncWeight", config.SyncWeight),
			utils.LogAttr("stakeWeight", config.StakeWeight),
			utils.LogAttr("manuallyOverridden", strings.Join(overridden, ",")),
			utils.LogAttr("rescaledToSumToOne", rescaled),
		)
	}

	return config, nil
}

// normalizeSelectionWeights scales the four QoS weights so they sum to 1.0, and reports
// whether it had to. Returns false when the set is already normalised or when it is invalid
// and must be left for NewUpstreamSelector to reject.
//
// The 0.001 tolerance mirrors NewUpstreamSelector's own check exactly. That is the point:
// matching it is what guarantees the selector's "weights do not sum to 1.0" warning cannot
// fire on a set that came through here.
func normalizeSelectionWeights(config *provideroptimizer.UpstreamSelectorConfig) bool {
	weights := []*float64{
		&config.AvailabilityWeight,
		&config.LatencyWeight,
		&config.SyncWeight,
		&config.StakeWeight,
	}

	total := 0.0
	for _, w := range weights {
		// A negative or non-finite weight is a config error, not a scaling problem. Leave
		// the whole set alone so the selector's validation still sees it as handed in.
		if math.IsNaN(*w) || math.IsInf(*w, 0) || *w < 0 {
			return false
		}
		total += *w
	}

	if total <= 0 || math.Abs(total-1.0) <= 0.001 {
		return false
	}

	for _, w := range weights {
		*w /= total
	}
	return true
}

// routerConfigOptimizerWeights is the JSON shape for a single provider-optimizer's
// active selection weights, used for each per-chain entry in the PerChainOptimizer map
// returned by GET /debug/runtime-config. Fields carry no json tags, so each key marshals
// as the bare Go identifier — tests grep the same string in test code and router source.
type routerConfigOptimizerWeights struct {
	AvailabilityWeight float64
	LatencyWeight      float64
	SyncWeight         float64
	StakeWeight        float64
	MinSelectionChance float64
	SelectionMode      string
}

// routerConfigResponse is the JSON body of GET /debug/runtime-config. It exposes the
// router's live tuning values so the test suite can read them at runtime instead of
// hardcoding copies that silently drift when the source changes (e.g. when
// MaxConsecutiveConnectionAttempts was raised from 5 to 50). Each field carries no json
// tag, so every key marshals as the exact Go identifier of its source symbol (no
// snake_case, no package prefix) — a test greps the same string in test code and router
// source. Durations are integer milliseconds.
type routerConfigResponse struct {
	SchemaVersion int

	// lavasession
	MaxConsecutiveConnectionAttempts                 int
	TimeoutForEstablishingAConnection                int64 // milliseconds
	MaximumNumberOfFailuresAllowedPerConsumerSession int

	// relaycore (flag-bound package vars — these report the live value)
	RelayRetryLimit          int
	DisableBatchRequestRetry bool

	// rpcsmartrouter retry/attempt ceilings
	MaximumNumberOfTickerRelayRetries int
	SendRelayAttempts                 int

	// SmartRouter state-machine config (read from SmartRouterStateMachineConfig())
	EnableCircuitBreaker    bool
	CircuitBreakerThreshold int
	EnableTimeoutPriority   bool

	// timeouts (integer milliseconds)
	TimePerCU                int64
	MinimumTimePerRelayDelay int64
	DefaultTimeout           int64
	CacheTimeout             int64

	// score config
	ProbeUpdateWeight         float64
	DefaultProbeUpdateWeight  float64
	MinAcceptableAvailability float64
	HighCuThreshold           uint64
	MidCuThreshold            uint64

	// chain-tracker polling. There is no fixed probe interval — the tracker polls
	// adaptively at averageBlockTime/multiplier — so the multipliers are what a test
	// timing assumption actually depends on. Extension beyond the ticket's listed
	// symbols; bare Go identifiers, no package prefix.
	MostFrequentPollingMultiplier int
	PollingUpdateLength           int

	// optimizer selection weights from DefaultUpstreamSelectorConfig(), as flat
	// top-level keys (the ticket's Phase 2 shape rule).
	AvailabilityWeight float64
	LatencyWeight      float64
	SyncWeight         float64
	StakeWeight        float64
	MinSelectionChance float64

	// Live per-chain optimizer weights — extension beyond the ticket. Keyed by chainID,
	// so it is inherently nested and sits alongside the flat defaults above.
	PerChainOptimizer map[string]routerConfigOptimizerWeights
}

// buildDebugMux constructs the /debug/time-warp, /debug/time, /debug/reset-scores,
// and /debug/reset-all HTTP handlers.
//
// See rpcconsumer.buildDebugMux for full documentation of time-warp / time / reset-scores
// — this is the rpcsmartrouter copy, extended with /debug/reset-all (a single endpoint
// that flushes every router-internal state store so black-box tests can return to a
// known-clean state without restarting the pod).
func buildDebugMux(deps debugMuxDeps) *http.ServeMux {
	optimizers := deps.optimizers
	currentOffsetNano := deps.offsetNano
	// maxDebugOffsetSeconds caps the allowed forward warp to exactly 24 h (86 400 s).
	// Upper: +24 h crosses a calendar-day boundary; ResetState() — called automatically
	//        whenever the offset decreases — purges future-dated ScoreStore entries so
	//        real-time samples are accepted immediately after reset.
	// Lower: negative offsets are rejected — a backward shift puts po.now() in
	//        the past, so existing ScoreStore entries (from real/forward time) are
	//        newer than the new sampleTime, triggering the same TimeConflictingScoresError
	//        freeze as an uncleared forward warp.
	const maxDebugOffsetSeconds = float64(24 * 3600) // 86400 s

	mux := http.NewServeMux()

	mux.HandleFunc("/debug/time-warp", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		// Cap the body at 1 KiB — payload is {"offset_seconds": N}, 1 KiB is
		// orders of magnitude over the legitimate size and prevents a caller
		// from streaming an unbounded body into the JSON decoder.
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		var body struct {
			OffsetSeconds float64 `json:"offset_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		// Reject NaN / ±Inf — these cast to math.MinInt64 when converted to
		// time.Duration (int64), producing a huge negative offset.
		if math.IsNaN(body.OffsetSeconds) || math.IsInf(body.OffsetSeconds, 0) {
			http.Error(w, "offset_seconds must be a finite number", http.StatusBadRequest)
			return
		}
		// Reject negative offsets — backward shifts freeze the optimizer.
		if body.OffsetSeconds < 0 {
			http.Error(w, "offset_seconds must be >= 0 (no travel to the past)", http.StatusBadRequest)
			return
		}
		// Reject values above 24 h.
		if body.OffsetSeconds > maxDebugOffsetSeconds {
			http.Error(w, fmt.Sprintf("offset_seconds must be <= %g (24h)", maxDebugOffsetSeconds), http.StatusBadRequest)
			return
		}

		newNano := int64(body.OffsetSeconds * float64(time.Second))
		prevNano := currentOffsetNano.Swap(newNano)
		needsReset := newNano < prevNano

		offset := time.Duration(newNano)
		optimizers.Range(func(chainID string, opt *provideroptimizer.ProviderOptimizer) bool {
			if offset == 0 {
				opt.NowFunc = nil
			} else {
				opt.NowFunc = func() time.Time { return time.Now().Add(offset) }
			}
			if needsReset {
				opt.ResetState()
			}
			return true
		})
		// /debug/time-warp is the optimizer/QoS clock ONLY. ChainState's TTL/staleness clock is driven
		// by the separate /debug/chain-state-time-warp endpoint, so a routine QoS warp-then-reset here
		// never disturbs ChainState (MAG-2307 review). The per-user seen-block cache this reset used to
		// flush was retired in T2 (consistency now reads the self-healing tip), so there is nothing to
		// flush here; /debug/reset-all and /debug/reset-scores reset ChainState instead (T11).
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"offset_seconds":%v,"applied_to_chains":true}`, body.OffsetSeconds)
	})

	// POST /debug/chain-state-time-warp — ages the per-chain ChainState TTL/staleness/consensus clock
	// WITHOUT waiting real time, so black-box tests can cross a ~130 s ETH staleness window in seconds.
	// Deliberately SEPARATE from /debug/time-warp (the optimizer/QoS clock): a routine QoS
	// warp-then-reset must never disturb ChainState (MAG-2307 review). offset_seconds is relative to
	// real time; 0 returns ChainState to real time. Same validation as /debug/time-warp (finite, >= 0,
	// <= 24 h). Responds with chain_states_warped (a count) — a distinct key from /debug/time-warp's
	// boolean applied_to_chains, so the two responses never collide on JSON type. The effect is
	// observable via TipFresh / BaselineFresh in /debug/chain-state.
	//
	// Semantic note: because stored observation timestamps use the real clock while freshness is
	// evaluated against the warp (see ChainState.effectiveNow), a block/consensus write that arrives
	// DURING an active >TTL warp reads as already aged — live traffic cannot refresh TipFresh while the
	// warp is held. That is the intended "age out state while endpoints are quiesced" behavior; for a
	// clean result, warp with the target endpoints quiesced.
	mux.HandleFunc("/debug/chain-state-time-warp", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		var body struct {
			OffsetSeconds float64 `json:"offset_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if math.IsNaN(body.OffsetSeconds) || math.IsInf(body.OffsetSeconds, 0) {
			http.Error(w, "offset_seconds must be a finite number", http.StatusBadRequest)
			return
		}
		if body.OffsetSeconds < 0 {
			http.Error(w, "offset_seconds must be >= 0 (no travel to the past)", http.StatusBadRequest)
			return
		}
		if body.OffsetSeconds > maxDebugOffsetSeconds {
			http.Error(w, fmt.Sprintf("offset_seconds must be <= %g (24h)", maxDebugOffsetSeconds), http.StatusBadRequest)
			return
		}
		offset := time.Duration(int64(body.OffsetSeconds * float64(time.Second)))
		applied := setAllChainStateDebugOffset(deps, offset)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"offset_seconds":%v,"chain_states_warped":%d}`, body.OffsetSeconds, applied)
	})

	// GET /debug/time — returns real and effective time so callers can verify the clock moved.
	mux.HandleFunc("/debug/time", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		nano := currentOffsetNano.Load()
		effective := now.Add(time.Duration(nano))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"real_time":%q,"effective_time":%q,"offset_seconds":%v}`,
			now.UTC().Format(time.RFC3339),
			effective.UTC().Format(time.RFC3339),
			float64(nano)/float64(time.Second))
	})

	// POST /debug/reset-scores — clears optimizer score state without changing
	// current time offset or NowFunc. Also zeroes per-endpoint chain-tracker
	// latest-block values and resets the per-chain ChainState tip/baseline. (It
	// used to flush the per-chain seen-block caches too; that store was retired by
	// Topic C C-G, and the ChainState tip that replaced it is what the reset below
	// now clears.) Gives a recovery path that does not touch session-manager state
	// — which trips a separate BTC-router regression on /debug/reset-all.
	mux.HandleFunc("/debug/reset-scores", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		count := 0
		optimizers.Range(func(chainID string, opt *provideroptimizer.ProviderOptimizer) bool {
			opt.ResetState()
			count++
			return true
		})
		trackersReset := resetAllChainTrackers(deps)
		chainStatesReset := resetAllChainStates(deps)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reset":true,"chains_reset":%d,"trackers_reset":%d,"chainstates_reset":%d}`,
			count, trackersReset, chainStatesReset)
	})

	// POST /debug/reset-all — flush every state store the test framework
	// cares about in a single call: in-process Ristretto, optimizer scores,
	// relay retry bans, sticky
	// sessions, reported providers, cross-epoch blocked-provider memory,
	// and — when --cache-be is configured — the external cache-be pod
	// (MAG-1764). Equivalent to the legacy time-warp(+3600) → time-warp(0)
	// → reset-scores dance plus the surviving state above.
	//
	// "Live pairing" (pairing tables, valid/backup addresses) is intentionally
	// left intact: we want the next relay to route normally without waiting
	// for an epoch transition.
	//
	// Returns 500 if cache-be is configured and its flush RPC fails — a
	// silent failure here would advertise a capability the deployment did
	// not actually fulfill.
	mux.HandleFunc("/debug/reset-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		// 1. Optimizers: drop score caches, T-Digests, latest-sync data, and
		//    the local Ristretto response cache. Also clear any active NowFunc
		//    and zero the offset so a stale forward-warp doesn't reappear after
		//    new samples come in.
		currentOffsetNano.Store(0)
		optimizers.Range(func(chainID string, opt *provideroptimizer.ProviderOptimizer) bool {
			opt.NowFunc = nil
			opt.ResetState()
			return true
		})
		// ChainState's debug warp is intentionally NOT cleared here: it is independent of the
		// optimizer/QoS state this endpoint resets, and is cleared only via its own
		// /debug/chain-state-time-warp endpoint (offset 0), so a QoS reset never disturbs ChainState
		// (MAG-2307 review).

		// 2. The per-chain seen-block consistency caches used to be flushed here. Topic C C-G
		//    retired that store entirely — consistency now measures against the anti-lie-guarded
		//    chain tip. The tip self-heals from most poisoning (Recompute snaps an out-of-band tip
		//    down; stale-tip re-adoption drops a frozen one within a TTL), but a WITHIN-BAND lie
		//    kept fresh by the liar's own traffic, and a cold-start lie before any peer baseline
		//    forms, have no automatic downward path. So the tip that replaced the seen-block store
		//    IS reset here explicitly — this is the operator big-hammer that self-heal can't cover.
		//    Surfaced in the response body as the "chain-state" capability below.
		resetAllChainStates(deps)
		//
		// 3. Per-server RelayRetriesManagers (6h hash ban cache), 4. per-CSM
		//    transient failure state, and 4b. per-CSM blocked-providers list.
		//    All require the router to be present; test fixtures without a
		//    router still get a useful partial reset above and we report which
		//    stores actually moved.
		//
		//    Why ResetBlockedProviders runs separately from
		//    ResetTransientFailureState:
		//    ResetTransientFailureState deliberately preserves
		//    currentlyBlockedProviderAddresses because in lava-pairing-network
		//    mode unblocking is an epoch-boundary operation. In direct-rpc mode
		//    (this fork's default) there are no epoch transitions, so blocked
		//    providers can only accumulate across test runs unless we mass-
		//    restore here. ResetBlockedProviders is the explicit escape hatch
		//    for /debug/* paths; production relay paths never reach it.
		if deps.router != nil {
			deps.router.mu.Lock()
			for _, server := range deps.router.rpcServers {
				if server != nil && server.relayRetriesManager != nil {
					server.relayRetriesManager.Reset()
				}
			}
			for _, csm := range deps.router.sessionManagers {
				if csm != nil {
					csm.ResetTransientFailureState()
					csm.ResetBlockedProviders()
				}
			}
			deps.router.mu.Unlock()
		}

		// 5. External cache-be pod (MAG-1764). When --cache-be is configured,
		//    the in-process Ristretto reset above is not the cache callers
		//    actually hit — the real cache lives in a separate pod. Flush it
		//    via RPC; on real failure return 500 so the test framework fails
		//    loud instead of trusting a misleading capability advertisement.
		//
		//    The codes.Unimplemented branch is the rolling-deploy escape:
		//    a new router talking to an old cache pod (no FlushCache RPC
		//    yet) must not break /debug/reset-all. We degrade quietly —
		//    in-process Ristretto cleared, "cache-be" omitted from cleared
		//    so the advertisement stays honest.
		//
		//    Bounded by a short context: a slow cache pod must not stall the
		//    debug handler. CacheActive() gates both the call and the
		//    capability advertisement so deployments without --cache-be don't
		//    advertise a key they didn't fulfill.
		cacheBeFlushed := false
		if deps.cache != nil && deps.cache.CacheActive() {
			flushCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			err := deps.cache.Flush(flushCtx)
			cancel()
			switch {
			case err == nil:
				cacheBeFlushed = true
			case status.Code(err) == codes.Unimplemented:
				utils.LavaFormatWarning("cache-be does not implement FlushCache; treating as legacy pod", err)
			default:
				http.Error(w, fmt.Sprintf("cache-be flush failed: %v", err), http.StatusInternalServerError)
				return
			}
		}

		// MAG-2186: additionally recover endpoint health and cold-rebuild pairing so a
		// single /debug/reset-all returns the router to a serving state after an
		// all-providers-down stress burst. Disabled endpoints (Endpoint.Enabled=false from
		// MaxConsecutiveConnectionAttempts) and demoted providers otherwise persist across
		// tests and contaminate later runs. rebuildPairingFromConfig is a cold rebuild (no
		// re-probing) and a no-op when the pairing is already whole, so the historical
		// "leave pairing intact" stance no longer applies — and every existing
		// /debug/reset-all caller inherits the fix for free, with no test migration.
		if deps.router != nil {
			deps.router.rebuildPairingFromConfig()
		}
		resetEndpointHealthAndGauge(deps)

		// Capability advertisement — hardcoded for in-process stores, plus a
		// conditional "cache-be" key when the external pod was actually
		// flushed. The test framework probes this body to decide between this
		// endpoint and the legacy 4-call dance. "seen-block" is retained for
		// external probers that still require the key: the per-user store it
		// named was retired by Topic C C-G, but reset-all now clears the
		// ChainState tip that replaced it (also advertised as "chain-state"), so
		// the key is honest again — a reset genuinely drops the tip. "cache-be"
		// signals MAG-1764 end-to-end coverage, "blocked-providers" signals
		// MAG-1810, and "endpoint-health" + "pairing" signal the MAG-2186
		// endpoint-health reset and cold pairing rebuild added above.
		w.Header().Set("Content-Type", "application/json")
		if cacheBeFlushed {
			fmt.Fprint(w, `{"reset":true,"cleared":["optimizer","ristretto","retries-manager","session-manager","reported-providers","sticky-sessions","seen-block","chain-state","blocked-providers","endpoint-health","pairing","cache-be"]}`)
		} else {
			fmt.Fprint(w, `{"reset":true,"cleared":["optimizer","ristretto","retries-manager","session-manager","reported-providers","sticky-sessions","seen-block","chain-state","blocked-providers","endpoint-health","pairing"]}`)
		}
	})

	// POST /debug/reset-pairing — cold-rebuild every chain's pairing table from
	// the startup provider config, re-admitting providers the per-epoch spec
	// re-verifier (applyReverification) demoted. Additive companion to
	// /debug/reset-all, which intentionally leaves pairing intact (see the comment
	// above): long test runs accumulate demotions faster than the 15m epoch tick
	// recovers them, and a pod restart is otherwise the only way to readmit a
	// demoted primary. Cold — no re-probing; the simulator controls provider
	// behaviour in the tests this serves. No-op 200 (empty restored) when the
	// router is absent (test fixtures) so callers can probe it unconditionally.
	mux.HandleFunc("/debug/reset-pairing", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		restored := map[string][]string{}
		if deps.router != nil {
			restored = deps.router.rebuildPairingFromConfig()
		}
		w.Header().Set("Content-Type", "application/json")
		body, err := json.Marshal(map[string]any{"reset": true, "restored": restored})
		if err != nil {
			// restored is map[string][]string — Marshal can't realistically fail;
			// emit a minimal valid body rather than a half-written response.
			fmt.Fprint(w, `{"reset":true,"restored":{}}`)
			return
		}
		w.Write(body)
	})

	// GET /debug/logs — return recent in-memory log records, optionally scoped
	// by request_id and/or a [from,to] time window, so an external test harness
	// can attach the router's logs to failing tests. The ring buffer is enabled
	// only in debug mode (see EnableDebugLogBuffer in Start); when it was never
	// enabled this returns an empty set. Each line is an already-valid JSON
	// object (zerolog JSON record); we assemble the array by joining the raw
	// bytes with commas rather than re-marshalling.
	mux.HandleFunc("/debug/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		requestID := q.Get("request_id")

		// from/to are RFC3339; empty or unparseable → zero time (ignored).
		var from, to time.Time
		if v := q.Get("from"); v != "" {
			if parsed, err := time.Parse(time.RFC3339, v); err == nil {
				from = parsed
			}
		}
		if v := q.Get("to"); v != "" {
			if parsed, err := time.Parse(time.RFC3339, v); err == nil {
				to = parsed
			}
		}

		// limit defaults to 5000, capped at the ring capacity (50000).
		limit := 5000
		if v := q.Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			}
		}
		if limit > 50000 {
			limit = 50000
		}

		lines := utils.ReadDebugLogBuffer(requestID, from, to, limit)

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"count":%d,"lines":[`, len(lines))
		for i, line := range lines {
			if i > 0 {
				w.Write([]byte{','})
			}
			// Strip a trailing newline zerolog appends to each record so the
			// assembled array stays compact and valid.
			w.Write([]byte(strings.TrimRight(string(line), "\n")))
		}
		fmt.Fprint(w, "]}")
	})

	// POST /debug/logs/clear — drop every buffered log record so a test can
	// start from a clean slate before exercising a scenario.
	mux.HandleFunc("/debug/logs/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		utils.ClearDebugLogBuffer()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"cleared":true}`)
	})

	// GET /debug/endpoint-state — per-endpoint poll-health + enable/recovery state (MAG-2202 endpoint 1).
	// Returns a flat array of self-describing records (ChainID + ApiInterface + NetworkAddress identify
	// each row; no object nesting), joining the per-endpoint observation record
	// (EndpointMonitor.SnapshotObservations: LatestBlock/ObservedAt/Source + poll-health) with the
	// endpoint's enable/recovery state (Endpoint.HealthSnapshot: Enabled/DisabledAt/
	// ConsecutiveHealthyProbes). Lets the automation suite verify F1 re-enable, recovery latency, the
	// relay-vs-poll Source gate, and failure-streak reset from the wire. Read-only; nil-router safe.
	mux.HandleFunc("/debug/endpoint-state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		rows := []map[string]any{}
		if deps.router != nil {
			deps.router.mu.Lock()
			for chainKey, server := range deps.router.rpcServers {
				if server == nil || server.endpointChainTrackerManager == nil || server.listenEndpoint == nil {
					continue
				}
				csm := deps.router.sessionManagers[chainKey]
				if csm == nil {
					continue
				}
				observations := server.endpointChainTrackerManager.SnapshotObservations()
				backoffByURL := server.endpointChainTrackerManager.BackoffSnapshot()
				for _, ep := range csm.GetAllDirectRPCEndpoints() {
					if ep == nil || ep.Endpoint == nil {
						continue
					}
					url := ep.Endpoint.NetworkAddress
					health := ep.Endpoint.HealthSnapshot()
					obs := observations[url] // zero value when no observation recorded yet
					rows = append(rows, map[string]any{
						"ChainID":                  server.listenEndpoint.ChainID,
						"ApiInterface":             server.listenEndpoint.ApiInterface,
						"NetworkAddress":           url,
						"Enabled":                  health.Enabled,
						"DisabledAt":               debugTimeRFC3339(health.DisabledAt),
						"ConsecutiveHealthyProbes": health.ConsecutiveHealthyProbes,
						"ConsecutivePollFailures":  obs.ConsecutivePollFailures,
						"LastSuccessfulPoll":       debugTimeRFC3339(obs.LastSuccessfulPoll),
						"LatestBlock":              obs.LatestBlock,
						"ObservedAt":               debugTimeRFC3339(obs.ObservedAt),
						"Source":                   obs.Source.String(),
						// The MAG-2550 replay gate, made visible: a disabled endpoint with a
						// RelayProbeMethod is held pending a successful replay of that method (or
						// the attempt-budget fallback) — NOT merely earning its poll streak.
						"RelayProbeMethod":   health.RelayProbeMethod,
						"RelayProbeAttempts": health.RelayProbeAttempts,
						"ReenableProbeFlaps": health.ReenableProbeFlaps,
						// PollIntervalMs is the live dedicated-poll cadence: base when healthy,
						// exponentialBackoff-stretched when the endpoint has been failing. This is the
						// observable /debug/reset-probe-backoff returns to base (MAG-2395).
						"PollIntervalMs": backoffByURL[url].Milliseconds(),
						// HashPolling says whether this chain's tracker does block-hash work
						// (fork detection) and, when it does not, WHY — "off-operator-choice"
						// (--enable-fork-detection not set) vs "off-spec-no-block-by-num" (the
						// chain cannot serve hashes at all, so the flag would not help).
						"HashPolling": server.endpointChainTrackerManager.HashPollingMode().String(),
					})
				}
			}
			deps.router.mu.Unlock()
		}
		writeDebugRows(w, rows)
	})

	// GET /debug/chain-state — per-chain consensus/tip state (MAG-2202 endpoint 2). Flat array of
	// self-describing records (ChainID + ApiInterface). Raw, NON-TTL-gated snapshot
	// (ChainState.DebugSnapshot) so the suite can assert anti-lie outlier rejection, downward
	// realignment, TTL expiry, empty-snapshot baseline clearing, and cold-start bootstrap from the raw
	// (block, timestamp) pairs — a TTL-gated getter would hide exactly those transitions. Read-only;
	// nil-router safe.
	mux.HandleFunc("/debug/chain-state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		rows := []map[string]any{}
		if deps.router != nil {
			deps.router.mu.Lock()
			for _, server := range deps.router.rpcServers {
				if server == nil || server.chainState == nil || server.listenEndpoint == nil {
					continue
				}
				s := server.chainState.DebugSnapshot()
				rows = append(rows, map[string]any{
					"ChainID":           server.listenEndpoint.ChainID,
					"ApiInterface":      server.listenEndpoint.ApiInterface,
					"ObservedTip":       s.ObservedTip,
					"LastObservedAt":    debugTimeRFC3339(s.LastObservedAt),
					"ConsensusBaseline": s.ConsensusBaseline,
					"HasBaseline":       s.HasBaseline,
					"BaselineSince":     debugTimeRFC3339(s.BaselineSince),
					"Initialized":       s.Initialized,
					"TipFresh":          s.TipFresh,
					"BaselineFresh":     s.BaselineFresh,
				})
			}
			deps.router.mu.Unlock()
		}
		writeDebugRows(w, rows)
	})

	// GET /debug/provider-routing — per-CSM routing-pool state (MAG-2202 endpoint 3). Flat array of
	// self-describing records (ChainID + ApiInterface): ValidAddresses / CurrentlyBlockedProviderAddresses
	// / BlockedBackupProviders, so the suite can confirm a re-enabled provider is actually back in the
	// routing pool rather than enabled-but-unroutable (F2). Read-only; nil-router safe.
	mux.HandleFunc("/debug/provider-routing", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		rows := []map[string]any{}
		if deps.router != nil {
			deps.router.mu.Lock()
			for _, csm := range deps.router.sessionManagers {
				if csm == nil {
					continue
				}
				ep := csm.RPCEndpoint()
				s := csm.ProviderRoutingSnapshot()
				rows = append(rows, map[string]any{
					"ChainID":                           ep.ChainID,
					"ApiInterface":                      ep.ApiInterface,
					"ValidAddresses":                    s.ValidAddresses,
					"CurrentlyBlockedProviderAddresses": s.CurrentlyBlockedProviderAddresses,
					"BlockedBackupProviders":            s.BlockedBackupProviders,
					// Blocked answers WHY each of the above is out (MAG-2599); HeldOff covers the
					// providers that are eligible and healthy but are not being asked yet because
					// they rate-limited us. The three original keys are unchanged on purpose — the
					// MAG-2202 suite reads them by name.
					"Blocked": s.Blocked,
					"HeldOff": s.HeldOff,
				})
			}
			deps.router.mu.Unlock()
		}
		writeDebugRows(w, rows)
	})

	// GET /debug/probe-loop — per-chain proactive-prober cycle telemetry (MAG-2202 endpoint 4). Flat
	// array of self-describing records (ChainID + ApiInterface): the configured --probe-loop-interval
	// cadence (CycleIntervalMs) plus a snapshot of the last completed runProbeCycle — start/duration,
	// endpoints scored, endpoints re-enabled (F1), and providers whose QoS sample fed no sync evidence
	// (F5). CyclesCompleted is the monotonic liveness counter for verifying the prober ticks on its own
	// cadence (F6). Read-only; nil-router safe. Timestamps/durations: RFC3339 + integer milliseconds.
	mux.HandleFunc("/debug/probe-loop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		rows := []map[string]any{}
		if deps.router != nil {
			deps.router.mu.Lock()
			for _, server := range deps.router.rpcServers {
				if server == nil || server.listenEndpoint == nil {
					continue
				}
				s := server.probeStats.snapshot()
				rows = append(rows, map[string]any{
					"ChainID":             server.listenEndpoint.ChainID,
					"ApiInterface":        server.listenEndpoint.ApiInterface,
					"CycleIntervalMs":     s.CycleIntervalMs,
					"CyclesCompleted":     s.CyclesCompleted,
					"LastCycleStartedAt":  debugTimeRFC3339(s.LastCycleStartedAt),
					"LastCycleDurationMs": s.LastCycleDurationMs,
					"EndpointsScored":     s.EndpointsScored,
					"ReEnabledCount":      s.ReEnabledCount,
					"SyncOmittedCount":    s.SyncOmittedCount,
					// MAG-2550 replay-gate telemetry: how many disabled endpoints are held on
					// recorded relay evidence, and the cumulative replay outcomes.
					"EvidenceGatedCount":  s.EvidenceGatedCount,
					"ReplaysAttempted":    s.ReplaysAttempted,
					"ReplaysRecovered":    s.ReplaysRecovered,
					"ReplaysStillFailing": s.ReplaysStillFailing,
					"ReplaysInconclusive": s.ReplaysInconclusive,
				})
			}
			deps.router.mu.Unlock()
		}
		writeDebugRows(w, rows)
	})

	// POST /debug/poll-now — force ONE endpoint's ChainTracker to run its dedicated poll right now,
	// and answer only once that poll's result has been recorded (MAG-2649). Without it a test that
	// checks what a poll records has to sit out the per-endpoint cadence (avgBlockTime/divisor —
	// ~6-12 s on Ethereum) before it can look; with it the sequence becomes set up state →
	// trigger → read.
	//
	// Fidelity is the whole point: this triggers the production poll cycle
	// (chaintracker.ChainTracker.PollNow → fetchAllPreviousBlocksIfNecessary) on the tracker's own
	// poll goroutine — the same function the cadence timer calls. Nothing about the poll is
	// reimplemented here, so what the caller reads back is real router behavior with only the wait
	// removed: same parse, same observation write (LatestBlock/Source, ConsecutivePollFailures,
	// LastSuccessfulPoll), same endpoint-tip and per-chain tip updates, all completed before this
	// handler responds.
	//
	// Body: {"network_address": "<url>"} — required; optional "chain_id" / "api_interface" narrow
	// the match when the same URL is registered on more than one chain or interface. The response is
	// a row per matched endpoint (see /debug/endpoint-state for the shared field vocabulary), plus:
	//   Polled       — the record in this row IS this poll's, and may be trusted as fresh. False
	//                  means the record predates the call and says nothing about now: either nothing
	//                  was polled (tracker still starting, retrying its init, or stopped), or a poll
	//                  was started and did not finish inside the handler's budget. Assert on this
	//                  before reading any other field.
	//   PollError    — the poll ran and failed upstream. That is a legitimate outcome to assert on
	//                  (it is what increments ConsecutivePollFailures), so it answers 200, not 5xx.
	//                  Only ever set alongside Polled=true, so the pair cannot describe a failure
	//                  streak that was never recorded.
	//   TriggerError — why this row carries no trustworthy record, including the tracker's lifecycle
	//                  state. Set whenever Polled is false.
	//
	// Two deliberate limits, both to keep this from lying to a test:
	//   - It bypasses the relay traffic gate, so a fresh relay tip cannot silently turn the trigger
	//     into a no-op — but it therefore resets the gate's skip budget. Do not call it from a test
	//     that measures poll cadence, the idle-poll ceiling, the gate's skip behaviour, or failure
	//     backoff growth; those must measure real elapsed time (the poll's cadence state is
	//     untouched here precisely so they stay measurable).
	//   - It records ONE endpoint's poll. The per-chain consensus baseline is recomputed on its own
	//     tick, so /debug/chain-state's ConsensusBaseline is NOT guaranteed to have moved when this
	//     returns; the endpoint's own record and the chain's ObservedTip are.
	//
	// Two consequences worth knowing before writing a test against it.
	//
	// Assert Polled before reading any other field in a row. A false Polled with a "not awaited"
	// TriggerError is not a broken tracker: the poll is running and will record, this response just
	// could not wait for it. Retry, or narrow the match with chain_id / api_interface so fewer
	// endpoints share the budget — do not treat it as an endpoint fault.
	//
	// A head moved BACKWARD is not visible here. The poll runs and is recorded, but the endpoint tip
	// is block-monotonic — a lower block is held off as a late straggler until the stored tip goes
	// stale (StalenessWindow, ~120 s on Ethereum) — and the tracker does not walk its own head down
	// either. So a setup that rewinds a simulator's head and then triggers a poll reads back the
	// previous, higher block. That is the production anti-flap rule faithfully applied, not a failed
	// trigger.
	mux.HandleFunc("/debug/poll-now", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		// Same 1 KiB cap as the other JSON debug handlers: the body is three short strings.
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		var body struct {
			NetworkAddress string `json:"network_address"`
			ChainID        string `json:"chain_id"`
			ApiInterface   string `json:"api_interface"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if body.NetworkAddress == "" {
			http.Error(w, "network_address is required", http.StatusBadRequest)
			return
		}

		targets := resolvePollNowTargets(deps, body.NetworkAddress, body.ChainID, body.ApiInterface)
		if len(targets) == 0 {
			http.Error(w, "no ChainTracker registered for that endpoint", http.StatusNotFound)
			return
		}

		// Ceiling on the whole handler: one poll is bounded by the tracker's own fetch timeout, but
		// a request that arrives mid-cycle also waits for the in-flight poll first. Cap the sum so
		// this can never hang a test harness; the poll itself is unaffected and still completes.
		//
		// Deliberately ONE budget for all matched targets rather than one each, so a multi-target
		// address cannot multiply the worst-case hang by the number of matches. The cost is that a
		// late target can inherit a nearly-spent budget and report Polled=false with TriggerError set
		// — honest, and the reason PollNow reports the not-awaited case that way instead of claiming
		// a poll it did not witness. Narrow with chain_id / api_interface when a test needs every
		// matched endpoint to get a full budget.
		ctx, cancel := context.WithTimeout(r.Context(), debugPollNowTimeout)
		defer cancel()

		rows := []map[string]any{}
		polledAny := false
		for _, target := range targets {
			start := time.Now()
			obs, polled, err := target.monitor.PollNow(ctx, body.NetworkAddress)
			row := map[string]any{
				"ChainID":                 target.chainID,
				"ApiInterface":            target.apiInterface,
				"NetworkAddress":          body.NetworkAddress,
				"Polled":                  polled,
				"DurationMs":              time.Since(start).Milliseconds(),
				"PollError":               "",
				"TriggerError":            "",
				"LatestBlock":             obs.LatestBlock,
				"ObservedAt":              debugTimeRFC3339(obs.ObservedAt),
				"Source":                  obs.Source.String(),
				"ConsecutivePollFailures": obs.ConsecutivePollFailures,
				"LastSuccessfulPoll":      debugTimeRFC3339(obs.LastSuccessfulPoll),
			}
			switch {
			case polled:
				polledAny = true
				if err != nil {
					row["PollError"] = err.Error()
				}
			case err != nil:
				row["TriggerError"] = err.Error()
			}
			rows = append(rows, row)
		}
		// No matched endpoint produced a record this response can vouch for: say so in the status
		// line as well as the rows, so a harness that only checks the code does not read "nothing
		// usable happened" as success.
		//
		// 504 covers two causes, and TriggerError is what separates them — the code alone must not be
		// read as "the tracker is broken". Either no poll could be STARTED (still starting, retrying
		// its init, stopped — the lifecycle state is named), or one was started and outlived the
		// budget above, which says nothing about the tracker and everything about the upstream's
		// latency or the budget being shared across matched targets.
		if !polledAny {
			w.Header().Set("Content-Type", "application/json") // must precede WriteHeader
			w.WriteHeader(http.StatusGatewayTimeout)
		}
		writeDebugRows(w, rows)
	})

	// GET /debug/provider-scores — the per-provider quality scores the router is holding RIGHT NOW:
	// the values it uses to decide where the next request goes (MAG-2707). Read-only.
	//
	// The router could already RESET these scores (/debug/reset-scores) but never read them: their
	// only exposure was Prometheus gauges on the metrics port, which is not published, so reading
	// them meant a hand-held tunnel — impossible from an automated run, and worse, when the tunnel
	// was down the read came back empty and the test passed having measured nothing.
	//
	// That failure mode dictates the error design here, and it is why this endpoint deliberately
	// breaks the convention of the other /debug state endpoints (which answer 200 [] when unwired):
	//   200  scores follow, in {"rows": [...], "chains_unavailable": [...]}. A provider that exists
	//        but has not been sampled yet IS included, with zero scores — that is a real answer, not
	//        an omission. chains_unavailable names any matched chain that produced NO rows, so a
	//        partly-populated multi-chain router cannot answer 200 while quietly omitting a chain
	//        (which would read as "that provider has no score" rather than "nothing was measured").
	//   404  chain_id was given and no optimizer is registered for it.
	//   503  NO scores could be obtained: no QoS sampler wired, no optimizer registered, or every
	//        matched chain has no providers known yet. The body names the chains, so a failing test
	//        says WHY rather than just "empty".
	// Never 200 with an empty, unexplained result — that is precisely the silent pass this endpoint
	// exists to end.
	//
	// Scores are computed ON DEMAND rather than served from the sampler's cache: that cache is empty
	// until the first tick and after /debug/reset-scores, so "not sampled yet" would be
	// indistinguishable from "no providers". Computing here also lets one test reset the scores, send
	// traffic, and read the result back in sequence. It publishes nothing — the usage-sink emit
	// belongs to the periodic sampler alone — so reading changes nothing about how the router behaves.
	//
	// Each row carries both score families, because they answer different questions: the RAW EWMA
	// values (AvailabilityScore, LatencyScore in seconds, SyncScore as lag in seconds) are what a
	// test asserting "this provider was not marked down" should read, while the Selection* values are
	// those same signals after normalisation, with Composite the number selection actually ranks on.
	// NetworkAddresses joins the row to /debug/endpoint-state (see providerEndpointURLs).
	mux.HandleFunc("/debug/provider-scores", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		chainIDFilter := r.URL.Query().Get("chain_id")

		snapshot := deps.qosClient.SnapshotReports(chainIDFilter) // nil-receiver safe
		if len(snapshot.ChainsRegistered) == 0 {
			if chainIDFilter != "" {
				http.Error(w, fmt.Sprintf("no optimizer registered for chain_id %q", chainIDFilter), http.StatusNotFound)
				return
			}
			http.Error(w, "provider scores unavailable: no optimizer registered (or no QoS sampler wired)", http.StatusServiceUnavailable)
			return
		}
		if len(snapshot.Reports) == 0 {
			http.Error(w, fmt.Sprintf("provider scores unavailable: no providers known yet for %v", snapshot.ChainsUnavailable), http.StatusServiceUnavailable)
			return
		}

		urlsByProvider := providerEndpointURLs(deps)
		rows := make([]map[string]any, 0, len(snapshot.Reports))
		for _, report := range snapshot.Reports {
			addresses := urlsByProvider[report.ProviderAddress]
			if addresses == nil {
				addresses = []string{} // marshal as [] rather than null
			}
			rows = append(rows, map[string]any{
				"ChainID":          report.ChainId,
				"ProviderAddress":  report.ProviderAddress,
				"NetworkAddresses": addresses,
				"Epoch":            report.Epoch,
				"EntryIndex":       report.EntryIndex,
				"Timestamp":        debugTimeRFC3339(report.Timestamp),
				// Raw EWMA values, as stored: availability 0-1 (higher better), latency seconds and
				// sync lag seconds (lower better).
				"AvailabilityScore": report.AvailabilityScore,
				"LatencyScore":      report.LatencyScore,
				"SyncScore":         report.SyncScore,
				// The same signals normalised for weighted random selection, and the composite the
				// selection actually ranks on.
				"SelectionAvailability": report.SelectionAvailability,
				"SelectionLatency":      report.SelectionLatency,
				"SelectionSync":         report.SelectionSync,
				"SelectionStake":        report.SelectionStake,
				"SelectionComposite":    report.SelectionComposite,
				// How much each parameter contributed to the composite.
				"AvailabilityContribution": report.AvailabilityContribution,
				"LatencyContribution":      report.LatencyContribution,
				"SyncContribution":         report.SyncContribution,
				"StakeContribution":        report.StakeContribution,
				// Traffic-side context for interpreting the scores above.
				"ProviderStake":     report.ProviderStake,
				"NodeErrorRate":     report.NodeErrorRate,
				"SelectionCount":    report.SelectionCount,
				"SelectionRate":     report.SelectionRate,
				"SelectionQoSScore": report.SelectionQoSScore,
				"SelectionRNGValue": report.SelectionRNGValue,
			})
		}
		sortDebugRows(rows)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(providerScoresResponse{
			Rows:              rows,
			ChainsUnavailable: snapshot.ChainsUnavailable,
		}); err != nil {
			utils.LavaFormatWarning("failed encoding provider scores response", err)
		}
	})

	// GET /debug/cross-validation-events — the cross-validation dissent the router actually recorded
	// (MAG-2772). Read-only.
	//
	// Same reason /debug/provider-scores exists (MAG-2707): the two surfaces that already held this
	// information could not be asserted on from an automated run. The info logs are diagnostic
	// evidence only by team rule, and smartrouter_cross_validation_mismatch_total sits on the
	// unpublished metrics port, where a down tunnel returns empty and the test passes having measured
	// nothing.
	//
	// It is EVENT-shaped rather than counter-shaped because the questions are per-request: which
	// provider dissented on THIS request, in which group. The counter's labels are
	// {spec, apiInterface, method, group, finality} — no provider, no request id — so counts can be
	// derived from these rows but these rows cannot be derived from the counter.
	//
	// One row per recorded comparison outcome, oldest first, from both recording paths (Source):
	//   reply-time  a content outlier seen before the reply — it is in the request's
	//               lava-cross-validation-disagreeing-providers header.
	//   straggler   a provider that lost the race to quorum (it is in pending-providers) and whose
	//               late answer the async watcher resolved. EVERY resolution is recorded, not only
	//               dissent: an "agreed" row is the positive control a test asserting "no dissent
	//               happened" anchors on, and node-error / protocol-error / not-received rows say a
	//               late answer arrived broken or never arrived.
	//
	// Filters (all optional, ANDed): request_id (the Lava-Guid response header value — NOT
	// /debug/logs' request_id, which is the caller's X-Request-Id), chain_id, outcome, limit (keeps
	// the most recent N). Status codes:
	//   200  the rows follow, as a flat JSON array; [] means the recorder was live and saw no dissent.
	//   503  the recorder is not installed, so nothing was being recorded and an empty answer would
	//        mean nothing. Structurally impossible on a router that serves this endpoint at all (the
	//        ring is installed by the same --debug-address condition that registers this mux), and
	//        answered anyway so the impossible case can never read as "no dissent".
	//
	// The ring is bounded; X-Cross-Validation-Events-Dropped reports how many events were evicted
	// since it was installed (or since the last clear), so truncation cannot pass for absence.
	mux.HandleFunc("/debug/cross-validation-events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		limit := 0 // 0 = the whole ring
		if v := q.Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			}
		}
		events, dropped, enabled := readCrossValidationEvents(crossValidationEventFilter{
			RequestID: q.Get("request_id"),
			ChainID:   q.Get("chain_id"),
			Outcome:   q.Get("outcome"),
			Limit:     limit,
		})
		if !enabled {
			http.Error(w, "cross-validation event recording is not enabled on this router", http.StatusServiceUnavailable)
			return
		}
		rows := make([]map[string]any, 0, len(events))
		for _, event := range events {
			rows = append(rows, crossValidationEventRow(event))
		}
		// Deliberately NOT sortDebugRows: these are events, not state, and recording order is the
		// meaningful one — it is what pairs a reply-time dissent with the straggler that followed it.
		// Seq makes that order explicit for a caller that re-sorts.
		w.Header().Set("X-Cross-Validation-Events-Dropped", strconv.FormatUint(dropped, 10))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(rows); err != nil {
			utils.LavaFormatWarning("failed encoding cross-validation events response", err)
		}
	})

	// POST /debug/cross-validation-events/clear — drop every recorded event so a test can isolate
	// itself from earlier traffic (MAG-2772). Optional for correctness — the rows are filterable by
	// request_id, which every test already holds — and provided because it is the same one-liner
	// /debug/logs/clear already offers. 503 when the recorder is not installed, so a clear that
	// cleared nothing is never reported as success.
	mux.HandleFunc("/debug/cross-validation-events/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		cleared, enabled := clearCrossValidationEvents()
		if !enabled {
			http.Error(w, "cross-validation event recording is not enabled on this router", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"cleared":true,"events_cleared":%d}`, cleared)
	})

	// POST /debug/reset-endpoint-health — focused companion to /debug/reset-all (MAG-2186):
	// re-enable every provider endpoint disabled by MaxConsecutiveConnectionAttempts
	// (Endpoint.Enabled=false) and mirror the reset onto the Prometheus health gauge,
	// nothing else. The name mirrors Endpoint.ResetHealth() in the source for discoverability.
	mux.HandleFunc("/debug/reset-endpoint-health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		reenabled := resetEndpointHealthAndGauge(deps)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reset":true,"endpoints_reenabled":%d}`, reenabled)
	})

	// POST /debug/reset-probe-backoff — clear the ChainTracker probe back-off on every endpoint so
	// each provider's poll schedule returns to base cadence (MAG-2395). The back-off (fetchFails
	// streak) is unreachable by reset-all / reset-scores / reset-endpoint-health / reset-pairing, so
	// a provider that failed before a reset otherwise keeps its stretched schedule (up to
	// BACKOFF_MAX_TIME = 1m) after it. Cadence only — nothing else is touched. The effect is
	// observable as PollIntervalMs returning to base in /debug/endpoint-state.
	mux.HandleFunc("/debug/reset-probe-backoff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		reset := resetAllProbeBackoff(deps)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reset":true,"endpoints_reset":%d}`, reset)
	})

	// POST /debug/reset-chaintracker-rows — re-register every configured endpoint that lost its
	// ChainTracker row (MAG-2395). The epoch cleanup can delete a briefly-unhealthy provider's row
	// (MAG-2445); no other reset recreates it, so it returns only on a later ~15-minute rebuild or a
	// pod restart. This re-adds any missing row from the config/pairing set and is a no-op for rows
	// that already exist. "rows_ensured" is the number of configured endpoints checked;
	// "rows_created" is how many rows were actually (re)created.
	mux.HandleFunc("/debug/reset-chaintracker-rows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		ensured, created := reregisterChainTrackerRows(deps)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reset":true,"rows_ensured":%d,"rows_created":%d}`, ensured, created)
	})

	// GET /debug/runtime-config — expose the router's live tuning values as JSON so the
	// test suite can read them at runtime instead of hardcoding copies that silently
	// drift when the source changes. Read-only; registered only in debug mode like the
	// rest of /debug/*. Values are read straight from their source symbols (and, for the
	// flag-bound vars, report the live value); the per-chain optimizer section is read
	// from the same optimizers map /debug/reset-scores uses.
	mux.HandleFunc("/debug/runtime-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}

		toWeights := func(c provideroptimizer.UpstreamSelectorConfig) routerConfigOptimizerWeights {
			return routerConfigOptimizerWeights{
				AvailabilityWeight: c.AvailabilityWeight,
				LatencyWeight:      c.LatencyWeight,
				SyncWeight:         c.SyncWeight,
				StakeWeight:        c.StakeWeight,
				MinSelectionChance: c.MinSelectionChance,
				SelectionMode:      c.SelectionMode.String(),
			}
		}

		// Non-nil so the JSON is {} rather than null when no optimizers are wired
		// (e.g. test fixtures).
		perChain := map[string]routerConfigOptimizerWeights{}
		if optimizers != nil {
			optimizers.Range(func(chainID string, opt *provideroptimizer.ProviderOptimizer) bool {
				perChain[chainID] = toWeights(opt.GetUpstreamSelectorConfig())
				return true
			})
		}

		smConfig := SmartRouterStateMachineConfig()

		optimizerDefaults := provideroptimizer.DefaultUpstreamSelectorConfig()

		resp := routerConfigResponse{
			SchemaVersion: 1,

			MaxConsecutiveConnectionAttempts:                 lavasession.MaxConsecutiveConnectionAttempts,
			TimeoutForEstablishingAConnection:                lavasession.TimeoutForEstablishingAConnection.Milliseconds(),
			MaximumNumberOfFailuresAllowedPerConsumerSession: lavasession.MaximumNumberOfFailuresAllowedPerConsumerSession,

			RelayRetryLimit:          relaycore.RelayRetryLimit,
			DisableBatchRequestRetry: relaycore.DisableBatchRequestRetry,

			MaximumNumberOfTickerRelayRetries: MaximumNumberOfTickerRelayRetries,
			SendRelayAttempts:                 SendRelayAttempts,

			EnableCircuitBreaker:    smConfig.EnableCircuitBreaker,
			CircuitBreakerThreshold: smConfig.CircuitBreakerThreshold,
			EnableTimeoutPriority:   smConfig.EnableTimeoutPriority,

			// TimePerCU is a uint64 of nanoseconds (not a time.Duration), so it is
			// divided by time.Millisecond rather than using .Milliseconds().
			TimePerCU:                int64(common.TimePerCU) / int64(time.Millisecond),
			MinimumTimePerRelayDelay: common.MinimumTimePerRelayDelay.Milliseconds(),
			DefaultTimeout:           common.DefaultTimeout.Milliseconds(),
			CacheTimeout:             common.CacheTimeout.Milliseconds(),

			ProbeUpdateWeight:         scoreutils.ProbeUpdateWeight,
			DefaultProbeUpdateWeight:  scoreutils.DefaultProbeUpdateWeight,
			MinAcceptableAvailability: scoreutils.MinAcceptableAvailability,
			HighCuThreshold:           scoreutils.HighCuThreshold,
			MidCuThreshold:            scoreutils.MidCuThreshold,

			MostFrequentPollingMultiplier: chaintracker.MostFrequentPollingMultiplier,
			PollingUpdateLength:           chaintracker.PollingUpdateLength,

			AvailabilityWeight: optimizerDefaults.AvailabilityWeight,
			LatencyWeight:      optimizerDefaults.LatencyWeight,
			SyncWeight:         optimizerDefaults.SyncWeight,
			StakeWeight:        optimizerDefaults.StakeWeight,
			MinSelectionChance: optimizerDefaults.MinSelectionChance,

			PerChainOptimizer: perChain,
		}

		w.Header().Set("Content-Type", "application/json")
		body, err := json.Marshal(resp)
		if err != nil {
			// resp is a flat struct of scalars + a string-keyed map — Marshal can't
			// realistically fail; surface a 500 rather than a half-written body.
			http.Error(w, "failed to marshal router config", http.StatusInternalServerError)
			return
		}
		w.Write(body)
	})

	return mux
}

// debugTimeRFC3339 formats a timestamp for the read-only /debug/* state endpoints, matching the
// /debug/time convention (UTC RFC3339). A zero time renders as the empty string rather than the
// Go zero-date ("0001-01-01T00:00:00Z"), so a test can cheaply distinguish "never happened"
// (e.g. DisabledAt on an enabled endpoint) from a real instant.
func debugTimeRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// debugRowKey is the deterministic sort key for a /debug/* state row: ChainID, then ApiInterface,
// then NetworkAddress (absent on chain/CSM rows → ""). comma-ok avoids a panic on the missing key.
func debugRowKey(m map[string]any) string {
	cid, _ := m["ChainID"].(string)
	api, _ := m["ApiInterface"].(string)
	na, _ := m["NetworkAddress"].(string)
	// ProviderAddress is the tiebreaker for rows keyed by provider rather than by endpoint
	// (/debug/provider-scores, MAG-2707). Without it every row of one chain shares a key and
	// sort.Slice — which is NOT stable — leaves their order to chance. Absent on every other row
	// type, where comma-ok yields "" and the key is unchanged.
	pa, _ := m["ProviderAddress"].(string)
	return cid + "\x00" + api + "\x00" + na + "\x00" + pa
}

// writeDebugRows sorts the records deterministically (so Go map-iteration order never leaks into the
// response, keeping output stable for test fixtures and humans) and encodes them as a JSON array.
// rows is always non-nil, so an empty result encodes as [] rather than null.
// sortDebugRows orders rows by debugRowKey in place, so Go map-iteration order never leaks into a
// response. Split out of writeDebugRows for the responses that wrap their rows in an envelope
// (/debug/provider-scores) and so cannot go through it.
func sortDebugRows(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool { return debugRowKey(rows[i]) < debugRowKey(rows[j]) })
}

func writeDebugRows(w http.ResponseWriter, rows []map[string]any) {
	sortDebugRows(rows)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rows); err != nil {
		utils.LavaFormatWarning("failed encoding debug response", err)
	}
}

// subscriptionCollectionProbe reports whether the collection at `internalPath`
// carries a SUBSCRIBE api, for the addons a node-url declares. It is the
// transport half of the chain router's rule for generating per-path urls
// (chainlib/chain_router.go): a subscription collection is served over ws, and
// everything else over http.
func subscriptionCollectionProbe(chainParser chainlib.ChainParser) func(internalPath string, declaredAddons []string) bool {
	return func(internalPath string, declaredAddons []string) bool {
		addons, _, err := chainParser.SeparateAddonsExtensions(context.Background(), declaredAddons)
		if err != nil {
			// The chain router surfaces this as a construction error. Here the
			// endpoint list is being built for relays, so an unreadable addon
			// set falls back to "http collection" — the shape all but STRK's
			// three ws paths have.
			return false
		}
		if len(addons) == 0 {
			addons = append(addons, "")
		}
		for _, connectionType := range []string{"POST", ""} {
			for _, addon := range addons {
				collectionKey := chainlib.CollectionKey{
					InternalPath:   internalPath,
					Addon:          addon,
					ConnectionType: connectionType,
				}
				if chainParser.IsTagInCollection(spectypes.FUNCTION_TAG_SUBSCRIBE, collectionKey) {
					return true
				}
			}
		}
		return false
	}
}

// expandInternalPaths resolves each configured node-url into the set of urls
// the router will actually dial, one per internal path the spec serves.
//
// It mirrors chainRouterImpl.BatchNodeUrlsByServices exactly, because the two
// have to agree on which url answers a given api collection: the chain router
// builds the proxies the tracker and the verifications run on, and this builds
// the direct-RPC endpoints relays run on. They diverged, and the relay path —
// having no internal path anywhere in its endpoint list — dialed whichever url
// session selection happened to hand it. A chain serving two API versions over
// two urls (TON: toncenter v2 and tonindex v3) answered v3 apis out of the v2
// upstream, which shows up as that vendor's 404 rather than as a routing fault.
//
//   - a url declaring `internal-path` IS that path's root and is taken as it
//     stands;
//   - a url declaring none is the shared root: it stays (for the spec's own
//     root collection, if it has one) and additionally yields one url per
//     other internal path, with the path appended — the same
//     `nodeUrl.Url = baseUrl + internalPath` the chain router does;
//   - a path is generated only on the transport that serves it. A ws url
//     yields the spec's subscription collections and an http url yields the
//     rest, so STRK's `/ws/rpc/v0_8` is a wss endpoint and its `/rpc/v0_8` an
//     https one — never the crossed pair, which no upstream answers.
//
// A spec with no internal paths at all (almost every chain) returns the input
// unchanged.
func expandInternalPaths(nodeUrls []common.NodeUrl, internalPaths []string, servesSubscriptions func(internalPath string, declaredAddons []string) bool) []common.NodeUrl {
	nonRoot := make([]string, 0, len(internalPaths))
	for _, internalPath := range internalPaths {
		// A path is appended to a url, so only a url path can be expanded.
		// Specs also use the internal-path field as a plain collection LABEL
		// for inheritance ingredients — STRK's "HTTP-ONLY" / "WS-ONLY". Those
		// are disabled today, so the parser never reports them, but the idiom
		// is in the spec repo and an enabled one would otherwise generate
		// `https://host` + `HTTP-ONLY`. Skipping it leaves the provider with no
		// endpoint for that path, which the selection filter reads as "not
		// populated for this provider" and falls back to today's behaviour.
		if strings.HasPrefix(internalPath, "/") {
			nonRoot = append(nonRoot, internalPath)
		}
	}
	if len(nonRoot) == 0 {
		return nodeUrls
	}
	// Deterministic: the endpoint list feeds session selection, and a map's
	// iteration order would reshuffle it every boot.
	sort.Strings(nonRoot)

	expanded := make([]common.NodeUrl, 0, len(nodeUrls))
	// An operator can declare the base url AND a pinned url that happens to
	// equal base+path. Both are legitimate; emitting the same (url, path) twice
	// would just probe it twice and register it twice in the metrics.
	seen := make(map[string]struct{}, len(nodeUrls))
	add := func(nodeUrl common.NodeUrl) {
		key := nodeUrl.Url + "\x00" + nodeUrl.InternalPath
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		expanded = append(expanded, nodeUrl)
	}
	for _, nodeUrl := range nodeUrls {
		add(nodeUrl)
		if nodeUrl.InternalPath != "" {
			continue
		}
		// gRPC node-urls are a bare host:port and do not parse as a url; the
		// chain router reads those as non-ws too.
		isWs, err := chainlib.IsUrlWebSocket(nodeUrl.Url)
		if err != nil {
			isWs = false
		}
		for _, internalPath := range nonRoot {
			// The transport has to serve the collection. A ws url answers the
			// spec's subscription collections and an http url answers the rest
			// — chain_router.go's autoGenerateMissingInternalPaths applies the
			// same rule to the proxies the tracker and the verifications run
			// on, and the two lists have to name the same urls. Generating
			// every path on every scheme would give a STRK config carrying both
			// `https://` and `wss://` the crossed pair as well
			// (`https://host/ws/rpc/v0_8`, `wss://host/rpc/v0_9`): urls no
			// upstream serves, probed on every sweep and registered in the
			// metrics, and now — with the filter below preferring an exact path
			// match — half of what a relay for that path can be routed onto.
			if isWs != servesSubscriptions(internalPath, nodeUrl.Addons) {
				continue
			}
			generated := nodeUrl
			if strings.HasSuffix(nodeUrl.Url, internalPath) {
				// The url already ENDS in this path — an operator who baked the
				// version into the url instead of declaring `internal-path`
				// (`https://vendor/rpc/v0_8`, no field). It is the endpoint for
				// that path as it stands; appending would ask the vendor for
				// `/rpc/v0_8/rpc/v0_8`. It also stays the root endpoint, added
				// above, which is what that config serves today.
				generated.InternalPath = internalPath
				add(generated)
				continue
			}
			generated.Url = nodeUrl.Url + internalPath
			generated.InternalPath = internalPath
			add(generated)
		}
	}
	return expanded
}

func (rpsr *RPCSmartRouter) CreateSmartRouterEndpoint(
	ctx context.Context,
	rpcEndpoint *lavasession.RPCEndpoint,
	errCh chan error,
	optimizers *common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer],
	chainMutexes map[string]*sync.Mutex,
	options *rpcSmartRouterStartOptions,
	smartRouterIdentifier string,
	rpcSmartRouterMetrics *metrics.RPCConsumerLogs,
	smartRouterOptimizerQoSClient *metrics.ConsumerOptimizerQoSClient,
	smartRouterMetricsManager *metrics.SmartRouterMetricsManager,
	relaysMonitorAggregator *metrics.RelaysMonitorAggregator,
) error {
	chainParser, err := chainlib.NewChainParser(rpcEndpoint.ApiInterface)
	if err != nil {
		err = utils.LavaFormatError("failed creating chain parser", err, utils.Attribute{Key: "endpoint", Value: rpcEndpoint})
		errCh <- err
		return err
	}
	chainID := rpcEndpoint.ChainID

	// Load spec from static file or query from blockchain
	// Smart router queries spec once during initialization (no ongoing updates)
	if len(options.cmdFlags.StaticSpecPaths) > 0 {
		// Load spec from static file/directory/URL sources
		err = statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken(ctx, chainParser, options.cmdFlags.StaticSpecPaths, *rpcEndpoint, options.cmdFlags.GitHubToken, options.cmdFlags.GitLabToken)
		if err != nil {
			err = utils.LavaFormatError("failed loading static spec", err, utils.Attribute{Key: "endpoint", Value: rpcEndpoint})
			errCh <- err
			return err
		}
	} else {
		err = utils.LavaFormatError("no static spec paths configured; smart router requires --static-spec-paths to load chain specs", nil, utils.Attribute{Key: "chainID", Value: chainID})
		errCh <- err
		return err
	}

	// Filter the relevant static providers.
	// IMPORTANT: filter on both ChainID *and* ApiInterface. A single chain (e.g. LAVA)
	// can expose several api-interfaces (rest, grpc, tendermintrpc); selecting only by
	// ChainID would let, say, the grpc endpoint pick a rest provider as its chain
	// tracker source. The chain tracker would then craft a grpc-shaped GET_BLOCKNUM
	// message (from the grpc chainParser) but dispatch it through the rest proxy,
	// which fails with "invalid message type in rest" and aborts startup.
	relevantStaticProviderList := []*lavasession.RPCStaticProviderEndpoint{}
	for _, staticProvider := range options.staticProvidersList {
		if staticProvider.ChainID == rpcEndpoint.ChainID &&
			staticProvider.ApiInterface == rpcEndpoint.ApiInterface {
			relevantStaticProviderList = append(relevantStaticProviderList, staticProvider)
		}
	}

	// Filter backup providers for this chain+interface (needed for policy derivation)
	relevantBackupProviderList := []*lavasession.RPCStaticProviderEndpoint{}
	for _, backupProvider := range options.backupProvidersList {
		if backupProvider.ChainID == rpcEndpoint.ChainID &&
			backupProvider.ApiInterface == rpcEndpoint.ApiInterface {
			relevantBackupProviderList = append(relevantBackupProviderList, backupProvider)
		}
	}

	if len(relevantStaticProviderList) == 0 && len(relevantBackupProviderList) == 0 {
		err = utils.LavaFormatError("no static or backup providers configured for chain", nil,
			utils.Attribute{Key: "chainID", Value: chainID})
		errCh <- err
		return err
	}

	// Auto-derive policy from BOTH static and backup providers' addons
	// This configures the extension parser and allowed addons based on what ALL providers support
	addonsMap := make(map[string]struct{})
	extensionsMap := make(map[string]struct{})

	// IMPORTANT: Always allow the default addon (empty string) for standard APIs
	// Without this, all standard requests without explicit addons will fail validation
	addonsMap[""] = struct{}{}

	// Scan static providers for addons
	for _, staticProvider := range relevantStaticProviderList {
		for _, nodeUrl := range staticProvider.NodeUrls {
			for _, addon := range nodeUrl.Addons {
				// Add the addon itself to policy
				addonsMap[addon] = struct{}{}
				// If provider has "archive" addon, also allow "archive" extension
				// This enables the archive retry mechanism to work correctly
				if addon == "archive" {
					extensionsMap["archive"] = struct{}{}
				}
				// Future addon->extension mappings can be added here
			}
		}
	}

	// Scan backup providers for addons (same logic as static providers)
	for _, backupProvider := range relevantBackupProviderList {
		for _, nodeUrl := range backupProvider.NodeUrls {
			for _, addon := range nodeUrl.Addons {
				addonsMap[addon] = struct{}{}
				if addon == "archive" {
					extensionsMap["archive"] = struct{}{}
				}
			}
		}
	}

	// Convert maps to slices for the policy struct
	addons := make([]string, 0, len(addonsMap))
	for addon := range addonsMap {
		addons = append(addons, addon)
	}
	extensions := make([]string, 0, len(extensionsMap))
	for ext := range extensionsMap {
		extensions = append(extensions, ext)
	}

	// Apply the derived policy to the chain parser if we found any addons or extensions
	if len(addons) > 0 || len(extensions) > 0 {
		policy := staticPolicy{
			addons:       addons,
			extensions:   extensions,
			apiInterface: rpcEndpoint.ApiInterface,
		}
		err = chainParser.SetPolicy(policy, chainID, rpcEndpoint.ApiInterface)
		if err != nil {
			utils.LavaFormatWarning("Failed to set auto-derived policy", err,
				utils.Attribute{Key: "chainID", Value: chainID},
				utils.Attribute{Key: "apiInterface", Value: rpcEndpoint.ApiInterface})
		} else {
			utils.LavaFormatInfo("Auto-derived policy from static providers",
				utils.Attribute{Key: "chainID", Value: chainID},
				utils.Attribute{Key: "apiInterface", Value: rpcEndpoint.ApiInterface},
				utils.Attribute{Key: "addons", Value: addons},
				utils.Attribute{Key: "extensions", Value: extensions})
		}
	}

	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	var optimizer *provideroptimizer.ProviderOptimizer

	// Create chain assets with mutex protection
	chainMutexes[chainID].Lock()
	defer chainMutexes[chainID].Unlock()

	// Create / Use existing optimizer
	// Smart-router serves each relay from a single selected provider (cross-validation
	// aside), so the optimizer's wanted-concurrency is always 1. The legacy
	// --concurrent-providers flag fed a write-only optimizer field and is removed.
	newOptimizer := provideroptimizer.NewProviderOptimizer(options.strategy, averageBlockTime, 1, smartRouterOptimizerQoSClient, chainID)
	newOptimizer.ConfigureUpstreamSelector(options.upstreamSelectorConfig)
	optimizer, loaded, err := optimizers.LoadOrStore(chainID, newOptimizer)
	if err != nil {
		errCh <- err
		return utils.LavaFormatError("failed loading optimizer", err, utils.LogAttr("endpoint", rpcEndpoint.Key()))
	}

	if !loaded && smartRouterOptimizerQoSClient != nil {
		// if this is a new optimizer, register it in the smartRouterOptimizerQoSClient
		smartRouterOptimizerQoSClient.RegisterOptimizer(optimizer, chainID)
	}

	// Create active subscription provider storage for each unique chain
	activeSubscriptionProvidersStorage := lavasession.NewActiveSubscriptionProvidersStorage()
	sessionManager := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, smartRouterMetricsManager, smartRouterIdentifier, activeSubscriptionProvidersStorage)

	// Set callback to get Lava blockchain block height for RelaySession.Epoch
	// Smart router doesn't connect to blockchain, so calculate approximate block height from epoch
	// Epoch duration is 15 minutes (900 seconds), and Lava block time is ~15 seconds
	// So each epoch is approximately 60 blocks (900 / 15)
	sessionManager.SetLavaBlockHeightCallback(func() int64 {
		currentEpoch := rpsr.epochTimer.GetCurrentEpoch()
		// Approximate blocks per epoch: epochDuration / averageBlockTime
		blocksPerEpoch := int64(rpsr.epochTimer.GetEpochDuration().Seconds() / 15) // 15 second Lava block time
		return int64(currentEpoch) * blocksPerEpoch
	})

	// Store session manager in router for epoch timer callbacks
	sessionManagerKey := rpcEndpoint.Key() // chainID-apiInterface
	rpsr.mu.Lock()
	rpsr.sessionManagers[sessionManagerKey] = sessionManager
	rpsr.mu.Unlock()

	// Helper function to convert provider endpoints to sessions
	convertProvidersToSessions := func(providerList []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		sessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider)
		// Every connection built below, so the readiness step at the end of this
		// function can run before any of them is reachable by a relay (MAG-2860).
		createdConnections := []lavasession.DirectRPCConnection{}
		for idx, provider := range providerList {
			// Only process providers matching this endpoint's API interface
			if provider.ApiInterface != rpcEndpoint.ApiInterface || provider.ChainID != rpcEndpoint.ChainID {
				continue
			}

			endpoints := []*lavasession.Endpoint{}
			for _, url := range expandInternalPaths(provider.NodeUrls, chainParser.GetAllInternalPaths(), subscriptionCollectionProbe(chainParser)) {
				extensions := map[string]struct{}{}
				for _, extension := range url.Addons {
					extensions[extension] = struct{}{}
				}

				// Create DirectRPCConnection for smart router (direct mode)
				// Use default parallel connections for HTTP connection pooling
				// Pass ApiInterface for proper protocol detection (bare host:port → gRPC when interface is gRPC)
				directConn, err := lavasession.NewDirectRPCConnection(
					ctx,
					url,
					uint(lavasession.MaximumStreamsOverASingleConnection),
					provider.ApiInterface, // Used for protocol detection when URL has no scheme
				)
				if err != nil {
					utils.LavaFormatWarning("failed to create direct RPC connection", err,
						utils.LogAttr("url", url.Url),
						utils.LogAttr("provider", provider.Name),
					)
					continue
				}

				utils.LavaFormatInfo("created direct RPC connection",
					utils.LogAttr("url", url.Url),
					utils.LogAttr("protocol", directConn.GetProtocol()),
					utils.LogAttr("provider", provider.Name),
				)
				createdConnections = append(createdConnections, directConn)

				endpoint := &lavasession.Endpoint{
					NetworkAddress:    url.Url,
					Enabled:           true,
					Addons:            extensions,
					Extensions:        extensions,
					InternalPath:      url.InternalPath,
					Connections:       nil,
					DirectConnections: []lavasession.DirectRPCConnection{directConn}, // Smart router uses direct RPC
				}
				endpoints = append(endpoints, endpoint)

				// Register endpoint with metrics manager for info metric visibility
				if smartRouterMetricsManager != nil {
					smartRouterMetricsManager.RegisterEndpoint(
						rpcEndpoint.ChainID,
						rpcEndpoint.ApiInterface,
						url.Url,       // raw URL — stored in endpoint_url label; used for URL->name resolution in ChainTracker callbacks
						provider.Name, // provider name — used as endpoint_id in all Prometheus metrics
					)
				}
			}

			// Skip provider entirely if every URL failed direct-connection creation.
			// Registering a provider with no usable endpoints would silently poison
			// the session manager: UpdateAllProviders makes it selectable, but any
			// relay attempt against it fails because there are no endpoints to dial.
			if len(endpoints) == 0 {
				utils.LavaFormatWarning("skipping static provider: all URL connections failed, no usable endpoints",
					nil,
					utils.LogAttr("provider", provider.Name),
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("apiInterface", provider.ApiInterface),
					utils.LogAttr("urlCount", len(provider.NodeUrls)),
				)
				continue
			}

			// Create provider session with static configuration.
			// If stake is specified in the static provider config, use it (ulava).
			// Otherwise keep stake=0 so CalcWeightsByStake applies the legacy static-provider boost.
			stake := provider.Stake
			if stake < 0 {
				stake = 0
			}
			stakeAmount := StaticProviderDummyStake
			if stake > 0 {
				stakeAmount = stake
			}
			providerEntry := lavasession.NewConsumerSessionWithProvider(
				provider.Name,
				endpoints,
				999999999, // High compute units for availability
				1,         // Fixed epoch (smart router doesn't track blockchain epochs)
				stakeAmount,
			)
			providerEntry.StaticProvider = true
			providerEntry.GroupLabel = provider.GroupLabel // cross-validation group-diversity label (may be empty)
			sessions[uint64(idx)] = providerEntry
		}

		// The readiness boundary (MAG-2860). Direct connections are lazy — a gRPC one
		// has made no network call at all when NewDirectRPCConnection returns — so
		// without this the first RELAY to an endpoint is what dials it and resolves
		// its protobuf descriptors. Reflection on a slow node outlasts a relay
		// timeout, and cross-validation needs N successes in one request, so that
		// first relay failing is the whole defect rather than a slow start.
		//
		// Here is the one place that covers every lifecycle which produces a
		// connection — boot, the failed-provider retry, epoch re-verification and
		// /debug/reset-pairing all funnel through this closure — and it runs before
		// the caller hands any of these sessions to UpdateAllProviders, which is what
		// makes an endpoint selectable.
		//
		// Bounded and never fatal, in keeping with MAG-2525: a connection that does
		// not finish is published anyway and falls back to on-demand resolution.
		lavasession.PrewarmDirectConnections(ctx, createdConnections, lavasession.DirectRPCPrewarmBudget)

		return sessions
	}

	// ============================================================================
	// PHASE 1: Provider Validation — both tiers, neither fatal
	// ============================================================================
	// Validate both tiers BEFORE converting to sessions or registering, so only
	// healthy providers ever reach the session manager.
	//
	// Neither tier is fatal (MAG-2525, and MAG-2532 for the backup-rescue case).
	// Provider health is a runtime condition, not a configuration error: the router
	// already tolerates every provider dropping while it is running — it stays up,
	// returns 5xx, and re-adopts providers as they recover. Boot now behaves the
	// same way instead of exiting, which turned a restart during a failover into an
	// outage precisely when a restart was most likely. The only fatal case is a
	// chain with nothing CONFIGURED, checked far above.
	//
	// What replaces the old fail-fast signal: a chain with no healthy provider in
	// either tier boots but reports unhealthy on its health endpoint (see the
	// relaysMonitor seeding below), so it is pulled from rotation rather than
	// silently accepting traffic it cannot serve.
	failedStaticSet, failedStaticEndpoints := validateProviderTier(
		ctx, relevantStaticProviderList, rpcEndpoint, chainParser, reverifyTierStatic, nil)
	failedBackupSet, failedBackupEndpoints := validateProviderTier(
		ctx, relevantBackupProviderList, rpcEndpoint, chainParser, reverifyTierBackup, nil)

	healthyStaticCount := len(relevantStaticProviderList) - len(failedStaticSet)
	healthyBackupCount := len(relevantBackupProviderList) - len(failedBackupSet)

	// chainServable drives the initial health verdict and the retry cadence.
	servingTier := metrics.ServingTier(healthyStaticCount, healthyBackupCount)
	chainServable := servingTier > metrics.ServingTierDark

	switch {
	case !chainServable:
		utils.LavaFormatWarning("ATTENTION: no healthy providers for endpoint — serving unavailable until one recovers", nil,
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("staticConfigured", len(relevantStaticProviderList)),
			utils.LogAttr("backupConfigured", len(relevantBackupProviderList)),
			utils.LogAttr("hint", "endpoint reports unhealthy; providers are retried in the background and adopted when they recover"),
		)
	case servingTier == metrics.ServingTierDegraded:
		utils.LavaFormatWarning("ATTENTION: no healthy static providers — serving from backup providers only", nil,
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("staticFailed", len(failedStaticSet)),
			utils.LogAttr("healthyBackups", healthyBackupCount),
			utils.LogAttr("hint", "degraded: static providers are retried in the background"),
		)
	default:
		utils.LavaFormatInfo("Providers validated for api-interface",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("healthyStatic", healthyStaticCount),
			utils.LogAttr("staticFailed", len(failedStaticSet)),
			utils.LogAttr("healthyBackups", healthyBackupCount),
			utils.LogAttr("backupFailed", len(failedBackupSet)),
		)
	}

	// Register the inputs updateEpoch needs every epoch tick: chain parser, the
	// convert closure built above (it carries ctx, rpcEndpoint,
	// smartRouterMetricsManager), and the full configured lists. There is no
	// separate goroutine — re-validation runs synchronously from the epoch
	// callback, bounded by SpecReVerifyConcurrency.
	rpsr.mu.Lock()
	rpsr.reverifyInputs[sessionManagerKey] = &chainReverifyInputs{
		chainParser:                chainParser,
		rpcEndpoint:                rpcEndpoint,
		convertProvidersToSessions: convertProvidersToSessions,
		configuredStatic:           relevantStaticProviderList,
		configuredBackup:           relevantBackupProviderList,
	}
	rpsr.mu.Unlock()

	// ============================================================================
	// Session Registration (after validation — only healthy providers)
	// ============================================================================
	// Filter to only healthy providers before converting to sessions.
	// This ensures the session manager and rpsr.providerSessions never contain
	// failed providers, so updateEpoch won't recreate sessions for dead nodes.
	healthyStaticProviders := relevantStaticProviderList
	if len(failedStaticSet) > 0 {
		healthyStaticProviders = make([]*lavasession.RPCStaticProviderEndpoint, 0, len(relevantStaticProviderList)-len(failedStaticSet))
		for _, p := range relevantStaticProviderList {
			if _, failed := failedStaticSet[p]; !failed {
				healthyStaticProviders = append(healthyStaticProviders, p)
			}
		}
	}

	healthyBackupProviders := relevantBackupProviderList
	if len(failedBackupSet) > 0 {
		healthyBackupProviders = make([]*lavasession.RPCStaticProviderEndpoint, 0, len(relevantBackupProviderList)-len(failedBackupSet))
		for _, p := range relevantBackupProviderList {
			if _, failed := failedBackupSet[p]; !failed {
				healthyBackupProviders = append(healthyBackupProviders, p)
			}
		}
	}

	// Convert only healthy providers to ConsumerSessionsWithProvider format
	providerSessions := convertProvidersToSessions(healthyStaticProviders)

	var backupProviderSessions map[uint64]*lavasession.ConsumerSessionsWithProvider
	if len(healthyBackupProviders) > 0 {
		backupProviderSessions = convertProvidersToSessions(healthyBackupProviders)
		utils.LavaFormatInfo("Configured backup providers for endpoint",
			utils.Attribute{Key: "chainID", Value: chainID},
			utils.Attribute{Key: "apiInterface", Value: rpcEndpoint.ApiInterface},
			utils.Attribute{Key: "backupCount", Value: len(backupProviderSessions)})
	}

	// Get current epoch for initial provider session setup
	currentEpoch := rpsr.epochTimer.GetCurrentEpoch()

	// Update PairingEpoch for all provider sessions to current epoch
	for _, providerSession := range providerSessions {
		providerSession.Lock.Lock()
		providerSession.PairingEpoch = currentEpoch
		providerSession.Lock.Unlock()
	}
	for _, backupSession := range backupProviderSessions {
		backupSession.Lock.Lock()
		backupSession.PairingEpoch = currentEpoch
		backupSession.Lock.Unlock()
	}

	// Register with session manager — one call, correct from the start
	err = sessionManager.UpdateAllProviders(currentEpoch, providerSessions, backupProviderSessions)
	if err != nil {
		errCh <- err
		return utils.LavaFormatError("failed updating static providers", err)
	}

	// Store provider sessions and failed providers for epoch updates and background retry
	rpsr.mu.Lock()
	rpsr.providerSessions[sessionManagerKey] = providerSessions
	if len(backupProviderSessions) > 0 {
		rpsr.backupProviderSessions[sessionManagerKey] = backupProviderSessions
	}
	if len(failedStaticEndpoints) > 0 {
		rpsr.failedStaticProviders[sessionManagerKey] = failedStaticEndpoints
	}
	if len(failedBackupEndpoints) > 0 {
		rpsr.failedBackupProviders[sessionManagerKey] = failedBackupEndpoints
	}
	rpsr.publishServingTierLocked(sessionManagerKey)
	rpsr.mu.Unlock()

	// Launch background retry for whichever tiers had failures. Backups are included
	// because a chain can now be serving on them alone — or waiting on them to be the
	// thing that lets it serve at all.
	if len(failedStaticEndpoints) > 0 || len(failedBackupEndpoints) > 0 {
		failedNames := make([]string, 0, len(failedStaticEndpoints)+len(failedBackupEndpoints))
		for _, p := range failedStaticEndpoints {
			failedNames = append(failedNames, p.Name)
		}
		for _, p := range failedBackupEndpoints {
			failedNames = append(failedNames, p.Name)
		}
		utils.LavaFormatInfo("Launching background retry goroutine for failed providers",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("failedStatic", len(failedStaticEndpoints)),
			utils.LogAttr("failedBackup", len(failedBackupEndpoints)),
			utils.LogAttr("failedProviders", failedNames),
			utils.LogAttr("initialRetryInterval", retryIntervalFor(!chainServable).String()),
		)
		go rpsr.retryFailedProviders(ctx, sessionManagerKey, chainParser, rpcEndpoint, convertProvidersToSessions)
	}

	var relaysMonitor *metrics.RelaysMonitor
	if options.cmdFlags.RelaysHealthEnableFlag {
		relaysMonitor = metrics.NewRelaysMonitor(options.cmdFlags.RelaysHealthIntervalFlag, rpcEndpoint.ChainID, rpcEndpoint.ApiInterface)
		// Boot validation already knows whether anything can serve, so don't let the
		// monitor's optimistic default claim health for a chain that came up dark.
		relaysMonitor.SeedInitialHealth(chainServable)
		relaysMonitorAggregator.RegisterRelaysMonitor(rpcEndpoint.String(), relaysMonitor)
	}

	rpcSmartRouterServer := &RPCSmartRouterServer{}

	// Create WebSocket subscription manager. Interface-typed so a chain with no
	// WebSocket endpoint gets the NoOp implementation instead of a real one.
	var wsSubscriptionManager chainlib.WSSubscriptionManager

	// Collect WebSocket-capable endpoints for direct subscriptions.
	// Primary tier serves all selections; backup tier is consulted on primary exhaustion.
	// Tiers hold only healthy providers — republishSubscriptionEndpointsLocked keeps them
	// in step with the live pairing from here on.
	wsEndpoints := collectWSEndpoints(healthyStaticProviders, "primary")
	wsBackupEndpoints := collectWSEndpoints(healthyBackupProviders, "backup")

	// Whether a real manager is installed is decided from the CONFIGURED providers, not
	// the healthy ones. Since MAG-2525 a chain can boot with nothing healthy, and keying
	// this off the healthy lists would install the NoOp manager permanently: every
	// eth_subscribe would fail for the process lifetime even after every provider
	// recovered and HTTP relays were serving, because nothing re-creates the manager.
	// A Direct manager with empty tiers returns the same "no endpoint" error today and
	// can be filled in on recovery.
	wsConfigured := len(collectWSEndpoints(relevantStaticProviderList, "")) > 0 ||
		len(collectWSEndpoints(relevantBackupProviderList, "")) > 0

	if wsConfigured {
		directWSManager := NewDirectWSSubscriptionManager(
			smartRouterMetricsManager,
			spectypes.APIInterfaceJsonRPC, // WebSocket subscriptions use JSON-RPC
			rpcEndpoint.ChainID,
			rpcEndpoint.ApiInterface,
			wsEndpoints,
			wsBackupEndpoints,
			optimizer, // Pass optimizer for endpoint selection
			nil,       // Use default WebSocket config (configurable via CLI flags later)
		)
		// Start background cleanup goroutine
		directWSManager.Start(ctx)
		wsSubscriptionManager = directWSManager
		utils.LavaFormatInfo("Using DirectWSSubscriptionManager for direct WebSocket subscriptions",
			utils.LogAttr("chainID", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("wsEndpointCount", len(wsEndpoints)),
			utils.LogAttr("wsBackupEndpointCount", len(wsBackupEndpoints)),
			utils.LogAttr("optimizerEnabled", optimizer != nil),
		)
	} else {
		// Nothing ws:// or wss:// in the config at all — no amount of recovery can
		// produce a WebSocket endpoint here, so the NoOp manager is permanent by
		// definition and its error is the honest answer.
		wsSubscriptionManager = NewNoOpWSSubscriptionManager(rpcEndpoint.ChainID, rpcEndpoint.ApiInterface)
		utils.LavaFormatInfo("No WebSocket endpoints configured for direct subscriptions",
			utils.LogAttr("chainID", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("hint", "Add ws:// or wss:// URLs to static-providers-list to enable subscriptions"),
		)
	}

	// This supports Cosmos Event Streaming, Solana Geyser, and other gRPC streaming protocols.
	// Primary tier from healthy static providers; backup tier from healthy backup providers.
	var grpcEndpoints, grpcBackupEndpoints []*common.NodeUrl
	grpcConfigured := false
	if rpcEndpoint.ApiInterface == spectypes.APIInterfaceGrpc {
		grpcEndpoints = collectGRPCEndpoints(healthyStaticProviders, "primary")
		grpcBackupEndpoints = collectGRPCEndpoints(healthyBackupProviders, "backup")
		// Same reasoning as wsConfigured above: keyed off the configured providers so a
		// dark boot still gets a manager. Leaving grpcSubscriptionManager nil would also
		// disable gRPC reflection permanently (GetGRPCReflectionConnection nil-checks it).
		grpcConfigured = len(collectGRPCEndpoints(relevantStaticProviderList, "")) > 0 ||
			len(collectGRPCEndpoints(relevantBackupProviderList, "")) > 0
	}

	if grpcConfigured {
		grpcSubManager := NewDirectGRPCSubscriptionManager(
			smartRouterMetricsManager, // Metrics manager for tracking
			rpcEndpoint.ChainID,
			rpcEndpoint.ApiInterface,
			grpcEndpoints,
			grpcBackupEndpoints,
			optimizer, // Pass optimizer for endpoint selection (same as WS manager)
			nil,       // Use default GRPCStreamingConfig
		)
		// Start background cleanup goroutine
		grpcSubManager.Start(ctx)
		rpcSmartRouterServer.grpcSubscriptionManager = grpcSubManager
		utils.LavaFormatInfo("Using DirectGRPCSubscriptionManager for gRPC streaming subscriptions",
			utils.LogAttr("chainID", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("grpcEndpointCount", len(grpcEndpoints)),
			utils.LogAttr("grpcBackupEndpointCount", len(grpcBackupEndpoints)),
			utils.LogAttr("optimizerEnabled", optimizer != nil),
		)
	}

	// The redundant global ChainTracker (bound to healthyStaticProviders[0]) was removed in
	// MAG-2160 / Topic C: the per-chain tip now comes from ChainState (fed by per-endpoint
	// relay + poll observations, with strict-majority consensus), constructed inside
	// ServeRPCRequests. No single-node tip, no fire-and-forget poller per pod.

	// Convert smartRouterIdentifier string to empty sdk.AccAddress for smart router
	err = rpcSmartRouterServer.ServeRPCRequests(ctx, rpcEndpoint, chainParser, sessionManager, options.cache, rpcSmartRouterMetrics, relaysMonitor, options.cmdFlags, options.stateShare, wsSubscriptionManager, smartRouterMetricsManager)
	if err != nil {
		err = utils.LavaFormatError("failed serving rpc requests", err, utils.Attribute{Key: "endpoint", Value: rpcEndpoint})
		errCh <- err
		return err
	}

	// Logged AFTER ServeRPCRequests returns, not before it (MAG-3022). Everything ServeRPCRequests
	// validates synchronously — the cross-validation startup guards, the chain listener construction —
	// happens before this line now, so the message is only written once the endpoint has actually been
	// brought up. Previously it was written first, which meant any synchronous failure inside
	// ServeRPCRequests produced a router that announced itself and then exited.
	//
	// Not a bind confirmation: ServeRPCRequests starts the listener with `go chainListener.Serve(...)`,
	// so the socket may still be a moment away. It does mean the endpoint was accepted and serving is
	// underway, which is what the line is read as.
	utils.LavaFormatInfo("RPCSmartRouter Listening", utils.Attribute{Key: "endpoints", Value: rpcEndpoint.String()})

	// Store server reference for per-endpoint ChainTracker cleanup on epoch updates
	rpsr.mu.Lock()
	rpsr.rpcServers[sessionManagerKey] = rpcSmartRouterServer
	rpsr.mu.Unlock()

	return nil
}

func ParseEndpoints(viper_endpoints *viper.Viper) (endpoints []*lavasession.RPCEndpoint, err error) {
	err = viper_endpoints.UnmarshalKey(common.EndpointsConfigName, &endpoints)
	if err != nil {
		utils.LavaFormatFatal("could not unmarshal endpoints", err, utils.Attribute{Key: "viper_endpoints", Value: viper_endpoints.AllSettings()})
	}
	for _, endpoint := range endpoints {
		if endpoint.HealthCheckPath == "" {
			endpoint.HealthCheckPath = common.DEFAULT_HEALTH_PATH
		}
	}
	return endpoints, err
}

func CreateRPCSmartRouterCobraCommand() *cobra.Command {
	cmdRPCSmartRouter := &cobra.Command{
		Use:   "rpcsmartrouter [config-file] | { {listen-ip:listen-port spec-chain-id api-interface} ... }",
		Short: `rpcsmartrouter sets up a centralized server with static providers to perform api requests`,
		// On startup failure (now only configuration errors — a chain with no
		// provider configured, an unresolvable spec — since MAG-2525 made provider
		// health non-fatal) cobra otherwise dumps the full --help text after the
		// error line, swamping kubectl logs in a CrashLoopBackOff. Operators need
		// to see the error, not the flag catalogue.
		SilenceUsage: true,
		Long: `rpcsmartrouter sets up a centralized server with static and backup providers to perform api requests through the lava protocol.
		This is the smart router mode that uses pre-configured static providers instead of dynamically discovering providers on-chain.
		if no arguments are passed, assumes default config file: ` + DefaultRPCSmartRouterFileName + `
		if one argument is passed, it is the config to load. An absolute path names the file
		outright; anything else — a relative path, or a bare name — is looked up in the
		local running directory, ./config, then ` + lavaDefaultNodeHome + `.
		An argument without a recognized extension has the supported ones appended, so
		"akash" and "config/akash" both find config/akash.yml.
		`,
		Example: `required: --direct-rpc ...
rpcsmartrouter <flags>
rpcsmartrouter rpcsmartrouter_conf <flags>
rpcsmartrouter 127.0.0.1:3333 OSMOSIS tendermintrpc 127.0.0.1:3334 OSMOSIS rest <flags>
rpcsmartrouter smartrouter_examples/smartrouter_eth.yml --cache-be "127.0.0.1:7778" [--debug-relays] --log_level <debug|warn|...>`,
		Args: func(cmd *cobra.Command, args []string) error {
			// Optionally run one of the validators provided by cobra
			if err := cobra.RangeArgs(0, 1)(cmd, args); err == nil {
				// zero or one argument is allowed
				return nil
			}
			if len(args)%len(Yaml_config_properties) != 0 {
				return fmt.Errorf("invalid number of arguments, either its a single config file or repeated groups of 3 HOST:PORT chain-id api-interface")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.LavaFormatInfo(common.ProcessStartLogText)
			common.ValidateAndCapMinRelayTimeout()

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			var err error
			// set viper — the argument is a config file path (absolute or relative) or a
			// bare name resolved against the search paths; see config_source.go.
			configTarget, configIsFile := pointViperAtConfig(args)

			// Bind all cobra flags to viper so viper.GetString/GetBool works.
			// Previously Cosmos SDK's AddTxFlagsToCmd did this automatically.
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			// Apply the probe debug toggle to its atomic global once, at startup (after flag parse).
			lavasession.SetDebugProbes(viper.GetBool(DebugProbesFlagName))

			// set log format
			logFormat := viper.GetString("log-format")
			utils.JsonFormat = logFormat == "json"
			// set rolling log.
			closeLoggerOnFinish := common.SetupRollingLogger()
			defer closeLoggerOnFinish()

			utils.LavaFormatInfo("RPCSmartRouter started:", utils.Attribute{Key: "args", Value: strings.Join(args, ",")})

			// setting the insecure option on provider dial, this should be used in development only!
			lavasession.AllowInsecureConnectionToProviders = viper.GetBool(lavasession.AllowInsecureConnectionToProvidersFlag)
			if lavasession.AllowInsecureConnectionToProviders {
				utils.LavaFormatWarning("AllowInsecureConnectionToProviders is set to true, this should be used only in development", nil, utils.Attribute{Key: lavasession.AllowInsecureConnectionToProvidersFlag, Value: lavasession.AllowInsecureConnectionToProviders})
			}

			var rpcEndpoints []*lavasession.RPCEndpoint
			var viper_endpoints *viper.Viper
			if len(args) > 1 {
				viper_endpoints, err = common.ParseEndpointArgs(args, Yaml_config_properties, common.EndpointsConfigName)
				if err != nil {
					return utils.LavaFormatError("invalid endpoints arguments", err, utils.Attribute{Key: "endpoint_strings", Value: strings.Join(args, "")})
				}
				viper.MergeConfigMap(viper_endpoints.AllSettings())
				err := viper.SafeWriteConfigAs(DefaultRPCSmartRouterFileName)
				if err != nil {
					utils.LavaFormatInfo("did not create new config file, if it's desired remove the config file", utils.Attribute{Key: "file_name", Value: viper.ConfigFileUsed()})
				} else {
					utils.LavaFormatInfo("created new config file", utils.Attribute{Key: "file_name", Value: DefaultRPCSmartRouterFileName})
				}
			} else if err = viper.ReadInConfig(); err != nil {
				// A missing config file is the most common operator mistake (e.g.
				// running `smartrouter` with no args in the wrong directory). Treat
				// it as a clean, actionable error instead of a fatal stack-trace
				// dump — reserve the loud path for genuinely unexpected read
				// failures like malformed YAML.
				if isConfigNotFound(err) {
					return utils.LavaFormatError(
						configNotFoundMessage(configTarget, configIsFile),
						err,
						configLocationAttributes(configTarget, configIsFile)...,
					)
				}
				utils.LavaFormatFatal("could not load config file", err, utils.Attribute{Key: "expected_config_name", Value: viper.ConfigFileUsed()})
			} else {
				utils.LavaFormatInfo("read config file successfully", utils.Attribute{Key: "expected_config_name", Value: viper.ConfigFileUsed()})
			}

			// consistency-relief: set the process-wide consistency override AFTER the config
			// file is merged into viper (above), so the flag resolves from CLI *or* config.yml
			// (viper precedence: CLI-if-passed > config file > default). Still set before any
			// consistency config is built. Zero = no relief; out-of-range values warn-and-revert
			// to the built-in default (not silent clamp).
			if f := viper.GetInt(relaycore.ConsistencyBlockGapFactorFlagName); f != 0 {
				if f < 2 || f > 8 {
					utils.LavaFormatWarning("--"+relaycore.ConsistencyBlockGapFactorFlagName+" out of allowed range [2,8]; reverting to default", nil, utils.LogAttr("provided", f))
				} else {
					relaycore.ConsistencyBlockGapFactorOverride = int64(f)
				}
			}
			if relaycore.ConsistencyBlockGapFactorOverride != 0 {
				utils.LavaFormatInfo("consistency-relief active",
					utils.LogAttr("consistencyBlockGapFactor", relaycore.ConsistencyBlockGapFactorOverride))
			}

			rpcEndpoints, err = ParseEndpoints(viper.GetViper())
			if err != nil || len(rpcEndpoints) == 0 {
				return utils.LavaFormatError("invalid endpoints definition", err)
			}

			// Reject a self-contradictory cross-validation config here, alongside the endpoints check,
			// rather than per-endpoint inside ServeRPCRequests (MAG-3022). Both are pure config-shape
			// checks and both are decidable from viper alone. Doing it here means the process exits
			// before the metrics port binds, before any provider is dialed, and before the router logs
			// that it is listening — so a bad config is a clean startup error instead of a crash loop
			// from a router that has already announced itself.
			if err := PreflightValidateCrossValidationConfig(viper.GetViper()); err != nil {
				return utils.LavaFormatError("invalid cross-validation configuration", err)
			}

			// Smart router doesn't need blockchain chain ID
			utils.LavaFormatInfo("Running Smart Router")

			logLevel, err := cmd.Flags().GetString("log-level")
			if err != nil {
				utils.LavaFormatFatal("failed to read log level flag", err)
			}
			utils.SetGlobalLoggingLevel(logLevel)

			test_mode, err := cmd.Flags().GetBool(common.TestModeFlagName)
			if err != nil {
				utils.LavaFormatFatal("failed to read test_mode flag", err)
			}
			ctx = context.WithValue(ctx, common.Test_mode_ctx_key{}, test_mode)
			// check if the command includes --pprof-address
			pprofAddressFlagUsed := cmd.Flags().Lookup("pprof-address").Changed
			if pprofAddressFlagUsed {
				// get pprof server ip address (default value: "")
				pprofServerAddress, err := cmd.Flags().GetString("pprof-address")
				if err != nil {
					utils.LavaFormatFatal("failed to read pprof address flag", err)
				}

				// start pprof HTTP server
				err = performance.StartPprofServer(pprofServerAddress)
				if err != nil {
					return utils.LavaFormatError("failed to start pprof HTTP server", err)
				}
			}
			// check if the command includes --pyroscope-address
			pyroscopeAddressFlagUsed := cmd.Flags().Lookup(performance.PyroscopeAddressFlagName).Changed
			if pyroscopeAddressFlagUsed {
				pyroscopeServerAddress, err := cmd.Flags().GetString(performance.PyroscopeAddressFlagName)
				if err != nil {
					utils.LavaFormatFatal("failed to read pyroscope address flag", err)
				}
				pyroscopeAppName, err := cmd.Flags().GetString(performance.PyroscopeAppNameFlagName)
				if err != nil || pyroscopeAppName == "" {
					pyroscopeAppName = "smartrouter"
				}
				mutexProfileFraction, err := cmd.Flags().GetInt(performance.PyroscopeMutexProfileFractionFlagName)
				if err != nil {
					mutexProfileFraction = performance.DefaultMutexProfileFraction
				}
				blockProfileRate, err := cmd.Flags().GetInt(performance.PyroscopeBlockProfileRateFlagName)
				if err != nil {
					blockProfileRate = performance.DefaultBlockProfileRate
				}
				tagsStr, _ := cmd.Flags().GetString(performance.PyroscopeTagsFlagName)
				tags := performance.ParseTags(tagsStr)
				err = performance.StartPyroscope(pyroscopeAppName, pyroscopeServerAddress, mutexProfileFraction, blockProfileRate, tags)
				if err != nil {
					return utils.LavaFormatError("failed to start pyroscope profiler", err)
				}
			}

			// Parse direct RPC endpoints
			var directRPCEndpoints []*lavasession.RPCStaticProviderEndpoint
			directRPCConfigKey := common.DirectRPCConfigName
			if viper.IsSet(directRPCConfigKey) {
				directRPCEndpoints, err = ParseStaticProviderEndpoints(viper.GetViper(), directRPCConfigKey)
				if err != nil {
					return utils.LavaFormatError("invalid direct-rpc endpoints definition", err)
				}
				for _, endpoint := range directRPCEndpoints {
					utils.LavaFormatInfo("Direct RPC Endpoint:",
						utils.Attribute{Key: "Name", Value: endpoint.Name},
						utils.Attribute{Key: "Stake", Value: endpoint.Stake},
						utils.Attribute{Key: "Urls", Value: endpoint.NodeUrls},
						utils.Attribute{Key: "Chain ID", Value: endpoint.ChainID},
						utils.Attribute{Key: "API Interface", Value: endpoint.ApiInterface})
				}
			}

			// Parse backup direct RPC endpoints
			var backupDirectRPCEndpoints []*lavasession.RPCStaticProviderEndpoint
			backupConfigKey := common.BackupDirectRPCConfigName
			if viper.IsSet(backupConfigKey) {
				utils.LavaFormatInfo("Backup direct-rpc config found", utils.Attribute{Key: "configKey", Value: backupConfigKey})
				backupDirectRPCEndpoints, err = ParseStaticProviderEndpoints(viper.GetViper(), backupConfigKey)
				if err != nil {
					return utils.LavaFormatError("invalid backup-direct-rpc endpoints definition", err)
				}
				for _, endpoint := range backupDirectRPCEndpoints {
					utils.LavaFormatInfo("Backup Direct RPC Endpoint:",
						utils.Attribute{Key: "Name", Value: endpoint.Name},
						utils.Attribute{Key: "Urls", Value: endpoint.NodeUrls},
						utils.Attribute{Key: "Chain ID", Value: endpoint.ChainID},
						utils.Attribute{Key: "API Interface", Value: endpoint.ApiInterface})
				}
			}

			// Refuse to start on a config that cannot be routed unambiguously (MAG-2724). A
			// provider's name is its routing identity — the key of csm.pairing and of the retry
			// skip-list — so two providers sharing one on a chain+api-interface collapse into a
			// single entry: one upstream serves everything, the other sits idle, and setting that
			// name aside after a failure leaves nothing to retry against.
			//
			// Both lists are checked together, which is the only complete check: static and backup
			// land in separate maps but are looked up across both by address, so a name shared
			// between the two is as ambiguous as one shared within either. Failing here means it
			// fails before any listener binds, and the error names every collision so one edit to
			// the config fixes it.
			if err := lavasession.ValidateUniqueProviderNames(directRPCEndpoints, backupDirectRPCEndpoints); err != nil {
				return utils.LavaFormatError("invalid direct-rpc endpoints definition", err)
			}

			if len(directRPCEndpoints) == 0 {
				return utils.LavaFormatError(
					"smart router requires direct-rpc endpoints configuration",
					nil,
					utils.Attribute{Key: "hint", Value: "add 'direct-rpc' section to config file"},
				)
			}

			for _, endpoint := range rpcEndpoints {
				hasDirectRPC := false
				for _, directEndpoint := range directRPCEndpoints {
					if directEndpoint.ChainID == endpoint.ChainID &&
						directEndpoint.ApiInterface == endpoint.ApiInterface {
						hasDirectRPC = true
						break
					}
				}

				if !hasDirectRPC {
					return utils.LavaFormatError(
						"no direct-rpc endpoints configured for listener",
						nil,
						utils.Attribute{Key: "chainID", Value: endpoint.ChainID},
						utils.Attribute{Key: "apiInterface", Value: endpoint.ApiInterface},
						utils.Attribute{Key: "hint", Value: "add endpoint in 'direct-rpc' section"},
					)
				}
			}

			rpcSmartRouter := RPCSmartRouter{}
			utils.LavaFormatInfo("smart-router Binary Version: " + version.Version)
			rand.InitRandomSeed()

			var cache *performance.Cache = nil
			// viper (not cmd.Flags) so --cache-be can also come from the config file.
			if cacheAddr := viper.GetString(performance.CacheFlagName); cacheAddr != "" {
				var err error
				cache, err = performance.InitCache(ctx, cacheAddr)
				if err != nil {
					utils.LavaFormatError("Failed To Connect to cache at address", err, utils.Attribute{Key: "address", Value: cacheAddr})
				} else {
					utils.LavaFormatInfo("cache service connected", utils.Attribute{Key: "address", Value: cacheAddr})
				}
			}
			if strategyFlag.Strategy != provideroptimizer.StrategyBalanced {
				utils.LavaFormatInfo("Working with selection strategy: " + strategyFlag.String())
			}

			analyticsServerAddresses := AnalyticsServerAddresses{
				MetricsListenAddress:   viper.GetString(metrics.MetricsListenFlagName),
				UsageOTelEnabled:       viper.GetBool(metrics.UsageOTelEnabledFlagName),
				UsageOTelEndpoint:      viper.GetString(metrics.UsageOTelEndpointFlagName),
				UsageOTelInsecure:      viper.GetBool(metrics.UsageOTelInsecureFlagName),
				UsageOTelQueueSize:     viper.GetInt(metrics.UsageOTelQueueSizeFlagName),
				UsageOTelBatchSize:     viper.GetInt(metrics.UsageOTelBatchSizeFlagName),
				UsageOTelFlushInterval: viper.GetDuration(metrics.UsageOTelFlushIntervalFlagName),
				UsageOTelExportTimeout: viper.GetDuration(metrics.UsageOTelExportTimeoutFlagName),
				UsageOTelServiceName:   viper.GetString(metrics.UsageOTelServiceNameFlagName),
				UsageOTelInstanceID:    viper.GetString(metrics.UsageOTelInstanceIDFlagName),
			}

			if err := scoreutils.SetProbeUpdateWeight(viper.GetFloat64(common.ProbeUpdateWeightFlagName)); err != nil {
				return err
			}
			upstreamSelectorConfig, err := resolveSelectionWeights(cmd.Flags())
			if err != nil {
				return err
			}
			upstreamSelectorConfig.MinSelectionChance = viper.GetFloat64(common.ProviderOptimizerMinSelectionChance)
			upstreamSelectorConfig.Strategy = strategyFlag.Strategy

			selectionMode, err := provideroptimizer.ParseSelectionMode(viper.GetString(common.ProviderOptimizerSelectionMode))
			if err != nil {
				return err
			}
			upstreamSelectorConfig.SelectionMode = selectionMode
			if selectionMode != provideroptimizer.SelectionModeWeightedRandom {
				utils.LavaFormatInfo("Working with provider selection mode: " + selectionMode.String())
			}

			// RPCSmartRouter always runs in standalone mode
			epochDuration := viper.GetDuration(common.EpochDurationFlag)
			if epochDuration == 0 {
				epochDuration = common.StandaloneEpochDuration // 15 minutes default for standalone
				utils.LavaFormatInfo("RPCSmartRouter: using default epoch duration for standalone mode",
					utils.LogAttr("epochDuration", epochDuration),
				)
			}

			consumerPropagatedFlags := common.ConsumerCmdFlags{
				HeadersFlag:              viper.GetString(common.CorsHeadersFlag),
				CredentialsFlag:          viper.GetString(common.CorsCredentialsFlag),
				OriginFlag:               viper.GetString(common.CorsOriginFlag),
				MethodsFlag:              viper.GetString(common.CorsMethodsFlag),
				ExposeHeadersFlag:        viper.GetString(common.CorsExposeHeadersFlag),
				CDNCacheDuration:         viper.GetString(common.CDNCacheDurationFlag),
				RelaysHealthEnableFlag:   viper.GetBool(common.RelaysHealthEnableFlag),
				RelaysHealthIntervalFlag: viper.GetDuration(common.RelayHealthIntervalFlag),
				DebugRelays:              viper.GetBool(DebugRelaysFlagName),
				StaticSpecPaths:          viper.GetStringSlice(common.UseStaticSpecFlag),
				GitHubToken:              viper.GetString(common.GitHubTokenFlag),
				GitLabToken:              viper.GetString(common.GitLabTokenFlag),
				EpochDuration:            epochDuration,
				EnableSelectionStats:     viper.GetBool(common.EnableSelectionStatsHeaderFlag),
				DebugAddress:             viper.GetString("debug-address"),
				ResponseCompression:      viper.GetString(common.ResponseCompressionFlag),
				ShutdownGracePeriod:      viper.GetDuration(common.ShutdownGracePeriodFlag),
			}

			rpcSmartRouterSharedState := viper.GetBool(common.SharedStateFlag)

			// Initialise OTel tracing. Standard OTel SDK environment variables drive
			// all configuration (endpoint, sampler, service name, etc.); see the
			// tracing.TraceBodyFlag doc-comment for the full list. The SDK is enabled
			// unless OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none; when no OTLP
			// endpoint is configured the SDK falls back to the spec default
			// (localhost:4317 for gRPC, localhost:4318 for HTTP).
			traceCfg := tracing.TraceConfig{
				TraceBody: viper.GetBool(tracing.TraceBodyFlag),
			}
			traceManager, err := tracing.New(ctx, traceCfg)
			if err != nil {
				return err
			}
			defer traceManager.Shutdown()

			err = rpcSmartRouter.Start(ctx, &rpcSmartRouterStartOptions{
				rpcEndpoints:             rpcEndpoints,
				cache:                    cache,
				strategy:                 strategyFlag.Strategy,
				analyticsServerAddresses: analyticsServerAddresses,
				cmdFlags:                 consumerPropagatedFlags,
				stateShare:               rpcSmartRouterSharedState,
				staticProvidersList:      directRPCEndpoints,
				backupProvidersList:      backupDirectRPCEndpoints,
				upstreamSelectorConfig:   upstreamSelectorConfig,
			})
			if err != nil {
				return err
			}

			<-ctx.Done()
			// Restore default signal handling so a second SIGINT/SIGTERM during
			// the drain phase force-terminates the process instead of being
			// swallowed by NotifyContext.
			cancel()
			rpcSmartRouter.Stop(consumerPropagatedFlags.ShutdownGracePeriod)

			return nil
		},
	}

	// RPCSmartRouter command flags - no blockchain flags needed
	cmdRPCSmartRouter.Flags().Bool(lavasession.AllowInsecureConnectionToProvidersFlag, false, "allow insecure provider-dialing. used for development and testing")
	cmdRPCSmartRouter.Flags().String(common.ResponseCompressionFlag, common.DefaultResponseCompression, "client-facing response compression: gzip (default), brotli, or off")
	cmdRPCSmartRouter.Flags().Uint64Var(&lavasession.MaximumStreamsOverASingleConnection, lavasession.MaximumStreamsOverASingleConnectionFlag, lavasession.DefaultMaximumStreamsOverASingleConnection, "maximum number of parallel streams over a single provider connection")
	cmdRPCSmartRouter.Flags().Bool(common.TestModeFlagName, false, "test mode sends dummy data and prints all metadata in listeners")
	cmdRPCSmartRouter.Flags().String(performance.PprofAddressFlagName, "", "pprof server address, used for code profiling")
	cmdRPCSmartRouter.Flags().String("debug-address", "", "debug HTTP server for integration tests, e.g. :9999 — exposes /debug/time-warp (QoS clock) and /debug/chain-state-time-warp (per-chain ChainState TTL/staleness)")
	if err := viper.BindPFlag("debug-address", cmdRPCSmartRouter.Flags().Lookup("debug-address")); err != nil {
		utils.LavaFormatFatal("failed binding debug-address flag", err)
	}
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeAddressFlagName, "", "pyroscope server address for continuous profiling (e.g., http://pyroscope:4040)")
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeAppNameFlagName, "smartrouter", "pyroscope application name for identifying this service")
	cmdRPCSmartRouter.Flags().Int(performance.PyroscopeMutexProfileFractionFlagName, performance.DefaultMutexProfileFraction, "mutex profile sampling rate (1 in N mutex events)")
	cmdRPCSmartRouter.Flags().Int(performance.PyroscopeBlockProfileRateFlagName, performance.DefaultBlockProfileRate, "block profile rate in nanoseconds (1 records all blocking events)")
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeTagsFlagName, "", "comma-separated list of tags in key=value format (e.g., instance=router-1,region=us-east)")
	cmdRPCSmartRouter.Flags().String(performance.CacheFlagName, "", "address for a cache server to improve performance")
	cmdRPCSmartRouter.Flags().Int(relaycore.ConsistencyBlockGapFactorFlagName, 0, "consistency-relief: widen the consistency endpoint-lag gate (blockLagForQosSync x factor; default 2). Allowed [2,8]; out-of-range reverts to default.")
	// Block-hash polling (fork detection) is OFF by default — see EnableForkDetectionFlagName
	// for why. Process-wide, which matters because one process can serve several chains (see
	// smartrouter_multichain.yml): there is no per-chain form of this switch, so a deployment
	// that wants fork detection on exactly one chain has to run that chain in its own process.
	cmdRPCSmartRouter.Flags().Bool(endpointstate.EnableForkDetectionFlagName, false, "turn ON per-endpoint block-hash polling (fork detection). OFF by default: the chain tracker then asks each upstream only for its latest block. Process-wide -- it applies to EVERY chain this process serves and cannot be scoped to one of them. The live state per endpoint is reported as HashPolling on /debug/endpoint-state, and the request volume as rpc_endpoint_tracker_requests_total.")
	// Dedicated-poll cadence. A ratio rather than an absolute interval, so ONE value stays
	// correct across every chain this process serves (each divides its own spec block time).
	// Validation lives in endpointstate so config.yml and embedded servers get it too.
	// Process-wide in the same sense as the flag above.
	cmdRPCSmartRouter.Flags().Float64(endpointstate.PollDivisorFlagName, 0, fmt.Sprintf("polling-relief: per-endpoint chain tracker polls every avgBlockTime/divisor (default %g). Below 1 polls SLOWER than the chain produces blocks — %g is one poll per four block times, the largest relief available. Allowed [%g,%g]; out-of-range reverts to default. Applies to EVERY chain this process serves.", endpointstate.DefaultPollDivisor, endpointstate.MinPollDivisor, endpointstate.MinPollDivisor, endpointstate.MaxPollDivisor))
	cmdRPCSmartRouter.Flags().Var(&strategyFlag, "strategy", fmt.Sprintf("the strategy to use to pick providers (%s)", strings.Join(strategyNames, "|")))
	defaultSelectorConfig := provideroptimizer.DefaultUpstreamSelectorConfig()
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerAvailabilityWeight, defaultSelectorConfig.AvailabilityWeight, "weight assigned to provider availability when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerLatencyWeight, defaultSelectorConfig.LatencyWeight, "weight assigned to provider latency when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerSyncWeight, defaultSelectorConfig.SyncWeight, "weight assigned to provider sync freshness when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerStakeWeight, defaultSelectorConfig.StakeWeight, "weight assigned to provider stake when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerMinSelectionChance, defaultSelectorConfig.MinSelectionChance, "minimum selection probability for any provider regardless of score")
	cmdRPCSmartRouter.Flags().String(common.ProviderOptimizerSelectionMode, defaultSelectorConfig.SelectionMode.String(), fmt.Sprintf("how the winner is picked from the scored providers (%s): weighted_random draws proportionally to score, best always takes the highest scorer", strings.Join(provideroptimizer.SelectionModeNames(), "|")))
	cmdRPCSmartRouter.Flags().String(common.ProviderOptimizerSelectionPriority, provideroptimizer.SelectionPriorityBalanced.String(), fmt.Sprintf("what to optimise for (%s) — a preset over the four qos-*-weight flags above; any weight set by hand overrides the preset", strings.Join(provideroptimizer.SelectionPriorityNames(), "|")))
	if err := viper.BindPFlag(common.ProviderOptimizerAvailabilityWeight, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerAvailabilityWeight)); err != nil {
		utils.LavaFormatFatal("failed binding availability weight flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerLatencyWeight, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerLatencyWeight)); err != nil {
		utils.LavaFormatFatal("failed binding latency weight flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerSyncWeight, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerSyncWeight)); err != nil {
		utils.LavaFormatFatal("failed binding sync weight flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerStakeWeight, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerStakeWeight)); err != nil {
		utils.LavaFormatFatal("failed binding stake weight flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerMinSelectionChance, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerMinSelectionChance)); err != nil {
		utils.LavaFormatFatal("failed binding min selection chance flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerSelectionMode, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerSelectionMode)); err != nil {
		utils.LavaFormatFatal("failed binding selection mode flag", err)
	}
	if err := viper.BindPFlag(common.ProviderOptimizerSelectionPriority, cmdRPCSmartRouter.Flags().Lookup(common.ProviderOptimizerSelectionPriority)); err != nil {
		utils.LavaFormatFatal("failed binding selection priority flag", err)
	}
	cmdRPCSmartRouter.Flags().String(metrics.MetricsListenFlagName, metrics.DisabledFlagOption, "the address to expose prometheus metrics (such as localhost:7779)")
	// Usage telemetry (OTel) — off by default. When enabled, per-relay and
	// optimizer-qos events are emitted as OTLP/HTTP logs to a local collector
	// that fans out to the operator's chosen backend(s).
	cmdRPCSmartRouter.Flags().Bool(metrics.UsageOTelEnabledFlagName, false, "emit per-relay usage + optimizer-qos events as OTLP logs to a collector (off by default; relay path pays nothing when off)")
	cmdRPCSmartRouter.Flags().String(metrics.UsageOTelEndpointFlagName, "", "OTLP/HTTP endpoint for the local OTel collector (default: localhost:4318 / OTEL_EXPORTER_OTLP_ENDPOINT)")
	cmdRPCSmartRouter.Flags().Bool(metrics.UsageOTelInsecureFlagName, true, "skip TLS for the OTLP exporter (default true; expected target is a sidecar collector)")
	cmdRPCSmartRouter.Flags().Int(metrics.UsageOTelQueueSizeFlagName, 50000, "in-memory queue capacity for usage events; a full queue drops events")
	cmdRPCSmartRouter.Flags().Int(metrics.UsageOTelBatchSizeFlagName, 1000, "usage event batch-size flush trigger")
	cmdRPCSmartRouter.Flags().Duration(metrics.UsageOTelFlushIntervalFlagName, 500*time.Millisecond, "usage event time-based flush trigger")
	cmdRPCSmartRouter.Flags().Duration(metrics.UsageOTelExportTimeoutFlagName, 10*time.Second, "OTLP per-batch export timeout")
	cmdRPCSmartRouter.Flags().String(metrics.UsageOTelServiceNameFlagName, "smartrouter", "OTel service.name resource attribute")
	cmdRPCSmartRouter.Flags().String(metrics.UsageOTelInstanceIDFlagName, "", "OTel service.instance.id (default: hostname-pid); useful when running multiple processes per host")
	cmdRPCSmartRouter.Flags().Bool(DebugRelaysFlagName, false, "adding debug information to relays")
	cmdRPCSmartRouter.Flags().Bool(common.EnableSelectionStatsHeaderFlag, false, "enable selection stats header for debugging provider selection")
	// CORS related flags
	cmdRPCSmartRouter.Flags().String(common.CorsCredentialsFlag, "true", "Set up CORS allowed credentials,default \"true\"")
	cmdRPCSmartRouter.Flags().String(common.CorsHeadersFlag, "", "Set up CORS allowed headers, * for all, default simple cors specification headers")
	cmdRPCSmartRouter.Flags().String(common.CorsOriginFlag, "*", "Set up CORS allowed origin, enabled * by default")
	cmdRPCSmartRouter.Flags().String(common.CorsMethodsFlag, "GET,POST,PUT,DELETE,OPTIONS", "set up Allowed OPTIONS methods, defaults to: \"GET,POST,PUT,DELETE,OPTIONS\"")
	cmdRPCSmartRouter.Flags().String(common.CorsExposeHeadersFlag, "", "Set up CORS Access-Control-Expose-Headers — response headers a browser may read (e.g. \"Lava-Provider-Address\", or \"*\" for all). Empty by default (only simple response headers are readable from JS).")
	cmdRPCSmartRouter.Flags().String(common.CDNCacheDurationFlag, "86400", "set up preflight options response cache duration, default 86400 (24h in seconds)")
	cmdRPCSmartRouter.Flags().Bool(common.SharedStateFlag, false, "Share state across router replicas through the cache backend (requires --cache-be): the consumer consistency seen-block, and per-endpoint chain-tracker poll observations so an upstream is polled about once per interval fleet-wide instead of once per pod")
	// relays health check related flags
	cmdRPCSmartRouter.Flags().Bool(common.RelaysHealthEnableFlag, RelaysHealthEnableFlagDefault, "enables relays health check")
	cmdRPCSmartRouter.Flags().Duration(common.RelayHealthIntervalFlag, RelayHealthIntervalFlagDefault, "interval between relay health checks")
	// Registered as a flagset-owned Bool (NOT BoolVar bound to the lavasession global): BoolVar writes
	// the bound global at registration time, which raced probe goroutines reading it. Applied to the
	// atomic global in RunE via lavasession.SetDebugProbes.
	cmdRPCSmartRouter.Flags().Bool(DebugProbesFlagName, false, "adding information to probes")
	cmdRPCSmartRouter.Flags().StringArray(common.UseStaticSpecFlag, nil, "load specs from file, directory, or remote URL (GitHub/GitLab). Can be specified multiple times; later sources override earlier ones for same chain ID")
	cmdRPCSmartRouter.Flags().String(common.GitHubTokenFlag, "", "GitHub personal access token for accessing private repositories (public repos are fetched via unmetered tarball downloads and need no token)")
	cmdRPCSmartRouter.Flags().String(common.GitLabTokenFlag, "", "GitLab personal access token for accessing private repositories (supports gitlab.com and self-hosted instances)")
	cmdRPCSmartRouter.Flags().Duration(common.EpochDurationFlag, 0, "duration of each epoch for time-based epoch system (e.g., 30m, 1h). If not set, epochs are disabled")
	cmdRPCSmartRouter.Flags().Duration(common.ShutdownGracePeriodFlag, common.DefaultShutdownGracePeriod, "graceful shutdown deadline for in-flight requests and WebSocket clients")
	cmdRPCSmartRouter.Flags().IntVar(&relaycore.RelayRetryLimit, common.SetRelayRetryLimitFlag, 2, "max total relay retry attempts across all error types (node and protocol errors combined; 0 disables retries)")
	cmdRPCSmartRouter.Flags().BoolVar(&rpcInterfaceMessages.BatchNodeErrorOnAny, common.BatchNodeErrorOnAnyFlag, false, "if true, batch requests are treated as node errors if ANY sub-request fails; if false (default), only if ALL fail")
	// optimizer qos sampling cadence — drives the in-memory /metrics
	// selection-score cache and the OTel optimizer_qos emit.
	cmdRPCSmartRouter.Flags().DurationVar(&metrics.OptimizerQosServerSamplingInterval, common.OptimizerQosServerSamplingIntervalFlag, time.Second*1, "interval to sample optimizer qos reports")
	// websocket flags
	cmdRPCSmartRouter.Flags().IntVar(&chainlib.WebSocketRateLimit, common.RateLimitWebSocketFlag, chainlib.WebSocketRateLimit, "rate limit (per second) websocket requests per user connection, default is unlimited")
	cmdRPCSmartRouter.Flags().Int64Var(&chainlib.MaximumNumberOfParallelWebsocketConnectionsPerIp, common.LimitParallelWebsocketConnectionsPerIpFlag, chainlib.MaximumNumberOfParallelWebsocketConnectionsPerIp, "limit number of parallel connections to websocket, per ip, default is unlimited (0)")
	cmdRPCSmartRouter.Flags().Int64Var(&chainlib.MaxIdleTimeInSeconds, common.LimitWebsocketIdleTimeFlag, chainlib.MaxIdleTimeInSeconds, "limit the idle time in seconds for a websocket connection, default is 20 minutes ( 20 * 60 )")
	cmdRPCSmartRouter.Flags().DurationVar(&chainlib.WebSocketBanDuration, common.BanDurationForWebsocketRateLimitExceededFlag, chainlib.WebSocketBanDuration, "once websocket rate limit is reached, user will be banned Xfor a duration, default no ban")

	cmdRPCSmartRouter.Flags().BoolVar(&chainlib.SkipWebsocketVerificationDefault, common.SkipWebsocketVerificationFlag, chainlib.SkipWebsocketVerificationDefault, "skip websocket verification for chains that require ws/wss endpoints")
	cmdRPCSmartRouter.Flags().BoolVar(&chainlib.SkipAllVerifications, common.SkipAllVerificationsFlag, chainlib.SkipAllVerifications, "skip ALL spec verifications for every provider this process serves, healthy ones included. An escape hatch for bringing a router up against upstreams that cannot survive being probed; prefer the per-node-url skip-verifications config (which accepts \"*\") for anything ongoing")

	cmdRPCSmartRouter.Flags().DurationVar(&lavasession.ProbeLoopInterval, common.ProbeLoopIntervalFlagName, lavasession.ProbeLoopInterval, "cadence of the proactive health prober (MAG-2161 Topic D); must be > 0, default 5s")
	cmdRPCSmartRouter.Flags().Float64(common.ProbeUpdateWeightFlagName, scoreutils.DefaultProbeUpdateWeight, "weight multiplier for provider-optimizer probe updates (liveness/latency); must be > 0")
	if err := viper.BindPFlag(common.ProbeUpdateWeightFlagName, cmdRPCSmartRouter.Flags().Lookup(common.ProbeUpdateWeightFlagName)); err != nil {
		utils.LavaFormatFatal("failed binding probe update weight flag", err)
	}

	cmdRPCSmartRouter.Flags().DurationVar(&common.DefaultTimeout, common.DefaultProcessingTimeoutFlagName, common.DefaultTimeout, "default timeout for relay processing (e.g., 30s, 1m)")
	cmdRPCSmartRouter.Flags().DurationVar(&common.MinimumTimePerRelayDelay, common.MinRelayTimeoutFlagName, common.MinimumTimePerRelayDelay, "minimum relay timeout floor applied to all methods when CU-based timeout is lower (e.g., 1s, 5s)")
	cmdRPCSmartRouter.Flags().IntVar(&lavasession.MaxSessionsAllowedPerProvider, common.MaxSessionsPerProviderFlagName, lavasession.MaxSessionsAllowedPerProvider, "max number of sessions allowed per provider")

	// batch request size limit
	cmdRPCSmartRouter.Flags().IntVar(&chainlib.MaxBatchRequestSize, common.MaxBatchRequestSizeFlag, common.DefaultMaxBatchRequestSize, "max number of requests allowed within a batch request, 0 means unlimited")
	cmdRPCSmartRouter.Flags().BoolVar(&relaycore.DisableBatchRequestRetry, common.DisableBatchRequestRetryFlag, true, "disable retries for batch requests (JSON-RPC batches)")

	// OpenTelemetry tracing.
	// All standard OTel knobs (endpoint, sampler, service name, TLS, headers, etc.)
	// come from OTEL_* environment variables per the SDK spec — see the
	// tracing.TraceBodyFlag doc-comment for the full list. Tracing is enabled
	// unless OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none; when no OTLP
	// endpoint is configured the SDK falls back to the spec default
	// (localhost:4317 for gRPC, localhost:4318 for HTTP).
	// --otel-trace-body is the only Lava-specific knob, exposed as a CLI flag
	// because it's a per-invocation debug toggle rather than deployment
	// configuration. Body size is delegated to the SDK via
	// OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT (SDK default: unlimited).
	cmdRPCSmartRouter.Flags().Bool(tracing.TraceBodyFlag, false, "record request/response bodies on trace spans (size limit delegated to OTel SDK via OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT)")
	if err := viper.BindPFlag(tracing.TraceBodyFlag, cmdRPCSmartRouter.Flags().Lookup(tracing.TraceBodyFlag)); err != nil {
		utils.LavaFormatFatal("failed binding otel-trace-body flag", err)
	}

	common.AddRollingLogConfig(cmdRPCSmartRouter)
	// Log level/format flags (previously provided by cosmos-sdk AddTxFlagsToCmd)
	cmdRPCSmartRouter.Flags().String("log-level", "info", "log level (debug|info|warn|error|fatal)")
	cmdRPCSmartRouter.Flags().String("log-format", "text", "log format (text|json)")
	return cmdRPCSmartRouter
}

func (rpsr *RPCSmartRouter) updateEpoch(ctx context.Context, epoch uint64) {
	// Copy session manager keys under lock to avoid iterating the map
	// concurrently with retryFailedProviders which writes to rpsr maps under rpsr.mu.
	rpsr.mu.Lock()
	chainKeys := make([]string, 0, len(rpsr.sessionManagers))
	for k := range rpsr.sessionManagers {
		chainKeys = append(chainKeys, k)
	}
	rpsr.mu.Unlock()

	for _, chainKey := range chainKeys {
		chainKeyLog := chainKey

		utils.LavaFormatInfo("ConsumerSessionManager: Epoch update triggered",
			utils.LogAttr("epoch", epoch),
			utils.LogAttr("chainKey", chainKeyLog),
			utils.LogAttr("time", time.Now().Format("15:04:05 MST")),
		)

		// Resolve the per-chain metrics manager once so endpoint health resets below
		// can also reset the corresponding Prometheus gauge. Without this, #2256's
		// endpoint.ResetHealth() fixes the in-memory struct but the
		// rpc_endpoint_overall_health gauge stays stuck at 0 (unhealthy) forever,
		// since the only path back to 1 is a successful relay that calls
		// SetEndpointOverallHealth(..., true) — which a backup may never receive.
		var epochMetrics *metrics.SmartRouterMetricsManager
		var epochChainID, epochApiInterface string
		// listenEndpoint is a *lavasession.RPCEndpoint — always guard its deref.
		// Skipping metric reset for a server with a nil listenEndpoint is preferable
		// to a nil-deref panic that would kill the whole epoch transition and leave
		// every endpoint.ResetHealth() undone.
		if server, exists := rpsr.rpcServers[chainKey]; exists && server != nil && server.listenEndpoint != nil {
			epochMetrics = server.smartRouterEndpointMetrics
			epochChainID = server.listenEndpoint.ChainID
			epochApiInterface = server.listenEndpoint.ApiInterface
		}
		// Lock for the read → create-fresh → write-back section.
		// This prevents races with retryFailedProviders, which merges
		// recovered providers into rpsr.providerSessions under the same lock.
		// The locked section is pure CPU work (map lookups + object creation).
		rpsr.mu.Lock()
		sessionManager := rpsr.sessionManagers[chainKey]
		oldProviderSessions := rpsr.providerSessions[chainKey]
		oldBackupSessions := rpsr.backupProviderSessions[chainKey]

		// Create FRESH ConsumerSessionsWithProvider objects to avoid session accumulation
		// This is critical: reusing the same objects causes sessions to accumulate in the Sessions map
		// until hitting the 1000-session limit, causing "No pairings available" errors
		freshProviderSessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider)
		for idx, oldSession := range oldProviderSessions {
			// Reset endpoint health so disabled endpoints get a fresh start each epoch.
			// Without this, an endpoint disabled by ConnectionRefusals stays disabled
			// forever since it can never receive the successful relay needed to trigger ResetHealth.
			for _, endpoint := range oldSession.Endpoints {
				endpoint.ResetHealth()
			}
			// Mirror the struct reset onto the Prometheus gauge so operators see the
			// provider recover at the epoch boundary rather than remaining stuck at 0.
			if epochMetrics != nil {
				epochMetrics.SetEndpointOverallHealth(epochChainID, epochApiInterface, oldSession.PublicLavaAddress, true)
			}
			freshSession := lavasession.NewConsumerSessionWithProvider(
				oldSession.PublicLavaAddress,
				oldSession.Endpoints,
				oldSession.MaxComputeUnits,
				epoch,
				oldSession.GetProviderStakeSize(),
			)
			freshSession.StaticProvider = oldSession.StaticProvider
			freshSession.GroupLabel = oldSession.GroupLabel // cross-validation group must survive epoch refresh
			freshProviderSessions[idx] = freshSession

			utils.LavaFormatDebug("Created fresh provider session for epoch",
				utils.LogAttr("provider", freshSession.PublicLavaAddress),
				utils.LogAttr("epoch", epoch),
				utils.LogAttr("chainKey", chainKeyLog))
		}

		// Create fresh backup sessions
		freshBackupSessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider)
		for idx, oldSession := range oldBackupSessions {
			for _, endpoint := range oldSession.Endpoints {
				endpoint.ResetHealth()
			}
			// Same rationale as above: backups are especially susceptible to a stuck
			// unhealthy gauge because they rarely receive the successful relay that
			// would otherwise toggle it back to 1.
			if epochMetrics != nil {
				epochMetrics.SetEndpointOverallHealth(epochChainID, epochApiInterface, oldSession.PublicLavaAddress, true)
			}
			freshSession := lavasession.NewConsumerSessionWithProvider(
				oldSession.PublicLavaAddress,
				oldSession.Endpoints,
				oldSession.MaxComputeUnits,
				epoch,
				oldSession.GetProviderStakeSize(),
			)
			freshSession.StaticProvider = oldSession.StaticProvider
			freshSession.GroupLabel = oldSession.GroupLabel // cross-validation group must survive epoch refresh
			freshBackupSessions[idx] = freshSession

			utils.LavaFormatDebug("Created fresh backup provider session for epoch",
				utils.LogAttr("provider", freshSession.PublicLavaAddress),
				utils.LogAttr("epoch", epoch),
				utils.LogAttr("chainKey", chainKeyLog))
		}

		// Re-verify configured providers and reconcile against the fresh maps:
		// drop entries whose provider failed validation (demote), and append
		// new sessions for configured providers that pass but weren't active
		// (promote — recovering from failed-init). Skipped when inputs are
		// absent (test path constructs RPCSmartRouter directly). Demoted
		// sessions are collected and their direct connections closed *after*
		// UpdateAllProviders below — closing earlier would race in-flight
		// relays still holding pointers to the prior pairing.
		var demotedSessions []*lavasession.ConsumerSessionsWithProvider
		if inputs := rpsr.reverifyInputs[chainKey]; inputs != nil {
			reverifyStart := time.Now()
			var demotedStatic, demotedBackup []*lavasession.ConsumerSessionsWithProvider
			var promotedStatic, promotedBackup []string
			freshProviderSessions, demotedStatic, promotedStatic = applyReverification(ctx, inputs, freshProviderSessions, reverifyTierStatic, epoch)
			freshBackupSessions, demotedBackup, promotedBackup = applyReverification(ctx, inputs, freshBackupSessions, reverifyTierBackup, epoch)
			demotedSessions = slices.Concat(demotedStatic, demotedBackup)

			// A promoted provider supersedes any pending failed-init retry, the same
			// invariant rebuildPairingFromConfig enforces. Without this the provider stays
			// in the failed list, retryFailedProviders revalidates it, succeeds, and
			// mergeRecoveredSessions appends a SECOND session for it — that helper keys by
			// index and does not dedupe by PublicLavaAddress. csm.pairing collapses the
			// duplicate but pairingAddresses and validAddresses do not, so the provider
			// lands twice in validAddresses with double its selection weight, and the
			// superseded ConsumerSessionsWithProvider is dropped without its
			// DirectRPCConnection being closed.
			//
			// Pruned per tier, not by the union: a name configured in both tiers must not
			// have its still-failing backup dropped from the retry loop because its static
			// twin recovered.
			pruneRestoredFromFailed(rpsr.failedStaticProviders, chainKey, nameSet(promotedStatic))
			pruneRestoredFromFailed(rpsr.failedBackupProviders, chainKey, nameSet(promotedBackup))
			// Per-chain re-verify is internally bounded by SpecReVerifyConcurrency,
			// but updateEpoch iterates chains serially. Surfacing per-chain duration
			// gives operators a signal when total tick time approaches epoch length —
			// the cross-chain bound is Σ ⌈N_chain/conc⌉ × SpecReVerifyAttemptTimeout.
			if elapsed := time.Since(reverifyStart); elapsed > SpecReVerifyAttemptTimeout {
				utils.LavaFormatWarning("re-verify: cycle exceeded single-attempt timeout — consider tuning SpecReVerifyConcurrency", nil,
					utils.LogAttr("chainKey", chainKeyLog),
					utils.LogAttr("elapsed", elapsed.String()),
					utils.LogAttr("attemptTimeout", SpecReVerifyAttemptTimeout.String()),
				)
			}
		}

		// Update stored sessions with fresh objects.
		// When re-verification demotes every backup the map can be empty (not nil).
		// In that case clear the entry so rpsr.backupProviderSessions stays consistent
		// with what we hand UpdateAllProviders below — otherwise the stored field would
		// retain the prior cycle's map.
		rpsr.providerSessions[chainKey] = freshProviderSessions
		if len(freshBackupSessions) > 0 {
			rpsr.backupProviderSessions[chainKey] = freshBackupSessions
		} else {
			delete(rpsr.backupProviderSessions, chainKey)
		}
		rpsr.publishServingTierLocked(chainKey)
		rpsr.republishSubscriptionEndpointsLocked(chainKey)
		server := rpsr.rpcServers[chainKey]

		// UpdateAllProviders stays under rpsr.mu so the (rpsr.providerSessions write
		// → csm push) pair is atomic with retryFailedProviders' matching pair.
		// Otherwise the two callers can push snapshots to csm in the opposite order
		// they wrote rpsr.providerSessions, silently dropping providers until the
		// next epoch. The synchronous body of UpdateAllProviders is a bounded map
		// rebuild; probing is dispatched to a goroutine.
		err := sessionManager.UpdateAllProviders(epoch, freshProviderSessions, freshBackupSessions)
		rpsr.mu.Unlock()

		if err != nil {
			utils.LavaFormatError("Failed to update providers on epoch change", err,
				utils.LogAttr("epoch", epoch),
				utils.LogAttr("chainKey", chainKeyLog),
			)
		}

		// Now that the session manager is on the new pairing, drop the
		// DirectRPCConnections that belonged to demoted providers. The session
		// manager's own purge path closes endpoint.Connections but not
		// endpoint.DirectConnections — those are smart-router-owned transports
		// and would otherwise leak across a flap (active → demoted → active rebuilds).
		if len(demotedSessions) > 0 {
			go closeDemotedDirectConnections(demotedSessions)
		}

		// cleanupStaleTrackers is the genuinely heavy work (tracker teardown +
		// connection close) and stays outside the lock. Must run AFTER
		// UpdateAllProviders so connections are closed first.
		if server != nil {
			rpsr.cleanupStaleTrackers(chainKey, server, freshProviderSessions, freshBackupSessions)
		}
	}
}

// Retry cadence for providers that failed verification. A chain that still has a
// healthy provider is merely degraded, so it waits the full interval — retries
// there buy redundancy, not availability, and there is no reason to hammer a dead
// upstream every few seconds. A chain that is dark cannot serve at all, so every
// second of delay is downtime: it starts at retryDarkBaseInterval and backs off to
// the same ceiling, which keeps recovery near-immediate for the common case (a
// brief all-down window at boot) without pinning a chain to a 2s poll forever.
// Package-level vars, not consts, so tests can shorten them.
var (
	retryDarkBaseInterval = 2 * time.Second
	retryMaxInterval      = 3 * time.Minute // same as SpecValidator's disabled-chain interval
)

func retryIntervalFor(dark bool) time.Duration {
	if dark {
		return retryDarkBaseInterval
	}
	return retryMaxInterval
}

// chainIsDark reports whether the chain has no provider registered in either
// tier. Reads the live session maps rather than the boot-time verdict so the
// answer tracks recoveries and demotions.
//
// This is registration, not reachability: a chain whose registered providers are
// all failing relays is not "dark" here. Those providers never entered the failed
// lists this loop walks — the CSM's blocking and the epoch reverification own that
// case — so treating them as dark would only make this loop spin without work.
func (rpsr *RPCSmartRouter) chainIsDark(sessionManagerKey string) bool {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	return len(rpsr.providerSessions[sessionManagerKey]) == 0 &&
		len(rpsr.backupProviderSessions[sessionManagerKey]) == 0
}

// publishServingTierLocked republishes the serving-tier gauge from the live
// session maps. Callers must already hold rpsr.mu.
//
// Every path that mutates those maps has to call this, not just boot. The gauge
// is what operators alert on now that a dark chain no longer crash-loops, and a
// stale reading is worse than none: left at 0 it pages forever after a recovery,
// left at 2 it stays silent after a demotion takes the last provider away.
func (rpsr *RPCSmartRouter) publishServingTierLocked(chainKey string) {
	if rpsr.smartRouterMetricsManager == nil {
		return
	}
	// Labels come from reverifyInputs, the only per-chain record of the endpoint.
	// Absent in tests that build RPCSmartRouter directly; nothing to publish then.
	inputs := rpsr.reverifyInputs[chainKey]
	if inputs == nil || inputs.rpcEndpoint == nil {
		return
	}
	rpsr.smartRouterMetricsManager.SetEndpointServingTier(
		inputs.rpcEndpoint.ChainID,
		inputs.rpcEndpoint.ApiInterface,
		len(rpsr.providerSessions[chainKey]),
		len(rpsr.backupProviderSessions[chainKey]),
	)
}

// subscriptionEndpointSetter is the slice of the Direct WS/gRPC subscription managers
// republishSubscriptionEndpointsLocked needs. The NoOp WS manager deliberately does not
// implement it — a chain with no ws:// URL configured has nothing to republish.
type subscriptionEndpointSetter interface {
	SetEndpoints(primary, backup []*common.NodeUrl) bool
}

// republishSubscriptionEndpointsLocked pushes the currently-serving WS and gRPC
// endpoints into the subscription managers. Callers must already hold rpsr.mu.
//
// Companion to publishServingTierLocked, and it belongs on every path that mutates the
// live pairing for the same reason: the managers' tiers are built once at boot, and
// nothing else updates them. Left alone they are frozen for the process lifetime, which
// after MAG-2525 turns both of this fix's own scenarios into permanent subscription
// outages — a chain that booted dark keeps two empty tiers and fails every eth_subscribe
// forever, and a chain that booted on backups alone never promotes a recovered primary.
// HTTP relays recover in both cases, so nothing else surfaces the fault.
//
// The tiers are rebuilt from the configured providers filtered by what is live in the
// session maps, which is the same source of truth publishServingTierLocked reads. Only
// live endpoints go in: a configured-but-dead endpoint in the primary tier would be
// handed to a subscription with no fallback, and would suppress the primary→backup
// cascade, which only fires when the primary tier is empty or fully ignored.
func (rpsr *RPCSmartRouter) republishSubscriptionEndpointsLocked(chainKey string) {
	server := rpsr.rpcServers[chainKey]
	if server == nil {
		return // not yet registered (boot seeds the tiers via the constructors)
	}
	inputs := rpsr.reverifyInputs[chainKey]
	if inputs == nil {
		return // no configured lists to filter — test-constructed router
	}

	liveStatic := activeProviders(inputs.configuredStatic, rpsr.providerSessions[chainKey])
	liveBackup := activeProviders(inputs.configuredBackup, rpsr.backupProviderSessions[chainKey])

	if setter, ok := server.wsSubscriptionManager.(subscriptionEndpointSetter); ok {
		primary := collectWSEndpoints(liveStatic, "")
		backup := collectWSEndpoints(liveBackup, "")
		if setter.SetEndpoints(primary, backup) {
			utils.LavaFormatInfo("subscriptions: WebSocket endpoint tiers updated to match live pairing",
				utils.LogAttr("chainKey", chainKey),
				utils.LogAttr("primaryCount", len(primary)),
				utils.LogAttr("backupCount", len(backup)),
			)
		}
	}

	if server.grpcSubscriptionManager != nil {
		primary := collectGRPCEndpoints(liveStatic, "")
		backup := collectGRPCEndpoints(liveBackup, "")
		if server.grpcSubscriptionManager.SetEndpoints(primary, backup) {
			utils.LavaFormatInfo("subscriptions: gRPC endpoint tiers updated to match live pairing",
				utils.LogAttr("chainKey", chainKey),
				utils.LogAttr("primaryCount", len(primary)),
				utils.LogAttr("backupCount", len(backup)),
			)
		}
	}
}

// activeProviders returns the configured providers that are currently in the pairing,
// in configured order. Sessions are keyed by index and carry the provider name as
// PublicLavaAddress (see convertProvidersToSessions), so this is the bridge from "what
// is live" back to the RPCStaticProviderEndpoint records that own the NodeUrls.
func activeProviders(
	configured []*lavasession.RPCStaticProviderEndpoint,
	sessions map[uint64]*lavasession.ConsumerSessionsWithProvider,
) []*lavasession.RPCStaticProviderEndpoint {
	if len(configured) == 0 || len(sessions) == 0 {
		return nil
	}
	active := make(map[string]struct{}, len(sessions))
	for _, s := range sessions {
		active[s.PublicLavaAddress] = struct{}{}
	}
	live := make([]*lavasession.RPCStaticProviderEndpoint, 0, len(active))
	for _, p := range configured {
		if _, ok := active[p.Name]; ok {
			live = append(live, p)
		}
	}
	return live
}

// retryFailedProviders periodically re-validates providers that failed
// verification and re-registers them when they recover. One goroutine per
// endpoint that had failures in either tier.
//
// It covers both tiers. Before MAG-2525 only static providers were retried here
// and backups waited for the 15m epoch reverification, which was tolerable when a
// chain could not boot on backups alone. Now it can, so a failed backup may be the
// only thing between the chain and serving traffic.
func (rpsr *RPCSmartRouter) retryFailedProviders(
	ctx context.Context,
	sessionManagerKey string,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	darkBackoff := retryDarkBaseInterval
	timer := time.NewTimer(retryIntervalFor(rpsr.chainIsDark(sessionManagerKey)))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		rpsr.mu.Lock()
		failedStatic := rpsr.failedStaticProviders[sessionManagerKey]
		failedBackup := rpsr.failedBackupProviders[sessionManagerKey]
		rpsr.mu.Unlock()

		if len(failedStatic) == 0 && len(failedBackup) == 0 {
			utils.LavaFormatInfo("All failed providers recovered — stopping retry loop",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			)
			return
		}

		utils.LavaFormatInfo("Retrying failed providers",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("failedStatic", len(failedStatic)),
			utils.LogAttr("failedBackup", len(failedBackup)),
		)

		recoveredStatic, stillFailedStatic := rpsr.revalidateTier(ctx, failedStatic, chainParser, rpcEndpoint, reverifyTierStatic)
		recoveredBackup, stillFailedBackup := rpsr.revalidateTier(ctx, failedBackup, chainParser, rpcEndpoint, reverifyTierBackup)

		rpsr.readmitRecoveredProviders(
			sessionManagerKey, rpcEndpoint, convertProvidersToSessions,
			recoveredStatic, stillFailedStatic, recoveredBackup, stillFailedBackup)

		if rpsr.chainIsDark(sessionManagerKey) {
			darkBackoff *= 2
			if darkBackoff > retryMaxInterval {
				darkBackoff = retryMaxInterval
			}
			timer.Reset(darkBackoff)
		} else {
			timer.Reset(retryMaxInterval)
			darkBackoff = retryDarkBaseInterval
		}
	}
}

// retryValidateFn is the probe retryFailedProviders runs against a failed
// provider. A package-level var purely so tests can substitute a fake without
// standing up upstreams — production never reassigns it. Mirrors the
// chainReverifyInputs.validateFn seam applyReverification uses.
var retryValidateFn = func(ctx context.Context, provider *lavasession.RPCStaticProviderEndpoint, chainParser chainlib.ChainParser) error {
	return validateProvider(ctx, provider, chainParser, BootValidateTimeout)
}

// revalidateTier re-runs verification over one tier's failed providers, returning
// the recovered and still-failing partitions in configured order.
func (rpsr *RPCSmartRouter) revalidateTier(
	ctx context.Context,
	failed []*lavasession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	tier reverifyTier,
) (recovered, stillFailed []*lavasession.RPCStaticProviderEndpoint) {
	for _, provider := range failed {
		if err := retryValidateFn(ctx, provider, chainParser); err != nil {
			stillFailed = append(stillFailed, provider)
			utils.LavaFormatWarning("retry: provider verification still failing", err,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("tier", tier.String()),
				utils.LogAttr("provider", provider.Name),
			)
			continue
		}
		recovered = append(recovered, provider)
		utils.LavaFormatInfo("[+] provider recovered and passed verification",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("tier", tier.String()),
			utils.LogAttr("provider", provider.Name),
		)
	}
	return recovered, stillFailed
}

// readmitRecoveredProviders merges recovered providers of both tiers back into the
// live pairing and pushes the result to the session manager in one call.
//
// The merge is copy-on-write: the old maps may still be referenced by goroutines
// (cleanupStaleTrackers) that iterate them without the lock.
// UpdateAllProviders stays under rpsr.mu so the (session-map write → csm push) pair
// is atomic with updateEpoch's matching pair — otherwise a concurrent epoch tick can
// push to the csm in the opposite order it wrote the session maps, silently dropping
// recovered providers until the next epoch.
func (rpsr *RPCSmartRouter) readmitRecoveredProviders(
	sessionManagerKey string,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
	recoveredStatic, stillFailedStatic []*lavasession.RPCStaticProviderEndpoint,
	recoveredBackup, stillFailedBackup []*lavasession.RPCStaticProviderEndpoint,
) {
	rpsr.mu.Lock()

	rpsr.failedStaticProviders[sessionManagerKey] = stillFailedStatic
	rpsr.failedBackupProviders[sessionManagerKey] = stillFailedBackup

	if len(recoveredStatic) == 0 && len(recoveredBackup) == 0 {
		rpsr.mu.Unlock()
		return
	}

	currentEpoch := rpsr.epochTimer.GetCurrentEpoch()
	mergedStatic := mergeRecoveredSessions(rpsr.providerSessions[sessionManagerKey], recoveredStatic, convertProvidersToSessions, currentEpoch)
	mergedBackup := mergeRecoveredSessions(rpsr.backupProviderSessions[sessionManagerKey], recoveredBackup, convertProvidersToSessions, currentEpoch)

	rpsr.providerSessions[sessionManagerKey] = mergedStatic
	if len(mergedBackup) > 0 {
		rpsr.backupProviderSessions[sessionManagerKey] = mergedBackup
	}
	rpsr.publishServingTierLocked(sessionManagerKey)
	rpsr.republishSubscriptionEndpointsLocked(sessionManagerKey)

	err := rpsr.sessionManagers[sessionManagerKey].UpdateAllProviders(currentEpoch, mergedStatic, mergedBackup)
	rpsr.mu.Unlock()

	if err != nil {
		utils.LavaFormatWarning("retry: failed to re-register recovered providers", err,
			utils.LogAttr("chain", rpcEndpoint.ChainID),
		)
		return
	}
	for _, tier := range []struct {
		name      reverifyTier
		recovered []*lavasession.RPCStaticProviderEndpoint
	}{
		{reverifyTierStatic, recoveredStatic},
		{reverifyTierBackup, recoveredBackup},
	} {
		for _, p := range tier.recovered {
			utils.LavaFormatInfo("[+] provider re-registered successfully",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("tier", tier.name.String()),
				utils.LogAttr("provider", p.Name),
			)
		}
	}
}

// nameSet turns a provider-name slice into the set pruneRestoredFromFailed wants.
func nameSet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return set
}

// pruneRestoredFromFailed removes providers named in `restored` from one chain's
// failed list, deleting the entry entirely once nothing is left pending.
func pruneRestoredFromFailed(
	failedByChain map[string][]*lavasession.RPCStaticProviderEndpoint,
	chainKey string,
	restored map[string]struct{},
) {
	failed := failedByChain[chainKey]
	if len(failed) == 0 {
		return
	}
	var kept []*lavasession.RPCStaticProviderEndpoint
	for _, p := range failed {
		if _, ok := restored[p.Name]; !ok {
			kept = append(kept, p)
		}
	}
	if len(kept) > 0 {
		failedByChain[chainKey] = kept
		return
	}
	delete(failedByChain, chainKey)
}

// mergeRecoveredSessions returns a new map holding `existing` plus freshly built
// sessions for `recovered`, appended at indices past the current maximum.
func mergeRecoveredSessions(
	existing map[uint64]*lavasession.ConsumerSessionsWithProvider,
	recovered []*lavasession.RPCStaticProviderEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
	currentEpoch uint64,
) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	if len(recovered) == 0 {
		return existing
	}
	recoveredSessions := convertProvidersToSessions(recovered)
	merged := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(existing)+len(recoveredSessions))
	maxIdx := uint64(0)
	for k, v := range existing {
		merged[k] = v
		if k >= maxIdx {
			maxIdx = k + 1
		}
	}
	for _, session := range recoveredSessions {
		session.Lock.Lock()
		session.PairingEpoch = currentEpoch
		session.Lock.Unlock()
		merged[maxIdx] = session
		maxIdx++
	}
	return merged
}

// rebuildPairingFromConfig restores configured static/backup providers that are
// currently absent from the live pairing — e.g. ones the per-epoch spec
// re-verifier (applyReverification) demoted after they returned errors — without
// waiting for the next 15m epoch tick or a pod restart. It backs the dev/test-only
// /debug/reset-pairing endpoint.
//
// COLD rebuild: absent providers are re-admitted straight from the startup config
// (reverifyInputs) with no spec re-probing — the simulator drives provider
// behaviour in the tests this serves, so verification is unnecessary. Only absent
// providers are converted (which opens fresh DirectRPCConnections); already-active
// sessions are reused untouched, so healthy providers are not churned. Mirrors
// retryFailedProviders' copy-on-write merge. Returns the restored provider
// names per chainKey (chains needing no restore are omitted).
func (rpsr *RPCSmartRouter) rebuildPairingFromConfig() map[string][]string {
	restored := make(map[string][]string)

	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()

	if rpsr.epochTimer == nil { // defensive: production always sets it in CreateSmartRouterEndpoint
		return restored
	}
	currentEpoch := rpsr.epochTimer.GetCurrentEpoch()

	for chainKey, sessionManager := range rpsr.sessionManagers {
		inputs := rpsr.reverifyInputs[chainKey]
		if sessionManager == nil || inputs == nil {
			continue
		}

		mergedStatic, restoredStatic := mergeAbsentProviders(
			rpsr.providerSessions[chainKey], inputs.configuredStatic, inputs.convertProvidersToSessions, currentEpoch)
		mergedBackup, restoredBackup := mergeAbsentProviders(
			rpsr.backupProviderSessions[chainKey], inputs.configuredBackup, inputs.convertProvidersToSessions, currentEpoch)

		if len(restoredStatic) == 0 && len(restoredBackup) == 0 {
			continue // pairing already whole — don't churn the session manager
		}

		rpsr.providerSessions[chainKey] = mergedStatic
		if len(mergedBackup) > 0 {
			rpsr.backupProviderSessions[chainKey] = mergedBackup
		} else {
			delete(rpsr.backupProviderSessions, chainKey)
		}
		rpsr.publishServingTierLocked(chainKey)
		rpsr.republishSubscriptionEndpointsLocked(chainKey)

		// A re-admitted provider supersedes any pending failed-init retry: drop it
		// from the failed lists so retryFailedProviders won't later merge a duplicate
		// session for the same name (and can self-terminate once both are empty).
		// Both tiers are pruned — retryFailedProviders now retries backups too.
		restoredSet := make(map[string]struct{}, len(restoredStatic)+len(restoredBackup))
		for _, n := range restoredStatic {
			restoredSet[n] = struct{}{}
		}
		for _, n := range restoredBackup {
			restoredSet[n] = struct{}{}
		}
		pruneRestoredFromFailed(rpsr.failedStaticProviders, chainKey, restoredSet)
		pruneRestoredFromFailed(rpsr.failedBackupProviders, chainKey, restoredSet)

		// UpdateAllProviders stays under rpsr.mu so the (providerSessions write →
		// csm push) pair is atomic with updateEpoch / retryFailedProviders —
		// otherwise a concurrent epoch tick can push to the csm in the opposite
		// order it wrote providerSessions, silently dropping the restored providers.
		if err := sessionManager.UpdateAllProviders(currentEpoch, mergedStatic, mergedBackup); err != nil {
			utils.LavaFormatError("reset-pairing: failed to push rebuilt pairing to session manager", err,
				utils.LogAttr("chainKey", chainKey))
			continue
		}

		restored[chainKey] = append(restoredStatic, restoredBackup...)
		utils.LavaFormatInfo("reset-pairing: restored providers from config",
			utils.LogAttr("chainKey", chainKey),
			utils.LogAttr("restored", restored[chainKey]),
			utils.LogAttr("epoch", currentEpoch),
		)
	}

	return restored
}

// mergeAbsentProviders returns a copy-on-write merge of current plus fresh sessions
// for every configured provider whose Name is absent from current, and the list of
// restored names (in config order, deterministic). Absent providers are built via
// convert, which opens fresh connections — providers whose connections all fail are
// skipped by convert and so are not reported restored. Present providers are reused
// untouched. Returns (current, nil) when nothing is absent.
func mergeAbsentProviders(
	current map[uint64]*lavasession.ConsumerSessionsWithProvider,
	configured []*lavasession.RPCStaticProviderEndpoint,
	convert func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
	epoch uint64,
) (map[uint64]*lavasession.ConsumerSessionsWithProvider, []string) {
	active := make(map[string]struct{}, len(current))
	for _, s := range current {
		active[s.PublicLavaAddress] = struct{}{}
	}

	var absent []*lavasession.RPCStaticProviderEndpoint
	for _, p := range configured {
		if _, ok := active[p.Name]; !ok {
			absent = append(absent, p)
		}
	}
	if len(absent) == 0 {
		return current, nil
	}

	// Copy-on-write: cleanupStaleTrackers may iterate the old map
	// without the lock, so never mutate it in place. Re-key appended sessions past
	// the existing max — convert() keys by its own list index, which would collide.
	merged := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(current)+len(absent))
	maxIdx := uint64(0)
	for idx, s := range current {
		merged[idx] = s
		if idx >= maxIdx {
			maxIdx = idx + 1
		}
	}

	converted := make(map[string]struct{})
	for _, session := range convert(absent) {
		session.Lock.Lock()
		session.PairingEpoch = epoch
		session.Lock.Unlock()
		merged[maxIdx] = session
		maxIdx++
		converted[session.PublicLavaAddress] = struct{}{}
	}

	var restored []string
	for _, p := range absent { // config order → deterministic output
		if _, ok := converted[p.Name]; ok {
			restored = append(restored, p.Name)
		}
	}
	return merged, restored
}

// cleanupStaleTrackers removes ChainTrackers for endpoints that are no longer in the current provider sessions.
// This prevents resource leaks from trackers polling endpoints that have been removed during epoch updates.
func (rpsr *RPCSmartRouter) cleanupStaleTrackers(
	chainKey string,
	server *RPCSmartRouterServer,
	providerSessions map[uint64]*lavasession.ConsumerSessionsWithProvider,
	backupSessions map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	if server.endpointChainTrackerManager == nil {
		return
	}

	// Build set of current endpoint URLs from both primary and backup providers
	currentEndpoints := make(map[string]bool)
	for _, provider := range providerSessions {
		for _, endpoint := range provider.Endpoints {
			currentEndpoints[endpoint.NetworkAddress] = true
		}
	}
	for _, provider := range backupSessions {
		for _, endpoint := range provider.Endpoints {
			currentEndpoints[endpoint.NetworkAddress] = true
		}
	}

	// Get all tracked endpoints and remove stale ones
	trackedEndpoints := server.endpointChainTrackerManager.GetAllEndpoints()
	removedCount := 0
	for _, trackedURL := range trackedEndpoints {
		if !currentEndpoints[trackedURL] {
			utils.LavaFormatInfo("removing stale ChainTracker on epoch update",
				utils.LogAttr("endpoint", trackedURL),
				utils.LogAttr("chainKey", chainKey),
			)
			server.endpointChainTrackerManager.RemoveTracker(trackedURL)
			removedCount++
		}
	}

	if removedCount > 0 {
		utils.LavaFormatInfo("epoch update: cleaned up stale ChainTrackers",
			utils.LogAttr("chainKey", chainKey),
			utils.LogAttr("removed", removedCount),
			utils.LogAttr("remaining", server.endpointChainTrackerManager.GetEndpointCount()),
		)
	}
}

func testModeWarn(desc string) {
	utils.LavaFormatWarning("------------------------------test mode --------------------------------\n\t\t\t"+
		desc+"\n\t\t\t"+
		"------------------------------test mode --------------------------------\n", nil)
}

// collectWSEndpoints returns every ws://|wss:// NodeUrl from the given providers.
// tierLabel ("primary" / "backup") is logged so operators can see which tier each
// endpoint came from; an empty tierLabel suppresses that per-endpoint line. Boot wants
// the full inventory, but the republish path runs on every pairing change and would
// otherwise re-log every endpoint of every chain each epoch — it logs deltas instead.
func collectWSEndpoints(providers []*lavasession.RPCStaticProviderEndpoint, tierLabel string) []*common.NodeUrl {
	var endpoints []*common.NodeUrl
	for _, provider := range providers {
		for i := range provider.NodeUrls {
			url := strings.ToLower(provider.NodeUrls[i].Url)
			if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
				endpoints = append(endpoints, &provider.NodeUrls[i])
				if tierLabel == "" {
					continue
				}
				utils.LavaFormatInfo("Found WebSocket endpoint for direct subscriptions",
					utils.LogAttr("tier", tierLabel),
					utils.LogAttr("url", provider.NodeUrls[i].Url),
					utils.LogAttr("provider", provider.Name),
					utils.LogAttr("chainID", provider.ChainID),
				)
			}
		}
	}
	return endpoints
}

// collectGRPCEndpoints returns every NodeUrl from providers whose ApiInterface is gRPC.
// tierLabel ("primary" / "backup") is logged for operator visibility; an empty label
// suppresses that line — see collectWSEndpoints.
func collectGRPCEndpoints(providers []*lavasession.RPCStaticProviderEndpoint, tierLabel string) []*common.NodeUrl {
	var endpoints []*common.NodeUrl
	for _, provider := range providers {
		if provider.ApiInterface != spectypes.APIInterfaceGrpc {
			continue
		}
		for i := range provider.NodeUrls {
			endpoints = append(endpoints, &provider.NodeUrls[i])
			if tierLabel == "" {
				continue
			}
			utils.LavaFormatInfo("Found gRPC endpoint for streaming subscriptions",
				utils.LogAttr("tier", tierLabel),
				utils.LogAttr("url", provider.NodeUrls[i].Url),
				utils.LogAttr("provider", provider.Name),
				utils.LogAttr("chainID", provider.ChainID),
			)
		}
	}
	return endpoints
}
