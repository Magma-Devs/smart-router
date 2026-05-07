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
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/statetracker"
	epochstoragetypes "github.com/magma-Devs/smart-router/types/epoch"
	planstypes "github.com/magma-Devs/smart-router/types/plans"
	protocoltypes "github.com/magma-Devs/smart-router/types/protocol"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	scoreutils "github.com/magma-Devs/smart-router/utils/score"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	DefaultRPCSmartRouterFileName = "rpcsmartrouter.yml"
	DebugRelaysFlagName           = "debug-relays"
	DebugProbesFlagName           = "debug-probes"
	reportsSendBEAddress          = "reports-be-address"

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

type AnalyticsServerAddresses struct {
	MetricsListenAddress  string
	RelayServerAddress    string
	RelayKafkaAddress     string
	RelayKafkaTopic       string
	RelayKafkaUsername    string
	RelayKafkaPassword    string
	RelayKafkaMechanism   string
	RelayKafkaTLSEnabled  bool
	RelayKafkaTLSInsecure bool
	ReportsAddressFlag    string
	OptimizerQoSAddress   string
	OptimizerQoSListen    bool
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

	// Server references for per-endpoint ChainTracker cleanup on epoch updates
	rpcServers map[string]*RPCSmartRouterServer // key: chainID-apiInterface
}

type rpcSmartRouterStartOptions struct {
	rpcEndpoints             []*lavasession.RPCEndpoint
	cache                    *performance.Cache
	strategy                 provideroptimizer.Strategy
	maxConcurrentProviders   uint
	analyticsServerAddresses AnalyticsServerAddresses
	cmdFlags                 common.ConsumerCmdFlags
	stateShare               bool
	staticProvidersList      []*lavasession.RPCStaticProviderEndpoint // define static providers as primary providers
	backupProvidersList      []*lavasession.RPCStaticProviderEndpoint // define backup providers as emergency fallback when no providers available
	geoLocation              uint64
	weightedSelectorConfig   provideroptimizer.WeightedSelectorConfig
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
	rpsr.rpcServers = make(map[string]*RPCSmartRouterServer)

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
	smartRouterReportsManager := metrics.NewConsumerReportsClient(options.analyticsServerAddresses.ReportsAddressFlag)

	// Smart router doesn't need consumer address from blockchain
	// Using a static identifier for metrics and logging
	smartRouterIdentifier := "smart-router-" + strconv.FormatUint(rand.Uint64(), 10)

	smartRouterUsageServeManager := metrics.NewConsumerRelayServerClient(options.analyticsServerAddresses.RelayServerAddress)                                                                                                                                                                                                                                                                                                                     // start up relay server reporting
	smartRouterKafkaClient := metrics.NewConsumerKafkaClient(options.analyticsServerAddresses.RelayKafkaAddress, options.analyticsServerAddresses.RelayKafkaTopic, options.analyticsServerAddresses.RelayKafkaUsername, options.analyticsServerAddresses.RelayKafkaPassword, options.analyticsServerAddresses.RelayKafkaMechanism, options.analyticsServerAddresses.RelayKafkaTLSEnabled, options.analyticsServerAddresses.RelayKafkaTLSInsecure) // start up kafka client
	var smartRouterOptimizerQoSClient *metrics.ConsumerOptimizerQoSClient
	if options.analyticsServerAddresses.OptimizerQoSAddress != "" || options.analyticsServerAddresses.OptimizerQoSListen {
		smartRouterOptimizerQoSClient = metrics.NewConsumerOptimizerQoSClient(smartRouterIdentifier, options.analyticsServerAddresses.OptimizerQoSAddress, options.geoLocation, metrics.OptimizerQosServerPushInterval) // start up optimizer qos client
		smartRouterOptimizerQoSClient.StartOptimizersQoSReportsCollecting(ctx, metrics.OptimizerQosServerSamplingInterval)
	}
	// SmartRouterMetricsManager is the single metrics owner for the smart router.
	// It serves its own HTTP endpoint and implements ConsumerMetricsManagerInf so it
	// can be passed to RPCConsumerLogs, ConsumerSessionManager, etc., eliminating the
	// need for a ConsumerMetricsManager (and all its lava_consumer_* metrics).
	smartRouterMetricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{
		NetworkAddress:     options.analyticsServerAddresses.MetricsListenAddress,
		StartHTTPServer:    true,
		OptimizerQoSClient: smartRouterOptimizerQoSClient,
	})

	rpcSmartRouterMetrics, err := metrics.NewRPCConsumerLogs(smartRouterMetricsManager, smartRouterUsageServeManager, smartRouterKafkaClient, smartRouterOptimizerQoSClient)
	if err != nil {
		utils.LavaFormatFatal("failed creating RPCSmartRouter logs", err)
	}

	smartRouterMetricsManager.SetVersion(protocoltypes.DefaultVersion.ConsumerTarget)
	smartRouterMetricsManager.StartSelectionStatsUpdater(ctx, metrics.OptimizerQosServerSamplingInterval)

	// we want one provider optimizer per chain so we will store them for reuse across rpcEndpoints
	chainMutexes := map[string]*sync.Mutex{}
	for _, endpoint := range options.rpcEndpoints {
		chainMutexes[endpoint.ChainID] = &sync.Mutex{} // create a mutex per chain for shared resources
	}

	optimizers := &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{}
	smartRouterConsistencies := &common.SafeSyncMap[string, relaycore.Consistency]{}

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
				optimizers, smartRouterConsistencies, chainMutexes,
				options, smartRouterIdentifier, rpcSmartRouterMetrics, smartRouterReportsManager, smartRouterOptimizerQoSClient,
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
	// This prevents multiple UpdateAllProviders calls with the same epoch to the same session manager
	rpsr.epochTimer.RegisterCallback(rpsr.updateEpoch)

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
		var currentOffsetNano atomic.Int64
		debugMux := buildDebugMux(optimizers, &currentOffsetNano)
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

	utils.LavaFormatInfo("RPCSmartRouter: graceful shutdown complete")
}

// buildDebugMux constructs the /debug/time-warp, /debug/time, and
// /debug/reset-scores HTTP handlers.
//
// See rpcconsumer.buildDebugMux for full documentation — this is an identical copy
// scoped to the rpcsmartrouter package.
func buildDebugMux(
	optimizers *common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer],
	currentOffsetNano *atomic.Int64,
) *http.ServeMux {
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
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"offset_seconds":%v,"applied_to_chains":true}`, body.OffsetSeconds)
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
	// current time offset or NowFunc.
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
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reset":true,"chains_reset":%d}`, count)
	})

	return mux
}

func (rpsr *RPCSmartRouter) CreateSmartRouterEndpoint(
	ctx context.Context,
	rpcEndpoint *lavasession.RPCEndpoint,
	errCh chan error,
	optimizers *common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer],
	smartRouterConsistencies *common.SafeSyncMap[string, relaycore.Consistency],
	chainMutexes map[string]*sync.Mutex,
	options *rpcSmartRouterStartOptions,
	smartRouterIdentifier string,
	rpcSmartRouterMetrics *metrics.RPCConsumerLogs,
	smartRouterReportsManager *metrics.ConsumerReportsClient,
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
		err = statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken(ctx, chainParser, options.cmdFlags.StaticSpecPaths, *rpcEndpoint, nil, options.cmdFlags.GitHubToken, options.cmdFlags.GitLabToken)
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
	var smartRouterConsistency relaycore.Consistency

	// Create chain assets with mutex protection
	chainMutexes[chainID].Lock()
	defer chainMutexes[chainID].Unlock()

	// Create / Use existing optimizer
	newOptimizer := provideroptimizer.NewProviderOptimizer(options.strategy, averageBlockTime, options.maxConcurrentProviders, smartRouterOptimizerQoSClient, chainID)
	newOptimizer.ConfigureWeightedSelector(options.weightedSelectorConfig)
	optimizer, loaded, err := optimizers.LoadOrStore(chainID, newOptimizer)
	if err != nil {
		errCh <- err
		return utils.LavaFormatError("failed loading optimizer", err, utils.LogAttr("endpoint", rpcEndpoint.Key()))
	}

	if !loaded && smartRouterOptimizerQoSClient != nil {
		// if this is a new optimizer, register it in the smartRouterOptimizerQoSClient
		smartRouterOptimizerQoSClient.RegisterOptimizer(optimizer, chainID)
	}

	// Create / Use existing Consistency
	newSmartRouterConsistency := relaycore.NewConsistency(chainID)
	smartRouterConsistency, _, err = smartRouterConsistencies.LoadOrStore(chainID, newSmartRouterConsistency)
	if err != nil {
		errCh <- err
		return utils.LavaFormatError("failed loading consumer consistency", err, utils.LogAttr("endpoint", rpcEndpoint.Key()))
	}

	// Create active subscription provider storage for each unique chain
	activeSubscriptionProvidersStorage := lavasession.NewActiveSubscriptionProvidersStorage()
	sessionManager := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, smartRouterMetricsManager, smartRouterReportsManager, smartRouterIdentifier, activeSubscriptionProvidersStorage)

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

	if lavasession.PeriodicProbeProviders {
		go sessionManager.PeriodicProbeProviders(ctx, lavasession.PeriodicProbeProvidersInterval)
	}

	// Helper function to convert provider endpoints to sessions
	convertProvidersToSessions := func(providerList []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		sessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider)
		for idx, provider := range providerList {
			// Only process providers matching this endpoint's API interface
			if provider.ApiInterface != rpcEndpoint.ApiInterface || provider.ChainID != rpcEndpoint.ChainID {
				continue
			}

			endpoints := []*lavasession.Endpoint{}
			for _, url := range provider.NodeUrls {
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

				endpoint := &lavasession.Endpoint{
					NetworkAddress:    url.Url,
					Enabled:           true,
					Addons:            extensions,
					Extensions:        extensions,
					Connections:       nil,
					DirectConnections: []lavasession.DirectRPCConnection{directConn}, // Smart router uses direct RPC
					Geolocation:       planstypes.Geolocation(provider.Geolocation),
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
			sessions[uint64(idx)] = providerEntry
		}
		return sessions
	}

	// ============================================================================
	// PHASE 1: Static Provider Validation
	// ============================================================================
	// Validate static providers BEFORE converting to sessions or registering.
	// Only validates providers matching this endpoint's api-interface.
	// See: the provider's validation approach for reference.
	var failedStaticSet map[*lavasession.RPCStaticProviderEndpoint]struct{}
	var failedStaticEndpoints []*lavasession.RPCStaticProviderEndpoint

	if len(relevantStaticProviderList) > 0 {
		utils.LavaFormatInfo("Validating static providers",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("providerCount", len(relevantStaticProviderList)),
		)

		totalAttemptedCount := 0
		failedStaticSet = make(map[*lavasession.RPCStaticProviderEndpoint]struct{})

		for _, staticProvider := range relevantStaticProviderList {
			// Skip providers with different api-interface (validated by their own endpoint)
			if staticProvider.ApiInterface != rpcEndpoint.ApiInterface {
				utils.LavaFormatDebug("Skipping provider - different api-interface",
					utils.LogAttr("provider", staticProvider.Name),
					utils.LogAttr("providerInterface", staticProvider.ApiInterface),
					utils.LogAttr("endpointInterface", rpcEndpoint.ApiInterface),
				)
				continue
			}
			totalAttemptedCount++

			// Prepare ALL URLs for validation together (matches provider behavior).
			// ChainRouter requires both with-addon and without-addon routes for addon URLs
			// (see chain_router.go:258 which appends "" to addons list).
			verificationNodeUrls := []common.NodeUrl{}
			for _, nodeUrl := range staticProvider.NodeUrls {
				verificationNodeUrls = append(verificationNodeUrls, nodeUrl)
				// For addon URLs, also add a non-addon copy for routing flexibility
				if len(nodeUrl.Addons) > 0 {
					noAddonUrl := nodeUrl
					noAddonUrl.Addons = []string{}
					verificationNodeUrls = append(verificationNodeUrls, noAddonUrl)
				}
			}

			verificationEndpoint := &lavasession.RPCProviderEndpoint{
				NetworkAddress: staticProvider.NetworkAddress,
				ChainID:        staticProvider.ChainID,
				ApiInterface:   staticProvider.ApiInterface,
				Geolocation:    staticProvider.Geolocation,
				NodeUrls:       verificationNodeUrls,
			}

			// Scoped context for this verification attempt. GetChainRouter creates
			// connector goroutines tied to ctx that only exit on cancellation.
			// Without this, temporary routers leak goroutines for the app lifetime.
			// Timeout bounds a hung provider (e.g. blackholed TCP) so it can't stall
			// validation of the remaining providers.
			verifyCtx, verifyCancel := context.WithTimeout(ctx, 30*time.Second)

			// Create chain router with all URLs for complete supportedMap (HTTP + WebSocket)
			parallelConnections := uint(lavasession.MaximumStreamsOverASingleConnection)
			verificationRouter, err := chainlib.GetChainRouter(verifyCtx, parallelConnections, verificationEndpoint, chainParser)
			if err != nil {
				verifyCancel()
				failedStaticSet[staticProvider] = struct{}{}
				failedStaticEndpoints = append(failedStaticEndpoints, staticProvider)
				utils.LavaFormatWarning("static provider: failed creating chain router — excluding from provider list", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", staticProvider.Name),
				)
				continue
			}

			// Create full ChainFetcher for verification (respects severity, skip-verifications)
			verificationFetcher := chainlib.NewChainFetcher(verifyCtx, &chainlib.ChainFetcherOptions{
				ChainRouter: verificationRouter,
				ChainParser: chainParser,
				Endpoint:    verificationEndpoint,
				Cache:       nil,
			})

			utils.LavaFormatInfo("Validating static provider",
				utils.LogAttr("name", staticProvider.Name),
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("urlCount", len(staticProvider.NodeUrls)),
			)

			err = verificationFetcher.Validate(verifyCtx)
			verifyCancel() // cleanup temporary router resources regardless of outcome
			if err != nil {
				failedStaticSet[staticProvider] = struct{}{}
				failedStaticEndpoints = append(failedStaticEndpoints, staticProvider)
				utils.LavaFormatWarning("static provider validation failed — excluding from provider list", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", staticProvider.Name),
				)
				continue
			}

			utils.LavaFormatInfo("Static provider validated successfully",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("provider", staticProvider.Name),
			)
		}

		healthyCount := totalAttemptedCount - len(failedStaticSet)

		// If ALL static providers failed verification, this endpoint cannot serve traffic
		if totalAttemptedCount > 0 && healthyCount == 0 {
			err := utils.LavaFormatError("all static providers failed verification — cannot serve endpoint", nil,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
				utils.LogAttr("failedCount", len(failedStaticSet)),
			)
			errCh <- err
			return err
		}

		if len(failedStaticSet) > 0 {
			utils.LavaFormatWarning("ATTENTION: some static providers failed verification and were excluded — they will be retried in the background",
				nil,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
				utils.LogAttr("failed", len(failedStaticSet)),
				utils.LogAttr("healthy", healthyCount),
			)
		} else {
			utils.LavaFormatInfo("All providers validated for api-interface",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
				utils.LogAttr("validated", healthyCount),
				utils.LogAttr("total", len(relevantStaticProviderList)),
			)
		}
	}

	// ============================================================================
	// PHASE 1B: Backup Provider Validation (non-fatal)
	// ============================================================================
	// Validate backup providers using the same logic as PHASE 1, but treat all
	// failures as non-fatal warnings. A broken backup should never block startup —
	// static providers must still serve. Operators are clearly notified at startup
	// so they can fix backup endpoints before they are actually needed in an emergency.
	// Providers that fail validation are excluded from the registered backup list.
	var failedBackupSet map[*lavasession.RPCStaticProviderEndpoint]struct{}

	if len(relevantBackupProviderList) > 0 {
		utils.LavaFormatInfo("Validating backup providers (non-fatal)",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("backupCount", len(relevantBackupProviderList)),
		)

		failedBackupSet = make(map[*lavasession.RPCStaticProviderEndpoint]struct{})
		validatedBackups := 0
		for _, backupProvider := range relevantBackupProviderList {
			if backupProvider.ApiInterface != rpcEndpoint.ApiInterface {
				utils.LavaFormatDebug("Skipping backup provider - different api-interface",
					utils.LogAttr("provider", backupProvider.Name),
					utils.LogAttr("providerInterface", backupProvider.ApiInterface),
					utils.LogAttr("endpointInterface", rpcEndpoint.ApiInterface),
				)
				continue
			}
			validatedBackups++

			// Build verificationNodeUrls with addon expansion (identical to PHASE 1)
			verificationNodeUrls := []common.NodeUrl{}
			for _, nodeUrl := range backupProvider.NodeUrls {
				verificationNodeUrls = append(verificationNodeUrls, nodeUrl)
				if len(nodeUrl.Addons) > 0 {
					noAddonUrl := nodeUrl
					noAddonUrl.Addons = []string{}
					verificationNodeUrls = append(verificationNodeUrls, noAddonUrl)
				}
			}

			verificationEndpoint := &lavasession.RPCProviderEndpoint{
				NetworkAddress: backupProvider.NetworkAddress,
				ChainID:        backupProvider.ChainID,
				ApiInterface:   backupProvider.ApiInterface,
				Geolocation:    backupProvider.Geolocation,
				NodeUrls:       verificationNodeUrls,
			}

			verifyCtx, verifyCancel := context.WithCancel(ctx)

			parallelConnections := uint(lavasession.MaximumStreamsOverASingleConnection)
			verificationRouter, err := chainlib.GetChainRouter(verifyCtx, parallelConnections, verificationEndpoint, chainParser)
			if err != nil {
				verifyCancel()
				failedBackupSet[backupProvider] = struct{}{}
				utils.LavaFormatWarning("backup provider: failed creating chain router — excluding from backup list", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", backupProvider.Name),
				)
				continue
			}

			verificationFetcher := chainlib.NewChainFetcher(verifyCtx, &chainlib.ChainFetcherOptions{
				ChainRouter: verificationRouter,
				ChainParser: chainParser,
				Endpoint:    verificationEndpoint,
				Cache:       nil,
			})

			utils.LavaFormatInfo("Validating backup provider",
				utils.LogAttr("name", backupProvider.Name),
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("urlCount", len(backupProvider.NodeUrls)),
			)

			err = verificationFetcher.Validate(verifyCtx)
			verifyCancel()
			if err != nil {
				failedBackupSet[backupProvider] = struct{}{}
				utils.LavaFormatWarning("backup provider validation failed — excluding from backup list", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", backupProvider.Name),
				)
				continue
			}

			utils.LavaFormatInfo("Backup provider validated successfully",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("provider", backupProvider.Name),
			)
		}

		if len(failedBackupSet) > 0 {
			utils.LavaFormatWarning("ATTENTION: some backup providers failed validation and were excluded — they will not be used during emergency failover",
				nil,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
				utils.LogAttr("failed", len(failedBackupSet)),
				utils.LogAttr("validated", validatedBackups),
			)
		} else {
			utils.LavaFormatInfo("All backup providers validated",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
				utils.LogAttr("validated", validatedBackups),
			)
		}
	}

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
	rpsr.mu.Unlock()

	// Launch background retry for failed static providers (if any)
	if len(failedStaticEndpoints) > 0 {
		failedNames := make([]string, len(failedStaticEndpoints))
		for i, p := range failedStaticEndpoints {
			failedNames[i] = p.Name
		}
		utils.LavaFormatInfo("Launching background retry goroutine for failed static providers",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("failedCount", len(failedStaticEndpoints)),
			utils.LogAttr("failedProviders", failedNames),
			utils.LogAttr("retryInterval", "3m"),
		)
		go rpsr.retryFailedStaticProviders(ctx, sessionManagerKey, chainParser, rpcEndpoint, convertProvidersToSessions)
	}

	var relaysMonitor *metrics.RelaysMonitor
	if options.cmdFlags.RelaysHealthEnableFlag {
		relaysMonitor = metrics.NewRelaysMonitor(options.cmdFlags.RelaysHealthIntervalFlag, rpcEndpoint.ChainID, rpcEndpoint.ApiInterface)
		relaysMonitorAggregator.RegisterRelaysMonitor(rpcEndpoint.String(), relaysMonitor)
	}

	rpcSmartRouterServer := &RPCSmartRouterServer{}

	// Create WebSocket subscription manager
	// Uses interface type to support both provider-based (ConsumerWSSubscriptionManager)
	// and direct RPC (DirectWSSubscriptionManager) implementations
	var wsSubscriptionManager chainlib.WSSubscriptionManager

	// Collect ALL WebSocket-capable endpoints from static providers for direct subscriptions
	// WebSocket URLs are identified by ws:// or wss:// prefix
	var wsEndpoints []*common.NodeUrl
	for _, provider := range healthyStaticProviders {
		for i := range provider.NodeUrls {
			url := strings.ToLower(provider.NodeUrls[i].Url)
			if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
				wsEndpoints = append(wsEndpoints, &provider.NodeUrls[i])
				utils.LavaFormatInfo("Found WebSocket endpoint for direct subscriptions",
					utils.LogAttr("url", provider.NodeUrls[i].Url),
					utils.LogAttr("provider", provider.Name),
					utils.LogAttr("chainID", provider.ChainID),
				)
			}
		}
	}

	// Create DirectWSSubscriptionManager if WebSocket endpoints are available
	// Otherwise fall back to provider-based subscription manager
	if len(wsEndpoints) > 0 {
		directWSManager := NewDirectWSSubscriptionManager(
			smartRouterMetricsManager,
			spectypes.APIInterfaceJsonRPC, // WebSocket subscriptions use JSON-RPC
			rpcEndpoint.ChainID,
			rpcEndpoint.ApiInterface,
			wsEndpoints,
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
			utils.LogAttr("optimizerEnabled", optimizer != nil),
		)
	} else {
		// No WebSocket endpoints configured — use NoOp manager that returns clear errors
		wsSubscriptionManager = NewNoOpWSSubscriptionManager(rpcEndpoint.ChainID, rpcEndpoint.ApiInterface)
		utils.LavaFormatInfo("No WebSocket endpoints configured for direct subscriptions",
			utils.LogAttr("chainID", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("hint", "Add ws:// or wss:// URLs to static-providers-list to enable subscriptions"),
		)
	}

	// Create gRPC streaming subscription manager for gRPC server-streaming methods
	// This supports Cosmos Event Streaming, Solana Geyser, and other gRPC streaming protocols
	var grpcEndpoints []*common.NodeUrl
	if rpcEndpoint.ApiInterface == spectypes.APIInterfaceGrpc {
		// Collect gRPC endpoints from static providers
		for _, provider := range healthyStaticProviders {
			if provider.ApiInterface == spectypes.APIInterfaceGrpc {
				for i := range provider.NodeUrls {
					grpcEndpoints = append(grpcEndpoints, &provider.NodeUrls[i])
					utils.LavaFormatInfo("Found gRPC endpoint for streaming subscriptions",
						utils.LogAttr("url", provider.NodeUrls[i].Url),
						utils.LogAttr("provider", provider.Name),
						utils.LogAttr("chainID", provider.ChainID),
					)
				}
			}
		}
	}

	// Initialize DirectGRPCSubscriptionManager if gRPC endpoints are available
	if len(grpcEndpoints) > 0 {
		grpcSubManager := NewDirectGRPCSubscriptionManager(
			smartRouterMetricsManager, // Metrics manager for tracking
			rpcEndpoint.ChainID,
			rpcEndpoint.ApiInterface,
			grpcEndpoints,
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
			utils.LogAttr("optimizerEnabled", optimizer != nil),
		)
	}

	// ============================================================================
	// PHASE 2: Chain Tracker Setup
	// ============================================================================
	// Create ChainTracker for latest block tracking using first healthy provider.
	// ChainTracker polls for latest block and maintains block history for sync verification.
	// Uses healthyStaticProviders (not the unfiltered list) to avoid polling a dead node.
	var chainTracker chaintracker.IChainTracker
	if len(healthyStaticProviders) > 0 {
		firstProvider := healthyStaticProviders[0]

		// Minimal endpoint for ChainTracker (no addons needed, only polls latest block)
		chainTrackerEndpoint := &lavasession.RPCProviderEndpoint{
			NetworkAddress: firstProvider.NetworkAddress,
			ChainID:        firstProvider.ChainID,
			ApiInterface:   firstProvider.ApiInterface,
			Geolocation:    firstProvider.Geolocation,
			NodeUrls: []common.NodeUrl{
				{
					Url:        firstProvider.NodeUrls[0].Url,
					AuthConfig: firstProvider.NodeUrls[0].AuthConfig,
					Addons:     []string{},
				},
			},
		}

		parallelConnections := uint(lavasession.MaximumStreamsOverASingleConnection)
		chainRouter, err := chainlib.GetChainRouter(ctx, parallelConnections, chainTrackerEndpoint, chainParser)
		if err != nil {
			utils.LavaFormatWarning("Failed to create chain router for chain tracker", err,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
			)
		} else {
			// Full ChainFetcher for chain tracker (matches provider behavior)
			chainFetcher := chainlib.NewChainFetcher(ctx, &chainlib.ChainFetcherOptions{
				ChainRouter: chainRouter,
				ChainParser: chainParser,
				Endpoint:    chainTrackerEndpoint,
				Cache:       options.cache,
			})

			_, averageBlockTime, blocksToFinalization, blocksInFinalizationData := chainParser.ChainBlockStats()
			blocksToSaveChainTracker := uint64(blocksToFinalization + blocksInFinalizationData)

			chainTrackerConfig := chaintracker.ChainTrackerConfig{
				BlocksToSave:          blocksToSaveChainTracker,
				AverageBlockTime:      averageBlockTime,
				ServerBlockMemory:     chaintracker.ChainTrackerDefaultMemory + blocksToSaveChainTracker,
				ChainId:               rpcEndpoint.ChainID,
				ParseDirectiveEnabled: chainParser.ParseDirectiveEnabled(),
			}

			chainTracker, err = chaintracker.NewChainTracker(ctx, chainFetcher, chainTrackerConfig)
			if err != nil {
				utils.LavaFormatWarning("Failed to create chain tracker, sync tracking disabled", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
				)
				chainTracker = nil
			} else {
				go func() {
					err := chainTracker.StartAndServe(ctx)
					if err != nil {
						utils.LavaFormatError("Chain tracker failed", err,
							utils.LogAttr("chain", rpcEndpoint.ChainID),
						)
					}
				}()

				utils.LavaFormatInfo("Chain tracker started",
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("pollingInterval", averageBlockTime/time.Duration(chaintracker.MostFrequentPollingMultiplier)),
					utils.LogAttr("blocksToSave", blocksToSaveChainTracker),
				)
			}
		}
	}

	if chainTracker == nil {
		utils.LavaFormatInfo("Starting without chain tracker (sync tracking disabled)",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
		)
	}

	utils.LavaFormatInfo("RPCSmartRouter Listening", utils.Attribute{Key: "endpoints", Value: rpcEndpoint.String()})
	// Convert smartRouterIdentifier string to empty sdk.AccAddress for smart router
	err = rpcSmartRouterServer.ServeRPCRequests(ctx, rpcEndpoint, chainParser, chainTracker, sessionManager, options.cache, rpcSmartRouterMetrics, smartRouterConsistency, relaysMonitor, options.cmdFlags, options.stateShare, wsSubscriptionManager, smartRouterMetricsManager)
	if err != nil {
		err = utils.LavaFormatError("failed serving rpc requests", err, utils.Attribute{Key: "endpoint", Value: rpcEndpoint})
		errCh <- err
		return err
	}

	// Store server reference for per-endpoint ChainTracker cleanup on epoch updates
	rpsr.mu.Lock()
	rpsr.rpcServers[sessionManagerKey] = rpcSmartRouterServer
	rpsr.mu.Unlock()

	return nil
}

func ParseEndpoints(viper_endpoints *viper.Viper, geolocation uint64) (endpoints []*lavasession.RPCEndpoint, err error) {
	err = viper_endpoints.UnmarshalKey(common.EndpointsConfigName, &endpoints)
	if err != nil {
		utils.LavaFormatFatal("could not unmarshal endpoints", err, utils.Attribute{Key: "viper_endpoints", Value: viper_endpoints.AllSettings()})
	}
	for _, endpoint := range endpoints {
		endpoint.Geolocation = geolocation
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
		Long: `rpcsmartrouter sets up a centralized server with static and backup providers to perform api requests through the lava protocol.
		This is the smart router mode that uses pre-configured static providers instead of dynamically discovering providers on-chain.
		all configs should be located in the local running directory /config or ` + lavaDefaultNodeHome + `
		if no arguments are passed, assumes default config file: ` + DefaultRPCSmartRouterFileName + `
		if one argument is passed, its assumed the config file name
		`,
		Example: `required flags: --geolocation 1 --static-providers ...
rpcsmartrouter <flags>
rpcsmartrouter rpcsmartrouter_conf <flags>
rpcsmartrouter 127.0.0.1:3333 OSMOSIS tendermintrpc 127.0.0.1:3334 OSMOSIS rest <flags>
rpcsmartrouter smartrouter_examples/full_smartrouter_example.yml --cache-be "127.0.0.1:7778" --geolocation 1 [--debug-relays] --log_level <debug|warn|...>`,
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
			// set viper
			config_name := DefaultRPCSmartRouterFileName
			if len(args) == 1 {
				config_name = args[0] // name of config file (without extension)
			}
			viper.SetConfigName(config_name)
			viper.SetConfigType("yml")
			viper.AddConfigPath(".")
			viper.AddConfigPath("./config")
			viper.AddConfigPath(lavaDefaultNodeHome)

			// Bind all cobra flags to viper so viper.GetString/GetBool works.
			// Previously Cosmos SDK's AddTxFlagsToCmd did this automatically.
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

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
				utils.LavaFormatFatal("could not load config file", err, utils.Attribute{Key: "expected_config_name", Value: viper.ConfigFileUsed()})
			} else {
				utils.LavaFormatInfo("read config file successfully", utils.Attribute{Key: "expected_config_name", Value: viper.ConfigFileUsed()})
			}
			geolocation, err := cmd.Flags().GetUint64(lavasession.GeolocationFlag)
			if err != nil {
				utils.LavaFormatFatal("failed to read geolocation flag, required flag", err)
			}
			rpcEndpoints, err = ParseEndpoints(viper.GetViper(), geolocation)
			if err != nil || len(rpcEndpoints) == 0 {
				return utils.LavaFormatError("invalid endpoints definition", err)
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
					pyroscopeAppName = "lavap-smartrouter"
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

			// Parse direct RPC endpoints (new key: "direct-rpc", backward compat: "static-providers")
			var directRPCEndpoints []*lavasession.RPCStaticProviderEndpoint
			directRPCConfigKey := common.DirectRPCConfigName
			if !viper.IsSet(directRPCConfigKey) {
				directRPCConfigKey = common.StaticProvidersConfigName // backward compat
			}
			if viper.IsSet(directRPCConfigKey) {
				directRPCEndpoints, err = ParseStaticProviderEndpoints(viper.GetViper(), directRPCConfigKey, geolocation)
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

			// Parse backup direct RPC endpoints (new key: "backup-direct-rpc", backward compat: "backup-providers")
			var backupDirectRPCEndpoints []*lavasession.RPCStaticProviderEndpoint
			backupConfigKey := common.BackupDirectRPCConfigName
			if !viper.IsSet(backupConfigKey) {
				backupConfigKey = common.BackupProvidersConfigName // backward compat
			}
			if viper.IsSet(backupConfigKey) {
				utils.LavaFormatInfo("Backup direct-rpc config found", utils.Attribute{Key: "configKey", Value: backupConfigKey})
				backupDirectRPCEndpoints, err = ParseStaticProviderEndpoints(viper.GetViper(), backupConfigKey, geolocation)
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
			utils.LavaFormatInfo("lavap Binary Version: " + protocoltypes.DefaultVersion.ConsumerTarget)
			rand.InitRandomSeed()

			var cache *performance.Cache = nil
			cacheAddr, err := cmd.Flags().GetString(performance.CacheFlagName)
			if err != nil {
				utils.LavaFormatError("Failed To Get Cache Address flag", err, utils.Attribute{Key: "flags", Value: cmd.Flags()})
			} else if cacheAddr != "" {
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
				MetricsListenAddress:  viper.GetString(metrics.MetricsListenFlagName),
				RelayServerAddress:    viper.GetString(metrics.RelayServerFlagName),
				RelayKafkaAddress:     viper.GetString(metrics.RelayKafkaFlagName),
				RelayKafkaTopic:       viper.GetString(metrics.RelayKafkaTopicFlagName),
				RelayKafkaUsername:    viper.GetString(metrics.RelayKafkaUsernameFlagName),
				RelayKafkaPassword:    viper.GetString(metrics.RelayKafkaPasswordFlagName),
				RelayKafkaMechanism:   viper.GetString(metrics.RelayKafkaMechanismFlagName),
				RelayKafkaTLSEnabled:  viper.GetBool(metrics.RelayKafkaTLSEnabledFlagName),
				RelayKafkaTLSInsecure: viper.GetBool(metrics.RelayKafkaTLSInsecureFlagName),
				ReportsAddressFlag:    viper.GetString(reportsSendBEAddress),
				OptimizerQoSAddress:   viper.GetString(common.OptimizerQosServerAddressFlag),
				OptimizerQoSListen:    viper.GetBool(common.OptimizerQosListenFlag),
			}

			maxConcurrentProviders := viper.GetUint(common.MaximumConcurrentProvidersFlagName)
			if err := scoreutils.SetProbeUpdateWeight(viper.GetFloat64(common.ProbeUpdateWeightFlagName)); err != nil {
				return err
			}
			weightedSelectorConfig := provideroptimizer.DefaultWeightedSelectorConfig()
			weightedSelectorConfig.AvailabilityWeight = viper.GetFloat64(common.ProviderOptimizerAvailabilityWeight)
			weightedSelectorConfig.LatencyWeight = viper.GetFloat64(common.ProviderOptimizerLatencyWeight)
			weightedSelectorConfig.SyncWeight = viper.GetFloat64(common.ProviderOptimizerSyncWeight)
			weightedSelectorConfig.StakeWeight = viper.GetFloat64(common.ProviderOptimizerStakeWeight)
			weightedSelectorConfig.MinSelectionChance = viper.GetFloat64(common.ProviderOptimizerMinSelectionChance)
			weightedSelectorConfig.Strategy = strategyFlag.Strategy

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
			err = rpcSmartRouter.Start(ctx, &rpcSmartRouterStartOptions{
				rpcEndpoints:             rpcEndpoints,
				cache:                    cache,
				strategy:                 strategyFlag.Strategy,
				maxConcurrentProviders:   maxConcurrentProviders,
				analyticsServerAddresses: analyticsServerAddresses,
				cmdFlags:                 consumerPropagatedFlags,
				stateShare:               rpcSmartRouterSharedState,
				staticProvidersList:      directRPCEndpoints,
				backupProvidersList:      backupDirectRPCEndpoints,
				geoLocation:              geolocation,
				weightedSelectorConfig:   weightedSelectorConfig,
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
	cmdRPCSmartRouter.Flags().Uint64(common.GeolocationFlag, 0, "geolocation to run from")
	cmdRPCSmartRouter.Flags().Uint(common.MaximumConcurrentProvidersFlagName, 3, "max number of concurrent providers to communicate with")
	cmdRPCSmartRouter.MarkFlagRequired(common.GeolocationFlag)
	cmdRPCSmartRouter.Flags().Bool(lavasession.AllowInsecureConnectionToProvidersFlag, false, "allow insecure provider-dialing. used for development and testing")
	cmdRPCSmartRouter.Flags().String(common.ResponseCompressionFlag, common.DefaultResponseCompression, "client-facing response compression: gzip (default), brotli, or off")
	cmdRPCSmartRouter.Flags().Uint64Var(&lavasession.MaximumStreamsOverASingleConnection, lavasession.MaximumStreamsOverASingleConnectionFlag, lavasession.DefaultMaximumStreamsOverASingleConnection, "maximum number of parallel streams over a single provider connection")
	cmdRPCSmartRouter.Flags().Bool(common.TestModeFlagName, false, "test mode sends dummy data and prints all metadata in listeners")
	cmdRPCSmartRouter.Flags().String(performance.PprofAddressFlagName, "", "pprof server address, used for code profiling")
	cmdRPCSmartRouter.Flags().String("debug-address", "", "debug HTTP server for integration tests, e.g. :9999 — exposes /debug/time-warp to shift QoS clock")
	if err := viper.BindPFlag("debug-address", cmdRPCSmartRouter.Flags().Lookup("debug-address")); err != nil {
		utils.LavaFormatFatal("failed binding debug-address flag", err)
	}
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeAddressFlagName, "", "pyroscope server address for continuous profiling (e.g., http://pyroscope:4040)")
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeAppNameFlagName, "lavap-smartrouter", "pyroscope application name for identifying this service")
	cmdRPCSmartRouter.Flags().Int(performance.PyroscopeMutexProfileFractionFlagName, performance.DefaultMutexProfileFraction, "mutex profile sampling rate (1 in N mutex events)")
	cmdRPCSmartRouter.Flags().Int(performance.PyroscopeBlockProfileRateFlagName, performance.DefaultBlockProfileRate, "block profile rate in nanoseconds (1 records all blocking events)")
	cmdRPCSmartRouter.Flags().String(performance.PyroscopeTagsFlagName, "", "comma-separated list of tags in key=value format (e.g., instance=router-1,region=us-east)")
	cmdRPCSmartRouter.Flags().String(performance.CacheFlagName, "", "address for a cache server to improve performance")
	cmdRPCSmartRouter.Flags().Var(&strategyFlag, "strategy", fmt.Sprintf("the strategy to use to pick providers (%s)", strings.Join(strategyNames, "|")))
	defaultWeightedConfig := provideroptimizer.DefaultWeightedSelectorConfig()
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerAvailabilityWeight, defaultWeightedConfig.AvailabilityWeight, "weight assigned to provider availability when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerLatencyWeight, defaultWeightedConfig.LatencyWeight, "weight assigned to provider latency when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerSyncWeight, defaultWeightedConfig.SyncWeight, "weight assigned to provider sync freshness when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerStakeWeight, defaultWeightedConfig.StakeWeight, "weight assigned to provider stake when computing selection scores")
	cmdRPCSmartRouter.Flags().Float64(common.ProviderOptimizerMinSelectionChance, defaultWeightedConfig.MinSelectionChance, "minimum selection probability for any provider regardless of score")
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
	cmdRPCSmartRouter.Flags().String(metrics.MetricsListenFlagName, metrics.DisabledFlagOption, "the address to expose prometheus metrics (such as localhost:7779)")
	cmdRPCSmartRouter.Flags().String(metrics.RelayServerFlagName, metrics.DisabledFlagOption, "the http address of the relay usage server api endpoint (example http://127.0.0.1:8080)")
	cmdRPCSmartRouter.Flags().String(metrics.RelayKafkaFlagName, metrics.DisabledFlagOption, "the kafka address for sending relay metrics (example localhost:9092)")
	cmdRPCSmartRouter.Flags().String(metrics.RelayKafkaTopicFlagName, "lava-relay-metrics", "the kafka topic for sending relay metrics")
	cmdRPCSmartRouter.Flags().String(metrics.RelayKafkaUsernameFlagName, "", "kafka username for SASL authentication")
	cmdRPCSmartRouter.Flags().String(metrics.RelayKafkaPasswordFlagName, "", "kafka password for SASL authentication")
	cmdRPCSmartRouter.Flags().String(metrics.RelayKafkaMechanismFlagName, "SCRAM-SHA-512", "kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	cmdRPCSmartRouter.Flags().Bool(metrics.RelayKafkaTLSEnabledFlagName, false, "enable TLS for kafka connections")
	cmdRPCSmartRouter.Flags().Bool(metrics.RelayKafkaTLSInsecureFlagName, false, "skip TLS certificate verification for kafka connections")
	cmdRPCSmartRouter.Flags().Bool(DebugRelaysFlagName, false, "adding debug information to relays")
	cmdRPCSmartRouter.Flags().Bool(common.EnableSelectionStatsHeaderFlag, false, "enable selection stats header for debugging provider selection")
	// CORS related flags
	cmdRPCSmartRouter.Flags().String(common.CorsCredentialsFlag, "true", "Set up CORS allowed credentials,default \"true\"")
	cmdRPCSmartRouter.Flags().String(common.CorsHeadersFlag, "", "Set up CORS allowed headers, * for all, default simple cors specification headers")
	cmdRPCSmartRouter.Flags().String(common.CorsOriginFlag, "*", "Set up CORS allowed origin, enabled * by default")
	cmdRPCSmartRouter.Flags().String(common.CorsMethodsFlag, "GET,POST,PUT,DELETE,OPTIONS", "set up Allowed OPTIONS methods, defaults to: \"GET,POST,PUT,DELETE,OPTIONS\"")
	cmdRPCSmartRouter.Flags().String(common.CDNCacheDurationFlag, "86400", "set up preflight options response cache duration, default 86400 (24h in seconds)")
	cmdRPCSmartRouter.Flags().Bool(common.SharedStateFlag, false, "Share the consumer consistency state with the cache service. this should be used with cache backend enabled if you want to state sync multiple rpc consumers")
	// relays health check related flags
	cmdRPCSmartRouter.Flags().Bool(common.RelaysHealthEnableFlag, RelaysHealthEnableFlagDefault, "enables relays health check")
	cmdRPCSmartRouter.Flags().Duration(common.RelayHealthIntervalFlag, RelayHealthIntervalFlagDefault, "interval between relay health checks")
	cmdRPCSmartRouter.Flags().String(reportsSendBEAddress, "", "address to send reports to")
	cmdRPCSmartRouter.Flags().BoolVar(&lavasession.DebugProbes, DebugProbesFlagName, false, "adding information to probes")
	cmdRPCSmartRouter.Flags().StringArray(common.UseStaticSpecFlag, nil, "load specs from file, directory, or remote URL (GitHub/GitLab). Can be specified multiple times; later sources override earlier ones for same chain ID")
	cmdRPCSmartRouter.Flags().String(common.GitHubTokenFlag, "", "GitHub personal access token for accessing private repositories and higher API rate limits (5,000 requests/hour vs 60 for unauthenticated)")
	cmdRPCSmartRouter.Flags().String(common.GitLabTokenFlag, "", "GitLab personal access token for accessing private repositories (supports gitlab.com and self-hosted instances)")
	cmdRPCSmartRouter.Flags().Duration(common.EpochDurationFlag, 0, "duration of each epoch for time-based epoch system (e.g., 30m, 1h). If not set, epochs are disabled")
	cmdRPCSmartRouter.Flags().Duration(common.ShutdownGracePeriodFlag, common.DefaultShutdownGracePeriod, "graceful shutdown deadline for in-flight requests and WebSocket clients")
	cmdRPCSmartRouter.Flags().IntVar(&relaycore.RelayRetryLimit, common.SetRelayRetryLimitFlag, 2, "max total relay retry attempts across all error types (node and protocol errors combined; 0 disables retries)")
	cmdRPCSmartRouter.Flags().BoolVar(&rpcInterfaceMessages.BatchNodeErrorOnAny, common.BatchNodeErrorOnAnyFlag, false, "if true, batch requests are treated as node errors if ANY sub-request fails; if false (default), only if ALL fail")
	// optimizer qos reports
	cmdRPCSmartRouter.Flags().String(common.OptimizerQosServerAddressFlag, "", "address to send optimizer qos reports to")
	cmdRPCSmartRouter.Flags().Bool(common.OptimizerQosListenFlag, false, "enable listening for optimizer qos reports on metrics endpoint i.e GET -> localhost:7779/provider_optimizer_metrics")
	cmdRPCSmartRouter.Flags().DurationVar(&metrics.OptimizerQosServerPushInterval, common.OptimizerQosServerPushIntervalFlag, time.Minute*5, "interval to push optimizer qos reports")
	cmdRPCSmartRouter.Flags().DurationVar(&metrics.OptimizerQosServerSamplingInterval, common.OptimizerQosServerSamplingIntervalFlag, time.Second*1, "interval to sample optimizer qos reports")
	// metrics
	cmdRPCSmartRouter.Flags().BoolVar(&metrics.ShowProviderEndpointInMetrics, common.ShowProviderEndpointInMetricsFlagName, metrics.ShowProviderEndpointInMetrics, "show provider endpoint in consumer metrics")
	// websocket flags
	cmdRPCSmartRouter.Flags().IntVar(&chainlib.WebSocketRateLimit, common.RateLimitWebSocketFlag, chainlib.WebSocketRateLimit, "rate limit (per second) websocket requests per user connection, default is unlimited")
	cmdRPCSmartRouter.Flags().Int64Var(&chainlib.MaximumNumberOfParallelWebsocketConnectionsPerIp, common.LimitParallelWebsocketConnectionsPerIpFlag, chainlib.MaximumNumberOfParallelWebsocketConnectionsPerIp, "limit number of parallel connections to websocket, per ip, default is unlimited (0)")
	cmdRPCSmartRouter.Flags().Int64Var(&chainlib.MaxIdleTimeInSeconds, common.LimitWebsocketIdleTimeFlag, chainlib.MaxIdleTimeInSeconds, "limit the idle time in seconds for a websocket connection, default is 20 minutes ( 20 * 60 )")
	cmdRPCSmartRouter.Flags().DurationVar(&chainlib.WebSocketBanDuration, common.BanDurationForWebsocketRateLimitExceededFlag, chainlib.WebSocketBanDuration, "once websocket rate limit is reached, user will be banned Xfor a duration, default no ban")

	cmdRPCSmartRouter.Flags().BoolVar(&chainlib.SkipWebsocketVerification, common.SkipWebsocketVerificationFlag, chainlib.SkipWebsocketVerification, "skip websocket verification for chains that require ws/wss endpoints")

	cmdRPCSmartRouter.Flags().BoolVar(&lavasession.PeriodicProbeProviders, common.PeriodicProbeProvidersFlagName, lavasession.PeriodicProbeProviders, "enable periodic probing of providers")
	cmdRPCSmartRouter.Flags().DurationVar(&lavasession.PeriodicProbeProvidersInterval, common.PeriodicProbeProvidersIntervalFlagName, lavasession.PeriodicProbeProvidersInterval, "interval for periodic probing of providers")
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

	common.AddRollingLogConfig(cmdRPCSmartRouter)
	// Log level/format flags (previously provided by cosmos-sdk AddTxFlagsToCmd)
	cmdRPCSmartRouter.Flags().String("log-level", "info", "log level (debug|info|warn|error|fatal)")
	cmdRPCSmartRouter.Flags().String("log-format", "text", "log format (text|json)")
	return cmdRPCSmartRouter
}

func (rpsr *RPCSmartRouter) updateEpoch(epoch uint64) {
	// Copy session manager keys under lock to avoid iterating the map
	// concurrently with retryFailedStaticProviders which writes to rpsr maps under rpsr.mu.
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
		// lava_rpc_endpoint_overall_health gauge stays stuck at 0 (unhealthy) forever,
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
		// This prevents races with retryFailedStaticProviders, which merges
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
			freshBackupSessions[idx] = freshSession

			utils.LavaFormatDebug("Created fresh backup provider session for epoch",
				utils.LogAttr("provider", freshSession.PublicLavaAddress),
				utils.LogAttr("epoch", epoch),
				utils.LogAttr("chainKey", chainKeyLog))
		}

		// Update stored sessions with fresh objects
		rpsr.providerSessions[chainKey] = freshProviderSessions
		if len(freshBackupSessions) > 0 {
			rpsr.backupProviderSessions[chainKey] = freshBackupSessions
		}
		server := rpsr.rpcServers[chainKey]

		// UpdateAllProviders stays under rpsr.mu so the (rpsr.providerSessions write
		// → csm push) pair is atomic with retryFailedStaticProviders' matching pair.
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

		// cleanupStaleTrackers is the genuinely heavy work (tracker teardown +
		// connection close) and stays outside the lock. Must run AFTER
		// UpdateAllProviders so connections are closed first.
		if server != nil {
			rpsr.cleanupStaleTrackers(chainKey, server, freshProviderSessions, freshBackupSessions)
		}
	}
}

// retryFailedStaticProviders periodically re-validates failed static providers
// and re-registers them with the session manager when they recover.
// It runs as a background goroutine, one per endpoint that had failures.
func (rpsr *RPCSmartRouter) retryFailedStaticProviders(
	ctx context.Context,
	sessionManagerKey string,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	retryInterval := 3 * time.Minute // same as SpecValidator's disabled-chain interval
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		rpsr.mu.Lock()
		failedProviders := rpsr.failedStaticProviders[sessionManagerKey]
		rpsr.mu.Unlock()

		if len(failedProviders) == 0 {
			utils.LavaFormatInfo("All failed static providers recovered — stopping retry loop",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			)
			return
		}

		utils.LavaFormatInfo("Retrying failed static providers",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
			utils.LogAttr("failedCount", len(failedProviders)),
		)

		var stillFailed []*lavasession.RPCStaticProviderEndpoint
		var recovered []*lavasession.RPCStaticProviderEndpoint

		for _, provider := range failedProviders {
			// Build verification endpoint (same logic as Phase 1)
			verificationNodeUrls := []common.NodeUrl{}
			for _, nodeUrl := range provider.NodeUrls {
				verificationNodeUrls = append(verificationNodeUrls, nodeUrl)
				if len(nodeUrl.Addons) > 0 {
					noAddonUrl := nodeUrl
					noAddonUrl.Addons = []string{}
					verificationNodeUrls = append(verificationNodeUrls, noAddonUrl)
				}
			}

			verificationEndpoint := &lavasession.RPCProviderEndpoint{
				NetworkAddress: provider.NetworkAddress,
				ChainID:        provider.ChainID,
				ApiInterface:   provider.ApiInterface,
				Geolocation:    provider.Geolocation,
				NodeUrls:       verificationNodeUrls,
			}

			// Scoped context for this verification attempt. GetChainRouter creates
			// connector goroutines tied to ctx that only exit on cancellation.
			// Without this, each retry iteration leaks goroutines and connections
			// for permanently failing providers.
			// Timeout bounds a hung provider so it can't stall retries of the
			// remaining providers in this cycle.
			attemptCtx, attemptCancel := context.WithTimeout(ctx, 30*time.Second)

			parallelConnections := uint(lavasession.MaximumStreamsOverASingleConnection)
			verificationRouter, err := chainlib.GetChainRouter(attemptCtx, parallelConnections, verificationEndpoint, chainParser)
			if err != nil {
				attemptCancel()
				stillFailed = append(stillFailed, provider)
				utils.LavaFormatWarning("retry: static provider chain router creation still failing", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", provider.Name),
				)
				continue
			}

			verificationFetcher := chainlib.NewChainFetcher(attemptCtx, &chainlib.ChainFetcherOptions{
				ChainRouter: verificationRouter,
				ChainParser: chainParser,
				Endpoint:    verificationEndpoint,
				Cache:       nil,
			})

			err = verificationFetcher.Validate(attemptCtx)
			attemptCancel() // cleanup temporary router resources regardless of outcome
			if err != nil {
				stillFailed = append(stillFailed, provider)
				utils.LavaFormatWarning("retry: static provider verification still failing", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
					utils.LogAttr("provider", provider.Name),
				)
				continue
			}

			recovered = append(recovered, provider)
			utils.LavaFormatInfo("[+] static provider recovered and passed verification",
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("provider", provider.Name),
			)
		}

		// Update state: move recovered providers into active sessions
		if len(recovered) > 0 {
			recoveredSessions := convertProvidersToSessions(recovered)

			rpsr.mu.Lock()
			currentEpoch := rpsr.epochTimer.GetCurrentEpoch()

			// Copy-on-write: create a new map merging old + recovered sessions.
			// The old map may still be referenced by goroutines (probeProviders,
			// cleanupStaleTrackers) that iterate it without the lock.
			oldSessions := rpsr.providerSessions[sessionManagerKey]
			mergedSessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(oldSessions)+len(recoveredSessions))
			for k, v := range oldSessions {
				mergedSessions[k] = v
			}
			maxIdx := uint64(0)
			for idx := range mergedSessions {
				if idx >= maxIdx {
					maxIdx = idx + 1
				}
			}
			for _, session := range recoveredSessions {
				session.Lock.Lock()
				session.PairingEpoch = currentEpoch
				session.Lock.Unlock()
				mergedSessions[maxIdx] = session
				maxIdx++
			}
			rpsr.providerSessions[sessionManagerKey] = mergedSessions

			// Update failed list
			rpsr.failedStaticProviders[sessionManagerKey] = stillFailed

			sessionManager := rpsr.sessionManagers[sessionManagerKey]
			backupSessions := rpsr.backupProviderSessions[sessionManagerKey]

			// UpdateAllProviders stays under rpsr.mu so this (rpsr.providerSessions
			// write → csm push) pair is atomic with updateEpoch's matching pair.
			// Otherwise a concurrent epoch tick can push to csm in the opposite
			// order it wrote rpsr.providerSessions, silently dropping recovered
			// providers until the next epoch.
			err := sessionManager.UpdateAllProviders(currentEpoch, mergedSessions, backupSessions)
			rpsr.mu.Unlock()

			if err != nil {
				utils.LavaFormatWarning("retry: failed to re-register recovered providers", err,
					utils.LogAttr("chain", rpcEndpoint.ChainID),
				)
			} else {
				for _, p := range recovered {
					utils.LavaFormatInfo("[+] static provider re-registered successfully",
						utils.LogAttr("chain", rpcEndpoint.ChainID),
						utils.LogAttr("provider", p.Name),
					)
				}
			}
		} else {
			rpsr.mu.Lock()
			rpsr.failedStaticProviders[sessionManagerKey] = stillFailed
			rpsr.mu.Unlock()
		}
	}
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
