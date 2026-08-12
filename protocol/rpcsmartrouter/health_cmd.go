package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/protocol/statetracker"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// healthVerification is one spec verification's result, as emitted in the JSON report.
type healthVerification struct {
	Name      string `json:"name"`
	Addon     string `json:"addon"`
	Extension string `json:"extension"`
	Ok        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

// healthEndpointResult is one (provider, chain, interface, node-url) probe result.
// One provider with multiple node-urls (e.g. an https + a wss endpoint) yields one
// row per url, distinguished by `url`/`transport`.
type healthEndpointResult struct {
	Name         string   `json:"name"`
	ChainID      string   `json:"chainId"`
	APIInterface string   `json:"apiInterface"`
	URL          string   `json:"url"`
	Transport    string   `json:"transport"`
	Addons       []string `json:"addons"`
	Extensions   []string `json:"extensions"`
	SpecValid    bool     `json:"specValid"`
	// LatestBlock is this URL's own height when a verification measured one. When it was
	// backfilled instead (see backfillLatestBlock) it is the ENDPOINT's height, obtained
	// through the router spanning every probed URL. Reporting only — never a verdict.
	LatestBlock   int64                `json:"latestBlock"`
	Ok            bool                 `json:"ok"`
	Error         string               `json:"error,omitempty"`
	Verifications []healthVerification `json:"verifications"`
}

// healthReport is the single, uniformly-shaped JSON document written to stdout.
// Consumers always parse this envelope and read `.error`/`.results` — they never
// inspect the process exit code (which is 0 for any completed run).
type healthReport struct {
	Ok      bool                   `json:"ok"`
	Error   *string                `json:"error"`
	Results []healthEndpointResult `json:"results"`
}

// healthProvider is the normalized probe target, sourced from either the config
// file (direct-rpc / backup-direct-rpc) or inline CLI args.
type healthProvider struct {
	name         string
	chainID      string
	apiInterface string
	nodeUrls     []commonlib.NodeUrl
}

// CreateHealthCobraCommand builds the `smartrouter health` command: a one-shot,
// spec-driven probe that crafts and sends the relays each spec defines (the universal
// GET_BLOCKNUM, plus every verification for the node's declared addons/extensions —
// archive/debug/trace/websocket) against every configured node URL, then prints a
// single JSON document to stdout. It is intentionally chain-agnostic: adding a new
// chain spec is the only thing ever required to support a new chain.
func CreateHealthCobraCommand() *cobra.Command {
	cmdHealth := &cobra.Command{
		Use:   `health [config-file] | { listen-ip:listen-port spec-chain-id api-interface ... }`,
		Short: `Spec-driven health probe of configured endpoints — emits a JSON report to stdout`,
		Long: `health loads the spec for every configured (chain, api-interface) and sends the
relays the spec itself defines to each node URL — the standard latest-block call plus
every verification declared for the node's addons/extensions (archive/debug/trace and,
when the spec supports subscriptions, websocket). It is fully spec-driven: no per-chain
or per-interface code is involved, so any chain with a spec works out of the box.

The result is a single JSON document on stdout (logs go to stderr). The process exits 0
for any completed run — endpoint failures are reported as data (ok:false, error:"..."),
never as a non-zero exit. Only a fatal setup error (bad config, missing --use-static-spec)
exits non-zero, and even then a JSON envelope with a populated "error" is printed first.

Endpoints can come from a smartrouter config file (probes every node-url under direct-rpc),
or from inline "address chain-id api-interface" triplets like the rpcsmartrouter command.`,
		Example: `  smartrouter health config/smartrouter_examples/smartrouter_eth.yml --use-static-spec specs/
  smartrouter health https://eth1.lava.build ETH1 jsonrpc --use-static-spec specs/`,
		Args: func(cmd *cobra.Command, args []string) error {
			// Either: 0-1 args (config file), or repeated groups of 3 (inline endpoints).
			if len(args) <= 1 {
				return nil
			}
			if len(args)%len(Yaml_config_properties) != 0 {
				return fmt.Errorf("invalid number of arguments: inline endpoints must be repeated groups of %d (address chain-id api-interface), got %d", len(Yaml_config_properties), len(args))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// This command sets a few chainlib package-level globals (JsonFormat,
			// IgnoreWsEnforcementForTestCommands, SkipWebsocketVerification). That's safe
			// because each CLI invocation runs exactly one subcommand in a one-shot process
			// then exits — the same pattern the `test` command uses (see testing.go).

			// Logs to stderr so stdout carries only the JSON report.
			utils.JsonFormat = true
			logLevel, _ := cmd.Flags().GetString("log-level")
			utils.SetGlobalLoggingLevel(logLevel)

			// The smart router has no live blockchain spec query — specs must be static.
			// WS-only setups must not panic in a one-shot diagnostic.
			chainlib.IgnoreWsEnforcementForTestCommands = true

			ctx := context.Background()
			staticSpecPaths, err := cmd.Flags().GetStringArray(commonlib.UseStaticSpecFlag)
			if err != nil {
				return emitFatal(err)
			}
			if len(staticSpecPaths) == 0 {
				return emitFatal(fmt.Errorf("--use-static-spec is required (smart-router mode has no live spec source)"))
			}

			includeBackup, _ := cmd.Flags().GetBool("include-backup")

			providers, err := collectHealthProviders(args, includeBackup)
			if err != nil {
				return emitFatal(err)
			}
			if len(providers) == 0 {
				return emitFatal(fmt.Errorf("no endpoints to probe — config has no direct-rpc providers and no inline endpoints were given"))
			}

			// Specs whose collection supports subscriptions augment every verification with the
			// websocket extension, and ws:// node URLs are probed by default — this is what makes
			// the health check exercise the full surface a supported chain exposes. The ws connector
			// dials with a deadline derived from --timeout (it checks ctx.Err() between handshake
			// retries), so a blocked ws node aborts at the timeout rather than hanging the run.
			// Pass --skip-websocket-verification to exclude ws endpoints (e.g. for a fast http-only
			// sanity check, or when ws nodes are known-unreachable and you don't want them probed).
			skipWs, _ := cmd.Flags().GetBool(commonlib.SkipWebsocketVerificationFlag)
			verifyWs := !skipWs
			// This is only the run-wide default; probeProvider narrows it per endpoint on
			// that endpoint's own parser (an endpoint with no ws URL can't have ws
			// verified) — see the note there.

			timeout, _ := cmd.Flags().GetDuration("timeout")
			results := runHealthProbes(ctx, providers, staticSpecPaths, timeout, verifyWs)
			report := buildHealthReport(results, nil)
			writeHealthReport(report)
			// Always exit 0 for a completed run; the JSON is the source of truth.
			return nil
		},
	}

	cmdHealth.Flags().String("log-level", "info", "log level (debug|info|warn|error) — written to stderr")
	cmdHealth.Flags().Bool("include-backup", false, "also probe providers under backup-direct-rpc")
	cmdHealth.Flags().Duration("timeout", 30*time.Second, "per-provider timeout — bounds router setup + all verification relays so a slow/blocked node aborts instead of hanging")
	cmdHealth.Flags().Bool(commonlib.SkipWebsocketVerificationFlag, false, "exclude ws://wss:// endpoints and the spec's websocket verification (ws is probed by default for chains whose spec supports it; bounded by --timeout)")
	cmdHealth.Flags().Bool(chainproxy.GRPCAllowInsecureConnection, false, "allow insecure (self-signed) grpc connections")
	cmdHealth.Flags().Bool(chainproxy.GRPCUseTls, true, "use tls for grpc connections")
	cmdHealth.Flags().StringArray(commonlib.UseStaticSpecFlag, nil, "load specs from file, directory, or remote URL — required (same paths as rpcsmartrouter --use-static-spec)")
	return cmdHealth
}

// collectHealthProviders normalizes probe targets from either inline args or a config file.
func collectHealthProviders(args []string, includeBackup bool) ([]healthProvider, error) {
	// Inline mode: repeated "address chain-id api-interface" triplets.
	if len(args) > 1 {
		viperEndpoints, err := commonlib.ParseEndpointArgs(args, Yaml_config_properties, commonlib.EndpointsConfigName)
		if err != nil {
			return nil, utils.FormatError("invalid inline endpoints", err, utils.Attribute{Key: "args", Value: strings.Join(args, " ")})
		}
		viper.Reset()
		viper.MergeConfigMap(viperEndpoints.AllSettings())
		rpcEndpoints, err := ParseEndpoints(viper.GetViper())
		if err != nil || len(rpcEndpoints) == 0 {
			return nil, utils.FormatError("invalid inline endpoints definition", err)
		}
		providers := make([]healthProvider, 0, len(rpcEndpoints))
		for _, ep := range rpcEndpoints {
			providers = append(providers, healthProvider{
				name:         ep.NetworkAddress, // inline mode has no provider name — use the address
				chainID:      ep.ChainID,
				apiInterface: ep.ApiInterface,
				nodeUrls:     []commonlib.NodeUrl{{Url: ep.NetworkAddress}},
			})
		}
		return providers, nil
	}

	// Config-file mode.
	configName := DefaultRPCSmartRouterFileName
	if len(args) == 1 {
		configName = args[0]
	}
	viper.Reset()
	viper.SetConfigName(configName)
	viper.SetConfigType("yml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath(lavaDefaultNodeHome)
	if err := viper.ReadInConfig(); err != nil {
		return nil, utils.FormatError("failed reading config file", err, utils.Attribute{Key: "config", Value: configName})
	}

	keys := []string{commonlib.DirectRPCConfigName}
	if includeBackup {
		keys = append(keys, commonlib.BackupDirectRPCConfigName)
	}
	lists := make([][]*routersession.RPCStaticProviderEndpoint, 0, len(keys))
	for _, key := range keys {
		if !viper.IsSet(key) {
			continue
		}
		static, err := ParseStaticProviderEndpoints(viper.GetViper(), key)
		if err != nil {
			return nil, err
		}
		lists = append(lists, static)
	}

	// A duplicate provider name stops the router from starting (MAG-2724), and this command is
	// exactly what an operator reaches for to work out why a config will not boot — so it reports
	// the collision and probes anyway, rather than refusing the config like the router does. The
	// same check the router runs, run here for its message and not for its verdict; the rows are
	// still told apart by their `url`, which is what identifies the broken node.
	if err := routersession.ValidateUniqueProviderNames(lists...); err != nil {
		utils.FormatWarning("the router will REFUSE TO START on this config — probing it anyway", err)
	}

	var providers []healthProvider
	for _, static := range lists {
		for _, ep := range static {
			providers = append(providers, healthProvider{
				name:         ep.Name,
				chainID:      ep.ChainID,
				apiInterface: ep.ApiInterface,
				nodeUrls:     ep.NodeUrls,
			})
		}
	}
	return providers, nil
}

// runHealthProbes probes every provider concurrently per (chain, interface) and flattens
// the per-node-url outcomes into report rows.
func runHealthProbes(ctx context.Context, providers []healthProvider, staticSpecPaths []string, timeout time.Duration, verifyWs bool) []healthEndpointResult {
	type indexed struct {
		idx  int
		rows []healthEndpointResult
	}
	out := make(chan indexed, len(providers))
	for i, provider := range providers {
		go func(i int, provider healthProvider) {
			out <- indexed{idx: i, rows: probeProvider(ctx, provider, staticSpecPaths, timeout, verifyWs)}
		}(i, provider)
	}

	// Global wall-clock guard: providers are probed concurrently and each already honors
	// its per-provider `timeout`, but a connector that wedges past its deadline (e.g. a
	// bogus gRPC host stuck in DNS) must not stall the whole command. If a provider hasn't
	// reported within the global deadline, we stop waiting and synthesize a timed-out row
	// for it so the JSON is still complete and the command always returns.
	byIdx := make([][]healthEndpointResult, len(providers))
	got := make([]bool, len(providers))
	var deadline <-chan time.Time
	if timeout > 0 {
		// A little headroom over the per-provider timeout so a provider that finishes
		// right at its own deadline still counts as completed rather than timed-out.
		t := time.NewTimer(timeout + 5*time.Second)
		defer t.Stop()
		deadline = t.C
	}
	remaining := len(providers)
collect:
	for remaining > 0 {
		select {
		case res := <-out:
			byIdx[res.idx] = res.rows
			got[res.idx] = true
			remaining--
		case <-deadline:
			break collect
		}
	}
	for i, provider := range providers {
		if got[i] {
			continue
		}
		byIdx[i] = timedOutRows(provider, timeout)
	}

	var results []healthEndpointResult
	for _, rows := range byIdx {
		results = append(results, rows...)
	}
	return results
}

// timedOutRows synthesizes one ok:false row per node URL for a provider that didn't
// report within the global deadline, so the report is always complete.
func timedOutRows(provider healthProvider, timeout time.Duration) []healthEndpointResult {
	rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
	for _, url := range provider.nodeUrls {
		rows = append(rows, healthEndpointResult{
			Name:          provider.name,
			ChainID:       provider.chainID,
			APIInterface:  provider.apiInterface,
			URL:           url.UrlStr(),
			Transport:     transportForURL(url.Url),
			Addons:        nonNilStrings(url.Addons),
			Extensions:    []string{},
			SpecValid:     false,
			LatestBlock:   spectypes.NOT_APPLICABLE,
			Ok:            false,
			Error:         fmt.Sprintf("probe timed out after %s", timeout),
			Verifications: []healthVerification{},
		})
	}
	return rows
}

// probeProvider sets up the spec + chain router for one provider and runs the spec
// verifications against each of its node URLs. A spec-load failure yields one ok:false
// row per node URL (specValid:false) with no relay attempted.
func probeProvider(ctx context.Context, provider healthProvider, staticSpecPaths []string, timeout time.Duration, verifyWs bool) []healthEndpointResult {
	// Bound the whole probe (router construction through every verification relay) so a
	// slow or blocked node aborts at the deadline instead of grinding through the full
	// connector retry budget. Every connector (HTTP / gRPC / ws) derives each attempt's
	// timeout from this ctx and checks ctx.Err() between attempts, so the deadline
	// propagates to ws handshake retries too.
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	base := func(url commonlib.NodeUrl) healthEndpointResult {
		return healthEndpointResult{
			Name:          provider.name,
			ChainID:       provider.chainID,
			APIInterface:  provider.apiInterface,
			URL:           url.UrlStr(),
			Transport:     transportForURL(url.Url),
			Addons:        nonNilStrings(url.Addons),
			Extensions:    []string{},
			LatestBlock:   spectypes.NOT_APPLICABLE,
			Verifications: []healthVerification{},
		}
	}

	rowsFromError := func(err error) []healthEndpointResult {
		rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
		for _, url := range provider.nodeUrls {
			row := base(url)
			row.SpecValid = false
			row.Ok = false
			row.Error = err.Error()
			rows = append(rows, row)
		}
		return rows
	}

	chainParser, err := chainlib.NewChainParser(provider.apiInterface)
	if err != nil {
		return rowsFromError(fmt.Errorf("create chain parser: %w", err))
	}

	// Per-endpoint websocket gating. For a chain whose spec supports subscriptions every
	// verification is augmented with the websocket extension — which can only route if this
	// endpoint actually has a ws:// URL. An http-only endpoint (e.g. an inline
	// `address chain-id api-interface` probe) would otherwise fail EVERY check with
	// "no chain proxy supporting requested extensions {websocket}".
	//
	// This is set on our own parser, which nothing else shares. It used to be a package
	// global flipped under a mutex held around ValidateCollect, and that lock is what
	// produced the reported symptom: it serialized every endpoint's verification phase
	// while each endpoint's own timeout kept running from goroutine launch, so late
	// endpoints entered ValidateCollect with most of their budget gone, their context
	// expired mid-probe, and connectorLoop closed the connector out from under the
	// remaining relays ("connector is closed" on healthy nodes, MAG-2333).
	//
	// The global's other reader, newChainRouter (chain_router.go:328), was NOT part of
	// that symptom under `health`: this command sets IgnoreWsEnforcementForTestCommands
	// before any probing, and that guard is the first operand of the same &&, so the ws
	// check short-circuits before SkipWebsocketVerification is ever evaluated there.
	// Per-parser state is still the right shape — it removes the shared cell instead of
	// synchronizing it, and newChainRouter does read it on the serving path, where the
	// guard is false — but a cross-endpoint race on that bool is not what `health` hit.
	chainParser.SetSkipWebsocketVerification(!(verifyWs && providerHasWebSocketURL(provider.nodeUrls)))

	rpcEndpoint := routersession.RPCEndpoint{ChainID: provider.chainID, ApiInterface: provider.apiInterface}
	if err := statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken(ctx, chainParser, staticSpecPaths, rpcEndpoint, "", ""); err != nil {
		return rowsFromError(fmt.Errorf("load spec: %w", err))
	}

	// ws:// URLs are probed by default. When --skip-websocket-verification is set, exclude
	// them from router construction (GetChainRouter builds a connector per URL) — each
	// excluded URL still gets a visible row marked as skipped.
	probedUrls := provider.nodeUrls
	var skippedWsUrls []commonlib.NodeUrl
	if !verifyWs {
		probedUrls = probedUrls[:0:0]
		for _, url := range provider.nodeUrls {
			if transportForURL(url.Url) == "ws" {
				skippedWsUrls = append(skippedWsUrls, url)
				continue
			}
			probedUrls = append(probedUrls, url)
		}
	}

	wsSkippedRows := func() []healthEndpointResult {
		rows := make([]healthEndpointResult, 0, len(skippedWsUrls))
		for _, url := range skippedWsUrls {
			row := base(url)
			row.SpecValid = true
			row.Ok = false
			row.Error = "websocket verification skipped (--skip-websocket-verification)"
			rows = append(rows, row)
		}
		return rows
	}

	// No probeable (non-ws) URLs left — report only the skipped ws rows.
	if len(probedUrls) == 0 {
		return wsSkippedRows()
	}

	// A provider that lists only ws:// URLs can't be probed on its own: the chain router
	// always requires the base (no-extension) collection, which only an http(s) URL serves.
	// Detect this up front and report an actionable error instead of the router's internal
	// "missing extensions or addons" dump. (Real configs pair every ws URL with an http one.)
	if allURLsAreWebSocket(probedUrls) {
		rows := make([]healthEndpointResult, 0, len(probedUrls))
		for _, url := range probedUrls {
			row := base(url)
			row.SpecValid = true
			row.Ok = false
			row.Error = "ws-only endpoint cannot be probed alone — add an http(s) base URL for this chain/interface (the router always needs the base collection)"
			rows = append(rows, row)
		}
		return append(rows, wsSkippedRows()...)
	}

	providerEndpoint := &routersession.RPCProviderEndpoint{
		ChainID:      provider.chainID,
		ApiInterface: provider.apiInterface,
		NodeUrls:     probedUrls,
	}
	chainRouter, err := chainlib.GetChainRouter(ctx, 1, providerEndpoint, chainParser)
	if err != nil {
		rows := make([]healthEndpointResult, 0, len(probedUrls))
		for _, url := range probedUrls {
			row := base(url)
			row.SpecValid = true
			row.Ok = false
			row.Error = fmt.Sprintf("create chain router: %v", err)
			rows = append(rows, row)
		}
		return append(rows, wsSkippedRows()...)
	}
	chainFetcher := chainlib.NewChainFetcher(ctx, &chainlib.ChainFetcherOptions{
		ChainRouter: chainRouter,
		ChainParser: chainParser,
		Endpoint:    providerEndpoint,
		Cache:       nil,
	})

	// Spec-driven probing: ValidateCollect returns one NodeURLValidation per node URL, in
	// the same order as probedUrls. Match positionally — NOT by url string — because a
	// provider can list the same URL twice with different addons (e.g. a base URL and an
	// `addons:[archive]` URL), which would otherwise collide in a url-keyed map.
	//
	// ws gating for this endpoint was applied to chainParser above, so nothing here is
	// shared with the endpoints probed concurrently alongside it.
	validations := chainFetcher.ValidateCollect(ctx)

	rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
	for i, url := range probedUrls {
		row := base(url)
		row.SpecValid = true
		if i >= len(validations) {
			row.Ok = false
			row.Error = "no validation result produced for node url"
			rows = append(rows, row)
			continue
		}
		applyValidation(&row, validations[i])
		rows = append(rows, row)
	}

	backfillLatestBlock(rows, func() (int64, error) { return chainFetcher.FetchLatestBlockNum(ctx) })

	return append(rows, wsSkippedRows()...)
}

// backfillLatestBlock gives rows that came back without a block height a reporting-only
// value from fetch.
//
// ValidateCollect fetches the latest block only when some verification needs it to
// compute a LatestDistance. A spec with no such verification therefore reports
// latestBlock 0 on legs that passed every check, which reads as a dead node — the
// "known related quirk" in MAG-2333.
//
// fetch is called at most once per endpoint and only if some row actually needs it, so
// a spec that already reports a height costs no extra relay. A fetch failure leaves the
// rows untouched: this is a reporting field and must never change a leg's ok verdict.
//
// Two limits follow from the value being ENDPOINT-level while the rows are per-URL:
//
//   - Only rows that passed are filled. fetch cannot say anything about a leg that failed
//     its checks, and a plausible height printed beside those failures reads as if the URL
//     were reachable. This also matches what the change set out to do — report a real
//     height on passing legs.
//   - fetch routes through the router built over ALL of this endpoint's probed URLs, so
//     the height belongs to the endpoint, not necessarily to the row it lands on. That is
//     acceptable for a field that never feeds a verdict, but it is not a per-URL
//     measurement and must not be read as one.
func backfillLatestBlock(rows []healthEndpointResult, fetch func() (int64, error)) {
	var block int64
	fetched := false
	for i := range rows {
		if !rows[i].Ok || rows[i].LatestBlock > 0 {
			continue
		}
		if !fetched {
			fetched = true
			if fetchedBlock, err := fetch(); err == nil {
				block = fetchedBlock
			}
		}
		if block > 0 {
			rows[i].LatestBlock = block
		}
	}
}

// applyValidation folds one node-URL's spec-verification results into its result row:
// copies each verification, collects the extensions it covered, sets the latest block,
// and computes the endpoint ok = every verification passed. Pure (no I/O) so the
// row-mapping and ok-rollup are unit-testable.
func applyValidation(row *healthEndpointResult, v chainlib.NodeURLValidation) {
	row.LatestBlock = v.LatestBlock
	extensions := map[string]struct{}{}
	allOk := true
	for _, vr := range v.Verifications {
		row.Verifications = append(row.Verifications, healthVerification{
			Name:      vr.Name,
			Addon:     vr.Addon,
			Extension: vr.Extension,
			Ok:        vr.Ok,
			Error:     vr.Error,
		})
		if vr.Extension != "" {
			extensions[vr.Extension] = struct{}{}
		}
		if !vr.Ok {
			allOk = false
		}
	}
	row.Extensions = sortedKeys(extensions)
	row.Ok = allOk
}

// buildHealthReport assembles the stdout envelope. fatalErr is non-nil only for setup
// failures that prevented any probing; otherwise the top-level ok is the AND of all rows.
func buildHealthReport(results []healthEndpointResult, fatalErr error) healthReport {
	report := healthReport{Results: results}
	if fatalErr != nil {
		msg := fatalErr.Error()
		report.Error = &msg
		report.Ok = false
		if report.Results == nil {
			report.Results = []healthEndpointResult{}
		}
		return report
	}
	report.Ok = true
	for _, r := range results {
		if !r.Ok {
			report.Ok = false
			break
		}
	}
	if report.Results == nil {
		report.Results = []healthEndpointResult{}
	}
	return report
}

// writeHealthReport prints the report as indented JSON to stdout.
func writeHealthReport(report healthReport) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}

// emitFatal prints a JSON envelope carrying the setup error to stdout, then returns the
// error so cobra exits non-zero. The consumer still gets parseable JSON on stdout.
func emitFatal(err error) error {
	writeHealthReport(buildHealthReport(nil, err))
	return err
}

// providerHasWebSocketURL reports whether any of the provider's node URLs is ws://wss://.
func providerHasWebSocketURL(urls []commonlib.NodeUrl) bool {
	for _, u := range urls {
		if transportForURL(u.Url) == "ws" {
			return true
		}
	}
	return false
}

// allURLsAreWebSocket reports whether every node URL is ws://wss:// (i.e. there's no http
// base collection) — such a provider can't have a chain router constructed for it.
func allURLsAreWebSocket(urls []commonlib.NodeUrl) bool {
	if len(urls) == 0 {
		return false
	}
	for _, u := range urls {
		if transportForURL(u.Url) != "ws" {
			return false
		}
	}
	return true
}

// transportForURL classifies a node URL's transport from its scheme, for the JSON `transport` field.
func transportForURL(rawURL string) string {
	lower := strings.ToLower(rawURL)
	switch {
	case strings.HasPrefix(lower, "ws://"), strings.HasPrefix(lower, "wss://"):
		return "ws"
	case strings.HasPrefix(lower, "http://"), strings.HasPrefix(lower, "https://"):
		return "http"
	default:
		return "other"
	}
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	// small sets — simple insertion-free sort
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}
