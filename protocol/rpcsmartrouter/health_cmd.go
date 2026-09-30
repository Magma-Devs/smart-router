package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/statetracker"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// healthVerification is one spec verification's result, as emitted in the JSON report.
type healthVerification struct {
	Name      string `json:"name"`
	Addon     string `json:"addon"`
	Extension string `json:"extension"`
	Severity  string `json:"severity"`
	Ok        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

// Router verdicts on one node url, as a router with the same flags would reach them.
const (
	routerAdmitted = "admitted" // served, minus refusedServices
	routerRefused  = "refused"  // dropped; the provider serves from its other urls
	routerExcluded = "excluded" // the whole provider is dropped
	routerUnknown  = "unknown"  // undecided: timed out, spec not loaded, or only rate-limited
)

// healthRouterVerdict is what the router does with one node url.
type healthRouterVerdict struct {
	Verdict         string   `json:"verdict"`
	RefusedServices []string `json:"refusedServices"`
	Reason          string   `json:"reason,omitempty"`
}

// healthEndpointResult is one (provider, chain, interface, node-url) probe result,
// verified over that node url's own connection.
type healthEndpointResult struct {
	Name         string   `json:"name"`
	ChainID      string   `json:"chainId"`
	APIInterface string   `json:"apiInterface"`
	URL          string   `json:"url"`
	Transport    string   `json:"transport"`
	Addons       []string `json:"addons"`
	Extensions   []string `json:"extensions"`
	SpecValid    bool     `json:"specValid"`
	// LatestBlock is this url's own height: 0 when the spec gives it no height
	// request, -1 when the request failed (latestBlockError says why).
	LatestBlock      int64                `json:"latestBlock"`
	LatestBlockError string               `json:"latestBlockError,omitempty"`
	Ok               bool                 `json:"ok"`
	Error            string               `json:"error,omitempty"`
	Verifications    []healthVerification `json:"verifications"`
	Router           healthRouterVerdict  `json:"router"`
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

// healthOptions are the flags every provider's probe shares.
type healthOptions struct {
	staticSpecPaths []string
	githubToken     string
	gitlabToken     string
	timeout         time.Duration
	skipWebsocket   bool
	concurrency     int
}

// CreateHealthCobraCommand builds the `smartrouter health` command: a one-shot,
// spec-driven probe that sends every configured node url the relays its spec defines,
// over that url's own connection, and prints a single JSON document to stdout.
func CreateHealthCobraCommand() *cobra.Command {
	cmdHealth := &cobra.Command{
		Use:   `health [config-file] | { node-url spec-chain-id api-interface ... }`,
		Short: `Spec-driven health probe of configured endpoints — emits a JSON report to stdout`,
		Long: `health loads the spec for every configured (chain, api-interface) and sends each node
url the relays the spec itself defines, over that url's own connection: the latest-block
call plus every verification declared for the url's addons/extensions. A ws(s) url answers
for the checks of a collection that subscribes, and is only dialed with
--skip-websocket-verification, as a router run with that flag does. It is fully
spec-driven: no per-chain or per-interface code is involved.

The result is a single JSON document on stdout (logs go to stderr): one row per node url,
each with its own checks, its own height, and "router" — what a router with the same flags
does with that url (admitted, refused, or excluded with its whole provider). The process
exits 0 for any completed run — endpoint failures are reported as data (ok:false,
error:"..."), never as a non-zero exit. Only a fatal setup error (bad config, missing
--use-static-spec) exits non-zero, and even then a JSON envelope with a populated "error"
is printed first.

Endpoints can come from a smartrouter config file (probes every node-url under direct-rpc),
or from inline "node-url chain-id api-interface" triplets.

The config argument resolves exactly as the rpcsmartrouter command's does: an absolute path
names the file outright, while a relative path or a bare name is looked up in the local
running directory, ./config, then ` + defaultNodeHome + `.`,
		Example: `  smartrouter health config/smartrouter_examples/smartrouter_eth.yml --use-static-spec specs/
  smartrouter health https://ethereum-rpc.publicnode.com ETH1 jsonrpc --use-static-spec specs/`,
		Args: func(cmd *cobra.Command, args []string) error {
			// Either: 0-1 args (config file), or repeated groups of 3 (inline endpoints).
			if len(args) <= 1 {
				return nil
			}
			if len(args)%len(Yaml_config_properties) != 0 {
				return fmt.Errorf("invalid number of arguments: inline endpoints must be repeated groups of %d (node-url chain-id api-interface), got %d", len(Yaml_config_properties), len(args))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Logs to stderr so stdout carries only the JSON report.
			utils.JsonFormat = true
			logLevel, _ := cmd.Flags().GetString("log-level")
			utils.SetGlobalLoggingLevel(logLevel)

			opts := healthOptions{}
			var err error
			if opts.staticSpecPaths, err = cmd.Flags().GetStringArray(commonlib.UseStaticSpecFlag); err != nil {
				return emitFatal(err)
			}
			// The smart router has no live blockchain spec query — specs must be static.
			if len(opts.staticSpecPaths) == 0 {
				return emitFatal(fmt.Errorf("--use-static-spec is required (smart-router mode has no live spec source)"))
			}
			opts.githubToken, _ = cmd.Flags().GetString(commonlib.GitHubTokenFlag)
			opts.gitlabToken, _ = cmd.Flags().GetString(commonlib.GitLabTokenFlag)
			opts.timeout, _ = cmd.Flags().GetDuration("timeout")
			opts.skipWebsocket, _ = cmd.Flags().GetBool(commonlib.SkipWebsocketVerificationFlag)
			opts.concurrency, _ = cmd.Flags().GetInt("concurrency")
			if opts.concurrency < 1 {
				return emitFatal(fmt.Errorf("--concurrency must be at least 1, got %d", opts.concurrency))
			}
			includeBackup, _ := cmd.Flags().GetBool("include-backup")

			providers, err := collectHealthProviders(args, includeBackup, inlineAuthFrom(cmd.Flags()))
			if err != nil {
				return emitFatal(err)
			}
			if len(providers) == 0 {
				return emitFatal(fmt.Errorf("no endpoints to probe — config has no direct-rpc providers and no inline endpoints were given"))
			}

			results := runHealthProbes(context.Background(), providers, opts)
			writeHealthReport(buildHealthReport(results, nil))
			// Always exit 0 for a completed run; the JSON is the source of truth.
			return nil
		},
	}

	cmdHealth.Flags().String("log-level", "info", "log level (debug|info|warn|error) — written to stderr")
	cmdHealth.Flags().Bool("include-backup", false, "also probe providers under backup-direct-rpc")
	cmdHealth.Flags().Duration("timeout", 30*time.Second, "per-provider timeout — bounds dialing and every relay of one provider, so a slow or blocked node aborts instead of hanging")
	cmdHealth.Flags().Int("concurrency", 8, "how many providers are probed at once")
	cmdHealth.Flags().Bool(commonlib.SkipWebsocketVerificationFlag, false, "only dial ws(s) urls, running no check over them — what a router run with this flag does")
	cmdHealth.Flags().Bool(chainproxy.GRPCAllowInsecureConnection, false, "inline endpoints: allow insecure (self-signed) grpc connections; a config's node urls carry their own auth-config")
	cmdHealth.Flags().Bool(chainproxy.GRPCUseTls, false, "inline endpoints: dial grpc over tls only; unset, a plaintext dial is tried first and upgraded to tls when it fails")
	cmdHealth.Flags().StringArray(commonlib.UseStaticSpecFlag, nil, "load specs from file, directory, or remote URL — required (same paths as rpcsmartrouter --use-static-spec)")
	cmdHealth.Flags().String(commonlib.GitHubTokenFlag, "", "GitHub personal access token for a private spec repository")
	cmdHealth.Flags().String(commonlib.GitLabTokenFlag, "", "GitLab personal access token for a private spec repository")
	return cmdHealth
}

// inlineAuthFrom is the auth-config the inline gRPC flags give an inline url. Both
// default off: a gRPC dial then tries plaintext first and upgrades to TLS when that
// fails.
func inlineAuthFrom(flags *pflag.FlagSet) commonlib.AuthConfig {
	useTLS, _ := flags.GetBool(chainproxy.GRPCUseTls)
	allowInsecure, _ := flags.GetBool(chainproxy.GRPCAllowInsecureConnection)
	return commonlib.AuthConfig{UseTLS: useTLS, AllowInsecure: allowInsecure}
}

// collectHealthProviders normalizes probe targets from either inline args or a config file.
func collectHealthProviders(args []string, includeBackup bool, inlineAuth commonlib.AuthConfig) ([]healthProvider, error) {
	// Inline mode: repeated "node-url chain-id api-interface" triplets.
	if len(args) > 1 {
		viperEndpoints, err := commonlib.ParseEndpointArgs(args, Yaml_config_properties, commonlib.EndpointsConfigName)
		if err != nil {
			return nil, utils.LavaFormatError("invalid inline endpoints", err)
		}
		viper.Reset()
		viper.MergeConfigMap(viperEndpoints.AllSettings())
		rpcEndpoints, err := ParseEndpoints(viper.GetViper())
		if err != nil || len(rpcEndpoints) == 0 {
			return nil, utils.LavaFormatError("invalid inline endpoints definition", err)
		}
		providers := make([]healthProvider, 0, len(rpcEndpoints))
		for _, ep := range rpcEndpoints {
			nodeUrl := commonlib.NodeUrl{Url: ep.NetworkAddress, AuthConfig: inlineAuth}
			providers = append(providers, healthProvider{
				// Inline mode has no provider name. The url is it, redacted like every url
				// this report prints: vendors put the key in the path or the query.
				name:         nodeUrl.UrlStr(),
				chainID:      ep.ChainID,
				apiInterface: ep.ApiInterface,
				nodeUrls:     []commonlib.NodeUrl{nodeUrl},
			})
		}
		return providers, nil
	}

	// Config-file mode. The argument is a config file path (absolute or relative) or a
	// bare name resolved against the search paths; see config_source.go.
	viper.Reset()
	configTarget, configIsFile := pointViperAtConfig(args)
	if err := viper.ReadInConfig(); err != nil {
		// This command is what an operator reaches for when a config will not boot, so a
		// config it cannot even find has to say where it looked, in the terms they used.
		if isConfigNotFound(err) {
			return nil, utils.LavaFormatError(configNotFoundMessage(configTarget, configIsFile), err,
				configLocationAttributes(configTarget, configIsFile)...)
		}
		return nil, utils.LavaFormatError("failed reading config file", err, utils.Attribute{Key: "config", Value: configTarget})
	}

	keys := []string{commonlib.DirectRPCConfigName}
	if includeBackup {
		keys = append(keys, commonlib.BackupDirectRPCConfigName)
	}
	lists := make([][]*lavasession.RPCStaticProviderEndpoint, 0, len(keys))
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
	// rows are still told apart by their `url`, which is what identifies the broken node.
	if err := lavasession.ValidateUniqueProviderNames(lists...); err != nil {
		utils.LavaFormatWarning("the router will REFUSE TO START on this config — probing it anyway", err)
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

// runHealthProbes probes up to opts.concurrency providers at once and flattens their
// rows in configured order.
func runHealthProbes(ctx context.Context, providers []healthProvider, opts healthOptions) []healthEndpointResult {
	byIdx := make([][]healthEndpointResult, len(providers))
	slots := make(chan struct{}, opts.concurrency)
	var wg sync.WaitGroup
	for i, provider := range providers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			byIdx[i] = probeWithDeadline(ctx, provider, opts)
		}()
	}
	wg.Wait()

	var results []healthEndpointResult
	for _, rows := range byIdx {
		results = append(results, rows...)
	}
	return results
}

// healthProbeFn and healthDeadlineGrace are seams for tests; production never
// reassigns them.
var (
	healthProbeFn       = probeProvider
	healthDeadlineGrace = 5 * time.Second
)

// probeWithDeadline stops waiting for a provider a little after its own --timeout: a
// connector that wedges past its deadline (a gRPC host stuck in DNS) must not stall
// the command, which then reports that provider as timed out. The wedged probe runs
// on without its --concurrency slot.
func probeWithDeadline(ctx context.Context, provider healthProvider, opts healthOptions) []healthEndpointResult {
	if opts.timeout <= 0 {
		return healthProbeFn(ctx, provider, opts)
	}
	done := make(chan []healthEndpointResult, 1)
	go func() { done <- healthProbeFn(ctx, provider, opts) }()
	wait := time.NewTimer(opts.timeout + healthDeadlineGrace)
	defer wait.Stop()
	select {
	case rows := <-done:
		return rows
	case <-wait.C:
		return timedOutRows(provider, opts.timeout)
	}
}

// timedOutRows synthesizes one ok:false row per node URL for a provider that didn't
// report within its deadline, so the report is always complete.
func timedOutRows(provider healthProvider, timeout time.Duration) []healthEndpointResult {
	rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
	for _, url := range provider.nodeUrls {
		row := baseRow(provider, url)
		row.Error = fmt.Sprintf("probe timed out after %s", timeout)
		row.Router = healthRouterVerdict{Verdict: routerUnknown, RefusedServices: []string{}, Reason: row.Error}
		rows = append(rows, row)
	}
	return rows
}

func baseRow(provider healthProvider, url commonlib.NodeUrl) healthEndpointResult {
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
		Router:        healthRouterVerdict{Verdict: routerUnknown, RefusedServices: []string{}},
	}
}

// probeProvider loads the provider's spec and verifies each of its node urls over its
// own connection, as the router's admission does. A spec that does not load yields
// one ok:false row per node url (specValid:false) with no relay attempted.
func probeProvider(ctx context.Context, provider healthProvider, opts healthOptions) []healthEndpointResult {
	// Bound the whole probe — dialing and every relay — so a slow or blocked node
	// aborts at the deadline instead of grinding through the connector retry budget.
	if opts.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}

	rowsFromError := func(err error) []healthEndpointResult {
		rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
		for _, url := range provider.nodeUrls {
			row := baseRow(provider, url)
			row.Error = err.Error()
			row.Router.Reason = err.Error()
			rows = append(rows, row)
		}
		return rows
	}

	chainParser, err := chainlib.NewChainParser(provider.apiInterface)
	if err != nil {
		return rowsFromError(fmt.Errorf("create chain parser: %w", err))
	}
	// This parser is the provider's alone, so the flag can be set on it directly.
	chainParser.SetSkipWebsocketVerification(opts.skipWebsocket)

	rpcEndpoint := lavasession.RPCEndpoint{ChainID: provider.chainID, ApiInterface: provider.apiInterface}
	if err := statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken(ctx, chainParser, opts.staticSpecPaths, rpcEndpoint, opts.githubToken, opts.gitlabToken); err != nil {
		return rowsFromError(fmt.Errorf("load spec: %w", err))
	}

	endpoint := &lavasession.RPCProviderEndpoint{
		ChainID:      provider.chainID,
		ApiInterface: provider.apiInterface,
		NodeUrls:     provider.nodeUrls,
	}
	nodeUrlRouter, err := chainlib.NewNodeUrlRouterFactory(1, endpoint)
	if err != nil {
		return rowsFromError(err)
	}
	fetcher := chainlib.NewChainFetcher(ctx, &chainlib.ChainFetcherOptions{
		ChainParser:   chainParser,
		Endpoint:      endpoint,
		NodeUrlRouter: nodeUrlRouter,
	})
	validations, admissionErr := fetcher.ValidateReport(ctx)

	// A shape the router refuses outright excludes the provider whatever its urls
	// answer; the rows still say what each url answered.
	excluded := chainlib.ProviderShapeError(endpoint, chainParser)
	if excluded == nil {
		excluded = admissionErr
	}

	rows := make([]healthEndpointResult, 0, len(provider.nodeUrls))
	for i, url := range provider.nodeUrls {
		row := baseRow(provider, url)
		row.SpecValid = true
		applyValidation(&row, validations[i])
		row.Router = routerVerdict(validations[i], excluded)
		rows = append(rows, row)
	}
	return rows
}

// applyValidation folds one node url's results into its row: every verification, the
// extensions they covered, the url's height, and ok = the url was reached and every
// verification passed, whatever its severity. Pure (no I/O), so the mapping and the
// rollup are unit-testable.
func applyValidation(row *healthEndpointResult, v chainlib.NodeURLValidation) {
	row.LatestBlock = v.LatestBlock
	row.LatestBlockError = v.LatestBlockError
	row.Error = v.Error
	extensions := map[string]struct{}{}
	allOk := v.Error == ""
	for _, vr := range v.Verifications {
		row.Verifications = append(row.Verifications, healthVerification{
			Name:      vr.Name,
			Addon:     vr.Addon,
			Extension: vr.Extension,
			Severity:  vr.Severity,
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

// routerVerdict is what the router does with one node url, given the provider's fate.
// A rate limit decides nothing, so a url or provider that only met one is unknown.
func routerVerdict(v chainlib.NodeURLValidation, providerExcluded error) healthRouterVerdict {
	switch {
	case providerExcluded != nil && chainlib.IsRateLimitFailure(providerExcluded):
		return healthRouterVerdict{Verdict: routerUnknown, RefusedServices: []string{}, Reason: "rate-limited: " + providerExcluded.Error()}
	case providerExcluded != nil:
		return healthRouterVerdict{Verdict: routerExcluded, RefusedServices: []string{}, Reason: providerExcluded.Error()}
	case v.Refused:
		return healthRouterVerdict{Verdict: routerRefused, RefusedServices: []string{}, Reason: v.Refusal}
	case v.Throttled != "":
		return healthRouterVerdict{Verdict: routerUnknown, RefusedServices: []string{}, Reason: "rate-limited: " + v.Throttled}
	}
	verdict := healthRouterVerdict{Verdict: routerAdmitted, RefusedServices: nonNilStrings(v.RefusedServices)}
	if len(v.ThrottledServices) > 0 {
		verdict.Reason = "rate-limited, not verified: " + strings.Join(v.ThrottledServices, ", ")
	}
	return verdict
}

// buildHealthReport assembles the stdout envelope. fatalErr is non-nil only for setup
// failures that prevented any probing; otherwise the top-level ok is the AND of all rows.
func buildHealthReport(results []healthEndpointResult, fatalErr error) healthReport {
	report := healthReport{Results: results}
	if report.Results == nil {
		report.Results = []healthEndpointResult{}
	}
	if fatalErr != nil {
		msg := fatalErr.Error()
		report.Error = &msg
		return report
	}
	report.Ok = true
	for _, r := range results {
		if !r.Ok {
			report.Ok = false
			break
		}
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
	sort.Strings(keys)
	return keys
}
