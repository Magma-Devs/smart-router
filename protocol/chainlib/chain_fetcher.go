package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/cacheformat"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
)

const (
	ChainFetcherHeaderName = "X-LAVA-Provider"
)

type IChainFetcher interface {
	FetchLatestBlockNum(ctx context.Context) (int64, error)
	FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error)
	FetchEndpoint() lavasession.RPCProviderEndpoint
	Validate(ctx context.Context) error
	GetVerificationsStatus() []*pairingtypes.Verification
	CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error)
}

type ChainFetcher struct {
	endpoint            *lavasession.RPCProviderEndpoint
	chainRouter         ChainRouter
	nodeUrlRouter       NodeUrlRouterFactory
	chainParser         ChainParser
	cache               performance.CacheBackend
	latestBlock         int64
	verificationsStatus common.SafeSyncMap[string, bool]
	cachedVerifications atomic.Value // holds []*pairingtypes.Verification for faster access
	cacheValid          atomic.Bool
}

func (cf *ChainFetcher) GetVerificationsStatus() []*pairingtypes.Verification {
	// Try to get from cache first
	if cf.cacheValid.Load() {
		value, ok := cf.cachedVerifications.Load().([]*pairingtypes.Verification)
		if ok {
			return value
		} else {
			utils.LavaFormatError("invalid usage of cachedVerifications, could not cast result into []*pairingtypes.Verification type", nil, utils.Attribute{Key: "cachedVerifications", Value: cf.cachedVerifications.Load()})
		}
	}

	// If not in cache, create new slice
	verifications := make([]*pairingtypes.Verification, 0)
	cf.verificationsStatus.Range(func(name string, passed bool) bool {
		verifications = append(verifications, &pairingtypes.Verification{
			Name:   name,
			Passed: passed,
		})
		return true
	})

	// Store in cache
	cf.cachedVerifications.Store(verifications)
	cf.cacheValid.Store(true)
	return verifications
}

// Add this method to invalidate cache when verification status changes
func (cf *ChainFetcher) invalidateVerificationsCache() {
	cf.cacheValid.Store(false)
}

func (cf *ChainFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return *cf.endpoint
}

func (cf *ChainFetcher) getVerificationsKey(verification VerificationContainer, apiInterface string, chainId string) string {
	key := chainId + "-" + apiInterface + "-" + verification.Name
	if verification.Addon != "" {
		key += "-" + verification.Addon
	}
	if verification.Extension != "" {
		key += "-" + verification.Extension
	}
	return key
}

// skipVerification is the single gate deciding whether a verification runs for a node-url.
// Two independent sources can suppress it: the per-node-url skip-verifications config (with
// its "*" wildcard), and the process-wide --skip-all-verifications flag. Everything that
// acts on behalf of a verification must go through here — including needsLatestBlock, so a
// suppressed verification cannot drag the latest-block probe out to the upstream anyway.
func skipVerification(url common.NodeUrl, name string) bool {
	return SkipAllVerifications || url.ShouldSkipVerification(name)
}

// verificationsForNodeUrl drops the base collection's verifications for a url that
// has opted out of serving it (NodeUrl.StandaloneAddons).
//
// GetVerifications always adds the empty addon to whatever a url declares, so an
// add-on node runs the base collection's verifications too. That is right when the
// add-on EXTENDS the base surface and wrong when it REPLACES it: Acala's base
// chain-id verification fires a Substrate chain_getBlockHash, which an EVM-only
// node answers with -32601, and an omitted severity in a spec means Fail — so one
// inherited verification excluded the provider (MAG-3296).
//
// This runs BEFORE needsLatestBlock for the same reason the skip filter does: a
// verification that is not going to run must not drag a head probe out to the
// upstream on its way past.
//
// Filtering here rather than inside GetVerifications keeps the parser's signature
// free of node-url concerns — and dropping Addon=="" containers afterwards is
// exactly equivalent to never having appended the empty addon.
func verificationsForNodeUrl(url common.NodeUrl, verifications []VerificationContainer) []VerificationContainer {
	if url.ServesBaseCollection() {
		return verifications
	}
	kept := make([]VerificationContainer, 0, len(verifications))
	for _, verification := range verifications {
		if verification.Addon == "" {
			utils.LavaFormatDebug("skipping base-collection verification for a standalone-addons url",
				utils.LogAttr("verification", verification.Name),
				utils.LogAttr("url", url.UrlStr()),
				utils.LogAttr("addons", url.Addons),
			)
			continue
		}
		kept = append(kept, verification)
	}
	return kept
}

// verificationNeedsHead reports whether a verification is measured against the
// chain head, and so cannot run without one.
func verificationNeedsHead(verification VerificationContainer) bool {
	return verification.Value == "" && verification.LatestDistance != 0
}

// needsLatestBlock reports whether any verification that will ACTUALLY RUN for this
// node-url depends on the chain head, and therefore whether Validate has to spend a
// FetchLatestBlockNum relay against the upstream before verifying.
//
// Skipped verifications are excluded deliberately. The latest-block fetch is a real
// relay: it retries 3x back-to-back and, in Validate, a failure aborts the whole
// provider. Letting a configured-away verification pull it in meant skip-verifications
// did not actually stop the router from probing the node — an upstream that rate-limits
// the burst still got its provider demoted with every verification skipped.
func needsLatestBlock(url common.NodeUrl, verifications []VerificationContainer) bool {
	for _, v := range verifications {
		if skipVerification(url, v.Name) {
			continue
		}
		if verificationNeedsHead(v) {
			return true
		}
	}
	return false
}

// stopValidateRetries reports whether a failed attempt should end Validate's zero-delay
// retry budget early. The budget exists for a cold-booting node; a rate-limited attempt
// is different in kind — the extra requests land inside the same limiter window and only
// add to the load that produced the 429.
func stopValidateRetries(err error) bool {
	return err == nil || errors.Is(err, common.StatusCodeError429)
}

// isWebSocketUrl reports whether a node url is dialed as a websocket. A gRPC
// host:port does not parse as a url and is not one.
func isWebSocketUrl(rawURL string) bool {
	isWs, err := IsUrlWebSocket(rawURL)
	return err == nil && isWs
}

// rateLimitTextSignatures covers the one transport where no status code exists to check.
//
// Every HTTP-family transport reaches us as common.StatusCodeError429 — ValidateStatusCodes
// mints it, each proxy propagates it as LavaFormat's cause, so Unwrap survives and errors.Is
// below is the real check. gRPC is different in kind: there is no HTTP status in the error at
// all. grpc-go reports codes.Unavailable and the vendor's 429 survives only inside the status
// description, so there is nothing structural to match on.
//
// Verbatim from a production failure. Keep this list minimal — a new entry here is usually a
// signal that some path is discarding a typed error, which is worth fixing at the source
// instead.
var rateLimitTextSignatures = []string{
	"429 (Too Many Requests)", // grpc transport: no status code, only the status description
}

// IsRateLimitFailure reports whether a failure was the upstream refusing us for asking too
// fast, rather than the upstream being unable to serve what it declares. Such a failure says
// nothing about the node: callers back off and must not refuse or demote on it.
func IsRateLimitFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, common.StatusCodeError429) {
		return true
	}
	msg := err.Error()
	for _, sig := range rateLimitTextSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// admissionKey identifies one refused service on one node url. A comparable
// struct rather than a joined string: the join needed a NUL sentinel, which is
// an invariant every reader has to re-verify.
type admissionKey struct {
	url          string
	internalPath string
	service      string
}

// ProviderAdmission records what a provider's node urls failed to verify, so a
// failure costs the narrowest thing that owns it: one service on one url
// (MAG-3326), else that url — and the whole provider only when a path it serves
// has no url left. Urls are the provider's configured node urls, by position.
//
// "Service" is a name as it appears in NodeUrl.Addons, which mixes add-on and
// extension names — the router's own config makes no distinction, and neither
// does Endpoint.Addons/.Extensions, both of which are built from that one list.
//
// A failure that was only a rate limit refuses nothing: it says nothing about the
// node. It is held apart (Throttled) so re-verification can keep what it knew.
//
// Empty means everything a url declared was admitted.
type ProviderAdmission struct {
	refused           map[admissionKey]struct{}
	refusedURLs       map[int]error
	throttledServices map[admissionKey]struct{}
	throttledURLs     map[int]error
	throttle          error
}

// refusedServiceFor returns the service a failed verification should be charged
// to, or "" when the failure belongs to no single service and costs the url.
//
// EXTENSION FIRST, and this is the whole correctness of the feature. A
// VerificationContainer carries both an Addon and an Extension, and the narrower
// one is what was actually probed:
//
//   - {Addon:"", Extension:"archive"} — every EVM and cosmos spec keys the archive
//     check this way, on the BASE collection. Charging it to Addon would find ""
//     and take the url-wide path, which is the motivating case of this whole
//     ticket: "attach an archive url to a healthy provider, fail the archive
//     check, lose the provider". Charging it to "archive" refuses archive and
//     keeps the rest.
//   - {Addon:"trace", Extension:"archive"} — ethereum's trace collection carries an
//     archive-scoped pruning value. What failed is the node's archive-ness, not its
//     trace support, so refusing "trace" would drop working traffic and leave the
//     failing archive extension advertised.
//   - {Addon:"debug", Extension:""} — an add-on with no extension scope; charge the
//     add-on.
//   - {Addon:"", Extension:""} — the base collection's own. Nothing narrower than
//     the url to charge it to.
func refusedServiceFor(verification VerificationContainer) string {
	if verification.Extension != "" {
		return verification.Extension
	}
	return verification.Addon
}

func serviceKey(url common.NodeUrl, service string) admissionKey {
	return admissionKey{url: url.Url, internalPath: url.InternalPath, service: service}
}

func (a *ProviderAdmission) fail(url common.NodeUrl, service string) {
	if a.refused == nil {
		a.refused = map[admissionKey]struct{}{}
	}
	a.refused[serviceKey(url, service)] = struct{}{}
}

func (a *ProviderAdmission) throttleService(url common.NodeUrl, service string, cause error) {
	if a.throttledServices == nil {
		a.throttledServices = map[admissionKey]struct{}{}
	}
	a.throttledServices[serviceKey(url, service)] = struct{}{}
	if a.throttle == nil {
		a.throttle = cause
	}
}

func (a *ProviderAdmission) refuseURL(index int, cause error) {
	if a.refusedURLs == nil {
		a.refusedURLs = map[int]error{}
	}
	a.refusedURLs[index] = cause
}

func (a *ProviderAdmission) throttleURL(index int, cause error) {
	if a.throttledURLs == nil {
		a.throttledURLs = map[int]error{}
	}
	a.throttledURLs[index] = cause
	if a.throttle == nil {
		a.throttle = cause
	}
}

// URLRefused reports whether the configured node url at this position could not be
// dialed or failed a check no single service owns. The router builds no endpoint
// for it and hands it no subscription.
func (a ProviderAdmission) URLRefused(index int) bool {
	_, refused := a.refusedURLs[index]
	return refused
}

// RefusedURLs lists the positions of the refused node urls, in order.
func (a ProviderAdmission) RefusedURLs() []int {
	indices := make([]int, 0, len(a.refusedURLs))
	for index := range a.refusedURLs {
		indices = append(indices, index)
	}
	slices.Sort(indices)
	return indices
}

// Throttled is the first rate limit this pass met in place of an answer, or nil.
func (a ProviderAdmission) Throttled() error { return a.throttle }

// Any reports whether any service or url was refused.
func (a ProviderAdmission) Any() bool { return len(a.refused) > 0 || len(a.refusedURLs) > 0 }

// Equal reports whether two admissions refuse exactly the same services and urls.
// The epoch path uses it to notice that a provider's admitted set has moved, which
// is the trigger for rebuilding its session — an active session reuses the
// endpoints it was built with, so a recovered service is not picked up by
// re-validating alone.
func (a ProviderAdmission) Equal(other ProviderAdmission) bool {
	if len(a.refused) != len(other.refused) || len(a.refusedURLs) != len(other.refusedURLs) {
		return false
	}
	for key := range a.refused {
		if _, ok := other.refused[key]; !ok {
			return false
		}
	}
	for index := range a.refusedURLs {
		if _, ok := other.refusedURLs[index]; !ok {
			return false
		}
	}
	return true
}

// KeepingThrottledFrom settles what this pass could not: each url or service the
// vendor only rate-limited keeps the verdict previous gave it.
func (a ProviderAdmission) KeepingThrottledFrom(previous ProviderAdmission) ProviderAdmission {
	kept := a.refusals()
	for index := range a.throttledURLs {
		if cause, refused := previous.refusedURLs[index]; refused {
			kept.refuseURL(index, cause)
		}
	}
	for key := range a.throttledServices {
		if _, refused := previous.refused[key]; refused {
			if kept.refused == nil {
				kept.refused = map[admissionKey]struct{}{}
			}
			kept.refused[key] = struct{}{}
		}
	}
	return kept
}

// WithoutURLRefusals admits the node urls at these positions again.
func (a ProviderAdmission) WithoutURLRefusals(indices []int) ProviderAdmission {
	kept := a.refusals()
	kept.throttledURLs, kept.throttledServices, kept.throttle = a.throttledURLs, a.throttledServices, a.throttle
	for _, index := range indices {
		delete(kept.refusedURLs, index)
	}
	return kept
}

// refusals is a copy of the refusals alone, sharing no map with a.
func (a ProviderAdmission) refusals() ProviderAdmission {
	var copied ProviderAdmission
	for key := range a.refused {
		if copied.refused == nil {
			copied.refused = map[admissionKey]struct{}{}
		}
		copied.refused[key] = struct{}{}
	}
	for index, cause := range a.refusedURLs {
		copied.refuseURL(index, cause)
	}
	return copied
}

// AdmittedServices returns the services this url may still claim, and whether the
// url is worth building an endpoint for at all. A refused url is the caller's to
// drop (URLRefused); this answers for the services of one that was not.
//
// keep is false when a url that serves ONLY its add-ons (standalone-addons) has
// had every one refused. Such a url has nothing left: emptying its service list
// would flip ServesBaseCollection() to true and hand a node its operator declared
// cannot answer the base collection straight back to base traffic — the MAG-3296
// failure, re-entered from the other side.
func (a ProviderAdmission) AdmittedServices(url common.NodeUrl) (services []string, keep bool) {
	// The common case by far — every healthy provider, on every session rebuild
	// (boot, each failed-provider retry, each epoch tick). Returning the input
	// avoids an allocation per url per rebuild; the test pins slice identity
	// rather than equality, since a deep compare would not notice its loss.
	if len(a.refused) == 0 || len(url.Addons) == 0 {
		return url.Addons, true
	}
	services = make([]string, 0, len(url.Addons))
	for _, service := range url.Addons {
		if _, bad := a.refused[serviceKey(url, service)]; bad {
			continue
		}
		services = append(services, service)
	}
	if len(services) == 0 && !url.ServesBaseCollection() {
		return nil, false
	}
	return services, true
}

// servicesIn lists, in the url's own order, the url's services found in set.
func servicesIn(set map[admissionKey]struct{}, url common.NodeUrl) []string {
	var found []string
	for _, service := range url.Addons {
		if _, in := set[serviceKey(url, service)]; in {
			found = append(found, service)
		}
	}
	return found
}

// servingError says why a provider has nothing left to serve from, or nil. A path
// it declares a non-websocket url for has failed when every url there was refused,
// and is unknown when the rest were only rate-limited. A failed path makes the
// error a failure; only unknown ones make it the rate limit, which re-verification
// reads as inconclusive.
func (a ProviderAdmission) servingError(nodeUrls []common.NodeUrl) error {
	type pathState struct {
		served            bool
		refused, throttle []error
	}
	paths := map[string]*pathState{}
	var order []string
	for index, url := range nodeUrls {
		if isWebSocketUrl(url.Url) {
			continue
		}
		state, seen := paths[url.InternalPath]
		if !seen {
			state = &pathState{}
			paths[url.InternalPath] = state
			order = append(order, url.InternalPath)
		}
		if cause, refused := a.refusedURLs[index]; refused {
			state.refused = append(state.refused, cause)
		} else if cause, throttled := a.throttledURLs[index]; throttled {
			state.throttle = append(state.throttle, cause)
		} else {
			state.served = true
		}
	}
	if len(order) == 0 {
		return errors.New("no node url left serving the root path: every configured url is a websocket")
	}
	where := func(path string) string {
		if path == "" {
			return "the root path"
		}
		return "internal path " + path
	}
	var unknown error
	for _, path := range order {
		state := paths[path]
		switch {
		case state.served:
		case len(state.throttle) == 0:
			return fmt.Errorf("no node url left serving %s: %w", where(path), state.refused[0])
		case unknown == nil:
			unknown = fmt.Errorf("no node url verified serving %s: %w", where(path), state.throttle[0])
		}
	}
	return unknown
}

// Validate reports the first Fail-severity failure on any node url, or nil.
func (cf *ChainFetcher) Validate(ctx context.Context) error {
	for _, outcome := range cf.verifyNodeUrls(ctx, verifyOptions{}) {
		if outcome.err != nil {
			return outcome.err
		}
		for _, check := range outcome.checks {
			if check.err != nil && check.verification.Severity == spectypes.ParseValue_Fail {
				return utils.LavaFormatError("invalid Verification on provider startup", check.err,
					utils.Attribute{Key: "Addons", Value: outcome.url.Addons}, utils.Attribute{Key: "verification", Value: check.verification.Name})
			}
		}
	}
	return nil
}

// ValidateCollections runs the same verifications Validate does, on every node
// url, and charges each Fail-severity failure to the narrowest thing that owns it:
// one service (MAG-3326), else the url. The provider is refused only when a path it
// serves has no url left — see servingError.
//
// Deliberately NOT derived: a base-collection failure does not turn a url into a
// standalone-addons one. Only an operator can tell "serves only EVM by design"
// from "its Substrate side is down right now".
func (cf *ChainFetcher) ValidateCollections(ctx context.Context) (ProviderAdmission, error) {
	return admissionFrom(cf.endpoint, cf.verifyNodeUrls(ctx, verifyOptions{}))
}

// VerificationResult is the outcome of running a single spec verification against one node URL.
// It carries the spec-derived identity of the verification (Name/Addon/Extension/Severity) plus
// whether the crafted relay succeeded. It is used by the `smartrouter health` command, which
// needs every verification's result as data rather than short-circuiting on the first failure
// (as Validate does).
type VerificationResult struct {
	Name      string `json:"name"`
	Addon     string `json:"addon"`
	Extension string `json:"extension"`
	// Severity is the spec's: the router refuses on a failed "Fail" and only logs
	// a failed "Warning".
	Severity string `json:"severity"`
	Ok       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
}

// NodeURLValidation is one node url's health outcome, verified over its own
// connection, and what the router does with that url as a result.
type NodeURLValidation struct {
	URL string
	// LatestBlock is this url's own height: 0 when the spec gives it no height
	// request at its own path, NOT_APPLICABLE when the request failed
	// (LatestBlockError says why).
	LatestBlock      int64
	LatestBlockError string
	// Error is set when the url could not be dialed or its verifications listed.
	Error         string
	Verifications []VerificationResult
	// Refused: the router builds nothing for this url, for the reason Refusal
	// gives. Throttled: its checks were only rate-limited, which decides nothing.
	// RefusedServices / ThrottledServices: the same, for one service of a url the
	// router otherwise admits.
	Refused           bool
	Refusal           string
	Throttled         string
	RefusedServices   []string
	ThrottledServices []string
}

// ValidateReport verifies every node url over its own connection, reads each url's
// own height, and returns every result with the router's verdict on the provider:
// nil when the router would admit it. It is the engine behind `smartrouter health`.
func (cf *ChainFetcher) ValidateReport(ctx context.Context) ([]NodeURLValidation, error) {
	outcomes := cf.verifyNodeUrls(ctx, verifyOptions{readEveryHead: true})
	admission, err := admissionFrom(cf.endpoint, outcomes)
	results := make([]NodeURLValidation, 0, len(outcomes))
	for index, outcome := range outcomes {
		results = append(results, outcome.report(index, admission))
	}
	return results, err
}

// errCheckNotRun marks a check its pass never started: the deadline had passed.
var errCheckNotRun = errors.New("not run: verification deadline exceeded before this check started")

// verifyOptions is how much one verification pass asks of a provider.
type verifyOptions struct {
	// readEveryHead reads every url's height, not just the ones a check needs.
	readEveryHead bool
}

type checkOutcome struct {
	verification VerificationContainer
	err          error
}

// nodeUrlOutcome is one node url's pass: its connection, its head, every check.
type nodeUrlOutcome struct {
	url common.NodeUrl
	// err is set when the url could not be dialed or its checks could not be listed.
	err     error
	head    int64
	headErr error
	checks  []checkOutcome
}

func (o nodeUrlOutcome) report(index int, admission ProviderAdmission) NodeURLValidation {
	result := NodeURLValidation{
		URL:           o.url.UrlStr(),
		LatestBlock:   o.head,
		Verifications: make([]VerificationResult, 0, len(o.checks)),
	}
	if o.err != nil {
		result.Error = o.err.Error()
	}
	if o.headErr != nil {
		result.LatestBlockError = o.headErr.Error()
	}
	for _, check := range o.checks {
		verification := VerificationResult{
			Name:      check.verification.Name,
			Addon:     check.verification.Addon,
			Extension: check.verification.Extension,
			Severity:  check.verification.Severity.String(),
			Ok:        check.err == nil,
		}
		if check.err != nil {
			verification.Error = check.err.Error()
		}
		result.Verifications = append(result.Verifications, verification)
	}
	if cause, refused := admission.refusedURLs[index]; refused {
		result.Refused, result.Refusal = true, cause.Error()
		return result
	}
	if cause, throttled := admission.throttledURLs[index]; throttled {
		result.Throttled = cause.Error()
	}
	result.RefusedServices = servicesIn(admission.refused, o.url)
	result.ThrottledServices = servicesIn(admission.throttledServices, o.url)
	return result
}

// admissionFrom charges every failed Fail-severity check to the narrowest thing
// that owns it — one service on one url, else the url — setting a rate limit apart
// from a refusal, and refuses the provider only when a path it serves is left with
// no url (servingError).
func admissionFrom(endpoint *lavasession.RPCProviderEndpoint, outcomes []nodeUrlOutcome) (ProviderAdmission, error) {
	var admission ProviderAdmission
	for index, outcome := range outcomes {
		var failed, throttled error
		charge := func(err error) {
			switch {
			case IsRateLimitFailure(err):
				if throttled == nil {
					throttled = err
				}
			case failed == nil:
				failed = err
			}
		}
		if outcome.err != nil {
			charge(outcome.err)
		}
		for _, check := range outcome.checks {
			if check.err == nil || check.verification.Severity != spectypes.ParseValue_Fail {
				continue
			}
			service := refusedServiceFor(check.verification)
			switch {
			case service == "":
				charge(fmt.Errorf("%s verification failed: %w", check.verification.Name, check.err))
			case IsRateLimitFailure(check.err):
				admission.throttleService(outcome.url, service, check.err)
			default:
				admission.fail(outcome.url, service)
				utils.LavaFormatWarning("refusing one service for this url, keeping the provider", check.err,
					utils.LogAttr("url", outcome.url.String()),
					utils.LogAttr("service", service),
					utils.LogAttr("addon", check.verification.Addon),
					utils.LogAttr("extension", check.verification.Extension),
					utils.LogAttr("verification", check.verification.Name),
				)
			}
		}
		switch {
		case failed != nil:
			admission.refuseURL(index, failed)
			utils.LavaFormatWarning("refusing node url", failed, utils.LogAttr("url", outcome.url.String()))
		case throttled != nil:
			admission.throttleURL(index, throttled)
			utils.LavaFormatWarning("node url rate-limited its checks; not refusing it", throttled, utils.LogAttr("url", outcome.url.String()))
		}
	}
	return admission, admission.servingError(endpoint.NodeUrls)
}

// verifyNodeUrls runs each node url's verifications over its own connection. Urls
// that share a connection — alike in all but the services they declare — are dialed
// once and asked each check once, in order; urls that do not are asked
// concurrently, so one that hangs costs only itself. Outcomes are in configured
// order.
func (cf *ChainFetcher) verifyNodeUrls(ctx context.Context, opts verifyOptions) []nodeUrlOutcome {
	defer cf.invalidateVerificationsCache()
	outcomes := make([]nodeUrlOutcome, len(cf.endpoint.NodeUrls))
	var wg sync.WaitGroup
	for _, positions := range connectionGroups(cf.endpoint.NodeUrls) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group := cf.newConnectionGroup(cf.endpoint.NodeUrls[positions[0]])
			for _, index := range positions {
				outcomes[index] = group.verify(ctx, cf.endpoint.NodeUrls[index], opts)
			}
		}()
	}
	wg.Wait()
	return outcomes
}

// connectionGroups splits node urls by the connection they need — everything but
// the services a url declares — keeping configured order within each group.
func connectionGroups(urls []common.NodeUrl) [][]int {
	var order []string
	groups := map[string][]int{}
	for index, url := range urls {
		key := connectionKey(url)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], index)
	}
	out := make([][]int, 0, len(order))
	for _, key := range order {
		out = append(out, groups[key])
	}
	return out
}

func connectionKey(url common.NodeUrl) string {
	url.Addons, url.SkipVerifications, url.Methods, url.StandaloneAddons = nil, nil, nil, false
	key, err := json.Marshal(url)
	if err != nil {
		// Unreachable for NodeUrl's field types; a url alone is still a correct, coarser key.
		return url.Url + "\x00" + url.InternalPath
	}
	return string(key)
}

// connectionGroup is one connection's share of a pass: the node urls that share
// it, dialed once, with each check asked once and each head read once.
type connectionGroup struct {
	cf      *ChainFetcher
	parser  ChainParser
	url     common.NodeUrl
	router  ChainRouter
	dialErr error
	dialed  bool
	heads   map[string]headRead
	checks  map[string]error
}

type headRead struct {
	block int64
	err   error
}

func (cf *ChainFetcher) newConnectionGroup(url common.NodeUrl) *connectionGroup {
	parser := cf.chainParser
	if cf.nodeUrlRouter != nil {
		// A gRPC proxy binds its parser to the connection it dials, so a group that
		// dials its own connection gets a parser of its own.
		parser = CloneChainParserForValidation(cf.chainParser)
	}
	return &connectionGroup{cf: cf, parser: parser, url: url, heads: map[string]headRead{}, checks: map[string]error{}}
}

func (g *connectionGroup) dial(ctx context.Context) (ChainRouter, error) {
	if !g.dialed {
		g.dialed = true
		if g.cf.nodeUrlRouter == nil {
			g.router = g.cf.chainRouter
		} else {
			g.router, g.dialErr = g.cf.nodeUrlRouter(ctx, g.url, g.parser)
		}
	}
	return g.router, g.dialErr
}

// verify is one url's share of a pass. A websocket url answers only for the checks
// the router widens to the websocket extension, and for none when websocket
// verification is off — it is then only dialed.
func (g *connectionGroup) verify(ctx context.Context, url common.NodeUrl, opts verifyOptions) (outcome nodeUrlOutcome) {
	outcome.url = url
	utils.LavaFormatInfo("starting validation for url", utils.LogAttr("url", url.String()))
	url, err := servedAs(g.parser, url)
	if err != nil {
		outcome.err = err
		return outcome
	}
	verifications, err := g.cf.nodeUrlVerifications(g.parser, url)
	if err != nil {
		outcome.err = err
		return outcome
	}
	socket := isWebSocketUrl(url.Url)
	if socket {
		verifications = socketVerifications(g.parser, verifications)
	} else if len(verifications) == 0 {
		utils.LavaFormatWarning("no verifications for url", nil, utils.LogAttr("url", url.String()))
	}

	router, err := g.dial(ctx)
	if err != nil {
		outcome.err = fmt.Errorf("connect: %w", err)
		return outcome
	}

	needHead := needsLatestBlock(url, verifications)
	if needHead || (opts.readEveryHead && readsOwnHead(g.parser, url, socket)) {
		outcome.head, outcome.headErr = g.head(ctx, router, url)
	}
	// Only a check that needs the head is handed one, so every mode verifies alike.
	var latestBlock uint64
	if needHead && outcome.headErr == nil {
		latestBlock = uint64(outcome.head)
	}

	for _, verification := range verifications {
		if skipVerification(url, verification.Name) {
			utils.LavaFormatInfo("Skipping Verification due to provider configuration (skip-verifications setting)", utils.LogAttr("verification", verification.Name))
			continue
		}
		var verifyErr error
		switch {
		case ctx.Err() != nil:
			verifyErr = errCheckNotRun
		case verificationNeedsHead(verification) && outcome.headErr != nil:
			verifyErr = fmt.Errorf("latest block unavailable: %w", outcome.headErr)
		default:
			verifyErr = g.check(ctx, router, verification, latestBlock)
		}
		g.cf.verificationsStatus.Store(g.cf.getVerificationsKey(verification, g.cf.endpoint.ApiInterface, g.cf.endpoint.ChainID), verifyErr == nil)
		outcome.checks = append(outcome.checks, checkOutcome{verification: verification, err: verifyErr})
		if verifyErr != nil {
			utils.LavaFormatWarning("failed verification on provider startup", verifyErr, utils.LogAttr("verification", verification.Name))
		}
	}
	return outcome
}

func (g *connectionGroup) head(ctx context.Context, router ChainRouter, url common.NodeUrl) (int64, error) {
	key := strings.Join(url.Addons, ",") + "\x00" + strconv.FormatBool(url.ServesBaseCollection())
	if read, ok := g.heads[key]; ok {
		return read.block, read.err
	}
	block, err := g.cf.readHead(ctx, router, g.parser, url)
	g.heads[key] = headRead{block: block, err: err}
	return block, err
}

func (g *connectionGroup) check(ctx context.Context, router ChainRouter, verification VerificationContainer, latestBlock uint64) error {
	parsing := verification.ParseDirective
	key := strings.Join([]string{
		verification.Name, verification.Addon, verification.Extension, verification.InternalPath,
		verification.ConnectionType, parsing.ApiName, parsing.FunctionTemplate, verification.Value,
		strconv.FormatUint(verification.LatestDistance, 10), strconv.FormatUint(latestBlock, 10),
	}, "\x00")
	if err, asked := g.checks[key]; asked {
		return err
	}
	err := g.cf.verifyWithRetries(ctx, router, g.parser, verification, latestBlock)
	g.checks[key] = err
	return err
}

// nodeUrlVerifications is everything a url serves: the base collection and each of
// its add-ons, and each scoped to its extensions. GetVerifications returns only the
// extension-scoped checks for a url declaring an extension, and such a url still
// takes base traffic.
func (cf *ChainFetcher) nodeUrlVerifications(chainParser ChainParser, url common.NodeUrl) ([]VerificationContainer, error) {
	scoped, err := chainParser.GetVerifications(url.Addons, url.InternalPath, cf.endpoint.ApiInterface)
	if err != nil || len(url.Addons) == 0 {
		return verificationsForNodeUrl(url, scoped), err
	}
	addons, extensions, err := chainParser.SeparateAddonsExtensions(context.Background(), url.Addons)
	if err != nil {
		return nil, err
	}
	if len(extensions) == 0 {
		return verificationsForNodeUrl(url, scoped), nil
	}
	unscoped, err := chainParser.GetVerifications(addons, url.InternalPath, cf.endpoint.ApiInterface)
	if err != nil {
		return nil, err
	}
	return verificationsForNodeUrl(url, append(unscoped, scoped...)), nil
}

// servedAs is a url as its relay endpoint serves it: standalone-addons names add-on
// collections, so a url declaring only extensions serves the base collection.
func servedAs(chainParser ChainParser, url common.NodeUrl) (common.NodeUrl, error) {
	if !url.StandaloneAddons || len(url.Addons) == 0 {
		return url, nil
	}
	addons, _, err := chainParser.SeparateAddonsExtensions(context.Background(), url.Addons)
	if err != nil {
		return url, err
	}
	if len(addons) == 0 {
		url.StandaloneAddons = false
	}
	return url, nil
}

// socketVerifications keeps the checks a websocket url answers for: those in a
// collection that subscribes, which the router widens to the websocket extension.
func socketVerifications(chainParser ChainParser, verifications []VerificationContainer) []VerificationContainer {
	if chainParser.SkipWebsocketVerification() {
		return nil
	}
	kept := make([]VerificationContainer, 0, len(verifications))
	for _, verification := range verifications {
		if slices.Contains(getExtensionsForVerification(verification, chainParser), WebSocketExtension) {
			kept = append(kept, verification)
		}
	}
	return kept
}

// readsOwnHead reports whether a report pass reads this url's height: the spec
// gives it a height request at its own path, and it is not a socket that is only
// dialed. A root url of an internal-path spec gets its height request at a path
// its relays never take.
func readsOwnHead(chainParser ChainParser, url common.NodeUrl, socket bool) bool {
	if socket && chainParser.SkipWebsocketVerification() {
		return false
	}
	_, collection, ok := chainParser.GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, url.Addons, url.InternalPath, url.ServesBaseCollection())
	return ok && collection != nil && collection.CollectionData.InternalPath == url.InternalPath
}

// readHead asks a url for the head of the collections it serves (MAG-3296),
// with the same retry budget a check gets.
func (cf *ChainFetcher) readHead(ctx context.Context, router ChainRouter, chainParser ChainParser, url common.NodeUrl) (int64, error) {
	var block int64
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		block, err = cf.fetchLatestBlockNumVia(ctx, router, chainParser, url.Addons, url.InternalPath, url.ServesBaseCollection())
		if stopValidateRetries(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return spectypes.NOT_APPLICABLE, deadlineContext(ctx, err)
	}
	return block, nil
}

// verifyWithRetries gives a check several chances, for a node still starting up.
func (cf *ChainFetcher) verifyWithRetries(ctx context.Context, router ChainRouter, chainParser ChainParser, verification VerificationContainer, latestBlock uint64) error {
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		err = cf.verifyVia(ctx, router, chainParser, verification, latestBlock)
		if stopValidateRetries(err) || ctx.Err() != nil {
			break
		}
	}
	return deadlineContext(ctx, err)
}

// deadlineContext says so when a failure came with the pass's deadline: the
// connector closes as the deadline passes, and its own words would hide why.
func deadlineContext(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil {
		return err
	}
	return fmt.Errorf("verification deadline exceeded: %w", err)
}

func (cf *ChainFetcher) populateCache(relayData *pairingtypes.RelayPrivateData, reply *pairingtypes.RelayReply, requestedBlockHash []byte, finalized bool) {
	if cf.cache != nil && cf.cache.CacheActive() && (requestedBlockHash != nil || finalized) {
		new_ctx := context.Background()
		new_ctx, cancel := context.WithTimeout(new_ctx, common.CacheWriteTimeout)
		defer cancel()
		// provider side doesn't use SharedStateId, so we default it to empty so it wont have effect.

		hash, _, err := HashCacheRequest(relayData, cf.endpoint.ChainID)
		if err != nil {
			utils.LavaFormatError("populateCache Failed getting Hash for request", err)
			return
		}

		_, averageBlockTime, _, _ := cf.chainParser.ChainBlockStats()
		err = cf.cache.SetEntry(new_ctx, &pairingtypes.RelayCacheSet{
			RequestHash:      hash,
			BlockHash:        requestedBlockHash,
			ChainId:          cf.endpoint.ChainID,
			Response:         reply,
			Finalized:        finalized,
			OptionalMetadata: nil,
			RequestedBlock:   relayData.RequestBlock,
			SeenBlock:        relayData.SeenBlock, // seen block is latestBlock so it will hit consumers requesting it.
			SharedStateId:    "",
			AverageBlockTime: int64(averageBlockTime),
		})
		if err != nil {
			utils.LavaFormatWarning("chain fetcher error updating cache with new entry", err)
		}
	}
}

func getExtensionsForVerification(verification VerificationContainer, chainParser ChainParser) []string {
	extensions := []string{verification.Extension}

	collectionKey := CollectionKey{
		InternalPath:   verification.InternalPath,
		Addon:          verification.Addon,
		ConnectionType: verification.ConnectionType,
	}

	if chainParser.IsTagInCollection(spectypes.FUNCTION_TAG_SUBSCRIBE, collectionKey) && !chainParser.SkipWebsocketVerification() {
		if verification.Extension == "" {
			extensions = []string{WebSocketExtension}
		} else {
			extensions = append(extensions, WebSocketExtension)
		}
	}

	return extensions
}

func (cf *ChainFetcher) verifyVia(ctx context.Context, router ChainRouter, chainParser ChainParser, verification VerificationContainer, latestBlock uint64) error {
	parsing := &verification.ParseDirective

	collectionType := verification.ConnectionType
	path := parsing.ApiName
	data := []byte(parsing.FunctionTemplate)

	if !verification.IsActive() {
		utils.LavaFormatDebug("skipping disabled verification", []utils.Attribute{
			{Key: "Extension", Value: verification.Extension},
			{Key: "Addon", Value: verification.Addon},
			utils.LogAttr("name", verification.Name),
			{Key: "chainID", Value: cf.endpoint.ChainID},
			{Key: "APIInterface", Value: cf.endpoint.ApiInterface},
		}...)
		return nil
	}

	// craft data for GET_BLOCK_BY_NUM verification that cannot use "earliest"
	// also check for %d because the data constructed assumes its presence
	if verification.ParseDirective.FunctionTag == spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM {
		if verification.LatestDistance != 0 && latestBlock != 0 {
			if latestBlock >= verification.LatestDistance {
				data = []byte(fmt.Sprintf(parsing.FunctionTemplate, latestBlock-verification.LatestDistance))
			} else {
				return utils.LavaFormatWarning("[-] verify failed getting non-earliest block for chainMessage", fmt.Errorf("latestBlock is smaller than latestDistance"),
					utils.LogAttr("path", path),
					utils.LogAttr("latest_block", latestBlock),
					utils.LogAttr("latest_distance", verification.LatestDistance),
				)
			}
		} else if verification.Value != "" {
			expectedValue, err := strconv.ParseInt(verification.Value, 10, 64)
			if err != nil {
				return utils.LavaFormatError("failed converting expected value to number", err, utils.LogAttr("value", verification.Value))
			}
			data = []byte(fmt.Sprintf(parsing.FunctionTemplate, expectedValue))
		} else {
			return utils.LavaFormatWarning("[-] verification misconfiguration", fmt.Errorf("FUNCTION_TAG_GET_BLOCK_BY_NUM defined without LatestDistance or LatestBlock or a proper expected value"),
				utils.LogAttr("latest_block", latestBlock),
				utils.LogAttr("latest_distance", verification.LatestDistance),
				utils.LogAttr("expected_value", verification.Value),
			)
		}
	}

	craftData := &CraftData{Path: path, Data: data, ConnectionType: collectionType, InternalPath: verification.InternalPath}
	chainMessage, err := CraftChainMessage(parsing, collectionType, chainParser, craftData, cf.ChainFetcherMetadata())
	if err != nil {
		return utils.LavaFormatError("[-] verify failed creating chainMessage", err, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}

	extensions := getExtensionsForVerification(verification, chainParser)

	reply, proxyUrl, chainId, err := router.SendNodeMsg(ctx, chainMessage, extensions)
	if err != nil {
		return utils.LavaFormatWarning("[-] verify failed sending chainMessage", err,
			utils.LogAttr("chainID", cf.endpoint.ChainID),
			utils.LogAttr("APIInterface", cf.endpoint.ApiInterface),
			utils.LogAttr("extensions", extensions),
		)
	}
	if reply == nil || reply.RelayReply == nil {
		return utils.LavaFormatWarning("[-] verify failed sending chainMessage, reply or reply.RelayReply are nil", nil,
			utils.LogAttr("chainID", cf.endpoint.ChainID),
			utils.LogAttr("APIInterface", cf.endpoint.ApiInterface),
		)
	}

	parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
	if err != nil {
		return utils.LavaFormatWarning("[-] verify failed to parse result", err,
			utils.LogAttr("chain_id", chainId),
			utils.LogAttr("Api_interface", cf.endpoint.ApiInterface),
			utils.LogAttr("function_template", parsing.FunctionTemplate),
		)
	}

	parsedInput := parser.ParseBlockFromReply(parserInput, parsing.ResultParsing, parsing.Parsers)
	if parsedInput.GetRawParsedData() == "" {
		return utils.LavaFormatWarning("[-] verify failed to parse result", nil,
			utils.LogAttr("chainId", chainId),
			utils.LogAttr("nodeUrl", proxyUrl.Url),
			utils.LogAttr("Method", parsing.GetApiName()),
			utils.LogAttr("Response", parser.CapStringLen(string(reply.RelayReply.Data))),
		)
	}

	parserError := parsedInput.GetParserError()
	if parserError != "" {
		return utils.LavaFormatWarning("[-] parser returned an error", nil,
			utils.LogAttr("error", parserError),
			utils.LogAttr("chainId", chainId),
			utils.LogAttr("nodeUrl", proxyUrl.Url),
			utils.LogAttr("Method", parsing.GetApiName()),
			utils.LogAttr("Response", parser.CapStringLen(string(reply.RelayReply.Data))),
		)
	}
	if verification.LatestDistance != 0 && latestBlock != 0 && verification.ParseDirective.FunctionTag != spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM {
		parsedResultAsNumber := parsedInput.GetBlock()
		if parsedResultAsNumber == spectypes.NOT_APPLICABLE {
			return utils.LavaFormatWarning("[-] verify failed to parse result as number", nil,
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("nodeUrl", proxyUrl.Url),
				utils.LogAttr("Method", parsing.GetApiName()),
				utils.LogAttr("Response", parser.CapStringLen(string(reply.RelayReply.Data))),
				utils.LogAttr("rawParsedData", parsedInput.GetRawParsedData()),
			)
		}
		uint64ParsedResultAsNumber := uint64(parsedResultAsNumber)
		if uint64ParsedResultAsNumber > latestBlock {
			return utils.LavaFormatWarning("[-] verify failed parsed result is greater than latestBlock", nil,
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("nodeUrl", proxyUrl.Url),
				utils.LogAttr("Method", parsing.GetApiName()),
				utils.LogAttr("latestBlock", latestBlock),
				utils.LogAttr("parsedResult", uint64ParsedResultAsNumber),
			)
		}
		if latestBlock-uint64ParsedResultAsNumber < verification.LatestDistance {
			return utils.LavaFormatWarning("[-] verify failed expected block distance is not sufficient", nil,
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("nodeUrl", proxyUrl.Url),
				utils.LogAttr("Method", parsing.GetApiName()),
				utils.LogAttr("latestBlock", latestBlock),
				utils.LogAttr("parsedResult", uint64ParsedResultAsNumber),
				utils.LogAttr("expected", verification.LatestDistance),
			)
		}
	}
	// some verifications only want the response to be valid, and don't care about the value
	if verification.Value != "*" && verification.Value != "" && verification.ParseDirective.FunctionTag != spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM {
		rawData := parsedInput.GetRawParsedData()
		if rawData != verification.Value {
			return utils.LavaFormatWarning("[-] verify failed expected and received are different", nil,
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("nodeUrl", proxyUrl.Url),
				utils.LogAttr("rawParsedBlock", rawData),
				utils.LogAttr("verification.Value", verification.Value),
				utils.LogAttr("Method", parsing.GetApiName()),
				utils.LogAttr("Extension", verification.Extension),
				utils.LogAttr("Addon", verification.Addon),
				utils.LogAttr("Verification", verification.Name),
			)
		}
	}

	utils.LavaFormatInfo("[+] verified successfully",
		utils.LogAttr("chainId", chainId),
		utils.LogAttr("nodeUrl", proxyUrl.Url),
		utils.LogAttr("verification", verification.Name),
		utils.LogAttr("block", parsedInput.GetBlock()),
		utils.LogAttr("rawData", parsedInput.GetRawParsedData()),
		utils.LogAttr("verificationKey", verification.VerificationKey),
		utils.LogAttr("apiInterface", cf.endpoint.ApiInterface),
		utils.LogAttr("internalPath", proxyUrl.InternalPath),
	)
	return nil
}

func (cf *ChainFetcher) ChainFetcherMetadata() []pairingtypes.Metadata {
	ret := []pairingtypes.Metadata{
		{Name: ChainFetcherHeaderName, Value: cf.FetchEndpoint().NetworkAddress.Address},
	}
	return ret
}

func (cf *ChainFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	utils.LavaFormatTrace("Sending CustomMessage", utils.Attribute{Key: "path", Value: path}, utils.Attribute{Key: "data", Value: data}, utils.Attribute{Key: "connectionType", Value: connectionType}, utils.Attribute{Key: "apiName", Value: apiName})
	craftData := &CraftData{Path: path, Data: data, ConnectionType: connectionType}
	parsing := &spectypes.ParseDirective{
		ApiName:          apiName,
		FunctionTemplate: "",
		ResultParsing:    spectypes.BlockParser{},
		Parsers:          []spectypes.GenericParser{},
	}
	chainMessage, err := CraftChainMessage(parsing, connectionType, cf.chainParser, craftData, cf.ChainFetcherMetadata())
	if err != nil {
		return nil, err
	}
	reply, _, _, err := cf.chainRouter.SendNodeMsg(ctx, chainMessage, nil)
	utils.LavaFormatTrace("CustomMessage", utils.Attribute{Key: "reply", Value: reply})
	if err != nil {
		return nil, err
	}
	return reply.RelayReply.Data, nil
}

// FetchLatestBlockNum probes the chain head using the base collection's
// GET_BLOCKNUM directive. Callers that know which collections the node actually
// serves should use FetchLatestBlockNumForCollection instead — see MAG-3296.
func (cf *ChainFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	return cf.FetchLatestBlockNumForCollection(ctx, nil, "", true)
}

// FetchLatestBlockNumForCollection probes the chain head with the GET_BLOCKNUM
// directive of the collection this node URL actually serves.
//
// MAG-3296: this used to be the base collection's directive for every node,
// which excluded any provider whose add-on is a disjoint api surface rather than
// a superset — an EVM-only Acala node answers the `evm` collection's
// eth_blockNumber and cannot answer the base collection's Substrate
// chain_getHeader at all. On the admission path a failed head probe returns
// before a single verification runs, so such a provider was dropped without ever
// being asked the questions it could answer.
func (cf *ChainFetcher) FetchLatestBlockNumForCollection(ctx context.Context, addons []string, internalPath string, allowBaseFallback bool) (int64, error) {
	return cf.fetchLatestBlockNumVia(ctx, cf.chainRouter, cf.chainParser, addons, internalPath, allowBaseFallback)
}

func (cf *ChainFetcher) fetchLatestBlockNumVia(ctx context.Context, router ChainRouter, chainParser ChainParser, addons []string, internalPath string, allowBaseFallback bool) (int64, error) {
	parsing, apiCollection, ok := chainParser.GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, addons, internalPath, allowBaseFallback)
	tagName := spectypes.FUNCTION_TAG_GET_BLOCKNUM.String()
	if !ok {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatError(tagName+" tag function not found", nil, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}
	collectionData := apiCollection.CollectionData
	var craftData *CraftData
	if parsing.FunctionTemplate != "" {
		path := parsing.ApiName
		data := []byte(parsing.FunctionTemplate)
		craftData = &CraftData{Path: path, Data: data, ConnectionType: collectionData.Type}
	}
	chainMessage, err := CraftChainMessage(parsing, collectionData.Type, chainParser, craftData, cf.ChainFetcherMetadata())
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatError(tagName+" failed creating chainMessage", err, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}
	reply, proxyUrl, chainId, err := router.SendNodeMsg(ctx, chainMessage, nil)
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebugErr(tagName+" failed sending chainMessage", err, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}
	parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebug(tagName+" Failed formatResponseForParsing", []utils.Attribute{
			{Key: "chainId", Value: chainId},
			{Key: "nodeUrl", Value: proxyUrl.Url},
			{Key: "Method", Value: parsing.ApiName},
			{Key: "Response", Value: parser.CapStringLen(string(reply.RelayReply.Data))},
			{Key: "error", Value: err},
		}...)
	}
	parsedInput := parser.ParseBlockFromReply(parserInput, parsing.ResultParsing, parsing.Parsers)
	blockNum := parsedInput.GetBlock()
	if blockNum == spectypes.NOT_APPLICABLE {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebug(tagName+" Failed to parse Response", []utils.Attribute{
			{Key: "chainId", Value: chainId},
			{Key: "nodeUrl", Value: proxyUrl.Url},
			{Key: "Method", Value: parsing.ApiName},
			{Key: "Response", Value: parser.CapStringLen(string(reply.RelayReply.Data))},
			{Key: "error", Value: err},
		}...)
	}
	atomic.StoreInt64(&cf.latestBlock, blockNum)
	return blockNum, nil
}

func (cf *ChainFetcher) constructRelayData(conectionType string, path string, data []byte, requestBlock int64, addon string, extensions []string, latestBlock int64) *pairingtypes.RelayPrivateData {
	relayData := &pairingtypes.RelayPrivateData{
		ConnectionType: conectionType,
		ApiUrl:         path,
		Data:           data,
		RequestBlock:   requestBlock,
		ApiInterface:   cf.endpoint.ApiInterface,
		Metadata:       nil,
		Addon:          addon,
		Extensions:     extensions,
		SeenBlock:      latestBlock,
	}
	return relayData
}

func (cf *ChainFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	parsing, apiCollection, ok := cf.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM)
	tagName := spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM.String()
	if !ok {
		return "", utils.LavaFormatError(tagName+" tag function not found", nil, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}
	collectionData := apiCollection.CollectionData

	if parsing.FunctionTemplate == "" {
		return "", utils.LavaFormatError(tagName+" missing function template", nil, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}

	if blockNum < 0 {
		return "", utils.LavaFormatError(tagName+" invalid negative block number", nil,
			[]utils.Attribute{{Key: "blockNum", Value: blockNum}, {Key: "chainID", Value: cf.endpoint.ChainID}}...)
	}

	if !common.IsSolanaFamily(cf.endpoint.ChainID) {
		res, _, err := cf.fetchSingleBlockHashByNum(ctx, blockNum, parsing, collectionData, tagName)
		return res, err
	}

	fetchFn := func(fCtx context.Context, block int64) (string, []byte, error) {
		return cf.fetchSingleBlockHashByNum(fCtx, block, parsing, collectionData, tagName)
	}
	hash, fetchedBlock, err := FetchBlockHashWithSolanaRetry(ctx, blockNum, SameSlotRetryDelay, fetchFn)
	if err != nil {
		return "", utils.LavaFormatError(tagName+" all block-not-available retries exhausted", err,
			utils.LogAttr("originalBlock", blockNum),
			utils.LogAttr("chainID", cf.endpoint.ChainID),
		)
	}
	if fetchedBlock != blockNum {
		utils.LavaFormatWarning("Chain Tracker fetched previous slot after block-not-available",
			nil,
			utils.LogAttr("originalBlock", blockNum),
			utils.LogAttr("fetchedBlock", fetchedBlock),
			utils.LogAttr("chainID", cf.endpoint.ChainID),
		)
	}
	return hash, nil
}

// fetchSingleBlockHashByNum fetches the block hash for a single block number.
// Returns the hash, the raw response data (for error inspection), and any error.
func (cf *ChainFetcher) fetchSingleBlockHashByNum(ctx context.Context, blockNum int64, parsing *spectypes.ParseDirective, collectionData spectypes.CollectionData, tagName string) (string, []byte, error) {
	path := parsing.ApiName
	data := []byte(fmt.Sprintf(parsing.FunctionTemplate, blockNum))
	chainMessage, err := CraftChainMessage(parsing, collectionData.Type, cf.chainParser, &CraftData{Path: path, Data: data, ConnectionType: collectionData.Type}, cf.ChainFetcherMetadata())
	if err != nil {
		return "", nil, utils.LavaFormatError(tagName+" failed CraftChainMessage on function template", err, []utils.Attribute{{Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}
	start := time.Now()
	reply, proxyUrl, chainId, err := cf.chainRouter.SendNodeMsg(ctx, chainMessage, nil)
	if err != nil {
		timeTaken := time.Since(start)
		return "", nil, utils.LavaFormatDebugErr(tagName+" failed sending chainMessage", err, []utils.Attribute{{Key: "sendTime", Value: timeTaken}, {Key: "chainID", Value: cf.endpoint.ChainID}, {Key: "APIInterface", Value: cf.endpoint.ApiInterface}}...)
	}

	responseData := reply.RelayReply.Data

	parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
	if err != nil {
		return "", responseData, utils.LavaFormatDebug(tagName+" Failed formatResponseForParsing", []utils.Attribute{
			{Key: "error", Value: err},
			{Key: "chainId", Value: chainId},
			{Key: "nodeUrl", Value: proxyUrl.Url},
			{Key: "Method", Value: parsing.ApiName},
			{Key: "Response", Value: parser.CapStringLen(string(responseData))},
		}...)
	}

	res, err := parser.ParseBlockHashFromReplyAndDecode(parserInput, parsing.ResultParsing, parsing.Parsers)
	if err != nil {
		return "", responseData, utils.LavaFormatDebug(tagName+" Failed ParseMessageResponse", []utils.Attribute{
			{Key: "error", Value: err},
			{Key: "chainId", Value: chainId},
			{Key: "nodeUrl", Value: proxyUrl.Url},
			{Key: "Method", Value: parsing.ApiName},
			{Key: "Response", Value: parser.CapStringLen(string(responseData))},
		}...)
	}
	_, _, blockDistanceToFinalization, _ := cf.chainParser.ChainBlockStats()
	latestBlock := atomic.LoadInt64(&cf.latestBlock) // assuming FetchLatestBlockNum is called before this one it's always true
	if latestBlock > 0 {
		finalized := spectypes.IsFinalizedBlock(blockNum, latestBlock, int64(blockDistanceToFinalization))
		isNodeError, _ := chainMessage.CheckResponseError(reply.RelayReply.Data, reply.StatusCode)
		if !isNodeError { // skip cache populate on node errors, this is a protection but should never get here with node error as we parse the result prior.
			cf.populateCache(cf.constructRelayData(collectionData.Type, path, data, blockNum, "", nil, latestBlock), reply.RelayReply, []byte(res), finalized)
		}
	}
	return res, responseData, nil
}

type ChainFetcherOptions struct {
	ChainRouter ChainRouter
	ChainParser ChainParser
	Endpoint    *lavasession.RPCProviderEndpoint
	Cache       performance.CacheBackend
	// NodeUrlRouter, when set, is what verification sends each node url's checks
	// through: a router over that url alone, so no check lands on another url.
	NodeUrlRouter NodeUrlRouterFactory
}

func NewChainFetcher(ctx context.Context, options *ChainFetcherOptions) *ChainFetcher {
	return &ChainFetcher{
		chainRouter:   options.ChainRouter,
		nodeUrlRouter: options.NodeUrlRouter,
		chainParser:   options.ChainParser,
		endpoint:      options.Endpoint,
		cache:         options.Cache,
	}
}

func FormatResponseForParsing(reply *pairingtypes.RelayReply, chainMessage ChainMessageForSend) (parsable parser.RPCInput, err error) {
	var parserInput parser.RPCInput
	respData := reply.Data
	if len(respData) == 0 {
		return nil, utils.LavaFormatDebug("result (reply.Data) is empty, can't be formatted for parsing", utils.Attribute{Key: "error", Value: err})
	}
	rpcMessage := chainMessage.GetRPCMessage()
	if customParsingMessage, ok := rpcMessage.(chainproxy.CustomParsingMessage); ok {
		parserInput, err = customParsingMessage.NewParsableRPCInput(respData)
		if err != nil {
			return nil, utils.LavaFormatError("failed creating NewParsableRPCInput from CustomParsingMessage", err, utils.Attribute{Key: "data", Value: string(respData)})
		}
	} else {
		parserInput = chainproxy.DefaultParsableRPCInput(respData)
	}
	return parserInput, nil
}

// this method will calculate the request hash by changing the original object, and returning the data back to it after calculating the hash
// couldn't be used in parallel
func HashCacheRequest(relayData *pairingtypes.RelayPrivateData, chainId string) ([]byte, func([]byte) []byte, error) {
	return hashCacheRequest(relayData, chainId, "")
}

// isClientBodyHeader reports whether a metadata entry is one of clientBodyHeaders.
func isClientBodyHeader(entry pairingtypes.Metadata) bool {
	_, ok := clientBodyHeaders[strings.ToLower(entry.Name)]
	return ok
}

// metadataWithoutClientBodyHeaders returns metadata with the clientBodyHeaders entries removed,
// or metadata itself when it carries none, so the common case allocates nothing.
func metadataWithoutClientBodyHeaders(metadata []pairingtypes.Metadata) []pairingtypes.Metadata {
	if !slices.ContainsFunc(metadata, isClientBodyHeader) {
		return metadata
	}
	kept := make([]pairingtypes.Metadata, 0, len(metadata)-1)
	for _, entry := range metadata {
		if !isClientBodyHeader(entry) {
			kept = append(kept, entry)
		}
	}
	return kept
}

// hashCacheRequest derives the cache key for a relay. explicitExtensionDirective carries the
// normalized value of the client's lava-extension directive header (empty when absent). When
// present it is folded into the hash so an explicitly requested extension (e.g. "archive") lands
// in its own cache lane and cannot collide with a request that was only auto-promoted to the same
// resolved Extensions. Passing "" reproduces the historical hash, keeping existing entries valid.
func hashCacheRequest(relayData *pairingtypes.RelayPrivateData, chainId, explicitExtensionDirective string) ([]byte, func([]byte) []byte, error) {
	originalData := relayData.Data
	originalSalt := relayData.Salt
	originalRequestedBlock := relayData.RequestBlock
	originalSeenBlock := relayData.SeenBlock

	originalRequestId := relayData.RequestId
	originalTaskId := relayData.XTaskId
	originalTxId := relayData.XTxId
	originalMetadata := relayData.Metadata
	defer func() {
		// return all information back to the object on defer (in any case)
		relayData.Data = originalData
		relayData.Salt = originalSalt
		relayData.RequestBlock = originalRequestedBlock
		relayData.SeenBlock = originalSeenBlock

		relayData.RequestId = originalRequestId
		relayData.XTaskId = originalTaskId
		relayData.XTxId = originalTxId
		relayData.Metadata = originalMetadata
	}()

	// we need to remove some data from the request so the cache will hit properly.
	inputFormatter, outputFormatter := cacheformat.FormatterForRelayRequestAndResponse(relayData.ApiInterface)
	relayData.Data = inputFormatter(relayData.Data) // remove id from request.
	relayData.Salt = nil                            // remove salt
	relayData.SeenBlock = 0                         // remove seen block
	relayData.RequestId = ""                        // remove request id (unique per request)
	relayData.XTaskId = nil                         // remove task id (unique per request)
	relayData.XTxId = nil                           // remove tx id (unique per request)
	// A client body header (content-type, forwarded on REST bodies since MAG-2745) says how the
	// node should read the bytes in Data, which are already in the key. Two clients sending the
	// same body with and without it are the same request; a value the node cannot read is a
	// non-2xx answer that is never written. Keep it out so it cannot split the lane.
	relayData.Metadata = metadataWithoutClientBodyHeaders(relayData.Metadata)
	// we remove the discrepancy of requested block from the hash, and add it on the cache side instead
	// this is due to the fact that we don't know the latest seen block at this moment, as on shared state
	// only the cache has this information. we make sure the hashing at this stage does not include the requested block.
	// It does include it on the cache key side.
	relayData.RequestBlock = 0

	cashHash := &pairingtypes.CacheHash{
		Request: relayData,
		ChainId: chainId,
	}
	cashHashBytes, err := json.Marshal(cashHash)
	if err != nil {
		return nil, outputFormatter, utils.LavaFormatError("Failed marshalling cash hash in HashCacheRequest", err)
	}

	// Fold an explicit lava-extension directive into the key so explicitly-requested extensions
	// get a dedicated cache lane, separate from requests auto-promoted to the same Extensions.
	if explicitExtensionDirective != "" {
		cashHashBytes = append(cashHashBytes, []byte("\x00lava-extension="+explicitExtensionDirective)...)
	}

	// return the value
	return sigs.HashMsg(cashHashBytes), outputFormatter, nil
}
