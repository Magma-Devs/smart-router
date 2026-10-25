package rpcsmartrouter

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/viper"
)

// Boot-time cross-validation configuration checks (MAG-3604).
//
// Start boots every endpoint concurrently and reads their errors only once all of them have returned. A
// configuration error one endpoint found late therefore used to surface after its siblings had bound
// their listeners and started serving: the router announced itself, answered its health checks, and
// then exited — the crash loop the preflight in the command exists to prevent. The preflight can only
// judge what the config text says on its own. The two policy guards that also need the spec (a policy
// on a write method, a policy naming a method the spec does not serve) run here instead: in
// CreateSmartRouterEndpoint as soon as the spec is loaded, before a provider is dialed, and behind a
// barrier every endpoint passes only once every other endpoint has passed its own check. One refused
// configuration therefore stops the whole router before any endpoint binds a listener. Specs are not
// loaded a second time for this: every boot already fetches them once per endpoint, unauthenticated
// when they come from GitHub, and a restart loop is throttled as it is.

// errBootAbortedBeforeConfigCheck is the verdict an endpoint leaves at the barrier when it returned before
// reaching its configuration check. Every such return is a failure (there is no success return before
// the check), so the siblings must not start either.
var errBootAbortedBeforeConfigCheck = errors.New("endpoint returned before its configuration was checked")

// bootConfigBarrier holds every endpoint's boot at the point where its configuration has been checked
// against its spec until every other endpoint has reached that point, and tells each whether any of
// them failed. It cannot deadlock: Start launches one goroutine per endpoint with no concurrency limit,
// and every return path of CreateSmartRouterEndpoint arrives, a deferred arrival covering the paths
// before the check (and a panic). Outside Start the barrier is nil and an endpoint waits for nobody.
type bootConfigBarrier struct {
	pending sync.WaitGroup
	mu      sync.Mutex
	failed  []error
}

func newBootConfigBarrier(endpoints int) *bootConfigBarrier {
	b := &bootConfigBarrier{}
	b.pending.Add(endpoints)
	return b
}

// arrive counts one endpoint in with its verdict; nil means its configuration passed. It must be called
// exactly once per endpoint the barrier was sized for, which is why CreateSmartRouterEndpoint guards it
// against a second call.
func (b *bootConfigBarrier) arrive(verdict error) {
	if verdict != nil {
		b.mu.Lock()
		b.failed = append(b.failed, verdict)
		b.mu.Unlock()
	}
	b.pending.Done()
}

// wait blocks until every endpoint has arrived. It returns nil when every configuration passed, and
// otherwise the first failure recorded, so a sibling can say why it did not start.
func (b *bootConfigBarrier) wait() error {
	b.pending.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.failed) == 0 {
		return nil
	}
	return b.failed[0]
}

// crossValidationBootCheck runs the spec-only cross-validation guards for one endpoint against the spec
// it just loaded. The config is read the way ServeRPCRequests reads it, so the two cannot disagree about
// which policies exist.
func crossValidationBootCheck(endpoint *lavasession.RPCEndpoint, chainParser chainlib.ChainParser) error {
	cfg, err := ParseCrossValidationConfig(viper.GetViper())
	if err != nil {
		return err
	}
	resolver, err := NewCrossValidationPolicyResolver(cfg)
	if err != nil {
		return err
	}
	return validateCrossValidationSpecGuards(resolver, chainParser, endpoint.ChainID, endpoint.ApiInterface)
}

// validateCrossValidationSpecGuards enforces, for the policies this endpoint is held to, the two guards
// that need the spec and nothing else:
//   - The stateful-write guard: an enabled policy on a CONSISTENCY_SELECT_ALL_PROVIDERS method is a no-op
//     and is rejected.
//   - The method guard: a policy naming a method the spec does not serve under that name can never be
//     selected by a request and is rejected (MAG-3604). The check is by name alone — the connection type
//     and internal path a request also resolves by are not consulted — so it proves a policy cannot
//     apply, never that it will. Two REST templates differing only in a placeholder's name share one slot
//     in the spec's api map, so the losing twin reads as unserved; the error names the survivor.
//
// Both FAIL CLOSED: a parser that cannot answer refuses to start rather than let a policy through
// unchecked. Nothing here needs a provider, so on the boot path it runs before any is dialed.
func validateCrossValidationSpecGuards(resolver *CrossValidationPolicyResolver, chainParser chainlib.ChainParser, chainID, apiInterface string) error {
	refs := resolver.PolicyRefs(chainID, apiInterface)
	if len(refs) == 0 {
		return nil
	}
	statefulChecker, ok := chainParser.(interface{ ApiHasStatefulCategory(string) bool })
	if !ok {
		return utils.LavaFormatError("cross-validation policies are configured but the chain parser cannot classify stateful methods; cannot enforce the write-method guard", nil,
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface))
	}
	isStateful := func(c, a, method string) bool {
		if !strings.EqualFold(c, chainID) || !strings.EqualFold(a, apiInterface) {
			return false // only this endpoint's parser can classify its own chain/api
		}
		return statefulChecker.ApiHasStatefulCategory(method)
	}
	if guardErr := resolver.ValidateNoStatefulPolicies(isStateful); guardErr != nil {
		return guardErr
	}
	methodChecker, ok := chainParser.(interface{ ApiNameDefined(string) bool })
	if !ok {
		return utils.LavaFormatError("cross-validation policies are configured but the chain parser cannot list its methods; cannot check the methods the policies name", nil,
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface))
	}
	lookalikes, _ := chainParser.(interface{ ApiNamesLike(string) []string })
	// Each policy is named by its position in the list as well as its method: the log redactor mistakes a
	// gRPC method name for a url and hides its method part, and the position still points at the one policy.
	var unserved, servedAs []string
	for _, ref := range refs {
		if methodChecker.ApiNameDefined(ref.Method) {
			continue
		}
		unserved = append(unserved, ref.String())
		if lookalikes != nil {
			if like := lookalikes.ApiNamesLike(ref.Method); len(like) > 0 {
				servedAs = append(servedAs, fmt.Sprintf("policy #%d: %s", ref.Position, strings.Join(like, " or ")))
			}
		}
	}
	if len(unserved) == 0 {
		return nil
	}
	attrs := []utils.Attribute{
		utils.LogAttr("policies", unserved),
		utils.LogAttr("chainID", chainID),
		utils.LogAttr("apiInterface", apiInterface),
	}
	if len(servedAs) > 0 {
		attrs = append(attrs, utils.LogAttr("servedAs", servedAs))
	}
	attrs = append(attrs, utils.LogAttr("hint", "a policy names the method as the spec does, exactly: the JSON-RPC method, the REST path template, or the gRPC service/method; positions count from 0 in cross-validation.policies, and servedAs lists the spec names that differ from the policy's only by letter case or by a placeholder's name"))
	return utils.LavaFormatError("cross-validation policies name methods this endpoint's spec does not serve under that name, so no request would select them", nil, attrs...)
}
