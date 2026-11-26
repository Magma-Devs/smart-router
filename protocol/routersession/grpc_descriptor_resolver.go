package routersession

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errNotDescribed marks an answer that the node's descriptor source does not describe a service or
// a method: the symbol is not there, it is not a service, the service lacks the method, or the node
// does not serve reflection at all. It is what the resolver remembers (MAG-5760); any other failure
// of a lookup (a timeout, an unreachable node, a closed connection) is not.
var errNotDescribed = errors.New("not described by the node")

// descriptorResolution is a single in-flight service-descriptor lookup, shared by
// every relay that asks for the same service while it runs.
//
// svc and err are written once, before done is closed, and read only after it —
// the close is the happens-before edge, so no lock is needed on them.
type descriptorResolution struct {
	done chan struct{}
	svc  *desc.ServiceDescriptor // nil unless err == nil
	err  error
}

// GRPCDescriptorResolver resolves a node's gRPC method descriptors through its
// configured descriptor source (reflection, file, or hybrid) and caches them for the
// life of its owner. The MAG-2860 rules live here, so every owner gets them: a cold
// service is resolved once however many callers ask for it (claimServiceResolution),
// one lookup caches the whole service (cacheServiceMethods), and the lookup runs on
// grpc-config's reflection-timeout rooted at the owner's lifetime rather than at the
// caller's deadline (lookupService). The stream path used to carry a copy of the
// pre-MAG-2860 shape: one reflection client per call, one method cached, the caller's
// context as the only bound, so N cold subscriptions opened N reflection streams
// against a slow node with no deadline of their own (MAG-4278 #18).
//
// There are TWO instances per endpoint, not one, and they do not share a cache: the
// unary GRPCDirectRPCConnection owns one, and the subscription stream pool owns the
// other for all of its channels (MAG-4397). They are separate because their borrow
// sources are — a resolver can only run reflection on a channel its owner holds.
//
// borrow hands out a client to run reflection on and the way to give it back: the
// unary connection borrows from its pooled connector, the stream pool lends any
// healthy channel of its endpoint.
type GRPCDescriptorResolver struct {
	grpcConfig *common.GrpcConfig
	url        string
	lifetime   context.Context
	borrow     func(ctx context.Context) (conn *grpc.ClientConn, release func(), err error)
	cache      *common.SafeSyncMap[string, *desc.MethodDescriptor]

	// inflight holds the one running descriptor lookup per service, so N concurrent
	// callers that miss on the same cold service wait on one lookup instead of each
	// opening their own. resolveMu guards it.
	resolveMu sync.Mutex
	inflight  map[string]*descriptorResolution

	// What the node does not describe, remembered for one reflection-timeout (MAG-5760). The
	// in-flight dedup above shares a lookup's answer with everyone who asks while it runs; this
	// shares a "not described" answer for one more lookup budget after it, so repeated requests
	// for a method the node lacks cost it one lookup per reflection-timeout, not one per request.
	// It is never longer: a service the node starts serving is found within one reflection-timeout.
	// memoryMu guards every field below.
	memoryMu sync.Mutex
	// undescribed is when the node last said it does not describe a service.
	undescribed map[string]time.Time
	// described is when a service's descriptor was last read. A method it lacks is not described:
	// cacheServiceMethods stores every method of the service at that moment.
	described map[string]time.Time
	// listed is the node's own service list in "reflection" mode, read at listedAt (listKnown is
	// false when the read failed). A service the node does not list is not described, so names it
	// has never heard of cost no lookup of their own: at most one list read per reflection-timeout.
	// listing is closed when the running list read ends, and nil when none runs.
	listed    map[string]struct{}
	listedAt  time.Time
	listKnown bool
	listing   chan struct{}
	// prunedAt is when expired undescribed entries were last dropped.
	prunedAt time.Time
}

// NewGRPCDescriptorResolver builds a resolver over cache, which the owner keeps a
// handle on (the warm-up sweep fills it; tests seed it). lifetime bounds every
// lookup and must be the connection's own context, never a caller's (MAG-2808).
func NewGRPCDescriptorResolver(
	lifetime context.Context,
	grpcConfig *common.GrpcConfig,
	url string,
	cache *common.SafeSyncMap[string, *desc.MethodDescriptor],
	borrow func(ctx context.Context) (*grpc.ClientConn, func(), error),
) *GRPCDescriptorResolver {
	return &GRPCDescriptorResolver{
		grpcConfig:  grpcConfig,
		url:         url,
		lifetime:    lifetime,
		borrow:      borrow,
		cache:       cache,
		inflight:    map[string]*descriptorResolution{},
		undescribed: map[string]time.Time{},
		described:   map[string]time.Time{},
	}
}

// MethodDescriptor retrieves the method descriptor from cache, or waits on a
// lookup against the node's configured descriptor source (reflection, file, or
// hybrid).
//
// The WAIT is bounded by the caller's ctx. The LOOKUP is not (MAG-2860): it runs
// on its own budget and caches what it finds even if this caller has already given
// up, so the cost of a cold cache is paid once for the endpoint rather than once
// per attempt. See resolveService.
func (r *GRPCDescriptorResolver) MethodDescriptor(
	ctx context.Context,
	service, methodName string,
) (*desc.MethodDescriptor, error) {
	fullMethodName := service + "." + methodName

	// Check cache first
	if methodDesc, found, _ := r.cache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	// What the node said it does not describe is answered without asking it again. The caller's
	// own context still wins over that answer, for the reason given below: a relay the router has
	// ended must keep its cancellation, and the send path adds ErrNoMethodDescriptor only to a
	// live relay's error, so without the sentinel the answer would be scored as the endpoint's.
	if err := r.rememberedNotDescribed(service, methodName); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("gRPC descriptor not looked up, the relay had ended: %w", ctxErr)
		}
		return nil, err
	}

	resolution := r.resolveService(service)

	var finished bool
	select {
	case <-resolution.done:
		finished = true
	case <-ctx.Done():
		// Both cases can be ready at once, and select picks among ready cases at
		// RANDOM — so landing here proves nothing about the lookup. Ask again before
		// deciding anything, or roughly half of the races report whichever outcome
		// the runtime happened to choose.
		select {
		case <-resolution.done:
			finished = true
		default:
		}
	}

	// A lookup that SUCCEEDED is used whatever the caller's context now says. The
	// descriptor is in hand and costs nothing to return; discarding it would fail a
	// relay we can still serve, and the cancellation will be noticed by the caller
	// on its own terms anyway.
	if finished && resolution.err == nil {
		methodDescriptor := resolution.svc.FindMethodByName(methodName)
		if methodDescriptor == nil {
			return nil, utils.FormatError("method not found in service", errNotDescribed,
				utils.LogAttr("service", service),
				utils.LogAttr("method", methodName))
		}
		return methodDescriptor, nil
	}

	// Some other resolution — the warm-up sweep, or another caller's lookup — may
	// have cached this method while we waited. A descriptor is a descriptor.
	if methodDesc, found, _ := r.cache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	// From here the lookup has given us nothing, so the question is only which
	// error to report. The caller's own context wins over the lookup's error
	// whenever both are available.
	//
	// This is not cosmetic. common.IsClientCancellation matches on the
	// context.Canceled SENTINEL, and a relay the router itself abandoned — a loser
	// of a stateful fan-out race, or a client that hung up — must not be scored
	// against the endpoint. Reporting the lookup's generic failure instead drops
	// that sentinel and hands endpoint health a node error that never happened.
	// The risk is asymmetric in the same way handleGRPCError's cancellation branch
	// is: over-reporting cancellation only skips scoring a relay whose result we
	// were discarding, while under-reporting it penalises an endpoint that did
	// nothing wrong.
	//
	// Passing ctx.Err() through rather than flattening it into a message is what
	// keeps the sentinel reachable by errors.Is.
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The lookup itself is NOT abandoned here — it runs on its own budget and
		// caches what it finds, so the next caller on this endpoint is not cold. That
		// is what stops one slow endpoint being permanently unusable (MAG-2860).
		//
		// The log line names the node; the error does not, since a relay's error can
		// reach its caller.
		utils.FormatWarning("gRPC descriptor lookup outlived the relay, continuing in background", ctxErr,
			utils.LogAttr("service", service),
			utils.LogAttr("method", methodName),
			utils.LogAttr("url", r.url))
		return nil, fmt.Errorf("gRPC descriptor lookup outlived the relay, continuing in background: %w", ctxErr)
	}

	// Unreachable unless the lookup finished: the select above only returns on one
	// of the two channels, and a live ctx means it was resolution.done.
	return nil, resolution.err
}

// resolveService starts — or joins — the one running lookup of `service` on this
// endpoint, and returns the handle to wait on.
//
// Deduplicating matters here for the same reason detaching does: a cold service is
// cold for every caller at once, and letting each of them open its own reflection
// stream turns one slow lookup into N concurrent ones against a node that is
// already too slow to answer inside a relay timeout.
func (r *GRPCDescriptorResolver) resolveService(service string) *descriptorResolution {
	resolution, claimed := r.claimServiceResolution(service)
	if claimed {
		go r.runServiceResolution(service, resolution)
	}
	return resolution
}

// claimServiceResolution registers the caller as the one resolving `service`, or
// hands back the resolution already running for it.
//
// The claim is what keeps the warm-up sweep and the on-demand path from resolving
// the same service twice: whichever reaches a service first owns it, and the
// other either waits on it (a relay) or skips it (the sweep, which has other
// services to get on with).
func (r *GRPCDescriptorResolver) claimServiceResolution(service string) (resolution *descriptorResolution, claimed bool) {
	r.resolveMu.Lock()
	defer r.resolveMu.Unlock()

	if existing, ok := r.inflight[service]; ok {
		return existing, false
	}

	resolution = &descriptorResolution{done: make(chan struct{})}
	r.inflight[service] = resolution
	return resolution, true
}

// publishServiceResolution ends a claimed resolution, waking every waiter.
//
// It must be reached on every path out of a claimed lookup, including failures:
// a claim that is never published is a resolution every future waiter blocks on
// until its own context expires.
func (r *GRPCDescriptorResolver) publishServiceResolution(
	service string,
	resolution *descriptorResolution,
	serviceDescriptor *desc.ServiceDescriptor,
	err error,
) {
	resolution.svc, resolution.err = serviceDescriptor, err
	if definitelyNotDescribed(err) {
		r.rememberNotDescribed(service)
	}

	// Drop the entry BEFORE publishing, so a caller that arrives after a failed
	// lookup starts a fresh one instead of joining a finished one and being handed
	// its stale error. Waiters already holding this resolution still get the real
	// answer. The cost is a short window in which a second lookup can start while
	// this one is finishing; the cache check in MethodDescriptor makes that a
	// no-op as soon as one of them succeeds.
	r.resolveMu.Lock()
	if r.inflight[service] == resolution {
		delete(r.inflight, service)
	}
	r.resolveMu.Unlock()

	close(resolution.done)
}

// runServiceResolution performs one claimed service lookup and publishes it.
func (r *GRPCDescriptorResolver) runServiceResolution(service string, resolution *descriptorResolution) {
	started := time.Now()

	serviceDescriptor, err := r.lookupService(service)
	if err == nil {
		// Cache before publishing: a waiter woken by the close below re-checks the
		// cache, and finding it already filled is one less way to race.
		r.cacheServiceMethods(serviceDescriptor)

		utils.FormatDebug("gRPC service descriptor resolved",
			utils.LogAttr("service", service),
			utils.LogAttr("methods", len(serviceDescriptor.GetMethods())),
			utils.LogAttr("duration", time.Since(started)),
			utils.LogAttr("url", r.url))
	}

	r.publishServiceResolution(service, resolution, serviceDescriptor, err)
}

// lookupService resolves one service descriptor on a budget of its own.
//
// The context is rooted at the connection's lifetime, never at a relay's. A relay's
// deadline is how long the CALLER waits, not how long the lookup may take; bounding
// the lookup by it meant a node whose reflection is slower than one relay could
// never finish one, and so could never cache one. Rooting it at the connection's
// lifetime keeps Close prompt all the same — the cancellation in Close aborts this
// and releases the borrowed client below before the drain waits on it.
//
// The budget itself is grpc-config's reflection-timeout (default 5s, validated to
// 100ms..30s). That knob was defined and validated but never applied to anything;
// this is the call it was written for.
//
// The bounds are checked here, before the context is derived, because not every
// owner checks them first: the unary connection refuses an out-of-bounds value in
// initialize, but the subscription stream pool has no such step. Without the check
// a unitless "reflection-timeout: 5" (5ns) times out every reflection lookup on the
// stream path, where the unary path refuses the value and names the config.
func (r *GRPCDescriptorResolver) lookupService(service string) (*desc.ServiceDescriptor, error) {
	if err := r.grpcConfig.ValidateReflectionTimeout(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(r.lifetime, r.grpcConfig.GetReflectionTimeout())
	defer cancel()

	// Borrow a client of our own rather than reusing the one the triggering relay
	// holds: that relay hands its client back the moment it gives up on us, and a
	// handed-back client can be closed outright (ReturnRpc closes it when the pool
	// is over-full or being torn down). Borrowing keeps the owner's accounting
	// true for exactly as long as we use it.
	conn, release, err := r.borrow(ctx)
	if err != nil {
		if errors.Is(err, ErrGRPCConnectionClosed) {
			return nil, err
		}
		// The log line names the node; the error does not, since a relay's error can reach
		// its caller.
		utils.FormatError("gRPC get connection failed for descriptor lookup", err,
			utils.LogAttr("service", service),
			utils.LogAttr("url", r.url))
		return nil, fmt.Errorf("gRPC get connection failed for descriptor lookup of %s: %w", service, err)
	}
	defer release()

	// Resolve through the node's configured descriptor source. In "reflection" mode
	// (the default) this is exactly the reflection client below; in "file" mode the
	// client is built but never queried.
	cl := grpcreflect.NewClientAuto(ctx, conn)
	defer cl.Reset()

	descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(r.grpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
	if err != nil {
		return nil, err
	}

	// In "reflection" mode the node's own service list says whether it describes the service at
	// all. It is read on this same reflection stream, so a cold lookup still opens one stream, and
	// reused for one reflection-timeout. Hybrid mode does not use it: its list can come back from
	// the protoset alone when the node's reflection fails, which proves nothing about the node.
	if r.grpcConfig.GetDescriptorSource() == common.GrpcDescriptorSourceReflection {
		if listed, known := r.nodeListsService(ctx, cl, service); known && !listed {
			return nil, fmt.Errorf("%w: service %s is not in the node's service list", errNotDescribed, service)
		}
	}

	serviceDescriptor, err := resolveServiceFrom(descriptorSource, service)
	if err != nil {
		return nil, utils.FormatError("failed to find service descriptor", err,
			utils.LogAttr("service", service),
			utils.LogAttr("descriptor-source", r.grpcConfig.GetDescriptorSource()),
			utils.LogAttr("reflection-timeout", r.grpcConfig.GetReflectionTimeout()))
	}

	return serviceDescriptor, nil
}

// cacheServiceMethods stores every method of a resolved service under
// service.method, so one lookup serves every method of the service.
func (r *GRPCDescriptorResolver) cacheServiceMethods(serviceDescriptor *desc.ServiceDescriptor) {
	service := serviceDescriptor.GetFullyQualifiedName()
	for _, method := range serviceDescriptor.GetMethods() {
		r.cache.Store(service+"."+method.GetName(), method)
	}

	// After the methods, so a reader that sees the service as read finds every method it has.
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	r.described[service] = time.Now()
	delete(r.undescribed, service)
}

// memoryFor is how long the resolver remembers what the node does not describe: one
// reflection-timeout, the budget of the lookup that found it out (MAG-5760).
func (r *GRPCDescriptorResolver) memoryFor() time.Duration {
	return r.grpcConfig.GetReflectionTimeout()
}

// rememberedNotDescribed answers, from memory, a method the node does not describe: a service it
// said it does not describe, a method missing from a service read within the memory, or a service
// missing from its service list read within the memory. nil when memory has no answer, and the
// caller asks the node.
func (r *GRPCDescriptorResolver) rememberedNotDescribed(service, methodName string) error {
	now, memory := time.Now(), r.memoryFor()
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()

	if at, found := r.undescribed[service]; found && now.Sub(at) < memory {
		return fmt.Errorf("%w: service %s", errNotDescribed, service)
	}
	if at, found := r.described[service]; found && now.Sub(at) < memory {
		return fmt.Errorf("%w: method %s of service %s", errNotDescribed, methodName, service)
	}
	if r.grpcConfig.GetDescriptorSource() == common.GrpcDescriptorSourceReflection && r.listKnown && now.Sub(r.listedAt) < memory {
		if _, listed := r.listed[service]; !listed {
			return fmt.Errorf("%w: service %s is not in the node's service list", errNotDescribed, service)
		}
	}
	return nil
}

// rememberNotDescribed records that the node does not describe service. Entries that have lapsed
// are dropped at most once per memory span, so the map holds the names asked for within about two
// spans and no more.
func (r *GRPCDescriptorResolver) rememberNotDescribed(service string) {
	now, memory := time.Now(), r.memoryFor()
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()

	if now.Sub(r.prunedAt) >= memory {
		for name, at := range r.undescribed {
			if now.Sub(at) >= memory {
				delete(r.undescribed, name)
			}
		}
		r.prunedAt = now
	}
	r.undescribed[service] = now
}

// noteServiceList records the node's service list, read by a lookup or by the warm-up sweep. A
// listed service is no longer remembered as undescribed.
func (r *GRPCDescriptorResolver) noteServiceList(services []string) {
	listed := make(map[string]struct{}, len(services))
	for _, service := range services {
		listed[service] = struct{}{}
	}

	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	r.listed, r.listedAt, r.listKnown = listed, time.Now(), true
	for service := range listed {
		delete(r.undescribed, service)
	}
}

// nodeListsService reports whether the node's service list holds service, reading the list on cl
// when the one held is older than the memory. One read runs at a time; a lookup that arrives while
// it runs waits for it rather than opening its own. known is false when the list could not be read,
// and a failed read is not retried within the memory either: the lookup then asks for the symbol
// itself, as it did before the list was used.
func (r *GRPCDescriptorResolver) nodeListsService(ctx context.Context, cl *grpcreflect.Client, service string) (listed, known bool) {
	for {
		memory := r.memoryFor()
		r.memoryMu.Lock()
		if !r.listedAt.IsZero() && time.Since(r.listedAt) < memory {
			_, listed = r.listed[service]
			known = r.listKnown
			r.memoryMu.Unlock()
			return listed, known
		}
		if running := r.listing; running != nil {
			r.memoryMu.Unlock()
			select {
			case <-running:
				continue
			case <-ctx.Done():
				return false, false
			}
		}
		running := make(chan struct{})
		r.listing = running
		r.memoryMu.Unlock()

		services, err := cl.ListServices()

		if err == nil {
			r.noteServiceList(services)
		}
		r.memoryMu.Lock()
		if err != nil {
			r.listed, r.listedAt, r.listKnown = nil, time.Now(), false
		}
		r.listing = nil
		r.memoryMu.Unlock()
		close(running)

		if err != nil {
			utils.FormatDebug("gRPC service list not read, looking the symbol up instead",
				utils.LogAttr("error", err.Error()),
				utils.LogAttr("url", r.url))
			return false, false
		}
	}
}

// definitelyNotDescribed reports whether a lookup's error is the node's descriptor source saying it
// does not describe the symbol: errNotDescribed, reflection's "not found" (a NotFound status, or
// grpcreflect's own not-found error), or a node that does not serve reflection (an Unimplemented
// status: a lookup makes no call but reflection's). A timeout, an unreachable node, a closed
// connection or a borrow that failed is none of these.
func definitelyNotDescribed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errNotDescribed) || errors.Is(err, grpcurl.ErrReflectionNotSupported) {
		return true
	}
	pending := []error{err}
	for len(pending) > 0 {
		e := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if grpcreflect.IsElementNotFoundError(e) {
			return true
		}
		if withStatus, ok := e.(interface{ GRPCStatus() *status.Status }); ok {
			if code := withStatus.GRPCStatus().Code(); code == codes.NotFound || code == codes.Unimplemented {
				return true
			}
		}
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			pending = append(pending, wrapped.Unwrap()...)
		case interface{ Unwrap() error }:
			if next := wrapped.Unwrap(); next != nil {
				pending = append(pending, next)
			}
		}
	}
	return false
}
