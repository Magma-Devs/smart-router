package lavasession

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type EndpointInfo struct {
	Latency  time.Duration
	Endpoint *Endpoint
}

// Slice to hold EndpointInfo
type EndpointInfoList []EndpointInfo

// Implement sort.Interface for EndpointInfoList
func (list EndpointInfoList) Len() int {
	return len(list)
}

func (list EndpointInfoList) Less(i, j int) bool {
	return list[i].Latency < list[j].Latency
}

func (list EndpointInfoList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

// SessionConnection is the base interface for RPC connections with QoS management.
type SessionConnection interface {
	GetQoSManager() *qos.QoSManager
	GetEndpointAddress() string
}

// ProviderRelayConnection wraps a provider-relay connection with QoS management.
type ProviderRelayConnection struct {
	EndpointConnection *EndpointConnection
	QoSManager         *qos.QoSManager
	EndpointAddress    string // Network address of the endpoint
}

func (prc *ProviderRelayConnection) GetQoSManager() *qos.QoSManager {
	return prc.QoSManager
}

func (prc *ProviderRelayConnection) GetEndpointAddress() string {
	return prc.EndpointAddress
}

// DirectRPCSessionConnection wraps a direct RPC connection with QoS management.
type DirectRPCSessionConnection struct {
	DirectConnection DirectRPCConnection
	QoSManager       *qos.QoSManager
	EndpointAddress  string
	Endpoint         *Endpoint // Direct reference to endpoint for per-endpoint tracking
}

func (drsc *DirectRPCSessionConnection) GetQoSManager() *qos.QoSManager {
	return drsc.QoSManager
}

func (drsc *DirectRPCSessionConnection) GetEndpointAddress() string {
	return drsc.EndpointAddress
}

const (
	AllowInsecureConnectionToProvidersFlag     = "allow-insecure-provider-dialing"
	AllowGRPCCompressionFlag                   = "enable-application-level-compression"
	MaximumStreamsOverASingleConnectionFlag    = "maximum-streams-per-connection"
	DefaultMaximumStreamsOverASingleConnection = 100
	WeightMultiplierForStaticProviders         = 10
)

var (
	AllowInsecureConnectionToProviders                   = false
	AllowGRPCCompressionForConsumerProviderCommunication = false
	MaximumStreamsOverASingleConnection                  = uint64(DefaultMaximumStreamsOverASingleConnection)
)

type UsedProvidersInf interface {
	RemoveUsed(providerAddress string, routerKey RouterKey, err error)
	TryLockSelection(context.Context) error
	AddUsed(ConsumerSessionsMap, error)
	GetUnwantedProvidersToSend(RouterKey) map[string]struct{}
	AddUnwantedAddresses(address string, routerKey RouterKey)
	CurrentlyUsed() int
}

type SessionInfo struct {
	Session           *SingleConsumerSession
	StakeSize         int64
	Epoch             uint64
	ReportedProviders []*pairingtypes.ReportedProvider
}

type ConsumerSessionsMap map[string]*SessionInfo

type ProviderOptimizer interface {
	AppendProbeRelayData(providerAddress string, latency time.Duration, success bool)
	AppendRelayFailure(providerAddress string)
	AppendRelayData(providerAddress string, latency time.Duration, cu, syncBlock uint64)
	AppendRelayDataConsensus(providerAddress string, latency time.Duration, cu, syncBlock uint64, syncRef provideroptimizer.SyncReference)
	ChooseProvider(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string)
	ChooseProviderWithStats(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string, stats *provideroptimizer.SelectionStats)
	ChooseBestProvider(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string)
	ChooseBestProviderWithStats(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string, stats *provideroptimizer.SelectionStats)
	GetReputationReportForProvider(string) (*pairingtypes.QualityOfServiceReport, time.Time)
	Strategy() provideroptimizer.Strategy
	UpdateWeights(map[string]int64, uint64)
}

type ignoredProviders struct {
	providers    map[string]struct{}
	currentEpoch uint64
}

type EndpointConnection struct {
	Client                              pairingtypes.RelayerClient
	connection                          *grpc.ClientConn
	numberOfSessionsUsingThisConnection uint64
	// blockListed - currently unused, use it carefully as it will block this provider's endpoint until next epoch without forgiveness.
	// Can be used in cases of data reliability, self provider conflict etc..
	blockListed atomic.Bool
	lbUniqueId  string
	// In case we got disconnected, we cant reconnect as we might lose stickiness
	// with the provider, if its using a load balancer
	disconnected bool
}

func (ec *EndpointConnection) GetLbUniqueId() string {
	return ec.lbUniqueId
}

func (ec *EndpointConnection) addSessionUsingConnection() {
	atomic.AddUint64(&ec.numberOfSessionsUsingThisConnection, 1)
}

func (ec *EndpointConnection) decreaseSessionUsingConnection() {
	for {
		knownValue := ec.getNumberOfLiveSessionsUsingThisConnection()
		if knownValue >= 1 {
			swapped := atomic.CompareAndSwapUint64(&ec.numberOfSessionsUsingThisConnection, knownValue, knownValue-1)
			if swapped {
				return
			}
		} else {
			utils.LavaFormatError("decreaseSessionUsingConnection, Value below 1 is stored in numberOfSessionsUsingThisConnection. it must always be above 1", nil)
			return
		}
	}
}

func (ec *EndpointConnection) getNumberOfLiveSessionsUsingThisConnection() uint64 {
	return atomic.LoadUint64(&ec.numberOfSessionsUsingThisConnection)
}

type EndpointAndChosenConnection struct {
	endpoint                 *Endpoint
	chosenEndpointConnection *EndpointConnection
}

type Endpoint struct {
	NetworkAddress string // change at the end to NetworkAddress
	Enabled        bool

	Connections       []*EndpointConnection // Provider-relay connections (legacy)
	DirectConnections []DirectRPCConnection // Direct RPC connections

	ConnectionRefusals uint64
	// consecutiveHealthyProbes counts DISTINCT post-disable successful polls confirmed while the
	// endpoint is DISABLED — the hysteresis used by the probe's proactive re-enable (Topic E / F1). It
	// is reset on any non-recovery verdict (failed/stale/pre-disable poll) and on re-enable, and is
	// held at 0 while the endpoint is Enabled (the probe never touches an enabled endpoint's state, so
	// it cannot undo the relay path's climb to the disable threshold).
	consecutiveHealthyProbes uint64
	// disabledAt is the instant the endpoint last transitioned Enabled→false (edge-triggered in
	// MarkUnhealthy, NOT re-stamped on repeated calls). Recovery requires a successful poll strictly
	// after this instant, so a poll that succeeded BEFORE the disable can never re-enable (F1).
	// Cleared (zeroed) on every re-enable.
	disabledAt time.Time
	// lastRecoveryPoll is the LastSuccessfulPoll value already counted toward the hysteresis streak,
	// so a probe cadence faster than the poll cadence cannot count one successful poll twice — only a
	// strictly newer successful poll advances the streak (F1: "distinct post-disable polls").
	lastRecoveryPoll time.Time
	Addons           map[string]struct{}
	Extensions       map[string]struct{}
	mu               sync.RWMutex // Protects Connections, ConnectionRefusals, Enabled, consecutiveHealthyProbes, disabledAt, lastRecoveryPoll

	// Per-endpoint observed tip lives in the shared endpointtip store (single source of
	// truth), keyed by chain+apiInterface+NetworkAddress — not on the Endpoint — so the
	// poll and relay-harvest writers and the QoS reader all touch one gated store instead
	// of this struct holding a second, ungated copy.
}

// IsDirectRPC returns true if this endpoint uses direct RPC connections (smart router mode)
func (e *Endpoint) IsDirectRPC() bool {
	return len(e.DirectConnections) > 0
}

// IsEnabled returns the endpoint's enable bit under the endpoint mutex. Enabled is written under
// e.mu by MarkUnhealthy / ResetHealth / RecordProbeVerdict; every production READ must go through a
// synchronized accessor like this to avoid the data race the race detector flagged (F3).
func (e *Endpoint) IsEnabled() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Enabled
}

// EndpointHealthSnapshot is a consistent, point-in-time view of an endpoint's enable/recovery state
// for read-only debug introspection (MAG-2202 /debug/endpoint-state). Captured under e.mu so a caller
// never observes a half-applied MarkUnhealthy / RecordProbeVerdict transition. DisabledAt and
// LastRecoveryPoll are zero while the endpoint is enabled / has never disabled.
type EndpointHealthSnapshot struct {
	Enabled                  bool
	DisabledAt               time.Time
	ConsecutiveHealthyProbes uint64 // distinct post-disable successful polls counted toward F1 re-enable
	LastRecoveryPoll         time.Time
}

// HealthSnapshot returns the endpoint's enable/recovery state under e.mu. Read-only companion to
// IsEnabled for debug introspection — it never mutates state. The unexported recovery fields
// (disabledAt / consecutiveHealthyProbes / lastRecoveryPoll) are otherwise invisible outside the
// package, so this is the only synchronized read path for them.
func (e *Endpoint) HealthSnapshot() EndpointHealthSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return EndpointHealthSnapshot{
		Enabled:                  e.Enabled,
		DisabledAt:               e.disabledAt,
		ConsecutiveHealthyProbes: e.consecutiveHealthyProbes,
		LastRecoveryPoll:         e.lastRecoveryPoll,
	}
}

// IsProviderRelay returns true if this endpoint uses provider-relay connections (consumer mode)
func (e *Endpoint) IsProviderRelay() bool {
	return len(e.Connections) > 0
}

func (e *Endpoint) CheckSupportForServices(addon string, extensions []string) (supported bool) {
	if addon != "" {
		if _, ok := e.Addons[addon]; !ok {
			return false
		}
	}
	for _, extension := range extensions {
		if extension == "" {
			continue
		}
		if _, ok := e.Extensions[extension]; !ok {
			return false
		}
	}
	return true
}

// MarkUnhealthy increments connection refusals and disables endpoint if threshold exceeded.
func (e *Endpoint) MarkUnhealthy() {
	e.markUnhealthyAt(time.Now())
}

// markUnhealthyAt is MarkUnhealthy with an injectable clock (tests drive the disable instant so they
// can order it relative to a poll's LastSuccessfulPoll). disabledAt is stamped ONLY on the actual
// Enabled→false transition (edge-triggered): a repeated MarkUnhealthy on an already-disabled endpoint
// must NOT push disabledAt forward, or it would silently invalidate post-disable poll evidence the
// prober has already accumulated (F1).
func (e *Endpoint) markUnhealthyAt(at time.Time) {
	e.mu.Lock()
	e.ConnectionRefusals++
	wasEnabled := e.Enabled
	disabled := e.ConnectionRefusals >= MaxConsecutiveConnectionAttempts
	transitioned := false
	if disabled && wasEnabled {
		e.Enabled = false
		e.disabledAt = at
		e.consecutiveHealthyProbes = 0
		e.lastRecoveryPoll = time.Time{}
		transitioned = true
	}
	addr, refusals, isDirect := e.NetworkAddress, e.ConnectionRefusals, e.IsDirectRPC()
	e.mu.Unlock()

	if transitioned {
		utils.LavaFormatWarning("disabled unhealthy endpoint", nil,
			utils.LogAttr("endpoint", addr),
			utils.LogAttr("refusals", refusals),
			utils.LogAttr("is_direct_rpc", isDirect),
		)
	}
}

// RecordProbeVerdict applies one probe-cycle health verdict to the endpoint's enable state — the
// PROACTIVE re-enable half of the Topic E contract (the O1 win). It is the only probe-driven
// transition; the relay path stays the fast disabler (MarkUnhealthy at MaxConsecutiveConnectionAttempts).
//
// Ownership / anti-flap rules:
//   - The probe NEVER touches an enabled endpoint. While Enabled, an endpoint's health is the relay
//     path's domain; if the probe reset ConnectionRefusals here it would undo a mid-climb toward the
//     disable threshold and the endpoint could never disable under partial failure. So we hold the
//     hysteresis counter at 0 and return without effect.
//   - On a DISABLED endpoint, the probe re-enables only after reEnableAfterK consecutive healthy
//     verdicts (hysteresis). This threshold is deliberately distinct from the relay disable threshold
//     so the two actors don't oscillate. A single unhealthy verdict resets the streak.
//   - Re-enabling gives the endpoint a clean slate (ConnectionRefusals = 0): it was already disabled
//     (refusals >= threshold), and it earned the reset by proving K healthy cycles.
//
// recoveryPoll is the endpoint's LastSuccessfulPoll and recoveryHealthy asserts the last poll
// succeeded AND the endpoint is keeping up (probing.RecoveryEvidence, passed as primitives to avoid a
// probing→lavasession import cycle). Re-enable requires a SUCCESSFUL POLL produced strictly AFTER the
// disable instant (recoveryPoll.After(disabledAt)) — a pre-disable observation, however fresh, never
// re-enables. Hysteresis counts DISTINCT such polls (a probe cadence faster than the poll cadence
// cannot count one poll twice), and ANY non-recovery verdict resets the streak.
//
// reEnableAfterK < 1 is treated as 1. Returns true only on the cycle it actually re-enables.
func (e *Endpoint) RecordProbeVerdict(recoveryPoll time.Time, recoveryHealthy bool, reEnableAfterK uint64) (reenabled bool) {
	if reEnableAfterK < 1 {
		reEnableAfterK = 1
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.Enabled {
		// Enabled endpoints are the relay path's domain — keep the probe out of the refusal climb.
		e.consecutiveHealthyProbes = 0
		e.lastRecoveryPoll = time.Time{}
		return false
	}

	// Valid recovery evidence = the last poll succeeded and is keeping up, AND that successful poll
	// landed after the disable. Anything else (failed/stale/pre-disable poll) resets the streak.
	validEvidence := recoveryHealthy && !recoveryPoll.IsZero() && recoveryPoll.After(e.disabledAt)
	if !validEvidence {
		e.consecutiveHealthyProbes = 0
		return false
	}
	// Only a STRICTLY NEWER successful poll than the one already counted advances the streak; seeing
	// the same poll again across faster probe cycles holds (neither advances nor resets).
	if !recoveryPoll.After(e.lastRecoveryPoll) {
		return false
	}
	e.lastRecoveryPoll = recoveryPoll
	e.consecutiveHealthyProbes++
	if e.consecutiveHealthyProbes < reEnableAfterK {
		return false
	}

	e.Enabled = true
	e.ConnectionRefusals = 0
	e.consecutiveHealthyProbes = 0
	e.disabledAt = time.Time{}
	e.lastRecoveryPoll = time.Time{}
	utils.LavaFormatInfo("probe re-enabled recovered endpoint",
		utils.LogAttr("endpoint", e.NetworkAddress),
		utils.LogAttr("is_direct_rpc", e.IsDirectRPC()),
		utils.LogAttr("re_enable_after_k", reEnableAfterK),
	)
	return true
}

// ResetHealth resets connection refusals and re-enables endpoint.
// Returns true if the endpoint was actually unhealthy and got reset.
// No-ops silently when the endpoint is already healthy to avoid log spam.
func (e *Endpoint) ResetHealth() bool {
	e.mu.Lock()
	if e.ConnectionRefusals == 0 && e.Enabled {
		e.mu.Unlock()
		return false
	}
	e.ConnectionRefusals = 0
	e.Enabled = true
	e.consecutiveHealthyProbes = 0
	e.disabledAt = time.Time{}
	e.lastRecoveryPoll = time.Time{}
	addr, isDirect := e.NetworkAddress, e.IsDirectRPC()
	e.mu.Unlock()

	utils.LavaFormatInfo("re-enabled healthy endpoint",
		utils.LogAttr("endpoint", addr),
		utils.LogAttr("is_direct_rpc", isDirect),
	)
	return true
}

type SessionWithProvider struct {
	SessionsWithProvider *ConsumerSessionsWithProvider
	CurrentEpoch         uint64
	retryConnecting      bool
}

type SessionWithProviderMap map[string]*SessionWithProvider // key is the provider address

type RPCEndpoint struct {
	NetworkAddress  string `yaml:"network-address,omitempty" json:"network-address,omitempty" mapstructure:"network-address"` // HOST:PORT
	ChainID         string `yaml:"chain-id,omitempty" json:"chain-id,omitempty" mapstructure:"chain-id"`                      // spec chain identifier
	ApiInterface    string `yaml:"api-interface,omitempty" json:"api-interface,omitempty" mapstructure:"api-interface"`
	TLSEnabled      bool   `yaml:"tls-enabled,omitempty" json:"tls-enabled,omitempty" mapstructure:"tls-enabled"`
	HealthCheckPath string `yaml:"health-check-path,omitempty" json:"health-check-path,omitempty" mapstructure:"health-check-path"` // health check status code 200 path, default is "/"
}

func (endpoint *RPCEndpoint) String() (retStr string) {
	retStr = endpoint.ChainID + ":" + endpoint.ApiInterface + " Network Address:" + endpoint.NetworkAddress
	return retStr
}

func (rpce *RPCEndpoint) Key() string {
	return rpce.ChainID + rpce.ApiInterface
}

type ConsumerSessionsWithProvider struct {
	Lock              sync.RWMutex
	PublicLavaAddress string
	Endpoints         []*Endpoint
	Sessions          map[int64]*SingleConsumerSession
	MaxComputeUnits   uint64
	UsedComputeUnits  uint64
	PairingEpoch      uint64
	// whether we already reported this provider this epoch, we can only report one conflict per provider per epoch
	conflictFoundAndReported uint32 // 0 == not reported, 1 == reported
	stakeSize                int64  // the stake size the provider staked (ulava)

	// blocked provider recovery status if 0 currently not used, if 1 a session has tried resume communication with this provider
	// if the provider is not blocked at all this field is irrelevant
	blockedAndUsedWithChanceForRecoveryStatus uint32
	// onSecondChanceProbation is 1 once this provider has consumed its single
	// second chance (see ConsumerSessionManager.secondChanceGivenToAddresses). A
	// successful relay clears it so a future isolated failure is treated as a
	// first offense again instead of an immediate hard block. 0 == not on probation.
	onSecondChanceProbation uint32
	StaticProvider          bool
	// GroupLabel is the provider's configured cross-validation group (from RPCStaticProviderEndpoint.GroupLabel).
	// It rides on the provider record so it can be read off a session's Parent without an address-keyed lookup.
	// Empty string means the implicit common.DefaultProviderGroup.
	GroupLabel string
}

func NewConsumerSessionWithProvider(publicLavaAddress string, pairingEndpoints []*Endpoint, maxCu uint64, epoch uint64, stakeSize int64) *ConsumerSessionsWithProvider {
	return &ConsumerSessionsWithProvider{
		PublicLavaAddress: publicLavaAddress,
		Endpoints:         pairingEndpoints,
		Sessions:          map[int64]*SingleConsumerSession{},
		MaxComputeUnits:   maxCu,
		PairingEpoch:      epoch,
		stakeSize:         stakeSize,
	}
}

func (cswp *ConsumerSessionsWithProvider) atomicReadBlockedStatus() uint32 {
	return atomic.LoadUint32(&cswp.blockedAndUsedWithChanceForRecoveryStatus)
}

func (cswp *ConsumerSessionsWithProvider) atomicWriteBlockedStatus(status uint32) {
	atomic.StoreUint32(&cswp.blockedAndUsedWithChanceForRecoveryStatus, status) // we can only set conflict to "reported".
}

// atomicMarkSecondChanceProbation records that the provider has used its single
// second chance. Called under ConsumerSessionManager.lock from blockProvider.
func (cswp *ConsumerSessionsWithProvider) atomicMarkSecondChanceProbation() {
	atomic.StoreUint32(&cswp.onSecondChanceProbation, 1)
}

// atomicTryClearSecondChanceProbation transitions the probation flag 1->0 and
// reports whether this call performed the transition. Only the first caller
// after a successful relay returns true, so exactly one cleanup is scheduled.
func (cswp *ConsumerSessionsWithProvider) atomicTryClearSecondChanceProbation() bool {
	return atomic.CompareAndSwapUint32(&cswp.onSecondChanceProbation, 1, 0)
}

func (cswp *ConsumerSessionsWithProvider) atomicReadConflictReported() bool {
	return atomic.LoadUint32(&cswp.conflictFoundAndReported) == 1
}

func (cswp *ConsumerSessionsWithProvider) atomicWriteConflictReported() {
	atomic.StoreUint32(&cswp.conflictFoundAndReported, 1) // we can only set conflict to "reported".
}

// checking if this provider was reported this epoch already, as we can only report once per epoch
func (cswp *ConsumerSessionsWithProvider) ConflictAlreadyReported() bool {
	// returns true if reported, false if not.
	return cswp.atomicReadConflictReported()
}

// setting this provider as conflict reported.
func (cswp *ConsumerSessionsWithProvider) StoreConflictReported() {
	cswp.atomicWriteConflictReported()
}

func (cswp *ConsumerSessionsWithProvider) IsSupportingAddon(addon string) bool {
	cswp.Lock.RLock()
	defer cswp.Lock.RUnlock()
	if addon == "" {
		return true
	}
	for _, endpoint := range cswp.Endpoints {
		if _, ok := endpoint.Addons[addon]; ok {
			return true
		}
	}
	return false
}

func (cswp *ConsumerSessionsWithProvider) IsSupportingExtensions(extensions []string, ctx context.Context) bool {
	cswp.Lock.RLock()
	defer cswp.Lock.RUnlock()

	// Debug logging for archive extension filtering
	if len(extensions) > 0 {
		utils.LavaFormatTrace("[Archive Debug] Checking extensions support",
			utils.LogAttr("providerAddress", cswp.PublicLavaAddress),
			utils.LogAttr("requestedExtensions", extensions),
			utils.LogAttr("endpointExtensions", cswp.Endpoints),
			utils.LogAttr("GUID", ctx))
	}

endpointLoop:
	for _, endpoint := range cswp.Endpoints {
		for _, extension := range extensions {
			if _, ok := endpoint.Extensions[extension]; !ok {
				// doesn't support the extension required, continue to next endpoint
				utils.LavaFormatTrace("[Archive Debug] Extension not supported",
					utils.LogAttr("providerAddress", cswp.PublicLavaAddress),
					utils.LogAttr("extension", extension),
					utils.LogAttr("endpointExtensions", endpoint.Extensions),
					utils.LogAttr("GUID", ctx))
				continue endpointLoop
			}
		}
		// get here only if all extensions are supported in the endpoint
		utils.LavaFormatTrace("[Archive Debug] All extensions supported",
			utils.LogAttr("providerAddress", cswp.PublicLavaAddress),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx))
		return true
	}

	utils.LavaFormatTrace("[Archive Debug] No endpoint supports all extensions",
		utils.LogAttr("providerAddress", cswp.PublicLavaAddress),
		utils.LogAttr("extensions", extensions),
		utils.LogAttr("GUID", ctx))
	return false
}

func (cswp *ConsumerSessionsWithProvider) atomicReadUsedComputeUnits() uint64 {
	return atomic.LoadUint64(&cswp.UsedComputeUnits)
}

func (cswp *ConsumerSessionsWithProvider) GetPairingEpoch() uint64 {
	return atomic.LoadUint64(&cswp.PairingEpoch)
}

func (cswp *ConsumerSessionsWithProvider) getPublicLavaAddressAndPairingEpoch() (string, uint64) {
	cswp.Lock.RLock()
	defer cswp.Lock.RUnlock()
	return cswp.PublicLavaAddress, cswp.PairingEpoch
}

// Validate the compute units for this provider
func (cswp *ConsumerSessionsWithProvider) validateComputeUnits(cu uint64, virtualEpoch uint64) error {
	cswp.Lock.RLock()
	defer cswp.Lock.RUnlock()
	// add additional CU for virtual epochs
	if (cswp.UsedComputeUnits + cu) > cswp.MaxComputeUnits*(virtualEpoch+1) {
		return utils.LavaFormatWarning("validateComputeUnits", MaxComputeUnitsExceededError,
			utils.LogAttr("cu", cswp.UsedComputeUnits+cu),
			utils.LogAttr("maxCu", cswp.MaxComputeUnits*(virtualEpoch+1)),
			utils.LogAttr("virtualEpoch", virtualEpoch),
		)
	}
	return nil
}

// Validate and add the compute units for this provider
func (cswp *ConsumerSessionsWithProvider) addUsedComputeUnits(cu, virtualEpoch uint64) error {
	cswp.Lock.Lock()
	defer cswp.Lock.Unlock()
	// add additional CU for virtual epochs
	if (cswp.UsedComputeUnits + cu) > cswp.MaxComputeUnits*(virtualEpoch+1) {
		return MaxComputeUnitsExceededError
	}
	cswp.UsedComputeUnits += cu
	return nil
}

// getProviderStakeSize returns the stake size (in ulava) for this provider.
func (cswp *ConsumerSessionsWithProvider) getProviderStakeSize() int64 {
	cswp.Lock.RLock()
	defer cswp.Lock.RUnlock()
	return cswp.stakeSize
}

// GetProviderStakeSize returns the provider stake (in ulava) used for selection weighting.
// Exported for cross-package callers (e.g., rpcsmartrouter) that need to copy sessions.
func (cswp *ConsumerSessionsWithProvider) GetProviderStakeSize() int64 {
	return cswp.getProviderStakeSize()
}

// Validate and add the compute units for this provider
func (cswp *ConsumerSessionsWithProvider) decreaseUsedComputeUnits(cu uint64) error {
	cswp.Lock.Lock()
	defer cswp.Lock.Unlock()
	if cswp.UsedComputeUnits < cu {
		return NegativeComputeUnitsAmountError
	}
	cswp.UsedComputeUnits -= cu
	return nil
}

func (cswp *ConsumerSessionsWithProvider) ConnectRawClientWithTimeout(ctx context.Context, addr string) (pairingtypes.RelayerClient, *grpc.ClientConn, error) {
	connectCtx, cancel := context.WithTimeout(ctx, TimeoutForEstablishingAConnection)
	defer cancel()
	conn, err := ConnectGRPCClient(connectCtx, addr, AllowInsecureConnectionToProviders, false, AllowGRPCCompressionForConsumerProviderCommunication)
	if err != nil {
		return nil, nil, err
	}

	// Wait for the connection to become Ready without spawning a goroutine.
	// If the context is cancelled/times out first, return an error and close the connection.
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if connectCtx.Err() != nil {
			_ = conn.Close()
			return nil, nil, connectCtx.Err()
		}
		// WaitForStateChange blocks until the state changes or ctx is done.
		if !conn.WaitForStateChange(connectCtx, state) {
			_ = conn.Close()
			return nil, nil, connectCtx.Err()
		}
	}
	c := pairingtypes.NewRelayerClient(conn)
	return c, conn, nil
}

func (cswp *ConsumerSessionsWithProvider) GetConsumerSessionInstanceFromEndpoint(endpointConnection *EndpointConnection, numberOfResets uint64, qosManager *qos.QoSManager, networkAddress string) (singleConsumerSession *SingleConsumerSession, pairingEpoch uint64, err error) {
	// TODO: validate that the endpoint even belongs to the ConsumerSessionsWithProvider and is enabled.

	// Multiply numberOfReset +1 by MaxAllowedBlockListedSessionPerProvider as every reset needs to allow more blocked sessions allowed.
	maximumBlockedSessionsAllowed := uint64(utils.Min(MaxSessionsAllowedPerProvider, GetMaxAllowedBlockListedSessionPerProvider()*(int(numberOfResets)+1))) // +1 as we start from 0
	cswp.Lock.Lock()
	defer cswp.Lock.Unlock()

	// Check if this is a provider-relay session (endpointConnection != nil)
	// or direct RPC session (endpointConnection == nil, need to find endpoint by networkAddress)
	isProviderRelay := (endpointConnection != nil)

	// try to lock an existing session, if can't create a new one
	var numberOfBlockedSessions uint64 = 0
	for _, session := range cswp.Sessions {
		// Match session to connection (different logic for provider-relay vs direct RPC)
		matchesConnection := false
		if isProviderRelay {
			matchesConnection = (session.EndpointConnection == endpointConnection)
		} else {
			// Direct RPC: match by network address
			matchesConnection = (session.Connection != nil && session.Connection.GetEndpointAddress() == networkAddress)
		}

		if !matchesConnection {
			// skip sessions that don't belong to the active connection
			continue
		}
		blocked, ok := session.TryUseSession()
		if ok {
			return session, cswp.PairingEpoch, nil
		}
		if blocked {
			numberOfBlockedSessions += 1 // increase the number of blocked sessions so we can block this provider is too many are blocklisted
		}

		// this must come after the TryUseSession, as we need to check if we reached the maximum number of blocked sessions allowed.
		if numberOfBlockedSessions >= maximumBlockedSessionsAllowed {
			return nil, 0, MaximumNumberOfBlockListedSessionsError
		}
	}

	// No Sessions available, create a new session or return an error upon maximum sessions allowed
	if len(cswp.Sessions) > MaxSessionsAllowedPerProvider {
		return nil, 0, MaximumNumberOfSessionsExceededError
	}

	randomSessionId := int64(0)
	for randomSessionId == 0 { // we don't allow 0
		randomSessionId = rand.Int63()
	}

	// Create appropriate connection type based on mode
	var sessionConnection SessionConnection
	if isProviderRelay {
		// Provider-relay mode: Create ProviderRelayConnection wrapper
		sessionConnection = &ProviderRelayConnection{
			EndpointConnection: endpointConnection,
			QoSManager:         qosManager,
			EndpointAddress:    networkAddress,
		}
	} else {
		// Direct RPC mode: find the endpoint and use its DirectConnection
		var directConn DirectRPCConnection
		var selectedEndpoint *Endpoint
		for _, endpoint := range cswp.Endpoints {
			if endpoint.NetworkAddress == networkAddress && endpoint.IsDirectRPC() {
				if len(endpoint.DirectConnections) > 0 {
					directConn = endpoint.DirectConnections[0]
					selectedEndpoint = endpoint // ✅ Store endpoint reference
					break
				}
			}
		}

		if directConn == nil {
			return nil, 0, fmt.Errorf("direct RPC connection not found for endpoint: %s", networkAddress)
		}

		sessionConnection = &DirectRPCSessionConnection{
			DirectConnection: directConn,
			QoSManager:       qosManager,
			EndpointAddress:  networkAddress,
			Endpoint:         selectedEndpoint, // ✅ Store for per-endpoint tracking
		}
	}
	consumerSession := &SingleConsumerSession{
		SessionId:          randomSessionId,
		Parent:             cswp,
		Connection:         sessionConnection,  // Use composition-based connection
		EndpointConnection: endpointConnection, // Legacy field for backward compatibility (nil for direct RPC)
		StaticProvider:     cswp.StaticProvider,
		routerKey:          NewRouterKey(nil),
		epoch:              cswp.PairingEpoch,
		QoSManager:         qosManager, // Legacy field for backward compatibility
	}

	consumerSession.TryUseSession()                            // we must lock the session so other requests wont get it.
	cswp.Sessions[consumerSession.SessionId] = consumerSession // applying the session to the pool of sessions.
	utils.LavaFormatTrace("GetConsumerSessionInstanceFromEndpoint returning session",
		utils.LogAttr("provider", cswp.PublicLavaAddress),
		utils.LogAttr("pairingEpoch", cswp.PairingEpoch),
		utils.LogAttr("sessionId", consumerSession.SessionId),
		utils.LogAttr("isDirectRPC", !isProviderRelay),
	)
	return consumerSession, cswp.PairingEpoch, nil
}

func (cswp *ConsumerSessionsWithProvider) sortEndpointsByLatency(endpointInfos []EndpointInfo) {
	cswp.Lock.Lock()
	defer cswp.Lock.Unlock()

	// validate we do not overflow no matter what.
	if len(endpointInfos) > len(cswp.Endpoints) {
		utils.LavaFormatError("Not suppose to have larger endpointInfos length than cswp.Endpoints length", nil, utils.LogAttr("endpointInfos", endpointInfos), utils.LogAttr("cswp.Endpoints", cswp.Endpoints))
		return
	}

	// endpoint infos are already sorted by the best latency endpoint
	for idx, endpoint := range endpointInfos {
		// find the endpoint, and swap if indexes do not match expected by latency
		for cswpEndpointIdx, cswpEndpoint := range cswp.Endpoints {
			if cswpEndpoint.NetworkAddress == endpoint.Endpoint.NetworkAddress {
				// found endpoint check the index location matches the order of best endpoints
				if cswpEndpointIdx == idx {
					break
				} else {
					// we need to swap the indexes of the endpoints.
					tmpEndpoint := cswp.Endpoints[idx]
					cswp.Endpoints[idx] = endpoint.Endpoint
					cswp.Endpoints[cswpEndpointIdx] = tmpEndpoint
					break
				}
			}
		}
	}
}

// fetching an endpoint from a ConsumerSessionWithProvider and establishing a connection,
// can fail without an error if trying to connect once to each endpoint but none of them are active.
func (cswp *ConsumerSessionsWithProvider) fetchEndpointConnectionFromConsumerSessionWithProvider(ctx context.Context, retryDisabledEndpoints bool, getAllEndpoints bool, addon string, extensionNames []string) (connected bool, endpointsList []*EndpointAndChosenConnection, providerAddress string, err error) {
	getConnectionFromConsumerSessionsWithProvider := func(ctx context.Context) (connected bool, endpointPtr []*EndpointAndChosenConnection, allDisabled bool) {
		endpoints := make([]*EndpointAndChosenConnection, 0)
		cswp.Lock.Lock()
		defer cswp.Lock.Unlock()
		for idx, endpoint := range cswp.Endpoints {
			// retryDisabledEndpoints will attempt to reconnect to the provider even though we have disabled the endpoint
			// this is used on a routine that tries to reconnect to a provider that has been disabled due to being unable to connect to it.
			endpoint.mu.RLock()
			enabled := endpoint.Enabled
			endpoint.mu.RUnlock()
			if !retryDisabledEndpoints && !enabled {
				continue
			}
			if retryDisabledEndpoints {
				utils.LavaFormatDebug("retrying to connect to disabled endpoint", utils.LogAttr("endpoint", endpoint.NetworkAddress), utils.LogAttr("provider", cswp.PublicLavaAddress), utils.LogAttr("GUID", ctx))
			}

			// check endpoint supports the requested addons
			supported := endpoint.CheckSupportForServices(addon, extensionNames)
			if !supported {
				continue
			}
			// connectEndpoint tries to get an existing connection or creates a new one.
			// Uses explicit lock scopes to avoid holding lock during network calls.
			connectEndpoint := func(cswp *ConsumerSessionsWithProvider, ctx context.Context, endpoint *Endpoint) (endpointConnection_ *EndpointConnection, connected_ bool) {
				// Check if this is a direct RPC endpoint (smart router mode)
				if endpoint.IsDirectRPC() {
					// Direct RPC connections are already established in convertProvidersToSessions.
					// We no longer gate selection on a per-socket health bit: a transient
					// transport blip used to latch that bit false with no automatic recovery,
					// silently dropping the endpoint here and never sending the request that
					// would heal it (the deadlock behind the `No pairings` bug class). Instead
					// the relay is always attempted — a genuinely dead socket fails fast, feeds
					// QoS via OnSessionFailure, and (after MaxConsecutiveConnectionAttempts
					// consecutive failures, currently 50) is backed off via endpoint.Enabled,
					// both of which self-heal. With the bit gone endpoint.Enabled is now the
					// *sole* automatic disable, so in a multi-endpoint pool a dead endpoint
					// stays in selection rotation for up to that many dial-and-fail cycles
					// before backoff (the threshold was raised 5→50 — see
					// MaxConsecutiveConnectionAttempts in common.go for why and the tradeoff).
					// This mirrors how WebSocket connections have always behaved (IsHealthy
					// hardcoded true).
					//
					// The != nil check is defensive: construction (rpcsmartrouter.go, via the
					// error-checked `continue` in convertProvidersToSessions) already guarantees a
					// non-nil element, so this guards a future construction regression — not a live
					// case — and lets a nil element fall through to the (nil, false) skip below
					// instead of panicking the relay goroutine.
					if len(endpoint.DirectConnections) > 0 && endpoint.DirectConnections[0] != nil {
						utils.LavaFormatTrace("using direct RPC connection",
							utils.LogAttr("url", endpoint.DirectConnections[0].GetURL()),
							utils.LogAttr("protocol", endpoint.DirectConnections[0].GetProtocol()),
							utils.LogAttr("GUID", ctx),
						)
						return nil, true
					}
					utils.LavaFormatWarning("direct RPC endpoint has no connection object", nil,
						utils.LogAttr("endpoint", endpoint.NetworkAddress),
						utils.LogAttr("GUID", ctx),
					)
					return nil, false
				}

				// Lock the endpoint to protect concurrent access to Connections, ConnectionRefusals, and Enabled
				endpoint.mu.Lock()

				// Provider-relay path: Clean up dead connections before iterating to prevent accumulation
				cleanedConnections := make([]*EndpointConnection, 0, len(endpoint.Connections))
				deadConnectionCount := 0
				for _, conn := range endpoint.Connections {
					// Only keep connections that are:
					// 1. Not marked as disconnected
					// 2. Still have a valid connection object
					// 3. Not in Shutdown state
					if conn.connection != nil &&
						!conn.disconnected &&
						conn.connection.GetState() != connectivity.Shutdown {
						cleanedConnections = append(cleanedConnections, conn)
					} else {
						deadConnectionCount++
						// Log cleanup for visibility
						utils.LavaFormatDebug("Cleaning up dead connection",
							utils.LogAttr("provider", cswp.PublicLavaAddress),
							utils.LogAttr("endpoint", endpoint.NetworkAddress),
							utils.LogAttr("reason", func() string {
								if conn.disconnected {
									return "marked disconnected"
								} else if conn.connection == nil {
									return "nil connection"
								} else {
									return "shutdown state"
								}
							}()),
							utils.LogAttr("GUID", ctx))
					}
				}

				// Update endpoint connections with cleaned list
				if deadConnectionCount > 0 {
					endpoint.Connections = cleanedConnections
					utils.LavaFormatDebug("Cleaned up dead connections",
						utils.LogAttr("provider", cswp.PublicLavaAddress),
						utils.LogAttr("endpoint", endpoint.NetworkAddress),
						utils.LogAttr("removedCount", deadConnectionCount),
						utils.LogAttr("remainingCount", len(cleanedConnections)),
						utils.LogAttr("GUID", ctx))
				}

				for _, endpointConnection := range endpoint.Connections {
					// If connection is active and we don't have more than maximumStreamsOverASingleConnection sessions using it already,
					// and it didn't disconnect before. Use it.
					if endpointConnection.Client != nil && endpointConnection.connection != nil && !endpointConnection.disconnected {
						// Check if the endpoint is not blocked
						if endpointConnection.blockListed.Load() {
							utils.LavaFormatDebug("Skipping provider's endpoint as its block listed", utils.LogAttr("address", endpoint.NetworkAddress), utils.LogAttr("PublicLavaAddress", cswp.PublicLavaAddress), utils.LogAttr("GUID", ctx))
							continue
						}
						connectionState := endpointConnection.connection.GetState()
						// Check Disconnections
						if connectionState == connectivity.Shutdown { // || connectionState == connectivity.Idle
							// We got disconnected, we can't use this connection anymore.
							endpointConnection.disconnected = true
							continue
						}
						// Check if we can use the connection later.
						if connectionState == connectivity.TransientFailure || connectionState == connectivity.Connecting {
							continue
						}
						// Check we didn't reach the maximum streams per connection.
						if endpointConnection.getNumberOfLiveSessionsUsingThisConnection() < MaximumStreamsOverASingleConnection {
							endpoint.mu.Unlock()
							return endpointConnection, true
						}
					}
				}

				// Release lock before making network call to avoid blocking other goroutines.
				networkAddress := endpoint.NetworkAddress
				endpoint.mu.Unlock()

				client, conn, err := cswp.ConnectRawClientWithTimeout(ctx, networkAddress)

				// Re-acquire lock to update endpoint state.
				endpoint.mu.Lock()
				defer endpoint.mu.Unlock()

				if err != nil {
					// Client-side cancellations (relay race loser / client disconnect)
					// are not a provider fault — skip the refusal counter. Uses the
					// shared rule from common.IsClientCancellation so the consumer
					// session, smart-router health, and refusal counter all agree on
					// what "the client cancelled" means. Note: context.DeadlineExceeded
					// is NOT exempt here — a slow/unreachable endpoint should still
					// increment refusals.
					if common.IsClientCancellation(err, ctx) {
						utils.LavaFormatDebug("skipping ConnectionRefusals increment: request context canceled (client disconnect)",
							utils.LogAttr("err", err),
							utils.LogAttr("ctx_err", ctx.Err()),
							utils.LogAttr("provider endpoint", networkAddress),
							utils.LogAttr("GUID", ctx),
						)
						return nil, false
					}
					endpoint.ConnectionRefusals++
					utils.LavaFormatInfo("error connecting to provider",
						utils.LogAttr("err", err),
						utils.LogAttr("provider endpoint", networkAddress),
						utils.LogAttr("providerName", cswp.PublicLavaAddress),
						utils.LogAttr("endpoint", endpoint),
						utils.LogAttr("refusals", endpoint.ConnectionRefusals),
						utils.LogAttr("GUID", ctx),
					)

					if endpoint.ConnectionRefusals >= MaxConsecutiveConnectionAttempts {
						// Edge-trigger disabledAt on the actual enabled→false transition, mirroring
						// MarkUnhealthy (F1). This path disables only PROVIDER-RELAY endpoints (direct-RPC
						// returns early above and is disabled solely via MarkUnhealthy), so it is inert for
						// the prober's recovery today — but stamping here keeps "every disable transition
						// records disabledAt" an invariant of the type, not a property of one caller.
						if endpoint.Enabled {
							endpoint.Enabled = false
							endpoint.disabledAt = time.Now()
							endpoint.consecutiveHealthyProbes = 0
							endpoint.lastRecoveryPoll = time.Time{}
						}
						utils.LavaFormatWarning("disabling provider endpoint for the duration of current epoch.", nil,
							utils.LogAttr("Endpoint", networkAddress),
							utils.LogAttr("address", cswp.PublicLavaAddress),
							utils.LogAttr("GUID", ctx),
						)
					}
					return nil, false
				}
				endpoint.ConnectionRefusals = 0
				newConnection := &EndpointConnection{connection: conn, Client: client, lbUniqueId: strconv.FormatUint(utils.GenerateUniqueIdentifier(), 10)}
				endpoint.Connections = append(endpoint.Connections, newConnection)
				return newConnection, true
			}

			endpointConnection, connected_ := connectEndpoint(cswp, ctx, endpoint)
			if !connected_ {
				continue
			}
			endpoint.mu.Lock()
			cswp.Endpoints[idx].Enabled = true // return enabled once we successfully reconnect
			// Clear recovery tracking on re-enable so a future disable starts a fresh streak (F1).
			cswp.Endpoints[idx].consecutiveHealthyProbes = 0
			cswp.Endpoints[idx].disabledAt = time.Time{}
			cswp.Endpoints[idx].lastRecoveryPoll = time.Time{}
			endpoint.mu.Unlock()
			// successful new connection add to endpoints list
			endpoints = append(endpoints, &EndpointAndChosenConnection{endpoint: endpoint, chosenEndpointConnection: endpointConnection})
			if !getAllEndpoints {
				return true, endpoints, false
			}
		}

		// if we managed to get at least one endpoint we can return the list of active endpoints
		if len(endpoints) > 0 {
			return true, endpoints, false
		}

		// checking disabled endpoints, as we can disable an endpoint mid run of the previous loop, we should re test the current endpoint state
		// before verifying all are Disabled.
		allDisabled = true
		for _, endpoint := range cswp.Endpoints {
			endpoint.mu.RLock()
			enabled := endpoint.Enabled
			endpoint.mu.RUnlock()
			if !enabled {
				continue
			}
			// even one endpoint is enough for us to not purge.
			allDisabled = false
		}
		return false, nil, allDisabled
	}

	var allDisabled bool
	connected, endpointsList, allDisabled = getConnectionFromConsumerSessionsWithProvider(ctx)
	if allDisabled {
		utils.LavaFormatInfo("purging provider after all endpoints are disabled",
			utils.LogAttr("provider endpoints", cswp.Endpoints),
			utils.LogAttr("providerName", cswp.PublicLavaAddress),
			utils.LogAttr("GUID", ctx),
		)
		// report provider.
		return connected, endpointsList, cswp.PublicLavaAddress, AllProviderEndpointsDisabledError
	}

	return connected, endpointsList, cswp.PublicLavaAddress, nil
}

func CalcWeightsByStake(providers map[uint64]*ConsumerSessionsWithProvider) (weights map[string]int64) {
	weights = make(map[string]int64)
	staticProvidersToBoost := make([]*ConsumerSessionsWithProvider, 0)
	maxWeight := int64(1)
	for _, cswp := range providers {
		stakeAmount := cswp.getProviderStakeSize()
		stake := int64(10) // defaults to 10 if stake isn't set
		if stakeAmount > 0 {
			stake = stakeAmount
		}
		// stakeOmitted is true when no explicit stake was configured (stakeAmount == 0).
		stakeOmitted := stakeAmount == 0
		// Preserve legacy behavior for static providers: if no explicit stake was set (stakeAmount==0),
		// boost them relative to the max stake in the pairing list.
		// If explicit stake is provided (>0), treat the static provider like any other provider.
		if cswp.StaticProvider && stakeOmitted {
			staticProvidersToBoost = append(staticProvidersToBoost, cswp)
			continue
		}
		if stake > maxWeight {
			maxWeight = stake
		}
		weights[cswp.PublicLavaAddress] = stake
	}
	for _, cswp := range staticProvidersToBoost {
		weights[cswp.PublicLavaAddress] = maxWeight * WeightMultiplierForStaticProviders
	}
	return weights
}
