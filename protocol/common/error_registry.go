package common

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

// ErrorCategory — top-level grouping: internal (router-introduced) vs external (pass-through)
type ErrorCategory int

const (
	CategoryInternal ErrorCategory = iota // Errors introduced by the router — user would never see these without it
	CategoryExternal                      // Pass-through errors — user would get the same error talking to the node directly
)

func (c ErrorCategory) String() string {
	switch c {
	case CategoryInternal:
		return "internal"
	case CategoryExternal:
		return "external"
	default:
		return "unknown"
	}
}

// ErrorSubCategory — finer classification within each category.
// Subcategories carry behavioral implications that the consumer hot path
// branches on (retries, CU charging, caching).
type ErrorSubCategory int

const (
	SubCategoryNone              ErrorSubCategory = iota
	SubCategoryUnsupportedMethod                  // zero retries, zero CU, cached response, no provider scoring
	SubCategoryRateLimit                          // rate-limit signal: endpoint is healthy but busy, apply backoff, do not mark unhealthy
	SubCategoryDataScope                          // endpoint does not hold the data: retry elsewhere may help, but it is not unhealthy
)

func (sc ErrorSubCategory) String() string {
	switch sc {
	case SubCategoryUnsupportedMethod:
		return "unsupported_method"
	case SubCategoryRateLimit:
		return "rate_limit"
	case SubCategoryDataScope:
		return "data_scope"
	default:
		return "none"
	}
}

func (sc ErrorSubCategory) IsUnsupportedMethod() bool {
	return sc == SubCategoryUnsupportedMethod
}

// IsRateLimit reports whether this subcategory represents a "slow down"
// signal from the endpoint. Health-tracking callers should apply backoff
// but NOT mark the endpoint unhealthy — it's working, just busy.
func (sc ErrorSubCategory) IsRateLimit() bool {
	return sc == SubCategoryRateLimit
}

// IsDataScope reports whether this subcategory represents "the endpoint answered
// authoritatively that it does not hold the requested data" — pruned history, an
// object that was never created, a height outside its retained range.
//
// It is a third fault axis, independent of the other two, and it exists because
// neither of them can express this case:
//
//   - Retryable stays TRUE. A pruned node and an archive node genuinely disagree,
//     so another endpoint can still answer. Marking these non-retryable to keep
//     them out of the availability signal would buy the exemption by killing the
//     archive fallback.
//   - The endpoint is nonetheless HEALTHY. It did its job and reported the truth
//     about its own data scope, so charging it an availability failure demotes it
//     for the shape of the request rather than for anything it did wrong.
//
// Health-tracking callers should therefore keep retrying elsewhere but leave the
// endpoint's availability score untouched.
func (sc ErrorSubCategory) IsDataScope() bool {
	return sc == SubCategoryDataScope
}

// RouterError is the central error definition — a classification struct, not a Go error.
// It is metadata *about* an error, used for logging, metrics, and retry decisions.
// The original error always passes through unchanged to the user (transparent hop).
type RouterError struct {
	Code        uint32
	Name        string
	Category    ErrorCategory
	SubCategory ErrorSubCategory
	Description string
	Retryable   bool // retrying same relay with same params to a different provider has a chance of succeeding
}

func (le *RouterError) String() string {
	return fmt.Sprintf("[%d] %s", le.Code, le.Name)
}

// Error implements the error interface so RouterError can be used directly as an error
// and checked with errors.Is().
func (le *RouterError) Error() string {
	return fmt.Sprintf("%s: %s", le.Name, le.Description)
}

// Is implements errors.Is matching by error code.
// This allows errors.Is(err, RouterErrorSomething) to work when err wraps a RouterError.
func (le *RouterError) Is(target error) bool {
	if t, ok := target.(*RouterError); ok {
		return le.Code == t.Code
	}
	return false
}

// ABCICode returns the error code for gRPC wire protocol compatibility.
// This replaces the sdkerrors.ABCICode() method so RouterError can be used
// in status.Error(codes.Code(err.ABCICode()), ...) calls.
func (le *RouterError) ABCICode() uint32 {
	return le.Code
}

// IsRateLimited reports whether this classification represents a rate-limit
// signal from any layer (protocol, node, or node-specific limit exceeded).
// Driven by SubCategoryRateLimit rather than a hardcoded code comparison so
// new rate-limit codes only need to be tagged at registration time.
func (le *RouterError) IsRateLimited() bool {
	return le != nil && le.SubCategory.IsRateLimit()
}

// ---------------------------------------------------------------------------
// Chain families — grouping chains by their error system
// ---------------------------------------------------------------------------

type ChainFamily int

// ChainFamilyUnknown is a sentinel returned for chains not registered in chainFamilyMap.
// Tier-2 lookups against this value intentionally miss so classification falls through
// to transport-scoped Tier-1 matchers instead of inheriting another family's semantics.
// Defined explicitly as -1 to avoid colliding with the iota-numbered real families below.
const ChainFamilyUnknown ChainFamily = -1

const (
	ChainFamilyEVM ChainFamily = iota
	ChainFamilySolana
	ChainFamilyBitcoin
	ChainFamilyCosmosSDK
	ChainFamilyStarknet
	ChainFamilyAptos
	ChainFamilyNEAR
	ChainFamilyXRP
	ChainFamilyStellar
	ChainFamilyTON
	ChainFamilyPolygonZkEVM
	ChainFamilyCardano
	ChainFamilyTron
	ChainFamilySui
)

func (cf ChainFamily) String() string {
	switch cf {
	case ChainFamilyUnknown:
		return "Unknown"
	case ChainFamilyEVM:
		return "EVM"
	case ChainFamilySolana:
		return "Solana"
	case ChainFamilyBitcoin:
		return "Bitcoin"
	case ChainFamilyCosmosSDK:
		return "CosmosSDK"
	case ChainFamilyStarknet:
		return "Starknet"
	case ChainFamilyAptos:
		return "Aptos"
	case ChainFamilyNEAR:
		return "NEAR"
	case ChainFamilyXRP:
		return "XRP"
	case ChainFamilyStellar:
		return "Stellar"
	case ChainFamilyTON:
		return "TON"
	case ChainFamilyPolygonZkEVM:
		return "PolygonZkEVM"
	case ChainFamilyCardano:
		return "Cardano"
	case ChainFamilyTron:
		return "Tron"
	case ChainFamilySui:
		return "Sui"
	default:
		return fmt.Sprintf("ChainFamily(%d)", int(cf))
	}
}

// chainFamilyMap maps chain ID strings (from spec JSON files) to their chain family.
// Sourced from specs/mainnet-1/specs/ and specs/testnet-2/specs/.
//
// INVARIANT: populated at package-init time and never mutated at runtime.
// Read paths (GetChainFamily, GetChainFamilyOrDefault, ClassifyError) rely on
// this for lock-free concurrent access. If a future change needs dynamic
// registration, it must introduce explicit synchronisation — do NOT mutate
// this map from anywhere other than its literal declaration below.
var chainFamilyMap = map[string]ChainFamily{
	// EVM — all jsonrpc chains with EVM execution semantics
	"ETH1": ChainFamilyEVM, "SEP1": ChainFamilyEVM, "HOL1": ChainFamilyEVM,
	"ARBITRUM": ChainFamilyEVM, "ARBITRUMN": ChainFamilyEVM, "ARBITRUMS": ChainFamilyEVM,
	"POLYGON": ChainFamilyEVM, "POLYGONA": ChainFamilyEVM,
	"BASE": ChainFamilyEVM, "BASES": ChainFamilyEVM,
	"OPTM": ChainFamilyEVM, "OPTMS": ChainFamilyEVM,
	"AVAX": ChainFamilyEVM, "AVAXT": ChainFamilyEVM,
	"AVAXC": ChainFamilyEVM, "AVAXCT": ChainFamilyEVM,
	"AVAXP": ChainFamilyEVM, "AVAXPT": ChainFamilyEVM,
	"BSC": ChainFamilyEVM, "BSCT": ChainFamilyEVM,
	"BLAST": ChainFamilyEVM, "BLASTSP": ChainFamilyEVM,
	"FTM250": ChainFamilyEVM, "FTM4002": ChainFamilyEVM,
	"SONIC": ChainFamilyEVM, "SONICT": ChainFamilyEVM,
	"CELO": ChainFamilyEVM, "ALFAJORES": ChainFamilyEVM,
	"FUSE": ChainFamilyEVM,
	"FVM":  ChainFamilyEVM, "FVMT": ChainFamilyEVM,
	"HEDERA": ChainFamilyEVM, "HEDERAT": ChainFamilyEVM,
	"HYPERLIQUID": ChainFamilyEVM, "HYPERLIQUIDT": ChainFamilyEVM,
	"WORLDCHAIN": ChainFamilyEVM, "WORLDCHAINS": ChainFamilyEVM,
	"AGR": ChainFamilyEVM, "AGRT": ChainFamilyEVM,
	"BERA": ChainFamilyEVM, "BERAT": ChainFamilyEVM, "BERAT2": ChainFamilyEVM,
	"CANTO":     ChainFamilyEVM,
	"KAKAROTT":  ChainFamilyEVM,
	"SPARK":     ChainFamilyEVM,
	"ETHBEACON": ChainFamilyEVM,
	"MOVEMENT":  ChainFamilyEVM, "MOVEMENTT": ChainFamilyEVM,

	// Solana
	"SOLANA": ChainFamilySolana, "SOLANAT": ChainFamilySolana,

	// Bitcoin/UTXO
	"BTC": ChainFamilyBitcoin, "BTCT": ChainFamilyBitcoin,
	"LTC": ChainFamilyBitcoin, "LTCT": ChainFamilyBitcoin,
	"DOGE": ChainFamilyBitcoin, "DOGET": ChainFamilyBitcoin,
	"BCH": ChainFamilyBitcoin, "BCHT": ChainFamilyBitcoin,

	// Cosmos SDK — chains with grpc/rest/tendermintrpc interfaces
	"COSMOSHUB": ChainFamilyCosmosSDK, "COSMOSHUBT": ChainFamilyCosmosSDK,
	"LAVA": ChainFamilyCosmosSDK, "LAV1": ChainFamilyCosmosSDK,
	"AXELAR": ChainFamilyCosmosSDK, "AXELART": ChainFamilyCosmosSDK,
	"EVMOS": ChainFamilyCosmosSDK, "EVMOST": ChainFamilyCosmosSDK,
	"OSMOSIS": ChainFamilyCosmosSDK, "OSMOSIST": ChainFamilyCosmosSDK,
	"JUN1": ChainFamilyCosmosSDK, "JUNT1": ChainFamilyCosmosSDK,
	"CELESTIA": ChainFamilyCosmosSDK, "CELESTIATA": ChainFamilyCosmosSDK, "CELESTIATM": ChainFamilyCosmosSDK,
	"STRGZ": ChainFamilyCosmosSDK, "STRGZT": ChainFamilyCosmosSDK,
	"SECRET": ChainFamilyCosmosSDK, "SECRETP": ChainFamilyCosmosSDK,
	"IBC":       ChainFamilyCosmosSDK,
	"COSMOSSDK": ChainFamilyCosmosSDK, "COSMOSSDK50": ChainFamilyCosmosSDK,
	"COSMOSSDK45DEP": ChainFamilyCosmosSDK, "COSMOSSDKFULL": ChainFamilyCosmosSDK,
	"COSMOSWASM": ChainFamilyCosmosSDK,
	"ETHERMINT":  ChainFamilyCosmosSDK,
	"TENDERMINT": ChainFamilyCosmosSDK,
	"UNIONT":     ChainFamilyCosmosSDK,

	// Starknet
	"STRK": ChainFamilyStarknet, "STRKS": ChainFamilyStarknet,

	// Aptos
	"APT1": ChainFamilyAptos,

	// Sui — shares Move heritage with Aptos but has a distinct JSON-RPC surface
	// and error taxonomy, so it gets its own family.
	"SUIT": ChainFamilySui,

	// NEAR
	"NEAR": ChainFamilyNEAR, "NEART": ChainFamilyNEAR,

	// XRP
	"XRP": ChainFamilyXRP, "XRPT": ChainFamilyXRP,

	// Stellar
	"XLM": ChainFamilyStellar, "XLMT": ChainFamilyStellar,

	// TON
	"TON": ChainFamilyTON, "TONT": ChainFamilyTON,

	// Tron — REST-based with own error format (string enums like CONTRACT_EXE_ERROR)
	"TRX": ChainFamilyTron, "TRXT": ChainFamilyTron,

	// Cardano
	"CARDANO": ChainFamilyCardano, "CARDANOT": ChainFamilyCardano,

	// Koii
	"KOII": ChainFamilySolana, "KOIIT": ChainFamilySolana,
}

// GetChainFamily returns the chain family for a given chain ID.
// Returns ok=false for unknown chains; the caller is responsible for handling that case.
func GetChainFamily(chainID string) (ChainFamily, bool) {
	family, ok := chainFamilyMap[chainID]
	return family, ok
}

// GetChainFamilyOrDefault returns the chain family for a given chain ID,
// or ChainFamilyUnknown if the chain is not registered. Defaulting to a concrete
// family (e.g. EVM) would silently inherit that family's Tier-2 matchers for
// unregistered chains — the sentinel ensures Tier-2 lookups miss and classification
// falls back to transport-scoped Tier-1 matchers instead.
func GetChainFamilyOrDefault(chainID string) ChainFamily {
	if family, ok := chainFamilyMap[chainID]; ok {
		return family
	}
	return ChainFamilyUnknown
}

// ---------------------------------------------------------------------------
// Transport types — determines which generic matchers to evaluate
// ---------------------------------------------------------------------------

type TransportType int

const (
	TransportJsonRPC TransportType = iota // EVM, Solana, Bitcoin, etc. (includes WebSocket subscriptions)
	TransportREST                         // Aptos, Stellar, some Cosmos endpoints
	TransportGRPC                         // Cosmos SDK chains
)

func (t TransportType) String() string {
	switch t {
	case TransportJsonRPC:
		return "JsonRPC"
	case TransportREST:
		return "REST"
	case TransportGRPC:
		return "gRPC"
	default:
		return fmt.Sprintf("TransportType(%d)", int(t))
	}
}

// ---------------------------------------------------------------------------
// Error Matchers
// ---------------------------------------------------------------------------

// ErrorMatcher classifies errors by matching error codes and/or messages.
type ErrorMatcher interface {
	Matches(errorCode int, errorMessage string) bool
}

// loweredMessageMatcher is an optional fast-path interface used by ClassifyError
// to avoid re-lowercasing the error message for every matcher. Matchers that do
// case-insensitive substring matching implement this; ClassifyError lowers the
// input message once per classification and feeds it here. Matchers that do not
// implement it continue to go through the standard Matches path.
type loweredMessageMatcher interface {
	matchesLowered(errorCode int, loweredMessage string) bool
}

// CodeEquals matches an exact error code (e.g., JSON-RPC -32601).
type codeEqualsMatcher struct {
	code int
}

func (m codeEqualsMatcher) Matches(errorCode int, _ string) bool {
	return errorCode == m.code
}

func CodeEquals(code int) ErrorMatcher {
	return codeEqualsMatcher{code: code}
}

// MessageContains matches a case-insensitive substring in the error message.
type messageContainsMatcher struct {
	substring string // pre-lowercased at construction time
}

func (m messageContainsMatcher) Matches(_ int, errorMessage string) bool {
	return strings.Contains(strings.ToLower(errorMessage), m.substring)
}

// matchesLowered implements the loweredMessageMatcher fast path: the caller
// already lowered the message, so we skip the per-call strings.ToLower.
func (m messageContainsMatcher) matchesLowered(_ int, loweredMessage string) bool {
	return strings.Contains(loweredMessage, m.substring)
}

func MessageContains(substring string) ErrorMatcher {
	return messageContainsMatcher{substring: strings.ToLower(substring)}
}

// MessageRegex matches the error message against a regex pattern.
type messageRegexMatcher struct {
	re *regexp.Regexp
}

func (m messageRegexMatcher) Matches(_ int, errorMessage string) bool {
	return m.re.MatchString(errorMessage)
}

func MessageRegex(pattern string) ErrorMatcher {
	return messageRegexMatcher{re: regexp.MustCompile(pattern)}
}

// HTTPStatusContains matches an HTTP status code in the error message with non-digit
// boundary checks. This prevents false positives like "500" matching inside "25001500".
// Prefer structured status codes (CodeEquals) when the HTTP status is available as an int.
type httpStatusMatcher struct {
	status string
}

func (m httpStatusMatcher) Matches(_ int, errorMessage string) bool {
	target := m.status
	msg := errorMessage
	for {
		idx := strings.Index(msg, target)
		if idx < 0 {
			return false
		}
		// Check that the character before the match (if any) is not a digit
		if idx > 0 && msg[idx-1] >= '0' && msg[idx-1] <= '9' {
			msg = msg[idx+len(target):]
			continue
		}
		// Check that the character after the match (if any) is not a digit
		end := idx + len(target)
		if end < len(msg) && msg[end] >= '0' && msg[end] <= '9' {
			msg = msg[end:]
			continue
		}
		return true
	}
}

func HTTPStatusContains(status int) ErrorMatcher {
	return httpStatusMatcher{status: fmt.Sprintf("%d", status)}
}

// GRPCCodeEquals matches a gRPC status code.
type grpcCodeMatcher struct {
	code uint32
}

func (m grpcCodeMatcher) Matches(errorCode int, _ string) bool {
	if errorCode < 0 || errorCode > math.MaxUint32 {
		return false
	}
	return uint32(errorCode) == m.code
}

func GRPCCodeEquals(code uint32) ErrorMatcher {
	return grpcCodeMatcher{code: code}
}

// ---------------------------------------------------------------------------
// Error mapping entry
// ---------------------------------------------------------------------------

type errorMapping struct {
	Matcher     ErrorMatcher
	RouterError *RouterError
}

// ---------------------------------------------------------------------------
// Reserved error codes
// ---------------------------------------------------------------------------

// RouterErrorUnknown is the fallback for unclassified errors.
// Category is External because unmatched errors are node pass-throughs.
//
// Retryable is intentionally TRUE: retrying unknowns is the deliberate
// fail-open policy. The rationale is that the registry cannot possibly
// enumerate every transient node/proxy error shape in the wild, so any
// unrecognised failure is *more likely* to be a one-off hiccup worth
// retrying on a different provider than a permanent condition. Failing
// closed (Retryable: false) would silently starve real transient errors
// — a brand-new 5xx variant from a CDN, an unrecognised proxy error
// body, etc. — the moment they started appearing in production.
//
// The retry amplification concern (a struggling provider repeatedly hit
// by retries) is handled independently:
//   - The relay retry limit caps per-request attempts (RelayRetryLimit).
//   - classifyEndpointHealth marks providers unhealthy on CategoryInternal
//     failures, which removes them from rotation.
//   - The relay-race design sends one attempt per provider per retry, not
//     a tight loop against a single struggling provider.
//
// If a new unknown-error class starts dominating smartrouter_errors_total in
// production, the right response is to add a matcher for it in
// error_classifier.go (demoting it from Unknown to a specific code with
// the appropriate Retryable flag), not to flip this default.
var RouterErrorUnknown = &RouterError{
	Code: 0, Name: "UNKNOWN_ERROR", Category: CategoryExternal,
	Description: "Unclassified error — no matcher matched", Retryable: true,
}

// ---------------------------------------------------------------------------
// Registry — unexported, access via lookup helpers
// ---------------------------------------------------------------------------

// INVARIANT: errorRegistry and errorRegistryName are populated exclusively at
// package-init time via registerError() calls from the var declarations in
// error_codes.go. After init, they are read-only — getRouterError() and
// getRouterErrorByName() access them without locking and rely on this invariant.
// Do NOT introduce runtime mutations without adding explicit synchronisation.
var (
	errorRegistry     = map[uint32]*RouterError{RouterErrorUnknown.Code: RouterErrorUnknown}
	errorRegistryName = map[string]*RouterError{RouterErrorUnknown.Name: RouterErrorUnknown}
)

func registerError(le *RouterError) *RouterError {
	// Code 0 is reserved for RouterErrorUnknown, which is wired into the registry
	// directly (not via registerError). Rejecting Code 0 here with an explicit
	// message avoids a confusing "duplicate error code: 0 (UNKNOWN_ERROR)" panic
	// when a new error is accidentally declared with Code: 0.
	if le.Code == 0 {
		panic(fmt.Sprintf("error code 0 is reserved for RouterErrorUnknown; %s must use a non-zero code", le.Name))
	}
	if existing, exists := errorRegistry[le.Code]; exists {
		panic(fmt.Sprintf("duplicate error code: %d — %s conflicts with existing %s", le.Code, le.Name, existing.Name))
	}
	if existing, exists := errorRegistryName[le.Name]; exists {
		panic(fmt.Sprintf("duplicate error name: %s — new code %d conflicts with existing code %d", le.Name, le.Code, existing.Code))
	}
	errorRegistry[le.Code] = le
	errorRegistryName[le.Name] = le
	return le
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

func getRouterError(code uint32) *RouterError {
	if le, ok := errorRegistry[code]; ok {
		return le
	}
	return RouterErrorUnknown
}

func getRouterErrorByName(name string) *RouterError {
	if le, ok := errorRegistryName[name]; ok {
		return le
	}
	return RouterErrorUnknown
}

func isRetryable(code uint32) bool {
	return getRouterError(code).Retryable
}

// RouterWrappedError wraps a RouterError with context, supporting errors.Is matching.
type RouterWrappedError struct {
	RouterErr *RouterError
	Context   string
	// cause is the error this classification was derived from, retained so sentinel
	// checks (errors.Is(err, context.Canceled), net.Error, syscall errnos) still work
	// on the far side of classification. Constructing with NewRouterError leaves it nil
	// and only the Context string survives — which is exactly how the relay-race
	// cancellation carve-out silently died (MAG-2648). Prefer NewRouterErrorWrapping
	// whenever the original error is in hand.
	cause error
}

// Every method below guards e.RouterErr. The constructors always set it today, so these are
// latent — but a nil there fails in three different ways (Error panics inside a logging
// call, Is silently mismatches, As hands back a nil AND halts the walk), and this type's
// whole reason for existing is that a quietly unreachable resolution path is expensive.

func (e *RouterWrappedError) Error() string {
	if e == nil {
		return ""
	}
	if e.RouterErr == nil {
		return e.Context
	}
	if e.Context != "" {
		return fmt.Sprintf("%s: %s", e.Context, e.RouterErr.Description)
	}
	return e.RouterErr.Error()
}

func (e *RouterWrappedError) Is(target error) bool {
	if e == nil || e.RouterErr == nil {
		return false
	}
	if t, ok := target.(*RouterError); ok {
		return e.RouterErr.Code == t.Code
	}
	return false
}

// Unwrap returns the original cause when one was retained, else the classification.
//
// Returning e.RouterErr unconditionally was the MAG-2648 bug: the cause's sentinel
// (context.Canceled) became unreachable, so the relay-race carve-out could never fire.
//
// The multi-error form (Unwrap() []error) looks like the tidier fix and is NOT used
// here on purpose: errors.Unwrap — the singular one — is documented to return nil for
// multi-unwrap types, which would silently break every caller walking a chain with it,
// including the sentinel walker in utils/score/score_errors.go. Keeping the singular
// form preserves that traversal; the RouterErr branch it displaces stays reachable
// through Is and As below, so nothing loses a path.
func (e *RouterWrappedError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.cause != nil {
		return e.cause
	}
	if e.RouterErr == nil {
		// Returning a typed nil here would hand back a non-nil error interface holding a
		// nil *RouterError, and the next errors.Is would call RouterError.Is on a nil receiver.
		return nil
	}
	return e.RouterErr
}

// As keeps the *RouterError reachable via errors.As even when Unwrap yields the cause
// instead. Without this, retaining a cause would quietly remove a resolution path that
// worked before — the same species of silent reachability loss this fix exists to undo.
// Guarding on nil is load-bearing, not defensive noise: returning true with a nil
// *RouterError would both hand the caller a nil AND stop errors.As walking to a real one
// deeper in the chain — strictly worse than never matching.
func (e *RouterWrappedError) As(target any) bool {
	if e == nil || e.RouterErr == nil {
		return false
	}
	if t, ok := target.(**RouterError); ok {
		*t = e.RouterErr
		return true
	}
	return false
}

// NewRouterError creates an error that wraps a RouterError with optional context.
// Supports errors.Is(err, RouterErrorSomething) matching by code.
//
// This constructor keeps no cause, so sentinel checks against the underlying error
// will NOT match. Use NewRouterErrorWrapping when the original error is available.
func NewRouterError(routerErr *RouterError, context string) error {
	return &RouterWrappedError{RouterErr: routerErr, Context: context}
}

// NewRouterErrorWrapping is NewRouterError that retains the original error, so both
// errors.Is(err, RouterErrorSomething) and errors.Is(err, <sentinel of the cause>)
// resolve. The Context string is derived from the cause, preserving the message
// shape NewRouterError(classified, err.Error()) produced.
func NewRouterErrorWrapping(routerErr *RouterError, cause error) error {
	if cause == nil {
		return &RouterWrappedError{RouterErr: routerErr}
	}
	return &RouterWrappedError{RouterErr: routerErr, Context: cause.Error(), cause: cause}
}

func IsInternal(code uint32) bool {
	return getRouterError(code).Category == CategoryInternal
}

func IsExternal(code uint32) bool {
	return getRouterError(code).Category == CategoryExternal
}
