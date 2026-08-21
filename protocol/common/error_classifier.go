package common

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
)

// ---------------------------------------------------------------------------
// Tier 2: Chain-specific error mappings (checked first, overrides generic)
// ---------------------------------------------------------------------------

// INVARIANT: chainErrorMappings is populated exclusively at package-init time
// via this literal declaration. It is never mutated at runtime, which is what
// lets ClassifyError read it lock-free on the hot classification path. If you
// need to support dynamic registration, add explicit synchronisation — do NOT
// silently mutate this map from elsewhere.
var chainErrorMappings = map[ChainFamily][]errorMapping{
	ChainFamilySolana: {
		// Source: Solana RPC custom error codes (agave rpc-client-api/src/custom_error.rs)
		{CodeEquals(-32001), RouterErrorChainStatePruned},                                        // "Block cleaned up, does not exist on node"
		{CodeEquals(-32002), RouterErrorChainSolanaSimulationFailed},                             // "Transaction simulation failed"
		{CodeEquals(-32003), RouterErrorChainSolanaSignatureVerifyFailed},                        // "Signature verification failure"
		{CodeEquals(-32004), RouterErrorChainBlockNotFound},                                      // "Block not available for slot N"
		{CodeEquals(-32005), RouterErrorNodeSolanaUnhealthy},                                     // "Node is unhealthy" / "Node is behind by N slots"
		{CodeEquals(-32007), RouterErrorChainSolanaLedgerJump},                                   // "Slot skipped or missing"
		{CodeEquals(-32009), RouterErrorChainSolanaMissingLongTerm},                              // "Slot missing in long-term storage"
		{MessageContains("missing in long-term storage"), RouterErrorChainSolanaMissingLongTerm}, // message-based fallback
		{CodeEquals(-32010), RouterErrorChainSolanaExcludedFromIndex},                            // "Excluded from account secondary indexes"
		{CodeEquals(-32013), RouterErrorChainSolanaSignatureLengthMismatch},                      // "Signature length mismatch"
		{CodeEquals(-32014), RouterErrorChainSolanaBlockStatusUnavailable},                       // "Block status unavailable"
		{CodeEquals(-32015), RouterErrorChainSolanaTxVersionUnsupported},                         // "Transaction version not supported"
		{CodeEquals(-32016), RouterErrorChainSolanaMinContextSlotNotReached},                     // "Minimum context slot not reached"
		// Blockhash expiry — extremely common under load when clients submit tx
		// with a recent blockhash that has since rolled off the 150-slot window.
		// Placed before generic Tier-1 matchers would see it; case-insensitive
		// via the matcher pre-lowering.
		{MessageContains("blockhash not found"), RouterErrorChainSolanaBlockhashNotFound},
	},
	ChainFamilyBitcoin: {
		// Source: Bitcoin Core src/rpc/protocol.h
		{CodeEquals(-28), RouterErrorNodeBitcoinWarmup},                  // RPC_IN_WARMUP: "Loading block index..."
		{CodeEquals(-10), RouterErrorNodeBitcoinInitialDownload},         // RPC_CLIENT_IN_INITIAL_DOWNLOAD
		{CodeEquals(-9), RouterErrorNodeBitcoinNotConnected},             // RPC_CLIENT_NOT_CONNECTED: "Shutting down"
		{CodeEquals(-25), RouterErrorChainBitcoinVerifyError},            // RPC_VERIFY_ERROR
		{CodeEquals(-26), RouterErrorChainBitcoinVerifyRejected},         // RPC_VERIFY_REJECTED
		{CodeEquals(-27), RouterErrorChainBitcoinAlreadyInChain},         // RPC_VERIFY_ALREADY_IN_CHAIN
		{CodeEquals(-6), RouterErrorChainBitcoinWalletInsufficientFunds}, // RPC_WALLET_INSUFFICIENT_FUNDS
		// Message-based: UTXO already spent / double-spend detection.
		// Bitcoin Core returns these through sendrawtransaction rejections.
		{MessageContains("already spent"), RouterErrorChainDoubleSpend},
		{MessageContains("double spend"), RouterErrorChainDoubleSpend},
		{MessageContains("txn-mempool-conflict"), RouterErrorChainDoubleSpend}, // Bitcoin Core specific
	},
	ChainFamilyCosmosSDK: {
		// Cosmos SDK transaction errors. These must be Tier-2 (not Tier-1)
		// because "insufficient fee" also collides with Starknet's dedicated
		// CHAIN_STARKNET_INSUFFICIENT_FEE (code 53) which is handled in the
		// Starknet block below — Tier-2 scoping keeps each chain's semantics
		// from bleeding into the other.
		{MessageContains("account sequence mismatch"), RouterErrorChainInvalidSequence}, // Cosmos SDK x/auth
		{MessageContains("insufficient fees"), RouterErrorChainInsufficientFee},         // Cosmos SDK x/auth plural
		{MessageContains("insufficient fee"), RouterErrorChainInsufficientFee},          // Singular variant
		// "account not found" is Cosmos SDK x/auth for a missing signer account.
		// Generic Tier-1 doesn't carry this matcher because EVM uses different
		// phrasing and would false-match contract calls against unfunded addresses.
		{MessageContains("account not found"), RouterErrorChainAccountNotFound},
	},
	ChainFamilyStarknet: {
		// Source: Starknet JSON-RPC spec error codes
		{CodeEquals(1), RouterErrorChainStarknetFailedToReceiveTx},
		{CodeEquals(20), RouterErrorChainStarknetContractNotFound},
		{CodeEquals(24), RouterErrorChainStarknetBlockNotFound},
		{CodeEquals(28), RouterErrorChainStarknetClassNotFound},
		{CodeEquals(29), RouterErrorChainStarknetTxHashNotFound},
		{CodeEquals(40), RouterErrorChainStarknetContractError},
		{CodeEquals(41), RouterErrorChainStarknetTxExecError},
		{CodeEquals(51), RouterErrorChainStarknetClassAlreadyDeclared},
		{CodeEquals(52), RouterErrorChainStarknetInvalidNonce},
		{CodeEquals(53), RouterErrorChainStarknetInsufficientFee},
		{CodeEquals(54), RouterErrorChainStarknetInsufficientBalance},
		{CodeEquals(55), RouterErrorChainStarknetValidationFailure},
		{CodeEquals(56), RouterErrorChainStarknetCompilationFailed},
		{CodeEquals(59), RouterErrorChainStarknetDuplicateTx},
		{CodeEquals(61), RouterErrorChainStarknetTxVersionUnsupported},
		{CodeEquals(63), RouterErrorChainStarknetUnexpectedError},
	},
	ChainFamilyNEAR: {
		// Source: NEAR RPC docs — matched by error.cause.name in message
		{MessageContains("UNKNOWN_BLOCK"), RouterErrorChainNEARUnknownBlock},
		{MessageContains("UNKNOWN_CHUNK"), RouterErrorChainNEARUnknownChunk},
		{MessageContains("INVALID_SHARD_ID"), RouterErrorChainNEARInvalidShardID},
		{MessageContains("NOT_SYNCED_YET"), RouterErrorChainNEARNotSyncedYet},
	},
}

// ---------------------------------------------------------------------------
// Tier 1: Generic error mappings, partitioned by transport type.
// Evaluated in declaration order — first match wins.
// Matchers MUST be ordered most-specific first.
// ---------------------------------------------------------------------------

// INVARIANT: genericErrorMappings is populated at package-init time — first by
// this literal, then extended by the init() function below which appends
// shared HTTP status matchers per transport. After init returns it is never
// mutated, so ClassifyError can read it lock-free on the hot path. Any future
// dynamic registration must add explicit synchronisation.
var genericErrorMappings = map[TransportType][]errorMapping{
	TransportJsonRPC: {
		// --- Code-based matchers first (more precise than substring matching) ---

		// Standard JSON-RPC 2.0 codes
		{CodeEquals(-32601), RouterErrorNodeMethodNotFound},
		{CodeEquals(-32700), RouterErrorUserParseError},
		{CodeEquals(-32600), RouterErrorUserInvalidRequest},
		{CodeEquals(-32602), RouterErrorUserInvalidParams},
		{CodeEquals(-32603), RouterErrorNodeInternalError},

		// EIP-1474 server error codes
		{CodeEquals(-32001), RouterErrorNodeResourceNotFound},    // "Resource not found"
		{CodeEquals(-32002), RouterErrorNodeResourceUnavailable}, // "Resource unavailable"
		{CodeEquals(-32003), RouterErrorChainTxRejected},         // "Transaction rejected"
		{CodeEquals(-32004), RouterErrorNodeMethodNotSupported},  // "Method not supported"
		{CodeEquals(-32005), RouterErrorNodeLimitExceeded},       // "Limit exceeded"

		// --- Message-based matchers (for -32000 catch-all and codeless errors) ---

		// Unsupported methods (message-based fallback for nodes that use -32000).
		// Matchers here must be tightly scoped: NodeMethodNotFound carries
		// SubCategoryUnsupportedMethod (zero CU, no retry), so a false positive
		// silently stops retries and bills nothing. When in doubt, require the
		// literal word "method" to appear alongside the trigger phrase.
		{MessageContains("method not found"), RouterErrorNodeMethodNotFound},
		{MessageContains("method not supported"), RouterErrorNodeMethodNotSupported},
		{MessageContains("unknown method"), RouterErrorNodeMethodNotFound},
		{MessageContains("method does not exist"), RouterErrorNodeMethodNotFound},
		{MessageRegex(`(?i)method .* does not exist`), RouterErrorNodeMethodNotFound},
		// "<keyword> ... is not available" — require a method/rpc/endpoint/api keyword
		// in the same clause so chain-native messages like Solana's "Block is not
		// available for slot N" or generic "data is not available for block" don't
		// get pinned as unsupported method.
		{MessageRegex(`(?i)\b(?:method|rpc|endpoint|api)\b[^,;.]*\bis not available\b`), RouterErrorNodeMethodNotSupported},
		// "invalid method" — match only when the phrase is terminal, quoted, or
		// followed by "name"/colon. This rejects "invalid method argument",
		// "invalid method parameters", "invalid method signature", etc., which
		// would otherwise trip the zero-CU path on a user-input error.
		{MessageRegex(`(?i)invalid method(?:\s*$|\s*[:'"]|\s+name\b)`), RouterErrorNodeMethodNotFound},
		// Provider-disabled methods (e.g. QuickNode paid tier). Require "method"
		// or "rpc" near the "blocked" token so unrelated firewall/proxy messages
		// ("blocked external request") aren't misclassified.
		{MessageRegex(`(?i)blocked[^,]*\b(?:method|rpc)\b|(?i)\b(?:method|rpc)\b[^,]*\bblocked\b`), RouterErrorNodeMethodNotSupported},

		// --- User input validation (Layer D) ---
		// These must be ordered BEFORE the broader chain-transaction matchers so
		// "invalid address" / "invalid block number" don't leak into -32000 catch-all.
		// Matchers are deliberately tight: false positives would silently
		// classify real chain errors as user input errors (Retryable=false),
		// stopping retries that could have succeeded on another provider.
		//
		// Block format: "hex string without 0x prefix" is Geth's exact phrase;
		// "invalid block number" and "invalid block hash" are generic.
		{MessageContains("hex string without 0x prefix"), RouterErrorUserInvalidBlockFormat}, // Geth
		{MessageContains("invalid block number"), RouterErrorUserInvalidBlockFormat},
		{MessageContains("invalid block hash"), RouterErrorUserInvalidBlockFormat},
		// Address format: "bad address checksum" is Geth; bare "invalid address"
		// covers most EVM variants.
		{MessageContains("bad address checksum"), RouterErrorUserInvalidAddress},
		{MessageContains("invalid address"), RouterErrorUserInvalidAddress},
		// Hex encoding: "hex string has odd length" is Geth's exact phrase; bare
		// "invalid hex" is a common catch-all but placed last so the more
		// specific phrases (block, address) win first.
		{MessageContains("hex string has odd length"), RouterErrorUserInvalidHex}, // Geth
		{MessageContains("invalid hex"), RouterErrorUserInvalidHex},

		// Rate limiting
		{MessageContains("rate limit"), RouterErrorNodeRateLimited},
		{MessageContains("enhance_your_calm"), RouterErrorNodeRateLimited}, // HTTP/2 GOAWAY with ENHANCE_YOUR_CALM — server-side rate limit

		// Chain transaction errors — matchers cover Geth, Erigon, and Nethermind variants
		{MessageContains("nonce too low"), RouterErrorChainNonceTooLow},
		{MessageContains("nonce is too low"), RouterErrorChainNonceTooLow}, // Nethermind processor
		{MessageContains("nonce too high"), RouterErrorChainNonceTooHigh},
		{MessageContains("nonce is too high"), RouterErrorChainNonceTooHigh},                // Nethermind processor
		{MessageContains("insufficient funds"), RouterErrorChainInsufficientFunds},          // Geth + Erigon
		{MessageContains("insufficientfunds"), RouterErrorChainInsufficientFunds},           // Nethermind PascalCase
		{MessageContains("insufficient maxfeeper"), RouterErrorChainInsufficientFunds},      // Nethermind: "insufficient MaxFeePerGas..."
		{MessageContains("insufficient sender balance"), RouterErrorChainInsufficientFunds}, // Nethermind
		{MessageContains("intrinsic gas too low"), RouterErrorChainGasTooLow},               // Geth
		{MessageContains("intrinsicgas"), RouterErrorChainGasTooLow},                        // Erigon: "IntrinsicGas"
		{MessageContains("gas limit below intrinsic gas"), RouterErrorChainGasTooLow},       // Nethermind
		{MessageContains("exceeds block gas limit"), RouterErrorChainGasLimitExceeded},
		{MessageContains("replacement transaction underpriced"), RouterErrorChainTxReplacementUnderpriced},
		{MessageContains("could not replace existing tx"), RouterErrorChainTxReplacementUnderpriced}, // Erigon
		{MessageContains("transaction underpriced"), RouterErrorChainTxUnderpriced},                  // Geth
		{MessageContains("underpriced"), RouterErrorChainTxUnderpriced},                              // Erigon: bare "underpriced"
		{MessageContains("fee too low"), RouterErrorChainTxUnderpriced},                              // Erigon: "fee too low"
		{MessageContains("already known"), RouterErrorChainTxAlreadyKnown},                           // Geth + Erigon
		{MessageContains("alreadyknown"), RouterErrorChainTxAlreadyKnown},                            // Nethermind PascalCase
		{MessageContains("txpool is full"), RouterErrorChainMempoolFull},
		{MessageContains("max fee per gas less than block base fee"), RouterErrorChainMaxFeeBelowBase},
		// Node is still catching up to the chain head. NEAR carries its own
		// Tier-2 matcher (CHAIN_NEAR_NOT_SYNCED_YET), so this Tier-1 matcher
		// only fires for chains that surface a generic string message. The
		// phrase is tightly bounded to avoid matching unrelated "syncing"
		// contexts (e.g. a smart contract whose name contains "sync").
		{MessageContains("node is syncing"), RouterErrorNodeSyncing},
		{MessageContains("node is still syncing"), RouterErrorNodeSyncing},
		{MessageContains("catching up to the chain"), RouterErrorNodeSyncing},

		// Tx size limits — Geth/Erigon variants
		{MessageContains("oversized data"), RouterErrorChainTxTooLarge},           // Geth
		{MessageContains("transaction size exceeds"), RouterErrorChainTxTooLarge}, // Erigon
		{MessageContains("tx too large"), RouterErrorChainTxTooLarge},
		// Invalid signature — Tier-2 Solana (code 3306) runs first and wins
		// for the Solana family, so this generic matcher only fires for
		// non-Solana chains that use a free-form error message.
		{MessageContains("invalid signature"), RouterErrorChainInvalidSignature},
		{MessageContains("signature verification failed"), RouterErrorChainInvalidSignature},

		// Chain execution errors
		{MessageContains("execution reverted"), RouterErrorChainExecutionReverted},
		{MessageContains("out of gas"), RouterErrorChainOutOfGas},
		{MessageContains("stack limit reached"), RouterErrorChainStackOverflow},
		{MessageContains("invalid opcode"), RouterErrorChainInvalidOpcode},
		{MessageContains("write protection"), RouterErrorChainWriteProtection},
		// EIP-170 contract bytecode size limit. Geth/Erigon emit this as
		// "max code size exceeded" during contract creation.
		{MessageContains("max code size exceeded"), RouterErrorChainContractSizeExceeded},
		// Polygon zkEVM prover exceeded the circuit counter budget
		// (arithmetic, keccak, storage, etc.). zkEVMs speak EVM JSON-RPC
		// so this matcher sits in the generic Tier-1 block rather than a
		// family-scoped Tier-2 entry.
		{MessageContains("out of counters"), RouterErrorChainZkEVMOutOfCounters},

		// Chain state/data errors
		{MessageContains("missing trie node"), RouterErrorChainStatePruned},
		{MessageContains("historical state"), RouterErrorChainStatePruned},
		{MessageContains("block not found"), RouterErrorChainBlockNotFound},
		{MessageRegex(`(?i)block #?\w+ not found`), RouterErrorChainBlockNotFound},
		{MessageContains("transaction not found"), RouterErrorChainTxNotFound},  // some nodes return an error instead of null
		{MessageContains("receipt not found"), RouterErrorChainReceiptNotFound}, // Cosmos-EVM variant
		{MessageContains("response is too big"), RouterErrorChainLogResponseTooLarge},
		{MessageContains("exceeded max limit"), RouterErrorChainLogResponseTooLarge},

		// Truncated node responses — historically mapped to NODE_INTERNAL_ERROR
		// but this is a transport-layer symptom (connection closed mid-body /
		// reset by proxy) rather than the node signaling an application error.
		// Classifying it alongside PROTOCOL_CONNECTION_RESET lets endpoint-
		// health tracking treat it as a retryable connection failure, which
		// matches the production traces observed in commit 3136d4f35.
		{MessageContains("unexpected end of JSON input"), RouterErrorConnectionReset},

		// Node server/generic errors — broadest matchers last
		{MessageContains("all attempts exhausted"), RouterErrorNodeServerError},
		{CodeEquals(-32000), RouterErrorNodeServerError},

		// HTTP status matchers are appended by init() via httpStatusMessageMappings()
	},

	TransportREST: {
		// Message-based matchers for common REST error patterns
		{MessageContains("endpoint not found"), RouterErrorNodeEndpointNotFound},
		{MessageContains("route not found"), RouterErrorNodeEndpointNotFound},
		{MessageContains("path not found"), RouterErrorNodeEndpointNotFound},
		{MessageContains("method not allowed"), RouterErrorNodeMethodNotAllowed},
		// CodeEquals and HTTPStatusContains matchers are appended by init()
		// via httpStatusCodeMappings() and httpStatusMessageMappings()
	},

	TransportGRPC: {
		// gRPC status code matchers (codes from google.golang.org/grpc/codes)
		//
		// A code earns a row here only when its verdict is the SAME at every
		// endpoint. Rows are what let the direct-RPC availability gate
		// (rpcsmartrouter.shouldFailSessionForResult) exempt a status from
		// demoting the endpoint, and an unregistered code scores — which is the
		// right default for "we have not catalogued this failure".
		//
		// codes.InvalidArgument is the one caller-fault code in the set: gRPC
		// defines it as arguments "problematic regardless of the state of the
		// system", so every endpoint rejects the same malformed request
		// identically. Left unregistered it classified UNKNOWN_ERROR, and a burst
		// of malformed client requests demoted the whole healthy pairing toward
		// the selection floor — blocklisting on the 16th consecutive one
		// (MAG-2549). USER_INVALID_PARAMS is Retryable=false, which both stops the
		// pointless retry and keeps the gate off the endpoint.
		//
		// Reach: this table is keyed by TRANSPORT, not by relay path, so the row also
		// applies on the provider-based gRPC path via chainlib.GrpcErrorHandler
		// .HandleNodeError — an INVALID_ARGUMENT becomes non-retryable there too. That
		// is intended and follows from gRPC's own definition of the code, but it is
		// wider than "direct RPC only" and should be read as such.
		{GRPCCodeEquals(3), RouterErrorUserInvalidParams}, // codes.InvalidArgument
		// codes.NotFound and codes.OutOfRange are the ordinary "I do not have this"
		// outcomes of a Cosmos or Sui gRPC query — a missing object, an account that
		// was never funded, a height below the node's pruning window. They are the
		// most COMMON non-OK codes on those chains, not a failure mode.
		//
		// They need a row for a reason the other two do not: sendGRPCRelay marks every
		// non-OK status IsNodeError, so without one they fall through to UNKNOWN_ERROR
		// and the availability gate scores them — one demotion per retry, on every
		// endpoint asked, for a query whose answer is simply "no". NODE_DATA_NOT_HELD
		// is Retryable=true so the pruned-node-to-archive-node retry survives, and
		// SubCategoryDataScope is what keeps the gate off an endpoint that answered
		// truthfully about its own data scope.
		{GRPCCodeEquals(5), RouterErrorNodeDataNotHeld},         // codes.NotFound
		{GRPCCodeEquals(11), RouterErrorNodeDataNotHeld},        // codes.OutOfRange
		{GRPCCodeEquals(12), RouterErrorNodeUnimplemented},      // codes.Unimplemented
		{GRPCCodeEquals(14), RouterErrorNodeServiceUnavailable}, // codes.Unavailable
		// Deliberately NOT registered, each for its own reason — do not add them
		// as a block, which is how `Code >= 13` went wrong in the first place:
		//   4  DeadlineExceeded  - this endpoint was too slow; another may not be.
		//                          Demoting it is the correct signal.
		//   7  PermissionDenied  - credentials are configured per endpoint, so one
		//   16 Unauthenticated     rejecting ours is unusable to us and should demote.
		//   8  ResourceExhausted - ambiguous in the gRPC spec (per-user quota vs.
		//                          out of disk). The "rate limit" message row below
		//                          catches the quota case without asserting the code
		//                          alone means "healthy but busy".
		//   1  Canceled          - a LOCAL cancellation never reaches this table;
		//                          handleGRPCError resolves it structurally. One that
		//                          does reach here is remote and unproven.
		// Message-based matchers for gRPC errors conveyed without status codes
		{MessageContains("rate limit"), RouterErrorNodeRateLimited},
		{MessageContains("enhance_your_calm"), RouterErrorNodeRateLimited}, // HTTP/2 GOAWAY ENHANCE_YOUR_CALM
		{MessageContains("unimplemented"), RouterErrorNodeUnimplemented},
		{MessageContains("not implemented"), RouterErrorNodeUnimplemented},
		{MessageContains("service not found"), RouterErrorNodeUnimplemented},
	},
}

// ---------------------------------------------------------------------------
// Shared HTTP status mappings — appended to JSON-RPC and REST at init time
// ---------------------------------------------------------------------------

// httpStatusCodeMappings returns CodeEquals matchers for common HTTP error status codes.
// These are used for REST transport where the error code is the HTTP status code itself.
func httpStatusCodeMappings() []errorMapping {
	return []errorMapping{
		{CodeEquals(401), RouterErrorNodeUnauthorized},
		{CodeEquals(404), RouterErrorNodeEndpointNotFound},
		{CodeEquals(405), RouterErrorNodeMethodNotAllowed},
		{CodeEquals(413), RouterErrorUserRequestTooLarge},
		{CodeEquals(429), RouterErrorNodeRateLimited},
		{CodeEquals(500), RouterErrorNodeInternalError},
		// 501 Not Implemented: node lacks this method/endpoint (e.g. Cosmos REST
		// gRPC-gateway). Non-retryable node error, not a transient server failure.
		{CodeEquals(501), RouterErrorNodeUnimplemented},
		{CodeEquals(502), RouterErrorNodeBadGateway},
		{CodeEquals(503), RouterErrorNodeServiceUnavailable},
		{CodeEquals(504), RouterErrorNodeGatewayTimeout},
		// Cloudflare custom 5xx errors
		{CodeEquals(520), RouterErrorNodeServerError},    // Web server returned unknown error
		{CodeEquals(521), RouterErrorNodeServerError},    // Web server is down
		{CodeEquals(522), RouterErrorNodeGatewayTimeout}, // Connection timed out
		{CodeEquals(523), RouterErrorNodeServerError},    // Origin is unreachable
		{CodeEquals(524), RouterErrorNodeGatewayTimeout}, // A timeout occurred
		{CodeEquals(525), RouterErrorNodeServerError},    // SSL handshake failed
		{CodeEquals(526), RouterErrorNodeServerError},    // Invalid SSL certificate
		{CodeEquals(527), RouterErrorNodeServerError},    // Railgun error
		{CodeEquals(530), RouterErrorNodeServerError},    // Origin DNS error
	}
}

// httpStatusMessageMappings returns HTTPStatusContains matchers for common HTTP error status codes.
// These match status codes appearing as substrings in error messages (e.g., "HTTP status 429").
func httpStatusMessageMappings() []errorMapping {
	return []errorMapping{
		{HTTPStatusContains(401), RouterErrorNodeUnauthorized},
		{HTTPStatusContains(404), RouterErrorNodeEndpointNotFound},
		{HTTPStatusContains(405), RouterErrorNodeMethodNotAllowed},
		{HTTPStatusContains(413), RouterErrorUserRequestTooLarge},
		{HTTPStatusContains(429), RouterErrorNodeRateLimited},
		{HTTPStatusContains(500), RouterErrorNodeInternalError},
		// 501 Not Implemented: node lacks this method/endpoint. Non-retryable.
		{HTTPStatusContains(501), RouterErrorNodeUnimplemented},
		{HTTPStatusContains(502), RouterErrorNodeBadGateway},
		{HTTPStatusContains(503), RouterErrorNodeServiceUnavailable},
		{HTTPStatusContains(504), RouterErrorNodeGatewayTimeout},
		// Cloudflare custom 5xx errors
		{HTTPStatusContains(520), RouterErrorNodeServerError},
		{HTTPStatusContains(521), RouterErrorNodeServerError},
		{HTTPStatusContains(522), RouterErrorNodeGatewayTimeout},
		{HTTPStatusContains(523), RouterErrorNodeServerError},
		{HTTPStatusContains(524), RouterErrorNodeGatewayTimeout},
		{HTTPStatusContains(525), RouterErrorNodeServerError},
		{HTTPStatusContains(526), RouterErrorNodeServerError},
		{HTTPStatusContains(527), RouterErrorNodeServerError},
		{HTTPStatusContains(530), RouterErrorNodeServerError},
	}
}

// init is the ONLY place allowed to mutate genericErrorMappings. It runs before
// any reader can observe the map, so the runtime invariant ("read-only after
// package init") holds. Do not add mutations outside this function.
func init() {
	// Append shared HTTP status message matchers to JSON-RPC transport
	genericErrorMappings[TransportJsonRPC] = append(genericErrorMappings[TransportJsonRPC], httpStatusMessageMappings()...)

	// Append both CodeEquals and HTTPStatusContains matchers to REST transport
	genericErrorMappings[TransportREST] = append(genericErrorMappings[TransportREST], httpStatusCodeMappings()...)
	genericErrorMappings[TransportREST] = append(genericErrorMappings[TransportREST], httpStatusMessageMappings()...)

	// Append HTTPStatusContains matchers to gRPC transport — HTTP status codes can appear
	// in gRPC error messages when the underlying transport is HTTP (e.g. provider relay errors)
	genericErrorMappings[TransportGRPC] = append(genericErrorMappings[TransportGRPC], httpStatusMessageMappings()...)
}

// ---------------------------------------------------------------------------
// ClassifyError — the central classification function
// ---------------------------------------------------------------------------

// classifySubCategoryAcrossTransports tries every transport, returns the
// SubCategory of the first non-Unknown classification, or SubCategoryNone.
// Used by IsUnsupportedMethodError and IsUserInputError below.
//
// TRANSPORT ORDER IS SEMANTICALLY SIGNIFICANT. We try JSON-RPC first because
// it is the most common transport in production, then REST, then gRPC. The
// function returns on the first non-Unknown match — it does NOT aggregate
// across transports. Callers that know their transport exactly should call
// ClassifyError directly and avoid this helper; it is intended only for the
// narrow case where the transport is genuinely unknown.
//
// Edge case: a chain that returns gRPC status codes inside HTTP response
// bodies (e.g. grpc-web proxies) may match under the JSON-RPC bucket first
// with a subtly wrong classification. If that becomes a production issue,
// prefer threading the real transport through from the call site over
// changing the iteration order here.
func classifySubCategoryAcrossTransports(chainID string, statusCode int, message string) ErrorSubCategory {
	family := ChainFamilyUnknown
	if chainID != "" {
		family = GetChainFamilyOrDefault(chainID)
	}
	for _, transport := range []TransportType{TransportJsonRPC, TransportREST, TransportGRPC} {
		if c := ClassifyError(nil, family, transport, statusCode, message); c != RouterErrorUnknown {
			return c.SubCategory
		}
	}
	return SubCategoryNone
}

// IsUnsupportedMethodError returns true when the error identified by statusCode and
// message is classified as an unsupported method (the node does not recognise the
// method at all).
//
// chainID is used to consult chain-specific Tier-2 matchers first, so chain-native
// messages like Solana's "Block is not available for slot N" don't accidentally
// match a broad Tier-1 substring rule (e.g. "is not available") and get pinned as
// unsupported-method — which would skip retries and zero out CU charging.
// Pass "" when the chain is genuinely unknown; Tier-2 is then skipped.
func IsUnsupportedMethodError(chainID string, statusCode int, message string) bool {
	return classifySubCategoryAcrossTransports(chainID, statusCode, message).IsUnsupportedMethod()
}

// IsNonRetryableNodeError returns true when the classified RouterError for the
// given node-error response has Retryable=false.
func IsNonRetryableNodeError(chainID string, statusCode int, message string) bool {
	family := ChainFamilyUnknown
	if chainID != "" {
		family = GetChainFamilyOrDefault(chainID)
	}
	for _, transport := range []TransportType{TransportJsonRPC, TransportREST, TransportGRPC} {
		c := ClassifyError(nil, family, transport, statusCode, message)
		if c == nil || c == RouterErrorUnknown {
			continue
		}
		return !c.Retryable
	}
	return false
}

// IsNonRetryableNodeErrorWithContext is the variant of IsNonRetryableNodeError
// for call sites that already know the chain family and transport exactly.
func IsNonRetryableNodeErrorWithContext(family ChainFamily, transport TransportType, statusCode int, message string) bool {
	c := ClassifyError(nil, family, transport, statusCode, message)
	if c == nil || c == RouterErrorUnknown {
		return false
	}
	return !c.Retryable
}

// NodeErrorClassification aggregates the policy flags derived from a
// single registry lookup on a node-error response:
//   - IsNonRetryable: registry entry is Retryable=false (hard stop for retries).
//   - IsUnsupportedMethod: SubCategory-based predicate used by the consumer
//     to apply the zero-CU carve-out and response caching.
//   - IsRateLimited: SubCategoryRateLimit, i.e. "the endpoint is healthy but
//     busy". Health- and QoS-tracking callers must apply backoff but must NOT
//     mark the endpoint unhealthy — see ErrorSubCategory's declaration.
//
// The three are orthogonal axes, not a hierarchy. IsNonRetryable answers
// "would retrying elsewhere help", the SubCategory flags answer "whose fault
// is this". NODE_RATE_LIMITED (2005) is retryable AND rate-limited;
// NODE_LIMIT_EXCEEDED (2011) is non-retryable AND rate-limited. A caller that
// needs "is this the node's fault" must read the fault axis, not infer it from
// retryability.
type NodeErrorClassification struct {
	IsNonRetryable      bool
	IsUnsupportedMethod bool
	IsRateLimited       bool
	// IsDataScope is the third fault axis: the endpoint does not hold the
	// requested data. Independent of IsNonRetryable — these errors ARE retryable
	// (an archive node may answer) but must not demote the endpoint that told us
	// the truth. See ErrorSubCategory.IsDataScope.
	IsDataScope bool
}

// ClassifyNodeErrorForRetry runs ClassifyError exactly once and derives the
// policy flags consumed by the consumer's retry decision, its CU/caching
// carve-outs, and the direct-RPC availability-scoring gate.
//
// An unmatched error yields the zero value — every flag false. That is a
// deliberate absence of information, not a positive "retryable, node's fault"
// verdict: retrying unknowns is the documented default (see error_codes.go),
// and callers that treat a false flag as an affirmative classification will
// misread every novel error body.
func ClassifyNodeErrorForRetry(family ChainFamily, transport TransportType, errorCode int, message string) NodeErrorClassification {
	c := ClassifyError(nil, family, transport, errorCode, message)
	if c == nil || c == RouterErrorUnknown {
		return NodeErrorClassification{}
	}
	return NodeErrorClassification{
		IsNonRetryable:      !c.Retryable,
		IsUnsupportedMethod: c.SubCategory.IsUnsupportedMethod(),
		IsRateLimited:       c.SubCategory.IsRateLimit(),
		IsDataScope:         c.SubCategory.IsDataScope(),
	}
}

// ApiInterfaceToTransport maps a spec API interface string (jsonrpc,
// tendermintrpc, rest, grpc) to the TransportType used by the error registry.
func ApiInterfaceToTransport(apiInterface string) TransportType {
	switch apiInterface {
	case "rest":
		return TransportREST
	case "grpc":
		return TransportGRPC
	default:
		return TransportJsonRPC
	}
}

// ClassifyError classifies an error into a RouterError for internal use (logging, metrics, endpoint health).
// The original error always passes through unchanged to the user (transparent hop).
//
// Parameters:
//   - connectionError: pre-detected connection-level error (timeout, refused, etc.), or nil
//   - chainFamily: the chain family for Tier 2 lookups, use -1 if unknown
//   - transport: the transport type for Tier 1 generic matcher partitioning
//   - errorCode: the numeric error code (e.g., JSON-RPC error code), or 0 if not applicable
//   - errorMessage: the error message string for substring/regex matching
func ClassifyError(connectionError *RouterError, chainFamily ChainFamily, transport TransportType, errorCode int, errorMessage string) *RouterError {
	// Step 0: If caller already identified a connection-level error, use it
	if connectionError != nil {
		return connectionError
	}

	// Lower the message once per classification. Tier-1 has ~60 case-insensitive
	// substring matchers; re-lowering for each one allocates a fresh KB-scale copy
	// on the hot retry path. The loweredMessageMatcher fast path lets matchers
	// consume the pre-lowered form directly.
	loweredMessage := strings.ToLower(errorMessage)

	// Step 1: Check chain-specific mappings (Tier 2)
	if chainMappings, ok := chainErrorMappings[chainFamily]; ok {
		for _, mapping := range chainMappings {
			if matchMapping(mapping.Matcher, errorCode, errorMessage, loweredMessage) {
				return mapping.RouterError
			}
		}
	}

	// Step 2: Fall back to generic semantic mappings (Tier 1), scoped by transport
	if transportMappings, ok := genericErrorMappings[transport]; ok {
		for _, mapping := range transportMappings {
			if matchMapping(mapping.Matcher, errorCode, errorMessage, loweredMessage) {
				return mapping.RouterError
			}
		}
	}

	// Step 3: Unknown
	return RouterErrorUnknown
}

// matchMapping dispatches to the loweredMessageMatcher fast path when available,
// falling back to the standard ErrorMatcher interface otherwise.
func matchMapping(m ErrorMatcher, errorCode int, errorMessage, loweredMessage string) bool {
	if fast, ok := m.(loweredMessageMatcher); ok {
		return fast.matchesLowered(errorCode, loweredMessage)
	}
	return m.Matches(errorCode, errorMessage)
}

// DetectConnectionError inspects err for connection-level failures and returns the
// corresponding RouterError, or nil if the error is not connection-related.
// This is the single place for connection detection — callers pass the result as the
// connectionError argument to ClassifyError.
//
// Detection happens in three ordered layers:
//  1. Structured checks via errors.Is / errors.As (context, net.Error timeouts).
//  2. String-fallback table (detectConnectionErrorFromString) for wrapped errors
//     that lose their sentinel chain (e.g. fmt.Errorf without %w).
//  3. net.OpError unwrap for raw syscall errno codes.
//
// The string fallback is deliberately second, so structured detection wins when
// it can. Within the string fallback, the match order is explicit in the
// stringConnectionFallbacks data table to make precedence easy to audit.
func DetectConnectionError(err error) *RouterError {
	if err == nil {
		return nil
	}
	// Layer 1: structured sentinel checks
	if errors.Is(err, context.Canceled) {
		return RouterErrorContextCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RouterErrorContextDeadline
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RouterErrorConnectionTimeout
	}

	// Layer 2: string fallback for errors wrapped without %w.
	if le := detectConnectionErrorFromString(strings.ToLower(err.Error())); le != nil {
		return le
	}

	// Layer 3: net.OpError with a raw syscall errno.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		var syscallErr *syscall.Errno
		if errors.As(opErr.Err, &syscallErr) {
			switch *syscallErr {
			case syscall.ECONNREFUSED:
				return RouterErrorConnectionRefused
			case syscall.ECONNRESET:
				return RouterErrorConnectionReset
			case syscall.ENETUNREACH, syscall.EHOSTUNREACH:
				return RouterErrorNetworkUnreachable
			}
		}
	}
	return nil
}

// stringConnectionFallback is one row of the string-fallback table used by
// detectConnectionErrorFromString. The first matching row wins, so ordering
// matters and is audited in-place.
type stringConnectionFallback struct {
	// substrings that must all appear in the lowered message for the row to match.
	mustContain []string
	// substrings that, if any are present, disqualify the row. Used to carve
	// exceptions out of a broad match (e.g. GOAWAY except ENHANCE_YOUR_CALM).
	mustNotContain []string
	// router error returned when the row matches.
	result *RouterError
}

func (r stringConnectionFallback) matches(msg string) bool {
	for _, sub := range r.mustContain {
		if !strings.Contains(msg, sub) {
			return false
		}
	}
	for _, sub := range r.mustNotContain {
		if strings.Contains(msg, sub) {
			return false
		}
	}
	return true
}

// stringConnectionFallbacks is the precedence-ordered table used when the
// structured errors.Is/errors.As checks did not identify a connection error.
//
// Ordering rules:
//  1. Context cancel/deadline rows come first but are guarded so remote gRPC
//     status errors ("rpc error: code = DeadlineExceeded desc = ...") do NOT
//     match here — those carry a *remote* deadline and must fall through to
//     transport-scoped Tier-1 classification.
//  2. HTTP/2 GOAWAY is checked before the broader "connection reset" catch so
//     the ENHANCE_YOUR_CALM carve-out lands on the right row.
//  3. Refused / unreachable / reset / termination are mutually exclusive in
//     practice — each maps a distinct proxy/sidecar shape — but ordering is
//     preserved explicitly here so a future contributor doesn't have to infer
//     it from control-flow order.
//
// INVARIANT: written at package-init time (declaration), read-only thereafter.
var stringConnectionFallbacks = []stringConnectionFallback{
	// Remote gRPC status errors start with "rpc error"; those carry a *remote*
	// deadline/cancel and must not be classified as a local context error.
	// The guard is encoded as a mustNotContain on the rpc-error prefix marker.
	//
	// The guard is deliberately the BARE substring "rpc error", not the fuller
	// "rpc error: code =" prefix. Widening it to the full prefix was tried and
	// reverted (MAG-2687): routersession.GRPCStatusError renders as
	// "gRPC error 1: context canceled", which lowercased contains "rpc error"
	// inside the word "gRPC" but NOT "rpc error: code =". That wrapper is built
	// for REMOTE statuses — a cancel the *endpoint* reported — so admitting it
	// here labels an upstream fault as local orchestration and, because
	// PROTOCOL_CONTEXT_CANCELED is Retryable=false, silently suppresses the
	// retry that would have reached a healthy endpoint.
	//
	// Nothing is lost by keeping the guard broad. A cancellation the router
	// itself caused never depends on this table: GRPCDirectRPCConnection
	// .handleGRPCError returns a nil response and an error wrapping the real
	// context.Canceled sentinel, which DetectConnectionError resolves in its
	// layer-1 errors.Is check well before the string fallback runs. This table
	// only ever sees errors whose sentinel chain was already lost, and for those
	// "contains an rpc-error marker" is the conservative read.
	{
		mustContain:    []string{"context deadline exceeded"},
		mustNotContain: []string{"rpc error"},
		result:         RouterErrorContextDeadline,
	},
	{
		mustContain:    []string{"context canceled"},
		mustNotContain: []string{"rpc error"},
		result:         RouterErrorContextCanceled,
	},
	// HTTP/2 GOAWAY closes the whole connection. Exclude ENHANCE_YOUR_CALM —
	// that's a server-side rate-limit signal handled by the transport matchers.
	{
		mustContain:    []string{"goaway"},
		mustNotContain: []string{"enhance_your_calm"},
		result:         RouterErrorConnectionReset,
	},
	// HTTP/2 RST_STREAM appears as "stream error: stream ID ...; ...".
	// This is a narrow hazard — "stream error" could in principle appear in
	// unrelated messages — but in practice the gRPC/HTTP2 stack is the only
	// known producer, and losing this signal would break retry of RST_STREAM.
	{mustContain: []string{"stream error"}, result: RouterErrorConnectionReset},
	// Proxy / sidecar variants — Envoy wraps upstream failures with verbose
	// bodies that still contain these phrases. Order is not load-bearing here
	// because the phrases are mutually exclusive in practice, but keeping the
	// explicit order avoids accidental churn during future reordering.
	{mustContain: []string{"connection refused"}, result: RouterErrorConnectionRefused},
	{mustContain: []string{"network is unreachable"}, result: RouterErrorNetworkUnreachable},
	{mustContain: []string{"host is unreachable"}, result: RouterErrorNetworkUnreachable},
	{mustContain: []string{"no route to host"}, result: RouterErrorNetworkUnreachable},
	{mustContain: []string{"connection reset"}, result: RouterErrorConnectionReset},
	// Envoy "connection termination" — proxy/sidecar closed an established
	// upstream stream (distinct from "connection refused" which is a connect
	// failure before any stream was established).
	{mustContain: []string{"connection termination"}, result: RouterErrorConnectionReset},
}

// detectConnectionErrorFromString walks stringConnectionFallbacks in order and
// returns the first matching result, or nil if no row matches. msg must
// already be lowercased by the caller.
func detectConnectionErrorFromString(msg string) *RouterError {
	for _, row := range stringConnectionFallbacks {
		if row.matches(msg) {
			return row.result
		}
	}
	return nil
}

// IsClientCancellation reports whether err represents a request that was
// cancelled by the client / relay orchestration, as opposed to an upstream
// (provider/endpoint) fault. Two scenarios reach this path:
//
//  1. Relay race: multiple goroutines race in parallel. When one wins, the
//     parent context's cancel() fires and the losing goroutines observe
//     context.Canceled as their result — no provider is at fault.
//  2. Client disconnect: the upstream caller closed the connection before we
//     responded, producing context.Canceled on the in-flight request.
//
// The rule is: the error must be context.Canceled AND the context itself must
// now carry an error. A standalone context.Canceled without ctx.Err() would
// be suspicious — it should never happen in practice, but the conjunction
// guards against misclassifying a provider-emitted "context canceled" string
// as a client cancellation.
//
// Call this at endpoint-health / refusal-counter decision points instead of
// hand-rolling an errors.Is(err, context.Canceled) check — having one rule in
// one place avoids drift between the consumer session layer, the smart router
// health tracker, and the connection-refusal counter.
func IsClientCancellation(err error, ctx context.Context) bool {
	if err == nil || ctx == nil {
		return false
	}
	return errors.Is(err, context.Canceled) && ctx.Err() != nil
}
