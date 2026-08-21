package common

// ---------------------------------------------------------------------------
// Layer A: Protocol Errors (Internal) — range 1000-1999
// Errors raised from within the router itself — not from nodes or chains.
// ---------------------------------------------------------------------------

var (
	// Connection errors (1001-1009)
	RouterErrorConnectionTimeout = registerError(&RouterError{
		Code: 1001, Name: "PROTOCOL_CONNECTION_TIMEOUT", Category: CategoryInternal,
		Description: "Network operation timed out connecting to provider", Retryable: true,
	})
	RouterErrorConnectionRefused = registerError(&RouterError{
		Code: 1002, Name: "PROTOCOL_CONNECTION_REFUSED", Category: CategoryInternal,
		Description: "Provider connection refused", Retryable: true,
	})
	RouterErrorDNSFailure = registerError(&RouterError{
		Code: 1003, Name: "PROTOCOL_DNS_FAILURE", Category: CategoryInternal,
		Description: "DNS resolution failed", Retryable: true,
	})
	RouterErrorTLSMismatch = registerError(&RouterError{
		Code: 1004, Name: "PROTOCOL_TLS_MISMATCH", Category: CategoryInternal,
		Description: "HTTP/HTTPS protocol mismatch", Retryable: false,
	})
	RouterErrorConnectionReset = registerError(&RouterError{
		Code: 1005, Name: "PROTOCOL_CONNECTION_RESET", Category: CategoryInternal,
		Description: "Connection reset by peer", Retryable: true,
	})
	RouterErrorConnectionClosed = registerError(&RouterError{
		Code: 1006, Name: "PROTOCOL_CONNECTION_CLOSED", Category: CategoryInternal,
		Description: "Connection closed (EOF)", Retryable: true,
	})
	RouterErrorNetworkUnreachable = registerError(&RouterError{
		Code: 1009, Name: "PROTOCOL_NETWORK_UNREACHABLE", Category: CategoryInternal,
		Description: "Network or host unreachable (no route)", Retryable: true,
	})
	RouterErrorContextDeadline = registerError(&RouterError{
		Code: 1007, Name: "PROTOCOL_CONTEXT_DEADLINE", Category: CategoryInternal,
		Description: "Caller's context.Context deadline expired before the relay completed (can fire at any layer — consumer request timeout, processing timeout, subrequest timeout)", Retryable: true,
	})
	RouterErrorContextCanceled = registerError(&RouterError{
		Code: 1008, Name: "PROTOCOL_CONTEXT_CANCELED", Category: CategoryInternal,
		Description: "Request context was canceled (client disconnect or relay race resolved)", Retryable: false,
	})

	// Provider availability (1010-1019)
	RouterErrorNoProviders = registerError(&RouterError{
		Code: 1010, Name: "PROTOCOL_NO_PROVIDERS", Category: CategoryInternal,
		Description: "No providers/pairings available", Retryable: false,
	})
	RouterErrorAllEndpointsDisabled = registerError(&RouterError{
		Code: 1011, Name: "PROTOCOL_ALL_ENDPOINTS_DISABLED", Category: CategoryInternal,
		Description: "All provider endpoints disabled", Retryable: false,
	})
	RouterErrorProviderUnavailable = registerError(&RouterError{
		Code: 1012, Name: "PROTOCOL_PROVIDER_UNAVAILABLE", Category: CategoryInternal,
		Description: "Provider service unavailable (gRPC UNAVAILABLE)", Retryable: true,
	})
	RouterErrorProviderAborted = registerError(&RouterError{
		Code: 1013, Name: "PROTOCOL_PROVIDER_ABORTED", Category: CategoryInternal,
		Description: "Provider aborted (gRPC ABORTED)", Retryable: true,
	})
	RouterErrorProviderDataLoss = registerError(&RouterError{
		Code: 1014, Name: "PROTOCOL_PROVIDER_DATA_LOSS", Category: CategoryInternal,
		Description: "Provider data loss (gRPC DATA_LOSS)", Retryable: true,
	})
	RouterErrorInsufficientProviders = registerError(&RouterError{
		Code: 1015, Name: "PROTOCOL_INSUFFICIENT_PROVIDERS", Category: CategoryInternal,
		Description: "Insufficient providers available for addon or cross-validation", Retryable: false,
	})

	// Rate limiting / CU (1020-1029)
	RouterErrorRateLimited = registerError(&RouterError{
		Code: 1020, Name: "PROTOCOL_RATE_LIMITED", Category: CategoryInternal,
		SubCategory: SubCategoryRateLimit,
		Description: "Smart-Router-side rate limit exceeded", Retryable: false,
	})
	RouterErrorMaxCUExceeded = registerError(&RouterError{
		Code: 1021, Name: "PROTOCOL_MAX_CU_EXCEEDED", Category: CategoryInternal,
		Description: "Maximum compute units exceeded for session", Retryable: false,
	})
	RouterErrorBatchSizeExceeded = registerError(&RouterError{
		Code: 1022, Name: "PROTOCOL_BATCH_SIZE_EXCEEDED", Category: CategoryInternal,
		Description: "Batch request size exceeded limit", Retryable: false,
	})
	RouterErrorCUMismatch = registerError(&RouterError{
		Code: 1023, Name: "PROTOCOL_CU_MISMATCH", Category: CategoryInternal,
		Description: "CU accounting inconsistency or security violation", Retryable: false,
	})

	// Session errors (1030-1039)
	RouterErrorSessionNotFound = registerError(&RouterError{
		Code: 1030, Name: "PROTOCOL_SESSION_NOT_FOUND", Category: CategoryInternal,
		Description: "Session does not exist", Retryable: false,
	})
	RouterErrorEpochMismatch = registerError(&RouterError{
		Code: 1031, Name: "PROTOCOL_EPOCH_MISMATCH", Category: CategoryInternal,
		Description: "Epoch mismatch or too old", Retryable: false,
	})
	RouterErrorConsumerBlocked = registerError(&RouterError{
		Code: 1032, Name: "PROTOCOL_CONSUMER_BLOCKED", Category: CategoryInternal,
		Description: "Consumer is blocklisted", Retryable: false,
	})
	RouterErrorConsumerNotRegistered = registerError(&RouterError{
		Code: 1033, Name: "PROTOCOL_CONSUMER_NOT_REGISTERED", Category: CategoryInternal,
		Description: "Consumer not registered", Retryable: false,
	})
	RouterErrorRelayNumberMismatch = registerError(&RouterError{
		Code: 1034, Name: "PROTOCOL_RELAY_NUMBER_MISMATCH", Category: CategoryInternal,
		Description: "Relay number mismatch", Retryable: false,
	})
	RouterErrorSessionOutOfSync = registerError(&RouterError{
		Code: 1035, Name: "PROTOCOL_SESSION_OUT_OF_SYNC", Category: CategoryInternal,
		Description: "Session out of sync", Retryable: false,
	})
	RouterErrorInvalidRelayRequest = registerError(&RouterError{
		Code: 1036, Name: "PROTOCOL_INVALID_RELAY_REQUEST", Category: CategoryInternal,
		Description: "Relay request validation failed (wrong provider/specID/chainID/seen block/content hash)", Retryable: false,
	})
	RouterErrorRequestBlockMismatch = registerError(&RouterError{
		Code: 1037, Name: "PROTOCOL_REQUEST_BLOCK_MISMATCH", Category: CategoryInternal,
		Description: "Block height mismatch between consumer request and provider state", Retryable: true,
	})
	RouterErrorSessionAccountingFailed = registerError(&RouterError{
		Code: 1038, Name: "PROTOCOL_SESSION_ACCOUNTING_FAILED", Category: CategoryInternal,
		Description: "Session accounting (OnSessionDone/OnSessionFailure) failed", Retryable: false,
	})
	RouterErrorSubscriptionCleanupFailed = registerError(&RouterError{
		Code: 1039, Name: "PROTOCOL_SUBSCRIPTION_CLEANUP_FAILED", Category: CategoryInternal,
		Description: "Subscription consumer removal (RemoveConsumer) failed", Retryable: false,
	})

	// Verification / consensus (1040-1049)
	RouterErrorFinalizationError = registerError(&RouterError{
		Code: 1040, Name: "PROTOCOL_FINALIZATION_ERROR", Category: CategoryInternal,
		Description: "Provider finalization data incorrect", Retryable: true,
	})
	RouterErrorConsistencyError = registerError(&RouterError{
		Code: 1041, Name: "PROTOCOL_CONSISTENCY_ERROR", Category: CategoryInternal,
		Description: "Response consistency validation failed", Retryable: true,
	})
	RouterErrorHashConsensusError = registerError(&RouterError{
		Code: 1042, Name: "PROTOCOL_HASH_CONSENSUS_ERROR", Category: CategoryInternal,
		Description: "Conflicting response hashes detected", Retryable: true,
	})
	RouterErrorNoResponseTimeout = registerError(&RouterError{
		Code: 1043, Name: "PROTOCOL_NO_RESPONSE_TIMEOUT", Category: CategoryInternal,
		Description: "Relay race timeout — no provider returned any response (success or error) within the protocol-level cutoff. Distinct from PROTOCOL_CONTEXT_DEADLINE (1007) which is the caller's own context expiring.", Retryable: true,
	})
	RouterErrorRelayProcessingFailed = registerError(&RouterError{
		Code: 1044, Name: "PROTOCOL_RELAY_PROCESSING_FAILED", Category: CategoryInternal,
		Description: "Relay response processing failed on consumer side", Retryable: true,
	})

	// Subscriptions (1050-1059)
	RouterErrorSubscriptionNotFound = registerError(&RouterError{
		Code: 1050, Name: "PROTOCOL_SUBSCRIPTION_NOT_FOUND", Category: CategoryInternal,
		Description: "Subscription not found", Retryable: false,
	})
	RouterErrorSubscriptionInitFailed = registerError(&RouterError{
		Code: 1051, Name: "PROTOCOL_SUBSCRIPTION_INIT_FAILED", Category: CategoryInternal,
		Description: "Failed to initialize subscription", Retryable: false,
	})
	RouterErrorWebSocketIdleTimeout = registerError(&RouterError{
		Code: 1052, Name: "PROTOCOL_WEBSOCKET_IDLE_TIMEOUT", Category: CategoryInternal,
		Description: "WebSocket idle timeout", Retryable: false,
	})
	RouterErrorSubscriptionAlreadyExists = registerError(&RouterError{
		Code: 1053, Name: "PROTOCOL_SUBSCRIPTION_ALREADY_EXISTS", Category: CategoryInternal,
		Description: "Subscription already exists for this consumer/key", Retryable: false,
	})
)

// ---------------------------------------------------------------------------
// Layer B: Node Errors (External) — range 2000-2999
// Errors returned by the blockchain node itself (not execution/state errors).
// ---------------------------------------------------------------------------

var (
	// Generic node errors (2000-2099)
	RouterErrorNodeMethodNotFound = registerError(&RouterError{
		Code: 2001, Name: "NODE_METHOD_NOT_FOUND", Category: CategoryExternal,
		SubCategory: SubCategoryUnsupportedMethod,
		Description: "Method does not exist on this node (unknown to the API surface); non-retryable", Retryable: false,
	})
	// NODE_METHOD_NOT_SUPPORTED: the method exists but this node has disabled
	// it (e.g. provider tier, admin config). Distinct from NODE_METHOD_NOT_FOUND
	// in that it IS retryable on another provider. The Name field is a
	// Prometheus label so keep it stable — the distinction lives in the
	// description and the Retryable flag.
	RouterErrorNodeMethodNotSupported = registerError(&RouterError{
		Code: 2002, Name: "NODE_METHOD_NOT_SUPPORTED", Category: CategoryExternal,
		Description: "Method exists but is DISABLED on this specific node (provider tier / policy / admin config). Retryable on a different provider. Distinct from NODE_METHOD_NOT_FOUND (2001) which means the method does not exist at all.", Retryable: true,
	})
	RouterErrorNodeInternalError = registerError(&RouterError{
		Code: 2003, Name: "NODE_INTERNAL_ERROR", Category: CategoryExternal,
		Description: "Internal node error", Retryable: true,
	})
	RouterErrorNodeServerError = registerError(&RouterError{
		Code: 2004, Name: "NODE_SERVER_ERROR", Category: CategoryExternal,
		Description: "Generic server error", Retryable: true,
	})
	RouterErrorNodeRateLimited = registerError(&RouterError{
		Code: 2005, Name: "NODE_RATE_LIMITED", Category: CategoryExternal,
		SubCategory: SubCategoryRateLimit,
		Description: "Rate limited by node", Retryable: true,
	})
	RouterErrorNodeServiceUnavailable = registerError(&RouterError{
		Code: 2006, Name: "NODE_SERVICE_UNAVAILABLE", Category: CategoryExternal,
		Description: "Node temporarily unavailable", Retryable: true,
	})
	RouterErrorNodeSyncing = registerError(&RouterError{
		Code: 2007, Name: "NODE_SYNCING", Category: CategoryExternal,
		Description: "Node is syncing/catching up", Retryable: true,
	})
	RouterErrorNodeUnimplemented = registerError(&RouterError{
		Code: 2008, Name: "NODE_UNIMPLEMENTED", Category: CategoryExternal,
		SubCategory: SubCategoryUnsupportedMethod,
		Description: "gRPC method unimplemented", Retryable: false,
	})
	RouterErrorNodeEndpointNotFound = registerError(&RouterError{
		Code: 2009, Name: "NODE_ENDPOINT_NOT_FOUND", Category: CategoryExternal,
		SubCategory: SubCategoryUnsupportedMethod,
		Description: "REST endpoint not found", Retryable: false,
	})
	RouterErrorNodeMethodNotAllowed = registerError(&RouterError{
		Code: 2010, Name: "NODE_METHOD_NOT_ALLOWED", Category: CategoryExternal,
		SubCategory: SubCategoryUnsupportedMethod,
		Description: "REST method not allowed", Retryable: false,
	})
	RouterErrorNodeLimitExceeded = registerError(&RouterError{
		Code: 2011, Name: "NODE_LIMIT_EXCEEDED", Category: CategoryExternal,
		SubCategory: SubCategoryRateLimit,
		Description: "Request exceeds node limit (e.g., eth_getLogs range)", Retryable: false,
	})
	RouterErrorNodeResourceNotFound = registerError(&RouterError{
		Code: 2012, Name: "NODE_RESOURCE_NOT_FOUND", Category: CategoryExternal,
		Description: "Resource not found at node level", Retryable: true,
	})
	RouterErrorNodeResourceUnavailable = registerError(&RouterError{
		Code: 2013, Name: "NODE_RESOURCE_UNAVAILABLE", Category: CategoryExternal,
		Description: "Resource exists but unavailable", Retryable: true,
	})
	RouterErrorNodeGatewayTimeout = registerError(&RouterError{
		Code: 2014, Name: "NODE_GATEWAY_TIMEOUT", Category: CategoryExternal,
		Description: "Gateway timeout (HTTP 504 from provider)", Retryable: true,
	})
	RouterErrorNodeBadGateway = registerError(&RouterError{
		Code: 2015, Name: "NODE_BAD_GATEWAY", Category: CategoryExternal,
		Description: "Bad gateway (HTTP 502 from provider)", Retryable: true,
	})
	// NODE_UNAUTHORIZED: upstream rejected the smart-router's credentials
	// (HTTP 401). Non-retryable because the same credentials are reused on
	// every attempt — retrying just multiplies the same auth failure across
	// providers. Operator must fix the auth-config for the affected endpoint.
	RouterErrorNodeUnauthorized = registerError(&RouterError{
		Code: 2016, Name: "NODE_UNAUTHORIZED", Category: CategoryExternal,
		Description: "Upstream rejected router credentials (HTTP 401)", Retryable: false,
	})
	// NODE_DATA_NOT_HELD: the endpoint answered correctly and the answer is "I do
	// not have this" — a pruned height, an object that never existed, a request
	// outside the range this node retains. Distinct from NODE_RESOURCE_NOT_FOUND
	// (2012) in the fault axis, not the symptom: 2012 stays scoreable because a
	// JSON-RPC -32001 is an error the node RAISED, whereas this is the node
	// truthfully describing its own data scope.
	//
	// Retryable stays true so a pruned node still falls through to an archive one.
	// SubCategoryDataScope is what keeps it out of the availability signal, which
	// Retryable alone cannot express (see ErrorSubCategory.IsDataScope).
	RouterErrorNodeDataNotHeld = registerError(&RouterError{
		Code: 2017, Name: "NODE_DATA_NOT_HELD", Category: CategoryExternal,
		SubCategory: SubCategoryDataScope,
		Description: "Endpoint does not hold the requested data (pruned or never existed)", Retryable: true,
	})

	// Bitcoin/UTXO node errors (2100-2149)
	// Source: Bitcoin Core src/rpc/protocol.h
	RouterErrorNodeBitcoinWarmup = registerError(&RouterError{
		Code: 2101, Name: "NODE_BITCOIN_WARMUP", Category: CategoryExternal,
		Description: "Node still warming up (RPC_IN_WARMUP, Bitcoin -28)", Retryable: true,
	})
	RouterErrorNodeBitcoinInitialDownload = registerError(&RouterError{
		Code: 2102, Name: "NODE_BITCOIN_INITIAL_DOWNLOAD", Category: CategoryExternal,
		Description: "Node in initial block download (RPC_CLIENT_IN_INITIAL_DOWNLOAD, Bitcoin -10)", Retryable: true,
	})
	RouterErrorNodeBitcoinNotConnected = registerError(&RouterError{
		Code: 2103, Name: "NODE_BITCOIN_NOT_CONNECTED", Category: CategoryExternal,
		Description: "Node not connected / shutting down (RPC_CLIENT_NOT_CONNECTED, Bitcoin -9)", Retryable: true,
	})

	// Solana node errors (2150-2169)
	RouterErrorNodeSolanaUnhealthy = registerError(&RouterError{
		Code: 2150, Name: "NODE_SOLANA_UNHEALTHY", Category: CategoryExternal,
		Description: "Solana node behind/unhealthy (-32005)", Retryable: true,
	})
)

// ---------------------------------------------------------------------------
// Layer C: Blockchain Errors (External) — range 3000-3999
// Errors from the blockchain execution/state layer.
// ---------------------------------------------------------------------------

var (
	// Transaction errors (3000-3099)
	RouterErrorChainNonceTooLow = registerError(&RouterError{
		Code: 3001, Name: "CHAIN_NONCE_TOO_LOW", Category: CategoryExternal,
		Description: "Nonce/sequence too low", Retryable: false,
	})
	RouterErrorChainNonceTooHigh = registerError(&RouterError{
		Code: 3002, Name: "CHAIN_NONCE_TOO_HIGH", Category: CategoryExternal,
		Description: "Nonce too high", Retryable: false,
	})
	RouterErrorChainInsufficientFunds = registerError(&RouterError{
		Code: 3003, Name: "CHAIN_INSUFFICIENT_FUNDS", Category: CategoryExternal,
		Description: "Insufficient funds for transfer/gas", Retryable: false,
	})
	RouterErrorChainGasTooLow = registerError(&RouterError{
		Code: 3004, Name: "CHAIN_GAS_TOO_LOW", Category: CategoryExternal,
		Description: "Intrinsic gas too low", Retryable: false,
	})
	RouterErrorChainGasLimitExceeded = registerError(&RouterError{
		Code: 3005, Name: "CHAIN_GAS_LIMIT_EXCEEDED", Category: CategoryExternal,
		Description: "Exceeds block gas limit", Retryable: false,
	})
	RouterErrorChainTxUnderpriced = registerError(&RouterError{
		Code: 3006, Name: "CHAIN_TX_UNDERPRICED", Category: CategoryExternal,
		Description: "Transaction gas price too low", Retryable: false,
	})
	RouterErrorChainTxAlreadyKnown = registerError(&RouterError{
		Code: 3007, Name: "CHAIN_TX_ALREADY_KNOWN", Category: CategoryExternal,
		Description: "Transaction already in mempool", Retryable: false,
	})
	RouterErrorChainTxReplacementUnderpriced = registerError(&RouterError{
		Code: 3008, Name: "CHAIN_TX_REPLACEMENT_UNDERPRICED", Category: CategoryExternal,
		Description: "Replacement tx gas too low", Retryable: false,
	})
	RouterErrorChainMempoolFull = registerError(&RouterError{
		Code: 3009, Name: "CHAIN_MEMPOOL_FULL", Category: CategoryExternal,
		Description: "Mempool/tx pool is full", Retryable: false,
	})
	RouterErrorChainTxTooLarge = registerError(&RouterError{
		Code: 3010, Name: "CHAIN_TX_TOO_LARGE", Category: CategoryExternal,
		Description: "Transaction exceeds size limit", Retryable: false,
	})
	RouterErrorChainMaxFeeBelowBase = registerError(&RouterError{
		Code: 3011, Name: "CHAIN_MAX_FEE_BELOW_BASE", Category: CategoryExternal,
		Description: "Max fee per gas below base fee", Retryable: false,
	})
	RouterErrorChainInvalidSequence = registerError(&RouterError{
		Code: 3012, Name: "CHAIN_INVALID_SEQUENCE", Category: CategoryExternal,
		Description: "Invalid sequence (Cosmos nonce equivalent)", Retryable: false,
	})
	RouterErrorChainInsufficientFee = registerError(&RouterError{
		Code: 3013, Name: "CHAIN_INSUFFICIENT_FEE", Category: CategoryExternal,
		Description: "Insufficient fee", Retryable: false,
	})
	RouterErrorChainTxRejected = registerError(&RouterError{
		Code: 3014, Name: "CHAIN_TX_REJECTED", Category: CategoryExternal,
		Description: "Transaction rejected by network rules", Retryable: false,
	})
	RouterErrorChainDoubleSpend = registerError(&RouterError{
		Code: 3015, Name: "CHAIN_DOUBLE_SPEND", Category: CategoryExternal,
		Description: "Double spend / UTXO already spent", Retryable: false,
	})
	RouterErrorChainInvalidSignature = registerError(&RouterError{
		Code: 3016, Name: "CHAIN_INVALID_SIGNATURE", Category: CategoryExternal,
		Description: "Invalid transaction signature", Retryable: false,
	})

	// Execution errors (3100-3199)
	RouterErrorChainExecutionReverted = registerError(&RouterError{
		Code: 3101, Name: "CHAIN_EXECUTION_REVERTED", Category: CategoryExternal,
		Description: "Smart contract execution reverted", Retryable: false,
	})
	RouterErrorChainOutOfGas = registerError(&RouterError{
		Code: 3102, Name: "CHAIN_OUT_OF_GAS", Category: CategoryExternal,
		Description: "Out of gas during execution", Retryable: false,
	})
	RouterErrorChainStackOverflow = registerError(&RouterError{
		Code: 3103, Name: "CHAIN_STACK_OVERFLOW", Category: CategoryExternal,
		Description: "Stack limit reached", Retryable: false,
	})
	RouterErrorChainInvalidOpcode = registerError(&RouterError{
		Code: 3104, Name: "CHAIN_INVALID_OPCODE", Category: CategoryExternal,
		Description: "Invalid opcode encountered", Retryable: false,
	})
	RouterErrorChainWriteProtection = registerError(&RouterError{
		Code: 3105, Name: "CHAIN_WRITE_PROTECTION", Category: CategoryExternal,
		Description: "Write in STATICCALL context", Retryable: false,
	})
	RouterErrorChainContractSizeExceeded = registerError(&RouterError{
		Code: 3106, Name: "CHAIN_CONTRACT_SIZE_EXCEEDED", Category: CategoryExternal,
		Description: "Contract bytecode exceeds 24KB EIP-170 limit (Geth: 'max code size exceeded')", Retryable: false,
	})
	RouterErrorChainAccountNotFound = registerError(&RouterError{
		Code: 3107, Name: "CHAIN_ACCOUNT_NOT_FOUND", Category: CategoryExternal,
		Description: "Account/contract does not exist", Retryable: false,
	})
	RouterErrorChainZkEVMOutOfCounters = registerError(&RouterError{
		Code: 3108, Name: "CHAIN_ZKEVM_OUT_OF_COUNTERS", Category: CategoryExternal,
		Description: "Polygon zkEVM prover exceeded circuit counter budget", Retryable: false,
	})

	// State/data errors (3200-3299)
	RouterErrorChainBlockNotFound = registerError(&RouterError{
		Code: 3201, Name: "CHAIN_BLOCK_NOT_FOUND", Category: CategoryExternal,
		Description: "Block not found", Retryable: true,
	})
	RouterErrorChainTxNotFound = registerError(&RouterError{
		Code: 3202, Name: "CHAIN_TX_NOT_FOUND", Category: CategoryExternal,
		Description: "Transaction not found", Retryable: true,
	})
	RouterErrorChainReceiptNotFound = registerError(&RouterError{
		Code: 3203, Name: "CHAIN_RECEIPT_NOT_FOUND", Category: CategoryExternal,
		Description: "Transaction receipt not found", Retryable: true,
	})
	RouterErrorChainStatePruned = registerError(&RouterError{
		Code: 3204, Name: "CHAIN_STATE_PRUNED", Category: CategoryExternal,
		Description: "State pruned/missing trie node", Retryable: true,
	})
	RouterErrorChainDataNotAvailable = registerError(&RouterError{
		Code: 3205, Name: "CHAIN_DATA_NOT_AVAILABLE", Category: CategoryExternal,
		Description: "Historical data not available", Retryable: true,
	})
	RouterErrorChainBlockTooOld = registerError(&RouterError{
		Code: 3206, Name: "CHAIN_BLOCK_TOO_OLD", Category: CategoryExternal,
		Description: "Block results only for recent blocks", Retryable: true,
	})
	RouterErrorChainLogResponseTooLarge = registerError(&RouterError{
		Code: 3207, Name: "CHAIN_LOG_RESPONSE_TOO_LARGE", Category: CategoryExternal,
		Description: "Log query returned too many results", Retryable: false,
	})

	// Solana-specific (3300-3319) — Tier 2
	// Source: Solana RPC custom error codes (agave rpc-client-api/src/custom_error.rs)
	RouterErrorChainSolanaMissingLongTerm = registerError(&RouterError{
		Code: 3302, Name: "CHAIN_SOLANA_MISSING_LONG_TERM", Category: CategoryExternal,
		Description: "Slot missing in long-term storage (-32009)", Retryable: false,
	})
	RouterErrorChainSolanaLedgerJump = registerError(&RouterError{
		Code: 3303, Name: "CHAIN_SOLANA_LEDGER_JUMP", Category: CategoryExternal,
		Description: "Missing due to ledger jump/snapshot (-32007)", Retryable: true,
	})
	RouterErrorChainSolanaBlockhashNotFound = registerError(&RouterError{
		Code: 3304, Name: "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", Category: CategoryExternal,
		Description: "Blockhash not found/expired", Retryable: false,
	})
	RouterErrorChainSolanaSimulationFailed = registerError(&RouterError{
		Code: 3305, Name: "CHAIN_SOLANA_SIMULATION_FAILED", Category: CategoryExternal,
		Description: "Transaction simulation failed (-32002)", Retryable: false,
	})
	RouterErrorChainSolanaSignatureVerifyFailed = registerError(&RouterError{
		Code: 3306, Name: "CHAIN_SOLANA_SIGNATURE_VERIFY_FAILED", Category: CategoryExternal,
		Description: "Signature verification failure (-32003)", Retryable: false,
	})
	RouterErrorChainSolanaExcludedFromIndex = registerError(&RouterError{
		Code: 3307, Name: "CHAIN_SOLANA_EXCLUDED_FROM_INDEX", Category: CategoryExternal,
		Description: "Excluded from account secondary indexes (-32010)", Retryable: false,
	})
	RouterErrorChainSolanaSignatureLengthMismatch = registerError(&RouterError{
		Code: 3308, Name: "CHAIN_SOLANA_SIGNATURE_LENGTH_MISMATCH", Category: CategoryExternal,
		Description: "Incorrectly formatted signature (-32013)", Retryable: false,
	})
	RouterErrorChainSolanaBlockStatusUnavailable = registerError(&RouterError{
		Code: 3309, Name: "CHAIN_SOLANA_BLOCK_STATUS_UNAVAILABLE", Category: CategoryExternal,
		Description: "Block status unavailable (-32014)", Retryable: true,
	})
	RouterErrorChainSolanaTxVersionUnsupported = registerError(&RouterError{
		Code: 3310, Name: "CHAIN_SOLANA_TX_VERSION_UNSUPPORTED", Category: CategoryExternal,
		Description: "Transaction version not supported (-32015)", Retryable: false,
	})
	RouterErrorChainSolanaMinContextSlotNotReached = registerError(&RouterError{
		Code: 3311, Name: "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", Category: CategoryExternal,
		Description: "Minimum context slot not reached (-32016)", Retryable: true,
	})

	// Starknet-specific (3320-3349) — Tier 2
	// Source: Starknet JSON-RPC spec error codes
	RouterErrorChainStarknetFailedToReceiveTx = registerError(&RouterError{
		Code: 3320, Name: "CHAIN_STARKNET_FAILED_TO_RECEIVE_TX", Category: CategoryExternal,
		Description: "Sequencer rejected transaction (code 1)", Retryable: false,
	})
	RouterErrorChainStarknetClassNotFound = registerError(&RouterError{
		Code: 3321, Name: "CHAIN_STARKNET_CLASS_NOT_FOUND", Category: CategoryExternal,
		Description: "Class hash not found (code 28)", Retryable: false,
	})
	RouterErrorChainStarknetCompilationFailed = registerError(&RouterError{
		Code: 3322, Name: "CHAIN_STARKNET_COMPILATION_FAILED", Category: CategoryExternal,
		Description: "Sierra to CASM compilation failed (code 56)", Retryable: false,
	})
	RouterErrorChainStarknetClassAlreadyDeclared = registerError(&RouterError{
		Code: 3323, Name: "CHAIN_STARKNET_CLASS_ALREADY_DECLARED", Category: CategoryExternal,
		Description: "Class already declared (code 51)", Retryable: false,
	})
	RouterErrorChainStarknetContractError = registerError(&RouterError{
		Code: 3324, Name: "CHAIN_STARKNET_CONTRACT_ERROR", Category: CategoryExternal,
		Description: "Contract error during execution (code 40)", Retryable: false,
	})
	RouterErrorChainStarknetTxExecError = registerError(&RouterError{
		Code: 3325, Name: "CHAIN_STARKNET_TX_EXEC_ERROR", Category: CategoryExternal,
		Description: "Transaction execution error (code 41)", Retryable: false,
	})
	RouterErrorChainStarknetInvalidNonce = registerError(&RouterError{
		Code: 3326, Name: "CHAIN_STARKNET_INVALID_NONCE", Category: CategoryExternal,
		Description: "Invalid transaction nonce (code 52)", Retryable: false,
	})
	RouterErrorChainStarknetInsufficientFee = registerError(&RouterError{
		Code: 3327, Name: "CHAIN_STARKNET_INSUFFICIENT_FEE", Category: CategoryExternal,
		Description: "Insufficient max fee (code 53)", Retryable: false,
	})
	RouterErrorChainStarknetInsufficientBalance = registerError(&RouterError{
		Code: 3328, Name: "CHAIN_STARKNET_INSUFFICIENT_BALANCE", Category: CategoryExternal,
		Description: "Insufficient account balance (code 54)", Retryable: false,
	})
	RouterErrorChainStarknetValidationFailure = registerError(&RouterError{
		Code: 3329, Name: "CHAIN_STARKNET_VALIDATION_FAILURE", Category: CategoryExternal,
		Description: "Account validation failed (code 55)", Retryable: false,
	})
	RouterErrorChainStarknetContractNotFound = registerError(&RouterError{
		Code: 3330, Name: "CHAIN_STARKNET_CONTRACT_NOT_FOUND", Category: CategoryExternal,
		Description: "Contract address not found (code 20)", Retryable: false,
	})
	RouterErrorChainStarknetBlockNotFound = registerError(&RouterError{
		Code: 3331, Name: "CHAIN_STARKNET_BLOCK_NOT_FOUND", Category: CategoryExternal,
		Description: "Block not found (code 24)", Retryable: true,
	})
	RouterErrorChainStarknetTxHashNotFound = registerError(&RouterError{
		Code: 3332, Name: "CHAIN_STARKNET_TX_HASH_NOT_FOUND", Category: CategoryExternal,
		Description: "Transaction hash not found (code 29)", Retryable: true,
	})
	RouterErrorChainStarknetDuplicateTx = registerError(&RouterError{
		Code: 3333, Name: "CHAIN_STARKNET_DUPLICATE_TX", Category: CategoryExternal,
		Description: "Duplicate transaction in mempool (code 59)", Retryable: false,
	})
	RouterErrorChainStarknetTxVersionUnsupported = registerError(&RouterError{
		Code: 3334, Name: "CHAIN_STARKNET_TX_VERSION_UNSUPPORTED", Category: CategoryExternal,
		Description: "Unsupported transaction version (code 61)", Retryable: false,
	})
	RouterErrorChainStarknetUnexpectedError = registerError(&RouterError{
		Code: 3335, Name: "CHAIN_STARKNET_UNEXPECTED_ERROR", Category: CategoryExternal,
		Description: "Unexpected server error (code 63)", Retryable: true,
	})

	// Bitcoin/UTXO-specific (3340-3359) — Tier 2
	// Source: Bitcoin Core src/rpc/protocol.h
	RouterErrorChainBitcoinVerifyError = registerError(&RouterError{
		Code: 3341, Name: "CHAIN_BITCOIN_VERIFY_ERROR", Category: CategoryExternal,
		Description: "Transaction/block verification failed (RPC_VERIFY_ERROR, Bitcoin -25)", Retryable: false,
	})
	RouterErrorChainBitcoinVerifyRejected = registerError(&RouterError{
		Code: 3342, Name: "CHAIN_BITCOIN_VERIFY_REJECTED", Category: CategoryExternal,
		Description: "Transaction rejected by network rules (RPC_VERIFY_REJECTED, Bitcoin -26)", Retryable: false,
	})
	RouterErrorChainBitcoinAlreadyInChain = registerError(&RouterError{
		Code: 3343, Name: "CHAIN_BITCOIN_ALREADY_IN_CHAIN", Category: CategoryExternal,
		Description: "Transaction already confirmed (RPC_VERIFY_ALREADY_IN_CHAIN, Bitcoin -27)", Retryable: false,
	})
	RouterErrorChainBitcoinWalletInsufficientFunds = registerError(&RouterError{
		Code: 3344, Name: "CHAIN_BITCOIN_WALLET_INSUFFICIENT_FUNDS", Category: CategoryExternal,
		Description: "Wallet UTXO coin selection failed (RPC_WALLET_INSUFFICIENT_FUNDS, Bitcoin -6). Distinct from EVM CHAIN_INSUFFICIENT_FUNDS (3003) which is a tx submission failure.", Retryable: false,
	})

	// NEAR-specific (3360-3379) — Tier 2
	// Source: NEAR RPC docs (error names in JSON-RPC error.cause.name)
	RouterErrorChainNEARUnknownBlock = registerError(&RouterError{
		Code: 3360, Name: "CHAIN_NEAR_UNKNOWN_BLOCK", Category: CategoryExternal,
		Description: "Block not found or garbage-collected (UNKNOWN_BLOCK)", Retryable: true,
	})
	RouterErrorChainNEARUnknownChunk = registerError(&RouterError{
		Code: 3361, Name: "CHAIN_NEAR_UNKNOWN_CHUNK", Category: CategoryExternal,
		Description: "Chunk not found (UNKNOWN_CHUNK)", Retryable: true,
	})
	RouterErrorChainNEARInvalidShardID = registerError(&RouterError{
		Code: 3362, Name: "CHAIN_NEAR_INVALID_SHARD_ID", Category: CategoryExternal,
		Description: "Shard ID does not exist (INVALID_SHARD_ID)", Retryable: false,
	})
	RouterErrorChainNEARNotSyncedYet = registerError(&RouterError{
		Code: 3363, Name: "CHAIN_NEAR_NOT_SYNCED_YET", Category: CategoryExternal,
		Description: "Node still syncing (NOT_SYNCED_YET)", Retryable: true,
	})
)

// ---------------------------------------------------------------------------
// Layer D: User Errors (External) — range 4000-4999
// Errors caused by malformed or invalid client requests.
// ---------------------------------------------------------------------------

// Layer D errors are classified for metrics/observability but do NOT receive
// special CU treatment — providers still charge normal CU for processing
// invalid client requests (since responses are not cached and the provider
// does real work on every call).
var (
	RouterErrorUserParseError = registerError(&RouterError{
		Code: 4001, Name: "USER_PARSE_ERROR", Category: CategoryExternal,
		Description: "Invalid JSON in request", Retryable: false,
	})
	RouterErrorUserInvalidRequest = registerError(&RouterError{
		Code: 4002, Name: "USER_INVALID_REQUEST", Category: CategoryExternal,
		Description: "Request is not a valid JSON-RPC/REST/gRPC object", Retryable: false,
	})
	RouterErrorUserInvalidParams = registerError(&RouterError{
		Code: 4003, Name: "USER_INVALID_PARAMS", Category: CategoryExternal,
		Description: "Invalid method parameters", Retryable: false,
	})
	RouterErrorUserInvalidBlockFormat = registerError(&RouterError{
		Code: 4004, Name: "USER_INVALID_BLOCK_FORMAT", Category: CategoryExternal,
		Description: "Invalid block number format (e.g., non-hex)", Retryable: false,
	})
	RouterErrorUserInvalidAddress = registerError(&RouterError{
		Code: 4005, Name: "USER_INVALID_ADDRESS", Category: CategoryExternal,
		Description: "Invalid address format", Retryable: false,
	})
	RouterErrorUserRequestTooLarge = registerError(&RouterError{
		Code: 4006, Name: "USER_REQUEST_TOO_LARGE", Category: CategoryExternal,
		Description: "Request body exceeds size limit", Retryable: false,
	})
	RouterErrorUserInvalidHex = registerError(&RouterError{
		Code: 4007, Name: "USER_INVALID_HEX", Category: CategoryExternal,
		Description: "Invalid hex encoding", Retryable: false,
	})
)
