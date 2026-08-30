package common

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fault axis answers "was the endpoint at fault", and it is deliberately NOT the same question
// as Retryable ("would asking someone else help"). FAILOVER-TASKS section 2 decision 4 separates
// them: an endpoint that truthfully reports it does not hold the data, or does not offer the
// method, is healthy and must keep its availability score — while the request still travels on to
// a node that can serve it.
//
// This table is the contract. It exists to be a tripwire in two directions:
//
//   - Every Retryable value here is what it was BEFORE the fault axis was introduced. The
//     subcategory re-labelling must not have moved routing, and a diff in this column is how you
//     find out that it did.
//   - A new code added without a deliberate fault-axis decision fails
//     TestFaultAxis_EveryRegisteredCodeIsAccountedFor below, rather than silently defaulting to
//     "the endpoint's fault".
type faultAxisCase struct {
	code      uint32
	name      string
	sub       ErrorSubCategory
	retryable bool
	why       string
}

var faultAxisTable = []faultAxisCase{
	// Not the endpoint's fault — the method does not exist anywhere. Retrying is pointless.
	{2001, "NODE_METHOD_NOT_FOUND", SubCategoryUnsupportedMethod, false, "absent from every API surface"},
	{2008, "NODE_UNIMPLEMENTED", SubCategoryUnsupportedMethod, false, "gRPC unimplemented"},
	{2009, "NODE_ENDPOINT_NOT_FOUND", SubCategoryUnsupportedMethod, false, "REST path does not exist"},
	{2010, "NODE_METHOD_NOT_ALLOWED", SubCategoryUnsupportedMethod, false, "REST verb not allowed"},

	// Not the endpoint's fault, but retrying DOES help — the capability exists elsewhere.
	// The whole reason SubCategoryNodeCapability is not SubCategoryUnsupportedMethod.
	{2002, "NODE_METHOD_NOT_SUPPORTED", SubCategoryNodeCapability, true, "disabled on THIS node; another tier serves it"},

	// Not the endpoint's fault — healthy, just busy. Layer 6 owns these.
	{2005, "NODE_RATE_LIMITED", SubCategoryRateLimit, true, "retryable AND rate limited"},
	{2011, "NODE_LIMIT_EXCEEDED", SubCategoryRateLimit, false, "non-retryable AND rate limited"},

	// Not the endpoint's fault — it answered truthfully about what it holds.
	{2012, "NODE_RESOURCE_NOT_FOUND", SubCategoryDataScope, true, ""},
	{2013, "NODE_RESOURCE_UNAVAILABLE", SubCategoryDataScope, true, ""},
	{2017, "NODE_DATA_NOT_HELD", SubCategoryDataScope, true, ""},
	{3201, "CHAIN_BLOCK_NOT_FOUND", SubCategoryDataScope, true, ""},
	{3202, "CHAIN_TX_NOT_FOUND", SubCategoryDataScope, true, "the pending-transaction poll: every node says no"},
	{3203, "CHAIN_RECEIPT_NOT_FOUND", SubCategoryDataScope, true, ""},
	{3204, "CHAIN_STATE_PRUNED", SubCategoryDataScope, true, ""},
	{3205, "CHAIN_DATA_NOT_AVAILABLE", SubCategoryDataScope, true, ""},
	{3206, "CHAIN_BLOCK_TOO_OLD", SubCategoryDataScope, true, ""},

	{3303, "CHAIN_SOLANA_LEDGER_JUMP", SubCategoryDataScope, true, "missing due to ledger jump/snapshot"},
	{3309, "CHAIN_SOLANA_BLOCK_STATUS_UNAVAILABLE", SubCategoryDataScope, true, ""},
	{3311, "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", SubCategoryDataScope, true, "this node's ledger does not reach that slot"},
	{3331, "CHAIN_STARKNET_BLOCK_NOT_FOUND", SubCategoryDataScope, true, ""},
	{3332, "CHAIN_STARKNET_TX_HASH_NOT_FOUND", SubCategoryDataScope, true, ""},
	{3360, "CHAIN_NEAR_UNKNOWN_BLOCK", SubCategoryDataScope, true, "not found or garbage-collected"},
	{3361, "CHAIN_NEAR_UNKNOWN_CHUNK", SubCategoryDataScope, true, ""},

	// The endpoint's fault — it is telling us it is broken or not ready.
	{2003, "NODE_INTERNAL_ERROR", SubCategoryNone, true, ""},
	{2004, "NODE_SERVER_ERROR", SubCategoryNone, true, ""},
	{2006, "NODE_SERVICE_UNAVAILABLE", SubCategoryNone, true, ""},
	{2007, "NODE_SYNCING", SubCategoryNone, true, "cannot serve yet, which is a health fact"},
	{2014, "NODE_GATEWAY_TIMEOUT", SubCategoryNone, true, ""},
	{2015, "NODE_BAD_GATEWAY", SubCategoryNone, true, ""},
	{2101, "NODE_BITCOIN_WARMUP", SubCategoryNone, true, ""},
	{2102, "NODE_BITCOIN_INITIAL_DOWNLOAD", SubCategoryNone, true, ""},
	{2103, "NODE_BITCOIN_NOT_CONNECTED", SubCategoryNone, true, ""},
	{2150, "NODE_SOLANA_UNHEALTHY", SubCategoryNone, true, ""},
	{3335, "CHAIN_STARKNET_UNEXPECTED_ERROR", SubCategoryNone, true, "unexpected server error — the node broke"},
	{3363, "CHAIN_NEAR_NOT_SYNCED_YET", SubCategoryNone, true, "same shape as NODE_SYNCING (2007)"},

	// The caller's fault. Non-retryable, and no fault-axis label needed — the default arm of
	// classifyEndpointHealth already excuses CategoryExternal + !Retryable.
	{2016, "NODE_UNAUTHORIZED", SubCategoryNone, false, "credentials rejected; see the note below"},
	{3001, "CHAIN_NONCE_TOO_LOW", SubCategoryNone, false, ""},
	{3002, "CHAIN_NONCE_TOO_HIGH", SubCategoryNone, false, ""},
	{3003, "CHAIN_INSUFFICIENT_FUNDS", SubCategoryNone, false, ""},
	{3004, "CHAIN_GAS_TOO_LOW", SubCategoryNone, false, ""},
	{3207, "CHAIN_LOG_RESPONSE_TOO_LARGE", SubCategoryNone, false, "describes the query, not the node"},
	{3005, "CHAIN_GAS_LIMIT_EXCEEDED", SubCategoryNone, false, ""},
	{3006, "CHAIN_TX_UNDERPRICED", SubCategoryNone, false, ""},
	{3007, "CHAIN_TX_ALREADY_KNOWN", SubCategoryNone, false, ""},
	{3008, "CHAIN_TX_REPLACEMENT_UNDERPRICED", SubCategoryNone, false, ""},
	{3009, "CHAIN_MEMPOOL_FULL", SubCategoryNone, false, ""},
	{3010, "CHAIN_TX_TOO_LARGE", SubCategoryNone, false, ""},
	{3011, "CHAIN_MAX_FEE_BELOW_BASE", SubCategoryNone, false, ""},
	{3012, "CHAIN_INVALID_SEQUENCE", SubCategoryNone, false, ""},
	{3013, "CHAIN_INSUFFICIENT_FEE", SubCategoryNone, false, ""},
	{3014, "CHAIN_TX_REJECTED", SubCategoryNone, false, ""},
	{3015, "CHAIN_DOUBLE_SPEND", SubCategoryNone, false, ""},
	{3016, "CHAIN_INVALID_SIGNATURE", SubCategoryNone, false, ""},
	{3101, "CHAIN_EXECUTION_REVERTED", SubCategoryNone, false, ""},
	{3102, "CHAIN_OUT_OF_GAS", SubCategoryNone, false, ""},
	{3103, "CHAIN_STACK_OVERFLOW", SubCategoryNone, false, ""},
	{3104, "CHAIN_INVALID_OPCODE", SubCategoryNone, false, ""},
	{3105, "CHAIN_WRITE_PROTECTION", SubCategoryNone, false, ""},
	{3106, "CHAIN_CONTRACT_SIZE_EXCEEDED", SubCategoryNone, false, ""},
	{3107, "CHAIN_ACCOUNT_NOT_FOUND", SubCategoryNone, false, "non-retryable: the account exists nowhere"},
	{3108, "CHAIN_ZKEVM_OUT_OF_COUNTERS", SubCategoryNone, false, ""},
	{3302, "CHAIN_SOLANA_MISSING_LONG_TERM", SubCategoryNone, false, ""},
	{3304, "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", SubCategoryNone, false, ""},
	{3305, "CHAIN_SOLANA_SIMULATION_FAILED", SubCategoryNone, false, ""},
	{3306, "CHAIN_SOLANA_SIGNATURE_VERIFY_FAILED", SubCategoryNone, false, ""},
	{3307, "CHAIN_SOLANA_EXCLUDED_FROM_INDEX", SubCategoryNone, false, ""},
	{3308, "CHAIN_SOLANA_SIGNATURE_LENGTH_MISMATCH", SubCategoryNone, false, ""},
	{3310, "CHAIN_SOLANA_TX_VERSION_UNSUPPORTED", SubCategoryNone, false, ""},
	{3320, "CHAIN_STARKNET_FAILED_TO_RECEIVE_TX", SubCategoryNone, false, ""},
	{3321, "CHAIN_STARKNET_CLASS_NOT_FOUND", SubCategoryNone, false, ""},
	{3322, "CHAIN_STARKNET_COMPILATION_FAILED", SubCategoryNone, false, ""},
	{3323, "CHAIN_STARKNET_CLASS_ALREADY_DECLARED", SubCategoryNone, false, ""},
	{3324, "CHAIN_STARKNET_CONTRACT_ERROR", SubCategoryNone, false, ""},
	{3325, "CHAIN_STARKNET_TX_EXEC_ERROR", SubCategoryNone, false, ""},
	{3326, "CHAIN_STARKNET_INVALID_NONCE", SubCategoryNone, false, ""},
	{3327, "CHAIN_STARKNET_INSUFFICIENT_FEE", SubCategoryNone, false, ""},
	{3328, "CHAIN_STARKNET_INSUFFICIENT_BALANCE", SubCategoryNone, false, ""},
	{3329, "CHAIN_STARKNET_VALIDATION_FAILURE", SubCategoryNone, false, ""},
	{3330, "CHAIN_STARKNET_CONTRACT_NOT_FOUND", SubCategoryNone, false, ""},
	{3333, "CHAIN_STARKNET_DUPLICATE_TX", SubCategoryNone, false, ""},
	{3334, "CHAIN_STARKNET_TX_VERSION_UNSUPPORTED", SubCategoryNone, false, ""},
	{3341, "CHAIN_BITCOIN_VERIFY_ERROR", SubCategoryNone, false, ""},
	{3342, "CHAIN_BITCOIN_VERIFY_REJECTED", SubCategoryNone, false, ""},
	{3343, "CHAIN_BITCOIN_ALREADY_IN_CHAIN", SubCategoryNone, false, ""},
	{3344, "CHAIN_BITCOIN_WALLET_INSUFFICIENT_FUNDS", SubCategoryNone, false, ""},
	{3362, "CHAIN_NEAR_INVALID_SHARD_ID", SubCategoryNone, false, ""},
}

func TestFaultAxis_RegistryContract(t *testing.T) {
	for _, tc := range faultAxisTable {
		t.Run(tc.name, func(t *testing.T) {
			le := getLavaError(tc.code)
			require.NotEqual(t, LavaErrorUnknown, le, "code %d is not registered", tc.code)
			require.Equal(t, tc.name, le.Name, "code %d changed name — this is an operator contract", tc.code)
			require.Equal(t, tc.sub, le.SubCategory,
				"%s fault axis moved. If deliberate, update this table and say why in the commit. %s", tc.name, tc.why)
			require.Equal(t, tc.retryable, le.Retryable,
				"%s retryability moved. The fault-axis work must NOT change routing — if this fails, "+
					"a subcategory change leaked into the retry decision", tc.name)
		})
	}
}

// The capability label must not stop the retry. That is the entire reason it exists rather than
// reusing SubCategoryUnsupportedMethod, which ShouldRetryErrorWithContext hard-stops on regardless
// of the Retryable flag — and stopping the retry would break the full-node-primary /
// archive-backup routing that 2002 is the trigger for.
func TestFaultAxis_CapabilityStillRetries(t *testing.T) {
	le := getLavaError(LavaErrorNodeMethodNotSupported.Code)
	require.True(t, le.Retryable, "2002 must stay retryable")
	require.True(t, le.SubCategory.IsNodeCapability(), "and must carry the capability label")
	require.False(t, le.SubCategory.IsUnsupportedMethod(),
		"but must NOT carry the unsupported-method label, which would hard-stop the retry")
}

// The four fault-axis predicates must stay mutually exclusive. Each answers "not the endpoint's
// fault" for a different reason, and callers branch on them individually — an error matching two
// would take whichever arm is written first, which is not a decision anyone made.
func TestFaultAxis_LabelsAreMutuallyExclusive(t *testing.T) {
	for _, sub := range []ErrorSubCategory{
		SubCategoryNone, SubCategoryUnsupportedMethod, SubCategoryRateLimit,
		SubCategoryDataScope, SubCategoryNodeCapability,
	} {
		matches := 0
		for _, hit := range []bool{
			sub.IsUnsupportedMethod(), sub.IsRateLimit(), sub.IsDataScope(), sub.IsNodeCapability(),
		} {
			if hit {
				matches++
			}
		}
		require.LessOrEqual(t, matches, 1, "%s matches %d fault-axis predicates", sub, matches)
	}
}

// Every registered node-level code (2xxx/3xxx) must appear in the contract table above.
//
// Without this, adding a code is a silent fault-axis decision: an unlabelled retryable external
// error lands in classifyEndpointHealth's blame arm, so a new "the node does not have that"
// variant would start demoting healthy endpoints with nobody having chosen it.
func TestFaultAxis_EveryRegisteredCodeIsAccountedFor(t *testing.T) {
	covered := map[uint32]struct{}{}
	for _, tc := range faultAxisTable {
		covered[tc.code] = struct{}{}
	}

	var missing []int
	for code, le := range errorRegistry {
		if code < 2000 || code >= 4000 {
			continue // node/chain layers only; transport and user layers are a different question
		}
		if _, ok := covered[code]; !ok {
			missing = append(missing, int(code))
			t.Logf("uncovered: %d %s (retryable=%v sub=%s)", code, le.Name, le.Retryable, le.SubCategory)
		}
	}
	sort.Ints(missing)
	require.Empty(t, missing,
		"these codes have no fault-axis decision recorded in TestFaultAxis_RegistryContract. "+
			"Add each one with a deliberate label — the default is 'the endpoint's fault', which is "+
			"rarely what a new not-found or capability variant should mean: %v", missing)
}
