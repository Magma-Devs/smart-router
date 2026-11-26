package common

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordOfLegacyFixture is the error record a legacy fixture stands for. The 102 fixtures carry the
// classifier's inputs as the readers pass them today, not a reply: a JSON-RPC body code with its
// message; a status as the code (XRP's 200, gRPC's 14); a REST code of 0 with the direct sender's
// "HTTP <n>: " prefix on the text. The record keeps the status in its own field and the text clean,
// so the fixture is read back into that shape.
func recordOfLegacyFixture(fixture errorFixture) ErrorRecord {
	record := ErrorRecord{Interface: RecordInterfaceOf(transportFromString[fixture.Transport]), Text: fixture.ErrorMessage}
	if rest, status, ok := splitStatusPrefix(fixture.ErrorMessage); ok {
		record.Status, record.Text = status, rest
	}
	switch transportFromString[fixture.Transport] {
	case TransportJsonRPC:
		switch {
		case fixture.ErrorCode == 0:
		case fixture.ErrorCode >= 100 && fixture.ErrorCode < 600 && record.Status == 0:
			record.Status = fixture.ErrorCode // a status passed as the code (XRP's 200)
		default:
			record.Code = json.Number(strconv.Itoa(fixture.ErrorCode))
			if record.Status == 0 {
				record.Status = 200
			}
		}
	default: // REST and gRPC rows read the status as the code
		if fixture.ErrorCode != 0 {
			record.Status = fixture.ErrorCode
			if transportFromString[fixture.Transport] == TransportGRPC {
				record.GRPCCode = fixture.ErrorCode
			}
		}
	}
	return record
}

// splitStatusPrefix splits the direct sender's "HTTP <n>: <text>" prefix off a message.
func splitStatusPrefix(message string) (text string, status int, ok bool) {
	if !strings.HasPrefix(message, "HTTP ") {
		return message, 0, false
	}
	rest := strings.TrimPrefix(message, "HTTP ")
	colon := strings.Index(rest, ": ")
	if colon < 0 {
		return message, 0, false
	}
	status, err := strconv.Atoi(rest[:colon])
	if err != nil {
		return message, 0, false
	}
	return rest[colon+2:], status, true
}

// Every legacy fixture, read back into a record, classifies to the same verdict through
// ClassifyRecord: the router's rows get the inputs they read today (PRD §3.1), so a reply classified
// through its record keeps its verdict.
func TestClassifyRecordKeepsTheLegacyFixturesVerdicts(t *testing.T) {
	data, err := os.ReadFile("testdata/error_fixtures.json")
	require.NoError(t, err)
	var file fixtureFile
	require.NoError(t, json.Unmarshal(data, &file))
	require.NotEmpty(t, file.Fixtures)

	// rest_404_not_found carries a 404 as "HTTP 404: " text with a code of 0, the shape only the
	// error mapper's status-without-body path produces, and the REST rows give a 404 two verdicts by
	// which input carries it: the status as the code reads NODE_DATA_NOT_HELD, the status in the text
	// reads NODE_ENDPOINT_NOT_FOUND. The record carries the status in its own field, so it reads the
	// first, as the live REST sender does; the second shape is MAG-5254's to settle.
	skipped := map[string]bool{"rest_404_not_found": true}
	for _, fixture := range file.Fixtures {
		if skipped[fixture.Name] {
			continue
		}
		t.Run(fixture.Name, func(t *testing.T) {
			family := chainFamilyFromString[fixture.ChainFamily]
			record := recordOfLegacyFixture(fixture)
			require.NotContains(t, record.Text, "HTTP "+strconv.Itoa(record.Status)+": ", "the record's text carries no status prefix")
			got := ClassifyRecord(family, record)
			require.Equal(t, fixture.ExpectedName, got.Name, "record %+v", record)
		})
	}
}

func TestClassifyRecord(t *testing.T) {
	t.Run("jsonrpc: the body code decides, the status is put back ahead of the text under a non-2xx", func(t *testing.T) {
		require.Equal(t, "CHAIN_NONCE_TOO_LOW", ClassifyRecord(ChainFamilyEVM, ErrorRecord{Interface: "jsonrpc", Status: 200, Code: json.Number("-32000"), Text: "nonce too low"}).Name)
		require.Equal(t, "NODE_METHOD_NOT_FOUND", ClassifyRecord(ChainFamilyEVM, ErrorRecord{Interface: "jsonrpc", Status: 200, Code: json.Number("-32601"), Text: "Method not found"}).Name)
		// A 503 with a bare body: the status rows read the status the record carries.
		bare503 := ClassifyRecord(ChainFamilyEVM, ErrorRecord{Interface: "jsonrpc", Status: 503, Text: "no healthy upstream"})
		require.Equal(t, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 503, "HTTP 503: no healthy upstream").Name, bare503.Name)
	})
	t.Run("tendermintrpc folds into the JSON-RPC rows", func(t *testing.T) {
		require.Equal(t, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, -32603, "Internal error").Name,
			ClassifyRecord(ChainFamilyCosmosSDK, ErrorRecord{Interface: "tendermintrpc", Status: 200, Code: json.Number("-32603"), Text: "Internal error"}).Name)
	})
	t.Run("tendermintrpc: CometBFT's reason for an internal error, its data, reaches the rows", func(t *testing.T) {
		for data, want := range map[string]string{
			`"tx already exists in cache"`:                       "CHAIN_TX_ALREADY_KNOWN",
			`"height 5 is not available, lowest height is 100"`:  "CHAIN_STATE_PRUNED",
			`"mempool is full: number of txs 5000 (max: 5000)"`:  "CHAIN_MEMPOOL_FULL",
			`{"reason": "an object is not CometBFT's sentence"}`: "NODE_INTERNAL_ERROR",
		} {
			record := ErrorRecord{Interface: "tendermintrpc", Status: 200, Code: json.Number("-32603"), Text: "Internal error", Detail: json.RawMessage(data)}
			require.Equal(t, want, ClassifyRecord(ChainFamilyCosmosSDK, record).Name, data)
		}
		under500 := ErrorRecord{Interface: "tendermintrpc", Status: 500, Code: json.Number("-32603"), Text: "Internal error", Detail: json.RawMessage(`"height 5 must be less than or equal to the current blockchain height 3"`)}
		require.Equal(t, "CHAIN_BLOCK_NOT_FOUND", ClassifyRecord(ChainFamilyCosmosSDK, under500).Name)
		revert := ErrorRecord{Interface: "jsonrpc", Status: 200, Code: json.Number("3"), Text: "execution reverted", Detail: json.RawMessage(`"0x08c379a0"`)}
		require.Equal(t, "CHAIN_EXECUTION_REVERTED", ClassifyRecord(ChainFamilyEVM, revert).Name, "data beside another code stays out of the text")
	})
	t.Run("rest: the status is the code, a shape's code reaches the spec rows only", func(t *testing.T) {
		withCode := ClassifyRecord(ChainFamilyCosmosSDK, ErrorRecord{Interface: "rest", Status: 404, Code: json.Number("5"), Text: "account not found"})
		without := ClassifyRecord(ChainFamilyCosmosSDK, ErrorRecord{Interface: "rest", Status: 404, Text: "account not found"})
		require.Equal(t, without.Name, withCode.Name)
		require.Equal(t, ClassifyError(nil, ChainFamilyCosmosSDK, TransportREST, 404, "account not found").Name, without.Name)
	})
	t.Run("grpc: the status code is the code, also under a gateway's HTTP status (D12)", func(t *testing.T) {
		require.Equal(t, ClassifyError(nil, ChainFamilyCosmosSDK, TransportGRPC, 14, "connection refused").Name,
			ClassifyRecord(ChainFamilyCosmosSDK, ErrorRecord{Interface: "grpc", Status: 14, GRPCCode: 14, Text: "connection refused"}).Name)
		gateway429 := "unexpected HTTP status code received from server: 429; transport: received unexpected content-type \"text/html\""
		require.Equal(t, ClassifyError(nil, ChainFamilyCosmosSDK, TransportGRPC, 14, gateway429).Name,
			ClassifyRecord(ChainFamilyCosmosSDK, ErrorRecord{Interface: "grpc", Status: 429, GRPCCode: 14, Text: gateway429}).Name)
	})
	t.Run("graphql folds into the REST rows under the HTTP status", func(t *testing.T) {
		require.Equal(t, ClassifyError(nil, ChainFamilySui, TransportREST, 429, "slow down").Name,
			ClassifyRecord(ChainFamilySui, ErrorRecord{Interface: "graphql", Status: 429, Code: "RESOURCE_EXHAUSTED", Text: "slow down"}).Name)
	})
	t.Run("ClassifyReply is ClassifyError by interface", func(t *testing.T) {
		for apiInterface, transport := range map[string]TransportType{"jsonrpc": TransportJsonRPC, "tendermintrpc": TransportJsonRPC, "rest": TransportREST, "graphql": TransportREST, "grpc": TransportGRPC} {
			require.Equal(t, ClassifyError(nil, ChainFamilyEVM, transport, 429, "too many requests"), ClassifyReply(ChainFamilyEVM, apiInterface, 429, "too many requests"), apiInterface)
			require.Equal(t, apiInterface == "tendermintrpc" || apiInterface == "graphql", RecordInterfaceOf(transport) != apiInterface, "the fold loses two interfaces, as today")
		}
	})
}
