package rpcInterfaceMessages

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

const ogyLedger = "lkwrt-vyaaa-aaaaq-aadhq-cai"

func TestIsICCanisterPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/api/v2/canister/" + ogyLedger + "/query", true},
		{"/api/v3/canister/" + ogyLedger + "/call", true},
		{"/api/v2/canister/" + ogyLedger + "/read_state", true},
		// Node-level endpoints carry no canister and need no crafted body.
		{"/api/v2/status", false},
		{"/api/v2/subnet/abc-def/read_state", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := IsICCanisterPath(tt.path); got != tt.want {
			t.Errorf("IsICCanisterPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestDecodePrincipal(t *testing.T) {
	decoded, err := DecodePrincipal(ogyLedger)
	require.NoError(t, err)
	// Principal bytes are the base32 payload minus the 4-byte CRC32 prefix.
	require.NotEmpty(t, decoded)
	require.Less(t, len(decoded), 30, "a canister principal is short")

	_, err = DecodePrincipal("!!!not-base32!!!")
	require.Error(t, err)
}

// The envelope must be valid CBOR carrying exactly the fields the IC expects,
// with an ingress_expiry in the future — the field a static spec template
// cannot express, and the entire reason this is built in Go.
func TestCraftICQueryBody(t *testing.T) {
	before := time.Now()
	body, err := CraftICQueryBody("/api/v2/canister/"+ogyLedger+"/query", "icrc1_symbol")
	require.NoError(t, err)

	var envelope map[string]interface{}
	require.NoError(t, cbor.Unmarshal(body, &envelope), "crafted body must be valid CBOR")

	content, ok := envelope["content"].(map[interface{}]interface{})
	require.True(t, ok, "envelope must have a content map")

	require.Equal(t, "query", content["request_type"])
	require.Equal(t, "icrc1_symbol", content["method_name"])
	require.Equal(t, []byte{0x04}, content["sender"], "sender must be the anonymous principal")
	require.Equal(t, []byte("DIDL\x00\x00"), content["arg"], "no-arg call must send empty Candid args")

	// Anonymous requests must omit the signature fields entirely.
	require.NotContains(t, envelope, "sender_sig")
	require.NotContains(t, envelope, "sender_pubkey")
	require.NotContains(t, envelope, "sender_delegation")

	expiry, ok := content["ingress_expiry"].(uint64)
	require.True(t, ok, "ingress_expiry must be an unsigned integer of nanoseconds")
	require.Greater(t, expiry, uint64(before.UnixNano()), "expiry must be in the future")
	require.Less(t, expiry, uint64(before.Add(5*time.Minute).UnixNano()),
		"expiry must stay inside the IC's accepted window")
}

// Two crafts must differ: a fixed expiry would work for minutes and then fail
// permanently, which is exactly the failure a spec template would have had.
func TestCraftICQueryBodyExpiryIsFresh(t *testing.T) {
	path := "/api/v2/canister/" + ogyLedger + "/query"
	first, err := CraftICQueryBody(path, "icrc1_symbol")
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)
	second, err := CraftICQueryBody(path, "icrc1_symbol")
	require.NoError(t, err)
	require.NotEqual(t, hex.EncodeToString(first), hex.EncodeToString(second),
		"each craft must carry a freshly computed ingress_expiry")
}

func TestCraftICQueryBodyErrors(t *testing.T) {
	_, err := CraftICQueryBody("/api/v2/canister/"+ogyLedger+"/query", "")
	require.Error(t, err, "a missing method name must be reported, not silently sent")
	require.Contains(t, err.Error(), "function_template")

	_, err = CraftICQueryBody("/api/v2/status", "icrc1_symbol")
	require.Error(t, err, "a path with no canister id must be rejected")
	require.Contains(t, err.Error(), "canister id")
}

func TestDecodeCandidPrimitive(t *testing.T) {
	tests := []struct {
		name    string
		blob    []byte
		want    string
		wantErr string
	}{
		{
			// DIDL, no type table, 1 value, text(0x71), len 3, "OGY"
			name: "text (icrc1_symbol)",
			blob: append([]byte("DIDL\x00\x01\x71\x03"), []byte("OGY")...),
			want: "OGY",
		},
		{
			// nat(0x7d) with a LEB128 value of 8
			name: "nat",
			blob: []byte("DIDL\x00\x01\x7d\x08"),
			want: "8",
		},
		{
			// nat8(0x7b), single raw byte — icrc1_decimals returns this
			name: "nat8 (icrc1_decimals)",
			blob: []byte("DIDL\x00\x01\x7b\x08"),
			want: "8",
		},
		{
			// nat64(0x78), little-endian
			name: "nat64",
			blob: []byte{'D', 'I', 'D', 'L', 0x00, 0x01, 0x78, 0x01, 0x02, 0, 0, 0, 0, 0, 0},
			want: "513", // 0x0201 little-endian
		},
		{
			name: "bool true",
			blob: []byte("DIDL\x00\x01\x7e\x01"),
			want: "true",
		},
		{
			// LEB128 of 1380764 (the OGY ledger's live log_length):
			//   28 + 35<<7 + 84<<14 = 28 + 4480 + 1376256
			name: "large nat (log_length)",
			blob: []byte{'D', 'I', 'D', 'L', 0x00, 0x01, 0x7d, 0x9c, 0xa3, 0x54},
			want: "1380764",
		},
		{
			name:    "not Candid at all",
			blob:    []byte("not candid"),
			wantErr: "missing DIDL magic",
		},
		{
			name:    "a type table means a composite type we do not decode",
			blob:    []byte("DIDL\x01\x01\x7d\x08"),
			wantErr: "type table",
		},
		{
			name:    "multiple return values are not decoded",
			blob:    []byte("DIDL\x00\x02\x7d\x08\x7d\x09"),
			wantErr: "expected exactly 1 return value",
		},
		{
			name:    "truncated text is reported, not silently truncated",
			blob:    append([]byte("DIDL\x00\x01\x71\x10"), []byte("short")...),
			wantErr: "malformed Candid text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeCandidPrimitive(tt.blob)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// The full response path: an IC query envelope must come out of the transcoder
// with reply.arg already decoded, so a spec can compare against a readable
// expected_value rather than the hex of a Candid encoding.
func TestCBORTranscodeDecodesICReplyArg(t *testing.T) {
	body, err := cbor.Marshal(map[interface{}]interface{}{
		"status": "replied",
		"reply": map[interface{}]interface{}{
			"arg": append([]byte("DIDL\x00\x01\x71\x03"), []byte("OGY")...),
		},
	})
	require.NoError(t, err)

	rm := RestMessage{Path: "/api/v2/canister/x/query", Encoding: "cbor"}
	input, err := rm.NewParsableRPCInput(body)
	require.NoError(t, err)
	require.Contains(t, string(input.GetResult()), `"OGY"`,
		"reply.arg must be Candid-decoded so expected_value can stay readable")
	require.NotContains(t, string(input.GetResult()), "4449444c",
		"the raw Candid hex must not leak through once decoded")
}

// A reply shape we cannot decode must degrade to hex rather than fail the whole
// response — the transcode is best-effort on top of a guaranteed CBOR decode.
func TestCBORTranscodeLeavesUndecodableArgAsHex(t *testing.T) {
	body, err := cbor.Marshal(map[interface{}]interface{}{
		"status": "replied",
		"reply": map[interface{}]interface{}{
			"arg": []byte("DIDL\x01\x01\x7d\x08"), // has a type table
		},
	})
	require.NoError(t, err)

	rm := RestMessage{Path: "/api/v2/canister/x/query", Encoding: "cbor"}
	input, err := rm.NewParsableRPCInput(body)
	require.NoError(t, err, "an undecodable arg must not fail the transcode")
	require.Contains(t, string(input.GetResult()), hex.EncodeToString([]byte("DIDL\x01\x01\x7d\x08")))
}
