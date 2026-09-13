package relay

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cache wire types are hand-written structs serialized as JSON, so cross-version
// compatibility rests on encoding/json semantics: absent fields decode to zero values
// and unknown fields are ignored. These tests lock both directions for the
// CacheRelayReply.IsNodeError extension: an old
// cache server's reply (no is_node_error) must decode to false, and a newer peer's
// extra fields must not break this reader.

func TestCacheRelayReplyLegacyPayloadDecodesIsNodeErrorFalse(t *testing.T) {
	legacy := []byte(`{"reply":{"data":"aGVsbG8=","latest_block":42},"seen_block":42}`)
	var reply CacheRelayReply
	require.NoError(t, json.Unmarshal(legacy, &reply))
	require.False(t, reply.GetIsNodeError())
	require.Equal(t, int64(42), reply.GetSeenBlock())
	require.NotNil(t, reply.GetReply())
}

func TestCacheRelayReplyUnknownFieldsIgnored(t *testing.T) {
	future := []byte(`{"seen_block":7,"is_node_error":true,"some_future_field":"x"}`)
	var reply CacheRelayReply
	require.NoError(t, json.Unmarshal(future, &reply))
	require.True(t, reply.GetIsNodeError())
}

func TestCacheRelayReplyStatusCodeWireCompat(t *testing.T) {
	// legacy payload without status_code decodes to zero = unknown
	var legacy CacheRelayReply
	require.NoError(t, json.Unmarshal([]byte(`{"seen_block":7}`), &legacy))
	require.Equal(t, 0, legacy.GetStatusCode())

	// recorded status round-trips
	raw, err := json.Marshal(CacheRelayReply{StatusCode: 429})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"status_code":429`)
	var out CacheRelayReply
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Equal(t, 429, out.GetStatusCode())

	var nilReply *CacheRelayReply
	require.Equal(t, 0, nilReply.GetStatusCode())
}

func TestCacheRelayReplyIsNodeErrorRoundTrip(t *testing.T) {
	in := CacheRelayReply{SeenBlock: 9, IsNodeError: true}
	raw, err := json.Marshal(in)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"is_node_error":true`)

	var out CacheRelayReply
	require.NoError(t, json.Unmarshal(raw, &out))
	require.True(t, out.GetIsNodeError())

	var nilReply *CacheRelayReply
	require.False(t, nilReply.GetIsNodeError())
}

// The two tests below lock the FIELD LIST, not any one field's behaviour. The
// four tests above each name only the one or two fields they care about, so a
// seventh field can be added and all four stay green.
//
// That matters because the entry crosses a zone boundary: a reader in another
// zone decides what to trust and what to drop, field by field. A field added
// without anybody noticing is a field with no defence and no test.
//
// Why reflection and not json.Marshal alone. Marshalling a value asks
// encoding/json the same question the cache asks it, which is the right
// question -- but omitempty makes it unable to answer this one. A field added
// later with `json:"x,omitempty"` is absent from any literal written today, so
// it marshals to nothing and a marshal-based check stays green while the field
// ships. Measured: that is exactly what happened to the first version of this
// test. Reflection reads the DECLARATION, so it sees every field whatever its
// tag says.
//
// Marshalling still earns its place below, as the cross-check that reflection
// and encoding/json agree about the names.
//
// When one of these fails, the fix is one line here AND a message to whoever
// tests this cache from outside, because their coverage list is keyed on these
// names.

// declaredWireNames returns the JSON name of every field a struct type declares,
// read from the declaration rather than from a value.
func declaredWireNames(t *testing.T, v any) []string {
	t.Helper()
	typ := reflect.TypeOf(v)
	names := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		require.NotEmpty(t, tag,
			"field %q carries no JSON name, so what a cross-zone reader sees "+
				"depends on the Go field name. Give it an explicit tag.", field.Name)
		require.NotEqual(t, "-", tag,
			"field %q is tagged never-serialise. That is a real change to what "+
				"crosses the zone boundary -- decide it deliberately, then update "+
				"this test.", field.Name)
		names = append(names, tag)
	}
	return names
}

// marshalledWireNames returns the JSON names a populated value actually produces,
// as the cross-check that the declaration above matches what encoding/json emits.
func marshalledWireNames(t *testing.T, v any) []string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	return names
}

func TestCacheRelayReplyFieldListIsLocked(t *testing.T) {
	expected := []string{
		"reply",
		"optional_metadata",
		"seen_block",
		"blocks_hashes_to_heights",
		"is_node_error",
		"status_code",
	}
	const why = "the cached entry's field list changed. A reader in another zone " +
		"decides what to trust field by field, so a new field arrives with no " +
		"defence and no test until somebody is told. Update this list, and tell " +
		"whoever tests this cache from outside."

	require.ElementsMatch(t, expected, declaredWireNames(t, CacheRelayReply{}), why)

	// Every field populated, so the cross-check compares like with like.
	require.ElementsMatch(t, expected, marshalledWireNames(t, CacheRelayReply{
		Reply:                 &RelayReply{},
		OptionalMetadata:      []Metadata{{}},
		SeenBlock:             1,
		BlocksHashesToHeights: []*BlockHashToHeight{{}},
		IsNodeError:           true,
		StatusCode:            200,
	}), "what encoding/json emits and what the struct declares disagree")
}

func TestRelayReplyFieldListIsLocked(t *testing.T) {
	expected := []string{
		"data",
		"sig",
		"latest_block",
		"finalized_blocks_hashes",
		"sig_blocks",
		"metadata",
	}
	const why = "the inner reply's field list changed. Same reading as the test " +
		"above: this is what a reader in another zone sees, field by field."

	require.ElementsMatch(t, expected, declaredWireNames(t, RelayReply{}), why)

	require.ElementsMatch(t, expected, marshalledWireNames(t, RelayReply{
		Data:                  []byte("x"),
		Sig:                   []byte("x"),
		LatestBlock:           1,
		FinalizedBlocksHashes: []byte("x"),
		SigBlocks:             []byte("x"),
		Metadata:              []Metadata{{}},
	}), "what encoding/json emits and what the struct declares disagree")
}
