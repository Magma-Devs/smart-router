package relay

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cache wire types are hand-written structs serialized as JSON, so cross-version
// compatibility rests on encoding/json semantics: absent fields decode to zero values
// and unknown fields are ignored. These tests lock both directions for the
// CacheRelayReply.IsNodeError extension (docs/SECONDARY-CACHE-DESIGN.md §7): an old
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
