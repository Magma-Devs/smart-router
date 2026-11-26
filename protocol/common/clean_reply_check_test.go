package common

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func checkedResult(body string, statusCode int) *RelayResult {
	result := &RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(body)}, StatusCode: statusCode}
	result.MarkReplyCheckedClean()
	return result
}

// The record holds for the reply and the status code that were checked.
func TestCleanReplyCheck_HoldsForTheCheckedReply(t *testing.T) {
	result := checkedResult(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, 200)
	require.True(t, result.ReplyCheckedClean())

	// A copy of the result carries the record, as the results manager's copy does.
	stored := *result
	require.True(t, stored.ReplyCheckedClean())

	// So does a result the record was copied to with the reply.
	other := &RelayResult{Reply: result.Reply, StatusCode: result.StatusCode, ReplyCheck: result.ReplyCheck}
	require.True(t, other.ReplyCheckedClean())

	// Metadata added to the reply leaves its bytes as checked.
	result.Reply.Metadata = append(result.Reply.Metadata, pairingtypes.Metadata{Name: "x", Value: "y"})
	require.True(t, result.ReplyCheckedClean())
}

// A reply that is replaced, cut, or filed under another status code is not the reply that was
// checked, and neither is a result that was never marked.
func TestCleanReplyCheck_DoesNotHoldForAnotherReply(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"result":"0x1"}`

	require.False(t, (&RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(body)}, StatusCode: 200}).ReplyCheckedClean(), "never marked")
	require.False(t, (*RelayResult)(nil).ReplyCheckedClean())

	replaced := checkedResult(body, 200)
	replaced.Reply.Data = []byte(body) // the same text in other bytes: a redacted or rebuilt reply
	require.False(t, replaced.ReplyCheckedClean(), "replaced bytes")

	cut := checkedResult(body, 200)
	cut.Reply.Data = cut.Reply.Data[:len(cut.Reply.Data)-1]
	require.False(t, cut.ReplyCheckedClean(), "shorter reply")

	shifted := checkedResult(body, 200)
	shifted.Reply.Data = shifted.Reply.Data[1:]
	require.False(t, shifted.ReplyCheckedClean(), "reply starting elsewhere")

	restatused := checkedResult(body, 200)
	restatused.StatusCode = 500
	require.False(t, restatused.ReplyCheckedClean(), "another status code")

	dropped := checkedResult(body, 200)
	dropped.Reply = nil
	require.False(t, dropped.ReplyCheckedClean(), "no reply")

	otherResult := checkedResult(body, 200)
	borrowed := &RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(body)}, StatusCode: 200, ReplyCheck: otherResult.ReplyCheck}
	require.False(t, borrowed.ReplyCheckedClean(), "a record copied without its reply")
}

// An empty reply has no bytes to name, so it is never recorded and is checked where it lands.
func TestCleanReplyCheck_AnEmptyReplyIsNotRecorded(t *testing.T) {
	for _, data := range [][]byte{nil, {}} {
		result := &RelayResult{Reply: &pairingtypes.RelayReply{Data: data}, StatusCode: 200}
		result.MarkReplyCheckedClean()
		require.False(t, result.ReplyCheckedClean())
	}
	var none *RelayResult
	none.MarkReplyCheckedClean() // no reply to record, and no panic
	noReply := &RelayResult{StatusCode: 200}
	noReply.MarkReplyCheckedClean()
	require.False(t, noReply.ReplyCheckedClean())
}
