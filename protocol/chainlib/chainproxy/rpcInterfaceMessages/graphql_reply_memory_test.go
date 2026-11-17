package rpcInterfaceMessages

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// largeGraphQLAnswer is a read's answer of about size bytes: one big data object.
func largeGraphQLAnswer(size int) []byte {
	var body strings.Builder
	body.WriteString(`{"data":{"checkpoints":{"nodes":[`)
	for body.Len() < size {
		body.WriteString(`{"digest":"BGbVv2NxQ8kq3Jm2Yh7Wf9Zr6Lp4Dt1Sc5Nv8Bx0Ae3Gu","sequenceNumber":123456789,"timestamp":"2026-11-17T10:00:00Z"},`)
	}
	body.WriteString(`{"digest":"x","sequenceNumber":1}]}}}`)
	return []byte(body.String())
}

// bytesAllocatedPerRun reports the bytes run allocates on average over runs calls, after one call
// to warm the decoder's reused buffers.
func bytesAllocatedPerRun(runs int, run func()) uint64 {
	run()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		run()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// TestGraphQLReadingALargeReplyDoesNotCopyIt pins that reading a reply's verdict costs memory
// unrelated to the size of its data. The verdict needs only to know that data is an object; kept
// as a json.RawMessage, every reading copied the whole of it (10.8 MB for one 5 MB reply read by
// the sender).
func TestGraphQLReadingALargeReplyDoesNotCopyIt(t *testing.T) {
	read := parsedRead(t)
	body := largeGraphQLAnswer(5 << 20)

	hasError, _, _ := read.ReplyVerdict(body, 200)
	require.False(t, hasError, "the large reply is an answer")

	// The decoder copies its input into a buffer it reuses from one call to the next (goccy/go-json
	// RuntimeContext.SetInput), so a reading may still pay one copy of the body when that buffer
	// was dropped by a collection. What must not come back is a copy per reading.
	const runs = 10
	perRead := bytesAllocatedPerRun(runs, func() { read.ReplyVerdict(body, 200) })
	require.Less(t, perRead, uint64(len(body)/4), "reading a %d-byte answer allocated %d bytes per reading", len(body), perRead)
}

// TestGraphQLReplyVerdictAgreesWithItsParts pins that one reading gives what CheckResponseError
// and ClassificationStatus give apart, on reads and writes.
func TestGraphQLReplyVerdictAgreesWithItsParts(t *testing.T) {
	write, err := ParseGraphQLMsg([]byte(`{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`))
	require.NoError(t, err)
	replies := []struct {
		status int
		body   string
	}{
		{200, `{"data":{"checkpoint":{"digest":"x"}}}`},
		{200, `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`},
		{200, `{"errors":[{"message":"what"}]}`},
		{200, `{"errors":[{"message":"Too many requests"}]}`},
		{200, `{}`},
		{200, `{"data":"ok","errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`},
		{429, `{"errors":[{"message":"m"}]}`},
		{503, `down`},
	}
	for _, message := range []*GraphQLMessage{parsedRead(t), write} {
		for _, reply := range replies {
			hasError, errorMessage, status := message.ReplyVerdict([]byte(reply.body), reply.status)
			wantError, wantMessage := message.CheckResponseError([]byte(reply.body), reply.status)
			require.Equal(t, wantError, hasError, reply.body)
			require.Equal(t, wantMessage, errorMessage, reply.body)
			require.Equal(t, message.ClassificationStatus([]byte(reply.body), reply.status), status, reply.body)
		}
	}
}

// TestGraphQLDataOfAnotherShapeIsStillRead pins the fallback reading: a data member that is not an
// object or null does not fit the no-copy decode, and the reply is read again keeping data raw.
func TestGraphQLDataOfAnotherShapeIsStillRead(t *testing.T) {
	read := parsedRead(t)
	require.Equal(t, effectFault, readEffect(t, read, `{"data":"ok","errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, 200),
		"the errors beside a data string are read")
	require.Equal(t, effectFault, readEffect(t, read, `{"data":[1,2]}`, 200),
		"a data list with no errors is not a GraphQL response")
	require.Equal(t, effectAnswer, readEffect(t, read, `{"data":{}}`, 200), "an empty data object is an answer")
}

func BenchmarkGraphQLReadLargeReply(b *testing.B) {
	read, _ := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	body := largeGraphQLAnswer(5 << 20)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read.ReplyVerdict(body, 200)
	}
}
