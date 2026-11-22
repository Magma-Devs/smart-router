package endpointstate

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A GraphQL node answers most errors inside an HTTP 200. A head poll that reads only the head
// out of such a reply takes an erroring provider for a healthy one, and its healthy polls are
// what bring a removed provider back (MAG-5124). The poll must read the reply by the same rules
// a client request is read by (MAG-5121): only an answer is a healthy poll.
func TestEndpointPoller_FetchLatestBlockNum_GraphQLErrorReplyIsAFailedPoll(t *testing.T) {
	for name, body := range map[string]string{
		// The ticket's case: a Sui provider whose database is down.
		"node fault, no data": `{"data":null,"errors":[{"message":"Internal error occurred while processing request","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
		// The same fault beside a head the result parsing can read: before MAG-5124 this was a
		// healthy poll at checkpoint 334287093.
		"node fault beside a readable head": `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}},"errors":[{"message":"Internal error occurred while processing request","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
		// An error with no code is one the router cannot read; it is still not an answer.
		"unreadable error beside a readable head": `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}},"errors":[{"message":"something went wrong"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			poller := newReplyingSuiGraphQLPoller(t, body)

			_, err := poller.FetchLatestBlockNum(context.Background())
			require.Error(t, err, "a reply that carries an error is not a healthy poll")
			require.NotErrorIs(t, err, common.StatusCodeError429, "and it is not a rate limit")
			require.Zero(t, poller.latestBlock, "the head beside an error must not be stored")
		})
	}
}
