package chainlib

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// replyFixture is one entry of testdata/reply_fixtures.json: a reply as a node sent it, per
// interface, with what the reader must make of it (PRD §3.1, §8.2 "Fixtures per interface").
type replyFixture struct {
	Name         string                   `json:"name"`
	Interface    string                   `json:"interface"`
	ChainFamily  string                   `json:"chain_family"`
	Method       string                   `json:"method"`
	Status       int                      `json:"status"`
	Body         json.RawMessage          `json:"body"`
	BodyText     string                   `json:"body_text"`
	Shapes       []*spectypes.ErrorShape  `json:"shapes"`
	PlainText    bool                     `json:"plain_text"`
	CheckerText  string                   `json:"checker_text"`
	Occurrences  []replyFixtureOccurrence `json:"graphql_occurrences"`
	AnswerRows   []*spectypes.ErrorRow    `json:"answer_rows"`
	NodeSoftware string                   `json:"node_software"`
	Version      string                   `json:"version"`
	ExpectedKind string                   `json:"expected_kind"`
	Records      []replyFixtureRecord     `json:"expected_records"`
	ExpectedName string                   `json:"expected_name"`
	ExpectedRows []string                 `json:"expected_answer_rows"`
	Comment      string                   `json:"_comment"`
}

type replyFixtureOccurrence struct {
	Field string `json:"field"`
	Alias string `json:"alias"`
}

type replyFixtureRecord struct {
	Code       json.RawMessage `json:"code"`
	Text       string          `json:"text"`
	DetailText string          `json:"detail_text"`
	Path       []string        `json:"path"`
	InSuccess  bool            `json:"in_success"`
}

var replyFixtureFamilies = map[string]common.ChainFamily{
	"EVM": common.ChainFamilyEVM, "XRP": common.ChainFamilyXRP, "CosmosSDK": common.ChainFamilyCosmosSDK,
	"TON": common.ChainFamilyTON, "Aptos": common.ChainFamilyAptos, "Sui": common.ChainFamilySui,
}

func loadReplyFixtures(t *testing.T) []replyFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/reply_fixtures.json")
	require.NoError(t, err)
	var file struct {
		Fixtures []replyFixture `json:"fixtures"`
	}
	require.NoError(t, json.Unmarshal(data, &file))
	require.NotEmpty(t, file.Fixtures)
	return file.Fixtures
}

// Each fixture runs through its interface's reader, shapes and record, not ClassifyError alone
// (MAG-5595 Done when): the kind the reply is, the records it yields, and the verdict the router's
// rows give the first record through ClassifyRecord.
func TestReplyFixtures(t *testing.T) {
	fixtures := loadReplyFixtures(t)
	seen := map[string]int{}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			family, ok := replyFixtureFamilies[fixture.ChainFamily]
			require.True(t, ok, "unknown family %q", fixture.ChainFamily)
			require.NotEmpty(t, fixture.NodeSoftware, "the evidence names the node software")
			require.NotEmpty(t, fixture.Version, "and its version")
			seen[fixture.Interface]++
			body := []byte(fixture.BodyText)
			if len(fixture.Body) > 0 {
				body = fixture.Body
			}
			policy := &spectypes.ErrorsSection{SchemaVersion: spectypes.ErrorsSchemaVersion, Shapes: fixture.Shapes}

			kind := readFixtureReply(fixture, policy, body)
			require.Equal(t, fixture.ExpectedKind, kind.String())
			if kind == common.ReplyAnswer {
				if fixture.AnswerRows != nil {
					doc, ok := common.DecodeJSONDocument(body)
					require.True(t, ok)
					occurrences := make([]common.GraphQLOccurrence, 0, len(fixture.Occurrences))
					for _, occurrence := range fixture.Occurrences {
						occurrences = append(occurrences, common.GraphQLOccurrence{Field: occurrence.Field, Alias: occurrence.Alias})
					}
					var matched []string
					for _, match := range common.MatchAnswerRowPerOccurrence(fixture.AnswerRows, occurrences, false, true, doc) {
						matched = append(matched, match.Row.ID)
					}
					require.Equal(t, fixture.ExpectedRows, matched)
				}
				return
			}

			// Through the builder every reader uses, with the checker's words where the fixture gives
			// them (an error that sits inside a result, which Step 1 turns into a shape).
			built := RecordsFromReply(policy, fixture.Interface, fixture.Method, fixture.Status, body, fixture.CheckerText)
			require.Len(t, built, len(fixture.Records), "records: %+v", built)
			for i, want := range fixture.Records {
				got := built[i]
				require.Equal(t, fixture.Interface, got.Interface, "record %d", i)
				require.Equal(t, fixture.Status, got.Status, "record %d", i)
				if len(want.Code) > 0 {
					require.True(t, common.JSONValueEquals(got.Code, want.Code), "record %d code: got %v want %s", i, got.Code, want.Code)
				} else {
					require.Nil(t, got.Code, "record %d carries no code", i)
				}
				require.Equal(t, want.Text, got.Text, "record %d text", i)
				require.Equal(t, want.DetailText, got.DetailText(), "record %d detail", i)
				require.Equal(t, want.Path, got.Path, "record %d path", i)
				require.Equal(t, want.InSuccess, got.InSuccess, "record %d in_success", i)
			}
			if fixture.ExpectedName != "" {
				require.Equal(t, fixture.ExpectedName, common.ClassifyRecord(family, built[0]).Name)
			}
		})
	}
	for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest, spectypes.APIInterfaceGraphQL, spectypes.APIInterfaceGrpc} {
		assert.Positive(t, seen[apiInterface], "no fixture on %s", apiInterface)
	}
}

// readFixtureReply reads the fixture's body the way its interface's standard reading does, with the
// fixture's shapes and plain-text flag. The checker's text stands in for a chain's reply check in
// router code (XRPL's engine result, Tendermint's response code, Cosmos' tx_response on REST), which
// the standard reading cannot see: with it the reply is an error.
func readFixtureReply(fixture replyFixture, policy *spectypes.ErrorsSection, body []byte) common.ReplyKind {
	var kind common.ReplyKind
	switch fixture.Interface {
	case spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC:
		kind, _ = common.ReadJSONRPCReply(fixture.Interface, fixture.Status, body)
	case spectypes.APIInterfaceRest:
		kind, _ = common.ReadRESTReply(policy.Shapes, fixture.Method, fixture.PlainText, fixture.Status, body)
	case spectypes.APIInterfaceGraphQL:
		kind, _ = common.ReadGraphQLReply(fixture.Status, body)
	case spectypes.APIInterfaceGrpc:
		kind, _ = common.ReadGRPCReply(fixture.Status, "", nil, false)
	}
	if kind == common.ReplyAnswer && fixture.CheckerText != "" {
		kind = common.ReplyWellFormedError
	}
	return kind
}
