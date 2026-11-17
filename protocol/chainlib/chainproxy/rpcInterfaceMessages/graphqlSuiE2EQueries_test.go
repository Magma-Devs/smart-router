package rpcInterfaceMessages

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// suiE2EQueriesFile holds the queries of Sui's own GraphQL end-to-end tests. Read README.md beside
// it for where they come from and the licence they are under.
const suiE2EQueriesFile = "testdata/sui-e2e-queries/queries.json"

var (
	updateSuiE2EQueries = flag.String("update-sui-e2e-queries", "",
		"rewrite "+suiE2EQueriesFile+" from a checkout of MystenLabs/sui at this path")
	suiE2EQueriesCommit = flag.String("sui-e2e-queries-commit", "",
		"the commit of the MystenLabs/sui checkout -update-sui-e2e-queries reads, recorded in the file")
)

type suiE2EQueries struct {
	Source  string          `json:"source"`
	Commit  string          `json:"commit"`
	License string          `json:"license"`
	Rule    string          `json:"rule"`
	Queries []suiE2EQueryAt `json:"queries"`
}

type suiE2EQueryAt struct {
	// At is the test file and the line of its `//# run-graphql` command.
	At    string `json:"at"`
	Query string `json:"query"`
}

// TestParseGraphQLMsgServesEverySuiE2EQuery is the guard MAG-4351's decision asks for. Refusing a
// query the router cannot read is only safe while the router's parser is not stricter than Sui's: a
// query Sui runs and the router refuses is a valid request the customer can no longer send. So every
// query of Sui's own end-to-end tests, which Sui's server parses and answers, must be served by the
// router: parsed, an operation selected, its root fields named, and no refusal of any kind.
//
// A new refusal that turns one of these away has to be a decided one, and the failure names the
// test and line so the decision can be checked against the query.
func TestParseGraphQLMsgServesEverySuiE2EQuery(t *testing.T) {
	if *updateSuiE2EQueries != "" {
		writeSuiE2EQueries(t, *updateSuiE2EQueries, *suiE2EQueriesCommit)
	}

	raw, err := os.ReadFile(suiE2EQueriesFile)
	require.NoError(t, err)
	var corpus suiE2EQueries
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.NotEmpty(t, corpus.Commit, "the file records the Sui commit it was read from")
	// The count is pinned so a regeneration that loses queries says so here.
	require.Len(t, corpus.Queries, 538, "the queries of Sui %s", corpus.Commit)

	for _, entry := range corpus.Queries {
		body, err := json.Marshal(map[string]string{"query": entry.Query})
		require.NoError(t, err)
		message, err := ParseGraphQLMsg(body)
		require.NoError(t, err, "Sui's own test %s at Sui %s is refused by the router", entry.At, corpus.Commit)
		require.NotEmpty(t, message.RootFieldNames(), entry.At)
	}
}

// suiSnapshotTask is a task header in a Sui transactional test's snapshot: "task 5, lines 18-30:".
// The first line number is the line of the task's own `//#` command in the test file.
var suiSnapshotTask = regexp.MustCompile(`(?m)^task \d+, lines? (\d+)(?:-\d+)?:$`)

// writeSuiE2EQueries reads every `//# run-graphql` query of Sui's transactional GraphQL tests,
// crates/sui-indexer-alt-e2e-tests/tests/graphql under suiRoot, and writes them to
// suiE2EQueriesFile.
//
// A query is the text of the task's block comment, unwrapped as Sui's own runner unwraps it. It is
// kept as Sui wrote it: placeholders such as @{obj_1_0} sit inside string literals, where they do
// not change how the document parses. A query whose reply in the test's snapshot is
// GRAPHQL_PARSE_FAILED is left out, because Sui's parser does not read it either.
func writeSuiE2EQueries(t *testing.T, suiRoot, commit string) {
	t.Helper()
	require.NotEmpty(t, commit, "-sui-e2e-queries-commit names the commit of the checkout")
	testsDir := filepath.Join(suiRoot, "crates", "sui-indexer-alt-e2e-tests", "tests")
	graphqlDir := filepath.Join(testsDir, "graphql")

	var moveFiles []string
	require.NoError(t, filepath.WalkDir(graphqlDir, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".move") {
			moveFiles = append(moveFiles, path)
		}
		return err
	}))
	sort.Strings(moveFiles)
	require.NotEmpty(t, moveFiles, "no transactional tests under %s", graphqlDir)

	corpus := suiE2EQueries{
		Source:  "https://github.com/MystenLabs/sui/tree/" + commit + "/crates/sui-indexer-alt-e2e-tests/tests/graphql",
		Commit:  commit,
		License: "Apache-2.0, Copyright (c) Mysten Labs, Inc. See LICENSE beside this file.",
		Rule: "Every //# run-graphql query of the transactional tests under graphql/, unwrapped as Sui's " +
			"unwrap_graphql_query does, whose reply in the test's .snap is not GRAPHQL_PARSE_FAILED. " +
			"Written by TestParseGraphQLMsgServesEverySuiE2EQuery -update-sui-e2e-queries.",
	}
	for _, path := range moveFiles {
		rel, err := filepath.Rel(testsDir, path)
		require.NoError(t, err)
		source, err := os.ReadFile(path)
		require.NoError(t, err)
		snapshot, err := os.ReadFile(strings.TrimSuffix(path, ".move") + ".snap")
		require.NoError(t, err, "every transactional test has a snapshot")
		replies := suiSnapshotReplies(string(snapshot))

		lines := strings.Split(string(source), "\n")
		for index, line := range lines {
			if !strings.HasPrefix(line, "//# run-graphql") {
				continue
			}
			// The task's text is its block comment, which ends at the first line that ends with */:
			// Sui refuses a query that holds */, and a `//` comment after the block is one the
			// runner keeps apart from the task (move-transactional-test-runner, tasks.rs taskify).
			end := index + 1
			for end < len(lines) && !strings.HasPrefix(lines[end], "//#") && !strings.HasSuffix(strings.TrimRight(lines[end], " \t\r"), "*/") {
				end++
			}
			require.Less(t, end, len(lines), "%s:%d: the block comment never closes", rel, index+1)
			require.False(t, strings.HasPrefix(lines[end], "//#"), "%s:%d: the block comment never closes", rel, index+1)
			query, err := unwrapSuiGraphQLQuery(strings.Join(lines[index+1:end+1], "\n"))
			require.NoError(t, err, "%s:%d", rel, index+1)
			reply, found := replies[index+1]
			require.True(t, found, "%s:%d has no task in its snapshot", rel, index+1)
			if strings.Contains(reply, "GRAPHQL_PARSE_FAILED") {
				continue
			}
			corpus.Queries = append(corpus.Queries, suiE2EQueryAt{
				At:    filepath.ToSlash(rel) + ":" + strconv.Itoa(index+1),
				Query: query,
			})
		}
	}

	out, err := json.MarshalIndent(corpus, "", " ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(suiE2EQueriesFile, append(out, '\n'), 0o600))
	t.Logf("wrote %d queries from %d tests to %s", len(corpus.Queries), len(moveFiles), suiE2EQueriesFile)
}

// suiSnapshotReplies maps each task of a snapshot, by the line of its command, to the task's output.
func suiSnapshotReplies(snapshot string) map[int]string {
	replies := map[int]string{}
	headers := suiSnapshotTask.FindAllStringSubmatchIndex(snapshot, -1)
	for i, header := range headers {
		end := len(snapshot)
		if i+1 < len(headers) {
			end = headers[i+1][0]
		}
		line, err := strconv.Atoi(snapshot[header[2]:header[3]])
		if err == nil {
			replies[line] = snapshot[header[1]:end]
		}
	}
	return replies
}

// unwrapSuiGraphQLQuery is Sui's unwrap_graphql_query (crates/sui-transactional-test-runner,
// src/test_adapter.rs), line for line: the query is the text inside a /* ... */ block comment, whose
// opening and closing lines may carry a ```graphql fence.
func unwrapSuiGraphQLQuery(text string) (string, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(text), "/*")
	if !ok {
		return "", errors.New("GraphQL queries must be wrapped in a /* ... */ block comment")
	}
	opening, rest, _ := strings.Cut(rest, "\n")
	fenced := false
	switch strings.TrimSpace(opening) {
	case "":
	case "```graphql":
		fenced = true
	default:
		return "", fmt.Errorf("unexpected text after /* in a GraphQL query: %s", opening)
	}
	rest, ok = strings.CutSuffix(rest, "*/")
	if !ok {
		return "", errors.New("GraphQL queries must end with */ on its own line")
	}
	query, closing := "", rest
	if newline := strings.LastIndex(rest, "\n"); newline >= 0 {
		query, closing = rest[:newline], rest[newline+1:]
	}
	if want := map[bool]string{false: "", true: "```"}[fenced]; strings.TrimSpace(closing) != want {
		return "", fmt.Errorf("unexpected text before */ in a GraphQL query: %s", closing)
	}
	if strings.Contains(query, "/*") || strings.Contains(query, "*/") {
		return "", errors.New("GraphQL queries cannot contain /* or */")
	}
	return query, nil
}
