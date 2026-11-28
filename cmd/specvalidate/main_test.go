package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunReportsFindingsAndExitsOne(t *testing.T) {
	dir := t.TempDir()
	proposal := `{"proposal":{"specs":[{"index":"SOLANA","api_collections":[{"collection_data":{"api_interface":"jsonrpc"}}],
		"errors":{"schema_version":1,"rows":[{"id":"solana.a","interface":"jsonrpc","code":1,"error":"NODE_RATE_LIMITED"}]}}]}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "solana.json"), []byte(proposal), 0o600))
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"-json", dir}, &stdout, &stderr), stderr.String())
	var findings []map[string]string
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &findings))
	require.Len(t, findings, 1)
	require.Equal(t, "4.2/error", findings[0]["rule"])
	require.Equal(t, "rule_violation", findings[0]["reason"])
}

func TestRunOnTheBundledSpecsIsClean(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"../../specs"}, &stdout, &stderr), stdout.String()+stderr.String())
}

func TestRunOnAPathThatIsNotThereExitsTwo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run([]string{filepath.Join(t.TempDir(), "missing")}, &stdout, &stderr))
}

// PRD §4.5: the table lava-specs' stateful check reads.
func TestWriteMethodsExportsTheLocksTable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"write-methods"}, &stdout, &stderr))
	var methods []struct{ Interface, Name string }
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &methods))
	require.Contains(t, methods, struct{ Interface, Name string }{"jsonrpc", "eth_sendRawTransaction"})
}
