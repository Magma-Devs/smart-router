package chainlib

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestRealSpecArchiveVerificationIsExtensionKeyed ties the attribution rule to the
// container shape the REAL parser builds from a REAL spec file.
//
// This is the test that would have caught MAG-3326's original defect. That version
// attributed failures on verification.Addon alone, and its unit tests manufactured
// VerificationKey{Addon: "archive"} — a shape no spec produces — so the suite was
// green while the ticket's motivating case ("attach an archive url to an otherwise
// healthy provider, fail the archive check, lose the provider") was unfixed.
//
// It cannot be verified against a live upstream: the archive check is
// eth_getBlockByNumber("earliest") expecting 0x0, and every public endpoint retains
// genesis, so no real node fails it. The spec file is the authority available here.
func TestRealSpecArchiveVerificationIsExtensionKeyed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "specs", "ethereum.json"))
	require.NoError(t, err, "specs/ethereum.json ships in this repo")

	var proposal struct {
		Proposal struct {
			Specs []spectypes.Spec `json:"specs"`
		} `json:"proposal"`
	}
	require.NoError(t, json.Unmarshal(raw, &proposal))

	var eth1 spectypes.Spec
	for _, spec := range proposal.Proposal.Specs {
		if spec.Index == "ETH1" {
			eth1 = spec
		}
	}
	require.Equal(t, "ETH1", eth1.Index)
	require.Empty(t, eth1.Imports, "ETH1 is a base spec, so this needs no import resolution")

	_, _, _, _, _, verifications := getServiceApis(eth1, spectypes.APIInterfaceJsonRPC)

	// The archive check lives on the BASE collection, scoped by extension.
	containers := verifications[VerificationKey{Addon: "", Extension: "archive"}][""]
	require.NotEmpty(t, containers, "ETH1 keys its archive check {Addon:\"\", Extension:\"archive\"}")

	var pruning *VerificationContainer
	for i := range containers {
		if containers[i].Name == "pruning" {
			pruning = &containers[i]
		}
	}
	require.NotNil(t, pruning)

	// Severity is omitted in the spec, and Fail is the zero value — which is why
	// this failure is fatal rather than advisory when it is not attributed.
	require.Equal(t, spectypes.ParseValue_Fail, pruning.Severity,
		"an omitted severity means Fail, so mis-attributing this costs the provider")
	require.Equal(t, "", pruning.Addon, "attributing on Addon alone finds \"\" and goes fatal")
	require.Equal(t, "archive", pruning.Extension)
	require.Equal(t, "archive", refusedServiceFor(*pruning),
		"the service refused must be the extension the node actually failed")

	// And the contrasting shape from the same file: an add-on with no extension
	// scope, which attributes to the add-on.
	traceContainers := verifications[VerificationKey{Addon: "trace", Extension: ""}][""]
	require.NotEmpty(t, traceContainers)
	for _, container := range traceContainers {
		if container.Name == "trace" {
			require.Equal(t, "trace", refusedServiceFor(container))
		}
	}
}
