package chainlib

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// headDependent is a verification that needs the chain head resolved before it can run
// (no fixed Value, non-zero LatestDistance) — the kind that pulls in FetchLatestBlockNum.
func headDependent(name string) VerificationContainer {
	return VerificationContainer{Name: name, LatestDistance: 100}
}

// selfContained carries its own expected Value, so it never needs the chain head.
func selfContained(name string) VerificationContainer {
	return VerificationContainer{Name: name, Value: "0x1"}
}

// Regression for the skip-verifications bypass: the latest-block probe is a real relay
// to the upstream, and in Validate a failure aborts the provider outright. It used to be
// decided over ALL verifications — including skipped ones — so an operator who skipped
// every verification still had the node probed, and a rate-limited upstream still got
// its provider demoted. Skipped verifications must not pull the fetch in.
func TestNeedsLatestBlock(t *testing.T) {
	playbook := []struct {
		name          string
		skip          []string
		verifications []VerificationContainer
		want          bool
	}{
		{
			name:          "head-dependent verification needs the fetch",
			verifications: []VerificationContainer{headDependent("pruning")},
			want:          true,
		},
		{
			name:          "self-contained verification does not",
			verifications: []VerificationContainer{selfContained("chain-id")},
			want:          false,
		},
		{
			name:          "no verifications at all",
			verifications: nil,
			want:          false,
		},
		{
			// The bug: skipping the only head-dependent verification still fetched.
			name:          "skipping the only head-dependent verification drops the fetch",
			skip:          []string{"pruning"},
			verifications: []VerificationContainer{headDependent("pruning")},
			want:          false,
		},
		{
			name:          "wildcard drops the fetch",
			skip:          []string{common.SkipVerificationsWildcard},
			verifications: []VerificationContainer{headDependent("pruning"), headDependent("trace")},
			want:          false,
		},
		{
			// Partial skips must NOT over-reach: a surviving head-dependent
			// verification still needs the chain head resolved.
			name:          "an unskipped head-dependent verification still forces the fetch",
			skip:          []string{"pruning"},
			verifications: []VerificationContainer{headDependent("pruning"), headDependent("trace")},
			want:          true,
		},
		{
			name:          "skipping a self-contained verification changes nothing",
			skip:          []string{"chain-id"},
			verifications: []VerificationContainer{selfContained("chain-id"), headDependent("pruning")},
			want:          true,
		},
		{
			name:          "skipping every head-dependent one but leaving a self-contained one",
			skip:          []string{"pruning"},
			verifications: []VerificationContainer{selfContained("chain-id"), headDependent("pruning")},
			want:          false,
		},
	}

	for _, tc := range playbook {
		t.Run(tc.name, func(t *testing.T) {
			nurl := common.NodeUrl{Url: "https://example.invalid", SkipVerifications: tc.skip}
			require.Equal(t, tc.want, needsLatestBlock(nurl, tc.verifications))
		})
	}
}
