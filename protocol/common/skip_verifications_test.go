package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ShouldSkipVerification is the single gate every verification-driven probe consults,
// so both the exact-name and wildcard paths need to hold — a false negative silently
// re-enables probing an upstream the operator configured away.
func TestShouldSkipVerification(t *testing.T) {
	playbook := []struct {
		name  string
		skip  []string
		query string
		want  bool
	}{
		{"nil list skips nothing", nil, "pruning", false},
		{"empty list skips nothing", []string{}, "pruning", false},
		{"exact name matches", []string{"pruning"}, "pruning", true},
		{"unrelated name does not match", []string{"pruning"}, "chain-id", false},
		{"one of several matches", []string{"pruning", "trace", "chain-id"}, "trace", true},
		{"wildcard matches any name", []string{SkipAllVerifications}, "chain-id", true},
		{"wildcard matches a name nobody enumerated", []string{SkipAllVerifications}, "tracking-shard-11", true},
		{"wildcard alongside explicit names still matches", []string{"pruning", SkipAllVerifications}, "enabled", true},
		{"wildcard is the literal star only", []string{"*pruning"}, "pruning", false},
		{"match is case sensitive", []string{"Pruning"}, "pruning", false},
		{"empty query is not matched by an explicit list", []string{"pruning"}, "", false},
		{"empty query IS matched by the wildcard", []string{SkipAllVerifications}, "", true},
	}

	for _, tc := range playbook {
		t.Run(tc.name, func(t *testing.T) {
			nurl := NodeUrl{Url: "https://example.invalid", SkipVerifications: tc.skip}
			require.Equal(t, tc.want, nurl.ShouldSkipVerification(tc.query))
		})
	}
}

// The wildcard is a config-surface constant; pinning it guards against a rename
// silently invalidating every deployed values file that carries it.
func TestSkipAllVerificationsToken(t *testing.T) {
	require.Equal(t, "*", SkipAllVerifications)
}
