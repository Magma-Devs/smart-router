package chainlib

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// A url runs its checks in the order of what failing one costs, so a hanging add-on
// check cannot keep a check that refuses the url from running (MAG-3950).
func TestInCostOrder_TheChecksThatRefuseTheUrlRunFirst(t *testing.T) {
	warning := baseCollectionOnly("node-info")
	warning.Severity = spectypes.ParseValue_Warning
	mandatory := addonScoped("mandatory-debug", "debug")
	mandatory.Severity = spectypes.ParseValue_Fail
	archive := archiveOnBaseCollection()
	archive.Name = "archive-pruning"
	in := []VerificationContainer{
		addonScoped("enabled", "debug"), warning, archive, baseCollectionOnly("chain-id"), mandatory, baseCollectionOnly("pruning"),
	}
	names := func(verifications []VerificationContainer) []string {
		out := make([]string, 0, len(verifications))
		for _, v := range verifications {
			out = append(out, v.Name)
		}
		return out
	}
	require.Equal(t, []string{"chain-id", "mandatory-debug", "pruning", "enabled", "archive-pruning", "node-info"}, names(inCostOrder(in)),
		"the url's checks first, then each service's, then the Warnings, each in the spec's order")
	require.Equal(t, "enabled", in[0].Name, "the parser's list is left as it was")
}

// A url refused only by checks the deadline cut off is still refused, since nothing
// proves it serves, but nothing that ran refused it: no strike. One a check that ran
// failed is refused under that check, whatever the deadline cut off besides.
func TestURLRefusalRan_ACheckTheDeadlineCutOffIsNoStrike(t *testing.T) {
	url := common.NodeUrl{Url: "https://a"}
	pass := func(errs ...error) ProviderAdmission {
		checks := make([]checkOutcome, 0, len(errs))
		for i, err := range errs {
			checks = append(checks, checkOutcome{verification: baseCollectionOnly(fmt.Sprint("check-", i)), err: err})
		}
		admission, _ := admissionFrom([]nodeUrlOutcome{{url: url, served: url, checks: checks}})
		return admission
	}

	cutOff := pass(errCheckNotRun, errCheckNotRun)
	require.True(t, cutOff.URLRefused(0), "nothing proves the url serves")
	require.False(t, cutOff.URLRefusalRan(0), "but nothing that ran refused it")
	require.False(t, cutOff.RefusalRan())
	require.Equal(t, CheckNotRun, FailedCheck(cutOff.URLRefusal(0)))

	failed := pass(errCheckNotRun, errors.New("wrong chain"))
	require.True(t, failed.URLRefusalRan(0), "a check that ran and failed is a strike")
	require.Equal(t, "check-1", FailedCheck(failed.URLRefusal(0)), "and the url is refused under it")
}

// A provider left with no url is refused under a check that failed, not under a url the
// deadline cut off before it got its turn.
func TestAdmissionFrom_AProviderIsRefusedUnderWhatFailed(t *testing.T) {
	a, b := common.NodeUrl{Url: "https://a"}, common.NodeUrl{Url: "https://b"}
	_, err := admissionFrom([]nodeUrlOutcome{
		{url: a, served: a, checks: []checkOutcome{{verification: baseCollectionOnly("chain-id"), err: errCheckNotRun}}},
		{url: b, served: b, checks: []checkOutcome{{verification: baseCollectionOnly("chain-id"), err: errors.New("wrong chain")}}},
	})
	require.Error(t, err)
	require.Equal(t, "chain-id", FailedCheck(err))
}

// A service check the deadline cut off decided nothing, as a rate-limited one: the
// service keeps the verdict the last pass gave it, refused or admitted, while a service
// whose check failed is refused.
func TestAdmissionFrom_AServiceCheckTheDeadlineCutOffKeepsItsLastVerdict(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"debug", "trace"}}
	pass := func(traceErr error) ProviderAdmission {
		admission, err := admissionFrom([]nodeUrlOutcome{{url: url, served: url, checks: []checkOutcome{
			{verification: baseCollectionOnly("chain-id")},
			{verification: addonScoped("enabled", "debug"), err: errors.New("verification deadline exceeded")},
			{verification: addonScoped("trace", "trace"), err: traceErr},
		}}})
		require.NoError(t, err)
		return admission
	}
	debugOff := []RefusedService{{Service: "debug", Check: "enabled"}}

	cutOff := pass(errCheckNotRun)
	require.Equal(t, debugOff, cutOff.Refused(), "debug failed; trace never ran")
	require.NotNil(t, cutOff.Throttled(), "what this pass did not decide is kept from the last one")
	require.Equal(t, []RefusedService{{Service: "debug", Check: "enabled"}, {Service: "trace", Check: "trace"}},
		cutOff.KeepingThrottledFrom(pass(errors.New("trace_block is not available"))).Refused(), "trace stays off if the last pass refused it")
	require.Equal(t, debugOff, cutOff.KeepingThrottledFrom(pass(nil)).Refused(), "and on if it served")
}
