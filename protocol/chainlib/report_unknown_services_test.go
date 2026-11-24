package chainlib

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A url refused for a failed basic check still names the add-ons the spec does not
// define, so health's row says both at once instead of hiding the typo until the
// node is fixed.
func TestReport_RefusedURLStillNamesUndefinedAddons(t *testing.T) {
	url := common.NodeUrl{Url: "http://node", Addons: []string{"archiv"}}
	outcome := nodeUrlOutcome{
		url:     url,
		served:  url,
		unknown: []string{"archiv"},
		checks: []checkOutcome{{
			verification: VerificationContainer{Name: "chain-id"},
			err:          errors.New("wrong chain id"),
		}},
	}
	admission, _ := admissionFrom([]nodeUrlOutcome{outcome})
	require.True(t, admission.URLRefused(0))

	result := outcome.report(0, admission)
	require.True(t, result.Refused)
	require.Equal(t, []string{"archiv"}, result.UnknownServices)
}
