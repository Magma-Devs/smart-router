package relaycore

import (
	"errors"
	"strconv"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRelayError(err error, routerErr *common.RouterError, stake int64, reputation string) RelayError {
	rep, _ := strconv.ParseFloat(reputation, 64)
	return RelayError{
		Err: err,
		ProviderInfo: common.ProviderInfo{
			ProviderStake:             stake,
			ProviderReputationSummary: rep,
			ProviderAddress:           "lava@test",
		},
		RouterError: routerErr,
	}
}

func TestGetBestError_PrefersExternalOverInternal(t *testing.T) {
	// GIVEN 5 errors with unique messages (no majority since 1 < 5/2=2):
	// 3 internal with higher scores, 2 external with lower scores
	// WHEN GetBestErrorMessageForUser is called
	// THEN the external error is preferred over higher-scored internal errors
	re := &RelayErrors{RelayErrors: []RelayError{
		makeRelayError(errors.New("timeout alpha"), common.RouterErrorConnectionTimeout, 1000, "1.0"),
		makeRelayError(errors.New("timeout beta"), common.RouterErrorConnectionRefused, 900, "1.0"),
		makeRelayError(errors.New("timeout gamma"), common.RouterErrorConnectionReset, 800, "1.0"),
		makeRelayError(errors.New("nonce too low"), common.RouterErrorChainNonceTooLow, 100, "0.5"),
		makeRelayError(errors.New("execution reverted"), common.RouterErrorChainExecutionReverted, 50, "0.3"),
	}}

	best := re.GetBestErrorMessageForUser()
	require.NotNil(t, best.RouterError)
	assert.Equal(t, common.CategoryExternal, best.RouterError.Category)
}

func TestGetBestError_ExternalTiebreakByScore(t *testing.T) {
	// GIVEN 5 external errors with different provider scores (no majority)
	// WHEN GetBestErrorMessageForUser is called
	// THEN the higher-scored provider's error wins
	re := &RelayErrors{RelayErrors: []RelayError{
		makeRelayError(errors.New("nonce too low"), common.RouterErrorChainNonceTooLow, 100, "0.5"),
		makeRelayError(errors.New("execution reverted"), common.RouterErrorChainExecutionReverted, 1000, "1.0"),
		makeRelayError(errors.New("out of gas"), common.RouterErrorChainOutOfGas, 500, "0.8"),
		makeRelayError(errors.New("insufficient funds"), common.RouterErrorChainInsufficientFunds, 200, "0.6"),
		makeRelayError(errors.New("block not found"), common.RouterErrorChainBlockNotFound, 300, "0.7"),
	}}

	best := re.GetBestErrorMessageForUser()
	assert.Equal(t, "execution reverted", best.Err.Error())
}

func TestGetBestError_MajorityWins(t *testing.T) {
	// GIVEN 3 errors where 2 have the same message (majority)
	// WHEN GetBestErrorMessageForUser is called
	// THEN the majority error wins regardless of category/score
	re := &RelayErrors{RelayErrors: []RelayError{
		makeRelayError(errors.New("nonce too low"), common.RouterErrorChainNonceTooLow, 100, "0.5"),
		makeRelayError(errors.New("nonce too low"), common.RouterErrorChainNonceTooLow, 200, "0.5"),
		makeRelayError(errors.New("timeout"), common.RouterErrorConnectionTimeout, 1000, "1.0"),
	}}

	best := re.GetBestErrorMessageForUser()
	assert.Equal(t, "nonce too low", best.Err.Error())
}

func TestGetBestError_NilRouterErrorFallsBack(t *testing.T) {
	// GIVEN 5 errors where RouterError is nil (unclassified, no majority)
	// WHEN GetBestErrorMessageForUser is called
	// THEN it falls back to score-based selection (backwards compatible)
	re := &RelayErrors{RelayErrors: []RelayError{
		makeRelayError(errors.New("error A"), nil, 100, "0.5"),
		makeRelayError(errors.New("error B"), nil, 1000, "1.0"),
		makeRelayError(errors.New("error C"), nil, 50, "0.3"),
		makeRelayError(errors.New("error D"), nil, 30, "0.2"),
		makeRelayError(errors.New("error E"), nil, 20, "0.1"),
	}}

	best := re.GetBestErrorMessageForUser()
	assert.Equal(t, "error B", best.Err.Error())
}

func TestGetBestError_RouterErrorPropagated(t *testing.T) {
	// GIVEN a relay error with RouterError populated
	// WHEN GetBestErrorMessageForUser returns it
	// THEN the RouterError field is preserved
	re := &RelayErrors{RelayErrors: []RelayError{
		makeRelayError(errors.New("nonce too low"), common.RouterErrorChainNonceTooLow, 100, "0.5"),
	}}

	best := re.GetBestErrorMessageForUser()
	require.NotNil(t, best.RouterError)
	assert.Equal(t, common.RouterErrorChainNonceTooLow, best.RouterError)
	assert.Equal(t, "CHAIN_NONCE_TOO_LOW", best.RouterError.Name)
	assert.False(t, best.RouterError.Retryable)
}
