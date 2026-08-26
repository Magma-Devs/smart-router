package utils_test

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

var errTest = errors.New("error for tests")

func TestErrorTypeChecks(t *testing.T) {
	err := errTest
	newErr := utils.LavaFormatError("testing 123", err, utils.Attribute{"attribute", "test"})
	require.True(t, errors.Is(newErr, errTest))
}
