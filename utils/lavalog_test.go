package utils_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

var TestError = errors.New("error for tests")

func TestErrorTypeChecks(t *testing.T) {
	err := TestError
	newErr := utils.LavaFormatError("testing 123", err, utils.Attribute{"attribute", "test"})
	require.True(t, errors.Is(newErr, TestError))
}

// ToHexString keys and logs subscription hashes; its output is the %x rendering it always had.
func TestToHexStringMatchesPercentX(t *testing.T) {
	for _, in := range []string{"", "abc", "\x00\xff\x10binary\x7f"} {
		require.Equal(t, fmt.Sprintf("%x", in), utils.ToHexString(in))
	}
}
