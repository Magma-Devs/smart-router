package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	speckeeper "github.com/magma-Devs/smart-router/utils/keeper"
)

// The write-method lock (utils/keeper, MAG-5593) sets the Stateful level without importing this
// package; the two constants are one value.
func TestStatefulAllProvidersIsCommons(t *testing.T) {
	require.Equal(t, uint32(CONSISTENCY_SELECT_ALL_PROVIDERS), speckeeper.StatefulAllProviders)
}
