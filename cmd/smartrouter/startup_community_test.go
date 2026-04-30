//go:build !enterprise

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Magma-Devs/smart-router/protocol/rpcsmartrouter"
)

func TestInstallLicenseCheck_CommunityInstallsPreRunE(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	require.Nil(t, cmd.PreRunE, "test setup: cmd starts with no PreRunE")

	installLicenseCheck(cmd)

	require.NotNil(t, cmd.PreRunE, "community installLicenseCheck must install a PreRunE for the INFO log line")
	require.NoError(t, cmd.PreRunE(cmd, nil),
		"community PreRunE has no failure path — only logs the edition")
}

func TestInstallLicenseCheck_CommunityChainsExistingPreRunE(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	var existingFired bool
	cmd.PreRunE = func(*cobra.Command, []string) error {
		existingFired = true
		return nil
	}

	installLicenseCheck(cmd)

	require.NoError(t, cmd.PreRunE(cmd, nil))
	assert.True(t, existingFired, "community PreRunE must chain (not overwrite) the previously-installed PreRunE")
}

func TestInstallLicenseCheck_CommunityDoesNotPromoteActiveConfig(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	installLicenseCheck(cmd)

	// Run the PreRunE — community has no license, no factory, nothing should
	// promote activeConfig.
	require.NoError(t, cmd.PreRunE(cmd, nil))

	got := rpcsmartrouter.ActiveConfig()
	assert.Equal(t, "community", got.Edition(),
		"community build's PreRunE must leave activeConfig as community — no promotion path exists without an enterprise factory")
	assert.False(t, rpcsmartrouter.IsEnterpriseBuild(),
		"community build cannot register an enterprise factory; IsEnterpriseBuild must be false")
}