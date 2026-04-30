//go:build !enterprise

package main

import (
	"github.com/spf13/cobra"

	"github.com/Magma-Devs/smart-router/utils"
)

// installLicenseCheck wires an INFO-logging PreRunE onto the router command.
// Community builds have nothing to validate — there is no license, no embedded
// license file, and no licensing import. The hook exists only so that the
// "edition" log line is predictable across editions.
//
// Wraps any existing PreRunE rather than overwriting it, so a future
// contributor adding a PreRunE inside rpcsmartrouter doesn't silently lose it.
func installLicenseCheck(cmd *cobra.Command) {
	existing := cmd.PreRunE
	cmd.PreRunE = func(c *cobra.Command, args []string) error {
		utils.LavaFormatInfo("Smart Router Community Edition — https://github.com/Magma-Devs/smart-router")
		if existing != nil {
			return existing(c, args)
		}
		return nil
	}
}