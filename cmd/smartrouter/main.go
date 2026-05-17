package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"

	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/performance/connection"
	"github.com/magma-Devs/smart-router/protocol/rpcsmartrouter"
	"github.com/magma-Devs/smart-router/version"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := rpcsmartrouter.CreateRPCSmartRouterCobraCommand()
	rootCmd.Use = "smartrouter [config-file] | { {listen-ip:listen-port spec-chain-id api-interface} ... }"
	rootCmd.Short = "Lava Smart Router — centralized RPC routing engine"

	cmdVersion := &cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.Version)
		},
	}

	rootCmd.AddCommand(cmdVersion)
	rootCmd.AddCommand(cache.CreateCacheCobraCommand())

	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Test commands for the smart router",
	}
	rootCmd.AddCommand(testCmd)
	testCmd.AddCommand(rpcsmartrouter.CreateTestRPCSmartRouterCobraCommand())
	testCmd.AddCommand(connection.CreateTestConnectionServerCobraCommand())
	testCmd.AddCommand(connection.CreateTestConnectionProbeCobraCommand())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
