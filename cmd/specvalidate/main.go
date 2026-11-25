// specvalidate checks the `errors` sections of chain specs against the contract the router
// enforces (PRD §4, MAG-5604), and exports the write-method table lava-specs' stateful check reads
// (PRD §4.5). lava-specs CI and the router's own CI run the same rules: the router's.
//
//	specvalidate [-json] PATH...    every spec under PATH (a proposal file or a directory)
//	specvalidate write-methods      the write-method table, as JSON
//
// Exit status: 0 clean, 1 findings, 2 a path that could not be read.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/magma-Devs/smart-router/protocol/specvalidate"
	"github.com/magma-Devs/smart-router/utils/keeper"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "write-methods" {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(keeper.WriteMethods()); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		return 0
	}
	flags := flag.NewFlagSet("specvalidate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	asJSON := flags.Bool("json", false, "print the findings as a JSON array")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() == 0 {
		fmt.Fprintln(stderr, "usage: specvalidate [-json] PATH... | specvalidate write-methods")
		return 2
	}
	loaded := map[string]specvalidate.Loaded{}
	for _, path := range flags.Args() {
		specs, err := specvalidate.LoadPath(path)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		for index, entry := range specs {
			loaded[index] = entry
		}
	}
	findings := specvalidate.Validate(loaded)
	if *asJSON {
		if findings == nil {
			findings = []specvalidate.Finding{}
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(findings)
	} else {
		for _, finding := range findings {
			fmt.Fprintln(stdout, finding.String())
		}
	}
	if len(findings) > 0 {
		return 1
	}
	return 0
}
