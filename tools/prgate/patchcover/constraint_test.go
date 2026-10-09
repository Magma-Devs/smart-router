package patchcover

import (
	"go/parser"
	"go/token"
	"testing"
)

func TestPlatformName(t *testing.T) {
	for file, want := range map[string]bool{
		"x/ipc_windows.go":       true,
		"x/ipc_unix.go":          false, // unix is a build tag, not a file name suffix
		"x/sys_linux_arm64.go":   true,
		"x/sys_arm64.go":         true,
		"x/conn_windows_test.go": true,
		"x/windows.go":           false, // no underscore: an ordinary name
		"x/read_your_writes.go":  false,
		"x/handler_test.go":      false,
	} {
		if got := platformName(file); got != want {
			t.Errorf("platformName(%q) = %v, want %v", file, got, want)
		}
	}
}

func TestPlatformOnly(t *testing.T) {
	for header, want := range map[string]bool{
		"":                                   false,
		"//go:build windows":                 true,
		"//go:build darwin || freebsd || js": true,
		"//go:build linux && !arm64":         true,
		"//go:build unix && go1.21":          true,
		"//go:build ignore":                  true,
		"//go:build netgo":                   false,
		"//go:build windows || cgo":          false, // Eval would stop at windows and miss cgo
		"//go:build !cgo":                    false,
		"//go:build integration":             false,
		"// +build windows":                  true, // the old syntax, honoured without a //go:build line
		"// +build windows\n// +build cgo":   false,
	} {
		src := header + "\n\npackage x\n"
		f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		if got := platformOnly("x/x.go", f); got != want {
			t.Errorf("platformOnly(%q) = %v, want %v", header, got, want)
		}
	}

	// A constraint after the package clause is no constraint.
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", "package x\n\n//go:build windows\n", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	if platformOnly("x/x.go", f) {
		t.Error("a //go:build line after the package clause must not count")
	}
}
