package patchcover

import (
	"go/ast"
	"go/build/constraint"
	"path"
	"strings"
)

// The GOOS and GOARCH names Go knows, as in go/build's syslist.go.
var (
	knownOS = set("aix android darwin dragonfly freebsd hurd illumos ios js linux nacl netbsd " +
		"openbsd plan9 solaris wasip1 windows zos")
	knownArch = set("386 amd64 amd64p32 arm armbe arm64 arm64be loong64 mips mipsle mips64 mips64le " +
		"mips64p32 mips64p32le ppc ppc64 ppc64le riscv riscv64 s390 s390x sparc sparc64 wasm")
)

func set(words string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(words) {
		m[w] = true
	}
	return m
}

// platformOnly reports whether a file builds only for some operating systems
// or architectures: by its name (x_windows.go, x_linux_arm64.go), or by a
// //go:build line that names only GOOS and GOARCH values, "unix", "ignore" or
// a Go version. The tests compile such a file only on its platform, so on
// another one it holds no block and that is no gap. Any other tag (netgo, cgo,
// an integration tag) leaves code the tests never compile, and that counts.
func platformOnly(file string, f *ast.File) bool {
	if platformName(file) {
		return true
	}
	expr := buildConstraint(f)
	if expr == nil {
		return false
	}
	only := true
	walkTags(expr, func(tag string) {
		if !knownOS[tag] && !knownArch[tag] && tag != "unix" && tag != "ignore" && !strings.HasPrefix(tag, "go1.") {
			only = false
		}
	})
	return only
}

// platformName applies go/build's file name rule: the parts after the first
// underscore end in _GOOS, _GOARCH or _GOOS_GOARCH, before an optional _test.
func platformName(file string) bool {
	name := strings.TrimSuffix(path.Base(file), ".go")
	i := strings.Index(name, "_")
	if i < 0 {
		return false
	}
	parts := strings.Split(name[i:], "_")
	if n := len(parts); n > 0 && parts[n-1] == "test" {
		parts = parts[:n-1]
	}
	n := len(parts)
	if n >= 2 && knownOS[parts[n-2]] && knownArch[parts[n-1]] {
		return true
	}
	return n >= 1 && (knownOS[parts[n-1]] || knownArch[parts[n-1]])
}

// buildConstraint returns the file's build constraint, or nil: its //go:build
// line, or else its // +build lines ANDed, which the go command still honours
// in a file without the former.
func buildConstraint(f *ast.File) constraint.Expr {
	var plus constraint.Expr
	for _, group := range f.Comments {
		if group.Pos() > f.Package {
			break
		}
		for _, c := range group.List {
			expr, err := constraint.Parse(c.Text)
			switch {
			case err != nil:
			case constraint.IsGoBuild(c.Text):
				return expr
			case plus == nil:
				plus = expr
			default:
				plus = &constraint.AndExpr{X: plus, Y: expr}
			}
		}
	}
	return plus
}

// walkTags calls visit on every tag of an expression. Expr.Eval would stop at
// the first operand that decides an || or an &&, and skip the rest.
func walkTags(expr constraint.Expr, visit func(string)) {
	switch e := expr.(type) {
	case *constraint.TagExpr:
		visit(e.Tag)
	case *constraint.NotExpr:
		walkTags(e.X, visit)
	case *constraint.AndExpr:
		walkTags(e.X, visit)
		walkTags(e.Y, visit)
	case *constraint.OrExpr:
		walkTags(e.X, visit)
		walkTags(e.Y, visit)
	}
}
