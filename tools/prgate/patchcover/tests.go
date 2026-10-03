package patchcover

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ChangedTests returns the names of the top-level Test and Fuzz functions of
// _test.go files that the change adds lines to, sorted. A test the change adds
// or edits must pass on its first run: scripts/patch-coverage.sh does not give
// it the retry a flaky test on main gets.
func ChangedTests(diff map[string]*FileDiff, root string) ([]string, error) {
	seen := map[string]bool{}
	for file, fd := range diff {
		if !strings.HasSuffix(file, "_test.go") || len(fd.Added) == 0 {
			continue
		}
		fset := token.NewFileSet()
		src, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(fset, file, src, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !(strings.HasPrefix(fn.Name.Name, "Test") || strings.HasPrefix(fn.Name.Name, "Fuzz")) {
				continue
			}
			first, last := fset.Position(fn.Pos()).Line, fset.Position(fn.End()).Line
			for _, l := range fd.Added {
				if l.Number >= first && l.Number <= last {
					seen[fn.Name.Name] = true
					break
				}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
