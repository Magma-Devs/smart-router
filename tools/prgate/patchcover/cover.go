package patchcover

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Range is a run of changed lines with a statement no test ran, inclusive.
type Range struct {
	Start, End int
}

// FileResult is the coverage of one changed file.
type FileResult struct {
	Path      string
	Coverable int // changed statements
	Covered   int // of those, the statements a test ran
	Uncovered []Range
}

// Result is the coverage of a whole change, counted in statements.
type Result struct {
	Files     []FileResult // files with at least one changed statement, by path
	Coverable int
	Covered   int
	// NoStatements lists changed files the tests do not build because they
	// build only on another GOOS or GOARCH.
	NoStatements []string
	// Skipped lists changed Go files the check does not measure: tests,
	// generated code, and files outside any package (testdata, _ and . dirs).
	Skipped []string
}

// Pass reports whether tests run at least minPercent of the changed
// statements. A change without one passes.
func (r Result) Pass(minPercent int) bool {
	return r.Coverable == 0 || r.Covered*100 >= minPercent*r.Coverable
}

// Percent is the share of changed statements a test ran, 100 when there are none.
func (r Result) Percent() float64 {
	if r.Coverable == 0 {
		return 100
	}
	return float64(r.Covered) * 100 / float64(r.Coverable)
}

// Measure computes the coverage of the statements a change adds or edits.
// root is the checkout the diff was taken against, and modules lists the
// directories, relative to root, whose profiles cov holds.
//
// A statement counts when a line of it changed: for a compound statement (if,
// for, switch, select), a line of its header. A test ran it when the coverage
// block holding its first position ran, so a line with two statements of
// which one ran counts one covered and one not. Blank lines, comments and
// declarations hold no statement, so they neither help nor hurt.
//
// A changed Go file that belongs to a module without a profile is an error:
// its statements would otherwise go unseen.
func Measure(diff map[string]*FileDiff, cov *Coverage, root string, modules []string) (Result, error) {
	measured := map[string]bool{}
	for _, dir := range modules {
		measured[path.Clean(dir)] = true
	}
	paths := make([]string, 0, len(diff))
	for file := range diff {
		paths = append(paths, file)
	}
	sort.Strings(paths)

	var res Result
	for _, file := range paths {
		if !strings.HasSuffix(file, ".go") {
			continue
		}
		if strings.HasSuffix(file, "_test.go") || outsidePackages(file) {
			res.Skipped = append(res.Skipped, file)
			continue
		}
		fset := token.NewFileSet()
		src, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			return Result{}, err
		}
		f, err := parser.ParseFile(fset, file, src, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return Result{}, err
		}
		if generated(f, diff[file], file) {
			res.Skipped = append(res.Skipped, file)
			continue
		}
		mod, err := moduleOf(root, file)
		if err != nil {
			return Result{}, err
		}
		if !measured[mod] {
			return Result{}, fmt.Errorf("%s belongs to the Go module in %s, which has no coverage profile", file, mod)
		}

		stmts := changedStatements(fset, f, diff[file].Added)
		if len(stmts) == 0 {
			continue
		}
		compiled := len(cov.blocks(file)) > 0
		if !compiled && platformOnly(file, f) {
			res.NoStatements = append(res.NoStatements, file)
			continue
		}

		fr := FileResult{Path: file}
		var open *Range
		for _, s := range stmts {
			// A file the tests do not compile, though it builds somewhere (a tag
			// such as netgo that the tests do not set): every statement counts,
			// and none ran.
			coverable, covered := true, false
			if compiled {
				coverable, covered = cov.At(file, s.at.Line, s.at.Column)
			}
			if !coverable {
				continue
			}
			fr.Coverable++
			if covered {
				fr.Covered++
				open = nil
				continue
			}
			if open != nil && s.line <= open.End+1 {
				open.End = max(open.End, s.line)
				continue
			}
			fr.Uncovered = append(fr.Uncovered, Range{Start: s.line, End: s.line})
			open = &fr.Uncovered[len(fr.Uncovered)-1]
		}
		if fr.Coverable > 0 {
			res.Files = append(res.Files, fr)
			res.Coverable += fr.Coverable
			res.Covered += fr.Covered
		}
	}
	return res, nil
}

// statement is a changed statement: the line it starts on, and the position
// whose coverage block decides whether a test ran it.
type statement struct {
	line int
	at   token.Position
}

// changedStatements returns the statements of f that a line of added
// changes, in source order.
func changedStatements(fset *token.FileSet, f *ast.File, added []AddedLine) []statement {
	lines := map[int]bool{}
	for _, l := range added {
		lines[l.Number] = true
	}
	// The parts of a header (a for loop's init and post, a type switch's
	// assignment, a select case's receive) are statements of their own in the
	// syntax tree, but they belong to the statement whose header holds them.
	header := map[ast.Node]bool{}
	var out []statement
	ast.Inspect(f, func(n ast.Node) bool {
		s, ok := n.(ast.Stmt)
		if !ok || header[n] {
			return true
		}
		from, to, at := s.Pos(), s.End(), s.Pos()
		switch s := s.(type) {
		case *ast.BlockStmt, *ast.EmptyStmt, *ast.LabeledStmt:
			return true // the statements inside count, not these
		case *ast.IfStmt:
			to, header[s.Init] = s.Body.Lbrace, true
		case *ast.ForStmt:
			to, header[s.Init], header[s.Post] = s.Body.Lbrace, true, true
		case *ast.RangeStmt:
			to = s.Body.Lbrace
		case *ast.SwitchStmt:
			to, header[s.Init] = s.Body.Lbrace, true
		case *ast.TypeSwitchStmt:
			to, header[s.Init], header[s.Assign] = s.Body.Lbrace, true, true
		case *ast.SelectStmt:
			to = s.Body.Lbrace
		case *ast.CaseClause:
			// The case runs when its body's block runs.
			to, at = s.Colon, s.Colon+1
		case *ast.CommClause:
			to, at, header[s.Comm] = s.Colon, s.Colon+1, true
		default:
			// A function literal inside counts statement by statement: only
			// the lines before its body belong to this statement.
			ast.Inspect(s, func(m ast.Node) bool {
				if lit, ok := m.(*ast.FuncLit); ok {
					to = min(to, lit.Body.Lbrace)
					return false
				}
				return true
			})
		}
		for l := fset.Position(from).Line; l <= fset.Position(to).Line; l++ {
			if lines[l] {
				out = append(out, statement{line: fset.Position(s.Pos()).Line, at: fset.Position(at)})
				break
			}
		}
		return true
	})
	sort.SliceStable(out, func(i, j int) bool { return out[i].line < out[j].line })
	return out
}

// outsidePackages reports whether the go tool ignores a path: anything under
// testdata, or under a directory whose name starts with "." or "_".
func outsidePackages(file string) bool {
	for _, d := range strings.Split(path.Dir(file), "/") {
		if d == "testdata" || (d != "." && (strings.HasPrefix(d, ".") || strings.HasPrefix(d, "_"))) {
			return true
		}
	}
	return false
}

// generatedMarker is the line Go tools put on generated files
// (https://go.dev/s/generatedcode).
var generatedMarker = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.$`)

// generated reports whether a file is generated code. The marker alone is not
// enough: anyone can type it. It counts when the file had it before the change,
// or when the change creates the file under a generator's name.
func generated(f *ast.File, fd *FileDiff, file string) bool {
	if !ast.IsGenerated(f) {
		return false
	}
	if fd.New {
		return generatorName(file)
	}
	for _, l := range fd.Added {
		if generatedMarker.MatchString(l.Text) {
			return false
		}
	}
	return true
}

// generatorName reports whether a file name is one code generators write:
// protoc (.pb.go), mockgen (_mock.go, mock_*.go), stringer (_string.go).
func generatorName(file string) bool {
	base := path.Base(file)
	for _, suffix := range []string{".pb.go", ".pb.gw.go", "_mock.go", "_string.go"} {
		if strings.HasSuffix(base, suffix) {
			return true
		}
	}
	return strings.HasPrefix(base, "mock_") || strings.HasPrefix(base, "zz_generated")
}

// moduleOf returns the directory, relative to root, of the Go module a file
// belongs to: the nearest directory at or above it holding a go.mod.
func moduleOf(root, file string) (string, error) {
	dir := path.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(root, dir, "go.mod")); err == nil {
			return dir, nil
		}
		if dir == "." {
			return "", fmt.Errorf("%s belongs to no Go module under %s", file, root)
		}
		dir = path.Dir(dir)
	}
}
