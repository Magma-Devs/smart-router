package patchcover

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// position is where a basic block sits in its file.
type position struct {
	startLine, startCol, endLine, endCol int
}

// block is a basic block and whether any test ran it.
type block struct {
	position
	covered bool
}

// Coverage holds the blocks the coverage profiles recorded, by repository path.
type Coverage struct {
	files  map[string]map[position]bool
	sorted map[string][]block
}

// NewCoverage returns an empty Coverage.
func NewCoverage() *Coverage {
	return &Coverage{files: map[string]map[position]bool{}}
}

// profileLine is "import/path/file.go:12.5,14.2 3 1": the block's position,
// its statement count, and how many times it ran (or 0/1 in set mode).
var profileLine = regexp.MustCompile(`^(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)

// AddProfile reads one profile written by `go test -coverprofile` inside the
// module whose path is modulePath and whose directory, relative to the
// repository root, is moduleDir. Run with -coverpkg, every test binary reports
// every block of the module, so a block appears many times: it counts as
// covered when any copy of it ran.
//
// A block outside the module is an error rather than something to skip: if
// the path mapping were wrong, every changed line would look like it holds no
// statement and the check would pass on nothing.
func (c *Coverage) AddProfile(r io.Reader, modulePath, moduleDir string) error {
	c.sorted = nil
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "mode: ") {
		return fmt.Errorf("not a coverage profile: missing the mode line")
	}
	blocks := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		m := profileLine.FindStringSubmatch(line)
		if m == nil {
			return fmt.Errorf("malformed coverage line %q", line)
		}
		rel, ok := strings.CutPrefix(m[1], modulePath+"/")
		if !ok {
			return fmt.Errorf("coverage line for %s is outside module %s", m[1], modulePath)
		}
		var pos position
		for i, field := range []*int{&pos.startLine, &pos.startCol, &pos.endLine, &pos.endCol} {
			*field, _ = strconv.Atoi(m[i+2]) // the pattern admits digits only
		}
		count, _ := strconv.Atoi(m[7])

		file := path.Join(moduleDir, rel)
		if c.files[file] == nil {
			c.files[file] = map[position]bool{}
		}
		c.files[file][pos] = c.files[file][pos] || count > 0
		blocks++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read coverage profile: %w", err)
	}
	if blocks == 0 {
		return fmt.Errorf("coverage profile of module %s holds no block", modulePath)
	}
	return nil
}

// At reports whether a position of a file lies in a block, which means a
// statement starts there that the tests compiled, and whether a block holding
// it ran.
func (c *Coverage) At(file string, line, col int) (coverable, covered bool) {
	for _, b := range c.blocks(file) {
		if b.startLine > line {
			break
		}
		if before(line, col, b.startLine, b.startCol) || before(b.endLine, b.endCol, line, col) {
			continue
		}
		coverable = true
		covered = covered || b.covered
	}
	return coverable, covered
}

// before reports whether line:col a comes before line:col b.
func before(aLine, aCol, bLine, bCol int) bool {
	return aLine < bLine || (aLine == bLine && aCol < bCol)
}

// blocks returns a file's blocks ordered by start line.
func (c *Coverage) blocks(file string) []block {
	if c.sorted == nil {
		c.sorted = map[string][]block{}
	}
	if list, ok := c.sorted[file]; ok {
		return list
	}
	list := make([]block, 0, len(c.files[file]))
	for pos, covered := range c.files[file] {
		list = append(list, block{position: pos, covered: covered})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].startLine != list[j].startLine {
			return list[i].startLine < list[j].startLine
		}
		return list[i].startCol < list[j].startCol
	})
	c.sorted[file] = list
	return list
}

// ModulePath returns the module path declared in dir/go.mod.
func ModulePath(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	return "", fmt.Errorf("%s/go.mod declares no module", dir)
}
