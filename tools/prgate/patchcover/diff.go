// Package patchcover measures how much of a change the unit tests run. It reads
// the lines a unified diff adds and the profiles `go test -coverprofile` writes,
// and counts the added lines that hold a statement and whether a test ran them.
package patchcover

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// AddedLine is one line a diff adds: its number in the new file, and its text.
type AddedLine struct {
	Number int
	Text   string
}

// FileDiff is what a diff does to one file.
type FileDiff struct {
	New   bool // the diff creates the file
	Added []AddedLine
}

// ParseDiff returns what a unified diff does to each file, keyed by the file's
// new path. The diff must carry the b/ prefix on new paths. A deleted file and
// a pure rename add no line, so neither appears.
func ParseDiff(r io.Reader) (map[string]*FileDiff, error) {
	files := map[string]*FileDiff{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var file *FileDiff
	created := false
	next := 0 // number the next added or context line gets in the new file
	inHunk := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "diff --git "):
			file, created, inHunk = nil, false, false
		case strings.HasPrefix(line, "@@ "):
			start, err := hunkStart(line)
			if err != nil {
				return nil, err
			}
			next, inHunk = start, true
		case !inHunk:
			// File header: whether the file is new, and its new path.
			if strings.HasPrefix(line, "new file mode ") {
				created = true
			}
			if path, ok := strings.CutPrefix(line, "+++ "); ok && newPath(path) != "" {
				file = &FileDiff{New: created}
				files[newPath(path)] = file
			}
		case strings.HasPrefix(line, "+"):
			if file != nil {
				file.Added = append(file.Added, AddedLine{Number: next, Text: line[1:]})
			}
			next++
		case strings.HasPrefix(line, " "):
			next++
		}
		// A removed line, or "\ No newline at end of file", moves nothing.
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read diff: %w", err)
	}
	return files, nil
}

// hunkStart returns the new-file line a hunk starts at: c in "@@ -a,b +c,d @@".
func hunkStart(header string) (int, error) {
	fields := strings.Fields(header)
	if len(fields) < 4 || !strings.HasPrefix(fields[2], "+") {
		return 0, fmt.Errorf("malformed hunk header %q", header)
	}
	start, _, _ := strings.Cut(fields[2][1:], ",")
	n, err := strconv.Atoi(start)
	if err != nil {
		return 0, fmt.Errorf("malformed hunk header %q", header)
	}
	return n, nil
}

// newPath turns the path of a "+++ " header into a repository path. It returns
// "" for /dev/null, the new side of a deleted file.
func newPath(raw string) string {
	raw = strings.TrimRight(raw, "\t")
	if strings.HasPrefix(raw, `"`) {
		if unquoted, err := strconv.Unquote(raw); err == nil {
			raw = unquoted
		}
	}
	if raw == "/dev/null" {
		return ""
	}
	return strings.TrimPrefix(raw, "b/")
}
