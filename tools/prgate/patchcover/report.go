package patchcover

import (
	"fmt"
	"io"
	"strings"
)

// WriteMarkdown writes the result as the job summary a reviewer reads.
func (r Result) WriteMarkdown(w io.Writer, minPercent int) {
	fmt.Fprintf(w, "### Unit test coverage of the change\n\n")
	switch {
	case r.Coverable == 0:
		fmt.Fprintf(w, "The change adds or edits no statement the unit tests could run: **pass**.\n")
	case r.Pass(minPercent):
		fmt.Fprintf(w, "Unit tests run **%d of %d** changed statements (%.1f%%, threshold %d%%): **pass**.\n",
			r.Covered, r.Coverable, r.Percent(), minPercent)
	default:
		fmt.Fprintf(w, "Unit tests run **%d of %d** changed statements (%.1f%%, threshold %d%%): **fail**. "+
			"Add unit tests that run the statements on the lines below.\n", r.Covered, r.Coverable, r.Percent(), minPercent)
	}
	if len(r.Files) > 0 {
		fmt.Fprintf(w, "\n| File | Statements | Covered | Lines with a statement no test runs |\n|---|---:|---:|---|\n")
		for _, f := range r.Files {
			fmt.Fprintf(w, "| `%s` | %d | %d | %s |\n", f.Path, f.Coverable, f.Covered, ranges(f.Uncovered, 12))
		}
	}
	if len(r.NoStatements) > 0 {
		fmt.Fprintf(w, "\nChanged, but built only for another platform: %s.\n", codeList(r.NoStatements))
	}
	if len(r.Skipped) > 0 {
		fmt.Fprintf(w, "\nNot measured (tests, generated code, testdata): %s.\n", codeList(r.Skipped))
	}
}

// WriteAnnotations writes GitHub workflow commands that mark the uncovered
// ranges on the diff, at most limit of them: GitHub shows only the first few
// of a step's annotations.
func (r Result) WriteAnnotations(w io.Writer, limit int) {
	n := 0
	for _, f := range r.Files {
		for _, rg := range f.Uncovered {
			if n == limit {
				return
			}
			n++
			msg := fmt.Sprintf("Line %d holds a statement no unit test runs.", rg.Start)
			if rg.End > rg.Start {
				msg = fmt.Sprintf("Lines %d-%d hold statements no unit test runs.", rg.Start, rg.End)
			}
			fmt.Fprintf(w, "::warning file=%s,line=%d,endLine=%d,title=Not covered by unit tests::%s\n",
				escapeProperty(f.Path), rg.Start, rg.End, escapeData(msg))
		}
	}
}

// ranges renders up to max ranges as "3, 7-9", noting how many more exist.
func ranges(list []Range, max int) string {
	if len(list) == 0 {
		return "-"
	}
	parts := make([]string, 0, max+1)
	for i, rg := range list {
		if i == max {
			parts = append(parts, fmt.Sprintf("and %d more", len(list)-max))
			break
		}
		if rg.End > rg.Start {
			parts = append(parts, fmt.Sprintf("%d-%d", rg.Start, rg.End))
		} else {
			parts = append(parts, fmt.Sprintf("%d", rg.Start))
		}
	}
	return strings.Join(parts, ", ")
}

func codeList(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = "`" + p + "`"
	}
	return strings.Join(quoted, ", ")
}

// escapeData and escapeProperty follow the escaping GitHub's toolkit applies to
// workflow command messages and properties.
func escapeData(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(s)
}

func escapeProperty(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C").Replace(s)
}
