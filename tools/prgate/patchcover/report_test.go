package patchcover

import (
	"strings"
	"testing"
)

func TestWriteMarkdown(t *testing.T) {
	res := Result{
		Files: []FileResult{
			{Path: "pkg/a.go", Coverable: 4, Covered: 2, Uncovered: []Range{{3, 4}}},
			{Path: "pkg/b.go", Coverable: 1, Covered: 1},
		},
		Coverable:    5,
		Covered:      3,
		NoStatements: []string{"pkg/w_windows.go"},
		Skipped:      []string{"pkg/a_test.go"},
	}
	var b strings.Builder
	res.WriteMarkdown(&b, 80)
	out := b.String()
	for _, want := range []string{
		"**3 of 5** changed statements", "60.0%", "threshold 80%", "**fail**",
		"| `pkg/a.go` | 4 | 2 | 3-4 |",
		"| `pkg/b.go` | 1 | 1 | - |",
		"`pkg/w_windows.go`",
		"`pkg/a_test.go`",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary lacks %q:\n%s", want, out)
		}
	}

	b.Reset()
	res.WriteMarkdown(&b, 60)
	if !strings.Contains(b.String(), "**pass**") {
		t.Errorf("60%% at a 60%% threshold must pass:\n%s", b.String())
	}

	b.Reset()
	Result{}.WriteMarkdown(&b, 80)
	if !strings.Contains(b.String(), "no statement the unit tests could run") {
		t.Errorf("an empty result must say there was nothing to measure:\n%s", b.String())
	}
}

func TestWriteAnnotations(t *testing.T) {
	res := Result{Files: []FileResult{
		{Path: "a,b:c.go", Uncovered: []Range{{1, 1}, {5, 7}}},
		{Path: "z.go", Uncovered: []Range{{9, 9}}},
	}}
	var b strings.Builder
	res.WriteAnnotations(&b, 2)
	want := "::warning file=a%2Cb%3Ac.go,line=1,endLine=1,title=Not covered by unit tests::Line 1 holds a statement no unit test runs.\n" +
		"::warning file=a%2Cb%3Ac.go,line=5,endLine=7,title=Not covered by unit tests::Lines 5-7 hold statements no unit test runs.\n"
	if b.String() != want {
		t.Fatalf("annotations:\n got %q\nwant %q", b.String(), want)
	}
}

func TestRanges(t *testing.T) {
	list := []Range{{1, 1}, {3, 5}, {8, 8}}
	if got := ranges(list, 5); got != "1, 3-5, 8" {
		t.Errorf("ranges = %q", got)
	}
	if got := ranges(list, 2); got != "1, 3-5, and 1 more" {
		t.Errorf("capped ranges = %q", got)
	}
}

func TestEscapeData(t *testing.T) {
	if got := escapeData("50%\r\nx"); got != "50%25%0D%0Ax" {
		t.Errorf("escapeData = %q", got)
	}
}
