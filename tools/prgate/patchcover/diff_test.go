package patchcover

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseDiff(t *testing.T) {
	const diff = `diff --git a/protocol/x.go b/protocol/x.go
index 1111111..2222222 100644
--- a/protocol/x.go
+++ b/protocol/x.go
@@ -10,0 +11,2 @@ func A() {
+	a := 1
+	b := 2
@@ -20 +22 @@ func B() {
-	old()
+	new()
diff --git a/protocol/new.go b/protocol/new.go
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/protocol/new.go
@@ -0,0 +1,2 @@
+package protocol
+++counter
\ No newline at end of file
diff --git a/protocol/gone.go b/protocol/gone.go
deleted file mode 100644
index 4444444..0000000
--- a/protocol/gone.go
+++ /dev/null
@@ -1,2 +0,0 @@
-package protocol
-var gone = 1
diff --git a/protocol/moved.go b/protocol/renamed.go
similarity index 100%
rename from protocol/moved.go
rename to protocol/renamed.go
diff --git a/protocol/ctx.go b/protocol/ctx.go
--- a/protocol/ctx.go
+++ b/protocol/ctx.go
@@ -4,3 +4,4 @@
 keep()
-drop()
+add()
+more()
 tail()
`
	got, err := ParseDiff(strings.NewReader(diff))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]*FileDiff{
		"protocol/x.go": {Added: []AddedLine{
			{Number: 11, Text: "\ta := 1"},
			{Number: 12, Text: "\tb := 2"},
			{Number: 22, Text: "\tnew()"},
		}},
		"protocol/new.go": {New: true, Added: []AddedLine{
			{Number: 1, Text: "package protocol"},
			// An added line whose text starts with "++" is still an added line.
			{Number: 2, Text: "++counter"},
		}},
		"protocol/ctx.go": {Added: []AddedLine{
			{Number: 5, Text: "add()"},
			{Number: 6, Text: "more()"},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		for k, v := range got {
			t.Logf("%s: %+v", k, *v)
		}
		t.Fatalf("ParseDiff differs")
	}
}

func TestParseDiffQuotedPath(t *testing.T) {
	const diff = "diff --git \"a/dir/\\303\\251.go\" \"b/dir/\\303\\251.go\"\n" +
		"--- \"a/dir/\\303\\251.go\"\n" +
		"+++ \"b/dir/\\303\\251.go\"\n" +
		"@@ -1 +1 @@\n" +
		"-x\n" +
		"+y\n"
	got, err := ParseDiff(strings.NewReader(diff))
	if err != nil {
		t.Fatal(err)
	}
	if fd := got["dir/é.go"]; fd == nil || len(fd.Added) != 1 || fd.Added[0].Number != 1 {
		t.Fatalf("quoted path not decoded: %#v", got)
	}
}

func TestParseDiffMalformedHunk(t *testing.T) {
	for _, header := range []string{"@@ -1 @@", "@@ -1 +x,2 @@", "@@ -1 1 @@ x"} {
		diff := "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n" + header + "\n+x\n"
		if _, err := ParseDiff(strings.NewReader(diff)); err == nil {
			t.Errorf("header %q: want an error", header)
		}
	}
}

func TestNewPath(t *testing.T) {
	for raw, want := range map[string]string{
		"b/a/b.go":   "a/b.go",
		"b/a b.go\t": "a b.go",
		"/dev/null":  "",
		`"b/q.go"`:   "q.go",
	} {
		if got := newPath(raw); got != want {
			t.Errorf("newPath(%q) = %q, want %q", raw, got, want)
		}
	}
}
