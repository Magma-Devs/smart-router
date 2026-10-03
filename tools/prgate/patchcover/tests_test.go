package patchcover

import (
	"reflect"
	"testing"
)

func TestChangedTests(t *testing.T) {
	root := t.TempDir()
	write(t, root, "pkg/a_test.go", `package pkg

import "testing"

func TestOne(t *testing.T) {
	t.Log("one")
}

func TestTwo(t *testing.T) {
	t.Log("two")
}

func helper() {}

type suite struct{}

func (suite) TestMethod(t *testing.T) {}

func FuzzThree(f *testing.F) {
	f.Add(1)
}
`)
	diff := map[string]*FileDiff{
		"pkg/a_test.go": added(6, 13, 17, 21),
		"pkg/a.go":      added(1),
		"pkg/b_test.go": {}, // renamed without changes
	}
	got, err := ChangedTests(diff, root)
	if err != nil {
		t.Fatal(err)
	}
	// Line 6 is in TestOne, 13 in helper, 17 in a method, 21 in FuzzThree.
	if want := []string{"FuzzThree", "TestOne"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ChangedTests = %v, want %v", got, want)
	}

	if _, err := ChangedTests(map[string]*FileDiff{"pkg/gone_test.go": added(1)}, root); err == nil {
		t.Error("a missing test file must be an error")
	}
	write(t, root, "pkg/bad_test.go", "package pkg\n\nfunc (\n")
	if _, err := ChangedTests(map[string]*FileDiff{"pkg/bad_test.go": added(1)}, root); err == nil {
		t.Error("an unparsable test file must be an error")
	}
}
