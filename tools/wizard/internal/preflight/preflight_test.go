package preflight

import (
	"os/exec"
	"slices"
	"testing"
)

// withStubs swaps the package-level probes for the duration of a test.
func withStubs(t *testing.T, present map[string]bool, composeV2 bool, fn func()) {
	t.Helper()
	origLook, origCompose := lookPath, dockerComposeV2
	lookPath = func(name string) (string, error) {
		if present[name] {
			return "/usr/bin/" + name, nil
		}
		return "", exec.ErrNotFound
	}
	dockerComposeV2 = func() bool { return composeV2 }
	t.Cleanup(func() { lookPath, dockerComposeV2 = origLook, origCompose })
	fn()
}

// allPresent is the set of executables for a fully-provisioned host.
var allPresent = map[string]bool{
	"bash": true, "envsubst": true, "docker": true, "go": true, "gh": true,
}

func TestToolsCoverEveryOS(t *testing.T) {
	for _, goos := range []string{"darwin", "linux", "windows", "freebsd"} {
		got := tools(goos)
		if len(got) == 0 {
			t.Fatalf("%s: no tools declared", goos)
		}
		for _, tool := range got {
			if tool.Why == "" || tool.Install == "" {
				t.Errorf("%s: tool %q missing Why/Install hint", goos, tool.Name)
			}
		}
	}
}

func TestInstallHintsAreOSSpecific(t *testing.T) {
	// gettext hint (envsubst) must differ between mac (brew) and linux (apt).
	find := func(goos, name string) Tool {
		for _, tool := range tools(goos) {
			if tool.Name == name {
				return tool
			}
		}
		t.Fatalf("%s: tool %q not found", goos, name)
		return Tool{}
	}
	mac := find("darwin", "envsubst").Install
	lin := find("linux", "envsubst").Install
	win := find("windows", "envsubst").Install
	if mac == lin || mac == win || lin == win {
		t.Errorf("envsubst install hints should be OS-specific, got mac=%q linux=%q win=%q", mac, lin, win)
	}
}

func TestRequiredVsOptionalTiers(t *testing.T) {
	byName := map[string]Tier{}
	for _, tool := range tools("linux") {
		byName[tool.Name] = tool.Tier
	}
	wantRequired := []string{"bash", "envsubst", "docker", "docker compose"}
	wantOptional := []string{"go", "gh"}
	for _, n := range wantRequired {
		if byName[n] != Required {
			t.Errorf("%q should be Required", n)
		}
	}
	for _, n := range wantOptional {
		if byName[n] != Optional {
			t.Errorf("%q should be Optional", n)
		}
	}
}

func TestOSReleaseIDs(t *testing.T) {
	body := `NAME="Ubuntu"
ID=ubuntu
ID_LIKE=debian
VERSION_ID="22.04"`
	got := osReleaseIDs(body)
	if len(got) < 2 || got[0] != "ubuntu" || got[1] != "debian" {
		t.Errorf("want [ubuntu debian ...], got %v", got)
	}
	// ID_LIKE can carry multiple space-separated tokens.
	multi := osReleaseIDs(`ID=rocky` + "\n" + `ID_LIKE="rhel centos fedora"`)
	if !slices.Contains(multi, "rocky") || !slices.Contains(multi, "fedora") {
		t.Errorf("ID_LIKE tokens not split: %v", multi)
	}
	// Garbage / empty → no ids, no panic.
	if len(osReleaseIDs("")) != 0 {
		t.Error("empty body should yield no ids")
	}
}

func TestDetectPkgManager(t *testing.T) {
	tests := []struct {
		name     string
		release  string
		wantHint string
	}{
		{"ubuntu", "ID=ubuntu\nID_LIKE=debian", "apt install gettext-base"},
		{"debian", "ID=debian", "apt install gettext-base"},
		{"pop via id_like", "ID=pop\nID_LIKE=\"ubuntu debian\"", "apt install gettext-base"},
		{"fedora", "ID=fedora", "dnf install gettext"},
		{"rocky via id_like", "ID=rocky\nID_LIKE=\"rhel centos fedora\"", "dnf install gettext"},
		{"alpine", "ID=alpine", "apk add gettext"},
		{"unknown falls back to full list", "ID=exoticos", "apt install gettext-base   ·   dnf install gettext   ·   apk add gettext"},
		{"no os-release falls back", "", "apt install gettext-base   ·   dnf install gettext   ·   apk add gettext"},
	}
	orig := readOSRelease
	t.Cleanup(func() { readOSRelease = orig })
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readOSRelease = func() string { return tc.release }
			got := detectPkgManager().hint("gettext-base", "gettext", "gettext")
			if got != tc.wantHint {
				t.Errorf("hint = %q, want %q", got, tc.wantHint)
			}
		})
	}
}

func TestLinuxToolsUseDetectedManager(t *testing.T) {
	orig := readOSRelease
	t.Cleanup(func() { readOSRelease = orig })
	readOSRelease = func() string { return "ID=fedora" }
	for _, tool := range tools("linux") {
		if tool.Name == "envsubst" {
			if tool.Install != "dnf install gettext" {
				t.Errorf("envsubst hint = %q, want dnf-narrowed", tool.Install)
			}
			return
		}
	}
	t.Fatal("envsubst tool not found in linux set")
}

func TestReportOK(t *testing.T) {
	tests := []struct {
		name    string
		missing []Tool
		winNoSh bool
		wantOK  bool
	}{
		{"clean", nil, false, true},
		{"optional missing", []Tool{{Name: "gh", Tier: Optional}}, false, true},
		{"required missing", []Tool{{Name: "docker", Tier: Required}}, false, false},
		{"windows no shell", nil, true, false},
		{"windows no shell overrides clean", nil, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Report{Missing: tc.missing, WindowsNoShell: tc.winNoSh}
			if r.OK() != tc.wantOK {
				t.Errorf("OK()=%v want %v", r.OK(), tc.wantOK)
			}
		})
	}
}

func TestRunAllPresent(t *testing.T) {
	withStubs(t, allPresent, true, func() {
		r := Run()
		if len(r.Missing) != 0 {
			t.Errorf("expected nothing missing, got %+v", r.Missing)
		}
		if !r.OK() {
			t.Error("OK() should be true when everything is present")
		}
	})
}

func TestRunDetectsMissingRequired(t *testing.T) {
	present := map[string]bool{"bash": true, "envsubst": true, "go": true, "gh": true}
	// docker absent; compose probe also fails.
	withStubs(t, present, false, func() {
		r := Run()
		if r.OK() {
			t.Fatal("OK() should be false when docker is missing")
		}
		var names []string
		for _, m := range r.Missing {
			names = append(names, m.Name)
		}
		if !slices.Contains(names, "docker") || !slices.Contains(names, "docker compose") {
			t.Errorf("expected docker + docker compose missing, got %v", names)
		}
	})
}

func TestRunComposeV1OnlyStillMisses(t *testing.T) {
	// docker binary present but the v2 plugin probe fails (only legacy
	// docker-compose installed) → the `docker compose` entry must be reported.
	withStubs(t, allPresent, false, func() {
		r := Run()
		var found bool
		for _, m := range r.Missing {
			if m.Name == "docker compose" {
				found = true
			}
		}
		if !found {
			t.Error("compose v2 miss should be reported even when docker binary exists")
		}
	})
}

func TestHasPOSIXShell(t *testing.T) {
	withStubs(t, map[string]bool{"bash": true}, false, func() {
		if !hasPOSIXShell() {
			t.Error("bash present → hasPOSIXShell true")
		}
	})
	withStubs(t, map[string]bool{}, false, func() {
		if hasPOSIXShell() {
			t.Error("no bash → hasPOSIXShell false")
		}
	})
}
