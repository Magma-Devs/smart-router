// Package preflight verifies the external system tools the wizard shells out to
// are present BEFORE the interactive flow starts — so the user learns about a
// missing `docker`/`envsubst`/`bash` up front, not three screens in when the
// run step fails.
//
// The wizard is bash-based by design: the run step is `exec.Command("bash",
// "-c", …)`, it uses `envsubst` to expand ${VAR} refs, and it writes a
// `#!/usr/bin/env bash` run.sh. That stack exists on macOS and Linux out of the
// box (envsubst ships in GNU gettext — see the per-tool install hint). On native
// Windows there is no bash/envsubst, so preflight detects that and points the
// user at WSL2 / Git Bash rather than pretending the run step will work.
//
// Tiers:
//   - Required — the wizard cannot complete a run without them. A missing
//     required tool is a hard stop.
//   - Optional — the wizard degrades gracefully (e.g. `gh` → net/http tarball
//     fallback, `go` only needed when no prebuilt binary exists). A miss is a
//     warning, not a stop.
package preflight

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

// Tier classifies how badly a missing tool hurts.
type Tier int

const (
	// Required tools block a successful run; missing one is a hard stop.
	Required Tier = iota
	// Optional tools have a graceful fallback; missing one is a warning.
	Optional
)

// Tool is one external binary the wizard depends on at runtime.
type Tool struct {
	// Name is the primary executable looked up on PATH.
	Name string
	// Probe, when set, overrides the default `exec.LookPath(Name)` check —
	// used for `docker compose` (a subcommand, not a bare binary).
	Probe func() bool
	// Tier is Required or Optional.
	Tier Tier
	// Why explains what the wizard uses the tool for (shown on a miss).
	Why string
	// Install is a terse, OS-appropriate install hint (shown on a miss).
	Install string
}

// found reports whether the tool resolves on this machine.
func (t Tool) found() bool {
	if t.Probe != nil {
		return t.Probe()
	}
	_, err := lookPath(t.Name)
	return err == nil
}

// lookPath is indirected so tests can stub tool presence without a real PATH.
var lookPath = exec.LookPath

// dockerComposeV2 reports whether the Docker Compose **v2 plugin** is available
// (`docker compose version`, space — NOT the legacy `docker-compose` binary).
// The wizard's run command is `docker compose …`, so v1 alone won't do.
var dockerComposeV2 = func() bool {
	if _, err := lookPath("docker"); err != nil {
		return false
	}
	return exec.Command("docker", "compose", "version").Run() == nil
}

// Report is the outcome of a preflight sweep.
type Report struct {
	// OS is the detected runtime.GOOS.
	OS string
	// WindowsNoShell is true on native Windows with no bash on PATH — the
	// wizard's bash-based run step cannot work; the user is steered to WSL.
	WindowsNoShell bool
	// Missing holds every tool that failed its probe, in declaration order.
	Missing []Tool
}

// missingRequired reports whether any Required tool is absent.
func (r Report) missingRequired() bool {
	for _, t := range r.Missing {
		if t.Tier == Required {
			return true
		}
	}
	return false
}

// OK reports whether the wizard can proceed: no missing required tools and,
// on Windows, a usable POSIX shell.
func (r Report) OK() bool {
	return !r.missingRequired() && !r.WindowsNoShell
}

// pkgManager is a detected Linux package manager. The zero value (unknown)
// falls back to listing every common manager, so we never guess wrong.
type pkgManager struct {
	// cmd is the install verb, e.g. "apt install" / "dnf install" / "apk add".
	// Empty means the distro wasn't recognised → hint() prints the full list.
	cmd string
}

// hint renders the install line for a package, narrowed to the detected manager
// when known, or the full apt·dnf·apk list when not. The three args are the
// package name under apt, dnf, and apk respectively (they differ — e.g. gettext
// is `gettext-base` on Debian but `gettext` on Fedora/Alpine).
func (p pkgManager) hint(apt, dnf, apk string) string {
	switch p.cmd {
	case "apt install":
		return "apt install " + apt
	case "dnf install":
		return "dnf install " + dnf
	case "apk add":
		return "apk add " + apk
	default: // unknown distro — show every option, let the user pick
		return "apt install " + apt + "   ·   dnf install " + dnf + "   ·   apk add " + apk
	}
}

// readOSRelease is indirected so tests can supply synthetic /etc/os-release
// contents. It returns the file body (empty on any read error).
var readOSRelease = func() string {
	b, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return ""
	}
	return string(b)
}

// detectPkgManager maps the distro's os-release ID / ID_LIKE to a package
// manager. Falls back to an unknown pkgManager (full-list hint) when the distro
// isn't recognised or /etc/os-release is unreadable (e.g. minimal containers).
func detectPkgManager() pkgManager {
	ids := osReleaseIDs(readOSRelease())
	for _, id := range ids {
		switch id {
		case "debian", "ubuntu", "linuxmint", "pop", "raspbian":
			return pkgManager{cmd: "apt install"}
		case "fedora", "rhel", "centos", "rocky", "almalinux":
			return pkgManager{cmd: "dnf install"}
		case "alpine":
			return pkgManager{cmd: "apk add"}
		}
	}
	return pkgManager{} // unknown → full list
}

// osReleaseIDs extracts the ID plus every ID_LIKE token from an os-release body,
// lower-cased and unquoted, ID first. ID_LIKE lets derivatives (Pop!_OS, Mint,
// Rocky) resolve via their upstream even when the exact ID isn't listed above.
func osReleaseIDs(body string) []string {
	var ids []string
	var like []string
	for line := range strings.SplitSeq(body, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		v = strings.ToLower(strings.Trim(v, `"'`))
		switch k {
		case "ID":
			ids = append(ids, v)
		case "ID_LIKE":
			like = append(like, strings.Fields(v)...)
		}
	}
	return append(ids, like...)
}

// tools returns the dependency set adapted to the host OS. The install hints
// differ per platform; the *set* is the same everywhere except that `bash` and
// `envsubst` are only meaningfully checkable where a POSIX shell exists.
func tools(goos string) []Tool {
	// Per-OS install hints. macOS leans on Homebrew; Linux notes the common
	// package managers; Windows routes everything through WSL because the
	// bash-based run step needs a POSIX userland regardless.
	var gettext, dockerHint, bashHint string
	switch goos {
	case "darwin":
		gettext = "brew install gettext && brew link --force gettext"
		dockerHint = "install Docker Desktop (includes the compose v2 plugin)"
		bashHint = "ships with macOS"
	case "windows":
		gettext = "inside WSL2/Git Bash: apt install gettext-base  (or is bundled)"
		dockerHint = "Docker Desktop with WSL2 backend; run the wizard from WSL"
		bashHint = "use WSL2 or Git Bash — native cmd/PowerShell has no bash"
	default: // linux and the rest
		pm := detectPkgManager()
		gettext = pm.hint("gettext-base", "gettext", "gettext")
		dockerHint = "install Docker Engine + the docker-compose-plugin package"
		bashHint = "install via your package manager (usually preinstalled)"
	}

	return []Tool{
		{
			Name: "bash", Tier: Required,
			Why:     "the render + up steps run via bash -c, and run.sh is a bash script",
			Install: bashHint,
		},
		{
			Name: "envsubst", Tier: Required,
			Why:     "expands ${VAR} secrets from .env into the rendered router config",
			Install: gettext,
		},
		{
			Name: "docker", Tier: Required,
			Why:     "builds and runs the smart-router stack",
			Install: dockerHint,
		},
		{
			Name: "docker compose", Probe: dockerComposeV2, Tier: Required,
			Why:     "the run command is `docker compose … up` (the v2 plugin, not docker-compose)",
			Install: dockerHint,
		},
		{
			Name: "go", Tier: Optional,
			Why:     "builds the smartrouter binary for endpoint health checks (skippable if build/ has one)",
			Install: "https://go.dev/dl/  (make wizard runs `make build` first, so this is usually covered)",
		},
		{
			Name: "gh", Tier: Optional,
			Why:     "fetches chain specs via the GitHub API; falls back to a plain HTTPS download when absent",
			Install: "https://cli.github.com/  (then `gh auth login`)",
		},
	}
}

// hasPOSIXShell reports whether a bash is resolvable — used to detect the
// native-Windows-without-WSL case. On Windows-with-Git-Bash bash IS on PATH, so
// this correctly lets that setup through.
func hasPOSIXShell() bool {
	_, err := lookPath("bash")
	return err == nil
}

// Run sweeps the dependency set for the current OS and returns a Report.
func Run() Report {
	goos := runtime.GOOS
	r := Report{OS: goos}
	for _, t := range tools(goos) {
		if !t.found() {
			r.Missing = append(r.Missing, t)
		}
	}
	if goos == "windows" && !hasPOSIXShell() {
		r.WindowsNoShell = true
	}
	return r
}

// Render draws the preflight result as a wizard section. On success it prints a
// single green line; on a miss it lists each absent tool with its reason and
// install hint, and (on native Windows) a WSL callout. It returns whether the
// wizard may proceed — the caller hard-stops when false.
func Render(r Report) bool {
	fmt.Println(ui.Section(0, "Prerequisites"))
	fmt.Println()

	if r.WindowsNoShell {
		fmt.Println(ui.Alert("Windows: no POSIX shell",
			"The wizard's run step needs bash + envsubst, which native Windows\n"+
				"(cmd/PowerShell) doesn't provide. Run the wizard inside WSL2 or\n"+
				"Git Bash. Docker Desktop's WSL2 backend still powers `docker compose`."))
		fmt.Println()
	}

	if len(r.Missing) == 0 && !r.WindowsNoShell {
		fmt.Println("  " + ui.Tick("all prerequisites present ("+r.OS+")"))
		return true
	}

	for _, t := range r.Missing {
		label := t.Name
		if t.Tier == Required {
			fmt.Println("  " + ui.XMark(ui.Er.Render(label)+ui.Hint.Render("  (required)")))
		} else {
			fmt.Println("  " + ui.Wn.Render("! ") + ui.Wn.Render(label) + ui.Hint.Render("  (optional — has a fallback)"))
		}
		fmt.Println("      " + ui.Hint.Render(t.Why))
		fmt.Println("      " + ui.Subtle.Render("install: ") + ui.Accent.Render(t.Install))
	}

	fmt.Println()
	if !r.OK() {
		var names []string
		for _, t := range r.Missing {
			if t.Tier == Required {
				names = append(names, t.Name)
			}
		}
		if r.WindowsNoShell {
			fmt.Println("  " + ui.Er.Render("✗ cannot continue on native Windows — re-run inside WSL2/Git Bash."))
		} else {
			fmt.Println("  " + ui.Er.Render("✗ missing required tool(s): "+strings.Join(names, ", ")+" — install and re-run."))
		}
		return false
	}

	// Only optional tools missing — safe to proceed with a heads-up.
	fmt.Println("  " + ui.Wn.Render("continuing — only optional tools are missing; features above will degrade."))
	return true
}
