// Package health validates RPC endpoints via the spec-driven `smartrouter
// health` subcommand (mirrors scripts/wizard/lib/health.sh).
//
// For each (chain, api-interface, node-url) the router runs the spec's own
// verifications — latest-block + every addon/extension check + websocket — and
// emits a JSON envelope. We shell out and parse it; one row per (provider,
// node-url) with transport http|ws.
package health

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Envelope is the `smartrouter health` JSON document.
type Envelope struct {
	OK      bool     `json:"ok"`
	Error   *string  `json:"error"`
	Results []Result `json:"results"`
}

type Result struct {
	Name          string         `json:"name"`
	ChainID       string         `json:"chainId"`
	APIInterface  string         `json:"apiInterface"`
	URL           string         `json:"url"`
	Transport     string         `json:"transport"` // http | ws
	Addons        []string       `json:"addons"`
	Extensions    []string       `json:"extensions"`
	SpecValid     bool           `json:"specValid"`
	LatestBlock   int64          `json:"latestBlock"`
	OK            bool           `json:"ok"`
	Error         string         `json:"error,omitempty"`
	Verifications []Verification `json:"verifications"`
}

type Verification struct {
	Name      string `json:"name"`
	Addon     string `json:"addon"`
	Extension string `json:"extension"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

// Prober runs health checks against a located smartrouter binary.
type Prober struct {
	bin      string
	specPath string
	repoRoot string
	timeout  time.Duration
}

// New locates a smartrouter binary ($SMARTROUTER_BIN → repoRoot/build → PATH →
// `go build`) and returns a Prober. specPath/repoRoot are repo-relative anchors.
func New(repoRoot string, timeout time.Duration) (*Prober, error) {
	bin, err := locate(repoRoot)
	if err != nil {
		return nil, err
	}
	return &Prober{bin: bin, specPath: "specs/", repoRoot: repoRoot, timeout: timeout}, nil
}

// SpecsGitHubURL is the remote spec source the health binary can fetch from —
// used when the wizard's catalog came from remote, so health can validate
// chains (e.g. LAV1) that aren't in the local specs/ dir.
const SpecsGitHubURL = "https://github.com/magma-Devs/lava-specs/tree/main"

// SetSpecPath points the health probe at a spec source: the local "specs/" dir
// or a remote GitHub URL. Must match where the chain catalog came from.
func (p *Prober) SetSpecPath(path string) {
	if path != "" {
		p.specPath = path
	}
}

func locate(repoRoot string) (string, error) {
	if b := os.Getenv("SMARTROUTER_BIN"); b != "" {
		if isExec(b) {
			return b, nil
		}
	}
	if b := filepath.Join(repoRoot, "build", "smartrouter"); isExec(b) {
		return b, nil
	}
	if b, err := exec.LookPath("smartrouter"); err == nil {
		return b, nil
	}
	// Build from the checkout.
	if _, err := exec.LookPath("go"); err == nil {
		out := filepath.Join(repoRoot, "build", "smartrouter")
		_ = os.MkdirAll(filepath.Join(repoRoot, "build"), 0o755)
		cmd := exec.Command("go", "build", "-o", out, "./cmd/smartrouter")
		cmd.Dir = repoRoot
		if err := cmd.Run(); err == nil && isExec(out) {
			return out, nil
		}
	}
	return "", fmt.Errorf("no smartrouter binary found (set SMARTROUTER_BIN or run 'make build')")
}

func isExec(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0
}

// Bin returns the located binary path (for display).
func (p *Prober) Bin() string { return p.bin }

// ProbeInline runs `smartrouter health <url> <chain> <iface>` (no addons).
func (p *Prober) ProbeInline(url, chainID, iface string) (*Envelope, error) {
	return p.run([]string{"health", url, chainID, iface})
}

// ProbeConfig runs `smartrouter health <repo-relative-config>`.
func (p *Prober) ProbeConfig(repoRelPath string, includeBackup bool) (*Envelope, error) {
	args := []string{"health", repoRelPath}
	if includeBackup {
		args = append(args, "--include-backup")
	}
	return p.run(args)
}

func (p *Prober) run(args []string) (*Envelope, error) {
	args = append(args,
		"--use-static-spec", p.specPath,
		"--timeout", p.timeout.String(),
		"--log-level", "error",
	)
	cmd := exec.Command(p.bin, args...)
	cmd.Dir = p.repoRoot // so viper resolves config + specs/ relative to repo
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = nil // logs go to stderr; ignore
	_ = cmd.Run()    // health exits 0 for completed runs; failures are data
	var env Envelope
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		return nil, fmt.Errorf("health emitted no/invalid JSON: %w", err)
	}
	return &env, nil
}

// --- summaries -----------------------------------------------------------

// Live reports whether every row in an inline probe verified, plus a detail
// string (max latest block, or the first error).
func (e *Envelope) Live() (ok bool, detail string) {
	if e == nil || len(e.Results) == 0 {
		if e != nil && e.Error != nil {
			return false, *e.Error
		}
		return false, "no result"
	}
	var maxBlock int64
	allOK := true
	firstErr := ""
	for _, r := range e.Results {
		if r.LatestBlock > maxBlock {
			maxBlock = r.LatestBlock
		}
		if !r.OK {
			allOK = false
			if firstErr == "" {
				firstErr = r.Error
			}
		}
	}
	if allOK {
		return true, fmt.Sprintf("block=%d", maxBlock)
	}
	if firstErr == "" {
		firstErr = "verification failed"
	}
	return false, firstErr
}

// WSLive judges the websocket node-url(s) specifically — used when a base+ws
// config is probed to gate the ws url alone. It returns OK only when at least one
// ws-transport row is present AND every ws row verified, so a failing http base
// row (e.g. one the user force-added despite a dead probe) can't masquerade as a
// ws failure, and a config with no ws row at all reports not-live rather than a
// vacuous pass. detail carries the ws row's block on success or its first error.
func (e *Envelope) WSLive() (ok bool, detail string) {
	if e == nil || len(e.Results) == 0 {
		if e != nil && e.Error != nil {
			return false, *e.Error
		}
		return false, "no result"
	}
	seen, allOK, firstErr := false, true, ""
	var block int64
	for _, r := range e.Results {
		if r.Transport != "ws" {
			continue
		}
		seen = true
		if r.LatestBlock > block {
			block = r.LatestBlock
		}
		if !r.OK {
			allOK = false
			if firstErr == "" {
				firstErr = r.Error
			}
		}
	}
	if !seen {
		return false, "no websocket endpoint was probed"
	}
	if allOK {
		return true, fmt.Sprintf("block=%d", block)
	}
	if firstErr == "" {
		firstErr = "verification failed"
	}
	return false, firstErr
}

// SupportedAddons returns which of candidates the probe confirms supported
// (every verification tagged with the addon/extension passed).
func (e *Envelope) SupportedAddons(candidates []string) []string {
	if e == nil {
		return nil
	}
	var out []string
	for _, a := range candidates {
		seen, allOK := false, true
		for _, r := range e.Results {
			for _, v := range r.Verifications {
				if v.Addon == a || v.Extension == a {
					seen = true
					if !v.OK {
						allOK = false
					}
				}
			}
		}
		if seen && allOK {
			out = append(out, a)
		}
	}
	return out
}

// FailingRows returns human-readable lines for endpoints that didn't verify.
func (e *Envelope) FailingRows() []string {
	var out []string
	for _, r := range e.Results {
		if !r.OK {
			msg := r.Error
			if msg == "" {
				msg = "verification failed"
			}
			out = append(out, fmt.Sprintf("%s [%s] %s: %s", r.Name, r.Transport, r.URL, msg))
		}
	}
	return out
}

// WSDefault guesses a wss:// pairing for an http(s) url. Lava gateways serve ws
// at /websocket; otherwise same origin/path.
func WSDefault(url string) string {
	ws := url
	if rest, ok := strings.CutPrefix(ws, "https:"); ok {
		ws = "wss:" + rest
	} else if rest, ok := strings.CutPrefix(ws, "http:"); ok {
		ws = "ws:" + rest
	}
	if strings.Contains(url, ".lava.build") && !strings.HasSuffix(ws, "/websocket") {
		ws = strings.TrimRight(ws, "/") + "/websocket"
	}
	return ws
}
