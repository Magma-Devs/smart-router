// Command wizard is the smart-router config wizard — a Charm TUI that builds a
// smartrouter YAML config and runs the local docker compose stack.
//
//	go run .                 (from tools/wizard, repo root two levels up)
//	wizard --repo /path      (explicit repo root)
//
// Flow: splash → chains (family + search) → interfaces → endpoints (health) →
// backups → cache → dashboard → save → run → smoke.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/flow"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/health"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/icons"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/laststate"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

func main() {
	repo := flag.String("repo", "", "repo root (default: two levels up from cwd)")
	specsSrc := flag.String("specs", "", "spec source: remote (github) | local (specs/ dir) | auto. Empty = ask.")
	diagnose := flag.Bool("diagnose", false, "print terminal + icon capability and exit")
	last := flag.Bool("last", false, "reprint the run command from the most recent wizard run and exit")
	flag.Parse()

	if *diagnose {
		icons.Diagnose(os.Stdout)
		return
	}

	if *last {
		showLastRun()
		return
	}

	repoRoot := resolveRepo(*repo)
	specDir := filepath.Join(repoRoot, "specs")

	fmt.Print(ui.Banner(100))

	// Spec source defaults to AUTO (remote github, fall back to local specs/).
	// An explicit --specs flag overrides; it can also be changed from inside the
	// chain picker (press 's').
	pref := catalog.SourceAuto
	switch *specsSrc {
	case "local":
		pref = catalog.SourceLocal
	case "remote":
		pref = catalog.SourceRemote
	case "auto":
		pref = catalog.SourceAuto
	}

	chains, source, err := catalog.LoadFrom(specDir, pref)
	if err != nil || len(chains) == 0 {
		fatal(fmt.Errorf("could not load chain catalog: %v", err))
	}

	renderer := icons.NewRenderer(2)
	prober, perr := health.New(repoRoot, 25*time.Second)

	st := &flow.State{
		RepoRoot: repoRoot,
		SpecDir:  specDir,
		Source:   source,
		Icons:    renderer,
		Prober:   prober,
		Metrics:  "0.0.0.0:7779",
	}
	if perr != nil {
		fmt.Println("  " + ui.Wn.Render("! "+perr.Error()+" — endpoint checks limited"))
		st.Prober = nil
	} else {
		syncHealthSpecs(st) // point health at the same spec source as the catalog
		fmt.Println("  " + ui.Tick("endpoint checks: spec-driven 'smartrouter health'"))
	}

	// Stepped flow with back-navigation. Each step returns a direction; "back"
	// re-runs the previous step so the user can redo earlier choices.
	steps := []struct {
		name string
		run  func(*flow.State, []catalog.Chain, *icons.Renderer) flow.Nav
	}{
		{"chains", stepChains},
		{"endpoints", stepEndpoints},
		{"backups", stepBackups},
		{"cache", stepCache},
		{"dashboard", stepDashboard},
		{"save+run", stepSaveRun},
	}
	i := 0
	for i < len(steps) {
		// Redraw from a clean screen each step so going back REPLACES the
		// previous step's output instead of stacking it in scrollback.
		stepNames := make([]string, len(steps))
		for k, s := range steps {
			stepNames[k] = s.name
		}
		// Clean slate each step so back/redo replaces output (never stacks).
		// The chains step opens an alt-screen picker, so it manages its own
		// screen — we only draw the breadcrumb header for the later steps.
		if i > 0 {
			clearScreen()
			fmt.Println(ui.Breadcrumb(stepNames, i))
		}
		switch steps[i].run(st, chains, renderer) {
		case flow.Next:
			i++
		case flow.Back:
			if i > 0 {
				i--
			}
		case flow.Cancel:
			clearScreen()
			fmt.Println(ui.Hint.Render("cancelled — bye."))
			return
		}
	}
}

// clearScreen resets the terminal to a clean top-of-screen state (incl.
// scrollback) so prior steps' output never lingers above.
func clearScreen() { ui.Clear() }

// syncHealthSpecs points the health prober at the SAME spec source the chain
// catalog came from. Remote catalog (gh/raw) → GitHub spec URL so health can
// validate any chain; local catalog → the local specs/ dir.
func syncHealthSpecs(st *flow.State) {
	if st.Prober == nil {
		return
	}
	if st.Source == "local" {
		st.Prober.SetSpecPath("specs/")
	} else {
		st.Prober.SetSpecPath(health.SpecsGitHubURL)
	}
}

// stepChains: chain picker → interfaces → listeners. Pressing 's' in the picker
// re-picks the spec source and reloads the catalog in place.
func stepChains(st *flow.State, chains []catalog.Chain, renderer *icons.Renderer) flow.Nav {
	for {
		model, err := tea.NewProgram(ui.NewChainPicker(chains, st.Source, renderer), tea.WithAltScreen()).Run()
		if err != nil {
			fatal(err)
		}
		pr := model.(ui.ChainPicker)

		if pr.ChangeSource() {
			pref := pickSpecSource()
			if reloaded, src, e := catalog.LoadFrom(st.SpecDir, pref); e == nil && len(reloaded) > 0 {
				chains, st.Source = reloaded, src
				syncHealthSpecs(st) // keep health's spec source in sync
			} else {
				fmt.Println("  " + ui.Wn.Render("! could not load from that source — keeping current catalog"))
			}
			continue // re-open the picker with the (maybe) new catalog
		}
		if pr.Cancelled() || len(pr.Result()) == 0 {
			return flow.Cancel
		}
		sel, ok := flow.SelectInterfaces(pr.Result())
		if !ok {
			return flow.Back
		}
		st.AssignListeners(sel, pr.Result())
		st.Primary, st.Backup = nil, nil // reset downstream so redo doesn't dup
		return flow.Next
	}
}

func stepEndpoints(st *flow.State, _ []catalog.Chain, _ *icons.Renderer) flow.Nav {
	st.Primary = nil // fresh each entry (supports redo)
	return st.CollectEndpoints("primary") // Esc inside returns Back
}

func stepBackups(st *flow.State, _ []catalog.Chain, _ *icons.Renderer) flow.Nav {
	st.Backup = nil
	return st.Backups()
}

func stepCache(st *flow.State, _ []catalog.Chain, _ *icons.Renderer) flow.Nav {
	return st.CacheStep()
}

func stepDashboard(st *flow.State, _ []catalog.Chain, _ *icons.Renderer) flow.Nav {
	return st.DashboardStep()
}

func stepSaveRun(st *flow.State, _ []catalog.Chain, _ *icons.Renderer) flow.Nav {
	rel, problems, nav := saveStep(st)
	if nav == flow.Back { // Esc on the save form → back to cache/dashboard
		return flow.Back
	}
	plan := st.Plan(rel)
	// Persist this plan as the "most recent run" so `wizard --last` can reprint
	// it later. The flow can't read the clock, so we stamp GeneratedAt here.
	// Best-effort: a write failure must never derail the run the user just built.
	rec := st.LastRunRecord(plan)
	rec.GeneratedAt = time.Now().Format(time.RFC3339)
	if err := laststate.Save(rec); err != nil {
		fmt.Println("  " + ui.Hint.Render("(couldn't save last-run record: "+err.Error()+")"))
	}
	st.ShowPlan(plan)
	if len(problems) == 0 && confirmRun() {
		if err := st.Up(plan); err != nil {
			fmt.Println("  " + ui.Er.Render(ui.Cross+" compose up failed: "+err.Error()))
		} else {
			st.Smoke()
		}
	}
	return flow.Next
}

func saveStep(st *flow.State) (string, []string, flow.Nav) {
	fmt.Println(ui.Section(6, "Save"))
	name := "smartrouter_custom"
	if st.Cache {
		name += "_cached"
	}
	dir := st.SaveDir()
	if flow.RunForm(huh.NewGroup(
		huh.NewInput().
			Title("save directory").
			Description("where to write the config (absolute, or relative to the repo root)").
			Value(&dir).
			Placeholder(dir).
			Validate(validDir),
		huh.NewInput().
			Title("config name").
			DescriptionFunc(func() string { return "→ " + joinPath(dir, name) + ".yml" }, &dir).
			Value(&name).
			Placeholder(name),
	)) == flow.Back {
		return "", nil, flow.Back
	}

	st.ConfigDir = resolveDir(st.RepoRoot, dir)
	rel, problems, err := st.Save(name)
	if err != nil {
		fatal(err)
	}
	if len(problems) == 0 {
		fmt.Println("  " + ui.Tick("wrote "+rel+" (lint clean)"))
	} else {
		fmt.Println("  " + ui.Wn.Render("! lint warnings:"))
		for _, p := range problems {
			fmt.Println("    " + ui.Hint.Render("· "+p))
		}
	}
	if st.Prober != nil && len(problems) == 0 {
		env, _ := st.Prober.ProbeConfig(rel, false)
		switch {
		case env != nil && env.OK:
			fmt.Println("  " + ui.Tick("health: every provider/node-url verified (http + ws + addons)"))
		case env != nil:
			fmt.Println("  " + ui.Wn.Render("! health: some endpoints did not verify —"))
			for _, r := range env.FailingRows() {
				fmt.Println("    " + ui.Hint.Render("✗ "+r))
			}
		}
	}
	return rel, problems, flow.Next
}

// resolveDir makes dir absolute: an absolute path is used as-is; a relative one
// is resolved against the repo root (so config/local works and so does ~/foo).
func resolveDir(repoRoot, dir string) string {
	dir = strings.TrimSpace(dir)
	if strings.HasPrefix(dir, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, dir[2:])
		}
	}
	if filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	return filepath.Join(repoRoot, dir)
}

// joinPath previews "<dir>/<name>" for the description.
func joinPath(dir, name string) string {
	if name == "" {
		name = "<name>"
	}
	return filepath.Join(dir, name)
}

// validDir rejects empty input and obvious non-directory paths (a file).
func validDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("enter a directory")
	}
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		return fmt.Errorf("that path is a file, not a directory")
	}
	return nil
}

// pickSpecSource asks where to load chain specs from (default: remote github).
func pickSpecSource() catalog.Source {
	choice := "remote"
	_ = flow.RunForm(huh.NewGroup(
		huh.NewSelect[string]().
			Title("Where should chain specs come from?").
			Description("the catalog of supported chains").
			Options(
				huh.NewOption("remote — magma-Devs/lava-specs on GitHub (latest)", "remote"),
				huh.NewOption("local — the repo's specs/ directory", "local"),
				huh.NewOption("auto — remote, fall back to local if offline", "auto"),
			).
			Value(&choice),
	))
	switch choice {
	case "local":
		return catalog.SourceLocal
	case "auto":
		return catalog.SourceAuto
	default:
		return catalog.SourceRemote
	}
}

// resolveRepo finds the smart-router repo root. An explicit --repo wins;
// otherwise we search upward from the binary's location AND the cwd for the
// directory that actually holds specs/ + cmd/smartrouter (the repo markers), so
// it works whether launched via `make wizard`, `go run .`, or a built binary
// run from anywhere.
func resolveRepo(flagVal string) string {
	if flagVal != "" {
		abs, _ := filepath.Abs(flagVal)
		return abs
	}
	var starts []string
	if exe, err := os.Executable(); err == nil {
		starts = append(starts, filepath.Dir(exe))
	}
	if wd, err := os.Getwd(); err == nil {
		starts = append(starts, wd)
	}
	for _, start := range starts {
		if root := walkUpForRepo(start); root != "" {
			return root
		}
	}
	// Last resort: cwd (caller will get a clear error if specs/ is absent).
	wd, _ := os.Getwd()
	return wd
}

// walkUpForRepo ascends from dir looking for the smart-router repo markers.
func walkUpForRepo(dir string) string {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	for {
		// Repo root has specs/ AND cmd/smartrouter (distinguishes it from the
		// tools/wizard module dir, which has neither).
		if isDir(filepath.Join(dir, "specs")) && isDir(filepath.Join(dir, "cmd", "smartrouter")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}


// showLastRun reprints the most recently generated run plan (`--last`). It reads
// the global record written on the last finish; with nothing recorded yet it
// nudges the user to run the wizard first. Printed under the banner so the
// out-of-context panel still reads as part of the product.
func showLastRun() {
	fmt.Print(ui.Banner(100))
	rec, err := laststate.Load()
	if err != nil {
		fatal(err)
	}
	if rec == nil {
		path, _ := laststate.Path()
		fmt.Println("  " + ui.Wn.Render("! no run recorded yet — finish the wizard once, then `--last` will reprint its run command."))
		if path != "" {
			fmt.Println("  " + ui.Hint.Render("(looked in "+path+")"))
		}
		return
	}
	flow.PrintLastRun(*rec)
}

func confirmRun() bool {
	v := false
	_ = huh.NewForm(huh.NewGroup(
		huh.NewConfirm().Title("Run the stack now? (docker compose up --build)").Value(&v),
	)).WithTheme(ui.HuhTheme()).Run()
	return v
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, ui.Er.Render(ui.Cross+" "+err.Error()))
	os.Exit(1)
}
