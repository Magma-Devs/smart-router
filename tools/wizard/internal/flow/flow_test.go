package flow
import (
	"os";"path/filepath";"strings";"testing"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/emit"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/health"
)
func TestAssignListeners(t *testing.T) {
	chains := []catalog.Chain{
		{Index:"ETH1", Interfaces:[]string{"jsonrpc"}},
		{Index:"LAV1", Interfaces:[]string{"grpc","rest","tendermintrpc"}},
	}
	s := &State{}
	s.AssignListeners(map[string][]string{
		"ETH1": {"jsonrpc"},
		"LAV1": {"rest","grpc"},
	}, chains)
	if len(s.Listeners) != 3 { t.Fatalf("want 3 listeners, got %d", len(s.Listeners)) }
	// ports sequential from 3360, ETH1 first (catalog order)
	if s.Listeners[0].Chain.Index != "ETH1" || s.Listeners[0].Port != 3360 { t.Errorf("L0 = %+v", s.Listeners[0]) }
	if s.Listeners[2].Port != 3362 { t.Errorf("last port = %d, want 3362", s.Listeners[2].Port) }
}
// TestPrevStep locks in Esc/back-navigation after the ws-before-addons reorder:
// the chain must be pDone → pAddons → pWSAsk → pAuthAsk (pLiveGate/pAuthDetails are
// never back-targets), with non-applicable steps skipped. (Naming is no longer a
// per-interface step — it's asked once per round in collectForChain — so pDone is
// the anchor for "the last applicable step of an interface".)
func TestPrevStep(t *testing.T) {
	httpURL := "https://x.lava.build"

	// subscription interface (jsonrpc) WITH addons → both pAddons and pWSAsk apply.
	sub := Listener{Chain: catalog.Chain{Index: "ETH1", Addons: []string{"archive", "debug"}}, Iface: "jsonrpc"}
	if got := prevStep(pDone, sub, httpURL); got != pAddons {
		t.Errorf("sub: prevStep(pDone) = %d, want pAddons(%d)", got, pAddons)
	}
	if got := prevStep(pAddons, sub, httpURL); got != pWSAsk {
		t.Errorf("sub: prevStep(pAddons) = %d, want pWSAsk(%d)", got, pWSAsk)
	}
	if got := prevStep(pWSAsk, sub, httpURL); got != pAuthAsk {
		t.Errorf("sub: prevStep(pWSAsk) = %d, want pAuthAsk(%d)", got, pAuthAsk)
	}

	// non-subscription interface (rest) with NO addons → both pAddons and pWSAsk
	// are skipped; pDone falls straight back to pAuthAsk.
	plain := Listener{Chain: catalog.Chain{Index: "LAVA"}, Iface: "rest"}
	if got := prevStep(pDone, plain, httpURL); got != pAuthAsk {
		t.Errorf("plain: prevStep(pDone) = %d, want pAuthAsk(%d)", got, pAuthAsk)
	}

	// subscription interface but NO addons → pAddons skipped, pWSAsk applies.
	subNoAddons := Listener{Chain: catalog.Chain{Index: "ETH1"}, Iface: "jsonrpc"}
	if got := prevStep(pDone, subNoAddons, httpURL); got != pWSAsk {
		t.Errorf("subNoAddons: prevStep(pDone) = %d, want pWSAsk(%d)", got, pWSAsk)
	}
}

// TestWSProbeConfig locks in the ws-gate fix: the ws url must be probed PAIRED
// with its http base on a SINGLE upstream, never alone. A ws-only provider can't
// be probed — the router always needs the base (no-extension) collection — so a
// lone-ws probe always fails with "ws-only endpoint cannot be probed alone",
// false-failing every healthy ws url at the pWSURL gate.
func TestWSProbeConfig(t *testing.T) {
	l := Listener{Chain: catalog.Chain{Index: "ETH1"}, Iface: "jsonrpc"}
	base := "https://eth1.lava.build"
	ws := "wss://eth1.lava.build/websocket"

	cfg := wsProbeConfig(l, base, ws)
	if n := len(cfg.Primary); n != 1 {
		t.Fatalf("probe config must have exactly ONE upstream (base+ws paired), got %d", n)
	}
	up := cfg.Primary[0]
	// Base FIRST, then ws — the order the emitter and router expect.
	want := []string{base, ws}
	if len(up.URLs) != len(want) || up.URLs[0] != base || up.URLs[1] != ws {
		t.Fatalf("upstream URLs = %v, want %v (http base first, then ws)", up.URLs, want)
	}
	// No addons at this step — ws is verified before addons are chosen, so no
	// archive ws-widening / skip-verifications should leak into the gate probe.
	if len(up.Addons) != 0 {
		t.Errorf("probe upstream must carry no addons, got %v", up.Addons)
	}
	if up.ChainID != "ETH1" || up.Iface != "jsonrpc" {
		t.Errorf("upstream chain/iface = %s/%s, want ETH1/jsonrpc", up.ChainID, up.Iface)
	}
	// The listener mirrors the upstream so the throwaway config is self-consistent.
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].ChainID != "ETH1" || cfg.Listeners[0].Iface != "jsonrpc" {
		t.Errorf("listeners = %+v, want one ETH1/jsonrpc", cfg.Listeners)
	}
	// The rendered YAML carries BOTH urls — proof the router sees the base
	// collection alongside the ws url (the whole point of the fix).
	y := cfg.YAML()
	for _, sub := range []string{`- url: "https://eth1.lava.build"`, `- url: "wss://eth1.lava.build/websocket"`} {
		if !strings.Contains(y, sub) {
			t.Errorf("probe YAML missing %q\n---\n%s", sub, y)
		}
	}
}

// TestAddonProbeConfigIsHTTPOnly locks in the archive false-negative fix: the addon
// probe declares NO ws url, even on a subscription interface. Declaring a ws url
// ws-widens the archive verification to {archive,websocket}; the emitter's
// skip-verifications:[pruning] then strips the archive verification entirely, leaving
// the row with zero verifications, which SupportedAddons reads as "unsupported" —
// the false negative the user hit. http-only keeps the real {archive} verification.
func TestAddonProbeConfigIsHTTPOnly(t *testing.T) {
	l := Listener{Chain: catalog.Chain{Index: "ETH1"}, Iface: "jsonrpc"}
	base := "https://eth1.lava.build"

	cfg := addonProbeConfig(l, base, "archive")
	if n := len(cfg.Primary); n != 1 {
		t.Fatalf("addon probe must have exactly one upstream, got %d", n)
	}
	up := cfg.Primary[0]
	// Exactly the base url — NO ws url paired (that's the whole fix).
	if len(up.URLs) != 1 || up.URLs[0] != base {
		t.Fatalf("addon probe URLs = %v, want [%s] (http base only, no ws)", up.URLs, base)
	}
	if len(up.Addons) != 1 || up.Addons[0] != "archive" {
		t.Errorf("addon probe addons = %v, want [archive]", up.Addons)
	}
	// The rendered YAML must NOT carry a ws url, and (no ws → no widening) must NOT
	// carry the skip-verifications the emitter would add for an archive+ws upstream.
	y := cfg.YAML()
	if strings.Contains(y, "wss://") || strings.Contains(y, "ws://") {
		t.Errorf("addon probe YAML must not declare a ws url\n---\n%s", y)
	}
	if strings.Contains(y, "skip-verifications") {
		t.Errorf("http-only archive probe must not emit skip-verifications (no ws → no widening)\n---\n%s", y)
	}
}

// TestArchiveWsCaveat: the archive-over-ws caveat shows ONLY for a confirmed
// `archive` on a subscription interface that paired a ws url. Everything else
// (no ws, non-archive addon, non-subscription interface) gets no caveat.
func TestArchiveWsCaveat(t *testing.T) {
	sub := Listener{Chain: catalog.Chain{Index: "ETH1"}, Iface: "jsonrpc"}
	rest := Listener{Chain: catalog.Chain{Index: "LAVA"}, Iface: "rest"}

	if c := archiveWsCaveat(sub, true, "archive"); c == "" {
		t.Error("archive + ws on jsonrpc should produce a caveat")
	} else if !strings.Contains(c, "archive over ws is not served") {
		t.Errorf("caveat should state archive-over-ws isn't served, got %q", c)
	}
	if c := archiveWsCaveat(sub, false, "archive"); c != "" {
		t.Errorf("archive WITHOUT ws → no caveat, got %q", c)
	}
	if c := archiveWsCaveat(sub, true, "debug"); c != "" {
		t.Errorf("non-archive addon → no caveat, got %q", c)
	}
	if c := archiveWsCaveat(rest, true, "archive"); c != "" {
		t.Errorf("archive on non-subscription interface → no caveat, got %q", c)
	}
}

// TestNameTaken locks in the duplicate-upstream-name rule: a name collides only
// within the SAME listener (chain + interface), case-insensitively, across BOTH
// primary and backup pools — never across different chains/interfaces. Names are the
// <base>-<iface> form the round-naming derives (e.g. lav1-1-jsonrpc).
func TestNameTaken(t *testing.T) {
	s := &State{
		Primary: []emit.Upstream{
			{Name: "lav1-1-jsonrpc", ChainID: "LAV1", Iface: "jsonrpc"},
			{Name: "lava-1-rest", ChainID: "LAVA", Iface: "rest"},
		},
		Backup: []emit.Upstream{
			{Name: "lav1-bak-jsonrpc", ChainID: "LAV1", Iface: "jsonrpc"},
		},
	}
	// exact + case-insensitive hit on a primary name
	if !s.nameTaken("LAV1", "jsonrpc", "lav1-1-jsonrpc") {
		t.Error("exact primary name must be taken")
	}
	if !s.nameTaken("LAV1", "jsonrpc", "LAV1-1-JSONRPC") {
		t.Error("name match must be case-insensitive")
	}
	// a backup name on the same chain+iface also collides (shared chain router)
	if !s.nameTaken("LAV1", "jsonrpc", "lav1-bak-jsonrpc") {
		t.Error("backup name of the same chain+iface must be taken")
	}
	// free name on the same chain+iface
	if s.nameTaken("LAV1", "jsonrpc", "lav1-2-jsonrpc") {
		t.Error("a fresh name must be available")
	}
	// same string, DIFFERENT interface → not taken (scoped to chain+iface)
	if s.nameTaken("LAV1", "grpc", "lav1-1-jsonrpc") {
		t.Error("name from a different interface must not collide")
	}
	// same string, DIFFERENT chain → not taken
	if s.nameTaken("BASE", "jsonrpc", "lav1-1-jsonrpc") {
		t.Error("name from a different chain must not collide")
	}
}

// TestRoundProviderName locks in the <base>-<iface> derivation: one backend name
// fans out to a distinct, lower-cased provider name per interface.
func TestRoundProviderName(t *testing.T) {
	if got := roundProviderName("lav1-1", "jsonrpc"); got != "lav1-1-jsonrpc" {
		t.Errorf("roundProviderName = %q, want lav1-1-jsonrpc", got)
	}
	// iface is lower-cased; the base is taken verbatim
	if got := roundProviderName("MyBackend", "TendermintRPC"); got != "MyBackend-tendermintrpc" {
		t.Errorf("roundProviderName = %q, want MyBackend-tendermintrpc", got)
	}
}

// TestRoundNameClash: naming a SECOND round with a base whose <base>-<iface> expansion
// duplicates a committed provider is rejected; a fresh base over the round's collected
// upstreams is accepted. This is what stops two backends of one chain sharing names.
// The check runs over the round's actual upstreams (a possibly-partial subset of the
// chain's interfaces, since a backend may skip interfaces), not the listener group.
func TestRoundNameClash(t *testing.T) {
	round := []emit.Upstream{
		{ChainID: "LAV1", Iface: "jsonrpc"},
		{ChainID: "LAV1", Iface: "rest"},
	}
	s := &State{Primary: []emit.Upstream{
		{Name: "lav1-1-jsonrpc", ChainID: "LAV1", Iface: "jsonrpc"},
		{Name: "lav1-1-rest", ChainID: "LAV1", Iface: "rest"},
	}}

	// reusing the first round's base collides on the very first interface
	if clash := s.roundNameClash(round, "lav1-1"); clash != "lav1-1-jsonrpc" {
		t.Errorf("clash = %q, want lav1-1-jsonrpc (base reuse must be rejected)", clash)
	}
	// a fresh base clears the whole round
	if clash := s.roundNameClash(round, "lav1-2"); clash != "" {
		t.Errorf("clash = %q, want \"\" (a fresh base must be free)", clash)
	}
	// a collision on a LATER interface is still caught (only rest collides here)
	s2 := &State{Primary: []emit.Upstream{
		{Name: "shared-rest", ChainID: "LAV1", Iface: "rest"},
	}}
	if clash := s2.roundNameClash(round, "shared"); clash != "shared-rest" {
		t.Errorf("clash = %q, want shared-rest (a later-interface collision must be caught)", clash)
	}

	// Skip case: a backend that serves only ONE of the chain's interfaces is a
	// single-element round. Its clash check is keyed off that upstream's own iface, so
	// reusing the base of an UNRELATED already-committed interface must NOT false-clash
	// (this is the positional-alignment bug the per-upstream keying fixes).
	partial := []emit.Upstream{{ChainID: "LAV1", Iface: "rest"}} // only rest, jsonrpc skipped
	if clash := s.roundNameClash(partial, "lav1-1"); clash != "lav1-1-rest" {
		t.Errorf("partial clash = %q, want lav1-1-rest (rest still collides with a committed rest)", clash)
	}
	// the same partial round under a fresh base commits cleanly — the skipped jsonrpc
	// must not drag a phantom lav1-2-jsonrpc collision into a rest-only backend.
	if clash := s.roundNameClash(partial, "lav1-2"); clash != "" {
		t.Errorf("partial clash = %q, want \"\" (a rest-only backend ignores the skipped jsonrpc)", clash)
	}
}

// TestRoundDefaultBase: both tiers follow one symmetric rule — a per-tier stem
// (<chain> for primary, <chain>-backup for backup) with the backend index appended
// only from the SECOND round on. So the first backend of each tier is unindexed and a
// second disambiguates with -2/-3. Each expands per interface as <base>-<iface>, so a
// backup reads as the same shape as a primary (eth1-backup-jsonrpc vs eth1-jsonrpc).
func TestRoundDefaultBase(t *testing.T) {
	cases := []struct {
		chain, tier string
		round       int
		want        string
	}{
		{"ETH1", "primary", 0, "eth1"},        // first primary: no index
		{"ETH1", "primary", 1, "eth1-2"},      // second primary: -2
		{"ETH1", "primary", 2, "eth1-3"},      // third: -3
		{"ETH1", "backup", 0, "eth1-backup"},  // first backup: stem only
		{"ETH1", "backup", 1, "eth1-backup-2"},
		{"ETH1", "backup", 2, "eth1-backup-3"},
	}
	for _, c := range cases {
		if got := roundDefaultBase(c.chain, c.tier, c.round); got != c.want {
			t.Errorf("roundDefaultBase(%q,%q,%d) = %q, want %q", c.chain, c.tier, c.round, got, c.want)
		}
	}
	// The defaults expand to the documented per-interface provider names, in parallel.
	if got := roundProviderName(roundDefaultBase("ETH1", "primary", 0), "jsonrpc"); got != "eth1-jsonrpc" {
		t.Errorf("primary default over jsonrpc = %q, want eth1-jsonrpc", got)
	}
	if got := roundProviderName(roundDefaultBase("ETH1", "backup", 0), "jsonrpc"); got != "eth1-backup-jsonrpc" {
		t.Errorf("backup default over jsonrpc = %q, want eth1-backup-jsonrpc", got)
	}
}

// TestListenerGroups locks in the per-chain grouping that drives the "add another
// upstream for the whole chain" round loop: contiguous listeners sharing a chain
// index collapse into one group, in catalog order, so a chain's interfaces are
// covered together in a round.
func TestListenerGroups(t *testing.T) {
	s := &State{Listeners: []Listener{
		{Chain: catalog.Chain{Index: "ETH1"}, Iface: "jsonrpc", Port: 3360},
		{Chain: catalog.Chain{Index: "LAV1"}, Iface: "rest", Port: 3361},
		{Chain: catalog.Chain{Index: "LAV1"}, Iface: "grpc", Port: 3362},
		{Chain: catalog.Chain{Index: "LAV1"}, Iface: "tendermintrpc", Port: 3363},
	}}
	groups := s.listenerGroups()
	if len(groups) != 2 {
		t.Fatalf("want 2 chain groups (ETH1, LAV1), got %d", len(groups))
	}
	if len(groups[0]) != 1 || groups[0][0].Chain.Index != "ETH1" {
		t.Errorf("group 0 = %+v, want one ETH1 listener", groups[0])
	}
	if len(groups[1]) != 3 || groups[1][0].Chain.Index != "LAV1" {
		t.Errorf("group 1 = %+v, want three LAV1 listeners", groups[1])
	}
}

func TestComposeFiles(t *testing.T) {
	cases := []struct{ cache, dash bool; want string }{
		{false,false,"-f docker/docker-compose.yml"},
		{true,false,"-f docker/docker-compose.yml -f docker/docker-compose.cache.yml"},
		{false,true,"-f docker/docker-compose.dashboard.yml"},
		{true,true,"-f docker/docker-compose.dashboard.yml -f docker/docker-compose.cache.yml"},
	}
	for _, c := range cases {
		s := &State{Cache:c.cache, Dashboard:c.dash}
		got := strings.Join(s.composeFiles()," ")
		if got != c.want { t.Errorf("cache=%v dash=%v: got %q want %q", c.cache, c.dash, got, c.want) }
	}
}
func TestPlan(t *testing.T) {
	s := &State{Cache:true}
	p := s.Plan("config/local/x.yml")
	if !strings.Contains(p.UpCommand, "SR_CONFIG=config/local/x.yml") { t.Error("up missing SR_CONFIG") }
	if !strings.Contains(p.UpCommand, "cache.yml") { t.Error("up missing cache overlay") }
	if !strings.Contains(p.RenderStep, "envsubst") { t.Error("render missing envsubst") }
	if !strings.Contains(p.DownCommand, "down") { t.Error("down missing") }
	// local catalog (default) → no SR_SPEC prefix (matches compose default)
	if strings.Contains(p.UpCommand, "SR_SPEC=") { t.Error("local catalog should not set SR_SPEC") }
}

func TestSpecArg(t *testing.T) {
	cases := []struct{ src, want string }{
		{"", "specs/"}, {"local", "specs/"},
		{"gh", health.SpecsGitHubURL}, {"raw", health.SpecsGitHubURL},
	}
	for _, c := range cases {
		if got := (&State{Source:c.src}).specArg(); got != c.want {
			t.Errorf("source %q: got %q want %q", c.src, got, c.want)
		}
	}
}

func TestMetricsPort(t *testing.T) {
	if got := (&State{}).metricsPort(); got != "7779" { t.Errorf("default metrics port = %q, want 7779", got) }
	if got := (&State{Metrics:"disabled"}).metricsPort(); got != "" { t.Errorf("disabled metrics port = %q, want empty", got) }
	if got := (&State{Metrics:"0.0.0.0:9999"}).metricsPort(); got != "9999" { t.Errorf("custom metrics port = %q, want 9999", got) }
}

// Remote catalog → SR_SPEC prefix points at the GitHub URL, and the generated
// port-override (publishing the listener ports) is written + added to the -f
// chain.
func TestPlanRemoteSpecAndOverride(t *testing.T) {
	dir := t.TempDir()
	rendered := filepath.Join(dir, "config", "local", "x.yml")
	if err := os.MkdirAll(filepath.Dir(rendered), 0o755); err != nil { t.Fatal(err) }
	s := &State{
		RepoRoot: dir, Source: "gh", RenderedPath: rendered,
		Listeners: []Listener{
			{Chain: catalog.Chain{Index:"ETH1"}, Iface:"jsonrpc", Port:3360},
			{Chain: catalog.Chain{Index:"LAV1"}, Iface:"tendermintrpc", Port:3363},
		},
	}
	rel, _ := filepath.Rel(dir, rendered)
	p := s.Plan(filepath.ToSlash(rel))

	if !strings.Contains(p.UpCommand, "SR_SPEC="+health.SpecsGitHubURL) { t.Errorf("up missing SR_SPEC GitHub URL: %s", p.UpCommand) }
	if !strings.Contains(p.UpCommand, "x.compose.override.yml") { t.Errorf("up missing -f override: %s", p.UpCommand) }

	ov := strings.TrimSuffix(rendered, ".yml") + ".compose.override.yml"
	b, err := os.ReadFile(ov)
	if err != nil { t.Fatalf("override not written: %v", err) }
	for _, want := range []string{`"3360:3360"`, `"3363:3363"`, `"7779:7779"`, "ETH1 jsonrpc", "LAV1 tendermintrpc"} {
		if !strings.Contains(string(b), want) { t.Errorf("override missing %q in:\n%s", want, b) }
	}
}
