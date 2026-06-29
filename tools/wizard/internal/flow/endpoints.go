package flow

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/huh/spinner"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/emit"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/health"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

// secUpstreams is the top-level wizard step number for the upstream-collection
// screen (matches the 1=listeners, 2=upstreams, 3=backup… sequence used across the
// flow). The per-upstream sub-steps (websocket, addons) render as "secUpstreams.N"
// indented headers under it, so the numbering stays in lockstep with this head.
const secUpstreams = 2

// CollectEndpoints gathers upstreams for every selected listener, grouped by chain.
// tier is "primary" / "backup".
//
// "Add another upstream" is asked ONCE PER CHAIN, not per interface: a chain with
// jsonrpc + rest + tendermintrpc collects one upstream for each in a single ROUND,
// then asks "add another upstream for <chain>?". A 'yes' runs another full round
// across all the chain's interfaces (a 2nd backend serving the same chain), rather
// than asking three separate per-interface "another?" questions.
//
// Esc on the very first URL prompt returns Back (to the previous wizard step); a
// clean finish returns Next.
func (s *State) CollectEndpoints(tier string) Nav {
	for _, group := range s.listenerGroups() {
		if nav := s.collectForChain(group, tier); nav != Next {
			return nav
		}
	}
	return Next
}

// listenerGroups splits s.Listeners into per-chain runs. Listeners are built in
// stable catalog order (see AssignListeners), so a chain's interfaces are already
// contiguous — group by a change in chain index.
func (s *State) listenerGroups() [][]Listener {
	var groups [][]Listener
	for _, l := range s.Listeners {
		if n := len(groups); n > 0 && groups[n-1][0].Chain.Index == l.Chain.Index {
			groups[n-1] = append(groups[n-1], l)
			continue
		}
		groups = append(groups, []Listener{l})
	}
	return groups
}

// nameTaken reports whether `name` (case-insensitively) is already used by an
// already-collected upstream of the SAME chain + interface, across BOTH primary and
// backup — they feed the same chain router, so their provider names must be jointly
// distinct or the emitted config is ambiguous. Scoped to (chain, iface), so the same
// name may freely recur across different chains/interfaces.
func (s *State) nameTaken(chain, iface, name string) bool {
	name = strings.ToLower(name)
	for _, up := range append(append([]emit.Upstream{}, s.Primary...), s.Backup...) {
		if up.ChainID == chain && up.Iface == iface && strings.ToLower(up.Name) == name {
			return true
		}
	}
	return false
}

// collectForChain runs the per-chain round loop. Each ROUND is one backend for the
// chain: it collects a URL (+auth/ws/addons) for every interface of the chain, then
// asks for ONE name for the whole backend — each interface's provider is committed as
// <base>-<iface>. The name's default reflects the tier — <chain> for primary,
// <chain>-backup for backup — with a -<n> backend index appended only from the second
// round on (see roundDefaultBase). After a round it asks whether to add another
// backend (another full round) for the chain.
func (s *State) collectForChain(group []Listener, tier string) Nav {
	chain := group[0].Chain.Index
	for round := 0; ; round++ {
		roundUps := make([]emit.Upstream, 0, len(group))
	interfaces:
		for i, l := range group {
			ui.Clear() // one clean screen per interface — no stacking
			if tier == "primary" {
				s.printListeners()
			}
			// Recap the distinct backends already committed for this chain so the user
			// sees them accumulate (and can tell upstream #1 from #2 at a glance).
			s.printChainUpstreams(chain, tier)
			// One head line carries all the context: chain/iface, the tier, which
			// backend is being built, and (only when the chain has >1 interface) the
			// interface counter — no separate badge/progress line to clutter the screen.
			head := fmt.Sprintf("%s/%s · %s upstream #%d", l.Chain.Index, l.Iface, tier, round+1)
			if len(group) > 1 {
				head += fmt.Sprintf(" · iface %d/%d", i+1, len(group))
			}
			fmt.Println(ui.Section(secUpstreams, head))
			// A backend may serve only SOME of its chain's interfaces: a blank URL skips
			// this interface rather than aborting. The round still groups whatever the
			// user fills under one name; a round where every interface is skipped commits
			// nothing. (Only meaningful to hint when the chain has >1 interface.)
			if len(group) > 1 {
				fmt.Println("  " + ui.Hint.Render("leave the URL blank to skip this interface for this backend."))
			}

			// Every interface is optional (required=false), so a blank URL returns Cancel
			// = "skip". collectOneUpstream still re-prompts on a blank it can't accept
			// elsewhere; here a blank simply drops the interface from the round.
			up, nav := s.collectOneUpstream(l, false)
			switch nav {
			case Back:
				// Esc on the very first prompt of the very first interface of the first
				// round → previous wizard step. Anywhere later, Esc means "done adding
				// interfaces to this backend" — commit what's collected so far.
				if round == 0 && i == 0 && len(roundUps) == 0 {
					return Back
				}
				break interfaces
			case Cancel:
				// Blank URL → this backend doesn't serve this interface. Skip it and move
				// to the next; don't abort the whole round.
				continue
			}
			roundUps = append(roundUps, up)
		}

		// A round with no filled interfaces commits nothing — e.g. the user skipped
		// every interface (or declined the optional backup outright on round 0). Stop
		// adding to this chain.
		if len(roundUps) == 0 {
			return Next
		}

		// Name the backend ONCE and commit each collected interface as <base>-<iface>.
		// Esc here re-asks (the name is the round's only naming step).
		if !s.nameAndCommitRound(group, tier, round, roundUps) {
			return Next // Esc out of naming → stop adding to this chain
		}

		// Offer another backend for the WHOLE chain (interfaces collected per round),
		// not a separate per-interface prompt.
		more, nav := confirmNav(fmt.Sprintf("  add another upstream for %s?", chain), false)
		if nav == Back || !more {
			return Next
		}
	}
}

// roundProviderName derives the provider name for one interface of a round-backend
// from the user's single base name: <base>-<iface> (lower-cased). So base "lav1-1"
// over jsonrpc/rest/tendermintrpc yields lav1-1-jsonrpc, lav1-1-rest, … — distinct
// per interface, and distinct across rounds as long as the base differs.
func roundProviderName(base, iface string) string {
	return fmt.Sprintf("%s-%s", base, strings.ToLower(iface))
}

// roundNameClash returns the first derived provider name (over the round's collected
// upstreams `ups`, using `base`) that already collides with a committed upstream, or
// "" if the base is free. Iterates the upstreams actually collected this round — which
// may be a SUBSET of the chain's interfaces, since a backend can skip interfaces — so
// each name is keyed off the upstream's own (chain, iface). Used to reject a backend
// name whose <base>-<iface> expansion would duplicate a prior round.
func (s *State) roundNameClash(ups []emit.Upstream, base string) string {
	for _, up := range ups {
		name := roundProviderName(base, up.Iface)
		if s.nameTaken(up.ChainID, up.Iface, name) {
			return name
		}
	}
	return ""
}

// roundDefaultBase is the suggested base name for a round's backend. The two tiers
// follow ONE symmetric rule so their defaults read in parallel: a per-tier stem —
// <chain> for primary, <chain>-backup for backup — with the backend index appended
// ONLY from the second round on. So the first (typical) backend of each tier carries
// no index, and a second backend disambiguates with -2, -3, …:
//
//	round 0   primary: eth1          backup: eth1-backup
//	round 1   primary: eth1-2        backup: eth1-backup-2
//
// Each default expands per interface as <base>-<iface> (see roundProviderName), so a
// backup reads as eth1-backup-jsonrpc — the same shape as a primary's eth1-jsonrpc,
// just with the "backup" stem.
func roundDefaultBase(chain, tier string, round int) string {
	stem := strings.ToLower(chain)
	if tier == "backup" {
		stem += "-backup"
	}
	if round == 0 {
		return stem
	}
	return fmt.Sprintf("%s-%d", stem, round+1)
}

// nameAndCommitRound asks for a single base name for the round's backend, derives a
// per-interface provider name as <base>-<iface>, validates none collide with an
// already-committed provider, then appends them to the tier and prints one aligned
// summary. Returns false if the user Esc'd out of the name prompt (nothing committed).
func (s *State) nameAndCommitRound(group []Listener, tier string, round int, ups []emit.Upstream) bool {
	def := roundDefaultBase(group[0].Chain.Index, tier, round)
	for {
		base := def
		fmt.Println(ui.Section(secUpstreams, fmt.Sprintf("%s — name this backend", group[0].Chain.Index)))
		fmt.Println("  " + ui.Hint.Render(fmt.Sprintf("one name for upstream #%d — each interface becomes <name>-<iface> (e.g. %s).",
			round+1, roundProviderName(def, ups[0].Iface))))
		if RunForm(huh.NewGroup(huh.NewInput().Title("  backend name").
			Value(&base).Placeholder(def))) == Back {
			return false
		}
		base = strings.TrimSpace(base)
		if base == "" {
			base = def
		}
		// A round can't reuse a prior round's names — reject if any <base>-<iface>
		// collides with an already-committed provider of the same (chain, iface).
		if clash := s.roundNameClash(ups, base); clash != "" {
			fmt.Println(ui.Alert("DUPLICATE UPSTREAM NAME",
				fmt.Sprintf("%q is already used by another %s upstream.\nPick a different backend name — each interface is committed as <name>-<iface>.",
					clash, group[0].Chain.Index)))
			continue // re-ask
		}
		for i := range ups {
			ups[i].Name = roundProviderName(base, ups[i].Iface)
			if tier == "backup" {
				s.Backup = append(s.Backup, ups[i])
			} else {
				s.Primary = append(s.Primary, ups[i])
			}
		}
		printRoundSummary(round, ups)
		return true
	}
}

// printRoundSummary prints one aligned block listing every provider a round
// committed, so the backend's interfaces read as a single result rather than a
// scattered set of per-interface "added X" lines.
func printRoundSummary(round int, ups []emit.Upstream) {
	w := 0
	for _, up := range ups {
		if len(up.Name) > w {
			w = len(up.Name)
		}
	}
	fmt.Printf("  %s\n", ui.Tick(ui.Subtle.Render(fmt.Sprintf("upstream #%d committed:", round+1))))
	for _, up := range ups {
		transport := "http"
		if len(up.URLs) > 1 {
			transport = "http + ws"
		}
		extras := ""
		if len(up.Addons) > 0 {
			extras = " " + ui.Hint.Render("· "+strings.Join(up.Addons, ", "))
		}
		fmt.Printf("    %s%-*s  %s%s\n",
			ui.Mark(), w, up.Name, ui.Hint.Render(fmt.Sprintf("%-13s %s", up.Iface, transport)), extras)
	}
}

// printListeners shows the chosen (chain, interface) → port map as context.
func (s *State) printListeners() {
	fmt.Println(ui.Section(1, "Selected listeners"))
	for _, l := range s.Listeners {
		fmt.Printf("  %s%s/%s %s 0.0.0.0:%d\n",
			ui.Mark(), l.Chain.Index, l.Iface, ui.Hint.Render(ui.Arrow), l.Port)
	}
}

// printChainUpstreams recaps the upstreams already collected for `chain` in this
// tier, so each round screen shows the distinct backends accumulating rather than
// scrolling them off. Renders nothing before the first upstream lands.
func (s *State) printChainUpstreams(chain, tier string) {
	pool := s.Primary
	if tier == "backup" {
		pool = s.Backup
	}
	var rows []emit.Upstream
	for _, up := range pool {
		if up.ChainID == chain {
			rows = append(rows, up)
		}
	}
	if len(rows) == 0 {
		return
	}
	w := 0
	for _, up := range rows {
		if len(up.Name) > w {
			w = len(up.Name)
		}
	}
	fmt.Println("  " + ui.Subtle.Render(fmt.Sprintf("%s upstreams so far", chain)))
	for _, up := range rows {
		transport := "http"
		if len(up.URLs) > 1 {
			transport = "http + ws"
		}
		extras := ""
		if len(up.Addons) > 0 {
			extras = " " + ui.Hint.Render("· "+strings.Join(up.Addons, ", "))
		}
		fmt.Printf("    %s%-*s  %s%s\n",
			ui.Mark(), w, up.Name, ui.Hint.Render(fmt.Sprintf("%-13s %s", up.Iface, transport)), extras)
	}
}

// upstream prompt steps, in order. Esc on a step goes to the previous
// APPLICABLE one (skipping steps that don't apply for this chain/interface,
// e.g. websocket on non-jsonrpc, addons when the chain has none) — otherwise
// Esc would bounce into an auto-skipped step and loop.
//
// The websocket steps run BEFORE addons on purpose: the addon probe's verdict
// for `archive` depends on whether a ws url is present (a subscription spec
// ws-widens the archive verification), so the ws decision must be known by the
// time addons are probed. See probeOneAddon.
const (
	pURL = iota
	pAuthAsk
	pAuthDetails
	pLiveGate
	pWSAsk
	pWSURL
	pAddons
	pDone
)

// wsApplies reports whether the websocket prompt is shown for this endpoint.
// Subscriptions (and thus ws) are a spec property, not a single interface:
// both jsonrpc (eth_subscribe) and tendermintrpc (Tendermint subscribe over
// /websocket) carry them. The health pass is the final authority; here we just
// offer the pairing for the interfaces that can use it.
func wsApplies(l Listener, url string) bool {
	if !(strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://")) {
		return false
	}
	return l.Iface == "jsonrpc" || l.Iface == "tendermintrpc"
}

// stepApplies reports whether a given step is an interactive prompt for this
// endpoint (so Esc can target the right previous step).
func stepApplies(step int, l Listener, url string) bool {
	switch step {
	case pAddons:
		return len(l.Chain.Addons) > 0
	case pWSAsk:
		return wsApplies(l, url)
	default:
		return true // URL / auth-ask are always interactive
	}
}

// prevStep returns the previous APPLICABLE interactive prompt before `from`.
// pLiveGate and pAuthDetails are skipped as back-targets (live-gate is shown
// only on failure; auth-details only when auth was requested) — Esc from after
// them lands on pAuthAsk, the real decision point.
func prevStep(from int, l Listener, url string) int {
	for s := from - 1; s > pURL; s-- {
		switch s {
		case pLiveGate, pAuthDetails:
			continue // not a stable back-target
		case pAddons, pWSAsk:
			if !stepApplies(s, l, url) {
				continue
			}
			return s
		case pAuthAsk:
			return s
		}
	}
	return pURL
}

// subHeaderSteps are the upstream sub-steps that render an indented "secUpstreams.N"
// header, in display order. Their number is positional, NOT a render counter, so
// Esc/back-navigation re-renders the SAME header with the SAME number (websocket
// stays 2.1, addons stays 2.2) instead of drifting upward on each revisit.
var subHeaderSteps = []int{pWSAsk, pAddons}

// subLabel returns the dotted "secUpstreams.N" label for a sub-step header, where N
// is the 1-based position of `step` among the sub-steps that APPLY to this endpoint
// (so addons becomes 2.1 when websocket doesn't apply, rather than leaving a gap).
func subLabel(step int, l Listener, url string) string {
	n := 0
	for _, hs := range subHeaderSteps {
		if !stepApplies(hs, l, url) {
			continue
		}
		n++
		if hs == step {
			break
		}
	}
	return fmt.Sprintf("%d.%d", secUpstreams, n)
}

// collectOneUpstream gathers a single interface's upstream (one provider for this
// listener's chain+interface) as a sequence of prompts where Esc steps back exactly
// ONE prompt (not the whole wizard step). It does NOT name the provider or commit it
// to state — the caller (collectForChain) names the whole round's backend once, after
// every interface is collected, and derives each provider name as <base>-<iface>.
// Returns the assembled upstream (name left blank) and a Nav:
//   Next   — an upstream was collected (first return is valid)
//   Back   — Esc on the URL prompt: caller decides (prev step / stop)
//   Cancel — blank URL while `required` is false: skip this interface
//
// required: if true a blank URL re-prompts (it's mandatory); if false a blank URL
// returns Cancel. collectForChain passes false for every interface — a backend may
// serve only SOME of its chain's interfaces, so a blank URL on any one skips just that
// interface rather than aborting the backend (the round still commits whatever was
// filled). A fully-empty round (every interface skipped) commits nothing.
func (s *State) collectOneUpstream(l Listener, required bool) (emit.Upstream, Nav) {
	var (
		url      string
		up       = emit.Upstream{ChainID: l.Chain.Index, Iface: l.Iface}
		wantAuth bool
		addWs    bool
		liveOK   bool
		liveDet  string
		probed   bool
	)
	step := pURL
	for step != pDone {
		switch step {
		case pURL:
			url = ""
			if RunForm(huh.NewGroup(huh.NewInput().Title("    upstream URL").
				Placeholder("https://…").Value(&url))) == Back {
				return up, Back
			}
			url = strings.TrimSpace(url)
			if url == "" {
				// A blank URL means "this backend doesn't serve this interface" — skip it.
				// When required (not used by collectForChain today) a blank re-prompts
				// instead; otherwise a blank returns Cancel so the caller drops just this
				// interface from the round.
				if required {
					fmt.Println(ui.Alert("AN UPSTREAM IS REQUIRED",
						fmt.Sprintf("%s/%s was selected, so it needs an RPC endpoint.\nEnter a URL — you can't continue without one.",
							l.Chain.Index, l.Iface)))
					continue // stay on pURL
				}
				return up, Cancel // optional — skip
			}
			up.URLs = []string{url}
			probed = false
			step = pAuthAsk

		case pAuthAsk:
			var nav Nav
			wantAuth, nav = confirmEmph(ui.Accent.Render("🔑 Does this endpoint need an API key / auth?"))
			if nav == Back {
				step = pURL
				continue
			}
			if wantAuth {
				step = pAuthDetails
			} else {
				up.Auth = nil
				step = pLiveGate
			}

		case pAuthDetails:
			a, nav := collectAuth(l.Chain.Index, len(s.envVars()))
			if nav == Back {
				step = pAuthAsk
				continue
			}
			up.Auth = a
			step = pLiveGate

		case pLiveGate:
			if !probed {
				liveOK, liveDet = s.probeLive(url, l.Chain.Index, l.Iface, up.Auth)
				probed = true
			}
			if liveOK {
				fmt.Println("  " + ui.Tick("live · "+liveDet))
				step = pWSAsk
				continue
			}
			fmt.Println(ui.Alert("ENDPOINT DID NOT VERIFY", url+"\n"+liveDet))
			add, nav := confirmNav("      add it anyway?", false)
			if nav == Back {
				step = pAuthAsk
				probed = false
				continue
			}
			if !add {
				step = pURL // re-enter a different URL
				continue
			}
			step = pWSAsk

		case pWSAsk:
			if !wsApplies(l, url) {
				addWs = false
				up.URLs = up.URLs[:1] // no ws for this interface
				step = pAddons        // not applicable — forward only
				continue
			}
			fmt.Println(ui.Subsection(subLabel(pWSAsk, l, url), "websocket"))
			fmt.Println("    " + ui.Hint.Render("subscriptions (eth_subscribe / tm /websocket) need a paired wss:// url."))
			var nav Nav
			addWs, nav = confirmNav("      add a paired websocket (wss://) url?", true)
			if nav == Back {
				step = prevStep(pWSAsk, l, url)
				continue
			}
			if addWs {
				step = pWSURL
			} else {
				up.URLs = up.URLs[:1] // drop any previously-added ws
				step = pAddons
			}

		case pWSURL:
			ws := health.WSDefault(url)
			if RunForm(huh.NewGroup(huh.NewInput().Title("      ws url").
				Placeholder(ws).Value(&ws))) == Back {
				step = pWSAsk
				continue
			}
			up.URLs = up.URLs[:1]
			ws = strings.TrimSpace(ws)
			if ws == "" {
				// blank = the user changed their mind; treat as "no ws".
				addWs = false
				step = pAddons
				continue
			}
			// Verify the ws url before keeping it — same proof-of-life the http
			// gate applies. The probe pairs the ws url with the http base on ONE
			// upstream (a ws-only provider can't be probed — the router always needs
			// the base collection), so it's a single round-trip that doesn't trip
			// lava.build's per-connection ws burst cap. On failure, let the user keep
			// it anyway (Esc=keep).
			if !s.gateWS(l, url, ws) {
				keep, nav := confirmNav("      keep this ws url anyway?", false)
				if nav == Back {
					step = pWSAsk
					continue
				}
				if !keep {
					step = pWSURL // re-enter a different ws url
					continue
				}
			}
			up.URLs = append(up.URLs, ws)
			addWs = true
			step = pAddons

		case pAddons:
			if len(l.Chain.Addons) == 0 {
				step = pDone // not applicable — done with this interface
				continue
			}
			fmt.Println(ui.Subsection(subLabel(pAddons, l, url), "addons / extensions"))
			addons, nav := s.collectAddons(l, url, addWs, up.Auth)
			if nav == Back {
				step = prevStep(pAddons, l, url)
				continue
			}
			up.Addons = addons
			step = pDone
		}
	}

	// Naming + commit happen once per round in collectForChain, after every
	// interface is collected — not here.
	return up, Next
}

// probeLive runs an inline health probe wrapped in a spinner.
func (s *State) probeLive(url, chain, iface string, auth *emit.Auth) (ok bool, detail string) {
	if s.Prober == nil {
		return false, "health binary unavailable"
	}
	var env *health.Envelope
	_ = spinner.New().
		Title(fmt.Sprintf("  checking %s via smartrouter health…", chain)).
		Action(func() { env, _ = s.Prober.ProbeInline(url, chain, iface) }).
		Run()
	return env.Live()
}

// gateWS verifies a paired ws url the same way probeLive verifies the http url,
// so a dead/misrouted ws url is caught here rather than silently failing
// subscriptions at runtime. It probes a throwaway config that pairs the ws url
// with its http base on ONE upstream — NOT the ws url alone: a ws-only provider
// can't be probed, because the chain router always requires the base (no-extension)
// collection, which only an http(s) url serves (see health_cmd.go → "ws-only
// endpoint cannot be probed alone"). Pairing reproduces the real emitted config,
// so health verifies the exact shape that boots. The probe carries no addons (ws
// is chosen before addons in this flow), so no archive ws-widening applies here.
// One config probe is a single short round-trip, so it stays under lava.build's
// per-connection ws burst cap (which only trips on many rapid reqs on one conn —
// that's the router's full STARTUP verification, not this one check). The verdict
// is read from env.WSLive(), which judges the ws node-url row(s) specifically — the
// base row is already gated at pLiveGate, so a base the user force-added despite a
// failed probe can't masquerade as a ws failure here. Prints the verdict; returns
// whether the ws url verified. With no prober, returns true (can't gate).
func (s *State) gateWS(l Listener, base, ws string) bool {
	if s.Prober == nil {
		fmt.Println("  " + ui.Hint.Render("(health unavailable — keeping the ws url unverified)"))
		return true
	}
	cfg := wsProbeConfig(l, base, ws)
	rel, cleanup, err := s.writeProbeConfig(cfg)
	if err != nil {
		fmt.Println("  " + ui.Hint.Render("(could not write probe config — keeping the ws url unverified)"))
		return true
	}
	defer cleanup()
	var env *health.Envelope
	_ = spinner.New().
		Title(fmt.Sprintf("  checking ws %s via smartrouter health…", l.Chain.Index)).
		Action(func() { env, _ = s.Prober.ProbeConfig(rel, false) }).
		Run()
	// Judge the ws row specifically: the base http row is verified at pLiveGate,
	// and a base the user force-added despite a failed probe shouldn't mislabel a
	// healthy ws url as a ws failure.
	ok, detail := env.WSLive()
	if ok {
		fmt.Println("  " + ui.Tick("ws live · "+detail))
		return true
	}
	fmt.Println(ui.Alert("WEBSOCKET DID NOT VERIFY", ws+"\n"+detail))
	return false
}

// wsProbeConfig builds the throwaway base+ws config gateWS probes: one upstream
// carrying the http base url first, then the ws url — the real boot shape for a
// subscription upstream. No addons (ws is verified before addons are chosen), so
// no archive ws-widening / skip-verifications applies at this step.
func wsProbeConfig(l Listener, base, ws string) *emit.Config {
	return &emit.Config{
		Metrics:   "disabled",
		Listeners: []emit.Listener{{ChainID: l.Chain.Index, Iface: l.Iface, Port: 3360}},
		Primary: []emit.Upstream{{
			Name: "probe", ChainID: l.Chain.Index, Iface: l.Iface,
			URLs: []string{base, ws},
		}},
	}
}

// collectAddons offers auto-detect (health) / explicit / none. Esc on any of
// its forms is swallowed (returns the running selection) — back-out happens at
// the URL prompt, which is the listener's restart point. addWs reflects the ws
// choice made in the preceding step, so the addon probe verifies against the same
// transports the real config will declare (see probeOneAddon).
func (s *State) collectAddons(l Listener, url string, addWs bool, auth *emit.Auth) ([]string, Nav) {
	mode := "auto"
	// No Title — the "addons / extensions" subsection header above this form already
	// labels the block; a second "How should addons be set?" heading just stacks.
	if RunForm(huh.NewGroup(
		huh.NewSelect[string]().
			Options(
				huh.NewOption("auto-detect", "auto"),
				huh.NewOption("choose explicitly", "explicit"),
				huh.NewOption("none", "none"),
			).Value(&mode),
	)) == Back {
		return nil, Back
	}

	switch mode {
	case "auto":
		return s.detectAddons(l, url, addWs), Next
	case "explicit":
		opts := make([]huh.Option[string], 0, len(l.Chain.Addons))
		for _, a := range l.Chain.Addons {
			opts = append(opts, huh.NewOption(a, a))
		}
		var picked []string
		if RunForm(huh.NewGroup(
			huh.NewMultiSelect[string]().
				Title("Select the addons this endpoint serves").
				Description("space toggles each · enter confirms · esc goes back").
				Options(opts...).
				Value(&picked),
		)) == Back {
			return nil, Back
		}
		return s.verifyExplicitAddons(l, url, addWs, picked), Next
	}
	return nil, Next
}

// verifyExplicitAddons live-checks each addon the user PICKED (same probe the
// auto-detector uses) and shows a per-addon verdict, so an explicit choice gets
// the same proof-of-support as auto-detect. Unlike auto-detect — which only
// returns what verified — an explicit pick is a deliberate intent, so a pick
// that doesn't verify is NOT silently dropped: the user is shown the failure and
// asked whether to keep it anyway (mirrors the pLiveGate "add it anyway?" gate).
// Esc on that prompt is treated as "keep" (the safe, intent-preserving default;
// back-out happens at the URL prompt, this flow's restart point).
func (s *State) verifyExplicitAddons(l Listener, url string, addWs bool, picked []string) []string {
	if len(picked) == 0 {
		return picked
	}
	if s.Prober == nil {
		// No probe available — honour the picks unverified, but say so.
		fmt.Println("  " + ui.Hint.Render("(health unavailable — keeping your selection unverified)"))
		return picked
	}

	fmt.Println("  " + ui.Hint.Render("verifying your selection: "+strings.Join(picked, ", ")))
	supported := map[string]bool{}
	_ = spinner.New().
		Title(fmt.Sprintf("      checking %d selected addon/extension(s) on %s…", len(picked), l.Chain.Index)).
		Action(func() {
			for _, a := range picked {
				if s.probeOneAddon(l, url, a) {
					supported[a] = true
				}
			}
		}).Run()

	kept := make([]string, 0, len(picked))
	for _, a := range picked {
		if supported[a] {
			fmt.Println("  " + ui.Tick(a) + ui.Hint.Render(" — verified"))
			if c := archiveWsCaveat(l, addWs, a); c != "" {
				fmt.Println("    " + ui.Hint.Render(c))
			}
			kept = append(kept, a)
			continue
		}
		// Selected but unverified — warn, then let the user decide. Esc (Back)
		// keeps it: the safe, intent-preserving default for a deliberate pick.
		fmt.Println("  " + ui.XMark(a+" — did NOT verify on this endpoint"))
		fmt.Println("    " + ui.Hint.Render(addonFailHint()))
		keep, nav := confirmNav("      keep it in the config anyway?", false)
		if keep || nav == Back {
			kept = append(kept, a)
		}
	}
	return kept
}

// addonFailHint gives a one-line reason an explicitly-chosen addon didn't verify,
// so the warning is actionable rather than opaque. Addons are probed over the http
// base collection (see probeOneAddon — never ws-widened now), so a failure here is
// genuine: the endpoint didn't answer the addon's verification probe.
func addonFailHint() string {
	return "the endpoint didn't answer the addon's verification probe — it may not serve this addon, or auth/the URL may be off."
}

// archiveWsCaveat returns a one-line note shown when `archive` is KEPT alongside a
// paired ws url on a subscription interface. archive verified over the http base
// (where archive-depth reads route), but no public gateway serves the ws-widened
// {archive,websocket} combination over one connection — so the emitter adds
// skip-verifications:[pruning] to boot, and archive over the ws transport (e.g.
// archive-depth subscriptions) is NOT served. Empty string when it doesn't apply.
func archiveWsCaveat(l Listener, addWs bool, addon string) string {
	subscriptionIface := l.Iface == "jsonrpc" || l.Iface == "tendermintrpc"
	if addon == "archive" && subscriptionIface && addWs {
		return "archive verified over the http base (archive-depth reads route there). Public gateways don't serve " +
			"{archive,websocket} over one connection, so it boots via skip-verifications:[pruning] — archive over ws is not served."
	}
	return ""
}

// detectAddons writes a throwaway config declaring the candidate addons on a
// base+addon node-url pair (via emit.Config, which renders the required base
// url alongside the addon one — an addon-only provider would fail router
// construction) and reads back which candidates the health probe confirms.
// addWs reflects whether the user paired a ws url — it no longer changes the probe
// (addons are verified http-only; see probeOneAddon), only whether the archive-over-ws
// caveat is shown next to a confirmed `archive`.
func (s *State) detectAddons(l Listener, url string, addWs bool) []string {
	if s.Prober == nil {
		return nil
	}
	cand := l.Chain.Addons

	// Probe each candidate INDEPENDENTLY — one base+addon config per candidate —
	// rather than declaring all candidates on a single node-url. The chain router
	// rejects the WHOLE provider with "invalid supported to check, is neither an
	// addon or an extension" the moment ONE declared addon isn't valid for this
	// (chain, interface). Batching all candidates together would therefore let a
	// single unsupported addon nuke the probe and report every candidate —
	// including genuinely-supported ones like archive — as "not detected".
	fmt.Println("  " + ui.Hint.Render("probing for: "+strings.Join(cand, ", ")))
	var got []string
	supported := map[string]bool{}
	_ = spinner.New().
		Title(fmt.Sprintf("      detecting %d addon/extension(s) on %s…", len(cand), l.Chain.Index)).
		Action(func() {
			for _, a := range cand {
				if s.probeOneAddon(l, url, a) {
					supported[a] = true
					got = append(got, a)
				}
			}
		}).Run()

	// Show each candidate's result so "detected" is concrete, not opaque.
	for _, a := range cand {
		if supported[a] {
			fmt.Println("  " + ui.Tick(a) + ui.Hint.Render(" — supported"))
			if c := archiveWsCaveat(l, addWs, a); c != "" {
				fmt.Println("    " + ui.Hint.Render(c))
			}
		} else {
			fmt.Println("  " + ui.Hint.Render(ui.Cross+" "+a+" — not detected"))
		}
	}
	if len(got) == 0 {
		fmt.Println("  " + ui.Hint.Render("(none of the candidate addons verified on this endpoint)"))
	}
	return got
}

// probeOneAddon writes a throwaway base+addon config for a SINGLE candidate and
// reports whether the health probe confirms it. Isolating each candidate keeps
// an addon that's invalid for this (chain, interface) from failing the others'
// router construction. A router-construction failure surfaces as zero
// verifications for that addon, so SupportedAddons correctly returns it unmet.
//
// The probe is ALWAYS http-only — it declares NO ws url, even when the user paired
// one. An addon is a property of the ENDPOINT, verified over the http base
// collection; the ws url's own health is proven separately at the ws gate (gateWS).
// This matters for `archive` specifically: on a subscription-tagged spec the router
// widens an archive startup-verification to {archive,websocket} (chain_fetcher.go
// getExtensionsForVerification) IFF a ws url is present, and no public gateway serves
// that combination over one connection. Probing archive WITH the ws url therefore
// reproduced the widening, and the `skip-verifications:[pruning]` the emitter adds to
// survive it ALSO strips the archive verification entirely — leaving the archive row
// with zero verifications, which SupportedAddons reads as "unsupported". That was a
// false negative: the config boots and archive-depth reads route via the base url.
//
// Probing http-only runs the real {archive} verification (no widening, no skip), so
// archive verifies genuinely. The emitted config still pairs archive WITH the ws url
// (and the skip), reflecting the true capability split: archive reads work over the
// http base; archive over ws is not served by public gateways and isn't claimed.
// add_ons (debug/trace) are never ws-widened, so dropping the ws url never affected
// their verdict either. See the live-capability note on emit.archiveNeedsSkipPruning.
func (s *State) probeOneAddon(l Listener, url string, addon string) bool {
	cfg := addonProbeConfig(l, url, addon)
	rel, cleanup, err := s.writeProbeConfig(cfg)
	if err != nil {
		return false
	}
	defer cleanup()
	env, e := s.Prober.ProbeConfig(rel, false)
	if e != nil {
		return false
	}
	return len(env.SupportedAddons([]string{addon})) == 1
}

// addonProbeConfig builds the throwaway config probeOneAddon writes: ONE http-only
// upstream (base url + the single candidate addon), NO ws url. Declaring no ws url
// is deliberate — it keeps the archive verification from being ws-widened to
// {archive,websocket} (which would then be stripped by skip-verifications:[pruning]
// and report archive as unsupported). The real emitted config still pairs the ws
// url; here we only verify the addon over the http base where it actually serves.
func addonProbeConfig(l Listener, url, addon string) *emit.Config {
	return &emit.Config{
		Metrics:   "disabled",
		Listeners: []emit.Listener{{ChainID: l.Chain.Index, Iface: l.Iface, Port: 3360}},
		Primary: []emit.Upstream{{
			Name: "probe", ChainID: l.Chain.Index, Iface: l.Iface,
			URLs: []string{url}, Addons: []string{addon},
		}},
	}
}

func collectAuth(chainIdx string, seq int) (*emit.Auth, Nav) {
	kind := "header"
	if RunForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Auth type").
			Options(huh.NewOption("custom header", "header"), huh.NewOption("query param", "query")).
			Value(&kind),
	)) == Back {
		return nil, Back
	}

	fieldName := "x-api-key"
	if kind == "query" {
		fieldName = "apikey"
	}
	var value string
	nameTitle := "Header name"
	if kind == "query" {
		nameTitle = "Query param name"
	}
	if RunForm(huh.NewGroup(
		huh.NewInput().Title(nameTitle).Placeholder(fieldName).Value(&fieldName),
		huh.NewInput().Title("Value (the API key / token)").
			Description("stored in the gitignored .env, never written into the YAML").
			EchoMode(huh.EchoModePassword).
			Value(&value),
	)) == Back {
		return nil, Back
	}

	varName := fmt.Sprintf("RPC_KEY_%s_%d", sanitize(chainIdx), seq)
	return &emit.Auth{Var: varName, Kind: kind + ":" + fieldName, Value: strings.TrimSpace(value)}, Next
}

func sanitize(s string) string {
	return strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(s))
}

func (s *State) envVars() []string {
	c := &emit.Config{Primary: s.Primary, Backup: s.Backup}
	return c.EnvVars()
}

// confirmNav is a yes/no that also reports Esc (Nav=Back) so callers can bubble
// "go one step back" out of the whole step.
func confirmNav(title string, dflt bool) (bool, Nav) {
	v := dflt
	nav := RunForm(huh.NewGroup(
		huh.NewConfirm().Title(title).Value(&v),
	))
	return v, nav
}

// confirmEmph is a styled confirm that also reports Esc.
func confirmEmph(styledTitle string) (bool, Nav) {
	v := false
	nav := RunForm(huh.NewGroup(
		huh.NewConfirm().Title(styledTitle).Affirmative("Yes").Negative("No").Value(&v),
	))
	return v, nav
}
