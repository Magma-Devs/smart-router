package ui

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/classify"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/icons"
)

// chainItem is one selectable row.
type chainItem struct {
	ch     catalog.Chain
	family classify.Family
}

// ChainPicker is a family-tabbed, fuzzy-searchable, multi-select list of chains.
// Tab/⇧Tab switch families; "/" focuses the filter; space toggles; enter
// confirms. Selection persists across family/filter changes.
type ChainPicker struct {
	all       []chainItem
	byFamily  map[classify.Family][]chainItem
	families  []classify.Family
	famIdx    int
	filter    textinput.Model
	filtering bool
	cursor    int
	selected  map[string]bool // chain index -> chosen
	visible   []chainItem     // current family ∩ filter
	width     int
	height    int
	source    string
	icons     *icons.Renderer
	done         bool
	cancelled    bool
	changeSource bool
}

// NewChainPicker builds the model from the resolved catalog. renderer may be
// nil (glyph-only).
func NewChainPicker(chains []catalog.Chain, source string, renderer *icons.Renderer) ChainPicker {
	buckets := classify.Classify(chains)
	ti := textinput.New()
	ti.Placeholder = "filter chains…"
	ti.Prompt = "  / "
	ti.PromptStyle = lipgloss.NewStyle().Foreground(Ember2)
	ti.TextStyle = lipgloss.NewStyle().Foreground(Ink)

	p := ChainPicker{
		byFamily: map[classify.Family][]chainItem{},
		filter:   ti,
		selected: map[string]bool{},
		source:   source,
		icons:    renderer,
		height:   16,
		width:    100,
	}
	for _, f := range classify.Order {
		cs := buckets[f]
		sort.Slice(cs, func(i, j int) bool { return cs[i].Index < cs[j].Index })
		for _, c := range cs {
			it := chainItem{ch: c, family: f}
			p.all = append(p.all, it)
			p.byFamily[f] = append(p.byFamily[f], it)
		}
		if len(cs) > 0 {
			p.families = append(p.families, f)
		}
	}
	p.recompute()
	return p
}

func (p ChainPicker) Init() tea.Cmd { return textinput.Blink }

func (p *ChainPicker) recompute() {
	fam := p.families[p.famIdx]
	q := strings.ToLower(strings.TrimSpace(p.filter.Value()))
	p.visible = p.visible[:0]
	for _, it := range p.byFamily[fam] {
		if q == "" || fuzzyMatch(q, it) {
			p.visible = append(p.visible, it)
		}
	}
	if p.cursor >= len(p.visible) {
		p.cursor = max(0, len(p.visible)-1)
	}
}

func fuzzyMatch(q string, it chainItem) bool {
	hay := strings.ToLower(it.ch.Index + " " + it.ch.Name + " " + strings.Join(it.ch.Addons, " "))
	return strings.Contains(hay, q) || subsequence(q, strings.ToLower(it.ch.Index))
}

// subsequence reports whether q's chars appear in order within s (loose fuzzy).
func subsequence(q, s string) bool {
	i := 0
	for j := 0; j < len(s) && i < len(q); j++ {
		if s[j] == q[i] {
			i++
		}
	}
	return i == len(q)
}

func (p ChainPicker) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width, p.height = msg.Width, min(msg.Height-12, 18)
		if p.height < 6 {
			p.height = 6
		}
	case tea.KeyMsg:
		if p.filtering {
			switch msg.String() {
			case "enter", "esc":
				p.filtering = false
				p.filter.Blur()
			default:
				var cmd tea.Cmd
				p.filter, cmd = p.filter.Update(msg)
				p.recompute()
				return p, cmd
			}
			return p, nil
		}
		switch msg.String() {
		case "ctrl+c", "q":
			p.cancelled = true
			return p, tea.Quit
		case "s": // change spec source (reloads the catalog)
			p.changeSource = true
			return p, tea.Quit
		case "/":
			p.filtering = true
			p.filter.Focus()
			return p, textinput.Blink
		case "tab", "right", "l":
			p.famIdx = (p.famIdx + 1) % len(p.families)
			p.cursor = 0
			p.recompute()
		case "shift+tab", "left", "h":
			p.famIdx = (p.famIdx - 1 + len(p.families)) % len(p.families)
			p.cursor = 0
			p.recompute()
		case "up", "k":
			if p.cursor > 0 {
				p.cursor--
			}
		case "down", "j":
			if p.cursor < len(p.visible)-1 {
				p.cursor++
			}
		case " ", "x":
			if p.cursor < len(p.visible) {
				idx := p.visible[p.cursor].ch.Index
				p.selected[idx] = !p.selected[idx]
				if !p.selected[idx] {
					delete(p.selected, idx)
				}
			}
		case "a": // toggle all VISIBLE: select all, or deselect all if already all selected
			allSelected := len(p.visible) > 0
			for _, it := range p.visible {
				if !p.selected[it.ch.Index] {
					allSelected = false
					break
				}
			}
			for _, it := range p.visible {
				if allSelected {
					delete(p.selected, it.ch.Index)
				} else {
					p.selected[it.ch.Index] = true
				}
			}
		case "enter":
			if len(p.selected) > 0 {
				p.done = true
				return p, tea.Quit
			}
		}
	}
	return p, nil
}

func (p ChainPicker) View() string {
	if p.done || p.cancelled {
		return ""
	}
	var b strings.Builder

	// Persistent compact logo header (the full banner is on the splash; this
	// keeps the brand on-screen inside the alt-screen TUI).
	b.WriteString("  " + LogoCompact())
	b.WriteString("\n")
	b.WriteString(Section(1, "Supported chains"))
	b.WriteString("  " + Hint.Render(fmt.Sprintf("(%d chains · source: %s)", len(p.all), p.source)))
	b.WriteString("\n\n")

	// Family tabs.
	b.WriteString(p.renderTabs())
	b.WriteString("\n")

	// Filter line (always visible; highlighted when focused).
	if p.filtering {
		b.WriteString(p.filter.View())
	} else if v := p.filter.Value(); v != "" {
		b.WriteString(Hint.Render("  / " + v))
	} else {
		b.WriteString(Hint.Render("  press / to filter"))
	}
	b.WriteString("\n\n")

	// List window (scrolls around the cursor).
	b.WriteString(p.renderList())

	// Footer: counts + key hints.
	b.WriteString("\n")
	count := Accent.Render(fmt.Sprintf("%d selected", len(p.selected)))
	b.WriteString("  " + count + Hint.Render(fmt.Sprintf("  ·  %d shown in %s", len(p.visible), p.families[p.famIdx])))
	b.WriteString("\n  ")
	b.WriteString(KeyHint(
		[2]string{"tab", "family"}, [2]string{"/", "filter"},
		[2]string{"↑/↓", "move"}, [2]string{"space", "toggle"},
		[2]string{"a", "all"}, [2]string{"s", "source"}, [2]string{"enter", "confirm"},
	))
	return b.String()
}

func (p ChainPicker) renderTabs() string {
	var tabs []string
	for i, f := range p.families {
		label := fmt.Sprintf("%s %s", f.Icon(), f)
		n := len(p.byFamily[f])
		sel := countSelectedIn(p.selected, p.byFamily[f])
		if sel > 0 {
			label += lipgloss.NewStyle().Foreground(Good).Render(fmt.Sprintf(" %d", sel))
		} else {
			label += Hint.Render(fmt.Sprintf(" %d", n))
		}
		st := lipgloss.NewStyle().Padding(0, 2).Foreground(Muted)
		if i == p.famIdx {
			st = st.Foreground(PanelBg).Background(Brand).Bold(true)
		}
		tabs = append(tabs, st.Render(label))
	}
	return "  " + lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
}

func (p ChainPicker) renderList() string {
	if len(p.visible) == 0 {
		return "  " + Hint.Render("no chains match — clear the filter (esc) or switch family (tab)")
	}
	// Window the visible slice around the cursor.
	start := 0
	if p.cursor >= p.height {
		start = p.cursor - p.height + 1
	}
	end := min(start+p.height, len(p.visible))

	var rows []string
	for i := start; i < end; i++ {
		it := p.visible[i]
		marker := Hint.Render(" • ")
		if p.selected[it.ch.Index] {
			marker = OK.Render(" "+Check+" ")
		}
		idx := it.ch.Index
		name := it.ch.Name
		meta := Hint.Render(strings.Join(it.ch.Interfaces, ","))
		if len(it.ch.Addons) > 0 {
			meta += "  " + lipgloss.NewStyle().Foreground(Ember3).Render("+"+strings.Join(it.ch.Addons, ","))
		}
		// Per-chain colored Unicode mark (distinct symbol per chain, family
		// glyph fallback). Raster images can't survive bubbletea's line-diff,
		// so this is the richest icon that works in any terminal.
		icon := ChainMark(it.ch)
		line := fmt.Sprintf("%s%-12s %-26s %s", icon, idx, name, meta)
		rowStyle := lipgloss.NewStyle().Foreground(Ink)
		if i == p.cursor {
			rowStyle = lipgloss.NewStyle().Foreground(Brand).Bold(true)
			marker = Accent.Render(" ▸ ")
			if p.selected[it.ch.Index] {
				marker = OK.Render(" "+Check+" ")
			}
		}
		rows = append(rows, marker+rowStyle.Render(line))
	}
	// Scroll affordance.
	if end < len(p.visible) {
		rows = append(rows, "  "+Hint.Render(fmt.Sprintf("↓ %d more", len(p.visible)-end)))
	}
	return strings.Join(rows, "\n")
}

func countSelectedIn(sel map[string]bool, items []chainItem) int {
	n := 0
	for _, it := range items {
		if sel[it.ch.Index] {
			n++
		}
	}
	return n
}

// Result returns the chosen chains (after the model quits via enter).
func (p ChainPicker) Result() []catalog.Chain {
	var out []catalog.Chain
	for _, it := range p.all {
		if p.selected[it.ch.Index] {
			out = append(out, it.ch)
		}
	}
	return out
}

// Cancelled reports whether the user quit without confirming.
func (p ChainPicker) Cancelled() bool { return p.cancelled }

// ChangeSource reports whether the user pressed 's' to switch spec source.
func (p ChainPicker) ChangeSource() bool { return p.changeSource }
