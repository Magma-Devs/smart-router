package icons

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/srwiley/oksvg"
	"github.com/srwiley/rasterx"
)

// Renderer fetches, rasterizes, and caches chain icons, then emits terminal
// image escape sequences (or a glyph fallback).
type Renderer struct {
	mode   Mode
	slugs  *slugMap
	cells  int // icon height/width in terminal cells
	px     int // raster size in pixels
	cache  map[string]string // specFile -> ready escape sequence ("" = none)
	cacheD string
	mu     sync.Mutex
}

// NewRenderer resolves the terminal mode and the docs slug map. cells is the
// on-screen size (e.g. 2 → a 2-cell square). Safe to call once at startup.
func NewRenderer(cells int) *Renderer {
	if cells <= 0 {
		cells = 2
	}
	r := &Renderer{
		mode:   Detect(),
		slugs:  loadSlugMap(),
		cells:  cells,
		px:     cells * 20, // ~20px per cell at typical font sizes
		cache:  map[string]string{},
		cacheD: filepath.Join(os.TempDir(), "sr-wizard-icons"),
	}
	_ = os.MkdirAll(r.cacheD, 0o755)
	return r
}

// Mode reports the active rendering mode (for status display).
func (r *Renderer) Mode() Mode { return r.mode }

// Glyph returns the family-glyph fallback unconditionally.
func (r *Renderer) Glyph(family string) string { return FamilyGlyph(family) }

// Icon returns a renderable icon for a chain: a terminal image sequence when
// the terminal supports it and the SVG resolves, else the family glyph.
func (r *Renderer) Icon(specFile, family string) string {
	if r.mode == ModeGlyph {
		return FamilyGlyph(family)
	}
	r.mu.Lock()
	if seq, ok := r.cache[specFile]; ok {
		r.mu.Unlock()
		if seq == "" {
			return FamilyGlyph(family)
		}
		return seq
	}
	r.mu.Unlock()

	seq := r.buildSequence(specFile)
	r.mu.Lock()
	r.cache[specFile] = seq
	r.mu.Unlock()
	if seq == "" {
		return FamilyGlyph(family)
	}
	return seq
}

func (r *Renderer) buildSequence(specFile string) string {
	slug := r.slugs.slugFor(specFile)
	if slug == "" {
		return ""
	}
	pngBytes, err := r.rasterizeCached(slug)
	if err != nil || len(pngBytes) == 0 {
		return ""
	}
	switch r.mode {
	case ModeKitty:
		return kittySequence(pngBytes, r.cells)
	case ModeITerm:
		return itermSequence(pngBytes, r.cells)
	case ModeSixel:
		if rgba, err := r.rasterizeRGBA(slug); err == nil && rgba != nil {
			return encodeSixel(rgba)
		}
	}
	return ""
}

// rasterizeRGBA returns the decoded RGBA for a slug (for Sixel encoding).
func (r *Renderer) rasterizeRGBA(slug string) (*image.RGBA, error) {
	svg, err := fetch(iconURL(slug))
	if err != nil {
		return nil, err
	}
	return svgToRGBA(svg, r.px)
}

// rasterizeCached returns PNG bytes for a slug, memoized on disk.
func (r *Renderer) rasterizeCached(slug string) ([]byte, error) {
	cacheFile := filepath.Join(r.cacheD, fmt.Sprintf("%s-%d.png", slug, r.px))
	if b, err := os.ReadFile(cacheFile); err == nil && len(b) > 0 {
		return b, nil
	}
	svg, err := fetch(iconURL(slug))
	if err != nil {
		return nil, err
	}
	pngBytes, err := svgToPNG(svg, r.px)
	if err != nil {
		return nil, err
	}
	_ = os.WriteFile(cacheFile, pngBytes, 0o644)
	return pngBytes, nil
}

func fetch(url string) ([]byte, error) {
	cl := &http.Client{Timeout: 8 * time.Second}
	resp, err := cl.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// svgToRGBA rasterizes SVG bytes to a square px×px RGBA image (transparent bg).
func svgToRGBA(svg []byte, px int) (*image.RGBA, error) {
	icon, err := oksvg.ReadIconStream(bytes.NewReader(svg))
	if err != nil {
		return nil, err
	}
	icon.SetTarget(0, 0, float64(px), float64(px))
	rgba := image.NewRGBA(image.Rect(0, 0, px, px))
	scanner := rasterx.NewScannerGV(px, px, rgba, rgba.Bounds())
	raster := rasterx.NewDasher(px, px, scanner)
	icon.Draw(raster, 1.0)
	return rgba, nil
}

// svgToPNG rasterizes SVG bytes to a square PNG of px×px (transparent bg).
func svgToPNG(svg []byte, px int) ([]byte, error) {
	rgba, err := svgToRGBA(svg, px)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, rgba); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// kittySequence encodes a PNG as a kitty graphics-protocol unicode placeholder.
func kittySequence(pngBytes []byte, cells int) string {
	b64 := base64.StdEncoding.EncodeToString(pngBytes)
	var sb bytes.Buffer
	// f=100 (PNG), a=T (transmit+display), c/r = cell footprint.
	const chunk = 4096
	first := true
	for len(b64) > 0 {
		n := min(chunk, len(b64))
		piece := b64[:n]
		b64 = b64[n:]
		more := 0
		if len(b64) > 0 {
			more = 1
		}
		if first {
			fmt.Fprintf(&sb, "\x1b_Gf=100,a=T,c=%d,r=%d,m=%d;%s\x1b\\", cells, cells, more, piece)
			first = false
		} else {
			fmt.Fprintf(&sb, "\x1b_Gm=%d;%s\x1b\\", more, piece)
		}
	}
	return sb.String()
}

// itermSequence encodes a PNG as an iTerm2 inline-image escape.
func itermSequence(pngBytes []byte, cells int) string {
	b64 := base64.StdEncoding.EncodeToString(pngBytes)
	return fmt.Sprintf("\x1b]1337;File=inline=1;width=%d;height=%d;preserveAspectRatio=1:%s\a",
		cells, cells, b64)
}
