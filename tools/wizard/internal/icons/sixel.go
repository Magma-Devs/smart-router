package icons

import (
	"bytes"
	"fmt"
	"image"
	"sort"
)

type rgb struct{ r, g, b uint8 }

// encodeSixel converts an RGBA image to a Sixel escape sequence. Pure Go: it
// quantizes to a palette (≤256 colors), then emits the sixel bands. Transparent
// pixels are left as background (not painted). Suitable for the small chain
// icons we render (≤ ~40px).
func encodeSixel(img *image.RGBA) string {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	if w == 0 || h == 0 {
		return ""
	}

	// Build a palette by quantizing each pixel to 6 levels/channel (216 cube),
	// tracking only colors actually used. Transparent → skipped.
	palIndex := map[rgb]int{}
	var palette []rgb
	idxAt := make([]int, w*h) // -1 = transparent
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			i := img.PixOffset(b.Min.X+x, b.Min.Y+y)
			a := img.Pix[i+3]
			if a < 128 {
				idxAt[y*w+x] = -1
				continue
			}
			q := rgb{quant6(img.Pix[i]), quant6(img.Pix[i+1]), quant6(img.Pix[i+2])}
			id, ok := palIndex[q]
			if !ok {
				if len(palette) >= 255 {
					id = nearest(palette, q)
				} else {
					id = len(palette)
					palette = append(palette, q)
					palIndex[q] = id
				}
			}
			idxAt[y*w+x] = id
		}
	}
	if len(palette) == 0 {
		return ""
	}

	var sb bytes.Buffer
	sb.WriteString("\x1bP0;1;0q") // DCS sixel intro, ratio 1:1
	// raster attributes
	fmt.Fprintf(&sb, "\"1;1;%d;%d", w, h)
	// palette definitions: #i;2;R;G;B (0..100 scale)
	for i, c := range palette {
		fmt.Fprintf(&sb, "#%d;2;%d;%d;%d", i, to100(c.r), to100(c.g), to100(c.b))
	}

	// Emit in 6-row bands.
	for band := 0; band*6 < h; band++ {
		y0 := band * 6
		// which palette colors appear in this band
		used := map[int]bool{}
		for y := y0; y < y0+6 && y < h; y++ {
			for x := 0; x < w; x++ {
				if id := idxAt[y*w+x]; id >= 0 {
					used[id] = true
				}
			}
		}
		ids := make([]int, 0, len(used))
		for id := range used {
			ids = append(ids, id)
		}
		sort.Ints(ids)

		for k, id := range ids {
			fmt.Fprintf(&sb, "#%d", id)
			for x := 0; x < w; x++ {
				var bits byte
				for row := 0; row < 6; row++ {
					y := y0 + row
					if y < h && idxAt[y*w+x] == id {
						bits |= 1 << row
					}
				}
				sb.WriteByte(0x3f + bits) // sixel char
			}
			if k < len(ids)-1 {
				sb.WriteByte('$') // carriage return (overlay next color on same band)
			}
		}
		sb.WriteByte('-') // next band (newline)
	}
	sb.WriteString("\x1b\\") // ST
	return sb.String()
}

func quant6(v uint8) uint8 {
	// snap to one of 6 levels then back to 0..255 for palette fidelity
	lvl := uint16(v) * 5 / 255
	return uint8(lvl * 255 / 5)
}

func to100(v uint8) int { return int(uint16(v) * 100 / 255) }

func nearest(pal []rgb, c rgb) int {
	best, bestD := 0, 1<<31-1
	for i, p := range pal {
		dr, dg, db := int(p.r)-int(c.r), int(p.g)-int(c.g), int(p.b)-int(c.b)
		d := dr*dr + dg*dg + db*db
		if d < bestD {
			best, bestD = i, d
		}
	}
	return best
}
