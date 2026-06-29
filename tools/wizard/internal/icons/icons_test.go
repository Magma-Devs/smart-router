package icons
import ("os";"testing")
func TestSlugResolve(t *testing.T) {
	m := loadSlugMap()
	if s := m.slugFor("ethereum.json"); s == "" { t.Error("no slug for ethereum.json") } else { t.Logf("ethereum.json -> %s", s) }
	if s := m.slugFor("avalanche_c.json"); s != "" { t.Logf("avalanche_c.json -> %s", s) }
	if s := deriveSlug("foo_bar.json"); s != "foo-bar" { t.Errorf("deriveSlug = %s", s) }
}
func TestRasterize(t *testing.T) {
	if os.Getenv("OFFLINE") != "" { t.Skip("offline") }
	svg, err := fetch(iconURL("ethereum"))
	if err != nil { t.Skip("cannot fetch:", err) }
	png, err := svgToPNG(svg, 40)
	if err != nil { t.Fatal("rasterize:", err) }
	if len(png) < 100 || string(png[1:4]) != "PNG" { t.Fatal("not a valid PNG") }
	t.Logf("ethereum.svg %d bytes -> PNG %d bytes", len(svg), len(png))
}
