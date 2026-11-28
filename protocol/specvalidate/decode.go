package specvalidate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// Loaded is one spec as the validator read it: decoded the way the router decodes it, and its
// `errors` section decoded strictly, so a key this router version doesn't know is a finding.
type Loaded struct {
	Spec spectypes.Spec
	File string
	// Strict is the strict decoding's finding, nil when the section is absent or decodes cleanly.
	Strict *Finding
}

// strictSection is ErrorsSection without its UnmarshalJSON. That method never fails, so a section
// the router can't read drops whole and its spec still loads (PRD §5.6), and a decoder hands it the
// bytes without DisallowUnknownFields; here an unknown key has to fail.
type strictSection spectypes.ErrorsSection

// DecodeSectionStrict decodes an `errors` section, refusing a key this router version doesn't know
// (PRD §4.1: "any value the router version doesn't know (schema mismatch)").
func DecodeSectionStrict(raw json.RawMessage) (*spectypes.ErrorsSection, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var section strictSection
	if err := decoder.Decode(&section); err != nil {
		return nil, err
	}
	decoded := spectypes.ErrorsSection(section)
	return &decoded, nil
}

// LoadPath reads every spec in path, a proposal file or a directory of them (its top-level *.json
// files), keyed by index; a later file wins for the same index, as the router's loader does.
func LoadPath(path string) (map[string]Loaded, error) {
	files, err := specFiles(path)
	if err != nil {
		return nil, err
	}
	loaded := map[string]Loaded{}
	for _, file := range files {
		contents, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var proposal struct {
			Proposal struct {
				Specs []json.RawMessage `json:"specs"`
			} `json:"proposal"`
		}
		if err := json.Unmarshal(contents, &proposal); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		for _, raw := range proposal.Proposal.Specs {
			var spec spectypes.Spec
			if err := json.Unmarshal(raw, &spec); err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			entry := Loaded{Spec: spec, File: file}
			var keys struct {
				Errors json.RawMessage `json:"errors"`
			}
			if err := json.Unmarshal(raw, &keys); err == nil && len(keys.Errors) > 0 && string(keys.Errors) != "null" {
				if _, err := DecodeSectionStrict(keys.Errors); err != nil {
					entry.Strict = &Finding{Spec: spec.Index, Rule: "4.1/schema", Reason: ReasonSchemaUnknown, Detail: err.Error()}
				}
			}
			loaded[spec.Index] = entry
		}
	}
	return loaded, nil
}

// Validate checks every loaded spec's `errors` section, the strict decoding first; findings are in
// spec order.
func Validate(loaded map[string]Loaded) []Finding {
	specs := make(map[string]spectypes.Spec, len(loaded))
	indexes := make([]string, 0, len(loaded))
	for index, entry := range loaded {
		specs[index] = entry.Spec
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	var findings []Finding
	for _, index := range indexes {
		entry := loaded[index]
		if entry.Strict != nil {
			findings = append(findings, *entry.Strict)
			continue
		}
		served, ok := ServedInterfaces(specs, index)
		if !ok {
			served = nil
		}
		findings = append(findings, Section(entry.Spec, served)...)
	}
	return findings
}

// specFiles is path itself, or a directory's top-level *.json files: specs live flat at a spec
// repository's root (lava-specs' guards read ^[^/]+\.json$), and what sits below is tooling, such
// as skill fixtures, not specs.
func specFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, filepath.Join(path, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}
