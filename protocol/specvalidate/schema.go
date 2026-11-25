package specvalidate

import _ "embed"

// Schema is the JSON Schema of the `errors` section, schema version 1 (PRD §2): its structure, for
// lava-specs CI's schema job. The rules that need the router's registry are Section's.
//
//go:embed errors.v1.schema.json
var Schema []byte
