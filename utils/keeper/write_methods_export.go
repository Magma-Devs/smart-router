package keeper

// WriteMethod is one entry of the write-method lock's table, as the spec validator exports it
// (PRD §4.5, MAG-5604): lava-specs' stateful check reads this table instead of its own list.
type WriteMethod struct {
	Interface    string   `json:"interface"`
	Verb         string   `json:"verb,omitempty"`
	InternalPath string   `json:"internal_path,omitempty"`
	Name         string   `json:"name"`
	Specs        []string `json:"specs,omitempty"`
}

// WriteMethods is a copy of the table the write-method lock enforces (writeMethodsTable).
func WriteMethods() []WriteMethod {
	methods := make([]WriteMethod, 0, len(writeMethodsTable))
	for _, method := range writeMethodsTable {
		methods = append(methods, WriteMethod{
			Interface:    method.Interface,
			Verb:         method.Verb,
			InternalPath: method.InternalPath,
			Name:         method.Name,
			Specs:        append([]string(nil), method.Specs...),
		})
	}
	return methods
}
