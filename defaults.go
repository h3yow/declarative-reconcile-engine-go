package dre

// Defaulting rules applied deterministically at admission time, BEFORE the
// version/conflict check, so repeated identical submissions converge.
var specDefaults = map[string]string{
	// Example domain rule: an unspecified replicas field means a single unit.
	"replicas": "1",
}

const defaultKind = "generic"

// ApplyDefaults fills deterministic defaults into a submitted declaration.
// It never overwrites an explicitly provided value. Defaulting is the FIRST
// admission step: idempotent submissions therefore carry the same bytes and
// produce no additional changes.
func ApplyDefaults(d *Desired) {
	if d.Kind == "" {
		d.Kind = defaultKind
	}
	d.Spec = EffectiveSpec(d.Spec)
}

// EffectiveSpec returns a copy of spec with defaults materialized.
// An explicitly provided empty string is a real value and is preserved.
func EffectiveSpec(spec Spec) Spec {
	out := make(Spec, len(specDefaults)+len(spec))
	for k, v := range specDefaults {
		out[k] = v
	}
	for k, v := range spec {
		out[k] = v
	}
	return out
}
