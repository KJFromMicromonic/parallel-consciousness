package protocol

// A message body is a map[string]any, and what a given key holds depends on
// which transport delivered it. The in-memory bus passes Go values through
// untouched, so a map[string]string arrives as a map[string]string. The SQLite
// bus stores the body as JSON, so the same value comes back as a
// map[string]any with string elements — and a []string comes back as a []any.
//
// Every consumer of a structured body value therefore has to accept both
// shapes. These two functions are that acceptance, in one place: three
// byte-equivalent copies of this logic previously lived in pkg/gate and
// internal/pcops, which is one drifting copy away from a wire-format bug that
// only appears across processes.
//
// Anything that is not the expected element type is skipped rather than
// guessed at, and an absent or wrongly-typed value yields nil rather than an
// error: a body is data from another process, so a missing field is a normal
// condition for a consumer to handle, not an exceptional one.

// Versions coerces a wire participant→version map back to map[string]string.
func Versions(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, raw := range m {
			if s, ok := raw.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}

// Strings coerces a wire string list back to []string.
func Strings(v any) []string {
	switch l := v.(type) {
	case []string:
		return l
	case []any:
		out := make([]string, 0, len(l))
		for _, raw := range l {
			if s, ok := raw.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
