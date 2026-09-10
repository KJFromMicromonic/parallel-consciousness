package protocol_test

import (
	"reflect"
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// Both shapes are real and neither is hypothetical: the in-memory bus passes
// a map[string]string straight through, while pkg/bus/sqlite stores the body
// as JSON and hands back map[string]any. A decoder that handled only one
// would work in unit tests and fail across processes.
func TestVersions(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want map[string]string
	}{
		{"in-memory shape", map[string]string{"billing": "abc"}, map[string]string{"billing": "abc"}},
		{"json shape", map[string]any{"billing": "abc"}, map[string]string{"billing": "abc"}},
		{"json shape skips non-strings", map[string]any{"billing": "abc", "n": 3}, map[string]string{"billing": "abc"}},
		{"absent key", nil, nil},
		{"wrong type entirely", "not a map", nil},
		{"empty json map", map[string]any{}, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := protocol.Versions(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Versions(%#v) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestStrings(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want []string
	}{
		{"in-memory shape", []string{"billing", "gateway"}, []string{"billing", "gateway"}},
		{"json shape", []any{"billing", "gateway"}, []string{"billing", "gateway"}},
		{"json shape skips non-strings", []any{"billing", 7}, []string{"billing"}},
		{"absent key", nil, nil},
		{"wrong type entirely", 42, nil},
		{"empty json list", []any{}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := protocol.Strings(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Strings(%#v) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}
