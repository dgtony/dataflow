package dataflow

import (
	"reflect"
	"sort"
	"testing"
)

func Test_convertStages(t *testing.T) {
	tests := []struct {
		name   string
		stages map[string]Stage
		want   map[string][]string
	}{
		{
			"empty graph",
			map[string]Stage{},
			map[string][]string{},
		},
		{
			"simple tree",
			map[string]Stage{
				"b": {requires: []string{"a"}},
				"c": {requires: []string{"a"}},
				"d": {requires: []string{"b"}},
				"e": {requires: []string{"b"}},
				"f": {requires: []string{"c"}},
			},
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d", "e"},
				"c": {"f"},
			},
		},
		{
			"diamond",
			map[string]Stage{
				"b": {requires: []string{"a"}},
				"c": {requires: []string{"a"}},
				"d": {requires: []string{"b", "c"}},
			},
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d"},
				"c": {"d"},
			},
		},
		{
			"graph with a cycle",
			map[string]Stage{
				"a": {requires: []string{"d"}},
				"b": {requires: []string{"a"}},
				"c": {requires: []string{"a"}},
				"d": {requires: []string{"b"}},
				"e": {requires: []string{"b"}},
				"f": {requires: []string{"d"}},
			},
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d", "e"},
				"d": {"a", "f"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertStages(tt.stages); !equalGraphs(got, tt.want) {
				t.Errorf("convertStages => %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_containsLoop(t *testing.T) {
	tests := []struct {
		name  string
		graph map[string][]string
		want  bool
	}{
		{
			"empty graph",
			map[string][]string{},
			false,
		},
		{
			"simple tree",
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d", "e"},
				"c": {"f"},
			},
			false,
		},
		{
			"graph with a single cycle",
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d", "e"},
				"d": {"a", "f"},
			},
			true,
		},
		{
			"graph with two cycles",
			map[string][]string{
				"a": {"b", "c"},
				"b": {"d", "e"},
				"d": {"f", "a"},
				"f": {"b", "h"},
			},
			true,
		},
		{
			"loop-graph",
			map[string][]string{
				"a": {"b"},
				"b": {"a"},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsLoop(tt.graph); got != tt.want {
				t.Errorf("containsLoop => %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_consistencyCheck(t *testing.T) {
	tests := []struct {
		name      string
		graph     map[string][]string
		expectErr bool
	}{
		{
			"detached input",
			map[string][]string{
				input: {},
				"f1":  {"f2", "f3"},
				"f2":  {final},
				"f3":  {final},
			},
			true,
		},
		{
			"no final stage",
			map[string][]string{
				input: {"f2"},
				"f2":  {"f3"},
				final: {},
			},
			true,
		},
		{
			"dangling execution results",
			map[string][]string{
				input: {"f1", "f2"},
				"f1":  {"f3"},
				"f2":  {"f4", "f5"}, // f4 missing output
				"f3":  {final},
				"f5":  {final},
			},
			true,
		},
		{
			"unreachable stage",
			map[string][]string{
				input: {"f1", "f2"},
				"f1":  {"f3"},
				"f2":  {"f4"},
				"f3":  {final},
				"f4":  {final},
				"f5":  {final}, // unreachable
			},
			true,
		},
		{
			"normal execution",
			map[string][]string{
				input: {"f1", "f2"},
				"f1":  {"f3"},
				"f2":  {"f4", "f5"},
				"f3":  {final},
				"f4":  {final},
				"f5":  {final},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := consistencyCheck(tt.graph); (err != nil) != tt.expectErr {
				t.Errorf("consistencyCheck error => %v, expected %v", err, tt.expectErr)
			}
		})
	}
}

func equalGraphs(m1, m2 map[string][]string) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k1, v1 := range m1 {
		v2, exist := m2[k1]
		if !exist {
			return false
		}

		sort.Strings(v1)
		sort.Strings(v2)

		if !reflect.DeepEqual(v1, v2) {
			return false
		}
	}

	return true
}
