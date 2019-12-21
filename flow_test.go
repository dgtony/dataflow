package dataflow

import (
	"fmt"
	"testing"
)

func TestComputation(t *testing.T) {
	// Emulated network:
	//
	//            +----------- B:( -1 ) -------------+
	//            |                                  |
	// Input:(x) -+-> C:( +2 ) -> E:( * ) -> Final:( + ) -> end.
	//            |                   |
	//            +----> D:( +5 ) ----+
	//
	stages := []*Stage{
		NewStage("b", func(args ...interface{}) (i interface{}, err error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("unexpected arguments: %v", args)
			}

			a, ok := args[0].(int)
			if !ok {
				return nil, fmt.Errorf("bad argument: %v", args[0])
			}

			return a - 1, nil
		}, Input),

		NewStage("c", func(args ...interface{}) (i interface{}, err error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("unexpected arguments: %v", args)
			}

			a, ok := args[0].(int)
			if !ok {
				return nil, fmt.Errorf("bad argument: %v", args[0])
			}

			return a + 2, nil
		}, Input),

		NewStage("d", func(args ...interface{}) (i interface{}, err error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("unexpected arguments: %v", args)
			}

			a, ok := args[0].(int)
			if !ok {
				return nil, fmt.Errorf("bad argument: %v", args[0])
			}

			return a + 5, nil
		}, Input),

		NewStage("e", func(args ...interface{}) (i interface{}, err error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("unexpected arguments: %v", args)
			}

			c, ok := args[0].(int)
			if !ok {
				return nil, fmt.Errorf("bad first argument: %v", args[0])
			}

			d, ok := args[1].(int)
			if !ok {
				return nil, fmt.Errorf("bad second argument: %v", args[1])
			}

			return c * d, nil
		}, "c", "d"),
	}

	finalInputs := []string{"b", "e"}
	finalExec := func(args ...interface{}) (i interface{}, err error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("unexpected arguments: %v", args)
		}

		b, ok := args[0].(int)
		if !ok {
			return nil, fmt.Errorf("bad first argument: %v", args[0])
		}

		e, ok := args[1].(int)
		if !ok {
			return nil, fmt.Errorf("bad second argument: %v", args[1])
		}

		return b + e, nil
	}

	// construct execution network
	graph, err := NewExecutionGraph(finalInputs, finalExec, stages...)
	if err != nil {
		t.Errorf("constructing execution graph: %v", err)
	}

	exec, collapse := graph.Run()

	tests := []struct {
		name string
		arg  int
		want int
	}{
		{"1", 1, 18},
		{"2", 2, 29},
		{"-4", -4, -7},
		{"0", 0, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec(tt.arg)
			if err != nil {
				t.Errorf("unexpected execution error: %v", err)
			}

			if tt.want != result {
				t.Errorf("unexpected result: want %v, but get %v, ", tt.want, result)
			}
		})
	}

	collapse()
}
