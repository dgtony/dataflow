package dataflow

import (
	"errors"
	"fmt"
)

const (
	// terminal labels
	input = "input"
	final = "final"
)

/* Execution stages */

// Constructor for intermediate execution stages.
func NewStage(label string, exec StageExecution, requires ...string) Stage {
	return Stage{label: label, exec: exec, requires: requires}
}

// Stage used for initial argument distribution in the network.
func startStage() Stage {
	return Stage{label: input, exec: func(args ...interface{}) (interface{}, error) { return args[0], nil }}
}

// Stage used for aggregation results from the network.
func finalStage(exec StageExecution, requires ...string) Stage {
	return Stage{label: final, exec: exec, requires: requires}
}

/* Execution graph */

func NewExecutionGraph(finalInputs []string, finalExec StageExecution, stages ...Stage) (*ExecutionGraph, error) {
	var stageMap = map[string]Stage{
		input: startStage(),
		final: finalStage(finalExec, finalInputs...),
	}

	for _, stage := range stages {
		if stage.label == input {
			return nil, errors.New("label 'input' interfering with internal stage")
		} else if stage.label == final {
			return nil, errors.New("label 'final' interfering with internal stage")
		}

		stageMap[stage.label] = stage
	}

	if err := analyze(stageMap); err != nil {
		return nil, err
	}

	return &ExecutionGraph{stages: stageMap}, nil
}

// On each method invocation a new flow network will be spawned,
// independently processing incoming requests.
func (g ExecutionGraph) Run() (TotalExecution, Collapse) {
	var (
		in     = make(chan either, 1)
		out    = make(chan either, 1)
		stages = make(map[string]*node)
	)

	// construct flow network
	for label, stage := range g.stages {
		stages[label] = &node{label: label, exec: stage.exec}
	}

	// data pipes wiring
	for label, stage := range g.stages {
		n := stages[label]
		for _, required := range stage.requires {
			pipe := make(chan either, 1)
			n.in = append(n.in, pipe)
			stages[required].out = append(stages[required].out, pipe)
		}
	}

	// communication with external world
	stages[input].in = []<-chan either{in}
	stages[final].out = []chan<- either{out}

	// spawn network
	for _, stage := range stages {
		go func(s *node) { runStage(s) }(stage)
	}

	var totalExec TotalExecution = func(arg interface{}) (interface{}, error) {
		in <- either{Value: arg}
		result := <-out
		return result.Value, result.Err
	}

	return totalExec, func() { close(in) }
}

func runStage(stage *node) {
	for {
		var executionErr error

		// wait until all arguments become available
		args := make([]interface{}, len(stage.in))
		for i := 0; i < len(stage.in); i++ {
			arg, ok := <-stage.in[i]
			if !ok {
				// propagate network collapsing
				for _, successor := range stage.out {
					close(successor)
				}

				return
			}

			args[i] = arg.Value
			if arg.Err != nil {
				// currently catches only last significant error
				executionErr = arg.Err
			}
		}

		if executionErr != nil {
			// if error emerged somewhere in the execution path - do not
			//run computation, just propagate error to all successors
			for _, successor := range stage.out {
				successor <- either{Err: executionErr}
			}

		} else {
			// execute stage computation
			val, err := stage.exec(args...)
			if err != nil {
				err = fmt.Errorf("stage %s: %w", stage.label, err)
			}

			result := either{Value: val, Err: err}

			// ... and fan out its result
			for _, successor := range stage.out {
				successor <- result
			}
		}
	}
}
