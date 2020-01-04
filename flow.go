package dataflow

import (
	"fmt"
)

const (
	// terminal labels
	Input = "exec-input" // used externally
	sink  = "exec-sink"
)

/* Execution stages */

// Constructor for intermediate execution stages.
func NewStage(label string, exec StageExecution, requires ...string) Stage {
	return Stage{label: label, exec: exec, requires: requires}
}

// Final stage aggregates results from the network.
func NewFinalStage(exec StageExecution, requires ...string) Stage {
	return Stage{label: sink, exec: exec, requires: requires}
}

// Stage used for initial argument distribution in the network.
func inputStage() Stage {
	return Stage{label: Input, exec: func(args ...interface{}) (interface{}, error) { return args[0], nil }}
}

/* Execution graph */

func NewExecutionGraph(final Stage, stages ...Stage) (*ExecutionGraph, error) {
	var stageMap = map[string]Stage{
		Input: inputStage(),
		sink:  final,
	}

	for _, stage := range stages {
		if stage.label == Input || stage.label == sink {
			return nil, fmt.Errorf("label '%s' interfering with internal stage", stage.label)
		}

		stageMap[stage.label] = stage
	}

	if err := analyze(stageMap); err != nil {
		return nil, err
	}

	return &ExecutionGraph{stages: stageMap}, nil
}

// On each method invocation a new instance of flow network
// will be spawned, to independently process incoming requests.
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
	stages[Input].in = []<-chan either{in}
	stages[sink].out = []chan<- either{out}

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
	var args = make([]interface{}, len(stage.in))

	for {
		var executionErr error

		// wait until all arguments become available
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
				// currently catches last error only
				executionErr = arg.Err
			}
		}

		if executionErr != nil {
			// if error emerged somewhere in the execution path - do not
			// run computation, just propagate error to all successors
			for _, successor := range stage.out {
				successor <- either{Err: executionErr}
			}

			continue
		}

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
