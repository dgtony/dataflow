package dataflow

import (
	"errors"
	"fmt"
)

const (
	// mandatory labels
	Input = "input"
	Final = "final"
)

// todo expose some errors (analysis)
var (
//  ErrEmptyStage = errors.New("empty stage")
)

// Constructor for intermediate execution stages.
func NewStage(label string, exec Execution, requires ...string) *Stage {
	return &Stage{label: label, requires: requires, exec: exec}
}

// Stage used for initial argument distribution in the network.
func startStage() *Stage {
	return &Stage{label: Input, exec: func(args ...interface{}) (interface{}, error) { return args[0], nil }}
}

// Stage used for aggregation results from the network.
func finalStage(exec Execution, requires ...string) *Stage {
	return &Stage{label: Final, requires: requires, exec: exec}
}

func NewExecutionGraph(finalInputs []string, finalExec Execution, stages ...*Stage) (*ExecutionGraph, error) {
	var (
		start = startStage()
		final = finalStage(finalExec, finalInputs...)
	)

	// construct graph from stages
	stageMap := make(map[string]*Stage) // fixme use *nodes internally instead of stages (and stages must be just a shallow information)
	for _, stage := range stages {
		if stage == nil {
			return nil, errors.New("empty stage")
		}

		stageMap[stage.label] = stage
	}

	if err := analyze(stageMap); err != nil {
		return nil, err
	}

	// add terminal stages
	stageMap[Input] = start
	stageMap[Final] = final

	// wiring with data pipes
	for _, stage := range stageMap {
		for _, required := range stage.requires {
			pipe := make(chan either, 1)
			stage.in = append(stage.in, pipe)
			stageMap[required].out = append(stageMap[required].out, pipe)
		}
	}

	// graph communication with external world
	in := make(chan either, 1)
	out := make(chan either, 1)
	start.in = []<-chan either{in}
	final.out = []chan<- either{out}

	return &ExecutionGraph{stages: stageMap, in: in, out: out}, nil
}

// Should only be invoked once!
func (g *ExecutionGraph) Run() (TotalExecution, Collapse) {

	// todo what about running multiple networks on the same graph?

	for _, stage := range g.stages {
		go func(s *Stage) { runStage(s) }(stage)
	}

	var totalExec TotalExecution = func(arg interface{}) (interface{}, error) {
		g.in <- either{Value: arg}
		result := <-g.out
		return result.Value, result.Err
	}

	return totalExec, func() { close(g.in) }
}

func runStage(stage *Stage) {
	for {
		var executionErr error

		// wait until all arguments become available
		args := make([]interface{}, len(stage.in))
		for i := 0; i < len(stage.in); i++ {
			arg, ok := <-stage.in[i]
			if !ok {

				// todo remove
				//fmt.Printf("stage %s collapsed\n", stage.label)

				// propagate network collapse
				for _, successor := range stage.out {
					close(successor)
				}

				return
			}

			args[i] = arg.Value
			if arg.Err != nil {
				// fixme maybe collect all distinct errors instead of returning only last seen ??
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

func analyze(stages map[string]*Stage) error {

	// todo
	//  - labels interfering with Input/Final
	//  - loops
	//  - unreachable states
	//  - dangling executions
	//  - recursive (loop as well)

	return nil
}
