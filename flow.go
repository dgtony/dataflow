package dataflow

import (
	"fmt"
)

const (
	// terminal labels
	Input = "input"
	Final = "final"
)

// todo expose some errors (analysis)
var (
//  ErrEmptyStage = errors.New("empty stage")
)

// Constructor for intermediate execution stages.
func NewStage(label string, exec StageExecution, requires ...string) Stage {
	return Stage{label: label, exec: exec, requires: requires}
}

// Stage used for initial argument distribution in the network.
func startStage() Stage {
	return Stage{label: Input, exec: func(args ...interface{}) (interface{}, error) { return args[0], nil }}
}

// Stage used for aggregation results from the network.
func finalStage(exec StageExecution, requires ...string) Stage {
	return Stage{label: Final, exec: exec, requires: requires}
}

func NewExecutionGraph(finalInputs []string, finalExec StageExecution, stages ...Stage) (*ExecutionGraph, error) {
	stageMap := make(map[string]Stage)
	for _, stage := range stages {
		stageMap[stage.label] = stage
	}

	if err := analyze(stageMap); err != nil {
		return nil, err
	}

	// add terminal stages
	stageMap[Input] = startStage()
	stageMap[Final] = finalStage(finalExec, finalInputs...)

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
	stages[Input].in = []<-chan either{in}
	stages[Final].out = []chan<- either{out}

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

				// todo remove
				//fmt.Printf("stage %s collapsed\n", stage.label)

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

func analyze(stages map[string]Stage) error {

	// todo
	//  - labels interfering with Input/Final
	//  - loops
	//  - unreachable states
	//  - dangling executions
	//  - recursive (loop as well)

	return nil
}
