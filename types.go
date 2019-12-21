package dataflow

type (
	// intermediate stage computations
	StageExecution func(args ...interface{}) (interface{}, error)

	// execution over entire network
	TotalExecution func(arg interface{}) (interface{}, error)

	// shutdown entire execution network
	Collapse func()

	// execution stage description
	Stage struct {
		label    string
		requires []string
		exec     StageExecution
	}

	// contains checked scheme of execution network flow
	ExecutionGraph struct {
		stages map[string]Stage
	}

	// node of execution network
	node struct {
		label string
		exec  StageExecution
		in    []<-chan either
		out   []chan<- either
	}

	either struct {
		Value interface{}
		Err   error
	}
)
