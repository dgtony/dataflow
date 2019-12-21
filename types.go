package dataflow

type (
	// intermediate stage computations
	Execution func(args ...interface{}) (interface{}, error)

	// execution over entire network
	TotalExecution func(arg interface{}) (interface{}, error)

	// shutdown entire execution network
	Collapse func()

	Stage struct {
		label    string
		requires []string
		exec     Execution
		in       []<-chan either
		out      []chan<- either
	}

	ExecutionGraph struct {
		stages map[string]*Stage
		in     chan either
		out    chan either
	}

	either struct {
		Value interface{}
		Err   error
	}

	// fixme need it?
	compoundError []error
)

func (c compoundError) Error() string {
	panic("implement me")
}
