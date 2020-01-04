
## Overview

Dataflow is a pretty simple flow network library for parallelizing arbitrary computations.

The basic idea: if there is potentially long-running computation that can be represented as a directed 
non-2-degenerate (i.e. not a line) graph with several stages in its nodes, one can reduce overall execution time
by running independent stages in parallel. And such parallelization can be performed automatically in an optimal way.
This kind of optimization may be especially useful for speeding up complex CPU- or I/O-bound multi-stage computations.


## Usage

Library takes execution plan as a set of stages and its requirements, and constructs and spawns special flow 
network that will automatically parallelize and optimally run given computation in order to reduce total execution 
time as much as possible.
 
It's easier to understand with a specific example. Let's say we need to compute following expression:

`f(x, y) = (x + y) + (x + y) * (x * y) + (x * y) ^ 2.`

Computation process can be represented with a directed graph:

```
               +------+
               | x, y |
               +------+
                  |
                  |
              ( input )
              /       \
             /         \
   ( sum: x + y )     ( prod: x * y )
      |       \                   \
      |        \                   \
      |  ( mult: sum * prod )  ( square: prod ^ 2 )
      |           |               /
       \          |              /
        \         |             /
     ( final: sum + mult + square )
                  |
                  |
              +--------+
              | result |
              +--------+
```

We decomposed entire computation into following stages:

* `sum`:    requires pair of arguments `x` and `y`, returns sum of arguments;
* `prod`:   requires pair of arguments `x` and `y`, returns product of arguments;
* `mult`:   requires results of `sum` and `prod`, returns multiplication of sum and product;
* `square`: requires results of `prod`, returns squared product;
* `final`:  requires results of `sum`, `mult` and `square`, returns computation result.

Let's define intermediate execution stages with **dataflow**:

```go
    stages := []Stage{
    	NewStage("sum", func(args ...interface{}) (interface{}, error) {
    		// there is always single argument received from the input
    		arg := args[0].([2]float64)
    		x, y := arg[0], arg[1]
    		return x + y, nil
    	}, Input),

    	NewStage("prod", func(args ...interface{}) (interface{}, error) {
    		arg := args[0].([2]float64)
    		x, y := arg[0], arg[1]
    		return x * y, nil
    	}, Input),

    	NewStage("mult", func(args ...interface{}) (interface{}, error) {
    		// order of arguments corresponds to provided requirements order!
    		sum, prod := args[0].(float64), args[1].(float64)
    		return sum * prod, nil
    	}, "sum", "prod"),

    	NewStage("square", func(args ...interface{}) (interface{}, error) {
    		prod := args[0].(float64)
    		return prod * prod, nil
    	}, "prod"),
    }
```

Each stage has unique label, parent stages as its inputs, and execution part itself. Final stage is responsible
for collecting results from other stages and combining it into computation result and will be defined separately:

```go
    // on the final stage we wait for all other stages to finish execution, collect
    // intermediate results, combine it and return as a result of entire computation.
    finalStage := NewFinalStage(func(args ...interface{}) (interface{}, error) {
        sum, mult, square := args[0].(float64), args[1].(float64), args[2].(float64)
        return sum + mult + square, nil
    }, "sum", "mult", "square")
```

Now having all the stages we can construct graph of execution:

```go
    graph, err := NewExecutionGraph(finalStage, stages...)
    if err != nil {
        fmt.Printf("error on graph construction: %v\n", err)
    }
```

Library verifies provided stages for consistency, presence of loop etc. and in case of success returns
*execution graph*. Execution graph is just a blueprint for constructing computational flow network.

```go
    execution, collapse := graph.Run()
```

On each invocation of `Run` we spawn a new independent instance of flow network, ready to make computations. Hence if
execution is heavily I/O-bound it's reasonable to run multiple instances and distribute load among them.

So now entire flow network is represented by single function `execution` with a following signature:
`func(arg interface{}) (interface{}, error)`. In our specific case this function takes pair of numbers
(`[2]float64`) as its argument and returns a single number (`float64`) as a result, and we can simply use 
it just if it was an ordinary hand-written function:

```go
    for _, arg := range []struct{ x, y float64 }{
        {0, 0},
        {0, 1},
        {1, 0},
        {1, 1},
    } {
        inputArg := [2]float64{arg.x, arg.y}
        result, err := execution(inputArg)
        if err != nil {
        	fmt.Printf("error on execution (x: %f, y: %f): %v\n", arg.x, arg.y, err)
        } else {
            fmt.Printf("execution result for (x: %f, y: %f): %f\n", arg.x, arg.y, result.(float64))
        }
    }
```

When all the computations are done and flow network is no longer needed we can destroy it and release runtime
resources using callback provided by the library.

```go
  collapse()
```


### Computation failures

Note that signature of stage execution includes error in return position, so it's supposed that any stage computation
may fail. In case of failure occurred during some stage processing, emerged error will be propagated through all the
remaining stages without execution and returned to the caller.


### Performance

Execution overhead depends on total number of stages and is pretty low. Here are benchmarking results for the example
above with 5 stages running on a Macbook Pro 2014:

```
BenchmarkExample-8               	  294891	      3834 ns/op	      56 B/op	       6 allocs/op
```

For our specific example it takes less than 4 microseconds to complete entire processing and return results. Sure
execution overhead looks significant for such a simple task, but it should be negligible for any complex long-running
computation.
