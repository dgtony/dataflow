package dataflow

import (
	"errors"
	"fmt"
)

func analyze(stages map[string]Stage) error {
	if _, found := stages[Input]; !found {
		return errors.New("data input stage not found")
	} else if _, found := stages[sink]; !found {
		return errors.New("final execution stage not found")
	}

	graph := convertStages(stages)

	if containsLoop(graph) {
		return errors.New("execution graph contains loops")
	}

	return consistencyCheck(graph)
}

// Reverse stage representation into adjacency list graph.
func convertStages(stages map[string]Stage) map[string][]string {
	graph := make(map[string][]string, len(stages))

	for node, stage := range stages {
		for _, predecessor := range stage.requires {
			graph[predecessor] = append(graph[predecessor], node)
		}
	}

	return graph
}

// Classical 3-color DFS loop detection algorithm.
func containsLoop(graph map[string][]string) bool {
	type color uint8

	const (
		white = 1 + iota
		gray
		black
	)

	var (
		colors = make(map[string]color, len(graph))
		dfs    func(n string) bool
	)

	dfs = func(n string) bool {
		// start node processing
		colors[n] = gray

		for _, successor := range graph[n] {
			switch colors[successor] {
			case gray:
				return true
			case white:
				if dfs(successor) {
					return true
				}
			}
		}

		// node processing finished
		colors[n] = black
		return false
	}

	// init all nodes as white
	for node := range graph {
		colors[node] = white
	}

	// traverse
	for node := range graph {
		if colors[node] == white {
			if dfs(node) {
				return true
			}
		}
	}

	return false
}

// Ensure execution graph consistency.
func consistencyCheck(graph map[string][]string) error {
	var (
		dfs     func(n string) error
		visited = make(map[string]bool)
	)

	dfs = func(n string) error {
		visited[n] = true

		if len(graph[n]) == 0 && n != sink {
			// dangling execution
			return fmt.Errorf("intermediate stage with no outputs: %s", n)
		} else {
			for _, succ := range graph[n] {
				if err := dfs(succ); err != nil {
					return err
				}
			}
		}

		return nil
	}

	for node, successors := range graph {
		visited[node] = false
		for _, succ := range successors {
			visited[succ] = false
		}
	}

	if err := dfs(Input); err != nil {
		return err
	}

	for stage, vis := range visited {
		if !vis {
			return fmt.Errorf("unreachable stage: %s", stage)
		}
	}

	return nil
}
