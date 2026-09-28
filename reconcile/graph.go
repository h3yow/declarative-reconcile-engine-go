package reconcile

import "sort"

// graph is an immutable dependency adjacency structure:
// edges[id] lists the resources that id depends on (its prerequisites).
// All mutating operations return a new graph so declared-state snapshots
// can be held without locking.
type graph struct {
	edges map[ResourceID][]ResourceID
}

func newGraph() *graph {
	return &graph{edges: map[ResourceID][]ResourceID{}}
}

func (g *graph) clone() *graph {
	if g == nil {
		return newGraph()
	}
	ng := &graph{edges: make(map[ResourceID][]ResourceID, len(g.edges))}
	for k, v := range g.edges {
		ng.edges[k] = append([]ResourceID(nil), v...)
	}
	return ng
}

// withEdges returns a copy of g where id maps to a deduped, sorted deps.
func (g *graph) withEdges(id ResourceID, deps []ResourceID) *graph {
	ng := g.clone()
	set := make(map[ResourceID]struct{}, len(deps))
	for _, d := range deps {
		if d != id { // a self edge is nonsensical and is a trivial cycle
			set[d] = struct{}{}
		}
	}
	out := make([]ResourceID, 0, len(set))
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	ng.edges[id] = out
	return ng
}

// retainVertex keeps a deleted declaration's vertex intact in the graph,
// including both incoming edges (from still-declared dependents, used to
// block premature prerequisite deletion) and outgoing edges (to its own
// prerequisites, used for deletion cascade once it is physically removed).
// Edges are finally purged only when the referencing vertices are deleted.
func (g *graph) retainVertex(id ResourceID) *graph {
	ng := g.clone()
	if _, ok := ng.edges[id]; !ok {
		ng.edges[id] = nil
	}
	return ng
}

// hasCycle reports whether the directed graph contains any cycle using the
// classic three-color DFS. live is the set of currently declared vertices;
// dangling vertices left behind by deletes are ignored for cycle purposes.
func (g *graph) hasCycle(live map[ResourceID]bool) bool {
	if live == nil {
		live = map[ResourceID]bool{}
		for v := range g.edges {
			live[v] = true
		}
	}
	const white, gray, black = 0, 1, 2
	color := make(map[ResourceID]int, len(g.edges))
	var visit func(node ResourceID) bool
	visit = func(node ResourceID) bool {
		switch color[node] {
		case gray:
			return true
		case black:
			return false
		}
		color[node] = gray
		for _, dep := range g.edges[node] {
			if live[dep] && visit(dep) {
				return true
			}
		}
		color[node] = black
		return false
	}
	vertices := make([]ResourceID, 0, len(g.edges))
	for v := range g.edges {
		if live[v] {
			vertices = append(vertices, v)
		}
	}
	sort.Strings(vertices)
	for _, v := range vertices {
		if color[v] == white && visit(v) {
			return true
		}
	}
	return false
}

// cycleContaining returns the cycle path involving start, or nil, for logs.
func (g *graph) cycleContaining(start ResourceID) []ResourceID {
	var stack []ResourceID
	onStack := map[ResourceID]bool{}
	var dfs func(node ResourceID) []ResourceID
	dfs = func(node ResourceID) []ResourceID {
		onStack[node] = true
		stack = append(stack, node)
		for _, dep := range g.edges[node] {
			if dep == start && onStack[start] {
				return append(append([]ResourceID(nil), stack...), start)
			}
			if !onStack[dep] {
				if p := dfs(dep); p != nil {
					return p
				}
			}
		}
		stack = stack[:len(stack)-1]
		onStack[node] = false
		return nil
	}
	return dfs(start)
}

// dependents returns the sorted set of resources that directly depend on id.
func (g *graph) dependents(id ResourceID) []ResourceID {
	var out []ResourceID
	for v, deps := range g.edges {
		for _, d := range deps {
			if d == id {
				out = append(out, v)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// transitiveDependents returns every resource that transitively depends on id.
func (g *graph) transitiveDependents(id ResourceID) []ResourceID {
	seen := map[ResourceID]bool{}
	frontier := []ResourceID{id}
	for len(frontier) > 0 {
		cur := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		for _, dep := range g.dependents(cur) {
			if !seen[dep] {
				seen[dep] = true
				frontier = append(frontier, dep)
			}
		}
	}
	out := make([]ResourceID, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// reachableMissing returns dependencies (of id, transitively) that are not
// in the known set, sorted and deduped.
func (g *graph) reachableMissing(id ResourceID, known map[ResourceID]bool) []ResourceID {
	seen := map[ResourceID]bool{}
	var walk func(node ResourceID)
	walk = func(node ResourceID) {
		for _, dep := range g.edges[node] {
			if !seen[dep] {
				seen[dep] = true
				walk(dep)
			}
		}
	}
	walk(id)
	var out []ResourceID
	for v := range seen {
		if !known[v] {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// prerequisites returns the sorted direct prerequisites of id.
func (g *graph) prerequisites(id ResourceID) []ResourceID {
	out := append([]ResourceID(nil), g.edges[id]...)
	sort.Strings(out)
	return out
}
