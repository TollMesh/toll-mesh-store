// Package scripting provides atomic, composable operation pipelines: an
// alternative to embedding a general-purpose scripting language (Lua, as
// Redis does via EVAL). Instead of an interpreter that then has to be bound
// back into the store's Go types, a Pipeline is an ordered list of Steps,
// each naming one existing store operation (the same ones exposed over
// HTTP, e.g. "zadd", "get", "enqueue") plus its arguments. A step can save
// its result under a name and later steps can reference it as "$name",
// letting several store operations run as a single atomic unit without any
// interpreter, sandboxing, or arbitrary-code-execution surface.
package scripting

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Handler executes one operation kind (e.g. "zadd") given its arguments.
type Handler func(args map[string]interface{}) (interface{}, error)

// Step is a single operation within a Pipeline.
type Step struct {
	Op     string                 `json:"op"`
	Args   map[string]interface{} `json:"args"`
	SaveAs string                 `json:"save_as,omitempty"`
}

// Pipeline is a named, ordered sequence of Steps. Created doubles as this
// pipeline's LWW-register version for gossip replication (RegisterPipeline
// re-stamps it on every registration, not just the first) -- Node is the
// registering node's ID, breaking ties between two nodes registering the
// same pipeline name in the same millisecond, the same pattern as cache's
// merge.
type Pipeline struct {
	Name       string `json:"name"`
	Steps      []Step `json:"steps"`
	Created    int64  `json:"created"`
	Node       string `json:"node,omitempty"`
	Executions int64  `json:"executions"`
	LastError  string `json:"last_error,omitempty"`
}

// StepResult is the outcome of executing a single step.
type StepResult struct {
	Op     string      `json:"op"`
	SaveAs string      `json:"save_as,omitempty"`
	Output interface{} `json:"output,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// ExecutionResult is the outcome of executing a whole pipeline.
type ExecutionResult struct {
	Steps     []StepResult `json:"steps"`
	Error     string       `json:"error,omitempty"`
	Duration  int64        `json:"duration_ms"`
	Timestamp int64        `json:"timestamp"`
}

// Engine registers pipelines and the operation Handlers they can call, and
// executes pipelines against those handlers.
type Engine struct {
	mu          sync.RWMutex
	pipelines   map[string]*Pipeline
	handlers    map[string]Handler
	maxSteps    int
	executionMu sync.Mutex // serializes execution so a pipeline's steps run atomically w.r.t. other pipelines
	execTimeout time.Duration
}

// NewEngine creates a new pipeline execution engine.
func NewEngine(maxSteps int, execTimeout time.Duration) *Engine {
	return &Engine{
		pipelines:   make(map[string]*Pipeline),
		handlers:    make(map[string]Handler),
		maxSteps:    maxSteps,
		execTimeout: execTimeout,
	}
}

// RegisterHandler registers the function that implements a given step
// operation name, e.g. RegisterHandler("zadd", func(args) {...}).
func (e *Engine) RegisterHandler(op string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[op] = h
}

// RegisterPipeline registers a named pipeline for later execution by name.
func (e *Engine) RegisterPipeline(p *Pipeline) error {
	if len(p.Steps) == 0 {
		return fmt.Errorf("pipeline must have at least one step")
	}
	if len(p.Steps) > e.maxSteps {
		return fmt.Errorf("pipeline has %d steps, exceeds max of %d", len(p.Steps), e.maxSteps)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, step := range p.Steps {
		if _, exists := e.handlers[step.Op]; !exists {
			return fmt.Errorf("unknown operation: %s", step.Op)
		}
	}

	p.Created = time.Now().UnixMilli()
	e.pipelines[p.Name] = p
	return nil
}

// GetPipeline retrieves a registered pipeline by name.
func (e *Engine) GetPipeline(name string) (*Pipeline, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, exists := e.pipelines[name]
	if !exists {
		return nil, fmt.Errorf("pipeline not found: %s", name)
	}
	return p, nil
}

// ListPipelines returns all registered pipelines.
func (e *Engine) ListPipelines() []*Pipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	pipelines := make([]*Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipelines = append(pipelines, p)
	}
	return pipelines
}

// Snapshot returns a copy of every registered pipeline, for gossip
// replication.
func (e *Engine) Snapshot() []Pipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		out = append(out, *p)
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: a (Created, Node)
// LWW-register comparison per pipeline name, the same pattern as cache's
// merge -- the peer's pipeline is adopted only when it's strictly newer.
//
// Known limitation, not solved here: DeletePipeline is a hard local
// delete with no tombstone, unlike cache (no delete at all) or Sorted
// Sets (tombstoned deletes that do replicate). A pipeline deleted on one
// node will be silently re-introduced by the next gossip round from any
// peer that still has it -- deletion does not replicate, only
// registration/update does.
func (e *Engine) MergeSnapshot(pipelines []Pipeline) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range pipelines {
		peer := &pipelines[i]

		local, exists := e.pipelines[peer.Name]
		if exists && !pipelineLess(local.Created, local.Node, peer.Created, peer.Node) {
			continue
		}

		// Every node registers the same fixed set of handlers at
		// startup (see MeshStore.registerPipelineHandlers), so this
		// should always pass in practice -- guarding it anyway rather
		// than installing a pipeline this node has no way to execute.
		valid := true
		for _, step := range peer.Steps {
			if _, ok := e.handlers[step.Op]; !ok {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}

		peerCopy := *peer
		e.pipelines[peer.Name] = &peerCopy
	}
}

// pipelineLess reports whether (createdA, nodeA) sorts strictly before
// (createdB, nodeB) in the pipeline LWW-register's version order.
func pipelineLess(createdA int64, nodeA string, createdB int64, nodeB string) bool {
	if createdA != createdB {
		return createdA < createdB
	}
	return nodeA < nodeB
}

// DeletePipeline removes a registered pipeline.
func (e *Engine) DeletePipeline(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.pipelines[name]; !exists {
		return fmt.Errorf("pipeline not found: %s", name)
	}
	delete(e.pipelines, name)
	return nil
}

// Execute runs a registered pipeline by name.
func (e *Engine) Execute(name string) (*ExecutionResult, error) {
	e.mu.RLock()
	p, exists := e.pipelines[name]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("pipeline not found: %s", name)
	}

	result, err := e.run(p.Steps)

	e.mu.Lock()
	p.Executions++
	if err != nil {
		p.LastError = err.Error()
	}
	e.mu.Unlock()

	return result, err
}

// ExecuteInline runs an ad-hoc list of steps without registering them.
func (e *Engine) ExecuteInline(steps []Step) (*ExecutionResult, error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("pipeline must have at least one step")
	}
	if len(steps) > e.maxSteps {
		return nil, fmt.Errorf("pipeline has %d steps, exceeds max of %d", len(steps), e.maxSteps)
	}
	return e.run(steps)
}

// run executes steps in order, atomically with respect to other pipeline
// executions (guarded by executionMu -- concurrent HTTP requests running
// pipelines are serialized against each other, so a pipeline's steps are
// never interleaved with another pipeline's steps). It does not serialize
// against operations issued outside the scripting engine (e.g. a plain
// POST /zset/add) -- those go straight to the store's own locking.
func (e *Engine) run(steps []Step) (*ExecutionResult, error) {
	e.executionMu.Lock()
	defer e.executionMu.Unlock()

	start := time.Now()
	result := &ExecutionResult{
		Steps:     make([]StepResult, 0, len(steps)),
		Timestamp: start.UnixMilli(),
	}

	saved := make(map[string]interface{})
	done := make(chan error, 1)

	go func() {
		for _, step := range steps {
			e.mu.RLock()
			handler, exists := e.handlers[step.Op]
			e.mu.RUnlock()

			if !exists {
				err := fmt.Errorf("unknown operation: %s", step.Op)
				result.Steps = append(result.Steps, StepResult{Op: step.Op, Error: err.Error()})
				done <- err
				return
			}

			resolvedArgs := resolveArgs(step.Args, saved)

			output, err := handler(resolvedArgs)
			stepResult := StepResult{Op: step.Op, SaveAs: step.SaveAs, Output: output}
			if err != nil {
				stepResult.Error = err.Error()
				result.Steps = append(result.Steps, stepResult)
				done <- fmt.Errorf("step %q failed: %w", step.Op, err)
				return
			}

			result.Steps = append(result.Steps, stepResult)
			if step.SaveAs != "" {
				saved[step.SaveAs] = output
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		result.Duration = time.Since(start).Milliseconds()
		if err != nil {
			result.Error = err.Error()
			return result, err
		}
		return result, nil
	case <-time.After(e.execTimeout):
		result.Duration = time.Since(start).Milliseconds()
		result.Error = "execution timeout"
		return result, fmt.Errorf("pipeline execution timeout")
	}
}

// resolveArgs replaces any string argument value of the form "$name" with
// the previously saved result under that name. Non-string values, and
// strings not starting with "$", pass through unchanged.
func resolveArgs(args map[string]interface{}, saved map[string]interface{}) map[string]interface{} {
	resolved := make(map[string]interface{}, len(args))
	for k, v := range args {
		if s, ok := v.(string); ok && strings.HasPrefix(s, "$") {
			name := s[1:]
			if val, exists := saved[name]; exists {
				resolved[k] = val
				continue
			}
		}
		resolved[k] = v
	}
	return resolved
}

// GetStats returns pipeline engine statistics.
func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	totalExecutions := int64(0)
	for _, p := range e.pipelines {
		totalExecutions += p.Executions
	}

	return map[string]interface{}{
		"registered_pipelines": len(e.pipelines),
		"registered_handlers":  len(e.handlers),
		"total_executions":     totalExecutions,
		"max_steps":            e.maxSteps,
		"execution_timeout":    e.execTimeout.String(),
	}
}
