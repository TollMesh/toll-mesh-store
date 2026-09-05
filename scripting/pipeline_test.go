package scripting

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestEngine() *Engine {
	e := NewEngine(10, time.Second)
	e.RegisterHandler("echo", func(args map[string]interface{}) (interface{}, error) {
		return args["value"], nil
	})
	e.RegisterHandler("add", func(args map[string]interface{}) (interface{}, error) {
		a, aok := args["a"].(float64)
		b, bok := args["b"].(float64)
		if !aok || !bok {
			return nil, fmt.Errorf("add requires numeric a and b")
		}
		return a + b, nil
	})
	e.RegisterHandler("fail", func(args map[string]interface{}) (interface{}, error) {
		return nil, fmt.Errorf("intentional failure")
	})
	return e
}

func TestExecuteInlineSingleStep(t *testing.T) {
	e := newTestEngine()
	result, err := e.ExecuteInline([]Step{
		{Op: "echo", Args: map[string]interface{}{"value": "hello"}},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.Steps[0].Output != "hello" {
		t.Errorf("expected 'hello', got %v", result.Steps[0].Output)
	}
}

func TestVariablePassingBetweenSteps(t *testing.T) {
	e := newTestEngine()
	result, err := e.ExecuteInline([]Step{
		{Op: "add", Args: map[string]interface{}{"a": 2.0, "b": 3.0}, SaveAs: "sum1"},
		{Op: "add", Args: map[string]interface{}{"a": "$sum1", "b": 10.0}, SaveAs: "sum2"},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.Steps[0].Output != 5.0 {
		t.Errorf("expected first sum 5.0, got %v", result.Steps[0].Output)
	}
	if result.Steps[1].Output != 15.0 {
		t.Errorf("expected second step to use $sum1 and produce 15.0, got %v", result.Steps[1].Output)
	}
}

func TestPipelineStopsOnStepFailure(t *testing.T) {
	e := newTestEngine()
	result, err := e.ExecuteInline([]Step{
		{Op: "echo", Args: map[string]interface{}{"value": "before"}, SaveAs: "s1"},
		{Op: "fail", Args: map[string]interface{}{}},
		{Op: "echo", Args: map[string]interface{}{"value": "should not run"}, SaveAs: "s3"},
	})
	if err == nil {
		t.Fatal("expected error from failing step")
	}
	if len(result.Steps) != 2 {
		t.Fatalf("expected execution to stop after the failing step (2 recorded steps), got %d", len(result.Steps))
	}
}

func TestUnknownOperationRejectedAtRegistration(t *testing.T) {
	e := newTestEngine()
	err := e.RegisterPipeline(&Pipeline{
		Name:  "bad",
		Steps: []Step{{Op: "does-not-exist", Args: map[string]interface{}{}}},
	})
	if err == nil {
		t.Error("expected error registering pipeline with unknown op")
	}
}

func TestRegisterAndExecuteByName(t *testing.T) {
	e := newTestEngine()
	err := e.RegisterPipeline(&Pipeline{
		Name: "greet",
		Steps: []Step{
			{Op: "echo", Args: map[string]interface{}{"value": "hi"}},
		},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	result, err := e.Execute("greet")
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.Steps[0].Output != "hi" {
		t.Errorf("unexpected output: %v", result.Steps[0].Output)
	}

	p, _ := e.GetPipeline("greet")
	if p.Executions != 1 {
		t.Errorf("expected 1 execution recorded, got %d", p.Executions)
	}
}

func TestMaxStepsEnforced(t *testing.T) {
	e := NewEngine(2, time.Second)
	e.RegisterHandler("echo", func(args map[string]interface{}) (interface{}, error) { return nil, nil })

	_, err := e.ExecuteInline([]Step{
		{Op: "echo", Args: map[string]interface{}{}},
		{Op: "echo", Args: map[string]interface{}{}},
		{Op: "echo", Args: map[string]interface{}{}},
	})
	if err == nil {
		t.Error("expected error exceeding max steps")
	}
}

func TestExecutionTimeout(t *testing.T) {
	e := NewEngine(10, 50*time.Millisecond)
	e.RegisterHandler("slow", func(args map[string]interface{}) (interface{}, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	_, err := e.ExecuteInline([]Step{{Op: "slow", Args: map[string]interface{}{}}})
	if err == nil {
		t.Error("expected timeout error")
	}
}

// Pipeline executions must be atomic with respect to each other: concurrent
// Execute calls should never interleave one pipeline's steps with another's.
func TestConcurrentExecutionsAreSerialized(t *testing.T) {
	e := NewEngine(10, time.Second)
	var order []string
	var mu sync.Mutex

	e.RegisterHandler("record", func(args map[string]interface{}) (interface{}, error) {
		id := args["id"].(string)
		mu.Lock()
		order = append(order, id+"-start")
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		order = append(order, id+"-end")
		mu.Unlock()
		return nil, nil
	})

	var wg sync.WaitGroup
	var completed int32
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			e.ExecuteInline([]Step{{Op: "record", Args: map[string]interface{}{"id": id}}})
			atomic.AddInt32(&completed, 1)
		}(fmt.Sprintf("p%d", i))
	}
	wg.Wait()

	if completed != 3 {
		t.Fatalf("expected 3 completed executions, got %d", completed)
	}

	// Verify no interleaving: every "start" must be immediately followed by
	// its own "end" before another "start" appears.
	for i := 0; i < len(order); i += 2 {
		id := order[i][:2]
		if order[i] != id+"-start" || order[i+1] != id+"-end" {
			t.Fatalf("executions interleaved, order was: %v", order)
		}
	}
}

// TestMergeSnapshotAdoptsNewerPeerPipeline is the regression test for
// pipeline gossip replication: MergeSnapshot must adopt a peer's pipeline
// only when it's strictly newer (by Created, then Node), the same
// LWW-register rule as cache -- and must not replace a local pipeline
// with an older or exactly-equal-version peer copy.
func TestMergeSnapshotAdoptsNewerPeerPipeline(t *testing.T) {
	e := newTestEngine()
	if err := e.RegisterPipeline(&Pipeline{
		Name:  "greet",
		Steps: []Step{{Op: "echo", Args: map[string]interface{}{"value": "local"}}},
		Node:  "node-1",
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	local, _ := e.GetPipeline("greet")
	localCreated := local.Created

	// An older peer version must not overwrite the newer local one.
	e.MergeSnapshot([]Pipeline{{
		Name:    "greet",
		Steps:   []Step{{Op: "echo", Args: map[string]interface{}{"value": "stale-peer"}}},
		Node:    "node-2",
		Created: localCreated - 1000,
	}})
	if p, _ := e.GetPipeline("greet"); p.Steps[0].Args["value"] != "local" {
		t.Fatalf("older peer pipeline was incorrectly adopted: %+v", p)
	}

	// A newer peer version must overwrite the local one.
	e.MergeSnapshot([]Pipeline{{
		Name:    "greet",
		Steps:   []Step{{Op: "echo", Args: map[string]interface{}{"value": "newer-peer"}}},
		Node:    "node-2",
		Created: localCreated + 1000,
	}})
	if p, _ := e.GetPipeline("greet"); p.Steps[0].Args["value"] != "newer-peer" {
		t.Fatalf("newer peer pipeline was not adopted: %+v", p)
	}

	// A brand-new pipeline name (not present locally at all) must always
	// be adopted regardless of version.
	e.MergeSnapshot([]Pipeline{{
		Name:    "farewell",
		Steps:   []Step{{Op: "echo", Args: map[string]interface{}{"value": "bye"}}},
		Node:    "node-2",
		Created: 1,
	}})
	if _, err := e.GetPipeline("farewell"); err != nil {
		t.Fatalf("new pipeline from peer was not adopted: %v", err)
	}
}

func TestGetStats(t *testing.T) {
	e := newTestEngine()
	e.RegisterPipeline(&Pipeline{Name: "p1", Steps: []Step{{Op: "echo", Args: map[string]interface{}{"value": "x"}}}})
	e.Execute("p1")

	stats := e.GetStats()
	if stats["registered_pipelines"] != 1 {
		t.Errorf("expected 1 pipeline, got %v", stats["registered_pipelines"])
	}
	if stats["total_executions"] != int64(1) {
		t.Errorf("expected 1 execution, got %v", stats["total_executions"])
	}
}
