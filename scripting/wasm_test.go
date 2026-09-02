package scripting

import (
	"strings"
	"testing"
	"time"
)

func newTestWasmEngine(t *testing.T, timeout time.Duration) *WasmEngine {
	t.Helper()
	e, err := NewWasmEngine("", timeout)
	if err != nil {
		t.Skipf("tinygo not available, skipping WASM tests: %v", err)
	}
	return e
}

const echoScript = `
package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	fmt.Printf("echo: %s\n", scanner.Text())
}
`

func TestCompileAndExecute(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)

	script, err := e.Compile("echo", echoScript)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if script.WasmSize == 0 {
		t.Fatal("expected non-empty compiled WASM module")
	}

	output, err := e.Execute("echo", "hello world")
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if strings.TrimSpace(output) != "echo: hello world" {
		t.Errorf("unexpected output: %q", output)
	}
}

func TestExecuteCanRunMultipleTimesWithoutRecompiling(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	e.Compile("echo", echoScript)

	for i := 0; i < 3; i++ {
		output, err := e.Execute("echo", "run")
		if err != nil {
			t.Fatalf("execute %d failed: %v", i, err)
		}
		if strings.TrimSpace(output) != "echo: run" {
			t.Errorf("run %d: unexpected output %q", i, output)
		}
	}

	script, _ := e.GetScript("echo")
	if script.Executions != 3 {
		t.Errorf("expected 3 executions recorded, got %d", script.Executions)
	}
}

func TestExecuteInline(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	output, err := e.ExecuteInline(echoScript, "inline test")
	if err != nil {
		t.Fatalf("inline execute failed: %v", err)
	}
	if strings.TrimSpace(output) != "echo: inline test" {
		t.Errorf("unexpected output: %q", output)
	}
}

func TestExecuteUnknownScript(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	_, err := e.Execute("does-not-exist", "")
	if err == nil {
		t.Error("expected error for unknown script")
	}
}

func TestCompileErrorOnInvalidGoSource(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	_, err := e.Compile("broken", "this is not valid go source {{{")
	if err == nil {
		t.Error("expected compile error for invalid Go source")
	}
}

// This is the critical safety property: a script that never returns
// control (an infinite loop with no I/O to check context cancellation on)
// must still be forcibly stopped by the engine's timeout, not hang the
// server forever.
func TestInfiniteLoopIsKilledByTimeout(t *testing.T) {
	e := newTestWasmEngine(t, 2*time.Second)

	infiniteLoopScript := `
package main

func main() {
	for {
	}
}
`
	if _, err := e.Compile("infinite", infiniteLoopScript); err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	start := time.Now()
	_, err := e.Execute("infinite", "")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error for infinite loop script, got none")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected a timeout error, got: %v", err)
	}
	// Must return at or shortly after the 2s timeout, not hang indefinitely.
	if elapsed > 5*time.Second {
		t.Errorf("execution took %v, expected it to be killed around the 2s timeout", elapsed)
	}
	t.Logf("infinite loop correctly killed after %v", elapsed)
}

func TestWasmEngineGetStats(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	e.Compile("echo", echoScript)
	e.Execute("echo", "x")

	stats := e.GetStats()
	if stats["registered_scripts"] != 1 {
		t.Errorf("expected 1 script, got %v", stats["registered_scripts"])
	}
	if stats["total_executions"] != int64(1) {
		t.Errorf("expected 1 execution, got %v", stats["total_executions"])
	}
}

func TestDeleteScript(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	e.Compile("echo", echoScript)

	if err := e.DeleteScript("echo"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := e.GetScript("echo"); err == nil {
		t.Error("expected script to be gone after delete")
	}
}
