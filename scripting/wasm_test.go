package scripting

import (
	"strings"
	"testing"
	"time"
)

func newTestWasmEngine(t *testing.T, timeout time.Duration) *WasmEngine {
	t.Helper()
	e, err := NewWasmEngine("", timeout, "node-1")
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

// TestExecuteReusesCompiledModule is a regression test for a bug where
// every Execute call re-decoded and re-compiled the raw WASM bytes from
// scratch (via wazero's InstantiateWithConfig on []byte) instead of reusing
// the CompiledModule cached at Compile time -- silently defeating the
// "compile once, execute many times cheaply" design and making Execute as
// slow as compilation itself. A real Execute call (just instantiating an
// already-compiled module) should complete in well under a second; the
// broken version took seconds, same order of magnitude as Compile.
func TestExecuteReusesCompiledModule(t *testing.T) {
	e := newTestWasmEngine(t, 5*time.Second)
	if _, err := e.Compile("echo", echoScript); err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	start := time.Now()
	if _, err := e.Execute("echo", "run"); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("Execute took %s, expected well under 500ms -- looks like it's recompiling the module instead of reusing the cached CompiledModule", elapsed)
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

// echoScriptV2 behaves observably differently from echoScript (a different
// prefix), so a test can tell which version actually got adopted/executed
// rather than just checking that *a* compile happened.
const echoScriptV2 = `
package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	fmt.Printf("echo-v2: %s\n", scanner.Text())
}
`

// TestMergeSnapshotNeverOverwritesExistingScriptName is the regression
// test for a real security finding: MergeSnapshot must never let a peer's
// gossiped script replace an existing local script under the same name,
// no matter what (Compiled, Node) values the peer claims -- otherwise a
// node holding only the cluster secret (which gates gossip) could hijack
// a script name that was only ever supposed to be registered through the
// API-key-gated Compile path, and a later /script/execute call by a
// legitimate caller would silently run different code than they
// registered. This asserts both a "newer" and a "far-future timestamp"
// peer claim are both rejected outright.
func TestMergeSnapshotNeverOverwritesExistingScriptName(t *testing.T) {
	e := newTestWasmEngine(t, 10*time.Second)
	local, err := e.Compile("echo", echoScript)
	if err != nil {
		t.Fatalf("local compile failed: %v", err)
	}

	attemptsClaimingToBeNewer := []CompiledScript{
		{Name: "echo", Source: echoScriptV2, Compiled: local.Compiled + 1000, Node: "node-2"},
		{Name: "echo", Source: echoScriptV2, Compiled: 99999999999999, Node: "zzz-wins-every-tiebreak"},
	}
	for _, peer := range attemptsClaimingToBeNewer {
		e.MergeSnapshot([]CompiledScript{peer})

		output, err := e.Execute("echo", "test")
		if err != nil {
			t.Fatalf("execute after hijack attempt failed: %v", err)
		}
		if strings.TrimSpace(output) != "echo: test" {
			t.Fatalf("existing script name was hijacked by a gossiped peer version, output = %q", output)
		}

		adopted, err := e.GetScript("echo")
		if err != nil {
			t.Fatalf("GetScript failed: %v", err)
		}
		if adopted.Node == peer.Node {
			t.Fatalf("existing script's Node was overwritten by a peer merge: %+v", adopted)
		}
	}
}

// TestMergeSnapshotCompilesUnknownPeerScript verifies a script registered
// only on a peer (never seen locally) is compiled and registered outright.
func TestMergeSnapshotCompilesUnknownPeerScript(t *testing.T) {
	e := newTestWasmEngine(t, 10*time.Second)

	peer := CompiledScript{
		Name:     "peer-only",
		Source:   echoScript,
		Compiled: time.Now().UnixMilli(),
		Node:     "node-2",
	}
	e.MergeSnapshot([]CompiledScript{peer})

	output, err := e.Execute("peer-only", "hi")
	if err != nil {
		t.Fatalf("execute of merged peer-only script failed: %v", err)
	}
	if strings.TrimSpace(output) != "echo: hi" {
		t.Fatalf("unexpected output from merged script: %q", output)
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
