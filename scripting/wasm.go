// Package scripting (this file) provides real arbitrary-code execution:
// a script is Go source code, compiled by the TinyGo toolchain to a WASI
// WebAssembly module, then run in a sandboxed wazero runtime (pure Go, no
// cgo) with a hard execution timeout. Input is delivered on the module's
// stdin and its result is read from stdout, mirroring Redis's SCRIPT
// LOAD + EVALSHA split: compilation is slow (TinyGo takes real seconds)
// and happens once via Compile; execution is cheap and happens many times
// via Execute.
//
// Execute reuses a single long-lived wazero.Runtime and a per-script
// wazero.CompiledModule cached at Compile time, so repeated Execute calls
// only pay for instantiation (cheap), not for re-decoding and re-compiling
// the WASM bytecode to native code (the expensive step) on every call.
package scripting

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// CompiledScript is a script's Go source and its compiled WASM module.
type CompiledScript struct {
	Name       string `json:"name"`
	Source     string `json:"source"`
	WasmSize   int    `json:"wasm_size"`
	Compiled   int64  `json:"compiled"`
	Executions int64  `json:"executions"`
	LastError  string `json:"last_error,omitempty"`
	// Node is this script's LWW-register tiebreaker for gossip
	// replication, alongside Compiled.
	Node string `json:"node,omitempty"`

	// compiled is the decoded-and-compiled WASM module, cached so Execute
	// only has to instantiate it, not re-decode raw bytes every call.
	compiled wazero.CompiledModule
}

// WasmEngine compiles Go source to WASI WebAssembly via TinyGo and executes
// compiled modules in a shared, sandboxed wazero runtime.
type WasmEngine struct {
	mu          sync.RWMutex
	scripts     map[string]*CompiledScript
	tinygoPath  string
	execTimeout time.Duration
	memoryPages uint32 // wasm memory limit, in 64KiB pages
	nodeID      string // stamped onto CompiledScript.Node on every compile, for gossip LWW

	runtime  wazero.Runtime
	instance uint64 // atomic counter for unique per-call module instance names
}

// NewWasmEngine creates a new engine. tinygoPath is the path to the tinygo
// binary (looked up on PATH if empty). Returns an error if tinygo cannot be
// found, since without it no script can ever be compiled.
func NewWasmEngine(tinygoPath string, execTimeout time.Duration, nodeID string) (*WasmEngine, error) {
	if tinygoPath == "" {
		found, err := exec.LookPath("tinygo")
		if err != nil {
			return nil, fmt.Errorf("tinygo not found on PATH: %w", err)
		}
		tinygoPath = found
	}
	if _, err := os.Stat(tinygoPath); err != nil {
		return nil, fmt.Errorf("tinygo binary not found at %s: %w", tinygoPath, err)
	}

	ctx := context.Background()
	memoryPages := uint32(256) // 16MiB

	// WithCloseOnContextDone makes the interpreter/compiler insert periodic
	// cancellation checks, so a timed-out or explicitly-cancelled call's
	// api.Module instance is force-closed even mid-tight-loop. It closes
	// only the module instance whose call context was cancelled, not this
	// shared Runtime -- confirmed against wazero's source, since otherwise
	// one script's timeout would tear down every other cached CompiledModule
	// and any concurrently running script.
	runtimeConfig := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(memoryPages).
		WithCloseOnContextDone(true)
	runtime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		runtime.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}

	return &WasmEngine{
		scripts:     make(map[string]*CompiledScript),
		tinygoPath:  tinygoPath,
		execTimeout: execTimeout,
		memoryPages: memoryPages,
		nodeID:      nodeID,
		runtime:     runtime,
	}, nil
}

// Close releases the engine's shared runtime and every module compiled
// against it. The engine must not be used afterward.
func (e *WasmEngine) Close() error {
	return e.runtime.Close(context.Background())
}

// Compile compiles Go source to a WASI WASM module via TinyGo, decodes and
// compiles it once against the shared runtime, and registers it under name,
// replacing (and releasing) any existing script with that name. This is the
// slow path (real seconds, since it invokes an external compiler process
// plus wazero's own bytecode-to-native compilation) and is expected to
// happen far less often than Execute.
func (e *WasmEngine) Compile(name, source string) (*CompiledScript, error) {
	wasmBytes, err := e.compileSource(source)
	if err != nil {
		return nil, err
	}

	compiled, err := e.runtime.CompileModule(context.Background(), wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile wasm module: %w", err)
	}

	script := &CompiledScript{
		Name:     name,
		Source:   source,
		WasmSize: len(wasmBytes),
		Compiled: time.Now().UnixMilli(),
		Node:     e.nodeID,
		compiled: compiled,
	}

	e.mu.Lock()
	old, existed := e.scripts[name]
	e.scripts[name] = script
	e.mu.Unlock()

	if existed {
		old.compiled.Close(context.Background())
	}

	return script, nil
}

// compileSource invokes tinygo build in a fresh temp directory and returns
// the resulting WASM bytes.
func (e *WasmEngine) compileSource(source string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "tollmesh-script-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create build dir: %w", err)
	}
	defer os.RemoveAll(dir)

	srcPath := filepath.Join(dir, "main.go")
	if err := os.WriteFile(srcPath, []byte(source), 0644); err != nil {
		return nil, fmt.Errorf("failed to write source: %w", err)
	}

	wasmPath := filepath.Join(dir, "script.wasm")

	// Compilation is CPU-bound and can take real seconds; bound it
	// separately from execution so a pathological script can't hang the
	// build step forever either.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.tinygoPath, "build", "-o", wasmPath, "-target=wasi", srcPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("compilation timed out")
		}
		return nil, fmt.Errorf("compilation failed: %w: %s", err, stderr.String())
	}

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read compiled module: %w", err)
	}

	return wasmBytes, nil
}

// Execute runs a registered script by name, feeding input on stdin and
// returning what it wrote to stdout. Enforces the engine's execution
// timeout regardless of what the script does (including an infinite loop).
// Reuses the script's cached CompiledModule, so this only pays for
// instantiation, not for recompiling the WASM bytecode.
func (e *WasmEngine) Execute(name, input string) (string, error) {
	e.mu.RLock()
	script, exists := e.scripts[name]
	e.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("script not found: %s", name)
	}

	output, err := e.run(script.compiled, input)

	e.mu.Lock()
	script.Executions++
	if err != nil {
		script.LastError = err.Error()
	}
	e.mu.Unlock()

	return output, err
}

// ExecuteInline compiles and immediately runs Go source without
// registering it. Both the (slow) compile and the (bounded) execution
// happen synchronously in this call; the one-shot CompiledModule is
// released once execution finishes.
func (e *WasmEngine) ExecuteInline(source, input string) (string, error) {
	wasmBytes, err := e.compileSource(source)
	if err != nil {
		return "", err
	}

	compiled, err := e.runtime.CompileModule(context.Background(), wasmBytes)
	if err != nil {
		return "", fmt.Errorf("failed to compile wasm module: %w", err)
	}
	defer compiled.Close(context.Background())

	return e.run(compiled, input)
}

// run instantiates an already-compiled WASM module against the engine's
// shared runtime and executes it, enforcing execTimeout via
// WithCloseOnContextDone (configured on the shared runtime at engine
// creation). Each call uses a unique module instance name since wazero
// requires instance names to be unique within a runtime, and the same
// CompiledModule can be instantiated concurrently by overlapping Execute
// calls.
func (e *WasmEngine) run(compiled wazero.CompiledModule, input string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.execTimeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	instanceName := fmt.Sprintf("run-%d", atomic.AddUint64(&e.instance, 1))
	moduleConfig := wazero.NewModuleConfig().
		WithName(instanceName).
		WithStdin(bytes.NewReader([]byte(input))).
		WithStdout(&stdout).
		WithStderr(&stderr)

	mod, err := e.runtime.InstantiateModule(ctx, compiled, moduleConfig)
	if mod != nil {
		defer mod.Close(context.Background())
	}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return stdout.String(), fmt.Errorf("script execution timed out after %s", e.execTimeout)
		}
		// A WASI program's os.Exit(0) surfaces here as a sys.ExitError
		// with code 0 -- that's normal completion, not a failure.
		if exitErr, ok := isCleanExit(err); ok && exitErr {
			return stdout.String(), nil
		}
		return stdout.String(), fmt.Errorf("script execution failed: %w: %s", err, stderr.String())
	}

	return stdout.String(), nil
}

// isCleanExit reports whether err represents a WASI program exiting with
// code 0 via os.Exit(0), which wazero represents as an error type even
// though it's a normal, successful completion.
func isCleanExit(err error) (isExit bool, clean bool) {
	type exitError interface {
		ExitCode() uint32
	}
	if ee, ok := err.(exitError); ok {
		return true, ee.ExitCode() == 0
	}
	return false, false
}

// GetScript retrieves a registered script by name.
func (e *WasmEngine) GetScript(name string) (*CompiledScript, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	script, exists := e.scripts[name]
	if !exists {
		return nil, fmt.Errorf("script not found: %s", name)
	}
	return script, nil
}

// ListScripts returns all registered scripts.
func (e *WasmEngine) ListScripts() []*CompiledScript {
	e.mu.RLock()
	defer e.mu.RUnlock()
	scripts := make([]*CompiledScript, 0, len(e.scripts))
	for _, s := range e.scripts {
		scripts = append(scripts, s)
	}
	return scripts
}

// DeleteScript removes a registered script and releases its compiled
// module.
func (e *WasmEngine) DeleteScript(name string) error {
	e.mu.Lock()
	script, exists := e.scripts[name]
	if !exists {
		e.mu.Unlock()
		return fmt.Errorf("script not found: %s", name)
	}
	delete(e.scripts, name)
	e.mu.Unlock()

	script.compiled.Close(context.Background())
	return nil
}

// Snapshot returns each registered script's source and version metadata
// (not the compiled module -- an unexported field, and not meaningfully
// serializable across nodes anyway), for gossip replication.
func (e *WasmEngine) Snapshot() []CompiledScript {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]CompiledScript, 0, len(e.scripts))
	for _, s := range e.scripts {
		out = append(out, CompiledScript{
			Name:       s.Name,
			Source:     s.Source,
			WasmSize:   s.WasmSize,
			Compiled:   s.Compiled,
			Executions: s.Executions,
			LastError:  s.LastError,
			Node:       s.Node,
		})
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: a (Compiled, Node)
// LWW-register comparison per script name, the same pattern as
// Cache/Pipelines/Search/Job Queues/Transactions -- but, unlike every one
// of those, adopting a peer's version here is NOT a cheap struct swap. A
// script's Source is Go code; there is no compiled module to gossip (it's
// neither serializable in any meaningful cross-machine sense nor safe to
// trust from the wire), so adopting a peer's newer version means actually
// invoking the TinyGo compiler on this node -- the same "real seconds"
// cost Compile's own doc comment already describes. This is deliberate:
// scripts change far less often than, say, cache keys, so paying a
// real compile once per genuine change is acceptable; it would not be if
// this ran on every gossip round for unchanged scripts, which the LWW
// skip-if-not-newer check above prevents.
//
// The double-checked lock (re-verifying under e.mu.Lock after compiling,
// which happens without holding any lock) exists because compilation can
// take longer than one gossip interval: two peers' concurrent merges, or a
// merge racing a local Compile call for the same name, could otherwise
// both decide "the current local version is stale" and race to replace
// each other's result without properly releasing the loser's compiled
// module.
//
// If this node has no WasmEngine at all (TinyGo wasn't found at startup),
// there is nothing to merge into -- see MeshStore.MergeState's nil check.
// A cluster mixing nodes with and without TinyGo will simply never
// converge WASM scripts onto the nodes lacking it.
func (e *WasmEngine) MergeSnapshot(peerScripts []CompiledScript) {
	for i := range peerScripts {
		peer := &peerScripts[i]

		e.mu.RLock()
		local, exists := e.scripts[peer.Name]
		e.mu.RUnlock()

		if exists && !scriptLess(local.Compiled, local.Node, peer.Compiled, peer.Node) {
			continue
		}

		wasmBytes, err := e.compileSource(peer.Source)
		if err != nil {
			// The peer's source failed to compile on this node (e.g. a
			// TinyGo version mismatch across the cluster) -- nothing
			// sensible to adopt, leave the local version as-is.
			continue
		}
		compiled, err := e.runtime.CompileModule(context.Background(), wasmBytes)
		if err != nil {
			continue
		}

		newScript := &CompiledScript{
			Name:     peer.Name,
			Source:   peer.Source,
			WasmSize: len(wasmBytes),
			Compiled: peer.Compiled,
			Node:     peer.Node,
			compiled: compiled,
		}

		e.mu.Lock()
		old, existed := e.scripts[peer.Name]
		if existed && !scriptLess(old.Compiled, old.Node, peer.Compiled, peer.Node) {
			// Someone else (a concurrent merge, or a local Compile call)
			// already adopted an equal-or-newer version while we were
			// compiling. Discard our redundant compile.
			e.mu.Unlock()
			compiled.Close(context.Background())
			continue
		}
		e.scripts[peer.Name] = newScript
		e.mu.Unlock()

		if existed {
			old.compiled.Close(context.Background())
		}
	}
}

// scriptLess reports whether (tsA, nodeA) sorts strictly before (tsB,
// nodeB) in the script LWW-register's version order.
func scriptLess(tsA int64, nodeA string, tsB int64, nodeB string) bool {
	if tsA != tsB {
		return tsA < tsB
	}
	return nodeA < nodeB
}

// GetStats returns engine statistics.
func (e *WasmEngine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var totalExecutions int64
	var totalWasmBytes int
	for _, s := range e.scripts {
		totalExecutions += s.Executions
		totalWasmBytes += s.WasmSize
	}

	return map[string]interface{}{
		"registered_scripts": len(e.scripts),
		"total_executions":   totalExecutions,
		"total_wasm_bytes":   totalWasmBytes,
		"exec_timeout":       e.execTimeout.String(),
		"memory_limit_bytes": int(e.memoryPages) * 64 * 1024,
	}
}
