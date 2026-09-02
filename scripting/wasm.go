// Package scripting (this file) provides real arbitrary-code execution:
// a script is Go source code, compiled by the TinyGo toolchain to a WASI
// WebAssembly module, then run in a sandboxed wazero runtime (pure Go, no
// cgo) with a hard execution timeout. Input is delivered on the module's
// stdin and its result is read from stdout, mirroring Redis's SCRIPT
// LOAD + EVALSHA split: compilation is slow (TinyGo takes real seconds)
// and happens once via Compile; execution is cheap and happens many times
// via Execute, which only instantiates the already-compiled module.
package scripting

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// CompiledScript is a script's Go source and its compiled WASM module.
type CompiledScript struct {
	Name       string `json:"name"`
	Source     string `json:"source"`
	WasmBytes  []byte `json:"-"`
	WasmSize   int    `json:"wasm_size"`
	Compiled   int64  `json:"compiled"`
	Executions int64  `json:"executions"`
	LastError  string `json:"last_error,omitempty"`
}

// WasmEngine compiles Go source to WASI WebAssembly via TinyGo and executes
// compiled modules in a sandboxed wazero runtime.
type WasmEngine struct {
	mu          sync.RWMutex
	scripts     map[string]*CompiledScript
	tinygoPath  string
	execTimeout time.Duration
	memoryPages uint32 // wasm memory limit, in 64KiB pages
}

// NewWasmEngine creates a new engine. tinygoPath is the path to the tinygo
// binary (looked up on PATH if empty). Returns an error if tinygo cannot be
// found, since without it no script can ever be compiled.
func NewWasmEngine(tinygoPath string, execTimeout time.Duration) (*WasmEngine, error) {
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

	return &WasmEngine{
		scripts:     make(map[string]*CompiledScript),
		tinygoPath:  tinygoPath,
		execTimeout: execTimeout,
		memoryPages: 256, // 16MiB
	}, nil
}

// Compile compiles Go source to a WASI WASM module via TinyGo and registers
// it under name, replacing any existing script with that name. This is the
// slow path (real seconds, since it invokes an external compiler process)
// and is expected to happen far less often than Execute.
func (e *WasmEngine) Compile(name, source string) (*CompiledScript, error) {
	wasmBytes, err := e.compileSource(source)
	if err != nil {
		return nil, err
	}

	script := &CompiledScript{
		Name:      name,
		Source:    source,
		WasmBytes: wasmBytes,
		WasmSize:  len(wasmBytes),
		Compiled:  time.Now().UnixMilli(),
	}

	e.mu.Lock()
	e.scripts[name] = script
	e.mu.Unlock()

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
func (e *WasmEngine) Execute(name, input string) (string, error) {
	e.mu.RLock()
	script, exists := e.scripts[name]
	e.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("script not found: %s", name)
	}

	output, err := e.run(script.WasmBytes, input)

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
// happen synchronously in this call.
func (e *WasmEngine) ExecuteInline(source, input string) (string, error) {
	wasmBytes, err := e.compileSource(source)
	if err != nil {
		return "", err
	}
	return e.run(wasmBytes, input)
}

// run instantiates a WASM module in a fresh, sandboxed wazero runtime and
// executes it, enforcing execTimeout via WithCloseOnContextDone. That
// option is off by default in wazero (it costs a small amount of
// performance, since it makes the interpreter/compiler insert periodic
// cancellation checks into the generated code), but without it a tight
// loop with no host calls has no point at which ctx cancellation, or even
// an explicit Close, can ever take effect -- confirmed by an earlier
// version of this function hanging for 2+ minutes against an infinite-loop
// script despite closing the runtime from a watcher goroutine on timeout.
func (e *WasmEngine) run(wasmBytes []byte, input string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.execTimeout)
	defer cancel()

	runtimeConfig := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(e.memoryPages).
		WithCloseOnContextDone(true)
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer r.Close(context.Background())

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return "", fmt.Errorf("failed to instantiate WASI: %w", err)
	}

	var stdout, stderr bytes.Buffer
	moduleConfig := wazero.NewModuleConfig().
		WithStdin(bytes.NewReader([]byte(input))).
		WithStdout(&stdout).
		WithStderr(&stderr)

	_, err := r.InstantiateWithConfig(ctx, wasmBytes, moduleConfig)
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

// DeleteScript removes a registered script.
func (e *WasmEngine) DeleteScript(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.scripts[name]; !exists {
		return fmt.Errorf("script not found: %s", name)
	}
	delete(e.scripts, name)
	return nil
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
