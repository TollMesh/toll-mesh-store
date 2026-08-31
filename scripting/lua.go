package scripting

import (
	"fmt"
	"sync"
	"time"
)

// LuaScript represents a registered Lua script
type LuaScript struct {
	Name       string
	Code       string
	SHA1       string
	Created    int64
	Executions int64
	LastError  string
}

// ScriptResult represents the result of script execution
type ScriptResult struct {
	Output    interface{}
	Error     string
	Duration  int64
	Timestamp int64
}

// LuaEngine manages Lua script execution (simplified, Go stdlib only)
type LuaEngine struct {
	mu               sync.RWMutex
	scripts          map[string]*LuaScript
	maxScriptSize    int
	executionTimeout time.Duration
	results          map[string]*ScriptResult
}

// NewLuaEngine creates a new Lua scripting engine
func NewLuaEngine(maxScriptSize int, executionTimeout time.Duration) *LuaEngine {
	le := &LuaEngine{
		scripts:          make(map[string]*LuaScript),
		maxScriptSize:    maxScriptSize,
		executionTimeout: executionTimeout,
		results:          make(map[string]*ScriptResult),
	}

	return le
}

// RegisterScript registers a new Lua script
func (le *LuaEngine) RegisterScript(name, code string) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if len(code) > le.maxScriptSize {
		return fmt.Errorf("script too large: %d > %d", len(code), le.maxScriptSize)
	}

	// Basic validation - check for balanced braces
	if !isValidLuaSyntax(code) {
		return fmt.Errorf("invalid Lua syntax")
	}

	// Calculate SHA1 (simplified - just use name for now)
	sha1 := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())

	script := &LuaScript{
		Name:    name,
		Code:    code,
		SHA1:    sha1,
		Created: time.Now().UnixMilli(),
	}

	le.scripts[name] = script

	return nil
}

// ExecuteScript executes a registered Lua script (simplified evaluation)
func (le *LuaEngine) ExecuteScript(name string, args map[string]interface{}) (*ScriptResult, error) {
	le.mu.RLock()
	script, exists := le.scripts[name]
	le.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("script not found: %s", name)
	}

	start := time.Now()
	result := &ScriptResult{
		Timestamp: start.UnixMilli(),
	}

	// Execute script with timeout
	done := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("script panic: %v", r)
			}
		}()

		// Simplified evaluation - execute script logic
		output := evaluateLuaScript(script.Code, args)
		done <- output
	}()

	// Wait for execution or timeout
	select {
	case output := <-done:
		if err, ok := output.(error); ok {
			result.Error = err.Error()
			script.LastError = err.Error()
			return result, err
		}
		result.Output = output
	case <-time.After(le.executionTimeout):
		result.Error = "execution timeout"
		script.LastError = "execution timeout"
		return result, fmt.Errorf("script execution timeout")
	}

	result.Duration = time.Since(start).Milliseconds()
	script.Executions++

	le.mu.Lock()
	le.results[name] = result
	le.mu.Unlock()

	return result, nil
}

// ExecuteScriptInline executes inline Lua code (simplified)
func (le *LuaEngine) ExecuteScriptInline(code string, args map[string]interface{}) (*ScriptResult, error) {
	if len(code) > le.maxScriptSize {
		return nil, fmt.Errorf("script too large: %d > %d", len(code), le.maxScriptSize)
	}

	start := time.Now()
	result := &ScriptResult{
		Timestamp: start.UnixMilli(),
	}

	// Execute with timeout
	done := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("script panic: %v", r)
			}
		}()

		output := evaluateLuaScript(code, args)
		done <- output
	}()

	select {
	case output := <-done:
		if err, ok := output.(error); ok {
			result.Error = err.Error()
			return result, err
		}
		result.Output = output
	case <-time.After(le.executionTimeout):
		result.Error = "execution timeout"
		return result, fmt.Errorf("script execution timeout")
	}

	result.Duration = time.Since(start).Milliseconds()
	return result, nil
}

// GetScript retrieves a registered script
func (le *LuaEngine) GetScript(name string) (*LuaScript, error) {
	le.mu.RLock()
	defer le.mu.RUnlock()

	script, exists := le.scripts[name]
	if !exists {
		return nil, fmt.Errorf("script not found: %s", name)
	}

	return script, nil
}

// ListScripts returns all registered scripts
func (le *LuaEngine) ListScripts() []*LuaScript {
	le.mu.RLock()
	defer le.mu.RUnlock()

	scripts := make([]*LuaScript, 0, len(le.scripts))
	for _, script := range le.scripts {
		scripts = append(scripts, script)
	}
	return scripts
}

// DeleteScript removes a registered script
func (le *LuaEngine) DeleteScript(name string) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if _, exists := le.scripts[name]; !exists {
		return fmt.Errorf("script not found: %s", name)
	}

	delete(le.scripts, name)
	delete(le.results, name)
	return nil
}

// GetStats returns scripting statistics
func (le *LuaEngine) GetStats() map[string]interface{} {
	le.mu.RLock()
	defer le.mu.RUnlock()

	totalExecutions := int64(0)
	for _, script := range le.scripts {
		totalExecutions += script.Executions
	}

	return map[string]interface{}{
		"registered_scripts": len(le.scripts),
		"total_executions":   totalExecutions,
		"max_script_size":    le.maxScriptSize,
		"execution_timeout":  le.executionTimeout.String(),
	}
}

// Close closes the Lua engine
func (le *LuaEngine) Close() {
	// No-op for stdlib implementation
}

// Helper functions for simplified Lua evaluation
func isValidLuaSyntax(code string) bool {
	braceCount := 0
	for _, ch := range code {
		if ch == '{' {
			braceCount++
		} else if ch == '}' {
			braceCount--
		}
	}
	return braceCount == 0
}

func evaluateLuaScript(code string, args map[string]interface{}) interface{} {
	// Simplified Lua evaluation - just return args for now
	// In production, this would use a proper Lua interpreter
	return map[string]interface{}{
		"code": code,
		"args": args,
		"note": "Simplified Lua evaluation - use gopher-lua for full support",
	}
}
