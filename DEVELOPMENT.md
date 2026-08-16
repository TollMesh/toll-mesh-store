# Development Guide for TollMeshStore

This guide will help you set up your development environment and get started contributing to TollMeshStore.

## Prerequisites

- **Go 1.25 or higher**: [Download Go](https://golang.org/dl/)
- **Git**: For version control
- **Make** (optional): For running common tasks
- **Text Editor/IDE**: VS Code, GoLand, or your preferred editor

### Verify Installation

```bash
go version
git --version
```

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/toll-mesh/store.git
cd store
```

### 2. Install Dependencies

```bash
go mod download
go mod tidy
```

### 3. Run Tests

```bash
go test ./...
```

All tests should pass. You should see output like:

```
ok  	github.com/toll-mesh/store/core	0.123s
ok  	github.com/toll-mesh/store/store	0.456s
```

### 4. Build the Project

```bash
go build ./...
```

## Project Structure

```
toll-mesh-store/
├── agents/              # Agent coordination and discovery
│   └── coordinator.go   # Agent registry and coordination logic
├── api/                 # HTTP API handlers
│   └── http.go          # REST API endpoints
├── coordination/        # Gossip protocol for peer-to-peer sync
│   └── gossip.go        # Gossip protocol implementation
├── core/                # Core CRDTs and types
│   ├── crdt.go          # CRDT implementations (GCounter, GSet, ExpiringSet)
│   ├── crdt_test.go     # CRDT tests
│   └── types.go         # Core type definitions
├── graph/               # Knowledge graph and RAG
│   └── rag.go           # Graph RAG implementation
├── metrics/             # Metrics collection
│   └── metrics.go       # Metrics tracking
├── persistence/         # Data persistence
│   └── persistence.go   # WAL and snapshot implementation
├── pubsub/              # Pub/Sub messaging
│   └── pubsub.go        # Pub/Sub engine
├── ranking/             # Ranking and reranking
│   └── ranker.go        # Multi-stage ranking pipeline
├── scripting/           # Lua scripting engine
│   └── lua.go           # Lua script execution
├── search/              # Hybrid search
│   └── hybrid.go        # BM25 + vector search
├── store/               # MeshStore implementation
│   ├── mesh_store.go    # Main MeshStore logic
│   └── mesh_store_test.go # MeshStore tests
├── transactions/        # ACID transactions
│   └── transactions.go  # Transaction engine
├── go.mod               # Go module definition
├── LICENSE              # MIT License
├── README.md            # Project overview
├── CONTRIBUTING.md      # Contribution guidelines
├── DEVELOPMENT.md       # This file
└── CODE_OF_CONDUCT.md   # Community standards
```

## Module Descriptions

### Core Modules

**agents/** - Agent Coordination
- Agent registration and discovery
- Capability matching
- Reputation tracking
- Coordination protocols

**core/** - CRDTs and Types
- GCounter: Grow-only counter
- GSet: Grow-only set
- ExpiringSet: Set with TTL
- Core type definitions

**store/** - MeshStore
- Rate limiting (token bucket)
- Replay protection
- Cache operations
- Concurrent access handling

### Advanced Modules

**search/** - Hybrid Search
- BM25 full-text indexing
- Dense vector search
- Hybrid ranking combining both

**graph/** - Knowledge Graph
- Node and edge management
- Multi-hop reasoning
- Relationship inference

**ranking/** - Multi-Stage Ranking
- BM25 ranker
- Vector ranker
- LLM ranker
- Rank fusion (linear, RRF, max)

**pubsub/** - Event Messaging
- Topic-based subscriptions
- Pattern matching
- Message history

**transactions/** - ACID Operations
- Transaction management
- Snapshot isolation
- Rollback support

**scripting/** - Lua Scripting
- Script registration
- Execution with timeout
- Error handling

## Common Development Tasks

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for specific package
go test -v ./agents

# Run specific test
go test -v ./agents -run TestAgentCoordinator

# Run tests with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Code Formatting

```bash
# Format all Go files
gofmt -w .

# Check formatting without modifying
gofmt -l .
```

### Linting

```bash
# Install golint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run linter
golangci-lint run ./...

# Run go vet
go vet ./...
```

### Building

```bash
# Build all packages
go build ./...

# Build specific package
go build ./agents

# Build with specific output
go build -o tollmesh ./cmd/tollmesh
```

## Debugging

### Using Print Statements

```go
fmt.Printf("Debug: value=%v, type=%T\n", value, value)
```

### Using Delve Debugger

```bash
# Install Delve
go install github.com/go-delve/delve/cmd/dlv@latest

# Debug tests
dlv test ./agents

# Debug with breakpoints
(dlv) break main.main
(dlv) continue
(dlv) next
(dlv) print variable
```

### Logging

```go
import "log"

log.Printf("Info: %v", value)
log.Fatalf("Error: %v", err)
```

## Performance Profiling

### CPU Profiling

```bash
# Generate CPU profile
go test -cpuprofile=cpu.prof ./agents

# Analyze profile
go tool pprof cpu.prof
```

### Memory Profiling

```bash
# Generate memory profile
go test -memprofile=mem.prof ./agents

# Analyze profile
go tool pprof mem.prof
```

## Making Changes

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Edit files in the appropriate module
- Follow code style guidelines (see CONTRIBUTING.md)
- Add tests for new functionality

### 3. Run Tests

```bash
go test ./...
```

### 4. Format and Lint

```bash
gofmt -w .
golangci-lint run ./...
go vet ./...
```

### 5. Commit Changes

```bash
git add .
git commit -m "feat(module): description of changes"
```

### 6. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Adding New Features

### Adding a New Module

1. Create new directory: `mkdir -p new_module`
2. Create main file: `new_module/main.go`
3. Create test file: `new_module/main_test.go`
4. Add package documentation
5. Implement functionality
6. Add tests (aim for >80% coverage)
7. Update CONTRIBUTING.md with module description

### Adding Tests

```go
package agents

import "testing"

func TestNewFeature(t *testing.T) {
	// Arrange
	expected := "value"
	
	// Act
	result := NewFeature()
	
	// Assert
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
```

## Troubleshooting

### Import Errors

```bash
# Update module dependencies
go mod tidy

# Download dependencies
go mod download
```

### Build Errors

```bash
# Clean build cache
go clean -cache

# Rebuild
go build ./...
```

### Test Failures

```bash
# Run specific test with verbose output
go test -v -run TestName ./package

# Run with race detector
go test -race ./...
```

## Documentation

### Adding Comments

```go
// Package agents provides agent coordination and discovery.
package agents

// Agent represents an agent in the system.
type Agent struct {
	ID string // Unique identifier
}

// NewAgent creates a new agent with the given ID.
func NewAgent(id string) *Agent {
	return &Agent{ID: id}
}
```

### Updating README

- Keep README.md up to date with major changes
- Add examples for new features
- Update feature list

### Updating Architecture

- Update ARCHITECTURE.md for significant changes
- Document new modules
- Explain design decisions

## Resources

- [Go Documentation](https://golang.org/doc/)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://golang.org/doc/effective_go)
- [TollMeshStore Architecture](ARCHITECTURE.md)
- [Contributing Guidelines](CONTRIBUTING.md)

## Getting Help

- **Questions**: Open an issue with the `question` label
- **Bugs**: Open an issue with the `bug` label
- **Features**: Open an issue with the `enhancement` label
- **Discussions**: Use GitHub Discussions

## Next Steps

1. Read [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines
2. Read [ARCHITECTURE.md](ARCHITECTURE.md) for system design
3. Pick an issue to work on
4. Follow the development workflow above
5. Submit a pull request

Happy coding! 🚀