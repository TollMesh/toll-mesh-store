# Contributing to TollMeshStore

Thank you for your interest in contributing to TollMeshStore! We welcome contributions from everyone. This document provides guidelines and instructions for contributing.

## Ways to Contribute

- **Code**: Bug fixes, new features, performance improvements
- **Documentation**: Improve README, add examples, fix typos
- **Issues**: Report bugs, suggest features, ask questions
- **Tests**: Add test coverage, improve test quality
- **Discussions**: Share ideas, help other contributors

## Code of Conduct

Please read our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing. We are committed to providing a welcoming and inclusive environment.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Create a branch** for your changes: `git checkout -b feature/your-feature-name`
4. **Make your changes** following our guidelines
5. **Test your changes** thoroughly
6. **Commit your changes** with clear messages
7. **Push to your fork** and submit a pull request

## Development Setup

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed setup instructions.

## Code Style Guidelines

### Go Code Style

- Follow [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Use `gofmt` for formatting: `gofmt -w .`
- Use `golint` for linting: `golint ./...`
- Use `go vet` for vetting: `go vet ./...`
- Keep lines under 100 characters when possible
- Use meaningful variable names
- Add comments for exported functions and types

### Example Function

```go
// ProcessAgent processes an agent and returns its reputation score.
// It validates the agent profile and updates internal state.
func (c *AgentCoordinator) ProcessAgent(agent *Agent) (float32, error) {
	if agent == nil {
		return 0, fmt.Errorf("agent cannot be nil")
	}
	
	// Process agent logic here
	return agent.Reputation, nil
}
```

## Testing Requirements

- **All tests must pass**: `go test ./...`
- **Add tests for new features**: Aim for >80% coverage
- **Test edge cases**: Empty inputs, nil values, concurrent access
- **Use table-driven tests** for multiple scenarios

### Example Test

```go
func TestAgentCoordinator_Coordinate(t *testing.T) {
	tests := []struct {
		name    string
		agent1  string
		agent2  string
		wantErr bool
	}{
		{"valid agents", "agent-1", "agent-2", false},
		{"same agent", "agent-1", "agent-1", true},
		{"invalid agent", "agent-1", "invalid", true},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test implementation
		})
	}
}
```

## Commit Message Format

Use clear, descriptive commit messages:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code refactoring without feature changes
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `chore`: Build process, dependencies, etc.

### Examples

```
feat(agents): add agent reputation tracking

Implement reputation system for agents with decay over time.
Agents gain/lose reputation based on behavior.

Closes #123
```

```
fix(search): fix BM25 scoring calculation

The IDF calculation was using incorrect formula.
Updated to use standard BM25 formula.

Fixes #456
```

## Pull Request Process

1. **Update documentation** if you change functionality
2. **Add tests** for new features or bug fixes
3. **Ensure all tests pass**: `go test ./...`
4. **Run linters**: `gofmt`, `golint`, `go vet`
5. **Write a clear PR description** explaining your changes
6. **Link related issues** using `Closes #123` or `Fixes #456`
7. **Be responsive** to review feedback

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
Describe testing performed

## Checklist
- [ ] Tests pass
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] No new warnings generated
```

## Project Structure

```
toll-mesh-store/
├── agents/          # Agent coordination and discovery
├── api/             # HTTP API handlers
├── coordination/    # Gossip protocol
├── core/            # CRDTs and core types
├── graph/           # Knowledge graph and RAG
├── metrics/         # Metrics collection
├── persistence/     # WAL and snapshots
├── pubsub/          # Pub/Sub messaging
├── ranking/         # Ranking and reranking
├── scripting/       # Lua scripting engine
├── search/          # Hybrid search (BM25 + vectors)
├── store/           # MeshStore implementation
└── transactions/    # ACID transactions
```

## Reporting Issues

### Bug Reports

Include:
- Clear description of the bug
- Steps to reproduce
- Expected behavior
- Actual behavior
- Go version and OS
- Relevant code or logs

### Feature Requests

Include:
- Clear description of the feature
- Use cases and benefits
- Possible implementation approach
- Examples or mockups if applicable

## Review Process

1. **Automated checks** run on all PRs (tests, linting)
2. **Code review** by maintainers
3. **Feedback** provided on changes
4. **Approval** once all feedback is addressed
5. **Merge** by maintainer

## License

By contributing to TollMeshStore, you agree that your contributions will be licensed under the MIT License.

## Questions?

- Open an issue for questions
- Check existing issues and discussions
- Read the documentation in [ARCHITECTURE.md](ARCHITECTURE.md)
- Review [DEVELOPMENT.md](DEVELOPMENT.md) for setup help

## Recognition

Contributors will be recognized in:
- CONTRIBUTORS.md file
- Release notes
- GitHub contributors page

Thank you for contributing to TollMeshStore! 🎉