package agents

import (
	"fmt"
	"sync"
	"time"
)

// Agent represents an agent in the system
type Agent struct {
	ID           string
	Name         string
	Capabilities []string
	Reputation   float32
	LastSeen     int64
	Metadata     map[string]interface{}
}

// AgentRegistry manages agent registration and discovery
type AgentRegistry struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

// AgentCoordinator manages agent coordination
type AgentCoordinator struct {
	registry *AgentRegistry
	mu       sync.RWMutex
}

// NewAgentRegistry creates a new agent registry
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		agents: make(map[string]*Agent),
	}
}

// NewAgentCoordinator creates a new agent coordinator
func NewAgentCoordinator() *AgentCoordinator {
	return &AgentCoordinator{
		registry: NewAgentRegistry(),
	}
}

// RegisterAgent registers a new agent
func (ar *AgentRegistry) RegisterAgent(agent *Agent) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if _, exists := ar.agents[agent.ID]; exists {
		return fmt.Errorf("agent already registered: %s", agent.ID)
	}

	agent.LastSeen = time.Now().UnixMilli()
	ar.agents[agent.ID] = agent
	return nil
}

// GetAgent retrieves an agent
func (ar *AgentRegistry) GetAgent(agentID string) (*Agent, error) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agent, exists := ar.agents[agentID]
	if !exists {
		return nil, fmt.Errorf("agent not found: %s", agentID)
	}
	return agent, nil
}

// ListAgents returns all registered agents
func (ar *AgentRegistry) ListAgents() []*Agent {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agents := make([]*Agent, 0, len(ar.agents))
	for _, agent := range ar.agents {
		agents = append(agents, agent)
	}
	return agents
}

// UpdateReputation updates an agent's reputation
func (ar *AgentRegistry) UpdateReputation(agentID string, delta float32) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	agent, exists := ar.agents[agentID]
	if !exists {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	agent.Reputation += delta
	agent.LastSeen = time.Now().UnixMilli()
	return nil
}

// FindSimilarAgents finds agents with similar capabilities
func (ar *AgentRegistry) FindSimilarAgents(agentID string, topK int) []*Agent {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agent, exists := ar.agents[agentID]
	if !exists {
		return nil
	}

	// Calculate similarity scores
	scores := make(map[string]float32)
	for id, other := range ar.agents {
		if id == agentID {
			continue
		}

		// Calculate capability overlap
		overlap := 0
		for _, cap := range agent.Capabilities {
			for _, otherCap := range other.Capabilities {
				if cap == otherCap {
					overlap++
				}
			}
		}

		if overlap > 0 {
			similarity := float32(overlap) / float32(len(agent.Capabilities)+len(other.Capabilities)-overlap)
			scores[id] = similarity
		}
	}

	// Sort by similarity
	type kv struct {
		Key   string
		Value float32
	}
	var sorted []kv
	for k, v := range scores {
		sorted = append(sorted, kv{k, v})
	}

	// Simple bubble sort for small lists
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Value > sorted[i].Value {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Return top-K
	result := make([]*Agent, 0, topK)
	for i := 0; i < len(sorted) && i < topK; i++ {
		if agent, ok := ar.agents[sorted[i].Key]; ok {
			result = append(result, agent)
		}
	}

	return result
}

// Coordinate establishes coordination between agents
func (ac *AgentCoordinator) Coordinate(agentID1, agentID2 string) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	agent1, err := ac.registry.GetAgent(agentID1)
	if err != nil {
		return err
	}

	agent2, err := ac.registry.GetAgent(agentID2)
	if err != nil {
		return err
	}

	// Record coordination in metadata
	if agent1.Metadata == nil {
		agent1.Metadata = make(map[string]interface{})
	}
	if agent2.Metadata == nil {
		agent2.Metadata = make(map[string]interface{})
	}

	coordList1, ok := agent1.Metadata["coordinated_with"].([]string)
	if !ok {
		coordList1 = make([]string, 0)
	}
	coordList1 = append(coordList1, agentID2)
	agent1.Metadata["coordinated_with"] = coordList1

	coordList2, ok := agent2.Metadata["coordinated_with"].([]string)
	if !ok {
		coordList2 = make([]string, 0)
	}
	coordList2 = append(coordList2, agentID1)
	agent2.Metadata["coordinated_with"] = coordList2

	return nil
}

// GetCoordinationGraph returns the coordination graph
func (ac *AgentCoordinator) GetCoordinationGraph() map[string][]string {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	graph := make(map[string][]string)
	for _, agent := range ac.registry.agents {
		if coordList, ok := agent.Metadata["coordinated_with"].([]string); ok {
			graph[agent.ID] = coordList
		}
	}
	return graph
}

// GetStats returns coordinator statistics
func (ac *AgentCoordinator) GetStats() map[string]interface{} {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	agents := ac.registry.ListAgents()
	totalReputation := float32(0)
	for _, agent := range agents {
		totalReputation += agent.Reputation
	}

	avgReputation := float32(0)
	if len(agents) > 0 {
		avgReputation = totalReputation / float32(len(agents))
	}

	return map[string]interface{}{
		"agent_count":      len(agents),
		"total_reputation": totalReputation,
		"avg_reputation":   avgReputation,
		"timestamp":        time.Now().UnixMilli(),
	}
}
