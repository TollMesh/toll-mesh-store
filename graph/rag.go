package graph

import (
	"fmt"
	"sync"
	"time"
)

// Node represents a node in the knowledge graph
type Node struct {
	ID         string
	Type       string // agent, threat, policy, challenge
	Properties map[string]interface{}
	Embeddings []float32
	Created    int64
}

// Edge represents a relationship between nodes
type Edge struct {
	Source string
	Target string
	Type   string // related_to, detected_by, mitigated_by
	Weight float32
}

// KnowledgeGraph manages the knowledge graph
type KnowledgeGraph struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	edges map[string][]*Edge
}

// GraphRAG combines knowledge graph with reasoning
type GraphRAG struct {
	kg    *KnowledgeGraph
	mu    sync.RWMutex
	cache map[string]interface{}
}

// NewKnowledgeGraph creates a new knowledge graph
func NewKnowledgeGraph() *KnowledgeGraph {
	return &KnowledgeGraph{
		nodes: make(map[string]*Node),
		edges: make(map[string][]*Edge),
	}
}

// NewGraphRAG creates a new Graph RAG system
func NewGraphRAG() *GraphRAG {
	return &GraphRAG{
		kg:    NewKnowledgeGraph(),
		cache: make(map[string]interface{}),
	}
}

// AddNode adds a node to the knowledge graph
func (kg *KnowledgeGraph) AddNode(node *Node) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	if _, exists := kg.nodes[node.ID]; exists {
		return fmt.Errorf("node already exists: %s", node.ID)
	}

	node.Created = time.Now().UnixMilli()
	kg.nodes[node.ID] = node
	return nil
}

// AddEdge adds an edge between two nodes
func (kg *KnowledgeGraph) AddEdge(edge *Edge) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	if _, exists := kg.nodes[edge.Source]; !exists {
		return fmt.Errorf("source node not found: %s", edge.Source)
	}
	if _, exists := kg.nodes[edge.Target]; !exists {
		return fmt.Errorf("target node not found: %s", edge.Target)
	}

	kg.edges[edge.Source] = append(kg.edges[edge.Source], edge)
	return nil
}

// GetNode retrieves a node
func (kg *KnowledgeGraph) GetNode(nodeID string) (*Node, error) {
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	node, exists := kg.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found: %s", nodeID)
	}
	return node, nil
}

// GetNeighbors returns neighboring nodes
func (kg *KnowledgeGraph) GetNeighbors(nodeID string) []*Node {
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	neighbors := make([]*Node, 0)
	if edges, exists := kg.edges[nodeID]; exists {
		for _, edge := range edges {
			if node, ok := kg.nodes[edge.Target]; ok {
				neighbors = append(neighbors, node)
			}
		}
	}
	return neighbors
}

// Reason performs multi-hop reasoning
func (gr *GraphRAG) Reason(query string, context map[string]interface{}) map[string]interface{} {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	result := map[string]interface{}{
		"query":     query,
		"timestamp": time.Now().UnixMilli(),
		"reasoning": []string{},
		"nodes":     []string{},
		"edges":     []string{},
	}

	// Extract entities from context
	if agentID, ok := context["agent_id"].(string); ok {
		if node, err := gr.kg.GetNode(agentID); err == nil {
			result["nodes"] = append(result["nodes"].([]string), node.ID)
			result["reasoning"] = append(result["reasoning"].([]string), fmt.Sprintf("Found agent: %s", node.ID))

			// Find related threats
			neighbors := gr.kg.GetNeighbors(agentID)
			for _, neighbor := range neighbors {
				result["nodes"] = append(result["nodes"].([]string), neighbor.ID)
				result["reasoning"] = append(result["reasoning"].([]string), fmt.Sprintf("Related to: %s (%s)", neighbor.ID, neighbor.Type))
			}
		}
	}

	return result
}

// FindRelated finds related nodes
func (gr *GraphRAG) FindRelated(nodeID string, maxDepth int) []*Node {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	visited := make(map[string]bool)
	related := make([]*Node, 0)

	var traverse func(string, int)
	traverse = func(id string, depth int) {
		if depth > maxDepth || visited[id] {
			return
		}
		visited[id] = true

		neighbors := gr.kg.GetNeighbors(id)
		for _, neighbor := range neighbors {
			related = append(related, neighbor)
			traverse(neighbor.ID, depth+1)
		}
	}

	traverse(nodeID, 0)
	return related
}

// GetStats returns graph statistics
func (gr *GraphRAG) GetStats() map[string]interface{} {
	gr.kg.mu.RLock()
	defer gr.kg.mu.RUnlock()

	nodeCount := len(gr.kg.nodes)
	edgeCount := 0
	for _, edges := range gr.kg.edges {
		edgeCount += len(edges)
	}

	return map[string]interface{}{
		"node_count": nodeCount,
		"edge_count": edgeCount,
		"timestamp":  time.Now().UnixMilli(),
	}
}
