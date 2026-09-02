package ranking

import (
	"sort"
	"sync"
)

// RankedItem represents an item with a rank score
type RankedItem struct {
	ID    string
	Score float32
	Rank  int
}

// Ranker interface for different ranking strategies
type Ranker interface {
	Rank(items []RankedItem) []RankedItem
}

// BM25Ranker ranks by BM25 score
type BM25Ranker struct{}

// VectorRanker ranks by vector similarity
type VectorRanker struct{}

// LLMRanker sorts items by their existing Score field. There is no LLM
// integration in this system; this is a plain score-based reranker
// intended to be used as one interchangeable stage of a RankingPipeline
// alongside BM25Ranker/VectorRanker, not an actual cross-encoder.
type LLMRanker struct{}

// ContextRanker ranks by domain-specific context
type ContextRanker struct {
	context map[string]interface{}
}

// RankingPipeline combines multiple rankers
type RankingPipeline struct {
	mu      sync.RWMutex
	rankers []Ranker
	weights []float32
	fusion  string // "linear", "rrf", "max"
}

// NewBM25Ranker creates a new BM25 ranker
func NewBM25Ranker() *BM25Ranker {
	return &BM25Ranker{}
}

// Rank implements BM25 ranking
func (br *BM25Ranker) Rank(items []RankedItem) []RankedItem {
	sorted := make([]RankedItem, len(items))
	copy(sorted, items)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})

	for i := range sorted {
		sorted[i].Rank = i + 1
	}

	return sorted
}

// NewVectorRanker creates a new vector ranker
func NewVectorRanker() *VectorRanker {
	return &VectorRanker{}
}

// Rank implements vector ranking
func (vr *VectorRanker) Rank(items []RankedItem) []RankedItem {
	sorted := make([]RankedItem, len(items))
	copy(sorted, items)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})

	for i := range sorted {
		sorted[i].Rank = i + 1
	}

	return sorted
}

// NewLLMRanker creates a new LLM ranker
func NewLLMRanker() *LLMRanker {
	return &LLMRanker{}
}

// Rank sorts by the existing Score field (see LLMRanker doc comment).
func (lr *LLMRanker) Rank(items []RankedItem) []RankedItem {
	sorted := make([]RankedItem, len(items))
	copy(sorted, items)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})

	for i := range sorted {
		sorted[i].Rank = i + 1
	}

	return sorted
}

// NewContextRanker creates a new context ranker
func NewContextRanker(context map[string]interface{}) *ContextRanker {
	return &ContextRanker{
		context: context,
	}
}

// Rank sorts by Score after applying per-ID multiplicative boosts from the
// ranker's context. Pass context["boosts"] as a map[string]float32 of item
// ID to multiplier (e.g. {"item-42": 2.0} doubles that item's score before
// sorting). Items with no boost entry are left at their original score.
func (cr *ContextRanker) Rank(items []RankedItem) []RankedItem {
	sorted := make([]RankedItem, len(items))
	copy(sorted, items)

	if boosts, ok := cr.context["boosts"].(map[string]float32); ok {
		for i := range sorted {
			if factor, exists := boosts[sorted[i].ID]; exists {
				sorted[i].Score *= factor
			}
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})

	for i := range sorted {
		sorted[i].Rank = i + 1
	}

	return sorted
}

// NewRankingPipeline creates a new ranking pipeline
func NewRankingPipeline(fusion string) *RankingPipeline {
	return &RankingPipeline{
		rankers: make([]Ranker, 0),
		weights: make([]float32, 0),
		fusion:  fusion,
	}
}

// AddRanker adds a ranker to the pipeline
func (rp *RankingPipeline) AddRanker(ranker Ranker, weight float32) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.rankers = append(rp.rankers, ranker)
	rp.weights = append(rp.weights, weight)
}

// Rank performs multi-stage ranking
func (rp *RankingPipeline) Rank(items []RankedItem) []RankedItem {
	rp.mu.RLock()
	defer rp.mu.RUnlock()

	if len(rp.rankers) == 0 {
		return items
	}

	// Get rankings from each ranker
	rankings := make([][]RankedItem, len(rp.rankers))
	for i, ranker := range rp.rankers {
		rankings[i] = ranker.Rank(items)
	}

	// Fuse rankings
	switch rp.fusion {
	case "linear":
		return rp.linearFusion(rankings)
	case "rrf":
		return rp.rrfFusion(rankings)
	case "max":
		return rp.maxFusion(rankings)
	default:
		return rp.linearFusion(rankings)
	}
}

// linearFusion combines rankings using weighted sum
func (rp *RankingPipeline) linearFusion(rankings [][]RankedItem) []RankedItem {
	scores := make(map[string]float32)

	for i, ranking := range rankings {
		weight := rp.weights[i]
		for _, item := range ranking {
			scores[item.ID] += item.Score * weight
		}
	}

	result := make([]RankedItem, 0, len(scores))
	for id, score := range scores {
		result = append(result, RankedItem{ID: id, Score: score})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	for i := range result {
		result[i].Rank = i + 1
	}

	return result
}

// rrfFusion combines rankings using reciprocal rank fusion
func (rp *RankingPipeline) rrfFusion(rankings [][]RankedItem) []RankedItem {
	scores := make(map[string]float32)

	for _, ranking := range rankings {
		for _, item := range ranking {
			scores[item.ID] += 1.0 / (60.0 + float32(item.Rank))
		}
	}

	result := make([]RankedItem, 0, len(scores))
	for id, score := range scores {
		result = append(result, RankedItem{ID: id, Score: score})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	for i := range result {
		result[i].Rank = i + 1
	}

	return result
}

// maxFusion combines rankings using max score
func (rp *RankingPipeline) maxFusion(rankings [][]RankedItem) []RankedItem {
	scores := make(map[string]float32)

	for _, ranking := range rankings {
		for _, item := range ranking {
			if item.Score > scores[item.ID] {
				scores[item.ID] = item.Score
			}
		}
	}

	result := make([]RankedItem, 0, len(scores))
	for id, score := range scores {
		result = append(result, RankedItem{ID: id, Score: score})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	for i := range result {
		result[i].Rank = i + 1
	}

	return result
}
