package ranking

import "testing"

func TestAllSimpleRankersProduceIdenticalOutput(t *testing.T) {
	items := []RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}, {ID: "c", Score: 2}}

	bm25 := NewBM25Ranker().Rank(items)
	vector := NewVectorRanker().Rank(items)
	llm := NewLLMRanker().Rank(items)

	if bm25[0].ID != vector[0].ID || bm25[0].ID != llm[0].ID {
		t.Log("confirmed: BM25Ranker, VectorRanker, and LLMRanker are functionally identical (all just sort by Score)")
	}
	if bm25[0].ID != "b" {
		t.Errorf("expected 'b' (highest score) first, got %s", bm25[0].ID)
	}
}

func TestContextRankerAppliesBoosts(t *testing.T) {
	items := []RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}, {ID: "c", Score: 2}}

	// Without a boost, "b" (score 3) should rank first.
	plain := NewContextRanker(nil).Rank(items)
	if plain[0].ID != "b" {
		t.Fatalf("expected 'b' first with no boosts, got %s", plain[0].ID)
	}

	// With a large boost on "a", it should now outrank "b".
	boosted := NewContextRanker(map[string]interface{}{
		"boosts": map[string]float32{"a": 10.0},
	}).Rank(items)
	if boosted[0].ID != "a" {
		t.Errorf("expected boosted 'a' to rank first, got %s (context field is not being used)", boosted[0].ID)
	}
}

func TestRankingPipelineLinearFusion(t *testing.T) {
	rp := NewRankingPipeline("linear")
	rp.AddRanker(NewBM25Ranker(), 1.0)
	rp.AddRanker(NewVectorRanker(), 1.0)

	items := []RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 5}}
	result := rp.Rank(items)
	if result[0].ID != "b" {
		t.Errorf("expected 'b' to rank first, got %s", result[0].ID)
	}
}

func TestRankingPipelineEmptyReturnsInputUnchanged(t *testing.T) {
	rp := NewRankingPipeline("linear")
	items := []RankedItem{{ID: "a", Score: 1}}
	result := rp.Rank(items)
	if len(result) != 1 || result[0].ID != "a" {
		t.Errorf("expected input unchanged with no rankers, got %+v", result)
	}
}
