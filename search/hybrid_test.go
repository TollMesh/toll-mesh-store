package search

import (
	"testing"
)

func TestBM25BasicSearch(t *testing.T) {
	hse := NewHybridSearchEngine()
	hse.IndexDocument(&Document{ID: "1", Content: "the quick brown fox"})
	hse.IndexDocument(&Document{ID: "2", Content: "the lazy dog sleeps"})

	results := hse.SearchBM25("fox", 10)
	if len(results) != 1 || results[0].Document.ID != "1" {
		t.Fatalf("expected doc 1 to match 'fox', got %+v", results)
	}
}

func TestAvgDocLenIsARunningAverageNotLastDocLen(t *testing.T) {
	hse := NewHybridSearchEngine()
	hse.IndexDocument(&Document{ID: "1", Content: "one two three four five six seven eight nine ten"}) // 10 terms
	hse.IndexDocument(&Document{ID: "2", Content: "short"})                                            // 1 term

	// avgDocLen must reflect both documents (avg of 10 and 1 = 5.5), not
	// just the length of whichever was indexed last (1).
	avg := hse.bm25.avgDocLen
	if avg < 5 || avg > 6 {
		t.Errorf("expected avgDocLen around 5.5 (running average), got %v (looks like it was overwritten by the last doc's length)", avg)
	}
}

// TestReindexingSameIDDoesNotDoubleCount is the regression test for a real
// bug: IndexDocument never removed a document's *previous* BM25
// contribution before re-indexing it under the same ID, so calling it
// twice for the same ID (an update -- or, once gossip replication
// re-indexes a merged/re-merged document, an ordinary occurrence) doubled
// docCount, duplicated postings, and inflated totalTerms, corrupting
// every BM25 score for every document (docCount feeds IDF, totalTerms/
// docCount is avgDocLen, both are direct terms in the scoring formula).
func TestReindexingSameIDDoesNotDoubleCount(t *testing.T) {
	hse := NewHybridSearchEngine()

	hse.IndexDocument(&Document{ID: "1", Content: "hello world"})
	hse.IndexDocument(&Document{ID: "1", Content: "hello world"})

	if hse.bm25.docCount != 1 {
		t.Errorf("docCount after indexing the same ID twice = %d, want 1", hse.bm25.docCount)
	}
	if got := hse.bm25.index["hello"]["1"]; got != 1 {
		t.Errorf("postings count for \"hello\"/doc1 = %d, want 1 (re-indexing duplicated postings instead of replacing)", got)
	}
	if hse.bm25.docTermsLen["1"] != 2 {
		t.Errorf("docTermsLen[1] = %d, want 2", hse.bm25.docTermsLen["1"])
	}
	if hse.bm25.totalTerms != 2 {
		t.Errorf("totalTerms = %d, want 2 (got inflated by the duplicate index call)", hse.bm25.totalTerms)
	}

	// Re-indexing with *different*, longer content must fully replace the
	// old contribution, not add to it.
	hse.IndexDocument(&Document{ID: "1", Content: "a completely different and longer sentence"})
	if hse.bm25.docCount != 1 {
		t.Errorf("docCount after updating doc 1's content = %d, want 1", hse.bm25.docCount)
	}
	if _, stillThere := hse.bm25.index["hello"]; stillThere {
		t.Error("old content's postings (\"hello\") were not removed after re-indexing doc 1 with new content")
	}
	if hse.bm25.docTermsLen["1"] != 6 {
		t.Errorf("docTermsLen[1] after update = %d, want 6 (the new content's term count)", hse.bm25.docTermsLen["1"])
	}
}

func TestSearchAfterDeleteDoesNotCrashOrReturnDeletedDoc(t *testing.T) {
	hse := NewHybridSearchEngine()
	hse.IndexDocument(&Document{ID: "1", Content: "unique searchable term"})
	hse.IndexDocument(&Document{ID: "2", Content: "other content entirely"})

	if err := hse.DeleteDocument("1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	results := hse.SearchBM25("unique searchable term", 10)
	for _, r := range results {
		if r.Document == nil {
			t.Fatal("search returned a result with a nil Document after delete -- stale index entry")
		}
		if r.Document.ID == "1" {
			t.Error("deleted document was still returned by search")
		}
	}
}

func TestVectorSearch(t *testing.T) {
	hse := NewHybridSearchEngine()
	hse.IndexDocument(&Document{ID: "1", Content: "a", Vector: []float32{1, 0, 0}})
	hse.IndexDocument(&Document{ID: "2", Content: "b", Vector: []float32{0, 1, 0}})

	results := hse.SearchVector([]float32{1, 0, 0}, 10)
	if len(results) == 0 || results[0].Document.ID != "1" {
		t.Fatalf("expected doc 1 to rank first for identical vector, got %+v", results)
	}
}

func TestHybridSearch(t *testing.T) {
	hse := NewHybridSearchEngine()
	hse.IndexDocument(&Document{ID: "1", Content: "machine learning", Vector: []float32{1, 0}})
	hse.IndexDocument(&Document{ID: "2", Content: "cooking recipes", Vector: []float32{0, 1}})

	results := hse.SearchHybrid("machine learning", []float32{1, 0}, 10)
	if len(results) == 0 || results[0].Document.ID != "1" {
		t.Fatalf("expected doc 1 to rank first, got %+v", results)
	}
}
