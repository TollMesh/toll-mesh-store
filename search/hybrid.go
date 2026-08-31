package search

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Document represents a searchable document
type Document struct {
	ID        string
	Content   string
	Metadata  map[string]interface{}
	Vector    []float32
	Timestamp int64
}

// SearchResult represents a search result with score
type SearchResult struct {
	Document *Document
	Score    float32
	Rank     int
}

// BM25Index implements BM25 full-text search
type BM25Index struct {
	mu        sync.RWMutex
	documents map[string]*Document
	index     map[string]map[string]int // term -> docID -> count
	docCount  int
	avgDocLen float32
}

// VectorIndex implements dense vector search
type VectorIndex struct {
	mu        sync.RWMutex
	documents map[string]*Document
	vectors   map[string][]float32
}

// HybridSearchEngine combines BM25 and vector search
type HybridSearchEngine struct {
	bm25   *BM25Index
	vector *VectorIndex
	mu     sync.RWMutex
}

// NewBM25Index creates a new BM25 index
func NewBM25Index() *BM25Index {
	return &BM25Index{
		documents: make(map[string]*Document),
		index:     make(map[string]map[string]int),
	}
}

// NewVectorIndex creates a new vector index
func NewVectorIndex() *VectorIndex {
	return &VectorIndex{
		documents: make(map[string]*Document),
		vectors:   make(map[string][]float32),
	}
}

// NewHybridSearchEngine creates a new hybrid search engine
func NewHybridSearchEngine() *HybridSearchEngine {
	return &HybridSearchEngine{
		bm25:   NewBM25Index(),
		vector: NewVectorIndex(),
	}
}

// IndexDocument adds a document to the search index
func (hse *HybridSearchEngine) IndexDocument(doc *Document) error {
	hse.mu.Lock()
	defer hse.mu.Unlock()

	// Index in BM25
	hse.bm25.mu.Lock()
	hse.bm25.documents[doc.ID] = doc
	hse.indexBM25(doc)
	hse.bm25.mu.Unlock()

	// Index in vector
	hse.vector.mu.Lock()
	hse.vector.documents[doc.ID] = doc
	if doc.Vector != nil {
		hse.vector.vectors[doc.ID] = doc.Vector
	}
	hse.vector.mu.Unlock()

	return nil
}

// indexBM25 indexes a document in BM25
func (hse *HybridSearchEngine) indexBM25(doc *Document) {
	terms := strings.Fields(strings.ToLower(doc.Content))
	hse.bm25.docCount++

	for _, term := range terms {
		if hse.bm25.index[term] == nil {
			hse.bm25.index[term] = make(map[string]int)
		}
		hse.bm25.index[term][doc.ID]++
	}

	// Update average document length
	hse.bm25.avgDocLen = float32(len(terms))
}

// SearchBM25 performs BM25 full-text search
func (hse *HybridSearchEngine) SearchBM25(query string, topK int) []SearchResult {
	hse.bm25.mu.RLock()
	defer hse.bm25.mu.RUnlock()

	terms := strings.Fields(strings.ToLower(query))
	scores := make(map[string]float32)

	// Calculate BM25 scores
	for _, term := range terms {
		if postings, exists := hse.bm25.index[term]; exists {
			idf := float32(math.Log(float64(hse.bm25.docCount) / float64(len(postings))))

			for docID, count := range postings {
				doc := hse.bm25.documents[docID]
				docLen := float32(len(strings.Fields(doc.Content)))
				bm25Score := idf * float32(count) * (2.2) / (float32(count) + 1.2*(1-0.75+0.75*(docLen/hse.bm25.avgDocLen)))
				scores[docID] += bm25Score
			}
		}
	}

	// Convert to results and sort
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		results = append(results, SearchResult{
			Document: hse.bm25.documents[docID],
			Score:    score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to topK
	if len(results) > topK {
		results = results[:topK]
	}

	// Set ranks
	for i := range results {
		results[i].Rank = i + 1
	}

	return results
}

// SearchVector performs vector similarity search
func (hse *HybridSearchEngine) SearchVector(queryVector []float32, topK int) []SearchResult {
	hse.vector.mu.RLock()
	defer hse.vector.mu.RUnlock()

	scores := make(map[string]float32)

	// Calculate cosine similarity
	for docID, docVector := range hse.vector.vectors {
		similarity := cosineSimilarity(queryVector, docVector)
		scores[docID] = similarity
	}

	// Convert to results and sort
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		results = append(results, SearchResult{
			Document: hse.vector.documents[docID],
			Score:    score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to topK
	if len(results) > topK {
		results = results[:topK]
	}

	// Set ranks
	for i := range results {
		results[i].Rank = i + 1
	}

	return results
}

// SearchHybrid performs hybrid search combining BM25 and vector search
func (hse *HybridSearchEngine) SearchHybrid(query string, queryVector []float32, topK int) []SearchResult {
	// Get BM25 results
	bm25Results := hse.SearchBM25(query, topK*2)

	// Get vector results
	vectorResults := hse.SearchVector(queryVector, topK*2)

	// Combine and rank
	combined := make(map[string]float32)
	for _, result := range bm25Results {
		combined[result.Document.ID] += result.Score * 0.5
	}
	for _, result := range vectorResults {
		combined[result.Document.ID] += result.Score * 0.5
	}

	// Convert to results and sort
	results := make([]SearchResult, 0, len(combined))
	for docID, score := range combined {
		doc := hse.bm25.documents[docID]
		if doc == nil {
			doc = hse.vector.documents[docID]
		}
		results = append(results, SearchResult{
			Document: doc,
			Score:    score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to topK
	if len(results) > topK {
		results = results[:topK]
	}

	// Set ranks
	for i := range results {
		results[i].Rank = i + 1
	}

	return results
}

// DeleteDocument removes a document from the index
func (hse *HybridSearchEngine) DeleteDocument(docID string) error {
	hse.mu.Lock()
	defer hse.mu.Unlock()

	hse.bm25.mu.Lock()
	delete(hse.bm25.documents, docID)
	hse.bm25.mu.Unlock()

	hse.vector.mu.Lock()
	delete(hse.vector.documents, docID)
	delete(hse.vector.vectors, docID)
	hse.vector.mu.Unlock()

	return nil
}

// GetStats returns search engine statistics
func (hse *HybridSearchEngine) GetStats() map[string]interface{} {
	hse.bm25.mu.RLock()
	bm25Count := len(hse.bm25.documents)
	hse.bm25.mu.RUnlock()

	hse.vector.mu.RLock()
	vectorCount := len(hse.vector.documents)
	hse.vector.mu.RUnlock()

	return map[string]interface{}{
		"bm25_documents":   bm25Count,
		"vector_documents": vectorCount,
		"timestamp":        time.Now().UnixMilli(),
	}
}

// Helper function: cosine similarity
func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}
