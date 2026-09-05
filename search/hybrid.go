package search

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Document represents a searchable document. Timestamp/Node are this
// document's LWW-register version for gossip replication -- previously
// Timestamp existed but was never actually set by any caller.
type Document struct {
	ID        string
	Content   string
	Metadata  map[string]interface{}
	Vector    []float32
	Timestamp int64
	Node      string
}

// SearchResult represents a search result with score
type SearchResult struct {
	Document *Document
	Score    float32
	Rank     int
}

// BM25Index implements BM25 full-text search
type BM25Index struct {
	mu          sync.RWMutex
	documents   map[string]*Document
	index       map[string]map[string]int // term -> docID -> count
	docTermsLen map[string]int            // docID -> term count, needed to maintain a true running average
	docCount    int
	totalTerms  int64 // sum of all indexed documents' term counts
	avgDocLen   float32
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
		documents:   make(map[string]*Document),
		index:       make(map[string]map[string]int),
		docTermsLen: make(map[string]int),
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
	// Re-indexing an ID that's already present (an update, or -- once
	// gossip replication re-indexes a merged document -- a repeated
	// merge) previously indexed on top of the old entry without removing
	// it first: indexBM25 unconditionally increments docCount, appends
	// postings, and adds to totalTerms, so indexing the same ID twice
	// double-counted it in every BM25 statistic, corrupting every score
	// (docCount feeds IDF, totalTerms/docCount is avgDocLen, both are
	// direct terms in the scoring formula). deindexBM25Locked undoes the
	// previous indexing of this ID first, the same cleanup
	// DeleteDocument already does, so a re-index is really "delete old,
	// index new" rather than "add on top of old".
	hse.deindexBM25Locked(doc.ID)
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

// deindexBM25Locked removes docID's contribution to the BM25 index
// entirely: its postings from every term, its term-length entry, and its
// share of totalTerms/docCount/avgDocLen. A no-op if docID isn't indexed.
// Callers must hold hse.bm25.mu and are responsible for removing docID
// from hse.bm25.documents themselves (this only undoes indexBM25's
// bookkeeping, not the raw document lookup).
func (hse *HybridSearchEngine) deindexBM25Locked(docID string) {
	if _, exists := hse.bm25.documents[docID]; !exists {
		return
	}

	for term, postings := range hse.bm25.index {
		delete(postings, docID)
		if len(postings) == 0 {
			delete(hse.bm25.index, term)
		}
	}

	hse.bm25.totalTerms -= int64(hse.bm25.docTermsLen[docID])
	delete(hse.bm25.docTermsLen, docID)
	hse.bm25.docCount--
	if hse.bm25.docCount > 0 {
		hse.bm25.avgDocLen = float32(hse.bm25.totalTerms) / float32(hse.bm25.docCount)
	} else {
		hse.bm25.avgDocLen = 0
	}
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

	// Maintain a true running average across all indexed documents. This
	// previously just assigned len(terms), so avgDocLen silently became
	// whichever document happened to be indexed most recently instead of
	// an average -- corrupting every BM25 score, since avgDocLen is a
	// direct term in the scoring formula.
	hse.bm25.docTermsLen[doc.ID] = len(terms)
	hse.bm25.totalTerms += int64(len(terms))
	hse.bm25.avgDocLen = float32(hse.bm25.totalTerms) / float32(hse.bm25.docCount)
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
	hse.deindexBM25Locked(docID)
	delete(hse.bm25.documents, docID)
	hse.bm25.mu.Unlock()

	hse.vector.mu.Lock()
	delete(hse.vector.documents, docID)
	delete(hse.vector.vectors, docID)
	hse.vector.mu.Unlock()

	return nil
}

// GetStats returns search engine statistics
// Snapshot returns a copy of every indexed document, for gossip
// replication.
func (hse *HybridSearchEngine) Snapshot() []Document {
	hse.bm25.mu.RLock()
	defer hse.bm25.mu.RUnlock()

	out := make([]Document, 0, len(hse.bm25.documents))
	for _, d := range hse.bm25.documents {
		out = append(out, *d)
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: a (Timestamp, Node)
// LWW-register comparison per document ID, the same pattern as Cache and
// Pipelines -- a peer's document is adopted only when it's strictly newer.
// Adopting it goes through IndexDocument, not a direct map write, so the
// BM25 bookkeeping (deindexBM25Locked-then-reindex) stays correct.
//
// Known limitation, not solved here: DeleteDocument is a hard local
// delete with no tombstone, like Pipelines and unlike Sorted Sets/Streams.
// A document deleted on one node will be silently re-introduced by the
// next gossip round from any peer that still has it.
func (hse *HybridSearchEngine) MergeSnapshot(docs []Document) {
	for i := range docs {
		peer := &docs[i]

		hse.bm25.mu.RLock()
		local, exists := hse.bm25.documents[peer.ID]
		hse.bm25.mu.RUnlock()

		if exists && !documentLess(local.Timestamp, local.Node, peer.Timestamp, peer.Node) {
			continue
		}

		peerCopy := *peer
		hse.IndexDocument(&peerCopy)
	}
}

// documentLess reports whether (tsA, nodeA) sorts strictly before (tsB,
// nodeB) in the document LWW-register's version order.
func documentLess(tsA int64, nodeA string, tsB int64, nodeB string) bool {
	if tsA != tsB {
		return tsA < tsB
	}
	return nodeA < nodeB
}

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
