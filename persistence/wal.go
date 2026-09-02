package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALConfig holds configuration for the Write-Ahead Log
type WALConfig struct {
	MaxSegmentSize     int64         // Maximum size of a WAL segment in bytes
	RotationTime       time.Duration // Time-based rotation interval
	CompressionEnabled bool          // Enable compression for WAL entries
}

// WALSegment represents a single WAL segment file
type WALSegment struct {
	ID        int64
	Path      string
	File      *os.File
	Writer    *bufio.Writer
	Size      int64
	CreatedAt time.Time
	Entries   int64
}

// WALEntryWithChecksum represents a WAL entry with checksum for integrity
type WALEntryWithChecksum struct {
	Entry    *WALEntry
	Checksum uint32
}

// WriteAheadLog manages durable operation logging
type WriteAheadLog struct {
	mu               sync.RWMutex
	config           WALConfig
	walDir           string
	currentSegment   *WALSegment
	segments         []*WALSegment
	nextSegmentID    int64
	totalEntries     int64
	rotationTicker   *time.Ticker
	stopChan         chan struct{}
	lastRotationTime time.Time
}

// NewWriteAheadLog creates a new Write-Ahead Log
func NewWriteAheadLog(walDir string, config WALConfig) (*WriteAheadLog, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WriteAheadLog{
		config:           config,
		walDir:           walDir,
		segments:         make([]*WALSegment, 0),
		stopChan:         make(chan struct{}),
		lastRotationTime: time.Now(),
	}

	// Load existing segments
	if err := wal.loadExistingSegments(); err != nil {
		return nil, fmt.Errorf("failed to load existing segments: %w", err)
	}

	// Create initial segment if none exist
	if len(wal.segments) == 0 {
		if err := wal.createNewSegment(); err != nil {
			return nil, fmt.Errorf("failed to create initial segment: %w", err)
		}
	}

	return wal, nil
}

// Append appends an entry to the WAL
func (wal *WriteAheadLog) Append(entry *WALEntry) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.currentSegment == nil {
		return fmt.Errorf("no active WAL segment")
	}

	// Calculate entry size
	valueLen := 0
	if val, ok := entry.Value.([]byte); ok {
		valueLen = len(val)
	}
	entrySize := int64(binary.MaxVarintLen64*3 + len(entry.Key) + valueLen + len(entry.Namespace) + 4)

	// Check if rotation is needed
	if wal.currentSegment.Size+entrySize > wal.config.MaxSegmentSize {
		if err := wal.rotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Write entry to segment
	if err := wal.writeEntry(entry); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	wal.currentSegment.Entries++
	wal.currentSegment.Size += entrySize
	wal.totalEntries++

	return nil
}

// Read reads entries from the WAL starting from a given timestamp
func (wal *WriteAheadLog) Read(fromTimestamp int64) ([]*WALEntry, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var entries []*WALEntry

	for _, segment := range wal.segments {
		segmentEntries, err := wal.readSegment(segment, fromTimestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to read segment: %w", err)
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}

// Rotate rotates the current WAL segment
func (wal *WriteAheadLog) Rotate() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.rotateSegment()
}

// GetStats returns WAL statistics
func (wal *WriteAheadLog) GetStats() map[string]interface{} {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	totalSize := int64(0)
	for _, segment := range wal.segments {
		totalSize += segment.Size
	}

	return map[string]interface{}{
		"total_entries":      wal.totalEntries,
		"total_segments":     len(wal.segments),
		"current_segment_id": wal.nextSegmentID - 1,
		"total_size_bytes":   totalSize,
		"wal_directory":      wal.walDir,
		"max_segment_size":   wal.config.MaxSegmentSize,
	}
}

// Start begins the rotation timer
func (wal *WriteAheadLog) Start() {
	wal.rotationTicker = time.NewTicker(wal.config.RotationTime)
	go wal.rotationLoop()
}

// Stop gracefully shuts down the WAL
func (wal *WriteAheadLog) Stop() error {
	close(wal.stopChan)
	if wal.rotationTicker != nil {
		wal.rotationTicker.Stop()
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.currentSegment != nil {
		if wal.currentSegment.Writer != nil {
			wal.currentSegment.Writer.Flush()
		}
		if wal.currentSegment.File != nil {
			wal.currentSegment.File.Close()
		}
	}

	return nil
}

// Private helper methods

func (wal *WriteAheadLog) loadExistingSegments() error {
	entries, err := os.ReadDir(wal.walDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse segment ID from filename
		var segmentID int64
		_, err := fmt.Sscanf(entry.Name(), "segment-%d.wal", &segmentID)
		if err != nil {
			continue
		}

		if segmentID >= wal.nextSegmentID {
			wal.nextSegmentID = segmentID + 1
		}
	}

	return nil
}

func (wal *WriteAheadLog) createNewSegment() error {
	segmentID := wal.nextSegmentID
	wal.nextSegmentID++

	path := filepath.Join(wal.walDir, fmt.Sprintf("segment-%d.wal", segmentID))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create segment file: %w", err)
	}

	segment := &WALSegment{
		ID:        segmentID,
		Path:      path,
		File:      file,
		Writer:    bufio.NewWriter(file),
		Size:      0,
		CreatedAt: time.Now(),
		Entries:   0,
	}

	wal.currentSegment = segment
	wal.segments = append(wal.segments, segment)

	return nil
}

func (wal *WriteAheadLog) rotateSegment() error {
	if wal.currentSegment != nil {
		if wal.currentSegment.Writer != nil {
			wal.currentSegment.Writer.Flush()
		}
		if wal.currentSegment.File != nil {
			wal.currentSegment.File.Close()
		}
	}

	return wal.createNewSegment()
}

func (wal *WriteAheadLog) writeEntry(entry *WALEntry) error {
	if wal.currentSegment == nil || wal.currentSegment.Writer == nil {
		return fmt.Errorf("no active segment writer")
	}

	// Write timestamp
	if err := binary.Write(wal.currentSegment.Writer, binary.BigEndian, entry.Timestamp); err != nil {
		return err
	}

	// Write operation
	if err := wal.writeString(wal.currentSegment.Writer, entry.Operation); err != nil {
		return err
	}

	// Write key
	if err := wal.writeString(wal.currentSegment.Writer, entry.Key); err != nil {
		return err
	}

	// Write namespace
	if err := wal.writeString(wal.currentSegment.Writer, entry.Namespace); err != nil {
		return err
	}

	// Write value as JSON
	valueBytes := []byte{}
	if val, ok := entry.Value.([]byte); ok {
		valueBytes = val
	}

	if err := binary.Write(wal.currentSegment.Writer, binary.BigEndian, int32(len(valueBytes))); err != nil {
		return err
	}
	if _, err := wal.currentSegment.Writer.Write(valueBytes); err != nil {
		return err
	}

	// Write a checksum so readers can detect corruption; skipEntry (used
	// when skipping entries older than a requested timestamp) also reads
	// this field, so it must always be written to keep the two paths
	// aligned on the same on-disk format.
	if err := binary.Write(wal.currentSegment.Writer, binary.BigEndian, wal.calculateChecksum(entry)); err != nil {
		return err
	}

	return wal.currentSegment.Writer.Flush()
}

func (wal *WriteAheadLog) readSegment(segment *WALSegment, fromTimestamp int64) ([]*WALEntry, error) {
	file, err := os.Open(segment.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []*WALEntry

	for {
		var timestamp int64
		if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		if timestamp < fromTimestamp {
			// Skip this entry
			wal.skipEntry(reader)
			continue
		}

		operation, err := wal.readString(reader)
		if err != nil {
			return nil, err
		}

		key, err := wal.readString(reader)
		if err != nil {
			return nil, err
		}

		namespace, err := wal.readString(reader)
		if err != nil {
			return nil, err
		}

		var valueLen int32
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}

		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		entry := &WALEntry{
			Timestamp: timestamp,
			Operation: operation,
			Key:       key,
			Namespace: namespace,
			Value:     value,
		}

		var storedChecksum uint32
		if err := binary.Read(reader, binary.BigEndian, &storedChecksum); err != nil {
			return nil, err
		}
		if storedChecksum != wal.calculateChecksum(entry) {
			return nil, fmt.Errorf("WAL entry checksum mismatch at timestamp %d: corrupted segment", timestamp)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (wal *WriteAheadLog) writeString(writer *bufio.Writer, s string) error {
	if err := binary.Write(writer, binary.BigEndian, int32(len(s))); err != nil {
		return err
	}
	_, err := writer.WriteString(s)
	return err
}

func (wal *WriteAheadLog) readString(reader *bufio.Reader) (string, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func (wal *WriteAheadLog) skipEntry(reader *bufio.Reader) error {
	// Skip operation
	if _, err := wal.readString(reader); err != nil {
		return err
	}

	// Skip key
	if _, err := wal.readString(reader); err != nil {
		return err
	}

	// Skip namespace
	if _, err := wal.readString(reader); err != nil {
		return err
	}

	// Skip value
	var valueLen int32
	if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
		return err
	}

	buf := make([]byte, valueLen)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return err
	}

	// Skip checksum
	var checksum uint32
	return binary.Read(reader, binary.BigEndian, &checksum)
}

func (wal *WriteAheadLog) calculateChecksum(entry *WALEntry) uint32 {
	crc := crc32.NewIEEE()
	crc.Write([]byte(entry.Operation))
	crc.Write([]byte(entry.Key))
	crc.Write([]byte(entry.Namespace))
	if val, ok := entry.Value.([]byte); ok {
		crc.Write(val)
	}
	return crc.Sum32()
}

func (wal *WriteAheadLog) rotationLoop() {
	for {
		select {
		case <-wal.stopChan:
			return
		case <-wal.rotationTicker.C:
			if err := wal.Rotate(); err != nil {
				// Log error but continue
				_ = err
			}
		}
	}
}
