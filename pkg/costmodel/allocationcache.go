package costmodel

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
)

const (
	allocationCacheDateFormat = "2006-01-02"
	allocationCacheExtension  = ".json"
	// completedDayBuffer is the minimum time after a day ends before we consider
	// it safe to cache. This avoids caching partial data from delayed metrics.
	completedDayBuffer = 1 * time.Hour
)

// allocationCacheEntry is a JSON-serializable wrapper for AllocationSet.
type allocationCacheEntry struct {
	Allocations  map[string]*opencost.Allocation `json:"allocations"`
	ExternalKeys map[string]bool                 `json:"externalKeys,omitempty"`
	IdleKeys     map[string]bool                 `json:"idleKeys,omitempty"`
	Window       opencost.Window                 `json:"window"`
	Warnings     []string                        `json:"warnings,omitempty"`
	Errors       []string                        `json:"errors,omitempty"`
}

func entryFromAllocationSet(as *opencost.AllocationSet) *allocationCacheEntry {
	return &allocationCacheEntry{
		Allocations:  as.Allocations,
		ExternalKeys: as.ExternalKeys,
		IdleKeys:     as.IdleKeys,
		Window:       as.Window,
		Warnings:     as.Warnings,
		Errors:       as.Errors,
	}
}

func (e *allocationCacheEntry) toAllocationSet() *opencost.AllocationSet {
	as := &opencost.AllocationSet{
		Allocations:  e.Allocations,
		ExternalKeys: e.ExternalKeys,
		IdleKeys:     e.IdleKeys,
		Window:       e.Window,
		Warnings:     e.Warnings,
		Errors:       e.Errors,
	}
	if as.Allocations == nil {
		as.Allocations = map[string]*opencost.Allocation{}
	}
	if as.ExternalKeys == nil {
		as.ExternalKeys = map[string]bool{}
	}
	if as.IdleKeys == nil {
		as.IdleKeys = map[string]bool{}
	}
	return as
}

// AllocationCache provides a disk-backed cache for completed daily AllocationSets.
type AllocationCache struct {
	store     storage.Storage
	mu        sync.RWMutex
	memory    map[string]*opencost.AllocationSet
	retention time.Duration
	stopCh    chan struct{}
}

// NewAllocationCache creates an AllocationCache backed by FileStorage at baseDir.
// It starts a background goroutine that expires old entries every hour.
func NewAllocationCache(baseDir string, retention time.Duration) *AllocationCache {
	ac := &AllocationCache{
		store:     storage.NewFileStorage(baseDir),
		memory:    make(map[string]*opencost.AllocationSet),
		retention: retention,
		stopCh:    make(chan struct{}),
	}

	go ac.expiryLoop()

	return ac
}

// Load reads all cached JSON files from disk into the in-memory map.
func (ac *AllocationCache) Load() error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	files, err := ac.store.List("")
	if err != nil {
		return fmt.Errorf("listing cache directory: %w", err)
	}

	loaded := 0
	for _, fi := range files {
		if !strings.HasSuffix(fi.Name, allocationCacheExtension) {
			continue
		}

		key := strings.TrimSuffix(fi.Name, allocationCacheExtension)
		as, err := ac.readFromDisk(fi.Name)
		if err != nil {
			log.Warnf("AllocationCache: failed to load %s: %v", fi.Name, err)
			continue
		}
		ac.memory[key] = as
		loaded++
	}

	if loaded > 0 {
		log.Infof("AllocationCache: loaded %d cached allocation sets from disk", loaded)
	}

	return nil
}

// Get returns a cached AllocationSet for the given time range if it exists
// and the range represents a completed day.
func (ac *AllocationCache) Get(start, end time.Time) (*opencost.AllocationSet, bool) {
	if !isCompletedDay(start, end) {
		return nil, false
	}

	key := start.UTC().Format(allocationCacheDateFormat)

	ac.mu.RLock()
	if as, ok := ac.memory[key]; ok {
		ac.mu.RUnlock()
		return as, true
	}
	ac.mu.RUnlock()

	// Try disk as fallback (shouldn't happen after Load, but be safe)
	filename := key + allocationCacheExtension
	as, err := ac.readFromDisk(filename)
	if err != nil {
		return nil, false
	}

	ac.mu.Lock()
	ac.memory[key] = as
	ac.mu.Unlock()

	return as, true
}

// Put caches an AllocationSet to disk and memory if it represents a completed day.
func (ac *AllocationCache) Put(as *opencost.AllocationSet) error {
	if as == nil {
		return nil
	}

	start, end := *as.Window.Start(), *as.Window.End()
	if !isCompletedDay(start, end) {
		return nil
	}

	key := start.UTC().Format(allocationCacheDateFormat)
	filename := key + allocationCacheExtension

	entry := entryFromAllocationSet(as)
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling allocation set for %s: %w", key, err)
	}

	if err := ac.store.Write(filename, data); err != nil {
		return fmt.Errorf("writing cache file %s: %w", filename, err)
	}

	ac.mu.Lock()
	ac.memory[key] = as
	ac.mu.Unlock()

	log.Debugf("AllocationCache: cached allocation set for %s", key)
	return nil
}

// Expire removes cache entries older than the given cutoff time.
func (ac *AllocationCache) Expire(before time.Time) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	for key := range ac.memory {
		t, err := time.Parse(allocationCacheDateFormat, key)
		if err != nil {
			continue
		}
		if t.Before(before) {
			delete(ac.memory, key)
			filename := key + allocationCacheExtension
			if err := ac.store.Remove(filename); err != nil && !storage.IsNotExist(err) {
				log.Warnf("AllocationCache: failed to remove expired cache file %s: %v", filename, err)
			}
		}
	}

	return nil
}

// Stop terminates the background expiry goroutine.
func (ac *AllocationCache) Stop() {
	close(ac.stopCh)
}

func (ac *AllocationCache) readFromDisk(filename string) (*opencost.AllocationSet, error) {
	data, err := ac.store.Read(filename)
	if err != nil {
		return nil, err
	}

	var entry allocationCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("unmarshaling %s: %w", filename, err)
	}

	return entry.toAllocationSet(), nil
}

func (ac *AllocationCache) expiryLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-ac.retention)
			if err := ac.Expire(cutoff); err != nil {
				log.Warnf("AllocationCache: expiry error: %v", err)
			}
		case <-ac.stopCh:
			return
		}
	}
}

// isCompletedDay returns true if start/end represent a full UTC day
// and the day has ended with sufficient buffer time.
func isCompletedDay(start, end time.Time) bool {
	s := start.UTC()
	e := end.UTC()

	// Must be day-aligned
	if s.Hour() != 0 || s.Minute() != 0 || s.Second() != 0 {
		return false
	}
	if e.Hour() != 0 || e.Minute() != 0 || e.Second() != 0 {
		return false
	}

	// Must be exactly 24 hours
	if e.Sub(s) != 24*time.Hour {
		return false
	}

	// End must be sufficiently in the past
	return time.Since(e) >= completedDayBuffer
}
