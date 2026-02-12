package costmodel

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func newTestAllocationSet(start, end time.Time) *opencost.AllocationSet {
	as := opencost.NewAllocationSet(start, end)
	alloc := &opencost.Allocation{
		Name: "test-cluster/test-node/test-ns/test-pod/test-container",
		Properties: &opencost.AllocationProperties{
			Cluster:   "test-cluster",
			Node:      "test-node",
			Namespace: "test-ns",
			Pod:       "test-pod",
			Container: "test-container",
		},
		Window:       opencost.NewWindow(&start, &end),
		Start:        start,
		End:          end,
		CPUCoreHours: 2.5,
		CPUCost:      0.05,
		RAMByteHours: 1024 * 1024 * 1024,
		RAMCost:      0.02,
	}
	as.Set(alloc)
	return as
}

func completedDay(daysAgo int) (time.Time, time.Time) {
	now := time.Now().UTC()
	end := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysAgo+1)
	start := end.AddDate(0, 0, -1)
	return start, end
}

func TestAllocationCache_PutGet(t *testing.T) {
	dir := t.TempDir()
	ac := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac.Stop()

	start, end := completedDay(3)
	as := newTestAllocationSet(start, end)

	// Put should succeed for a completed day
	err := ac.Put(as)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get should return the cached data
	got, ok := ac.Get(start, end)
	if !ok {
		t.Fatal("Get returned false for cached day")
	}
	if got == nil {
		t.Fatal("Get returned nil AllocationSet")
	}

	if len(got.Allocations) != len(as.Allocations) {
		t.Errorf("expected %d allocations, got %d", len(as.Allocations), len(got.Allocations))
	}

	for key, expected := range as.Allocations {
		actual, exists := got.Allocations[key]
		if !exists {
			t.Errorf("missing allocation key %s", key)
			continue
		}
		if actual.CPUCoreHours != expected.CPUCoreHours {
			t.Errorf("CPUCoreHours mismatch: got %f, want %f", actual.CPUCoreHours, expected.CPUCoreHours)
		}
		if actual.CPUCost != expected.CPUCost {
			t.Errorf("CPUCost mismatch: got %f, want %f", actual.CPUCost, expected.CPUCost)
		}
		if actual.RAMByteHours != expected.RAMByteHours {
			t.Errorf("RAMByteHours mismatch: got %f, want %f", actual.RAMByteHours, expected.RAMByteHours)
		}
	}
}

func TestAllocationCache_GetMiss(t *testing.T) {
	dir := t.TempDir()
	ac := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac.Stop()

	start, end := completedDay(5)

	_, ok := ac.Get(start, end)
	if ok {
		t.Fatal("Get should return false for uncached day")
	}
}

func TestAllocationCache_SkipsCurrentDay(t *testing.T) {
	dir := t.TempDir()
	ac := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac.Stop()

	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	as := newTestAllocationSet(start, end)
	err := ac.Put(as)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should not cache current/future day
	_, ok := ac.Get(start, end)
	if ok {
		t.Fatal("should not cache current or future day")
	}
}

func TestAllocationCache_SkipsNonDayAligned(t *testing.T) {
	dir := t.TempDir()
	ac := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac.Stop()

	// Non-day-aligned window
	start := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	as := newTestAllocationSet(start, end)
	err := ac.Put(as)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	_, ok := ac.Get(start, end)
	if ok {
		t.Fatal("should not cache non-day-aligned windows")
	}
}

func TestAllocationCache_Load(t *testing.T) {
	dir := t.TempDir()

	start, end := completedDay(5)
	as := newTestAllocationSet(start, end)

	// Write with one cache instance
	ac1 := NewAllocationCache(dir, 90*24*time.Hour)
	err := ac1.Put(as)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	ac1.Stop()

	// Load with a fresh instance
	ac2 := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac2.Stop()

	err = ac2.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	got, ok := ac2.Get(start, end)
	if !ok {
		t.Fatal("Get returned false after Load")
	}
	if len(got.Allocations) != 1 {
		t.Errorf("expected 1 allocation after Load, got %d", len(got.Allocations))
	}
}

func TestAllocationCache_Expire(t *testing.T) {
	dir := t.TempDir()
	ac := NewAllocationCache(dir, 90*24*time.Hour)
	defer ac.Stop()

	// Cache two days
	s1, e1 := completedDay(10)
	s2, e2 := completedDay(3)

	as1 := newTestAllocationSet(s1, e1)
	as2 := newTestAllocationSet(s2, e2)

	if err := ac.Put(as1); err != nil {
		t.Fatalf("Put as1 failed: %v", err)
	}
	if err := ac.Put(as2); err != nil {
		t.Fatalf("Put as2 failed: %v", err)
	}

	// Expire entries older than 5 days ago
	cutoff := time.Now().UTC().AddDate(0, 0, -5)
	if err := ac.Expire(cutoff); err != nil {
		t.Fatalf("Expire failed: %v", err)
	}

	// Old entry should be gone
	_, ok := ac.Get(s1, e1)
	if ok {
		t.Fatal("expired entry should not be returned")
	}

	// Recent entry should remain
	_, ok = ac.Get(s2, e2)
	if !ok {
		t.Fatal("recent entry should still be cached")
	}

	// Verify disk file was removed
	key := s1.UTC().Format(allocationCacheDateFormat)
	filename := key + allocationCacheExtension
	path := filepath.Join(dir, filename)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expired cache file should be removed from disk")
	}
}

func TestIsCompletedDay(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected bool
	}{
		{
			name:     "completed day 3 days ago",
			start:    time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -3),
			end:      time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2),
			expected: true,
		},
		{
			name:     "today - not completed",
			start:    time.Now().UTC().Truncate(24 * time.Hour),
			end:      time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1),
			expected: false,
		},
		{
			name:     "not day-aligned start",
			start:    time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 16, 6, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name:     "not 24 hours",
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCompletedDay(tt.start, tt.end)
			if got != tt.expected {
				t.Errorf("isCompletedDay(%v, %v) = %v, want %v", tt.start, tt.end, got, tt.expected)
			}
		})
	}
}
