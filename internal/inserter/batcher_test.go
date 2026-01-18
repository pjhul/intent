package inserter_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pjhul/intent/internal/inserter"
)

func TestBatcher_Add_BatchSizeTriggerFlush(t *testing.T) {
	ctx := context.Background()
	var flushCount int
	var flushedItems []string

	flushFunc := func(ctx context.Context, items []string) error {
		flushCount++
		flushedItems = append(flushedItems, items...)
		return nil
	}

	batcher := inserter.NewBatcher[string](3, time.Hour, flushFunc)

	// Add items up to maxSize
	if err := batcher.Add(ctx, "item1"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := batcher.Add(ctx, "item2"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Should not have flushed yet (only 2 items, max is 3)
	if flushCount != 0 {
		t.Errorf("flushCount = %d, expected 0 before reaching maxSize", flushCount)
	}

	// Adding third item should trigger flush
	if err := batcher.Add(ctx, "item3"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if flushCount != 1 {
		t.Errorf("flushCount = %d, expected 1 after reaching maxSize", flushCount)
	}

	if len(flushedItems) != 3 {
		t.Errorf("flushedItems length = %d, expected 3", len(flushedItems))
	}

	// Verify all items were flushed
	expected := []string{"item1", "item2", "item3"}
	for i, item := range expected {
		if flushedItems[i] != item {
			t.Errorf("flushedItems[%d] = %q, expected %q", i, flushedItems[i], item)
		}
	}
}

func TestBatcher_Add_TimerFlush(t *testing.T) {
	ctx := context.Background()
	var flushCount atomic.Int32
	var mu sync.Mutex
	var flushedItems []string

	flushFunc := func(ctx context.Context, items []string) error {
		flushCount.Add(1)
		mu.Lock()
		flushedItems = append(flushedItems, items...)
		mu.Unlock()
		return nil
	}

	// Use short interval for testing
	batcher := inserter.NewBatcher[string](100, 50*time.Millisecond, flushFunc)

	// Add one item
	if err := batcher.Add(ctx, "item1"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Should not have flushed immediately
	if flushCount.Load() != 0 {
		t.Errorf("flushCount = %d, expected 0 immediately after add", flushCount.Load())
	}

	// Wait for timer to trigger
	time.Sleep(100 * time.Millisecond)

	if flushCount.Load() != 1 {
		t.Errorf("flushCount = %d, expected 1 after timer", flushCount.Load())
	}

	mu.Lock()
	if len(flushedItems) != 1 || flushedItems[0] != "item1" {
		t.Errorf("flushedItems = %v, expected [item1]", flushedItems)
	}
	mu.Unlock()
}

func TestBatcher_Flush_EmptyBatch(t *testing.T) {
	ctx := context.Background()
	var flushCount int

	flushFunc := func(ctx context.Context, items []string) error {
		flushCount++
		return nil
	}

	batcher := inserter.NewBatcher[string](10, time.Hour, flushFunc)

	// Flush without adding any items
	if err := batcher.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Flush function should not be called for empty batch
	if flushCount != 0 {
		t.Errorf("flushCount = %d, expected 0 for empty batch", flushCount)
	}
}

func TestBatcher_Flush_WithItems(t *testing.T) {
	ctx := context.Background()
	var flushCount int
	var flushedItems []int

	flushFunc := func(ctx context.Context, items []int) error {
		flushCount++
		flushedItems = append(flushedItems, items...)
		return nil
	}

	batcher := inserter.NewBatcher[int](100, time.Hour, flushFunc)

	// Add some items
	batcher.Add(ctx, 1)
	batcher.Add(ctx, 2)
	batcher.Add(ctx, 3)

	// Manually flush
	if err := batcher.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if flushCount != 1 {
		t.Errorf("flushCount = %d, expected 1", flushCount)
	}

	if len(flushedItems) != 3 {
		t.Errorf("flushedItems length = %d, expected 3", len(flushedItems))
	}
}

func TestBatcher_Stop_FinalFlush(t *testing.T) {
	ctx := context.Background()
	var flushCount int
	var flushedItems []string

	flushFunc := func(ctx context.Context, items []string) error {
		flushCount++
		flushedItems = append(flushedItems, items...)
		return nil
	}

	batcher := inserter.NewBatcher[string](100, time.Hour, flushFunc)

	// Add items but don't reach maxSize
	batcher.Add(ctx, "item1")
	batcher.Add(ctx, "item2")

	// Should not have flushed yet
	if flushCount != 0 {
		t.Errorf("flushCount = %d, expected 0 before stop", flushCount)
	}

	// Stop should trigger final flush
	if err := batcher.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if flushCount != 1 {
		t.Errorf("flushCount = %d, expected 1 after stop", flushCount)
	}

	if len(flushedItems) != 2 {
		t.Errorf("flushedItems length = %d, expected 2", len(flushedItems))
	}
}

func TestBatcher_Stop_AlreadyStopped(t *testing.T) {
	ctx := context.Background()
	var flushCount int

	flushFunc := func(ctx context.Context, items []string) error {
		flushCount++
		return nil
	}

	batcher := inserter.NewBatcher[string](100, time.Hour, flushFunc)
	batcher.Add(ctx, "item1")

	// First stop
	batcher.Stop(ctx)
	if flushCount != 1 {
		t.Errorf("flushCount = %d, expected 1 after first stop", flushCount)
	}

	// Second stop should not trigger another flush (batch is empty)
	batcher.Stop(ctx)
	if flushCount != 1 {
		t.Errorf("flushCount = %d, expected 1 after second stop", flushCount)
	}
}

func TestBatcher_Add_AfterStop(t *testing.T) {
	ctx := context.Background()
	var flushedItems []string

	flushFunc := func(ctx context.Context, items []string) error {
		flushedItems = append(flushedItems, items...)
		return nil
	}

	batcher := inserter.NewBatcher[string](100, time.Hour, flushFunc)
	batcher.Add(ctx, "item1")
	batcher.Stop(ctx)

	// Add after stop should be ignored
	batcher.Add(ctx, "item2")

	if len(flushedItems) != 1 {
		t.Errorf("flushedItems length = %d, expected 1 (item2 should be ignored)", len(flushedItems))
	}
}

func TestBatcher_ConcurrentAdd(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var flushedCount int
	var totalItems int

	flushFunc := func(ctx context.Context, items []int) error {
		mu.Lock()
		flushedCount++
		totalItems += len(items)
		mu.Unlock()
		return nil
	}

	batcher := inserter.NewBatcher[int](10, time.Hour, flushFunc)

	// Add items concurrently from multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 10
	itemsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				batcher.Add(ctx, goroutineID*100+j)
			}
		}(i)
	}

	wg.Wait()

	// Stop to flush remaining items
	batcher.Stop(ctx)

	mu.Lock()
	expectedTotal := numGoroutines * itemsPerGoroutine
	if totalItems != expectedTotal {
		t.Errorf("totalItems = %d, expected %d", totalItems, expectedTotal)
	}
	mu.Unlock()
}

func TestBatcher_FlushError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("flush error")

	flushFunc := func(ctx context.Context, items []string) error {
		return expectedErr
	}

	batcher := inserter.NewBatcher[string](2, time.Hour, flushFunc)

	batcher.Add(ctx, "item1")
	err := batcher.Add(ctx, "item2") // Should trigger flush

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestBatcher_FlushError_OnManualFlush(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("manual flush error")

	flushFunc := func(ctx context.Context, items []string) error {
		return expectedErr
	}

	batcher := inserter.NewBatcher[string](100, time.Hour, flushFunc)
	batcher.Add(ctx, "item1")

	err := batcher.Flush(ctx)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestBatcher_FlushError_OnStop(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("stop flush error")

	flushFunc := func(ctx context.Context, items []string) error {
		return expectedErr
	}

	batcher := inserter.NewBatcher[string](100, time.Hour, flushFunc)
	batcher.Add(ctx, "item1")

	err := batcher.Stop(ctx)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestNewBatcher(t *testing.T) {
	flushFunc := func(ctx context.Context, items []string) error {
		return nil
	}

	batcher := inserter.NewBatcher[string](50, 5*time.Second, flushFunc)
	if batcher == nil {
		t.Error("NewBatcher returned nil")
	}
}

func TestBatcher_Start(t *testing.T) {
	ctx := context.Background()
	flushFunc := func(ctx context.Context, items []string) error {
		return nil
	}

	batcher := inserter.NewBatcher[string](50, 5*time.Second, flushFunc)

	// Start is a no-op but should not panic
	batcher.Start(ctx)
}

func TestBatcher_MultipleFlushCycles(t *testing.T) {
	ctx := context.Background()
	var flushCounts []int

	flushFunc := func(ctx context.Context, items []int) error {
		flushCounts = append(flushCounts, len(items))
		return nil
	}

	batcher := inserter.NewBatcher[int](3, time.Hour, flushFunc)

	// First batch
	batcher.Add(ctx, 1)
	batcher.Add(ctx, 2)
	batcher.Add(ctx, 3) // Triggers flush

	// Second batch
	batcher.Add(ctx, 4)
	batcher.Add(ctx, 5)
	batcher.Add(ctx, 6) // Triggers flush

	// Third batch (partial)
	batcher.Add(ctx, 7)
	batcher.Stop(ctx) // Triggers final flush

	if len(flushCounts) != 3 {
		t.Errorf("flushCounts length = %d, expected 3", len(flushCounts))
	}

	expected := []int{3, 3, 1}
	for i, count := range expected {
		if i < len(flushCounts) && flushCounts[i] != count {
			t.Errorf("flushCounts[%d] = %d, expected %d", i, flushCounts[i], count)
		}
	}
}

func TestBatcher_TimerResetOnBatchSizeFlush(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var flushCounts int

	flushFunc := func(ctx context.Context, items []int) error {
		mu.Lock()
		flushCounts++
		mu.Unlock()
		return nil
	}

	// Short interval, small batch size
	batcher := inserter.NewBatcher[int](2, 50*time.Millisecond, flushFunc)

	// Add two items to trigger size-based flush
	batcher.Add(ctx, 1)
	batcher.Add(ctx, 2)

	mu.Lock()
	if flushCounts != 1 {
		t.Errorf("flushCounts = %d, expected 1 after batch size trigger", flushCounts)
	}
	mu.Unlock()

	// Add one more item, wait for timer
	batcher.Add(ctx, 3)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if flushCounts != 2 {
		t.Errorf("flushCounts = %d, expected 2 after timer", flushCounts)
	}
	mu.Unlock()

	batcher.Stop(ctx)
}

func TestBatcher_TimerStoppedOnManualFlush(t *testing.T) {
	ctx := context.Background()
	var flushCount atomic.Int32

	flushFunc := func(ctx context.Context, items []int) error {
		flushCount.Add(1)
		return nil
	}

	batcher := inserter.NewBatcher[int](100, 100*time.Millisecond, flushFunc)

	// Add item to start timer
	batcher.Add(ctx, 1)

	// Manual flush should stop timer
	batcher.Flush(ctx)

	if flushCount.Load() != 1 {
		t.Errorf("flushCount = %d, expected 1 after manual flush", flushCount.Load())
	}

	// Wait past timer interval
	time.Sleep(150 * time.Millisecond)

	// Should still be 1 since batch is empty
	if flushCount.Load() != 1 {
		t.Errorf("flushCount = %d, expected 1 (no timer flush on empty batch)", flushCount.Load())
	}
}
