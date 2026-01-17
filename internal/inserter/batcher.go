package inserter

import (
	"context"
	"log"
	"sync"
	"time"
)

// FlushFunc is called when the batch is ready to be flushed
type FlushFunc[T any] func(ctx context.Context, items []T) error

// Batcher collects items and flushes them based on size or time
type Batcher[T any] struct {
	maxSize       int
	flushInterval time.Duration
	flushFunc     FlushFunc[T]

	mu      sync.Mutex
	items   []T
	timer   *time.Timer
	stopped bool
}

// NewBatcher creates a new batcher with the given configuration
func NewBatcher[T any](maxSize int, flushInterval time.Duration, flushFunc FlushFunc[T]) *Batcher[T] {
	return &Batcher[T]{
		maxSize:       maxSize,
		flushInterval: flushInterval,
		flushFunc:     flushFunc,
		items:         make([]T, 0, maxSize),
	}
}

// Add adds an item to the batch. If the batch is full, it triggers a flush.
func (b *Batcher[T]) Add(ctx context.Context, item T) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return nil
	}

	b.items = append(b.items, item)

	// Start timer on first item
	if len(b.items) == 1 {
		b.startTimer(ctx)
	}

	// Flush if batch is full
	if len(b.items) >= b.maxSize {
		return b.flushLocked(ctx)
	}

	return nil
}

// Start starts the timer-based flushing goroutine
func (b *Batcher[T]) Start(ctx context.Context) {
	// Timer is started lazily when first item is added
}

// Flush forces a flush of pending items
func (b *Batcher[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked(ctx)
}

// Stop stops the batcher and performs a final flush
func (b *Batcher[T]) Stop(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stopped = true
	if b.timer != nil {
		b.timer.Stop()
	}

	return b.flushLocked(ctx)
}

// flushLocked flushes the batch while holding the lock
func (b *Batcher[T]) flushLocked(ctx context.Context) error {
	if len(b.items) == 0 {
		return nil
	}

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	// Take current items
	items := b.items
	b.items = make([]T, 0, b.maxSize)

	// Call flush function
	if err := b.flushFunc(ctx, items); err != nil {
		log.Printf("flush error: %v", err)
		return err
	}

	log.Printf("flushed %d items", len(items))
	return nil
}

// startTimer starts the flush timer
func (b *Batcher[T]) startTimer(ctx context.Context) {
	if b.timer != nil {
		b.timer.Stop()
	}

	b.timer = time.AfterFunc(b.flushInterval, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if b.stopped {
			return
		}

		if err := b.flushLocked(ctx); err != nil {
			log.Printf("timer flush error: %v", err)
		}
	})
}
