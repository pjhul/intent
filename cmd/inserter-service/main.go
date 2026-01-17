package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
	"github.com/pjhul/intent/internal/inserter"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Load configuration
	cfg, err := inserter.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize ClickHouse client
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %v", err)
	}
	defer chClient.Close()

	// Create and start service
	service := inserter.NewService(cfg, chClient)

	// Handle shutdown signals
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("received shutdown signal")
		cancel()

		// Give the service time to flush
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := service.Stop(shutdownCtx); err != nil {
			log.Printf("error during shutdown: %v", err)
		}
	}()

	// Start the service (blocks until context is cancelled)
	if err := service.Start(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("service error: %v", err)
	}

	log.Println("service stopped")
}
