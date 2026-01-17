package migrations

import (
	"context"
	"embed"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed clickhouse/*.sql
var clickhouseMigrations embed.FS

//go:embed postgres/*.sql
var postgresMigrations embed.FS

// MigrationRunner handles database migrations
type MigrationRunner struct {
	pgPool *pgxpool.Pool
	chConn driver.Conn
}

// NewMigrationRunner creates a new migration runner
func NewMigrationRunner(pgPool *pgxpool.Pool, chConn driver.Conn) *MigrationRunner {
	return &MigrationRunner{
		pgPool: pgPool,
		chConn: chConn,
	}
}

// RunAll runs all pending migrations
func (r *MigrationRunner) RunAll(ctx context.Context) error {
	if r.pgPool != nil {
		if err := r.runPostgresMigrations(ctx); err != nil {
			return fmt.Errorf("postgres migrations failed: %w", err)
		}
	}

	if r.chConn != nil {
		if err := r.runClickHouseMigrations(ctx); err != nil {
			return fmt.Errorf("clickhouse migrations failed: %w", err)
		}
	}

	return nil
}

func (r *MigrationRunner) runPostgresMigrations(ctx context.Context) error {
	log.Println("Running PostgreSQL migrations...")

	// Create migrations table if not exists
	_, err := r.pgPool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get list of migration files
	entries, err := postgresMigrations.ReadDir("postgres")
	if err != nil {
		return fmt.Errorf("failed to read migrations: %w", err)
	}

	// Sort by filename
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	// Run each migration
	for _, file := range files {
		version := strings.TrimSuffix(file, ".sql")

		// Check if already applied
		var count int
		err := r.pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = $1", version).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check migration status: %w", err)
		}

		if count > 0 {
			log.Printf("  [skip] %s (already applied)", version)
			continue
		}

		// Read and execute migration
		content, err := postgresMigrations.ReadFile("postgres/" + file)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		log.Printf("  [run]  %s", version)

		_, err = r.pgPool.Exec(ctx, string(content))
		if err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", file, err)
		}

		// Record migration
		_, err = r.pgPool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version)
		if err != nil {
			return fmt.Errorf("failed to record migration %s: %w", file, err)
		}
	}

	log.Println("PostgreSQL migrations complete")
	return nil
}

func (r *MigrationRunner) runClickHouseMigrations(ctx context.Context) error {
	log.Println("Running ClickHouse migrations...")

	// Create database if not exists
	if err := r.chConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS cohort"); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create migrations table if not exists
	err := r.chConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cohort.schema_migrations (
			version String,
			applied_at DateTime64(3) DEFAULT now64(3)
		) ENGINE = MergeTree()
		ORDER BY version
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get list of migration files
	entries, err := clickhouseMigrations.ReadDir("clickhouse")
	if err != nil {
		return fmt.Errorf("failed to read migrations: %w", err)
	}

	// Sort by filename
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	// Run each migration
	for _, file := range files {
		version := strings.TrimSuffix(file, ".sql")

		// Check if already applied
		var count uint64
		row := r.chConn.QueryRow(ctx, "SELECT count() FROM cohort.schema_migrations WHERE version = ?", version)
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("failed to check migration status: %w", err)
		}

		if count > 0 {
			log.Printf("  [skip] %s (already applied)", version)
			continue
		}

		// Read migration file
		content, err := clickhouseMigrations.ReadFile("clickhouse/" + file)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		log.Printf("  [run]  %s", version)

		// Execute each statement (split by semicolons)
		statements := splitStatements(string(content))
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := r.chConn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("failed to execute migration %s: %w\nStatement: %s", file, err, stmt)
			}
		}

		// Record migration
		err = r.chConn.Exec(ctx, "INSERT INTO cohort.schema_migrations (version) VALUES (?)", version)
		if err != nil {
			return fmt.Errorf("failed to record migration %s: %w", file, err)
		}
	}

	log.Println("ClickHouse migrations complete")
	return nil
}

func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip comments
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		current.WriteString(line)
		current.WriteString("\n")

		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	// Add any remaining content
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}
