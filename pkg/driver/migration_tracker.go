package driver

import (
	"context"
	"time"
)

// MigrationTracker abstracts operations on the migrations table
type MigrationTracker interface {
	// Initialize sets up the migrations table if needed
	Initialize(ctx context.Context) error

	// RecordPending marks a migration as pending before execution
	RecordPending(ctx context.Context, name, content string) error

	// RecordComplete updates a migration as completed after execution
	RecordComplete(ctx context.Context, name string, success bool, executionMs int64, errorMsg string) error

	// AcquireLock attempts to acquire a distributed lock for migrations
	AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, string, error)

	// ReleaseLock releases the distributed lock
	ReleaseLock(ctx context.Context, lockID string) error

	// GetExecutedMigrations returns all migration records
	GetExecutedMigrations(ctx context.Context) ([]MigrationRecord, error)
}
