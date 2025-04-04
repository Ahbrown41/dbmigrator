package driver

import (
	"context"
	"time"
)

// MigrationRecord represents a record of an executed migration
type MigrationRecord struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Content     string    `json:"content"`
	Status      string    `json:"status"`
	StartedAt   time.Time `json:"started_at"`
	ExecutedAt  time.Time `json:"executed_at"`
	ExecutionMs int64     `json:"execution_ms"`
	Error       string    `json:"error,omitempty"`
	Success     bool      `json:"success"`
	HostID      string    `json:"host_id"`
}

// MigrationLock represents a distributed lock for migrations
type MigrationLock struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	HostID    string    `json:"host_id"`
}

// Driver is the interface that must be implemented by database drivers
type Driver interface {
	// Initialize prepares the database for migrations
	Initialize(ctx context.Context) error

	// AcquireLock attempts to acquire a distributed lock for migrations
	AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error)

	// ReleaseLock releases the distributed lock
	ReleaseLock(ctx context.Context) error

	// ExecuteMigration executes a migration within a transaction
	ExecuteMigration(ctx context.Context, name, content string) error

	// GetExecutedMigrations returns a list of already executed migrations
	GetExecutedMigrations(ctx context.Context) ([]MigrationRecord, error)

	// Close cleans up resources
	Close() error
}
