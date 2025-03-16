package migrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
)

const (
	// DefaultLockTimeout is the default time a lock is held
	DefaultLockTimeout = 5 * time.Minute

	// DefaultLockRetryInterval is the default time to wait between lock attempts
	DefaultLockRetryInterval = 1 * time.Second

	// DefaultMaxLockRetries is the default number of times to retry acquiring a lock
	DefaultMaxLockRetries = 60
)

// Migrator handles the migration process
type Migrator struct {
	driver            driver.Driver
	dir               string
	lockTimeout       time.Duration
	lockRetryInterval time.Duration
	maxLockRetries    int
}

// MigratorOption is a function that configures a Migrator
type MigratorOption func(*Migrator)

// WithLockTimeout sets the lock timeout
func WithLockTimeout(timeout time.Duration) MigratorOption {
	return func(m *Migrator) {
		m.lockTimeout = timeout
	}
}

// WithLockRetryInterval sets the lock retry interval
func WithLockRetryInterval(interval time.Duration) MigratorOption {
	return func(m *Migrator) {
		m.lockRetryInterval = interval
	}
}

// WithMaxLockRetries sets the maximum number of lock retries
func WithMaxLockRetries(retries int) MigratorOption {
	return func(m *Migrator) {
		m.maxLockRetries = retries
	}
}

// New creates a new Migrator
func New(driver driver.Driver, migrationsDir string, opts ...MigratorOption) *Migrator {
	m := &Migrator{
		driver:            driver,
		dir:               migrationsDir,
		lockTimeout:       DefaultLockTimeout,
		lockRetryInterval: DefaultLockRetryInterval,
		maxLockRetries:    DefaultMaxLockRetries,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Run executes all pending migrations
func (m *Migrator) Run(ctx context.Context) error {
	// Initialize the driver (create migration table if needed)
	if err := m.driver.Initialize(ctx); err != nil {
		return fmt.Errorf("initialization failed: %w", err)
	}

	// Acquire lock with retries
	acquired := false
	var err error

	for i := 0; i < m.maxLockRetries; i++ {
		acquired, err = m.driver.AcquireLock(ctx, m.lockTimeout)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}

		if acquired {
			break
		}

		fmt.Printf("Migration lock is held by another process, retrying in %v... (%d/%d)\n",
			m.lockRetryInterval, i+1, m.maxLockRetries)

		select {
		case <-time.After(m.lockRetryInterval):
			// Continue and retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !acquired {
		return fmt.Errorf("failed to acquire migration lock after %d attempts", m.maxLockRetries)
	}

	// Ensure we release the lock when done
	defer func() {
		if err := m.driver.ReleaseLock(ctx); err != nil {
			fmt.Printf("WARNING: Failed to release migration lock: %v\n", err)
		}
	}()

	// Get list of executed migrations
	executed, err := m.driver.GetExecutedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get executed migrations: %w", err)
	}

	// Build a map for quick lookup
	executedMap := make(map[string]bool)
	for _, migration := range executed {
		executedMap[migration.Name] = true
	}

	// Read migration files
	files, err := os.ReadDir(m.dir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Sort files alphabetically
	migrations := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() {
			migrations = append(migrations, file.Name())
		}
	}
	sort.Strings(migrations)

	// Execute migrations that haven't been run
	for _, filename := range migrations {
		if executedMap[filename] {
			fmt.Printf("Skipping migration: %s (already executed)\n", filename)
			continue
		}

		fmt.Printf("Executing migration: %s\n", filename)
		content, err := os.ReadFile(filepath.Join(m.dir, filename))
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", filename, err)
		}

		if err := m.driver.ExecuteMigration(ctx, filename, string(content)); err != nil {
			return fmt.Errorf("migration %s failed: %w", filename, err)
		}
	}

	return nil
}
