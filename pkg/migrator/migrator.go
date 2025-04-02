package migrator

import (
	"context"
	"fmt"
	"log/slog"
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
	name              string
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

// WithName sets the name of the migrator
func WithName(name string) MigratorOption {
	return func(m *Migrator) {
		m.name = name
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
		slog.Debug("Attempting to acquire migration lock",
			slog.Int("attempt", i+1),
			slog.String("migrator", m.name),
		)
		acquired, err = m.driver.AcquireLock(ctx, m.lockTimeout)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}

		if acquired {
			break
		}

		slog.Debug("Migration lock is held by another process",
			slog.Int("attempt", i+1),
			slog.Duration("interval", m.lockRetryInterval),
			slog.Int("max_retries", m.maxLockRetries),
			slog.String("migrator", m.name),
		)

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
			slog.Warn("WARNING: Failed to release migration lock", slog.String("error", err.Error()), slog.String("migrator", m.name))
		}
	}()

	// Get list of executed migrations
	slog.Debug("Executing migrations", slog.String("migrator", m.name))
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
			slog.Debug("Skipping migration: %s (already executed)", slog.String("filename", filename), slog.String("migrator", m.name))
			continue
		}

		slog.Debug("Executing migration", slog.String("filename", filename), slog.String("migrator", m.name))
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
