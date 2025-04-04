// pkg/driver/postgres/postgres_driver.go
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"log"
	"time"
)

// PostgresDriver implements the driver.Driver interface for PostgreSQL
type PostgresDriver struct {
	db      *sql.DB
	hostID  string
	lockID  string
	tracker *MigrationTracker
}

// New creates a new PostgreSQL driver
func New(db *sql.DB, dbName string) *PostgresDriver {
	hostID := fmt.Sprintf("postgres-%s-%d", dbName, time.Now().UnixNano())
	d := &PostgresDriver{
		db:     db,
		hostID: hostID,
	}
	d.tracker = NewMigrationTracker(db, hostID)
	return d
}

// GetTracker returns the migration tracker
func (d *PostgresDriver) GetTracker() driver.MigrationTracker {
	return d.tracker
}

// ExecuteMigration executes a migration within a transaction
func (d *PostgresDriver) ExecuteMigration(ctx context.Context, name, content string) error {
	// Start a transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Create a transaction-aware tracker
	txTracker := d.tracker.WithTransaction(tx)

	// Record migration as pending
	startTime := time.Now()
	if err := txTracker.RecordPending(ctx, name, content); err != nil {
		tx.Rollback()
		return err
	}

	// Execute the migration
	var execErr error
	if _, err := tx.ExecContext(ctx, content); err != nil {
		execErr = err
		tx.Rollback()
	}

	// Calculate execution time
	endTime := time.Now()
	executionMs := endTime.Sub(startTime).Milliseconds()

	// If there was an execution error, record the failure
	if execErr != nil {
		// Start a new transaction since the previous one was rolled back
		errorTx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin error transaction: %w", err)
		}
		defer errorTx.Rollback()

		errorTracker := d.tracker.WithTransaction(errorTx)
		if err := errorTracker.RecordComplete(ctx, name, false, executionMs, execErr.Error()); err != nil {
			log.Printf("Failed to record migration error: %v", err)
			return fmt.Errorf("migration execution failed: %w", execErr)
		}

		if err := errorTx.Commit(); err != nil {
			return fmt.Errorf("failed to commit error transaction: %w", err)
		}

		return fmt.Errorf("migration execution failed: %w", execErr)
	}

	// Record successful migration
	if err := txTracker.RecordComplete(ctx, name, true, executionMs, ""); err != nil {
		tx.Rollback()
		return err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration transaction: %w", err)
	}

	log.Printf("[PostgreSQL] Migration %s succeeded in %d ms", name, executionMs)
	return nil
}

// Initialize Implement other driver interface methods...
func (d *PostgresDriver) Initialize(ctx context.Context) error {
	return d.tracker.Initialize(ctx)
}

func (d *PostgresDriver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	return d.tracker.GetExecutedMigrations(ctx)
}

// AcquireLock attempts to acquire a distributed lock for migrations
func (d *PostgresDriver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	var suc bool
	var err error
	suc, d.lockID, err = d.tracker.AcquireLock(ctx, lockTimeout)
	return suc, err
}

// ReleaseLock releases the distributed lock
func (d *PostgresDriver) ReleaseLock(ctx context.Context) error {
	return d.tracker.ReleaseLock(ctx, d.lockID)
}

// Close cleans up resources
func (d *PostgresDriver) Close() error {
	log.Printf("[PostgreSQL] Driver %s closing", d.hostID)
	// Nothing special to do for postgres driver, db connection is closed by caller
	return nil
}
