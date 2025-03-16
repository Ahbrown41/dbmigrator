package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresDriver struct {
	db     *sql.DB
	hostID string
	lockID string
}

func NewDriver(connString string) (*PostgresDriver, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		err = db.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close db after ping error: %w", err)
		}
		return nil, err
	}

	// Generate a unique host ID for this instance
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	hostID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	return &PostgresDriver{
		db:     db,
		hostID: hostID,
	}, nil
}

func (d *PostgresDriver) Initialize(ctx context.Context) error {
	// Create migrations table
	_, err := d.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            content TEXT NOT NULL,
            executed_at TIMESTAMP NOT NULL,
            execution_ms BIGINT NOT NULL,
            success BOOLEAN NOT NULL,
            error TEXT
        )
    `)
	if err != nil {
		return err
	}

	// Create locks table
	_, err = d.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migration_locks (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            host_id TEXT NOT NULL
        )
    `)
	return err
}

func (d *PostgresDriver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	// Generate a unique lock ID
	lockID := uuid.New().String()

	now := time.Now()
	expiresAt := now.Add(lockTimeout)

	// First, clean up expired locks
	_, err := d.db.ExecContext(ctx, `
        DELETE FROM schema_migration_locks
        WHERE expires_at < $1
    `, now)
	if err != nil {
		return false, fmt.Errorf("failed to clean up expired locks: %w", err)
	}

	// Try to insert a new lock
	result, err := d.db.ExecContext(ctx, `
        INSERT INTO schema_migration_locks (id, created_at, expires_at, host_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    `, lockID, now, expiresAt, d.hostID)

	if err != nil {
		return false, fmt.Errorf("failed to insert lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		// We got the lock
		d.lockID = lockID
		return true, nil
	}

	// Check if there are any locks
	var count int
	err = d.db.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM schema_migration_locks
    `).Scan(&count)

	if err != nil {
		return false, fmt.Errorf("failed to check existing locks: %w", err)
	}

	// If no locks exist, try again (race condition where locks were deleted between our delete and insert)
	if count == 0 {
		return d.AcquireLock(ctx, lockTimeout)
	}

	// Lock exists and is held by someone else
	return false, nil
}

func (d *PostgresDriver) ReleaseLock(ctx context.Context) error {
	if d.lockID == "" {
		return nil // No lock to release
	}

	_, err := d.db.ExecContext(ctx, `
        DELETE FROM schema_migration_locks
        WHERE id = $1 AND host_id = $2
    `, d.lockID, d.hostID)

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	d.lockID = ""
	return nil
}

func (d *PostgresDriver) ExecuteMigration(ctx context.Context, name, content string) error {
	// Start transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Rollback() // Will be ignored if committed
		if err != nil && err != sql.ErrTxDone {
			fmt.Fprintf(os.Stderr, "Error rolling back transaction: %v\n", err)
		}
	}()

	startTime := time.Now()
	var execErr error

	// Execute the migration
	_, execErr = tx.ExecContext(ctx, content)
	endTime := time.Now()
	executionMs := endTime.Sub(startTime).Milliseconds()

	// Record the migration execution
	var errorMsg string
	if execErr != nil {
		errorMsg = execErr.Error()
	}

	_, err = tx.ExecContext(ctx, `
        INSERT INTO schema_migrations 
        (name, content, executed_at, execution_ms, success, error)
        VALUES ($1, $2, $3, $4, $5, $6)
    `, name, content, endTime, executionMs, execErr == nil, errorMsg)

	if err != nil {
		return err
	}

	if execErr != nil {
		return execErr
	}

	return tx.Commit()
}

func (d *PostgresDriver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	rows, err := d.db.QueryContext(ctx, `
        SELECT name, content, executed_at, execution_ms, success, error
        FROM schema_migrations
        ORDER BY executed_at
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []driver.MigrationRecord
	for rows.Next() {
		var m driver.MigrationRecord
		var executedAt time.Time
		err := rows.Scan(&m.Name, &m.Content, &executedAt, &m.ExecutionMs, &m.Success, &m.Error)
		if err != nil {
			return nil, err
		}
		m.ExecutedAt = executedAt
		migrations = append(migrations, m)
	}

	return migrations, rows.Err()
}

func (d *PostgresDriver) Close() error {
	// Release lock if we have one
	if d.lockID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = d.ReleaseLock(ctx) // Best effort
	}

	return d.db.Close()
}
