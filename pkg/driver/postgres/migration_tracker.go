package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
)

// MigrationTracker implements driver.MigrationTracker for PostgreSQL
type MigrationTracker struct {
	db     *sql.DB
	tx     *sql.Tx
	hostID string
}

// NewMigrationTracker creates a new PostgreSQL migration tracker
func NewMigrationTracker(db *sql.DB, hostID string) *MigrationTracker {
	return &MigrationTracker{
		db:     db,
		hostID: hostID,
	}
}

// WithTransaction returns a tracker that uses the provided transaction
func (t *MigrationTracker) WithTransaction(tx *sql.Tx) *MigrationTracker {
	return &MigrationTracker{
		db:     t.db,
		tx:     tx,
		hostID: t.hostID,
	}
}

// Initialize creates the migrations table if it doesn't exist
func (t *MigrationTracker) Initialize(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS migrations (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        content TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'completed',
        started_at TIMESTAMP WITH TIME ZONE,
        executed_at TIMESTAMP WITH TIME ZONE,
        execution_ms BIGINT,
        success BOOLEAN NOT NULL DEFAULT TRUE,
        error TEXT,
        host_id TEXT
    )`

	var err error
	if t.tx != nil {
		_, err = t.tx.ExecContext(ctx, query)
	} else {
		_, err = t.db.ExecContext(ctx, query)
	}

	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	return nil
}

// RecordPending marks a migration as pending
func (t *MigrationTracker) RecordPending(ctx context.Context, name, content string) error {
	query := `
        INSERT INTO migrations
            (name, content, status, started_at, host_id)
        VALUES
            ($1, $2, 'pending', $3, $4)
        ON CONFLICT (name) DO UPDATE SET
            content = EXCLUDED.content,
            status = 'pending',
            started_at = EXCLUDED.started_at,
            host_id = EXCLUDED.host_id
        WHERE
            migrations.status != 'completed' OR migrations.success = false
    `

	startTime := time.Now()
	var err error

	if t.tx != nil {
		_, err = t.tx.ExecContext(ctx, query, name, content, startTime, t.hostID)
	} else {
		_, err = t.db.ExecContext(ctx, query, name, content, startTime, t.hostID)
	}

	if err != nil {
		return fmt.Errorf("failed to record pending migration: %w", err)
	}

	return nil
}

// RecordComplete updates a migration record as completed
func (t *MigrationTracker) RecordComplete(ctx context.Context, name string, success bool, executionMs int64, errorMsg string) error {
	endTime := time.Now()

	query := `
        UPDATE migrations SET
            status = 'completed',
            executed_at = $1,
            execution_ms = $2,
            success = $3,
            error = $4
        WHERE name = $5
    `

	var err error
	if t.tx != nil {
		_, err = t.tx.ExecContext(ctx, query, endTime, executionMs, success, errorMsg, name)
	} else {
		_, err = t.db.ExecContext(ctx, query, endTime, executionMs, success, errorMsg, name)
	}

	if err != nil {
		return fmt.Errorf("failed to record migration result: %w", err)
	}

	return nil
}

// AcquireLock attempts to acquire a distributed lock for migrations
func (d *MigrationTracker) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, string, error) {
	slog.Debug("[PostgreSQL] Driver %s attempting to acquire lock with timeout %v", d.hostID, lockTimeout)

	// Create the advisory lock table if it doesn't exist
	_, err := d.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS migration_locks (
            id INT PRIMARY KEY,
            locked_at TIMESTAMP WITH TIME ZONE,
            locked_by TEXT
        )
    `)
	if err != nil {
		return false, "", fmt.Errorf("failed to create lock table: %w", err)
	}

	// Try to get exclusive lock
	result, err := d.db.ExecContext(ctx, `
        INSERT INTO migration_locks (id, locked_at, locked_by)
        VALUES (1, NOW(), $1)
        ON CONFLICT (id) DO UPDATE
        SET locked_at = NOW(), locked_by = $1
        WHERE (migration_locks.locked_at + $2::INTERVAL < NOW())
            OR migration_locks.locked_by = $1
    `, d.hostID, fmt.Sprintf("%d seconds", int(lockTimeout.Seconds())))

	if err != nil {
		return false, "", fmt.Errorf("failed to acquire lock: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, "", fmt.Errorf("failed to get rows affected: %w", err)
	}

	// If rows affected is 1, we got the lock
	lockAcquired := rows == 1
	slog.Debug("[PostgreSQL] Driver %s %s lock", d.hostID,
		map[bool]string{true: "acquired", false: "failed to acquire"}[lockAcquired])

	return lockAcquired, "1", nil
}

// ReleaseLock releases the distributed lock
func (d *MigrationTracker) ReleaseLock(ctx context.Context, lockID string) error {
	slog.Debug("[PostgreSQL] Driver %s releasing lock", slog.String("hostID", d.hostID))

	// Release our lock only if we hold it
	_, err := d.db.ExecContext(ctx, `
        DELETE FROM migration_locks
        WHERE id = $1 AND locked_by = $2
    `, lockID, d.hostID)

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	slog.Debug("[PostgreSQL] Driver released lock", slog.String("hostID", d.hostID))
	return nil
}

// GetExecutedMigrations returns all migration records
func (t *MigrationTracker) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	query := `
        SELECT
            id, name, content, status, started_at, executed_at, execution_ms, success, error, host_id
        FROM migrations
        ORDER BY executed_at ASC NULLS FIRST, id ASC
    `

	var rows *sql.Rows
	var err error

	if t.tx != nil {
		rows, err = t.tx.QueryContext(ctx, query)
	} else {
		rows, err = t.db.QueryContext(ctx, query)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}
	defer rows.Close()

	var migrations []driver.MigrationRecord
	for rows.Next() {
		var m driver.MigrationRecord
		var id sql.NullInt64
		var executedAt sql.NullTime
		var startedAt sql.NullTime
		var executionMs sql.NullInt64
		var success sql.NullBool
		var errorMsg sql.NullString
		var hostID sql.NullString
		var status sql.NullString

		err := rows.Scan(
			&id,
			&m.Name,
			&m.Content,
			&status,
			&startedAt,
			&executedAt,
			&executionMs,
			&success,
			&errorMsg,
			&hostID,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan migration record: %w", err)
		}

		if id.Valid {
			// Convert int64 to string for the ID field
			m.ID = fmt.Sprintf("%d", id.Int64)
		}

		if status.Valid {
			m.Status = status.String
		}

		if executedAt.Valid {
			m.ExecutedAt = executedAt.Time
		}

		if startedAt.Valid {
			m.StartedAt = startedAt.Time
		}

		if executionMs.Valid {
			m.ExecutionMs = executionMs.Int64
		}

		if success.Valid {
			m.Success = success.Bool
		}

		if errorMsg.Valid {
			m.Error = errorMsg.String
		}

		if hostID.Valid {
			m.HostID = hostID.String
		}

		migrations = append(migrations, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return migrations, nil
}
