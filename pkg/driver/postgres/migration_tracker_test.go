package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestMigrationTracker(t *testing.T) {
	ctx := context.Background()
	pgContainer, connStr := setupPostgresContainer(t)
	defer pgContainer.Terminate(ctx)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skip("Skipping PostgreSQL tests: no connection available")
	}
	defer db.Close()

	// Verify connection works
	if err := db.PingContext(ctx); err != nil {
		t.Skip("Skipping PostgreSQL tests: could not connect")
	}

	// Clean up before testing
	db.ExecContext(ctx, "DROP TABLE IF EXISTS migrations")

	// Create a tracker
	hostID := "test-host-1"
	tracker := NewMigrationTracker(db, hostID)

	t.Run("Initialize", func(t *testing.T) {
		err := tracker.Initialize(ctx)
		assert.NoError(t, err)

		// Verify table exists
		var exists bool
		err = db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM pg_tables 
				WHERE schemaname = 'public' AND tablename = 'migrations'
			)
		`).Scan(&exists)

		assert.NoError(t, err)
		assert.True(t, exists, "migrations table should exist")
	})

	t.Run("RecordPending", func(t *testing.T) {
		err := tracker.RecordPending(ctx, "test_migration_1", "CREATE TABLE test_table (id INT)")
		assert.NoError(t, err)

		// Verify record exists
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations WHERE name = $1", "test_migration_1").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		// Check status
		var status string
		err = db.QueryRowContext(ctx, "SELECT status FROM migrations WHERE name = $1", "test_migration_1").Scan(&status)
		assert.NoError(t, err)
		assert.Equal(t, "pending", status)
	})

	t.Run("RecordComplete", func(t *testing.T) {
		// Record completion with success
		err := tracker.RecordComplete(ctx, "test_migration_1", true, 123, "")
		assert.NoError(t, err)

		var status string
		var success bool
		var executionMs int64

		err = db.QueryRowContext(ctx, `
			SELECT status, success, execution_ms
			FROM migrations WHERE name = $1
		`, "test_migration_1").Scan(&status, &success, &executionMs)

		assert.NoError(t, err)
		assert.Equal(t, "completed", status)
		assert.True(t, success)
		assert.Equal(t, int64(123), executionMs)

		// Record a failed migration
		err = tracker.RecordPending(ctx, "test_migration_2", "INVALID SQL")
		assert.NoError(t, err)

		err = tracker.RecordComplete(ctx, "test_migration_2", false, 45, "Syntax error")
		assert.NoError(t, err)

		var errorMsg string
		err = db.QueryRowContext(ctx, `
			SELECT status, success, error
			FROM migrations WHERE name = $1
		`, "test_migration_2").Scan(&status, &success, &errorMsg)

		assert.NoError(t, err)
		assert.Equal(t, "completed", status)
		assert.False(t, success)
		assert.Equal(t, "Syntax error", errorMsg)
	})

	t.Run("GetExecutedMigrations", func(t *testing.T) {
		// Add another migration to ensure ordering works
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps

		err := tracker.RecordPending(ctx, "test_migration_3", "ALTER TABLE test")
		assert.NoError(t, err)

		err = tracker.RecordComplete(ctx, "test_migration_3", true, 55, "")
		assert.NoError(t, err)

		// Get all migrations
		migrations, err := tracker.GetExecutedMigrations(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(migrations))

		// Check order and content
		assert.Equal(t, "test_migration_1", migrations[0].Name)
		assert.Equal(t, "test_migration_2", migrations[1].Name)
		assert.Equal(t, "test_migration_3", migrations[2].Name)

		// Verify IDs are set correctly
		assert.NotEmpty(t, migrations[0].ID)
		assert.NotEmpty(t, migrations[1].ID)
		assert.NotEmpty(t, migrations[2].ID)

		// Check statuses
		assert.Equal(t, "completed", migrations[0].Status)
		assert.Equal(t, true, migrations[0].Success)

		assert.Equal(t, "completed", migrations[1].Status)
		assert.Equal(t, false, migrations[1].Success)
		assert.Equal(t, "Syntax error", migrations[1].Error)

		assert.Equal(t, "completed", migrations[2].Status)
		assert.Equal(t, true, migrations[2].Success)
	})

	t.Run("Transaction", func(t *testing.T) {
		// Test transaction behavior
		tx, err := db.BeginTx(ctx, nil)
		assert.NoError(t, err)

		// Create tracker with transaction
		txTracker := tracker.WithTransaction(tx)

		// Record a migration in the transaction
		err = txTracker.RecordPending(ctx, "transaction_migration", "INSIDE TRANSACTION")
		assert.NoError(t, err)

		// Should not be visible outside transaction
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations WHERE name = $1", "transaction_migration").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)

		// Complete inside transaction
		err = txTracker.RecordComplete(ctx, "transaction_migration", true, 99, "")
		assert.NoError(t, err)

		// Still not visible
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations WHERE name = $1", "transaction_migration").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)

		// Commit transaction
		err = tx.Commit()
		assert.NoError(t, err)

		// Now should be visible
		var migrations []driver.MigrationRecord
		migrations, err = tracker.GetExecutedMigrations(ctx)
		assert.NoError(t, err)

		found := false
		for _, m := range migrations {
			if m.Name == "transaction_migration" {
				found = true
				assert.Equal(t, "completed", m.Status)
				assert.Equal(t, true, m.Success)
				assert.Equal(t, int64(99), m.ExecutionMs)
			}
		}
		assert.True(t, found, "transaction_migration should be in the results")
	})
}
