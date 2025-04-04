package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupPostgresContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()

	// Start Postgres container
	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:13",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_USER":     "test",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	assert.NoError(t, err, "Failed to start container")

	// Get connection details
	host, err := pgContainer.Host(ctx)
	assert.NoError(t, err, "Failed to get host")

	port, err := pgContainer.MappedPort(ctx, "5432")
	assert.NoError(t, err, "Failed to get port")

	// Create connection string
	connStr := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable",
		host, port.Port())

	return pgContainer, connStr
}

func TestPostgresDriver(t *testing.T) {
	ctx := context.Background()
	pgContainer, connStr := setupPostgresContainer(t)
	defer pgContainer.Terminate(ctx)

	// Create test migrations
	migrationDir := "./migrations"

	// Create driver - updated to use the new pattern
	db, err := sql.Open("postgres", connStr)
	assert.NoError(t, err, "Failed to open database connection")
	defer db.Close()

	// Use a unique host ID for testing
	hostID := "test-host-" + time.Now().Format(time.RFC3339)
	driver := New(db, hostID)

	// Initialize schema
	err = driver.Initialize(ctx)
	assert.NoError(t, err, "Failed to initialize schema")

	// Run migrations
	m := migrator.New(driver, migrationDir, migrator.WithName("basic"))
	err = m.Run(ctx)
	assert.NoError(t, err, "Migration failed")

	// Check migrations were recorded
	executed, err := driver.GetExecutedMigrations(ctx)
	assert.NoError(t, err, "Failed to get migrations")
	assert.Equal(t, 4, len(executed), "Wrong number of executed migrations")

	// Verify data was inserted
	var count int
	err = driver.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err, "Query failed")
	assert.Equal(t, 5, count, "Wrong number of users inserted")
}

func TestConcurrentMigrations(t *testing.T) {
	ctx := context.Background()
	pgContainer, connStr := setupPostgresContainer(t)
	defer pgContainer.Terminate(ctx)

	// Migrations directory
	migrationDir := "./migrations"

	// Initialize a base driver to create schema
	baseDb, err := sql.Open("postgres", connStr)
	assert.NoError(t, err, "Failed to open database connection")
	defer baseDb.Close()

	baseHostID := "base-host-" + time.Now().Format(time.RFC3339)
	baseDriver := New(baseDb, baseHostID)

	err = baseDriver.Initialize(ctx)
	assert.NoError(t, err, "Failed to initialize schema")

	// Run concurrent migrations
	const numConcurrent = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numConcurrent)
	successCount := &atomic.Int32{}

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			t.Logf("Starting migration attempt %d", id)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				errCh <- fmt.Errorf("db connection %d: %w", id, err)
				return
			}
			defer db.Close()

			hostID := fmt.Sprintf("concurrent-host-%d-%s", id, time.Now().Format(time.RFC3339))
			driver := New(db, hostID)

			// Configure the driver - you'll need to add these methods to your driver if needed
			// driver.SetLockTimeout(lockTimeout)
			// driver.SetWaitTimeout(waitTimeout)

			// Introduce a small delay to ensure different start times
			time.Sleep(time.Duration(id*100) * time.Millisecond)

			m := migrator.New(driver, migrationDir, migrator.WithName(fmt.Sprintf("migration-%d", id)))
			err = m.Run(ctx)

			if err != nil {
				// This is NOT expected with the wait/retry implementation
				errCh <- fmt.Errorf("migrator %d: %w", id, err)
				return
			}

			t.Logf("Migration %d successfully completed", id)
			successCount.Add(1)
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errCh)

	// Check for errors
	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}
	assert.Equal(t, 0, len(errors), fmt.Sprintf("Got errors during concurrent migrations: %v", errors))

	// All concurrent migrations should have completed successfully, but the migrations
	// should only have been executed once (first process executes, others wait and skip)
	assert.Equal(t, int32(numConcurrent), successCount.Load(),
		"Expected all processes to complete successfully")

	// Verify migrations ran successfully
	finalDb, err := sql.Open("postgres", connStr)
	assert.NoError(t, err, "Failed to open database connection")
	defer finalDb.Close()

	finalDriver := New(finalDb, "verification-host")

	// All migrations should have been executed exactly once
	executed, err := finalDriver.GetExecutedMigrations(ctx)
	assert.NoError(t, err, "Failed to get migrations")
	assert.Equal(t, 4, len(executed), "Expected exactly 4 migrations")

	// Verify table exists
	var tableCount int
	err = finalDriver.db.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'users'
    `).Scan(&tableCount)
	assert.NoError(t, err, "Table verification failed")
	assert.Equal(t, 1, tableCount, "Users table should exist")

	// Check if records were inserted
	var userCount int
	err = finalDriver.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	assert.NoError(t, err, "Failed to count users")
	assert.Equal(t, 5, userCount, "Expected exactly 5 users")
}
