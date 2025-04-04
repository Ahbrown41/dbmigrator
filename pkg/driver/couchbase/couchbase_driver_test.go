package couchbase_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/assert"

	"github.com/ahbrown41/dbmigrator/pkg/driver/couchbase"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
)

func TestCouchbaseDriver(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource:   false,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Setup credentials
	const (
		username     = "admin"
		password     = "password"
		bucketName   = "test_bucket"
		scopeName    = "my_scope"
		migrationDir = "./migrations"
	)

	// Get singleton server instance instead of creating a new one
	server, err := GetCouchbaseServer(t, username, password, bucketName)
	require.NoError(t, err, "Failed to get Couchbase server")

	// For any test requiring a clean state, flush the bucket instead of recreating
	err = server.FlushBucket(t)
	require.NoError(t, err)

	// Create the scope and run setup
	cluster, err := gocb.Connect(server.ConnectionString(), gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	require.NoError(t, err)
	defer cluster.Close(nil)

	// Test basic migration functionality
	t.Run("BasicMigration", func(t *testing.T) {
		// Create driver - Changed from NewDriver to New
		driver, err := couchbase.New(cluster, bucketName, scopeName)
		if err != nil {
			t.Fatalf("Failed to create driver: %v", err)
		}

		// Initialize the driver
		if err := driver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize driver: %v", err)
		}

		// Run migrations
		m := migrator.New(driver, migrationDir)
		if err := m.Run(ctx); err != nil {
			t.Fatalf("Migration failed: %v", err)
		}

		// Check migrations were recorded
		executed, err := driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations: %v", err)
		}

		if len(executed) != 8 {
			t.Fatalf("Expected 8 migrations, got %d", len(executed))
		}

		// Verify data was inserted by connecting directly to Couchbase
		cluster, err := gocb.Connect(server.ConnectionString(), gocb.ClusterOptions{
			Username: username,
			Password: password,
		})
		if err != nil {
			t.Fatalf("Failed to connect to Couchbase: %v", err)
		}
		defer cluster.Close(nil)

		// Count users
		userCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.profiles
        `, bucketName, scopeName)

		userResult, err := cluster.Query(userCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count users: %v", err)
		}
		defer userResult.Close()

		var userCount struct {
			Count int `json:"count"`
		}

		if userResult.Next() {
			if err := userResult.Row(&userCount); err != nil {
				t.Fatalf("Failed to parse user count: %v", err)
			}
		}

		if userCount.Count != 1 {
			t.Fatalf("Expected 1 user, got %d", userCount.Count)
		}

		// Count posts
		postCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.posts
        `, bucketName, scopeName)

		postResult, err := cluster.Query(postCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count posts: %v", err)
		}
		defer postResult.Close()

		var postCount struct {
			Count int `json:"count"`
		}

		if postResult.Next() {
			if err := postResult.Row(&postCount); err != nil {
				t.Fatalf("Failed to parse post count: %v", err)
			}
		}

		if postCount.Count != 1 {
			t.Fatalf("Expected 1 post, got %d", postCount.Count)
		}

		// Run migrations again - should be idempotent
		if err := m.Run(ctx); err != nil {
			t.Fatalf("Second migration run failed: %v", err)
		}

		// Verify no additional migrations were executed
		executed, err = driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations after second run: %v", err)
		}

		if len(executed) != 8 {
			t.Fatalf("Expected still 8 migrations after second run, got %d", len(executed))
		}
	})

	// Reset bucket for concurrency test
	resetCluster, err := gocb.Connect(server.ConnectionString(), gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		t.Fatalf("Failed to connect to Couchbase: %v", err)
	}

	// Flush bucket to start fresh
	err = resetCluster.Buckets().FlushBucket(bucketName, nil)
	if err != nil {
		t.Fatalf("Failed to flush bucket: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(10 * time.Second)
	resetCluster.Close(nil)

	// Test concurrent migrations
	t.Run("ConcurrentMigration", func(t *testing.T) {
		// Create multiple migrators to simulate concurrent processes
		const numConcurrent = 5
		var wg sync.WaitGroup

		// Channel to collect errors
		errCh := make(chan error, numConcurrent)
		// Channel to track successful completions
		successCh := make(chan int, numConcurrent)

		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// First, create and initialize a driver to set up the collections and indexes
		setupDriver, err := couchbase.New(cluster, bucketName, scopeName)
		assert.NoError(t, err, "Failed to create setup driver")

		err = setupDriver.Initialize(ctx)
		assert.NoError(t, err, "Failed to initialize setup driver")
		defer setupDriver.Close()

		// Start concurrent migrations
		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(instanceID int) {
				defer wg.Done()

				// Create a new driver for each instance
				driver, err := couchbase.New(cluster, bucketName, scopeName)
				if err != nil {
					errCh <- fmt.Errorf("instance %d: failed to create driver: %w", instanceID, err)
					return
				}

				// Create migrator with lock retry options
				m := migrator.New(
					driver,
					migrationDir,
					migrator.WithLockTimeout(20*time.Second),
					migrator.WithLockRetryInterval(1*time.Second),
					migrator.WithMaxLockRetries(30),
					migrator.WithName(fmt.Sprintf("couchbase migrator-%d", instanceID)),
				)

				// Run migrations - don't treat it as an error if migrations were already processed
				err = m.Run(ctx)
				if err != nil {
					// Check if this is a "migrations already processed" type of error
					if strings.Contains(err.Error(), "already exists") ||
						strings.Contains(err.Error(), "already executed") {
						t.Logf("Instance %d found migrations already processed", instanceID)
						successCh <- instanceID
						return
					}
					errCh <- fmt.Errorf("instance %d: migration failed: %w", instanceID, err)
					return
				}

				t.Logf("Instance %d completed migrations successfully", instanceID)
				successCh <- instanceID
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(errCh)
		close(successCh)

		// Check for errors
		var errors []error
		for err := range errCh {
			errors = append(errors, err)
		}

		// Count successes
		successCount := 0
		for range successCh {
			successCount++
		}

		if len(errors) > 0 {
			hadErrors := false
			for _, err := range errors {
				if !strings.Contains(err.Error(), "failed to acquire lock") {
					t.Fatalf("Concurrent migration error: %v", err)
					hadErrors = true
					continue
				}
			}
			if hadErrors {
				t.Errorf("Concurrent migrations had %d errors", len(errors))
			}
		}

		// Make sure at least one instance succeeded
		assert.Greaterf(t, successCount, 0, "Expected at least one instance to successfully process migrations")
		t.Logf("Successfully completed migrations across %d instances", successCount)

		// Check migrations were recorded exactly once
		executed, err := setupDriver.GetExecutedMigrations(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(executed), "Expected exactly 8 migrations, got %d", len(executed))

		// Connect to verify data
		cluster, err := gocb.Connect(server.ConnectionString(), gocb.ClusterOptions{
			Username: username,
			Password: password,
		})
		assert.NoError(t, err)
		defer func() {
			if err := cluster.Close(nil); err != nil {
				t.Errorf("Failed to close cluster: %v", err)
			}
		}()

		// Count users
		userCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.profiles
        `, bucketName, scopeName)

		userResult, err := cluster.Query(userCountQuery, nil)
		require.NoError(t, err)
		defer userResult.Close()

		var userCount struct {
			Count int `json:"count"`
		}

		if userResult.Next() {
			err := userResult.Row(&userCount)
			assert.NoError(t, err, "Failed to parse user count: %v", err)
		}

		assert.Equal(t, 1, userCount.Count, "Expected at least 1 user, got %d", userCount.Count)

		// Count posts
		postCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.posts
        `, bucketName, scopeName)

		postResult, err := cluster.Query(postCountQuery, nil)
		assert.NoError(t, err)
		defer postResult.Close()

		var postCount struct {
			Count int `json:"count"`
		}

		if postResult.Next() {
			if err := postResult.Row(&postCount); err != nil {
				t.Fatalf("Failed to parse post count: %v", err)
			}
		}

		if postCount.Count != 1 {
			t.Fatalf("Expected exactly 1 post, got %d", postCount.Count)
		}

		// Count comments
		commentCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.comments
        `, bucketName, scopeName)

		commentResult, err := cluster.Query(commentCountQuery, nil)
		assert.NoError(t, err)
		defer commentResult.Close()

		var commentCount struct {
			Count int `json:"count"`
		}

		if commentResult.Next() {
			if err := commentResult.Row(&commentCount); err != nil {
				t.Fatalf("Failed to parse comment count: %v", err)
			}
		}

		if commentCount.Count != 1 {
			t.Fatalf("Expected exactly 1 comment, got %d", commentCount.Count)
		}

		// Check lock collection is empty (all locks released)
		lockCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s.%s.schema_migration_locks
        `, bucketName, scopeName)

		lockResult, err := cluster.Query(lockCountQuery, nil)
		assert.NoError(t, err)
		defer lockResult.Close()

		var lockCount struct {
			Count int `json:"count"`
		}

		if lockResult.Next() {
			if err := lockResult.Row(&lockCount); err != nil {
				t.Fatalf("Failed to parse lock count: %v", err)
			}
		}

		if lockCount.Count != 0 {
			t.Fatalf("Expected 0 locks remaining, got %d", lockCount.Count)
		}
	})
}
