package couchbase_test

import (
	"context"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver/couchbase"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/ahbrown41/dbmigrator/pkg/migrator"
)

func TestCouchbaseDriver(t *testing.T) {
	// Skip if running in CI environment without Docker
	if os.Getenv("CI") != "" && os.Getenv("DOCKER_AVAILABLE") != "true" {
		t.Skip("Skipping Couchbase test in CI environment without Docker")
	}

	ctx := context.Background()

	// Setup credentials
	username := "Administrator"
	password := "password"
	bucketName := "testbucket"

	// Start Couchbase container
	server := NewCouchbaseServer(username, password, bucketName)
	server.Start(t)
	defer server.Stop(t)

	// Create temp migration dir
	migrationDir, err := os.MkdirTemp("", "couchbase-migrations")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(migrationDir)

	// Create test migrations
	migrations := map[string]string{
		"000_create_primary_index.n1ql": `
            CREATE PRIMARY INDEX ON ${BUCKET};
        `,
		"001_create_users_index.n1ql": `
            CREATE INDEX idx_users_email ON ${BUCKET}(email) WHERE type = "user";
        `,
		"002_add_user.n1ql": `
            INSERT INTO ${BUCKET} (KEY, VALUE)
            VALUES ("user::1", {
                "type": "user",
                "id": "1",
                "name": "test",
                "email": "test@example.com",
                "createdAt": NOW_STR()
            });
        `,
		"003_create_posts_index.n1ql": `
            CREATE INDEX idx_posts_author ON ${BUCKET}(author) WHERE type = "post";
        `,
		"004_add_post.n1ql": `
            INSERT INTO ${BUCKET} (KEY, VALUE)
            VALUES ("post::1", {
                "type": "post",
                "id": "1",
                "title": "Test Post",
                "content": "This is a test post",
                "author": "test@example.com",
                "createdAt": NOW_STR()
            });
        `,
	}

	for name, content := range migrations {
		path := filepath.Join(migrationDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write migration %s: %v", name, err)
		}
	}

	// Test basic migration functionality
	t.Run("BasicMigration", func(t *testing.T) {
		// Create driver
		var driver *couchbase.CouchbaseDriver
		driver, err = couchbase.NewDriver(server.ConnectionString(), username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create driver: %v", err)
		}
		defer driver.Close()

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

		if len(executed) != 5 {
			t.Fatalf("Expected 5 migrations, got %d", len(executed))
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
            FROM %s
            WHERE type = "user"
        `, bucketName)

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
            FROM %s
            WHERE type = "post"
        `, bucketName)

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

		if len(executed) != 5 {
			t.Fatalf("Expected still 4 migrations after second run, got %d", len(executed))
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
	time.Sleep(5 * time.Second)
	resetCluster.Close(nil)

	// Test concurrent migrations
	t.Run("ConcurrentMigration", func(t *testing.T) {
		// Create multiple migrators to simulate concurrent processes
		const numConcurrent = 5
		var wg sync.WaitGroup

		// Channel to collect errors
		errCh := make(chan error, numConcurrent)

		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// First, create and initialize a driver to set up the collections and indexes
		setupDriver, err := couchbase.NewDriver(server.ConnectionString(), username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create setup driver: %v", err)
		}

		if err := setupDriver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize setup driver: %v", err)
		}
		setupDriver.Close()

		// Start concurrent migrations
		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(instanceID int) {
				defer wg.Done()

				// Create a new driver for each instance
				driver, err := couchbase.NewDriver(server.ConnectionString(), username, password, bucketName)
				if err != nil {
					errCh <- fmt.Errorf("instance %d: failed to create driver: %w", instanceID, err)
					return
				}
				defer driver.Close()

				// Create migrator with lock retry options
				m := migrator.New(
					driver,
					migrationDir,
					migrator.WithLockTimeout(20*time.Second),
					migrator.WithLockRetryInterval(1*time.Second),
					migrator.WithMaxLockRetries(30),
				)

				// Run migrations
				if err := m.Run(ctx); err != nil {
					errCh <- fmt.Errorf("instance %d: migration failed: %w", instanceID, err)
					return
				}

				t.Logf("Instance %d completed migrations successfully", instanceID)
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

		if len(errors) > 0 {
			for _, err := range errors {
				t.Errorf("Concurrent migration error: %v", err)
			}
			t.Fatalf("Concurrent migrations had %d errors", len(errors))
		}

		// Verify final state
		driver, err := couchbase.NewDriver(server.ConnectionString(), username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create verification driver: %v", err)
		}
		defer driver.Close()

		// Check migrations were recorded exactly once
		executed, err := driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations: %v", err)
		}

		if len(executed) != 5 {
			t.Fatalf("Expected exactly 5 migrations, got %d", len(executed))
		}

		// Connect to verify data
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
            FROM %s
            WHERE type = "user"
        `, bucketName)

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
			t.Fatalf("Expected exactly 1 user, got %d", userCount.Count)
		}

		// Count posts
		postCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "post"
        `, bucketName)

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
			t.Fatalf("Expected exactly 1 post, got %d", postCount.Count)
		}

		// Check lock collection is empty (all locks released)
		lockCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "lock"
        `, bucketName)

		lockResult, err := cluster.Query(lockCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count locks: %v", err)
		}
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
