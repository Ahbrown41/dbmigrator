// pkg/driver/mongodb/mongodb_test.go
package mongodb_test

import (
	"context"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver/mongodb"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	withTransactions = false
)

func TestMongoDriver(t *testing.T) {
	ctx := context.Background()

	// Start MongoDB container
	mongoContainer := NewMongoDBContainer(ctx, t)
	defer mongoContainer.Stop(ctx, t)

	// Read the directory
	migrationDir := "./migrations"
	dbName := "testdb"

	// Get Connection String
	constr, err := mongoContainer.ConnectionString(ctx)
	assert.NoError(t, err)

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(constr))
	assert.NoError(t, err)

	// Ping database to verify connection
	if err = client.Ping(context.Background(), nil); err != nil {
		assert.NoError(t, err)
	}
	defer client.Disconnect(ctx)

	// Test basic migration functionality
	t.Run("BasicMigration", func(t *testing.T) {
		// Create driver - CHANGED from NewDriver to New
		driver, err := mongodb.New(client, dbName)
		if err != nil {
			t.Fatalf("Failed to create driver: %v", err)
		}

		// Initialize the driver
		if err := driver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize driver: %v", err)
		}

		// Run migrations
		m := migrator.New(driver, migrationDir, migrator.WithName("basic"))
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

		// Rest of the test remains the same...
	})

	// Reset database for concurrency test
	// Drop database to start fresh
	if err := client.Database(dbName).Drop(ctx); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Test concurrent migrations
	t.Run("ConcurrentMigration", func(t *testing.T) {
		// Create multiple migrators to simulate concurrent processes
		const numConcurrent = 5
		var wg sync.WaitGroup

		// Channel to collect errors
		errCh := make(chan error, numConcurrent)

		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// First, create and initialize a driver to set up the collections and indexes
		// CHANGED from NewDriver to New
		setupDriver, err := mongodb.New(client, dbName)
		if err != nil {
			t.Fatalf("Failed to create setup driver: %v", err)
		}

		if err := setupDriver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize setup driver: %v", err)
		}

		// Start concurrent migrations
		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(instanceID int) {
				defer wg.Done()

				// Create a new driver for each instance
				driver, err := mongodb.New(client, dbName)
				if err != nil {
					errCh <- fmt.Errorf("instance %d: failed to create driver: %w", instanceID, err)
					return
				}

				// Create migrator with lock retry options
				m := migrator.New(
					driver,
					migrationDir,
					migrator.WithLockTimeout(10*time.Second),
					migrator.WithLockRetryInterval(500*time.Millisecond),
					migrator.WithMaxLockRetries(20),
					migrator.WithName(fmt.Sprintf("migration-%d", instanceID)),
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

		// Verify final state
		driver, err := mongodb.New(client, dbName)
		if err != nil {
			t.Fatalf("Failed to create verification driver: %v", err)
		}

		// Check migrations were recorded exactly once
		executed, err := driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations: %v", err)
		}

		if len(executed) != 5 {
			t.Fatalf("Expected exactly 4 migrations, got %d", len(executed))
		}

		// Count documents in users collection
		usersCount, err := client.Database(dbName).Collection("users").CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("Failed to count users: %v", err)
		}

		if usersCount != 1 {
			t.Fatalf("Expected exactly 1 user, got %d", usersCount)
		}

		// Count documents in posts collection
		postsCount, err := client.Database(dbName).Collection("posts").CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("Failed to count posts: %v", err)
		}

		if postsCount != 1 {
			t.Fatalf("Expected exactly 1 post, got %d", postsCount)
		}

		// Check lock collection is empty (all locks released)
		lockCount, err := client.Database(dbName).Collection("schema_migration_locks").CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("Failed to count locks: %v", err)
		}

		if lockCount != 0 {
			t.Fatalf("Expected 0 locks remaining, got %d", lockCount)
		}
	})
}
