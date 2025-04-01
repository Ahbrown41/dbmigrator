// pkg/driver/mongodb/mongodb_test.go
package mongodb_test

import (
	"context"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver/mongodb"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	mongoContainer, err := NewMongoDBContainer(ctx, t)
	require.NoError(t, err)
	defer mongoContainer.Cleanup(ctx)

	// Read the directory
	migrationDir := "./migrations"
	dbName := "testdb"

	// Test basic migration functionality
	t.Run("BasicMigration", func(t *testing.T) {
		// Create driver
		driver, err := mongodb.NewDriver(mongoContainer.URI, dbName, withTransactions)
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

		// Verify data was inserted by connecting directly to MongoDB
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoContainer.URI))
		if err != nil {
			t.Fatalf("Failed to connect to MongoDB: %v", err)
		}
		defer client.Disconnect(ctx)

		//// Count documents in users collection
		//usersCount, err := client.Database(dbName).Collection("users").CountDocuments(ctx, bson.M{})
		//if err != nil {
		//	t.Fatalf("Failed to count users: %v", err)
		//}
		//
		//if usersCount != 1 {
		//	t.Fatalf("Expected 1 user, got %d", usersCount)
		//}

		//// Count documents in posts collection
		//postsCount, err := client.Database(dbName).Collection("posts").CountDocuments(ctx, bson.M{})
		//if err != nil {
		//	t.Fatalf("Failed to count posts: %v", err)
		//}
		//
		//if postsCount != 1 {
		//	t.Fatalf("Expected 1 post, got %d", postsCount)
		//}

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

	// Reset database for concurrency test
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoContainer.URI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Drop database to start fresh
	if err := client.Database(dbName).Drop(ctx); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
	client.Disconnect(ctx)

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
		setupDriver, err := mongodb.NewDriver(mongoContainer.URI, dbName, withTransactions)
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
				driver, err := mongodb.NewDriver(mongoContainer.URI, dbName, withTransactions)
				if err != nil {
					errCh <- fmt.Errorf("instance %d: failed to create driver: %w", instanceID, err)
					return
				}
				defer driver.Close()

				// Create migrator with lock retry options
				m := migrator.New(
					driver,
					migrationDir,
					migrator.WithLockTimeout(10*time.Second),
					migrator.WithLockRetryInterval(500*time.Millisecond),
					migrator.WithMaxLockRetries(20),
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
		driver, err := mongodb.NewDriver(mongoContainer.URI, dbName, withTransactions)
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
			t.Fatalf("Expected exactly 4 migrations, got %d", len(executed))
		}

		// Connect to verify data
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoContainer.URI))
		if err != nil {
			t.Fatalf("Failed to connect to MongoDB: %v", err)
		}
		defer client.Disconnect(ctx)

		//// Count documents in users collection
		//usersCount, err := client.Database(dbName).Collection("users").CountDocuments(ctx, bson.M{})
		//if err != nil {
		//	t.Fatalf("Failed to count users: %v", err)
		//}
		//
		//if usersCount != 1 {
		//	t.Fatalf("Expected exactly 1 user, got %d", usersCount)
		//}

		//// Count documents in posts collection
		//postsCount, err := client.Database(dbName).Collection("posts").CountDocuments(ctx, bson.M{})
		//if err != nil {
		//	t.Fatalf("Failed to count posts: %v", err)
		//}
		//
		//if postsCount != 1 {
		//	t.Fatalf("Expected exactly 1 post, got %d", postsCount)
		//}

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
