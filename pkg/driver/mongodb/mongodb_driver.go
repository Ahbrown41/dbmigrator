package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Driver implements the driver.Driver interface for MongoDB
type Driver struct {
	client           *mongo.Client
	db               *mongo.Database
	hostID           string
	tracker          *MigrationTracker
	lockID           string
	withTransactions bool
}

type Option func(*Driver)

// WithTransactions enables or disables transactions
func WithTransactions(enabled bool) Option {
	return func(driver *Driver) {
		driver.withTransactions = enabled
	}
}

// New creates a new MongoDB driver
func New(client *mongo.Client, dbName string, opts ...Option) (*Driver, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	db := client.Database(dbName)
	hostID := fmt.Sprintf("mongo-%s-%d", dbName, time.Now().UnixNano())

	d := &Driver{
		client: client,
		db:     db,
		hostID: hostID,
	}
	for _, opt := range opts {
		opt(d)
	}

	d.tracker = NewMigrationTracker(db, hostID)
	return d, nil
}

// GetTracker returns the migration tracker
func (d *Driver) GetTracker() driver.MigrationTracker {
	return d.tracker
}

// Initialize prepares the database for migrations
func (d *Driver) Initialize(ctx context.Context) error {
	if err := d.tracker.Initialize(ctx); err != nil {
		return err
	}
	return nil
}

// ExecuteMigration executes a migration
func (d *Driver) ExecuteMigration(ctx context.Context, name, content string) error {
	startTime := time.Now()

	log.Printf("[MongoDB] Executing migration %s", name)

	// Parse the migration content (assumed to be BSON/JSON)
	var migration bson.D
	if err := bson.UnmarshalExtJSON([]byte(content), false, &migration); err != nil {
		log.Printf("[MongoDB] Failed to unmarshal JSON: %v: %s", err, content)
		// Record the failure
		errorMsg := fmt.Sprintf("failed to parse migration content: %v", err)
		executionMs := time.Since(startTime).Milliseconds()
		if err := d.tracker.RecordComplete(ctx, name, false, executionMs, errorMsg); err != nil {
			return fmt.Errorf("failed to record migration failure: %w", err)
		}

		return fmt.Errorf("failed to parse migration content: %w", err)
	}

	// Record migration as pending
	if err := d.tracker.RecordPending(ctx, name, content); err != nil {
		return fmt.Errorf("failed to record pending migration: %w", err)
	}

	// Execute the migration with retry for duplicate key errors
	var execErr error
	for retries := 0; retries < 3; retries++ {
		if d.withTransactions {
			// Start a session for transaction
			session, err := d.client.StartSession()
			if err != nil {
				execErr = fmt.Errorf("failed to start session: %w", err)
				break
			}
			defer session.EndSession(ctx)

			execErr = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
				if err := session.StartTransaction(); err != nil {
					return fmt.Errorf("failed to start transaction: %w", err)
				}

				// Execute the command within the transaction
				if err := d.db.RunCommand(sc, migration).Err(); err != nil {
					session.AbortTransaction(sc)
					return fmt.Errorf("command failed: %w", err)
				}

				// Record success within the transaction
				executionMs := time.Since(startTime).Milliseconds()

				if err := d.tracker.RecordComplete(sc, name, true, executionMs, ""); err != nil {
					session.AbortTransaction(sc)
					return fmt.Errorf("failed to record migration success: %w", err)
				}

				if err := session.CommitTransaction(sc); err != nil {
					return fmt.Errorf("failed to commit transaction: %w", err)
				}

				return nil
			})

			if execErr == nil {
				break
			}
		} else {
			// Execute without transaction
			err := d.db.RunCommand(ctx, migration).Err()
			if err == nil {
				execErr = nil
				break
			}

			if !mongo.IsDuplicateKeyError(err) {
				execErr = err
				break
			}

			// Retry for duplicate key errors, which can happen in concurrent scenarios
			time.Sleep(100 * time.Millisecond)
			execErr = err
		}
	}

	// Calculate execution time
	executionMs := time.Since(startTime).Milliseconds()

	// Record the result if not using transactions or if there was an error
	if !d.withTransactions || execErr != nil {
		var errorMsg string
		if execErr != nil {
			errorMsg = execErr.Error()
		}

		if err := d.tracker.RecordComplete(ctx, name, execErr == nil, executionMs, errorMsg); err != nil {
			return fmt.Errorf("failed to record migration result: %w", err)
		}
	}

	if execErr != nil {
		return fmt.Errorf("migration execution failed: %w", execErr)
	}

	log.Printf("[MongoDB] Successfully executed migration %s in %d ms", name, executionMs)
	return nil
}

// AcquireLock attempts to acquire a distributed lock for migrations
func (d *Driver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	var suc bool
	var err error
	suc, d.lockID, err = d.tracker.AcquireLock(ctx, lockTimeout)
	return suc, err
}

// ReleaseLock releases the distributed lock
func (d *Driver) ReleaseLock(ctx context.Context) error {
	return d.tracker.ReleaseLock(ctx, d.lockID)
}

// GetExecutedMigrations returns a list of already executed migrations
func (d *Driver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	return d.tracker.GetExecutedMigrations(ctx)
}

// Close cleans up resources
func (d *Driver) Close() error {
	return d.client.Disconnect(context.Background())
}
