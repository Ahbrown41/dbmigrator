package couchbase

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

type Driver struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	bucketName string
	scopeName  string
	hostID     string
	lockID     string
	tracker    *MigrationTracker
}

// New creates a new Couchbase driver instance
func New(cluster *gocb.Cluster, bucketName, scopeName string) (*Driver, error) {
	if cluster == nil {
		return nil, fmt.Errorf("cluster cannot be nil")
	}
	// Get bucket reference
	bucket := cluster.Bucket(bucketName)

	// Generate a unique host ID for this instance
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	hostID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	driver := &Driver{
		cluster:    cluster,
		bucket:     bucket,
		bucketName: bucketName,
		scopeName:  scopeName,
		hostID:     hostID,
		tracker:    NewMigrationTracker(bucket, scopeName, hostID),
	}

	return driver, nil
}

// Initialize initializes the Couchbase driver
func (d *Driver) Initialize(ctx context.Context) error {
	if err := d.tracker.Initialize(ctx); err != nil {
		return err
	}
	return nil
}

// ExecuteMigration executes a migration
func (d *Driver) ExecuteMigration(ctx context.Context, name, content string) error {
	// Record the migration as pending
	if err := d.tracker.RecordPending(ctx, name, content); err != nil {
		return err
	}

	// Now we can proceed with executing the migration
	startTime := time.Now()

	// Process N1QL content
	n1qlContent := strings.ReplaceAll(content, "${BUCKET}", d.bucketName)

	var execErr error
	// Execute each query in the N1QL content
	statements := driver.SplitQueries(n1qlContent)
	for _, statement := range statements {
		// Execute the N1QL query
		_, err := d.cluster.Transactions().Run(func(attemptContext *gocb.TransactionAttemptContext) error {
			_, err := d.cluster.Query(statement, &gocb.QueryOptions{Context: ctx})
			if err != nil {
				return fmt.Errorf("failed to execute migration: %w (%s)", err, statement)
			}
			slog.Debug("Executed migration", slog.String("statement", statement))
			return nil
		}, &gocb.TransactionOptions{})
		if err != nil {
			execErr = fmt.Errorf("failed to execute migration: %w (%s)", err, statement)
			break // Stop on first error
		}
	}

	// Calculate execution time
	executionMs := time.Since(startTime).Milliseconds()

	// Record the migration result
	var errorMessage string
	if execErr != nil {
		errorMessage = execErr.Error()
	}

	err := d.tracker.RecordComplete(ctx, name, execErr == nil, executionMs, errorMessage)

	if err != nil {
		// If we failed to record the migration but the migration itself succeeded,
		// we should still return an error to ensure data consistency
		if execErr == nil {
			return fmt.Errorf("migration executed successfully but failed to record: %w", err)
		}
	}

	if execErr != nil {
		return execErr
	}

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

// GetExecutedMigrations returns executed migrations
func (d *Driver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	return d.tracker.GetExecutedMigrations(ctx)
}

// Close closes the Couchbase driver
func (d *Driver) Close() error {
	if d.cluster != nil {
		err := d.cluster.Close(nil)
		if err != nil {
			return fmt.Errorf("failed to close Couchbase cluster: %w", err)
		}
	}
	return nil
}
