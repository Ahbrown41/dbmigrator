package couchbase

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

type CouchbaseDriver struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	migColl    *gocb.Collection
	lockColl   *gocb.Collection
	bucketName string
	hostID     string
	lockID     string
}

func NewDriver(connStr, username, password, bucketName string) (*CouchbaseDriver, error) {
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	// Get bucket reference
	bucket := cluster.Bucket(bucketName)

	// Get collection references
	migColl := bucket.DefaultCollection()
	lockColl := bucket.DefaultCollection()

	// Generate a unique host ID for this instance
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	hostID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	return &CouchbaseDriver{
		cluster:    cluster,
		bucket:     bucket,
		migColl:    migColl,
		lockColl:   lockColl,
		bucketName: bucketName,
		hostID:     hostID,
	}, nil
}

func (d *CouchbaseDriver) Initialize(ctx context.Context) error {
	// Create necessary indexes if they don't exist
	// Index for migrations by name
	indexQuery := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS idx_migrations_name 
        ON %s (name) 
        WHERE type = "migration"
    `, d.bucketName)

	_, err := d.cluster.Query(indexQuery, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to create migrations index: %w", err)
	}

	// Index for locks by expiration
	lockIndexQuery := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS idx_locks_expires 
        ON %s (expires_at) 
        WHERE type = "lock"
    `, d.bucketName)

	_, err = d.cluster.Query(lockIndexQuery, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to create locks index: %w", err)
	}

	return nil
}

func (d *CouchbaseDriver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	// Generate a unique lock ID
	lockID := fmt.Sprintf("lock::%s", uuid.New().String())

	now := time.Now()
	expiresAt := now.Add(lockTimeout)

	// First, clean up expired locks
	cleanupQuery := fmt.Sprintf(`
        DELETE FROM %s 
        WHERE type = "lock" AND expires_at < $1
    `, d.bucketName)

	_, err := d.cluster.Query(cleanupQuery, &gocb.QueryOptions{
		Context:              ctx,
		PositionalParameters: []interface{}{now},
	})
	if err != nil {
		return false, fmt.Errorf("failed to clean up expired locks: %w", err)
	}

	// Check if any locks exist
	countQuery := fmt.Sprintf(`
        SELECT COUNT(*) as count
        FROM %s
        WHERE type = "lock"
    `, d.bucketName)

	countResult, err := d.cluster.Query(countQuery, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return false, fmt.Errorf("failed to check existing locks: %w", err)
	}
	defer func() {
		err = countResult.Close()
		if err != nil {
			fmt.Printf("failed to close count result: %v\n", err)
		}
	}()

	var countDoc struct {
		Count int `json:"count"`
	}

	if countResult.Next() {
		if err := countResult.Row(&countDoc); err != nil {
			return false, fmt.Errorf("failed to parse count result: %w", err)
		}
	}

	if countDoc.Count > 0 {
		// Lock exists
		return false, nil
	}

	// Try to insert a new lock
	lock := struct {
		Type      string    `json:"type"`
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
		ExpiresAt time.Time `json:"expires_at"`
		HostID    string    `json:"host_id"`
	}{
		Type:      "lock",
		ID:        lockID,
		CreatedAt: now,
		ExpiresAt: expiresAt,
		HostID:    d.hostID,
	}

	// Use insert with expiry to ensure lock is automatically released
	_, err = d.lockColl.Insert(lockID, lock, &gocb.InsertOptions{
		Context: ctx,
		Expiry:  lockTimeout,
	})

	if err != nil {
		// Check if document exists error
		if err.Error() == gocb.ErrDocumentExists.Error() {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert lock: %w", err)
	}

	// We got the lock
	d.lockID = lockID
	return true, nil
}

func (d *CouchbaseDriver) ReleaseLock(ctx context.Context) error {
	if d.lockID == "" {
		return nil // No lock to release
	}

	// Remove the lock document
	_, err := d.lockColl.Remove(d.lockID, &gocb.RemoveOptions{
		Context: ctx,
	})

	if err != nil {
		// Check if document not found error
		if err.Error() == gocb.ErrDocumentNotFound.Error() {
			// Lock already expired or was removed
			d.lockID = ""
			return nil
		}
		return fmt.Errorf("failed to release lock: %w", err)
	}

	d.lockID = ""
	return nil
}

func (d *CouchbaseDriver) ExecuteMigration(ctx context.Context, name, content string) error {
	// First check if this migration has already been executed
	migrationID := fmt.Sprintf("migration::%s", name)

	// Check if migration exists
	checkQuery := fmt.Sprintf(`
        SELECT COUNT(*) as count
        FROM %s
        WHERE type = "migration" AND name = $1
    `, d.bucketName)

	checkResult, err := d.cluster.Query(checkQuery, &gocb.QueryOptions{
		Context:              ctx,
		PositionalParameters: []interface{}{name},
	})
	if err != nil {
		return fmt.Errorf("failed to check if migration exists: %w", err)
	}
	defer func() {
		err = checkResult.Close()
		if err != nil {
			fmt.Printf("failed to close count result: %v\n", err)
		}
	}()

	var countDoc struct {
		Count int `json:"count"`
	}

	if checkResult.Next() {
		if err := checkResult.Row(&countDoc); err != nil {
			return fmt.Errorf("failed to parse count result: %w", err)
		}
	}

	if countDoc.Count > 0 {
		// Migration already executed, skip it
		return nil
	}

	startTime := time.Now()

	// Process N1QL content
	// Replace placeholders if needed
	n1qlContent := strings.ReplaceAll(content, "${BUCKET}", d.bucketName)

	// Execute the N1QL query
	var execErr error
	_, err = d.cluster.Query(n1qlContent, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		execErr = err
	}

	endTime := time.Now()
	executionMs := endTime.Sub(startTime).Milliseconds()

	// Store migration record
	record := struct {
		Type        string    `json:"type"`
		Name        string    `json:"name"`
		Content     string    `json:"content"`
		ExecutedAt  time.Time `json:"executed_at"`
		ExecutionMs int64     `json:"execution_ms"`
		Success     bool      `json:"success"`
		Error       string    `json:"error,omitempty"`
	}{
		Type:        "migration",
		Name:        name,
		Content:     content,
		ExecutedAt:  endTime,
		ExecutionMs: executionMs,
		Success:     execErr == nil,
	}

	if execErr != nil {
		record.Error = execErr.Error()
	}

	// Use insert with CAS operations to ensure atomicity
	mutateOpts := &gocb.InsertOptions{Context: ctx}
	_, err = d.migColl.Insert(migrationID, record, mutateOpts)

	if err != nil {
		// Check if document exists error
		if err.Error() == gocb.ErrDocumentExists.Error() {
			// Check if the migration was successful
			getResult, err := d.migColl.Get(migrationID, &gocb.GetOptions{Context: ctx})
			if err != nil {
				return fmt.Errorf("failed to check existing migration: %w", err)
			}

			var existingMigration struct {
				Success bool `json:"success"`
			}

			if err := getResult.Content(&existingMigration); err != nil {
				return fmt.Errorf("failed to parse existing migration: %w", err)
			}

			if !existingMigration.Success {
				return fmt.Errorf("migration was previously executed but failed")
			}

			// Migration was already successfully executed by another process
			return nil
		}

		return fmt.Errorf("failed to record migration: %w", err)
	}

	if execErr != nil {
		return execErr
	}

	return nil
}

func (d *CouchbaseDriver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	query := fmt.Sprintf(`
        SELECT m.name, m.content, m.executed_at, m.execution_ms, m.success, m.error
        FROM %s m
        WHERE m.type = "migration"
        ORDER BY m.executed_at
    `, d.bucketName)

	result, err := d.cluster.Query(query, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return nil, err
	}
	defer func() {
		err = result.Close()
		if err != nil {
			fmt.Printf("failed to close query result: %v\n", err)
		}
	}()

	var records []driver.MigrationRecord
	for result.Next() {
		var record struct {
			Name        string    `json:"name"`
			Content     string    `json:"content"`
			ExecutedAt  time.Time `json:"executed_at"`
			ExecutionMs int64     `json:"execution_ms"`
			Success     bool      `json:"success"`
			Error       string    `json:"error"`
		}

		if err := result.Row(&record); err != nil {
			return nil, err
		}

		records = append(records, driver.MigrationRecord{
			Name:        record.Name,
			Content:     record.Content,
			ExecutedAt:  record.ExecutedAt,
			ExecutionMs: record.ExecutionMs,
			Success:     record.Success,
			Error:       record.Error,
		})
	}

	if err := result.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func (d *CouchbaseDriver) Close() error {
	// Release lock if we have one
	if d.lockID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = d.ReleaseLock(ctx) // Best effort
	}
	// Close the cluster connection
	defer func() {
		err := d.cluster.Close(nil)
		if err != nil {
			fmt.Printf("failed to close count result: %v\n", err)
		}
	}()

	return nil
}
