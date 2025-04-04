package couchbase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/couchbase/gocb/v2"
)

const (
	migrationsCollection = "schema_migrations"
	locksCollectionName  = "schema_migration_locks"
)

// MigrationTracker handles recording and retrieving migration information
type MigrationTracker struct {
	bucket    *gocb.Bucket
	scopeName string
	hostID    string
}

// NewMigrationTracker creates a new migration tracker for Couchbase
func NewMigrationTracker(bucket *gocb.Bucket, scopeName, hostID string) *MigrationTracker {
	return &MigrationTracker{
		bucket:    bucket,
		scopeName: scopeName,
		hostID:    hostID,
	}
}

// Initialize creates the necessary collections and indexes
func (t *MigrationTracker) Initialize(ctx context.Context) error {
	// Try to create collections if they don't exist
	collMgr := t.bucket.CollectionsV2()

	// Create scope if needed
	err := collMgr.CreateScope(
		t.scopeName,
		&gocb.CreateScopeOptions{},
	)

	// Ignore "scope already exists" errors
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create scope (%s): %w", t.scopeName, err)
	}

	// Wait for scope to be available
	if err := t.waitForScope(ctx, t.scopeName); err != nil {
		return fmt.Errorf("failed waiting for scope (%s): %w", t.scopeName, err)
	}

	collections := [][]string{{migrationsCollection, "name"}, {locksCollectionName, "expires_at"}}
	for _, collection := range collections {
		// Create collection if needed
		err = collMgr.CreateCollection(
			t.scopeName,
			collection[0],
			&gocb.CreateCollectionSettings{},
			&gocb.CreateCollectionOptions{},
		)

		// Ignore "collection already exists" errors
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create collection (%s): %w", collection[0], err)
		}

		// Wait for collection to be available
		if err := t.waitForCollection(ctx, t.scopeName, collection[0]); err != nil {
			return fmt.Errorf("failed waiting for collection (%s): %w", collection[0], err)
		}

		// Create index for migrations by name if needed
		time.Sleep(5 * time.Second)
		indexQuery := fmt.Sprintf(`
        	CREATE INDEX IF NOT EXISTS idx_%s ON %s.%s.%s(%s)
    	`, collection[0], t.bucket.Name(), t.scopeName, collection[0], collection[1])

		// Execute the query - using the cluster directly from the collection
		_, err = t.bucket.Scope(t.scopeName).Query(indexQuery, &gocb.QueryOptions{Context: ctx})
		if err != nil {
			return fmt.Errorf("failed to create migrations index: %w", err)
		}
	}

	return nil
}

// waitForScope waits until a scope exists and is available
func (d *MigrationTracker) waitForScope(ctx context.Context, scopeName string) error {
	maxRetries := 10
	retryDelay := time.Second

	for i := 0; i < maxRetries; i++ {
		// Check if scope exists by listing all scopes
		scopes, err := d.bucket.CollectionsV2().GetAllScopes(&gocb.GetAllScopesOptions{})
		if err == nil {
			for _, s := range scopes {
				if s.Name == scopeName {
					return nil // Scope found and available
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			// Continue with next iteration
		}

		retryDelay *= 2 // Exponential backoff
		if retryDelay > 10*time.Second {
			retryDelay = 10 * time.Second // Cap at 10s
		}
	}

	return fmt.Errorf("timeout waiting for scope %s", scopeName)
}

// waitForCollection waits until a collection exists and is available
func (d *MigrationTracker) waitForCollection(ctx context.Context, scopeName, collectionName string) error {
	maxRetries := 10
	retryDelay := time.Second

	for i := 0; i < maxRetries; i++ {
		// Check if collection exists by listing collections in the scope
		scopes, err := d.bucket.CollectionsV2().GetAllScopes(&gocb.GetAllScopesOptions{})
		if err == nil {
			for _, s := range scopes {
				if s.Name == scopeName {
					for _, c := range s.Collections {
						if c.Name == collectionName {
							return nil // Collection found and available
						}
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			// Continue with next iteration
		}

		retryDelay *= 2 // Exponential backoff
		if retryDelay > 10*time.Second {
			retryDelay = 10 * time.Second // Cap at 10s
		}
	}

	return fmt.Errorf("timeout waiting for collection %s in scope %s", collectionName, scopeName)
}

// RecordPending creates a pending migration record
func (t *MigrationTracker) RecordPending(ctx context.Context, name, content string) error {
	// Migration document ID
	migrationID := fmt.Sprintf("migration::%s", name)

	pendingMigration := struct {
		Name      string    `json:"name"`
		Status    string    `json:"status"`
		Content   string    `json:"content"`
		StartedAt time.Time `json:"started_at"`
		HostID    string    `json:"host_id"`
	}{
		Name:      name,
		Status:    "pending",
		Content:   content,
		StartedAt: time.Now(),
		HostID:    t.hostID,
	}

	// Try to insert the pending migration - this will fail if it already exists
	migCol := t.bucket.Scope(t.scopeName).Collection(migrationsCollection)
	_, err := migCol.Insert(migrationID, pendingMigration, &gocb.InsertOptions{Context: ctx})
	if err != nil {
		// If document already exists, check if it's completed or failed
		if errors.Is(err, gocb.ErrDocumentExists) {
			// Get the existing document
			getResult, err := migCol.Get(migrationID, &gocb.GetOptions{Context: ctx})
			if err != nil {
				return fmt.Errorf("failed to check existing migration: %w", err)
			}

			var existingMigration struct {
				Status  string `json:"status"`
				Success bool   `json:"success"`
			}

			if err := getResult.Content(&existingMigration); err != nil {
				return fmt.Errorf("failed to parse existing migration: %w", err)
			}

			// If already completed successfully, we can just return
			if existingMigration.Status == "completed" && existingMigration.Success {
				return nil
			}

			// If it's pending but from another host, return an error
			if existingMigration.Status == "pending" {
				return fmt.Errorf("migration is currently being processed by another instance")
			}

			// If it previously failed, we'll rerun it
			if existingMigration.Status == "completed" && !existingMigration.Success {
				// Update it to pending with our hostID using CAS to ensure atomicity
				_, err = migCol.Replace(migrationID, pendingMigration, &gocb.ReplaceOptions{
					Context: ctx,
					Cas:     getResult.Cas(),
				})
				if err != nil {
					return fmt.Errorf("failed to update failed migration to pending: %w", err)
				}
			}
		} else {
			return fmt.Errorf("failed to create pending migration: %w", err)
		}
	}

	return nil
}

// RecordComplete updates a migration record as completed
func (t *MigrationTracker) RecordComplete(ctx context.Context, name string, success bool, executionMs int64, errorMsg string) error {
	migrationID := fmt.Sprintf("migration::%s", name)

	// Get the current document for its startedAt time and CAS value
	migCol := t.bucket.Scope(t.scopeName).Collection(migrationsCollection)
	getResult, err := migCol.Get(migrationID, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to get migration document for updating: %w", err)
	}

	var existingMigration struct {
		StartedAt time.Time `json:"started_at"`
		Content   string    `json:"content"`
	}

	if err := getResult.Content(&existingMigration); err != nil {
		return fmt.Errorf("failed to parse existing migration: %w", err)
	}

	// Update the migration document with results
	completedMigration := struct {
		Name        string    `json:"name"`
		Status      string    `json:"status"`
		Content     string    `json:"content"`
		StartedAt   time.Time `json:"started_at"`
		ExecutedAt  time.Time `json:"executed_at"`
		ExecutionMs int64     `json:"execution_ms"`
		Success     bool      `json:"success"`
		Error       string    `json:"error,omitempty"`
		HostID      string    `json:"host_id"`
	}{
		Name:        name,
		Status:      "completed",
		Content:     existingMigration.Content,
		StartedAt:   existingMigration.StartedAt,
		ExecutedAt:  time.Now(),
		ExecutionMs: executionMs,
		Success:     success,
		HostID:      t.hostID,
	}

	if !success && errorMsg != "" {
		completedMigration.Error = errorMsg
	}

	// Replace with CAS to ensure we're updating the document we just read
	_, err = migCol.Replace(migrationID, completedMigration, &gocb.ReplaceOptions{
		Context: ctx,
		Cas:     getResult.Cas(),
	})

	if err != nil {
		return fmt.Errorf("failed to update migration status: %w", err)
	}

	return nil
}

// AcquireLock attempts to acquire a distributed lock for migrations
func (d *MigrationTracker) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, string, error) {
	// Generate a unique lock ID for this operation
	lockID := fmt.Sprintf("migration-lock-%s", d.hostID)
	lockExpiration := time.Now().Add(lockTimeout)

	// Create a lock document
	lockDoc := struct {
		ID        string    `json:"id"`
		Type      string    `json:"type"`
		HostID    string    `json:"host_id"`
		CreatedAt time.Time `json:"created_at"`
		ExpiresAt time.Time `json:"expires_at"`
	}{
		ID:        "migration_lock",
		Type:      "lock",
		HostID:    d.hostID,
		CreatedAt: time.Now(),
		ExpiresAt: lockExpiration,
	}

	// Try to insert the lock document with expiration
	lockCol := d.bucket.Scope(d.scopeName).Collection(locksCollectionName)
	_, err := lockCol.Insert("migration_lock", lockDoc, &gocb.InsertOptions{
		Context: ctx,
		Expiry:  lockTimeout,
	})

	// If we got a key exists error, the lock is held by someone else
	if errors.Is(err, gocb.ErrDocumentExists) {
		// Try to get the current lock to see who holds it
		result, err := lockCol.Get("migration_lock", &gocb.GetOptions{Context: ctx})
		if err == nil {
			var existingLock struct {
				HostID    string    `json:"host_id"`
				ExpiresAt time.Time `json:"expires_at"`
			}
			if err := result.Content(&existingLock); err == nil {
				return false, "", fmt.Errorf("lock held by %s until %v", existingLock.HostID, existingLock.ExpiresAt)
			}
		}
		return false, "", nil
	} else if err != nil {
		return false, "", fmt.Errorf("error acquiring lock: %w", err)
	}

	return true, lockID, nil
}

// ReleaseLock releases the distributed lock
func (d *MigrationTracker) ReleaseLock(ctx context.Context, lockID string) error {
	// Only try to release if we acquired the lock
	if lockID == "" {
		return nil
	}

	// Delete the lock document if our host ID matches
	lockCol := d.bucket.Scope(d.scopeName).Collection(locksCollectionName)
	_, err := lockCol.Remove("migration_lock", &gocb.RemoveOptions{
		Context: ctx,
	})

	if err != nil {
		// Don't return error if document not found (already expired/removed)
		if !errors.Is(err, gocb.ErrDocumentNotFound) {
			return fmt.Errorf("error releasing lock: %w", err)
		}
	}

	lockID = "" // Reset lock ID
	return nil
}

// GetExecutedMigrations retrieves all migration records
func (t *MigrationTracker) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	query := fmt.Sprintf(`
        SELECT m.name, m.content, m.started_at, m.executed_at, m.execution_ms, m.success,
               m.error, m.host_id, m.status
        FROM %s.%s.%s AS m
        ORDER BY m.executed_at ASC NULLS FIRST, m.started_at ASC
    `, t.bucket.Name(), t.scopeName, migrationsCollection)

	// Execute the query using the cluster
	result, err := t.bucket.Scope(t.scopeName).Query(query, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}
	defer result.Close()

	var records []driver.MigrationRecord
	for result.Next() {
		var record struct {
			Name        string     `json:"name"`
			Content     string     `json:"content"`
			StartedAt   time.Time  `json:"started_at"`
			ExecutedAt  *time.Time `json:"executed_at"`
			ExecutionMs *int64     `json:"execution_ms"`
			Success     *bool      `json:"success"`
			Error       *string    `json:"error"`
			HostID      *string    `json:"host_id"`
			Status      string     `json:"status"`
		}

		if err := result.Row(&record); err != nil {
			return nil, fmt.Errorf("failed to scan migration record: %w", err)
		}

		// Add ID field which is required by the interface
		// Use name as the ID since it's unique
		migRecord := driver.MigrationRecord{
			ID:        record.Name,
			Name:      record.Name,
			Content:   record.Content,
			StartedAt: record.StartedAt,
			Status:    record.Status,
			HostID:    "", // Default value
		}

		// Handle nullable fields
		if record.ExecutedAt != nil {
			migRecord.ExecutedAt = *record.ExecutedAt
		}

		if record.ExecutionMs != nil {
			migRecord.ExecutionMs = *record.ExecutionMs
		}

		if record.Success != nil {
			migRecord.Success = *record.Success
		}

		if record.Error != nil {
			migRecord.Error = *record.Error
		}

		if record.HostID != nil {
			migRecord.HostID = *record.HostID
		}

		records = append(records, migRecord)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return records, nil
}
