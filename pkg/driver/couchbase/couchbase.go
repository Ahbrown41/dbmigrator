package couchbase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

const (
	migrationsCollectionName = "schema_migrations"
	locksCollectionName      = "schema_migration_locks"
)

type CouchbaseDriver struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	scope      *gocb.Scope
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

	// Get default scope
	scope := bucket.DefaultScope()

	// Generate a unique host ID for this instance
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	hostID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	return &CouchbaseDriver{
		cluster:    cluster,
		bucket:     bucket,
		scope:      scope,
		bucketName: bucketName,
		hostID:     hostID,
	}, nil
}

func (d *CouchbaseDriver) Initialize(ctx context.Context) error {
	// Wait for bucket to be ready
	err := d.bucket.WaitUntilReady(time.Second*30, nil)
	if err != nil {
		return fmt.Errorf("failed to wait for bucket ready: %w", err)
	}

	// Try to create collections if they don't exist
	collMgr := d.bucket.CollectionsV2()

	// Create migrations collection
	err = collMgr.CreateCollection(
		"_default",
		migrationsCollectionName,
		&gocb.CreateCollectionSettings{},
		&gocb.CreateCollectionOptions{},
	)

	// Ignore "collection already exists" errors
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create migrations collection: %w", err)
	}

	// Create locks collection
	err = collMgr.CreateCollection(
		"_default",
		locksCollectionName,
		&gocb.CreateCollectionSettings{},
		&gocb.CreateCollectionOptions{},
	)

	// Ignore "collection already exists" errors
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create locks collection: %w", err)
	}

	// Get collection references after ensuring they exist
	d.migColl = d.scope.Collection(migrationsCollectionName)
	d.lockColl = d.scope.Collection(locksCollectionName)

	// Create necessary indexes
	// Index for migrations by name
	indexQuery := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_migrations_name 
		ON %s._default.%s(name)
	`, d.bucketName, migrationsCollectionName)

	_, err = d.cluster.Query(indexQuery, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to create migrations index: %w", err)
	}

	// Index for locks by expiration
	lockIndexQuery := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_locks_expires 
		ON %s._default.%s(expires_at)
	`, d.bucketName, locksCollectionName)

	_, err = d.cluster.Query(lockIndexQuery, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to create locks index: %w", err)
	}

	return nil
}

func (d *CouchbaseDriver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	// Generate a unique lock ID
	lockID := uuid.New().String()
	d.lockID = fmt.Sprintf("lock::%s", lockID)

	now := time.Now()
	expiresAt := now.Add(lockTimeout)

	// First, clean up expired locks
	cleanupQuery := fmt.Sprintf(`
        DELETE FROM %s._default.%s
        WHERE expires_at < $1
    `, d.bucketName, locksCollectionName)

	_, err := d.cluster.Query(cleanupQuery, &gocb.QueryOptions{
		Context:              ctx,
		PositionalParameters: []interface{}{now.Format(time.RFC3339)},
	})
	if err != nil {
		return false, fmt.Errorf("failed to clean up expired locks: %w", err)
	}

	// Check if any lock exists that's not expired
	checkQuery := fmt.Sprintf(`
        SELECT COUNT(*) as count
        FROM %s._default.%s
        WHERE expires_at > $1
    `, d.bucketName, locksCollectionName)

	checkResult, err := d.cluster.Query(checkQuery, &gocb.QueryOptions{
		Context:              ctx,
		PositionalParameters: []interface{}{now.Format(time.RFC3339)},
	})
	if err != nil {
		return false, fmt.Errorf("failed to check existing locks: %w", err)
	}
	defer checkResult.Close()

	var countDoc struct {
		Count int `json:"count"`
	}

	if checkResult.Next() {
		if err := checkResult.Row(&countDoc); err != nil {
			return false, fmt.Errorf("failed to parse count result: %w", err)
		}
	}

	if countDoc.Count > 0 {
		// Lock already exists and is not expired
		return false, nil
	}

	// Try to insert a new lock - this needs to be atomic
	lockDoc := struct {
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
		ExpiresAt time.Time `json:"expires_at"`
		HostID    string    `json:"host_id"`
	}{
		ID:        lockID,
		CreatedAt: now,
		ExpiresAt: expiresAt,
		HostID:    d.hostID,
	}

	// Use an insert with CAS operations to ensure atomicity
	_, err = d.lockColl.Insert(d.lockID, lockDoc, &gocb.InsertOptions{Context: ctx})

	if err != nil {
		// Check if document already exists error
		if strings.Contains(err.Error(), "document exists") {
			// Another process got the lock first
			return false, nil
		}

		return false, fmt.Errorf("failed to create lock: %w", err)
	}

	// We acquired the lock
	slog.Debug("Acquired migration lock", slog.String("lock_id", d.lockID), slog.String("host_id", d.hostID))
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
	// Migration document ID
	migrationID := fmt.Sprintf("migration::%s", name)

	// First try to create a "pending" migration document atomically
	// If it already exists, someone else is already working on this migration
	pendingMigration := struct {
		Name      string    `json:"name"`
		Status    string    `json:"status"`
		StartedAt time.Time `json:"started_at"`
		HostID    string    `json:"host_id"`
	}{
		Name:      name,
		Status:    "pending",
		StartedAt: time.Now(),
		HostID:    d.hostID,
	}

	// Try to insert the pending migration - this will fail if it already exists
	_, err := d.migColl.Insert(migrationID, pendingMigration, &gocb.InsertOptions{Context: ctx})
	if err != nil {
		// If document already exists, check if it's completed or failed
		if errors.Is(err, gocb.ErrDocumentExists) {
			// Get the existing document
			getResult, err := d.migColl.Get(migrationID, &gocb.GetOptions{Context: ctx})
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

			// If it's pending but from another host, wait a bit and retry
			if existingMigration.Status == "pending" {
				return fmt.Errorf("migration is currently being processed by another instance")
			}

			// If it previously failed, we'll rerun it
			if existingMigration.Status == "completed" && !existingMigration.Success {
				// We'll proceed to rerun it, but first we need to update it to pending
				// with our hostID using CAS to ensure atomicity
				_, err = d.migColl.Replace(migrationID, pendingMigration, &gocb.ReplaceOptions{
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

	// Now we can proceed with executing the migration
	startTime := time.Now()

	// Process N1QL content
	n1qlContent := strings.ReplaceAll(content, "${BUCKET}", d.bucketName)

	var execErr error
	// Split the content by semicolons, preserving multi-line statements
	statements := splitQueries(n1qlContent)
	for _, statement := range statements {
		// Skip empty statements and comments
		statement = strings.TrimSpace(statement)

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

	endTime := time.Now()
	executionMs := endTime.Sub(startTime).Milliseconds()

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
		Content:     content,
		StartedAt:   pendingMigration.StartedAt,
		ExecutedAt:  endTime,
		ExecutionMs: executionMs,
		Success:     execErr == nil,
		HostID:      d.hostID,
	}

	if execErr != nil {
		completedMigration.Error = execErr.Error()
	}

	// Get the current document to use its CAS value
	getResult, err := d.migColl.Get(migrationID, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("failed to get migration document for updating: %w", err)
	}

	// Replace with CAS to ensure we're updating the document we just created
	_, err = d.migColl.Replace(migrationID, completedMigration, &gocb.ReplaceOptions{
		Context: ctx,
		Cas:     getResult.Cas(),
	})

	if err != nil {
		return fmt.Errorf("failed to update migration status: %w", err)
	}

	if execErr != nil {
		return execErr
	}

	return nil
}

// SplitQueries splits migration content into separate queries
// handling multi-line statements and preserving formatting
func splitQueries(content string) []string {
	var queries []string
	var currentQuery strings.Builder
	inSingleLineComment := false
	inMultiLineComment := false
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(content); i++ {
		char := content[i]

		// Handle comments and quotes
		if !inSingleLineComment && !inMultiLineComment {
			// Check for start of comments
			if char == '-' && i+1 < len(content) && content[i+1] == '-' {
				inSingleLineComment = true
				currentQuery.WriteByte(char)
				continue
			} else if char == '/' && i+1 < len(content) && content[i+1] == '*' {
				inMultiLineComment = true
				currentQuery.WriteByte(char)
				continue
			} else if char == '\'' && !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			} else if char == '"' && !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		} else {
			// Check for end of comments
			if inSingleLineComment && char == '\n' {
				inSingleLineComment = false
			} else if inMultiLineComment && char == '*' && i+1 < len(content) && content[i+1] == '/' {
				inMultiLineComment = false
				currentQuery.WriteByte(char)
				currentQuery.WriteByte(content[i+1])
				i++
				continue
			}
		}

		// Check for end of query (semicolon outside of quotes and comments)
		if char == ';' && !inSingleQuote && !inDoubleQuote && !inSingleLineComment && !inMultiLineComment {
			currentQuery.WriteByte(char)
			queryStr := strings.TrimSpace(currentQuery.String())
			if queryStr != "" {
				queries = append(queries, queryStr)
			}
			currentQuery.Reset()
		} else {
			currentQuery.WriteByte(char)
		}
	}

	// Add the last query if it doesn't end with a semicolon
	queryStr := strings.TrimSpace(currentQuery.String())
	if queryStr != "" {
		queries = append(queries, queryStr)
	}

	return queries
}

func (d *CouchbaseDriver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	query := fmt.Sprintf(`
		SELECT m.name, m.content, m.executed_at, m.execution_ms, m.success, m.error
		FROM %s._default.%s m
		ORDER BY m.executed_at
	`, d.bucketName, migrationsCollectionName)

	result, err := d.cluster.Query(query, &gocb.QueryOptions{Context: ctx})
	if err != nil {
		return nil, err
	}
	defer result.Close()

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
	return d.cluster.Close(nil)
}
