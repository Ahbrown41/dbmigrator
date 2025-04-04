// pkg/driver/mongodb/migration_tracker.go
package mongodb

import (
	"context"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	migrationsCollection = "schema_migrations"
	locksCollectionName  = "schema_migration_locks"
	migrationLock        = "migration_lock"
)

// MigrationTracker implements driver.MigrationTracker for MongoDB
type MigrationTracker struct {
	db     *mongo.Database
	hostID string
}

// NewMigrationTracker creates a new MongoDB migration tracker
func NewMigrationTracker(db *mongo.Database, hostID string) *MigrationTracker {
	track := &MigrationTracker{
		db:     db,
		hostID: hostID,
	}
	return track
}

// Initialize ensures indexes exist on the migrations collection
func (t *MigrationTracker) Initialize(ctx context.Context) error {
	// Create index for migrations collection
	_, err := t.db.Collection(migrationsCollection).Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create index on migrations collection: %w", err)
	}

	// Create index for locks collection
	_, err = t.db.Collection(locksCollectionName).Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys:    bson.D{{Key: "identifier", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create index on locks collection: %w", err)
	}

	return nil
}

// RecordPending marks a migration as pending
func (t *MigrationTracker) RecordPending(ctx context.Context, name, content string) error {
	err := t.recordPendingInternal(ctx, name, content, time.Now())
	if err != nil {
		return fmt.Errorf("failed to record pending migration: %w", err)
	}
	return nil
}

func (t *MigrationTracker) recordPendingInternal(ctx context.Context, name, content string, startTime time.Time) error {
	_, err := t.db.Collection(migrationsCollection).UpdateOne(
		ctx,
		bson.M{
			"name": name,
			"$or": []bson.M{
				{"status": bson.M{"$ne": "completed"}},
				{"success": false},
			},
		},
		bson.M{
			"$set": bson.M{
				"name":       name,
				"content":    content,
				"status":     "pending",
				"started_at": startTime,
				"host_id":    t.hostID,
			},
		},
		options.Update().SetUpsert(true),
	)

	return err
}

// AcquireLock attempts to acquire a distributed lock for migrations
func (t *MigrationTracker) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, string, error) {
	slog.Debug("[MongoDB] Driver attempting to acquire lock",
		slog.String("hostID", t.hostID),
		slog.Duration("timeout", lockTimeout))

	lockID := primitive.NewObjectID().Hex()
	lockCollection := t.db.Collection(locksCollectionName)
	now := time.Now()
	expiresAt := now.Add(lockTimeout)

	lockDoc := bson.M{
		"identifier": migrationLock,
		"locked_by":  t.hostID,
		"locked_at":  now,
		"lock_id":    lockID,
		"expires_at": expiresAt,
	}

	// Try to acquire lock
	result, err := lockCollection.UpdateOne(
		ctx,
		bson.M{
			"$or": []bson.M{
				{"identifier": migrationLock, "expires_at": bson.M{"$lt": now}}, // Expired lock
				{"identifier": bson.M{"$exists": false}},                        // No lock exists
			},
		},
		bson.M{"$set": lockDoc},
		options.Update().SetUpsert(true),
	)

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Someone else holds the lock - get details
			var existingLock struct {
				LockedBy  string    `bson:"locked_by"`
				ExpiresAt time.Time `bson:"expires_at"`
			}

			err := lockCollection.FindOne(ctx, bson.M{"identifier": migrationLock}).Decode(&existingLock)
			if err != nil {
				return false, "", fmt.Errorf("lock exists but couldn't get details: %w", err)
			}

			return false, "", fmt.Errorf("lock held by %s until %s",
				existingLock.LockedBy, existingLock.ExpiresAt.Format(time.RFC3339))
		}
		return false, "", fmt.Errorf("failed to acquire lock: %w", err)
	}

	// If we modified an existing doc or inserted a new one
	if result.ModifiedCount > 0 || result.UpsertedCount > 0 {
		return true, lockID, nil
	}

	return false, "", fmt.Errorf("failed to acquire lock: no document matched or was inserted")
}

// ReleaseLock releases the distributed lock
func (t *MigrationTracker) ReleaseLock(ctx context.Context, lockID string) error {
	slog.Debug("[MongoDB] Driver releasing lock",
		slog.String("hostID", t.hostID),
		slog.String("lockID", lockID))

	lockCollection := t.db.Collection(locksCollectionName)

	_, err := lockCollection.DeleteOne(ctx, bson.M{
		"identifier": migrationLock,
		"locked_by":  t.hostID,
		"lock_id":    lockID,
	})

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	slog.Debug("[MongoDB] Driver released lock",
		slog.String("hostID", t.hostID),
		slog.String("lockID", lockID))

	return nil
}

// RecordComplete updates a migration record as completed
func (t *MigrationTracker) RecordComplete(ctx context.Context, name string, success bool, executionMs int64, errorMsg string) error {
	err := t.recordCompleteInternal(ctx, name, success, executionMs, errorMsg, time.Now())

	if err != nil {
		return fmt.Errorf("failed to record migration result: %w", err)
	}

	return nil
}

func (t *MigrationTracker) recordCompleteInternal(ctx context.Context, name string, success bool, executionMs int64, errorMsg string, endTime time.Time) error {
	_, err := t.db.Collection(migrationsCollection).UpdateOne(
		ctx,
		bson.M{"name": name},
		bson.M{
			"$set": bson.M{
				"status":       "completed",
				"executed_at":  endTime,
				"execution_ms": executionMs,
				"success":      success,
				"error":        errorMsg,
			},
		},
	)

	return err
}

// GetExecutedMigrations returns all migration records
func (t *MigrationTracker) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	cursor, err := t.db.Collection(migrationsCollection).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "executed_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}
	defer cursor.Close(ctx)

	var migrations []driver.MigrationRecord
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode migration: %w", err)
		}

		record := driver.MigrationRecord{
			Name:    doc["name"].(string),
			Content: doc["content"].(string),
		}

		// Convert MongoDB-specific types to standard types
		if id, ok := doc["_id"].(primitive.ObjectID); ok {
			record.ID = id.Hex()
		}

		if status, ok := doc["status"].(string); ok {
			record.Status = status
		}

		if executedAt, ok := doc["executed_at"].(primitive.DateTime); ok {
			record.ExecutedAt = executedAt.Time()
		}

		if startedAt, ok := doc["started_at"].(primitive.DateTime); ok {
			record.StartedAt = startedAt.Time()
		}

		if executionMs, ok := doc["execution_ms"].(int64); ok {
			record.ExecutionMs = executionMs
		} else if executionMs, ok := doc["execution_ms"].(int32); ok {
			record.ExecutionMs = int64(executionMs)
		}

		if success, ok := doc["success"].(bool); ok {
			record.Success = success
		}

		if errorMsg, ok := doc["error"].(string); ok {
			record.Error = errorMsg
		}

		if hostID, ok := doc["host_id"].(string); ok {
			record.HostID = hostID
		}

		migrations = append(migrations, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return migrations, nil
}
