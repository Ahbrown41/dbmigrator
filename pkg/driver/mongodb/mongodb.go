// pkg/driver/mongodb/mongodb.go
package mongodb

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ahbrown41/dbmigrator/pkg/driver"
)

type MongoDriver struct {
	client   *mongo.Client
	db       *mongo.Database
	migColl  *mongo.Collection
	lockColl *mongo.Collection
	hostID   string
	lockID   string
}

func NewDriver(uri, dbName string) (*MongoDriver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, err
	}

	db := client.Database(dbName)
	migColl := db.Collection("schema_migrations")
	lockColl := db.Collection("schema_migration_locks")

	// Generate a unique host ID for this instance
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	hostID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	return &MongoDriver{
		client:   client,
		db:       db,
		migColl:  migColl,
		lockColl: lockColl,
		hostID:   hostID,
	}, nil
}

func (d *MongoDriver) Initialize(ctx context.Context) error {
	// Create unique index on name field for migrations
	_, err := d.migColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	// Create unique index on id field for locks
	_, err = d.lockColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	// Create TTL index on expires_at field for locks
	_, err = d.lockColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expires_at", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})

	return err
}

func (d *MongoDriver) AcquireLock(ctx context.Context, lockTimeout time.Duration) (bool, error) {
	// Generate a unique lock ID
	lockID := uuid.New().String()

	now := time.Now()
	expiresAt := now.Add(lockTimeout)

	// First, clean up expired locks (MongoDB TTL index should handle this, but just to be sure)
	_, err := d.lockColl.DeleteMany(ctx, bson.M{
		"expires_at": bson.M{"$lt": now},
	})
	if err != nil {
		return false, fmt.Errorf("failed to clean up expired locks: %w", err)
	}

	// Try to insert a new lock
	lock := bson.M{
		"id":         lockID,
		"created_at": now,
		"expires_at": expiresAt,
		"host_id":    d.hostID,
	}

	_, err = d.lockColl.InsertOne(ctx, lock)
	if err != nil {
		// If duplicate key error, another process has the lock
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert lock: %w", err)
	}

	// We got the lock
	d.lockID = lockID
	return true, nil
}

func (d *MongoDriver) ReleaseLock(ctx context.Context) error {
	if d.lockID == "" {
		return nil // No lock to release
	}

	_, err := d.lockColl.DeleteOne(ctx, bson.M{
		"id":      d.lockID,
		"host_id": d.hostID,
	})

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	d.lockID = ""
	return nil
}

func (d *MongoDriver) ExecuteMigration(ctx context.Context, name, content string) error {
	// First check if this migration has already been executed
	// This is important for concurrent scenarios
	count, err := d.migColl.CountDocuments(ctx, bson.M{"name": name})
	if err != nil {
		return fmt.Errorf("failed to check if migration exists: %w", err)
	}

	if count > 0 {
		// Migration already executed, skip it
		return nil
	}

	startTime := time.Now()

	// Parse JavaScript content into BSON
	var doc bson.D
	var execErr error
	err = bson.UnmarshalExtJSON([]byte(content), true, &doc)

	if err != nil {
		execErr = err
	} else {
		// Use a session with transaction if possible
		err = d.db.Client().UseSession(ctx, func(sc mongo.SessionContext) error {
			if err := sc.StartTransaction(); err != nil {
				return err
			}

			// Execute the commands in the document
			for _, elem := range doc {
				command := bson.D{{Key: elem.Key, Value: elem.Value}}
				if err := d.db.RunCommand(sc, command).Err(); err != nil {
					sc.AbortTransaction(sc)
					return err
				}
			}

			return sc.CommitTransaction(sc)
		})

		if err != nil {
			execErr = err
		}
	}

	endTime := time.Now()
	executionMs := endTime.Sub(startTime).Milliseconds()

	// Record migration
	record := bson.M{
		"name":         name,
		"content":      content,
		"executed_at":  endTime,
		"execution_ms": executionMs,
		"success":      execErr == nil,
	}

	if execErr != nil {
		record["error"] = execErr.Error()
	}

	// Use upsert to handle concurrent inserts
	_, err = d.migColl.UpdateOne(
		ctx,
		bson.M{"name": name},
		bson.M{"$setOnInsert": record},
		options.Update().SetUpsert(true),
	)

	if err != nil {
		// If it's a duplicate key error, another process has already recorded this migration
		if mongo.IsDuplicateKeyError(err) {
			// Check if the migration was successful
			var existingMigration bson.M
			err = d.migColl.FindOne(ctx, bson.M{"name": name}).Decode(&existingMigration)
			if err != nil {
				return fmt.Errorf("failed to check existing migration: %w", err)
			}

			success, ok := existingMigration["success"].(bool)
			if !ok || !success {
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

func (d *MongoDriver) GetExecutedMigrations(ctx context.Context) ([]driver.MigrationRecord, error) {
	cursor, err := d.migColl.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "executed_at", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var migrations []driver.MigrationRecord
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		record := driver.MigrationRecord{
			Name:    doc["name"].(string),
			Content: doc["content"].(string),
		}

		if executedAt, ok := doc["executed_at"].(primitive.DateTime); ok {
			record.ExecutedAt = executedAt.Time()
		}

		if executionMs, ok := doc["execution_ms"].(int64); ok {
			record.ExecutionMs = executionMs
		}

		if success, ok := doc["success"].(bool); ok {
			record.Success = success
		}

		if errorMsg, ok := doc["error"].(string); ok {
			record.Error = errorMsg
		}

		migrations = append(migrations, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (d *MongoDriver) Close() error {
	// Release lock if we have one
	if d.lockID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = d.ReleaseLock(ctx) // Best effort
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.client.Disconnect(ctx)
}
