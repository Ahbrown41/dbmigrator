package mongodb_test

import (
	"context"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"testing"
)

// MongoDBContainer holds the resources for the MongoDB test container
type MongoDBContainer struct {
	container *mongodb.MongoDBContainer
}

// NewMongoDBContainer creates a new MongoDB container for testing
func NewMongoDBContainer(ctx context.Context, t *testing.T) *MongoDBContainer {
	container, err := mongodb.Run(ctx, "mongo:8")
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	return &MongoDBContainer{
		container: container,
	}
}

func (c *MongoDBContainer) ConnectionString(ctx context.Context) (string, error) {
	return c.container.ConnectionString(ctx)
}

// Cleanup removes the MongoDB container
func (c *MongoDBContainer) Stop(ctx context.Context, t *testing.T) {
	if c.container != nil {
		if err := c.container.Terminate(ctx); err != nil {
			t.Logf("Failed to remove MongoDB container: %v", err)
		}
	}
}
