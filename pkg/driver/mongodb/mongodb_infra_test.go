package mongodb_test

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBContainer represents a MongoDB container for testing
type MongoDBContainer struct {
	container testcontainers.Container
	URI       string
	Client    *mongo.Client
	tempDir   string
}

// NewMongoDBContainer creates a new MongoDB container configured as a replica set with auth
func NewMongoDBContainer(ctx context.Context, t *testing.T) (*MongoDBContainer, error) {
	// Create temp directory for keyfile
	tempDir := t.TempDir()

	// Create keyfile
	keyfilePath := filepath.Join(tempDir, "mongo-keyfile")
	if err := createKeyfile(keyfilePath); err != nil {
		return nil, fmt.Errorf("failed to create keyfile: %w", err)
	}

	// MongoDB port
	mongoPort := "27017/tcp"

	// Container request with replica set configuration
	req := testcontainers.ContainerRequest{
		Image:        "mongo:8.0.5",
		ExposedPorts: []string{mongoPort},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      keyfilePath,
				ContainerFilePath: "/data/configdb/keyfile",
				FileMode:          0400, // Read-only for owner
			},
		},
		Cmd: []string{
			"mongod",
			"--replSet", "rs0",
			"--bind_ip_all",
			"--keyFile", "/data/configdb/keyfile",
		},
		WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(time.Second * 60),
	}
	// Start container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Get host and port
	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	port, err := container.MappedPort(ctx, nat.Port(mongoPort))
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to get container port: %w", err)
	}

	// Wait for MongoDB to be ready
	time.Sleep(2 * time.Second)

	// Construct MongoDB URI for initial connection (without auth)
	uri := fmt.Sprintf("mongodb://%s:%s", host, port.Port())

	// Connect to MongoDB with short timeout
	clientOpts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second).
		SetConnectTimeout(5 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = client.Ping(pingCtx, nil)
	cancel()
	if err != nil {
		client.Disconnect(ctx)
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Initialize replica set using JS command
	err = initReplicaSet(ctx, client, host, port.Port())
	if err != nil {
		client.Disconnect(ctx)
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to initialize replica set: %w", err)
	}

	// Wait for replica set to initialize
	time.Sleep(5 * time.Second)

	// Disconnect initial client
	client.Disconnect(ctx)

	// Reconnect with replica set name
	uri = fmt.Sprintf("mongodb://%s:%s/?replicaSet=rs0", host, port.Port())
	clientOpts = options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(10 * time.Second).
		SetConnectTimeout(10 * time.Second)

	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to reconnect to MongoDB: %w", err)
	}

	// Wait for primary to be elected
	err = waitForPrimary(ctx, client)
	if err != nil {
		client.Disconnect(ctx)
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed waiting for primary: %w", err)
	}

	// Create admin user
	err = createAdminUser(ctx, client)
	if err != nil {
		client.Disconnect(ctx)
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to create admin user: %w", err)
	}

	// Disconnect client
	client.Disconnect(ctx)

	// Restart container with auth enabled
	err = container.Stop(ctx, nil)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to stop container: %w", err)
	}

	// Copy keyfile to container
	err = container.CopyFileToContainer(ctx, keyfilePath, "/data/keyfile", 0400)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to copy keyfile: %w", err)
	}

	// Start container with auth
	_, _, err = container.Exec(ctx, []string{"chmod", "400", "/data/keyfile"})
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to set keyfile permissions: %w", err)
	}

	// Start container with auth
	err = container.Start(ctx)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to restart container: %w", err)
	}

	// Wait for MongoDB to be ready again
	time.Sleep(5 * time.Second)

	// Reconnect with auth and replica set name
	uri = fmt.Sprintf("mongodb://admin:password@%s:%s/admin?replicaSet=rs0&authSource=admin", host, port.Port())
	clientOpts = options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(10 * time.Second)

	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to reconnect to MongoDB with auth: %w", err)
	}

	// Ping to verify connection
	pingCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = client.Ping(pingCtx, nil)
	if err != nil {
		client.Disconnect(ctx)
		container.Terminate(ctx)
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to ping MongoDB with auth: %w", err)
	}

	return &MongoDBContainer{
		container: container,
		URI:       uri,
		Client:    client,
		tempDir:   tempDir,
	}, nil
}

// createKeyfile creates a keyfile for MongoDB authentication
func createKeyfile(path string) error {
	// Generate a random string for the keyfile
	keyfileContent := "8cNNzoSZzAiZXMTUbNnd9Fmi3KfTaVjj48FwQGGN9Xj5JNCjXBFvCVNpBB4bYgFL\n"

	// Write to file
	err := os.WriteFile(path, []byte(keyfileContent), 0600)
	if err != nil {
		return err
	}

	return nil
}

// initReplicaSet initializes the MongoDB replica set
func initReplicaSet(ctx context.Context, client *mongo.Client, host, port string) error {
	// Replica set configuration
	rsConfig := bson.M{
		"_id": "rs0",
		"members": []bson.M{
			{
				"_id":      0,
				"host":     fmt.Sprintf("%s:%s", host, port),
				"priority": 1,
			},
		},
	}

	// Run initiate command
	cmd := bson.D{{"replSetInitiate", rsConfig}}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

// waitForPrimary waits for a primary to be elected in the replica set
func waitForPrimary(ctx context.Context, client *mongo.Client) error {
	// Create a context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Wait for primary to be elected
	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for primary")
		default:
			// Check if primary is available
			err := client.Ping(ctx, readpref.Primary())
			if err == nil {
				return nil
			}

			// Wait a bit before trying again
			time.Sleep(1 * time.Second)
		}
	}
}

// createAdminUser creates an admin user for authentication
func createAdminUser(ctx context.Context, client *mongo.Client) error {
	cmd := bson.D{
		{"createUser", "admin"},
		{"pwd", "password"},
		{"roles", bson.A{
			bson.M{"role": "root", "db": "admin"},
		}},
	}

	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

// Cleanup stops the container and disconnects the client
func (m *MongoDBContainer) Cleanup(ctx context.Context) error {
	if m.Client != nil {
		if err := m.Client.Disconnect(ctx); err != nil {
			return fmt.Errorf("failed to disconnect client: %w", err)
		}
	}

	if m.container != nil {
		if err := m.container.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate container: %w", err)
		}
	}

	// Clean up temp directory
	if m.tempDir != "" {
		os.RemoveAll(m.tempDir)
	}

	return nil
}
