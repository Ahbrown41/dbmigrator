package postgres

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
	"testing"

	"github.com/ahbrown41/dbmigrator/pkg/migrator"
)

func TestPostgresDriver(t *testing.T) {
	ctx := context.Background()

	// Start Postgres container
	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:13",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_USER":     "test",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}
	defer pgContainer.Terminate(ctx)

	// Get connection details
	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get host: %v", err)
	}

	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get port: %v", err)
	}

	// Create connection string
	connStr := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable",
		host, port.Port())

	// Create temp migration dir
	migrationDir, err := os.MkdirTemp("", "migrations")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(migrationDir)

	// Create test migrations
	migrations := map[string]string{
		"001_create_users.sql": `CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        );`,
		"002_add_user.sql": `INSERT INTO users (name, email) 
            VALUES ('test', 'test@example.com');`,
	}

	for name, content := range migrations {
		path := filepath.Join(migrationDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write migration %s: %v", name, err)
		}
	}

	// Create driver
	driver, err := NewDriver(connStr)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}
	defer driver.Close()

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

	if len(executed) != 2 {
		t.Fatalf("Expected 2 migrations, got %d", len(executed))
	}

	// Verify data was inserted
	var count int
	err = driver.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if count != 1 {
		t.Fatalf("Expected 1 user, got %d", count)
	}
}
