package couchbase

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ahbrown41/dbmigrator/pkg/migrator"
)

func TestCouchbaseDriver(t *testing.T) {
	// Skip if running in CI environment without Docker
	if os.Getenv("CI") != "" && os.Getenv("DOCKER_AVAILABLE") != "true" {
		t.Skip("Skipping Couchbase test in CI environment without Docker")
	}

	ctx := context.Background()

	// Start Couchbase container
	couchbaseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "couchbase:7.6.5",
			ExposedPorts: []string{
				"8091/tcp",  // Web UI
				"8092/tcp",  // API
				"8093/tcp",  // Query
				"8094/tcp",  // Search
				"11210/tcp", // Data
			},
			WaitingFor: wait.ForHTTP("/ui/index.html").WithPort("8091/tcp"),
			Env: map[string]string{
				"COUCHBASE_ADMINISTRATOR_USERNAME": "Administrator",
				"COUCHBASE_ADMINISTRATOR_PASSWORD": "password",
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Failed to start Couchbase container: %v", err)
	}
	defer couchbaseContainer.Terminate(ctx)

	// Get connection details
	host, err := couchbaseContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get host: %v", err)
	}

	// Setup credentials
	username := "Administrator"
	password := "password"
	rbac_user := "testuser"
	rbac_pass := "testpass"
	bucketName := "testbucket"

	// Get management port
	mgmtPort, err := couchbaseContainer.MappedPort(ctx, "8091/tcp")
	if err != nil {
		t.Fatalf("Failed to get port: %v", err)
	}

	// Initialize Couchbase cluster
	baseURL := fmt.Sprintf("http://%s:%s", host, mgmtPort.Port())

	// Step 1: Initialize node
	t.Log("Initializing Couchbase node...")
	initNodeData := "memoryQuota=512&indexMemoryQuota=512&ftsMemoryQuota=256&cbasMemoryQuota=1024&kv=on&n1ql=on&index=on&fts=on"
	req, _ := http.NewRequest("POST", baseURL+"/nodes/self/controller/settings", bytes.NewBufferString(initNodeData))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to initialize node: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to initialize node, status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Step 2: Set up services
	t.Log("Setting up services...")
	servicesData := "services=kv%2Cn1ql%2Cindex"
	req, _ = http.NewRequest("POST", baseURL+"/node/controller/setupServices", bytes.NewBufferString(servicesData))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to setup services: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to initialize node, status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Step 3: Set admin credentials
	t.Log("Setting admin credentials...")
	credentialsData := fmt.Sprintf("password=%s&username=%s&port=SAME", password, username)
	req, _ = http.NewRequest("POST", baseURL+"/settings/web", bytes.NewBufferString(credentialsData))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to set credentials: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to initialize node, status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Step 4: Create bucket
	t.Log("Creating bucket...")
	bucketData := fmt.Sprintf("name=%s&bucketType=couchbase&ramQuotaMB=128&replicaNumber=0", bucketName)
	req, _ = http.NewRequest("POST", baseURL+"/pools/default/buckets", bytes.NewBufferString(bucketData))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(username, password)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to initialize node, status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Step 5: Create RBAC user with access to the bucket
	t.Log("Creating RBAC user...")
	rbacUserData := fmt.Sprintf("name=%s&password=%s&roles=bucket_full_access[%s],query_select[%s],query_update[%s],query_insert[%s],query_delete[%s]", rbac_user, rbac_pass, bucketName, bucketName, bucketName, bucketName, bucketName)
	req, _ = http.NewRequest("PUT", baseURL+"/settings/rbac/users/local/testuser", bytes.NewBufferString(rbacUserData))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(username, password)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create RBAC user: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to create RBAC user, status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Wait for bucket and user to be ready
	t.Log("Waiting for bucket and user to be ready...")
	time.Sleep(15 * time.Second)

	clientPort, err := couchbaseContainer.MappedPort(ctx, "8093/tcp")
	if err != nil {
		t.Fatalf("Failed to get port: %v", err)
	}

	// Create connection string
	connStr := fmt.Sprintf("couchbase://%s:%s", host, clientPort.Port())

	// Create temp migration dir
	migrationDir, err := os.MkdirTemp("", "couchbase-migrations")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(migrationDir)

	// Create test migrations
	migrations := map[string]string{
		"001_create_users_index.n1ql": `
            CREATE PRIMARY INDEX ON ${BUCKET};
            CREATE INDEX idx_users_email ON ${BUCKET}(email) WHERE type = "user";
        `,
		"002_add_user.n1ql": `
            INSERT INTO ${BUCKET} (KEY, VALUE)
            VALUES ("user::1", {
                "type": "user",
                "id": "1",
                "name": "test",
                "email": "test@example.com",
                "createdAt": NOW_STR()
            });
        `,
		"003_create_posts_index.n1ql": `
            CREATE INDEX idx_posts_author ON ${BUCKET}(author) WHERE type = "post";
        `,
		"004_add_post.n1ql": `
            INSERT INTO ${BUCKET} (KEY, VALUE)
            VALUES ("post::1", {
                "type": "post",
                "id": "1",
                "title": "Test Post",
                "content": "This is a test post",
                "author": "test@example.com",
                "createdAt": NOW_STR()
            });
        `,
	}

	for name, content := range migrations {
		path := filepath.Join(migrationDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write migration %s: %v", name, err)
		}
	}

	// Create driver with the RBAC user credentials
	t.Log("Creating driver...")
	driver, err := NewDriver(connStr, rbac_user, rbac_pass, bucketName)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}
	defer driver.Close()

	// Test connection to verify credentials work
	t.Log("Testing connection...")
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: rbac_user,
		Password: rbac_pass,
	})
	if err != nil {
		t.Fatalf("Failed to connect to Couchbase with RBAC user: %v", err)
	}

	// Ping bucket to verify access
	bucket := cluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		t.Fatalf("Failed to access bucket with RBAC user: %v", err)
	}
	cluster.Close(nil)

	// Test basic migration functionality
	t.Run("BasicMigration", func(t *testing.T) {
		// Create driver
		driver, err := NewDriver(connStr, username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create driver: %v", err)
		}
		defer driver.Close()

		// Initialize the driver
		if err := driver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize driver: %v", err)
		}

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

		if len(executed) != 4 {
			t.Fatalf("Expected 4 migrations, got %d", len(executed))
		}

		// Verify data was inserted by connecting directly to Couchbase
		cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
			Username: username,
			Password: password,
		})
		if err != nil {
			t.Fatalf("Failed to connect to Couchbase: %v", err)
		}
		defer cluster.Close(nil)

		// Count users
		userCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "user"
        `, bucketName)

		userResult, err := cluster.Query(userCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count users: %v", err)
		}
		defer userResult.Close()

		var userCount struct {
			Count int `json:"count"`
		}

		if userResult.Next() {
			if err := userResult.Row(&userCount); err != nil {
				t.Fatalf("Failed to parse user count: %v", err)
			}
		}

		if userCount.Count != 1 {
			t.Fatalf("Expected 1 user, got %d", userCount.Count)
		}

		// Count posts
		postCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "post"
        `, bucketName)

		postResult, err := cluster.Query(postCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count posts: %v", err)
		}
		defer postResult.Close()

		var postCount struct {
			Count int `json:"count"`
		}

		if postResult.Next() {
			if err := postResult.Row(&postCount); err != nil {
				t.Fatalf("Failed to parse post count: %v", err)
			}
		}

		if postCount.Count != 1 {
			t.Fatalf("Expected 1 post, got %d", postCount.Count)
		}

		// Run migrations again - should be idempotent
		if err := m.Run(ctx); err != nil {
			t.Fatalf("Second migration run failed: %v", err)
		}

		// Verify no additional migrations were executed
		executed, err = driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations after second run: %v", err)
		}

		if len(executed) != 4 {
			t.Fatalf("Expected still 4 migrations after second run, got %d", len(executed))
		}
	})

	// Reset bucket for concurrency test
	resetCluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		t.Fatalf("Failed to connect to Couchbase: %v", err)
	}

	// Flush bucket to start fresh
	err = resetCluster.Buckets().FlushBucket(bucketName, nil)
	if err != nil {
		t.Fatalf("Failed to flush bucket: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(5 * time.Second)
	resetCluster.Close(nil)

	// Test concurrent migrations
	t.Run("ConcurrentMigration", func(t *testing.T) {
		// Create multiple migrators to simulate concurrent processes
		const numConcurrent = 5
		var wg sync.WaitGroup

		// Channel to collect errors
		errCh := make(chan error, numConcurrent)

		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// First, create and initialize a driver to set up the collections and indexes
		setupDriver, err := NewDriver(connStr, username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create setup driver: %v", err)
		}

		if err := setupDriver.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize setup driver: %v", err)
		}
		setupDriver.Close()

		// Start concurrent migrations
		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(instanceID int) {
				defer wg.Done()

				// Create a new driver for each instance
				driver, err := NewDriver(connStr, username, password, bucketName)
				if err != nil {
					errCh <- fmt.Errorf("instance %d: failed to create driver: %w", instanceID, err)
					return
				}
				defer driver.Close()

				// Create migrator with lock retry options
				m := migrator.New(
					driver,
					migrationDir,
					migrator.WithLockTimeout(20*time.Second),
					migrator.WithLockRetryInterval(1*time.Second),
					migrator.WithMaxLockRetries(30),
				)

				// Run migrations
				if err := m.Run(ctx); err != nil {
					errCh <- fmt.Errorf("instance %d: migration failed: %w", instanceID, err)
					return
				}

				t.Logf("Instance %d completed migrations successfully", instanceID)
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(errCh)

		// Check for errors
		var errors []error
		for err := range errCh {
			errors = append(errors, err)
		}

		if len(errors) > 0 {
			for _, err := range errors {
				t.Errorf("Concurrent migration error: %v", err)
			}
			t.Fatalf("Concurrent migrations had %d errors", len(errors))
		}

		// Verify final state
		driver, err := NewDriver(connStr, username, password, bucketName)
		if err != nil {
			t.Fatalf("Failed to create verification driver: %v", err)
		}
		defer driver.Close()

		// Check migrations were recorded exactly once
		executed, err := driver.GetExecutedMigrations(ctx)
		if err != nil {
			t.Fatalf("Failed to get migrations: %v", err)
		}

		if len(executed) != 4 {
			t.Fatalf("Expected exactly 4 migrations, got %d", len(executed))
		}

		// Connect to verify data
		cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
			Username: username,
			Password: password,
		})
		if err != nil {
			t.Fatalf("Failed to connect to Couchbase: %v", err)
		}
		defer cluster.Close(nil)

		// Count users
		userCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "user"
        `, bucketName)

		userResult, err := cluster.Query(userCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count users: %v", err)
		}
		defer userResult.Close()

		var userCount struct {
			Count int `json:"count"`
		}

		if userResult.Next() {
			if err := userResult.Row(&userCount); err != nil {
				t.Fatalf("Failed to parse user count: %v", err)
			}
		}

		if userCount.Count != 1 {
			t.Fatalf("Expected exactly 1 user, got %d", userCount.Count)
		}

		// Count posts
		postCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "post"
        `, bucketName)

		postResult, err := cluster.Query(postCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count posts: %v", err)
		}
		defer postResult.Close()

		var postCount struct {
			Count int `json:"count"`
		}

		if postResult.Next() {
			if err := postResult.Row(&postCount); err != nil {
				t.Fatalf("Failed to parse post count: %v", err)
			}
		}

		if postCount.Count != 1 {
			t.Fatalf("Expected exactly 1 post, got %d", postCount.Count)
		}

		// Check lock collection is empty (all locks released)
		lockCountQuery := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE type = "lock"
        `, bucketName)

		lockResult, err := cluster.Query(lockCountQuery, nil)
		if err != nil {
			t.Fatalf("Failed to count locks: %v", err)
		}
		defer lockResult.Close()

		var lockCount struct {
			Count int `json:"count"`
		}

		if lockResult.Next() {
			if err := lockResult.Row(&lockCount); err != nil {
				t.Fatalf("Failed to parse lock count: %v", err)
			}
		}

		if lockCount.Count != 0 {
			t.Fatalf("Expected 0 locks remaining, got %d", lockCount.Count)
		}
	})
}
