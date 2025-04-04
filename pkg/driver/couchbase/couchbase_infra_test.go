package couchbase_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	imageName  = "couchbase:7.6.5"
	MgmtPort   = "8091"
	QueryPort  = "8093"
	KvPort     = "11210"
	ViewPort   = "8092"
	initScript = `#!/bin/sh
  #!/bin/sh

  # Start Couchbase server in the background
  /entrypoint.sh couchbase-server &

  # Wait for Couchbase server to start (basic UI access)
  until curl -s http://127.0.0.1:8091/ui/index.html > /dev/null; do
   echo "Waiting for Couchbase server to start..."
   sleep 5
  done

  echo "Couchbase server started."

  # Provision Node
  couchbase-cli cluster-init \
    -c 127.0.0.1:8091 \
    --cluster-username ${ADMIN_USERNAME} \
    --cluster-password ${ADMIN_PASSWORD} \
    --services data,index,query \
    --cluster-ramsize 512 \
    --cluster-index-ramsize 256

  # General Settings
  couchbase-cli setting-cluster \
    -c 127.0.0.1:8091 \
    --username ${ADMIN_USERNAME} \
    --password ${ADMIN_PASSWORD} \
    --cluster-ramsize ${MEMORY_QUOTA} \
    --cluster-name 127.0.0.1 \
    --cluster-index-ramsize ${INDEX_MEMORY_QUOTA} \
    --cluster-fts-ramsize ${FTS_MEMORY_QUOTA}

  # Set up index settings
  couchbase-cli setting-index \
    -c 127.0.0.1:8091 \
    --username ${ADMIN_USERNAME} \
    --password ${ADMIN_PASSWORD} \
    --index-log-level info \
    --index-stable-snapshot-interval 40000 \
    --index-memory-snapshot-interval 150 \
    --index-storage-setting default \
    --index-threads 8 \
    --index-max-rollback-points 10

  # Provision Bucket
  couchbase-cli bucket-create \
    -c 127.0.0.1:8091 \
    --username ${ADMIN_USERNAME} \
    --password ${ADMIN_PASSWORD} \
    --bucket ${BUCKET_NAME} \
    --bucket-type couchbase \
    --bucket-ramsize ${BUCKET_RAM_QUOTA} \
    --enable-flush 1 \
    --wait

  echo "Couchbase setup completed successfully."

  # Keep the Couchbase server running
  wait
 `
)

// Global singleton instance of CouchbaseServer
var (
	globalCouchbaseServer *CouchbaseServer
	serverInitOnce        sync.Once
	serverInitError       error
)

type CouchbaseServer struct {
	container    testcontainers.Container
	clientConStr string
	adminConStr  string
	username     string
	password     string
	bucketName   string
	mu           sync.Mutex // To protect concurrent access to the server
}

func NewCouchbaseServer(username, password, bucketName string) *CouchbaseServer {
	return &CouchbaseServer{
		username:   username,
		password:   password,
		bucketName: bucketName,
	}
}

// GetCouchbaseServer returns the singleton instance of CouchbaseServer
func GetCouchbaseServer(t *testing.T, username, password, bucketName string) (*CouchbaseServer, error) {
	serverInitOnce.Do(func() {
		globalCouchbaseServer = NewCouchbaseServer(username, password, bucketName)
		serverInitError = globalCouchbaseServer.startInternal(t)
		if serverInitError != nil {
			t.Logf("Failed to initialize Couchbase server: %v", serverInitError)
		} else {
			// Register cleanup on test completion
			t.Cleanup(func() {
				ctx := context.Background()
				if globalCouchbaseServer != nil && globalCouchbaseServer.container != nil {
					if err := globalCouchbaseServer.container.Terminate(ctx); err != nil {
						t.Logf("Failed to terminate Couchbase container: %v", err)
					}
					globalCouchbaseServer = nil
				}
			})
		}
	})

	return globalCouchbaseServer, serverInitError
}

// Start initializes the Couchbase server if it hasn't been initialized already
func (c *CouchbaseServer) Start(t *testing.T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if globalCouchbaseServer != nil && globalCouchbaseServer.container != nil {
		// Server already started, nothing to do
		return
	}

	if err := c.startInternal(t); err != nil {
		t.Fatalf("Failed to start Couchbase server: %v", err)
	}
}

// startInternal contains the actual server startup logic
func (c *CouchbaseServer) startInternal(t *testing.T) error {
	ctx := context.Background()

	// Setup Script
	setupFile := filepath.Join(t.TempDir(), "setup_couchbase.sh")
	err := os.WriteFile(setupFile, []byte(initScript), 0600)
	if err != nil {
		return fmt.Errorf("failed to write setup script: %w", err)
	}
	defer func() {
		if err := os.Remove(setupFile); err != nil {
			t.Logf("Failed to remove setup script: %v", err)
		}
	}()

	c.container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: imageName,
			ExposedPorts: []string{
				"8091/tcp",  // Web UI
				"8092/tcp",  // API
				"8093/tcp",  // Query
				"8094/tcp",  // Search
				"11210/tcp", // Data
			},
			WaitingFor: wait.ForHTTP("/ui/index.html").WithPort("8091/tcp"),
			Env: map[string]string{
				"MEMORY_QUOTA":       "512",
				"INDEX_MEMORY_QUOTA": "512",
				"FTS_MEMORY_QUOTA":   "256",
				"CBAS_MEMORY_QUOTA":  "1024",
				"ADMIN_USERNAME":     c.username,
				"ADMIN_PASSWORD":     c.password,
				"BUCKET_NAME":        c.bucketName,
				"BUCKET_RAM_QUOTA":   "128",
			},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      setupFile,
					ContainerFilePath: "/setup_couchbase.sh",
					FileMode:          0400, // Read-only for owner
				},
			},
			Entrypoint: []string{"sh", "/setup_couchbase.sh"},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("failed to start Couchbase container: %w", err)
	}

	// Get Container Host and Port
	host, err := c.container.Host(ctx)
	if err != nil {
		return fmt.Errorf("failed to get container host: %w", err)
	}

	clientPort, err := c.container.MappedPort(ctx, nat.Port(KvPort))
	if err != nil {
		return fmt.Errorf("failed to get driver port: %w", err)
	}

	// Create connection string
	c.clientConStr = fmt.Sprintf("couchbase://%s:%s", host, clientPort.Port())

	t.Logf("Couchbase SDK URL %s", c.clientConStr)

	mgmtPort, err := c.container.MappedPort(ctx, nat.Port(MgmtPort))
	if err != nil {
		return fmt.Errorf("failed to get mgmt port: %w", err)
	}

	// Create management connection string
	c.adminConStr = fmt.Sprintf("http://%s:%s", host, mgmtPort.Port())

	t.Logf("Couchbase Admin URL %s", c.adminConStr)

	// Update Alternate Addresses
	urlStr := fmt.Sprintf("%s/node/controller/setupAlternateAddresses/external", c.adminConStr)
	params := url.Values{}
	params.Add("hostname", host)
	ports := map[string]string{"mgmt": MgmtPort, "kv": KvPort, "capi": ViewPort, "n1ql": QueryPort}
	for key, portName := range ports {
		if port, err := c.container.MappedPort(ctx, nat.Port(portName)); err != nil {
			return fmt.Errorf("failed to get mapped port %s: %w", portName, err)
		} else {
			params.Add(key, port.Port())
		}
	}
	body := []byte(params.Encode())
	req, err := http.NewRequest("PUT", urlStr, bytes.NewBuffer(body))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.username, c.password)
	req.Close = true

	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to connect to Couchbase server: %s", response.Status)
	}

	// Wait for Couchbase services to be available
	if err := c.waitForCouchbaseServices(ctx, host, mgmtPort.Port(), c.username, c.password); err != nil {
		return fmt.Errorf("failed waiting for Couchbase services: %w", err)
	}

	return nil
}

// Stop is a no-op for singleton usage, actual cleanup happens in the test.Cleanup function
func (c *CouchbaseServer) Stop(t *testing.T) {
	// No-op for compatibility with existing tests
	// Actual cleanup will happen via the t.Cleanup registered when the server is created
}

func (c *CouchbaseServer) ConnectionString() string {
	return c.clientConStr
}

func (c *CouchbaseServer) AdminConnectionString() string {
	return c.adminConStr
}

// FlushBucket flushes the specified bucket, useful for cleaning up between tests
func (c *CouchbaseServer) FlushBucket(t *testing.T) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	client := &http.Client{Timeout: 5 * time.Second}
	flushURL := fmt.Sprintf("%s/pools/default/buckets/%s/controller/doFlush", c.adminConStr, c.bucketName)

	req, err := http.NewRequest("POST", flushURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create flush request: %w", err)
	}

	req.SetBasicAuth(c.username, c.password)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute flush request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to flush bucket: HTTP status %d", resp.StatusCode)
	}

	// Wait for flush to complete with a small but sufficient delay
	time.Sleep(500 * time.Millisecond)
	return nil
}

// waitForCouchbaseServices polls the Couchbase services until they are all available
func (c *CouchbaseServer) waitForCouchbaseServices(ctx context.Context, host, port, username, password string) error {
	// Create HTTP client for API calls
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// URL for getting node services status
	nodeServicesURL := fmt.Sprintf("http://%s:%s/pools/default/nodeServices", host, port)

	// Retry configuration
	maxAttempts := 30
	retryInterval := 2 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Create request with basic auth
			req, err := http.NewRequestWithContext(ctx, "GET", nodeServicesURL, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.SetBasicAuth(username, password)

			// Send request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Attempt %d/%d: Service check failed: %v\n", attempt+1, maxAttempts, err)
				time.Sleep(retryInterval)
				continue
			}

			// Check if successful
			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				fmt.Printf("Attempt %d/%d: Service check returned status %s\n", attempt+1, maxAttempts, resp.Status)
				time.Sleep(retryInterval)
				continue
			}

			// Parse the response
			var servicesResp map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&servicesResp); err != nil {
				resp.Body.Close()
				fmt.Printf("Attempt %d/%d: Failed to parse service response: %v\n", attempt+1, maxAttempts, err)
				time.Sleep(retryInterval)
				continue
			}
			resp.Body.Close()

			// Check for needed services
			nodesArray, ok := servicesResp["nodesExt"].([]interface{})
			if !ok || len(nodesArray) == 0 {
				fmt.Printf("Attempt %d/%d: No nodes found in response\n", attempt+1, maxAttempts)
				time.Sleep(retryInterval)
				continue
			}

			// Assume first node should have all services we need
			node := nodesArray[0].(map[string]interface{})
			services, ok := node["services"].(map[string]interface{})
			if !ok {
				fmt.Printf("Attempt %d/%d: Node does not contain services section\n", attempt+1, maxAttempts)
				time.Sleep(retryInterval)
				continue
			}

			// Check for required services
			_, kvFound := services["kv"]
			_, n1qlFound := services["n1ql"]
			_, indexFound := services["indexHttp"]

			if kvFound && n1qlFound && indexFound {
				fmt.Println("All Couchbase services are available")
				return nil
			}

			fmt.Printf("Attempt %d/%d: Waiting for services - KV: %v, Query: %v, Index: %v\n",
				attempt+1, maxAttempts, kvFound, n1qlFound, indexFound)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("timeout waiting for Couchbase services to be available after %d attempts", maxAttempts)
}
