// cmd/dbmigrator/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/ahbrown41/dbmigrator/pkg/driver/couchbase"
	"github.com/ahbrown41/dbmigrator/pkg/driver/mongodb"
	"github.com/ahbrown41/dbmigrator/pkg/driver/postgres"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
	"log"
	"os"
	"strings"
)

func main() {
	dbType := flag.String("type", "", "Database type (postgres, mongodb, couchbase)")
	connStr := flag.String("conn", "", "Connection string")
	migrationsDir := flag.String("dir", "./migrations", "Migrations directory")
	dbName := flag.String("db", "", "Database name (for MongoDB)")
	username := flag.String("user", "", "Username (for Couchbase)")
	password := flag.String("pass", "", "Password (for Couchbase)")
	bucketName := flag.String("bucket", "", "Bucket name (for Couchbase)")
	flag.Parse()

	if *dbType == "" || *connStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Create the appropriate driver
	var d driver.Driver
	var err error

	ctx := context.Background()

	switch strings.ToLower(*dbType) {
	case "postgres":
		d, err = postgres.NewDriver(*connStr)
	case "mongodb":
		d, err = mongodb.NewDriver(*connStr, *dbName)
	case "couchbase":
		d, err = couchbase.NewDriver(*connStr, *username, *password, *bucketName)
	default:
		log.Fatalf("Unsupported database type: %s", *dbType)
	}

	if err != nil {
		log.Fatalf("Failed to create driver: %v", err)
	}
	defer d.Close()

	// Create and run migrator
	m := migrator.New(d, *migrationsDir)
	if err := m.Run(ctx); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Println("Migrations completed successfully")
}
