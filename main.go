// cmd/dbmigrator/main.go
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/ahbrown41/dbmigrator/pkg/driver"
	"github.com/ahbrown41/dbmigrator/pkg/driver/couchbase"
	"github.com/ahbrown41/dbmigrator/pkg/driver/mongodb"
	"github.com/ahbrown41/dbmigrator/pkg/driver/postgres"
	"github.com/ahbrown41/dbmigrator/pkg/migrator"
	"github.com/couchbase/gocb/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"log/slog"
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
	scopeName := flag.String("scope", "", "Scope name (for Couchbase)")
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
		db, err := sql.Open("postgres", *connStr)
		if err != nil {
			slog.Error("Failed to open PostgreSQL connection", slog.String("db", *connStr), slog.String("error", err.Error()))
		}
		defer db.Close()
		// Verify connection works
		if err := db.PingContext(ctx); err != nil {
			slog.Error("Failed to Ping PostgreSQL connection", slog.String("db", *connStr), slog.String("error", err.Error()))
		}
		d = postgres.New(db, *dbName)
	case "mongodb":
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(*connStr))
		if err != nil {
			slog.Error("Failed to open MongoDB connection", slog.String("db", *connStr), slog.String("error", err.Error()))
		}
		// Ping database to verify connection
		if err = client.Ping(context.Background(), nil); err != nil {
			slog.Error("Failed to Ping MongoDB connection", slog.String("db", *connStr), slog.String("error", err.Error()))
		}
		defer client.Disconnect(ctx)
		d, err = mongodb.New(client, *dbName)
	case "couchbase":
		cluster, err := gocb.Connect(*connStr, gocb.ClusterOptions{
			Username: *username,
			Password: *password,
		})
		if err != nil {
			slog.Error("Failed to open Couchbase connection", slog.String("db", *connStr), slog.String("error", err.Error()))
		}
		defer func() {
			cluster.Close(nil)
		}()
		d, err = couchbase.New(cluster, *bucketName, *scopeName)
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
