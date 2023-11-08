package spanner_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/spanner"
)

func TestConnectDB(t *testing.T) {
	const projectID = "local-spanner-20231030"
	const instanceID = "instance1"
	const databaseID = "database1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	instance, err := spanner.NewInstanceAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err := instance.Create(ctx, projectID, instanceID); err != nil {
		t.Fatal(err)
	}

	database, err := spanner.NewDatabaseAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewSpannerDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err = client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDeleteDB(t *testing.T) {
	const projectID = "local-spanner-20231030"
	const instanceID = "instance1"
	const databaseID = "database1"
	const table = "Store"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	instance, err := spanner.NewInstanceAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err := instance.Create(ctx, projectID, instanceID); err != nil {
		t.Fatal(err)
	}

	database, err := spanner.NewDatabaseAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewSpannerDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err = client.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatal(err)
	}

	if _, err = client.ExecContext(ctx, storeDdl); err != nil {
		t.Fatal(err)
	}

	tx, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, storeDml); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err = client.ExecContext(ctx, "DROP TABLE "+table); err != nil {
		t.Fatal(err)
	}
}
