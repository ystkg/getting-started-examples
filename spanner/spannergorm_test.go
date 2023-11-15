package spanner_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/spanner"
)

func TestConnectGorm(t *testing.T) {
	const projectID = "local-spanner-20231030"
	const instanceID = "instance1"
	const databaseID = "database31"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	instance, err := spanner.NewInstanceAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err = instance.Create(ctx, projectID, instanceID); err != nil {
		t.Fatal(err)
	}

	database, err := spanner.NewDatabaseAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err = database.DropDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	if err = database.CreateDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	db, err := spanner.NewSpannerGorm(dsn)
	if err != nil {
		t.Fatal(err)
	}

	sqlDb, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDb.Close()

	if err = sqlDb.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	if err = database.DropDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDeleteGorm(t *testing.T) {
	const projectID = "local-spanner-20231030"
	const instanceID = "instance1"
	const databaseID = "database32"
	const table = "Store"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	instance, err := spanner.NewInstanceAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err = instance.Create(ctx, projectID, instanceID); err != nil {
		t.Fatal(err)
	}

	database, err := spanner.NewDatabaseAdmin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err = database.DropDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	if err = database.CreateDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}

	if err = database.CreateTable(ctx, projectID, instanceID, databaseID, storeDdl); err != nil {
		t.Fatal(err)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	db, err := spanner.NewSpannerGorm(dsn)
	if err != nil {
		t.Fatal(err)
	}

	sqlDb, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDb.Close()

	reader := csv.NewReader(bytes.NewReader(storeItems))
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	items := records[1:]
	for _, v := range items {
		storeId, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Create(&Store{storeId, v[1]}).Error; err != nil {
			t.Fatal(err)
		}
	}

	if err = database.DropTable(ctx, projectID, instanceID, databaseID, table); err != nil {
		t.Fatal(err)
	}

	if err = database.DropDatabase(ctx, projectID, instanceID, databaseID); err != nil {
		t.Fatal(err)
	}
}
