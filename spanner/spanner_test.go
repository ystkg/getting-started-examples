package spanner_test

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	spannerapi "cloud.google.com/go/spanner"
	"github.com/ystkg/getting-started-examples/spanner"
	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Spanner struct {
			Ports []string
		}
	}
}

//go:embed testdata/store.ddl
var storeDdl string

//go:embed testdata/store.dml
var storeDml string

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatal(err)
	}
	m.Run()
}

func setup() error {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		return err
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		return err
	}

	port, _, _ := strings.Cut(conf.Services.Spanner.Ports[0], ":")
	if err = os.Setenv("SPANNER_EMULATOR_HOST", "localhost:"+port); err != nil {
		return err
	}

	return nil
}

func TestConnect(t *testing.T) {
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

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewSpanner(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const want = 1
	it := client.SingleQuery(ctx, fmt.Sprintf("SELECT %d", want))
	defer it.Stop()

	row, err := it.Next()
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	if err = row.Columns(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("%d, want %d", got, want)
	}
}

func TestTable(t *testing.T) {
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

	if err = database.CreateTable(ctx, projectID, instanceID, databaseID, storeDdl); err != nil {
		t.Fatal(err)
	}

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewSpanner(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err = client.UpdateSQL(ctx, storeDml); err != nil {
		t.Fatal(err)
	}

	m := []*spannerapi.Mutation{
		spannerapi.InsertOrUpdate("Store", []string{"StoreId", "Name"}, []interface{}{6, "store6"}),
		spannerapi.InsertOrUpdate("Store", []string{"StoreId", "Name"}, []interface{}{7, "store7"}),
		spannerapi.InsertOrUpdate("Store", []string{"StoreId", "Name"}, []interface{}{8, "store8"}),
	}
	if err = client.UpdateMutation(ctx, m); err != nil {
		t.Fatal(err)
	}

	if err = database.DropTable(ctx, projectID, instanceID, databaseID, "Store"); err != nil {
		t.Fatal(err)
	}
}
