package spanner_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
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

// *spannerapi.Row.ToStruct(&store)
type Store struct {
	StoreID int    `spanner:"StoreId"`
	Name    string `spanner:"Name"`
}

var (
	//go:embed testdata/store.ddl
	storeDdl string

	//go:embed testdata/store.dml
	storeDml string

	//go:embed testdata/store.tsv
	storeItems []byte
)

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
	rows, err := client.Query(ctx, fmt.Sprintf("SELECT %d", want))
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	if err = rows[0].Columns(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("%d, want %d", got, want)
	}
}

func TestCreateDelete(t *testing.T) {
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

	reader := csv.NewReader(bytes.NewReader(storeItems))
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	cols := records[0]

	m := []*spannerapi.Mutation{}
	for _, v := range records[1:] {
		vals := make([]any, len(v))
		for i, vv := range v {
			vals[i] = vv
		}
		m = append(m, spannerapi.InsertOrUpdate(table, cols, vals))
	}

	if err = client.UpdateMutation(ctx, m); err != nil {
		t.Fatal(err)
	}

	if err = database.DropTable(ctx, projectID, instanceID, databaseID, table); err != nil {
		t.Fatal(err)
	}
}
