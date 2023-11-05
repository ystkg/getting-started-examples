package bigquery_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/bigquery"

	bigqueryapi "cloud.google.com/go/bigquery"
	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Bigquery struct {
			Ports   []string
			Command string
		}
	}
}

// *bigqueryapi.RowIterator.Next(&store)
type Store struct {
	StoreID int    `bigquery:"store_id"`
	Name    string `bigquery:"name"`
}

var (
	//go:embed testdata/store.tsv
	storeItems []byte
)

func TestConnect(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port, _, _ := strings.Cut(conf.Services.Bigquery.Ports[0], ":")
	url := "http://localhost:" + port
	projectID := value(conf.Services.Bigquery.Command, "--project")
	datasetID := value(conf.Services.Bigquery.Command, "--dataset")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := bigquery.NewBigQuery(ctx, projectID, url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tables, err := client.Tables(ctx, datasetID)
	if err != nil {
		t.Fatal(err)
	}
	if tables == nil {
		t.Error("tables is nil")
	}

	const want = 1
	rows, err := client.Query(ctx, fmt.Sprintf("SELECT %d", want))
	if err != nil {
		t.Fatal(err)
	}

	got := rows[0][0].(int64)
	if got != want {
		t.Errorf("%d, want %d", got, want)
	}
}

func value(cmd, key string) string {
	for _, v := range strings.Split(cmd, " ") {
		if after, found := strings.CutPrefix(v, key+"="); found {
			return after
		}
	}
	return ""
}

func TestCreateDelete(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port, _, _ := strings.Cut(conf.Services.Bigquery.Ports[0], ":")
	url := "http://localhost:" + port
	projectID := value(conf.Services.Bigquery.Command, "--project")

	datasetID := "dataset2"
	tableID := "store"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := bigquery.NewBigQuery(ctx, projectID, url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err = client.CreateDataset(ctx, datasetID); err != nil {
		t.Fatal(err)
	}

	schema := bigqueryapi.Schema{
		{Name: "store_id", Type: bigqueryapi.IntegerFieldType},
		{Name: "name", Type: bigqueryapi.StringFieldType},
	}
	if err = client.CreateTable(ctx, datasetID, tableID, schema); err != nil {
		t.Fatal(err)
	}

	reader := csv.NewReader(bytes.NewReader(storeItems))
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	client.Insert(ctx, datasetID, tableID, records[1:])

	if err = client.DeleteTable(ctx, datasetID, tableID); err != nil {
		t.Fatal(err)
	}
	if err = client.DeleteDataset(ctx, datasetID); err != nil {
		t.Fatal(err)
	}
}
