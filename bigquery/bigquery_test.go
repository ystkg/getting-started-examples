package bigquery_test

import (
	"context"
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

type Store struct {
	StoreID int    `bigquery:"store_id"`
	Name    string `bigquery:"name"`
}

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
}

func value(cmd, key string) string {
	for _, v := range strings.Split(cmd, " ") {
		if after, found := strings.CutPrefix(v, key+"="); found {
			return after
		}
	}
	return ""
}

func TestCreate(t *testing.T) {
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

	items := []*Store{
		{StoreID: 1, Name: "store1"},
		{StoreID: 2, Name: "store2"},
		{StoreID: 3, Name: "store3"},
	}
	client.Insert(ctx, datasetID, tableID, items)

	if err = client.DeleteTable(ctx, datasetID, tableID); err != nil {
		t.Fatal(err)
	}
	if err = client.DeleteDataset(ctx, datasetID); err != nil {
		t.Fatal(err)
	}
}
