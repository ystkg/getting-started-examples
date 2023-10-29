package bigquery_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/ystkg/getting-started-examples/bigquery"

	"google.golang.org/api/iterator"
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

func TestNewClient(t *testing.T) {
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
	projectID := getValue(conf.Services.Bigquery.Command, "--project")
	datasetID := getValue(conf.Services.Bigquery.Command, "--dataset")

	client, err := bigquery.NewClient(projectID, url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	dataset := client.Dataset(datasetID)
	it := dataset.Tables(ctx)
	if _, err = it.Next(); err != nil && err != iterator.Done {
		t.Error(err)
	}
}

func getValue(cmd, key string) string {
	for _, v := range strings.Split(cmd, " ") {
		if after, found := strings.CutPrefix(v, key+"="); found {
			return after
		}
	}
	return ""
}
