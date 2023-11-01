package bigquery_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/bigquery"

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
