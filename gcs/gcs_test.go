package gcs_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/ystkg/getting-started-examples/gcs"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Gcs struct {
			Ports []string
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

	port, _, _ := strings.Cut(conf.Services.Gcs.Ports[0], ":")
	url := "localhost:" + port
	if err = os.Setenv("STORAGE_EMULATOR_HOST", url); err != nil {
		t.Fatal(err)
	}

	client, err := gcs.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	const projectID = "local-gcs-20231029"
	client.Bucket("bucket1").Delete(ctx)
	if err := client.Bucket("bucket1").Create(ctx, projectID, nil); err != nil {
		t.Fatal(err)
	}
}
