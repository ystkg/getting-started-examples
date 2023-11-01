package gcs_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

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

func TestMain(m *testing.M) {
	m.Run()
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

	port, _, _ := strings.Cut(conf.Services.Gcs.Ports[0], ":")
	if err = os.Setenv("STORAGE_EMULATOR_HOST", "localhost:"+port); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gcs, err := gcs.NewGcs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gcs.Close()

	const projectID = "local-gcs-20231029"
	const bucketName = "bucket1"

	exists, err := gcs.ExistsBucket(ctx, projectID, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		gcs.DeleteBucket(ctx, bucketName)
	}
	if err := gcs.CreateBucket(ctx, projectID, bucketName); err != nil {
		t.Fatal(err)
	}
}
