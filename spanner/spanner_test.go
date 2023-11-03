package spanner_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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

func TestConnect(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port, _, _ := strings.Cut(conf.Services.Spanner.Ports[0], ":")
	if err = os.Setenv("SPANNER_EMULATOR_HOST", "localhost:"+port); err != nil {
		t.Fatal(err)
	}

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
	if err := instance.CreateInstance(ctx, projectID, instanceID); err != nil {
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
