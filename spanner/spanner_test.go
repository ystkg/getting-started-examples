package spanner_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	sut "github.com/ystkg/getting-started-examples/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Spanner struct {
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

	port, _, _ := strings.Cut(conf.Services.Spanner.Ports[0], ":")
	if err = os.Setenv("SPANNER_EMULATOR_HOST", "localhost:"+port); err != nil {
		t.Fatal(err)
	}

	const projectID = "local-spanner-20231030"
	const instanceID = "instance1"
	const databaseID = "database1"

	ctx := context.Background()

	admin, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	op, err := admin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
	})
	if err == nil {
		if _, err = op.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	} else if status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}

	dbadmin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbadmin.Close()
	dbop, err := dbadmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE " + databaseID,
	})
	if err == nil {
		if _, err = dbop.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	} else if status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := sut.NewClient(db)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const want = 1
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT %d", want)}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
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
