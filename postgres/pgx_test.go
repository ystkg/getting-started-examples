package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ystkg/getting-started-examples/postgres"

	"gopkg.in/yaml.v3"
)

func TestNewPgxClient(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port := strings.SplitN(conf.Services.Postgres.Ports[0], ":", 2)[0]
	password := conf.Services.Postgres.Environment.PostgresPassword
	dsn := fmt.Sprintf("host=localhost port=%s user=postgres password=%s dbname=postgres sslmode=disable TimeZone=Asia/Tokyo", port, password)

	ctx := context.Background()

	conn, err := postgres.NewPgxClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	if err = conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
