package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/postgres"

	"gopkg.in/yaml.v3"
)

func TestConnectPgx(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port, _, _ := strings.Cut(conf.Services.Postgres.Ports[0], ":")
	password := conf.Services.Postgres.Environment.PostgresPassword
	dsn := fmt.Sprintf("host=localhost port=%s user=postgres password=%s dbname=postgres sslmode=disable TimeZone=Asia/Tokyo", port, password)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := postgres.NewPgxClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	if err = conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
