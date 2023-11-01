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

type DockerCompose struct {
	Services struct {
		Postgres struct {
			Ports       []string
			Environment struct {
				PostgresPassword string `yaml:"POSTGRES_PASSWORD"`
			}
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

	password := conf.Services.Postgres.Environment.PostgresPassword
	port, _, _ := strings.Cut(conf.Services.Postgres.Ports[0], ":")
	dsn := fmt.Sprintf("postgres://postgres:%s@localhost:%s/postgres?sslmode=disable", password, port)

	db, err := postgres.NewClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}
