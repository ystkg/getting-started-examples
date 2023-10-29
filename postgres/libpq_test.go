package postgres_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

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

func TestNewClient(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	password := conf.Services.Postgres.Environment.PostgresPassword
	port := strings.SplitN(conf.Services.Postgres.Ports[0], ":", 2)[0]
	dsn := fmt.Sprintf("postgres://postgres:%s@localhost:%s/postgres?sslmode=disable", password, port)

	db, err := postgres.NewClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
}
