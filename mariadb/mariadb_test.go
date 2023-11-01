package mariadb_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/mariadb"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Mariadb struct {
			Ports       []string
			Environment struct {
				MariadbRootPassword string `yaml:"MARIADB_ROOT_PASSWORD"`
				MariadbDatabase     string `yaml:"MARIADB_DATABASE"`
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

	password := conf.Services.Mariadb.Environment.MariadbRootPassword
	database := conf.Services.Mariadb.Environment.MariadbDatabase
	port, _, _ := strings.Cut(conf.Services.Mariadb.Ports[0], ":")
	dsn := fmt.Sprintf("root:%s@tcp(localhost:%s)/%s", password, port, database)

	db, err := mariadb.NewClient(dsn)
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
