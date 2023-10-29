package mysql_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ystkg/getting-started-examples/mysql"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Mysql struct {
			Ports       []string
			Environment struct {
				MysqlRootPassword string `yaml:"MYSQL_ROOT_PASSWORD"`
				MysqlDatabase     string `yaml:"MYSQL_DATABASE"`
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

	/*
		cfg := mysql.Config{
			DBName: "mysql",
			User:   "root",
			Passwd: "passwd",
			Addr:   "localhost:3306",
			Net:    "tcp",
		}
		dsn := cfg.FormatDSN()
	*/
	password := conf.Services.Mysql.Environment.MysqlRootPassword
	database := conf.Services.Mysql.Environment.MysqlDatabase
	port, _, _ := strings.Cut(conf.Services.Mysql.Ports[0], ":")
	dsn := fmt.Sprintf("root:%s@tcp(localhost:%s)/%s", password, port, database)

	db, err := mysql.NewClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
}
