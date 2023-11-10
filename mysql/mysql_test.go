package mysql_test

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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

var (
	//go:embed testdata/store.ddl
	storeDdl string

	//go:embed testdata/store.dml
	storeDml string
)

func TestConnect(t *testing.T) {
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

	mysql, err := mysql.NewMysql(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer mysql.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = mysql.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDelete(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	password := conf.Services.Mysql.Environment.MysqlRootPassword
	const database = "test01"
	port, _, _ := strings.Cut(conf.Services.Mysql.Ports[0], ":")

	dsn := fmt.Sprintf("root:%s@tcp(localhost:%s)/", password, port)
	admin, err := mysql.NewMysql(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = admin.CreateOrReplaceDatabase(ctx, database); err != nil {
		t.Fatal(err)
	}

	dsn = fmt.Sprintf("root:%s@tcp(localhost:%s)/%s", password, port, database)
	client, err := mysql.NewMysql(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tx, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS store"); err != nil {
		t.Fatal(err)
	}

	if _, err = tx.ExecContext(ctx, storeDdl); err != nil {
		t.Fatal(err)
	}

	if _, err = tx.ExecContext(ctx, storeDml); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err = admin.DropDatabase(ctx, database); err != nil {
		t.Fatal(err)
	}
}
