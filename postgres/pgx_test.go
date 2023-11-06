package postgres_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ystkg/getting-started-examples/postgres"

	"gopkg.in/yaml.v3"
)

var (
	//go:embed testdata/store.ddl
	storeDdl string

	//go:embed testdata/store.tsv
	storeItems []byte
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

	client, err := postgres.NewPgxClient(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err = client.Ping(ctx); err != nil {
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

	port, _, _ := strings.Cut(conf.Services.Postgres.Ports[0], ":")
	password := conf.Services.Postgres.Environment.PostgresPassword
	dsn := fmt.Sprintf("host=localhost port=%s user=postgres password=%s dbname=postgres sslmode=disable TimeZone=Asia/Tokyo", port, password)
	const table = "store"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := postgres.NewPgxClient(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := csv.NewReader(bytes.NewReader(storeItems))
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	columnNames := records[0]
	rows := records[1:]

	tx, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(ctx, storeDdl)
	if err != nil {
		t.Fatal(err)
	}

	inputRows := make([][]any, len(rows))
	for i, v := range rows {
		inputRow := make([]any, len(v))
		for j, vv := range v {
			inputRow[j] = vv
		}
		inputRows[i] = inputRow
	}

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{table},
		columnNames,
		pgx.CopyFromRows(inputRows),
	)
	if err != nil {
		t.Fatal(err)
	}

	want := int64(len(inputRows))
	if n != want {
		t.Errorf("%d, wnat %d", n, want)
	}

	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
