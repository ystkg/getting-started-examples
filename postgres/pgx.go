package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Postgres{pool}, nil
}

func (pg *Postgres) Close() {
	pg.pool.Close()
}

func (pg *Postgres) Ping(ctx context.Context) error {
	return pg.pool.Ping(ctx)
}

func (pg *Postgres) Begin(ctx context.Context) (pgx.Tx, error) {
	return pg.pool.Begin(ctx)
}

func (pg *Postgres) BeginReadOnly(ctx context.Context) (pgx.Tx, error) {
	return pg.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
}

func (pg *Postgres) CreateDatabase(ctx context.Context, dbname string) error {
	_, err := pg.pool.Exec(ctx, "CREATE DATABASE "+dbname)
	return err
}

func (pg *Postgres) CreateDatabaseIfNotExists(ctx context.Context, dbname string) error {
	if err := pg.CreateDatabase(ctx, dbname); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == "42P04" { // database already exists
			return nil
		}
		return err
	}
	return nil
}

func (pg *Postgres) CreateOrReplaceDatabase(ctx context.Context, dbname string) error {
	if err := pg.DropDatabaseIfExists(ctx, dbname); err != nil {
		return err
	}
	return pg.CreateDatabase(ctx, dbname)
}

func (pg *Postgres) DropDatabase(ctx context.Context, dbname string) error {
	_, err := pg.pool.Exec(ctx, "DROP DATABASE "+dbname)
	return err
}

func (pg *Postgres) DropDatabaseIfExists(ctx context.Context, dbname string) error {
	const SQL = "SELECT pid FROM pg_stat_activity WHERE datname = $1"
	rows, err := pg.pool.Query(ctx, SQL, dbname)
	if err != nil {
		return err
	}
	defer rows.Close()

	pids := make([]int, 0)
	for rows.Next() {
		var pid int
		rows.Scan(&pid)
		pids = append(pids, pid)
	}
	rows.Close()

	for _, v := range pids {
		if _, err := pg.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", v); err != nil {
			return err
		}
	}

	_, err = pg.pool.Exec(ctx, "DROP DATABASE IF EXISTS "+dbname)
	return err
}
