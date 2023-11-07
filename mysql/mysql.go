package mysql

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
	db *sql.DB
}

func NewMysql(dsn string) (*Mysql, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &Mysql{db}, nil
}

func (m *Mysql) Close() {
	m.db.Close()
}

func (m *Mysql) Ping(ctx context.Context) error {
	return m.db.PingContext(ctx)
}

func (m *Mysql) Begin(ctx context.Context) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, nil)
}

func (m *Mysql) BeginReadOnly(ctx context.Context) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
}

func (m *Mysql) CreateDatabase(ctx context.Context, dbname string) error {
	_, err := m.db.ExecContext(ctx, "CREATE DATABASE "+dbname)
	return err
}

func (m *Mysql) CreateDatabaseIfNotExists(ctx context.Context, dbname string) error {
	_, err := m.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	return err
}

func (m *Mysql) CreateOrReplaceDatabase(ctx context.Context, dbname string) error {
	if err := m.DropDatabaseIfExists(ctx, dbname); err != nil {
		return err
	}
	return m.CreateDatabase(ctx, dbname)
}

func (m *Mysql) DropDatabase(ctx context.Context, dbname string) error {
	_, err := m.db.ExecContext(ctx, "DROP DATABASE "+dbname)
	return err
}

func (m *Mysql) DropDatabaseIfExists(ctx context.Context, dbname string) error {
	_, err := m.db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbname)
	return err
}
