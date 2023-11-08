package spanner

import (
	"context"
	"database/sql"

	_ "github.com/googleapis/go-sql-spanner"
)

// go-sql-spanner
type SpannerDB struct {
	db *sql.DB
}

func NewSpannerDB(dsn string) (*SpannerDB, error) {
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return nil, err
	}
	return &SpannerDB{db}, nil
}

func (s *SpannerDB) Close() {
	s.db.Close()
}

func (s *SpannerDB) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SpannerDB) Begin(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

func (s *SpannerDB) BeginReadOnly(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
}

func (s *SpannerDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}
