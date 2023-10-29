package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func NewPgxClient(dsn string) (*pgx.Conn, error) {
	return pgx.Connect(context.Background(), dsn)
}
