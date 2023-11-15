package spanner

import (
	spannergorm "github.com/googleapis/go-gorm-spanner"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func NewSpannerGorm(dsn string) (*gorm.DB, error) {
	return gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{SingularTable: true},
		PrepareStmt:    true,
	})
}
