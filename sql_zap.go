package sqlzapwrapper

import (
	"database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
	"time"
)

// maxOpenDbConn sets max allowed connections to database for the current conn
const maxOpenDbConn = 5

// maxIdleDbConn sets max allowed idle connections to database for current conn
const maxIdleDbConn = 1

// maxDbLifetime sets max allowed lifetime for a connection to the database
const maxDbLifetime = 5 * time.Minute

type DB struct {
	Db  *sql.DB
	Log *zap.Logger
}

// NewDatabase creates a connection pool to sql using pgx as driver
func NewDatabase(dsn, driver string, log *zap.Logger) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	db.SetConnMaxLifetime(maxDbLifetime)
	db.SetMaxIdleConns(maxIdleDbConn)
	db.SetMaxOpenConns(maxOpenDbConn)

	return &DB{
		Db:  db,
		Log: log,
	}, nil
}

func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	d.Log.Debug("Executing SQL query",
		zap.String("query", query),
		zap.Any("args", args))

	return d.Db.Exec(query, args...)
}

func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	d.Log.Debug("Executing SQL query",
		zap.String("query", query),
		zap.Any("args", args))

	return d.Db.QueryRow(query, args...)
}

func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	d.Log.Debug("Executing SQL query",
		zap.String("query", query),
		zap.Any("args", args))

	return d.Db.Query(query, args...)
}
