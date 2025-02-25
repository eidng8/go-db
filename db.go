package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"entgo.io/ent/dialect"
	"github.com/go-sql-driver/mysql"
)

// ConnectX to the database using predefined environment variables, panic if
// failed. Returns the driver name and the database connection.
func ConnectX() (string, *sql.DB) {
	drv, db, err := Connect()
	if err != nil {
		log.Panic(err)
	}
	return drv, db
}

// Connect to the database using predefined environment variables. Returns the
// driver name, the database connection, and an error if any.
func Connect() (string, *sql.DB, error) {
	drv := os.Getenv("DB_DRIVER")
	if "" == drv {
		return drv, nil, errors.New("DB_DRIVER environment variable is not set")
	}
	dsn := os.Getenv("DB_DSN")
	if "" != dsn {
		conn, err := sql.Open(drv, dsn)
		if err != nil {
			return drv, nil, err
		}
		return drv, conn, nil
	}
	if dialect.MySQL != drv && "mysql-test" != drv {
		return drv, nil, errors.New("DB_DSN environment variable is not set")
	}
	db, err := ConnectMysql(nil)
	if err != nil {
		return drv, nil, err
	}
	return drv, db, nil
}

// ConnectMysql to the MySQL database using the given configuration `cfg`. If
// nil is passed, it reads configuration from predefined environment variables.
func ConnectMysql(cfg *mysql.Config) (*sql.DB, error) {
	return ConnectMysqlWithOptions(
		cfg, MysqlOptions{
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxLifetime: time.Hour,
		},
	)
}

// Transaction wraps the given function `fn` in a database transaction, and
// returns whatever `fn` returns.
func Transaction[T any](db *sql.DB, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	tx, err := db.Begin()
	if nil != err {
		return zero, err
	}
	v, err := fn(tx)
	if nil != err {
		er := tx.Rollback()
		if er != nil {
			return zero, fmt.Errorf("%w; %w", err, er)
		}
		return zero, err
	}
	err = tx.Commit()
	if nil != err {
		return zero, err
	}
	return v, nil
}

// MysqlOptions for the MySQL database connection.
type MysqlOptions struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

// ConnectMysqlWithOptions to the MySQL database using predefined environment
// variables and options.
func ConnectMysqlWithOptions(cfg *mysql.Config, opts MysqlOptions) (
	*sql.DB, error,
) {
	if nil == cfg {
		var err error
		cfg, err = mysqlCfg()
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open(os.Getenv("DB_DRIVER"), cfg.FormatDSN())
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf(
				"Failed to open MySQL connection: %s", err,
			),
		)
	}
	db.SetMaxIdleConns(opts.MaxIdleConns)
	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	return db, nil
}

func mysqlCfg() (*mysql.Config, error) {
	user := os.Getenv("DB_USER")
	if "" == user {
		return nil, errors.New("DB_USER environment variable is not set")
	}
	pass := os.Getenv("DB_PASSWORD")
	if "" == pass {
		return nil, errors.New("DB_PASSWORD environment variable is not set")
	}
	host := os.Getenv("DB_HOST")
	if "" == host {
		return nil, errors.New("DB_HOST environment variable is not set")
	}
	dbname := os.Getenv("DB_NAME")
	if "" == dbname {
		return nil, errors.New("DB_NAME environment variable is not set")
	}
	protocol := os.Getenv("DB_PROTOCOL")
	if "" == protocol {
		protocol = "tcp"
	}
	collation := os.Getenv("DB_COLLATION")
	if "" == collation {
		collation = "utf8mb4_unicode_ci"
	}
	location, err := time.LoadLocation(os.Getenv("DB_TIMEZONE"))
	if err != nil {
		return nil, err
	}
	return &mysql.Config{
		User:                     user,
		Passwd:                   pass,
		Net:                      protocol,
		Addr:                     host,
		DBName:                   dbname,
		Collation:                collation,
		Loc:                      location,
		AllowCleartextPasswords:  false,
		AllowFallbackToPlaintext: false,
		AllowNativePasswords:     true,
		AllowOldPasswords:        false,
		MultiStatements:          true,
		ParseTime:                true,
	}, nil
}
