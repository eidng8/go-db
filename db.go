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

func ConnectX() (string, *sql.DB) {
	drv, db, err := Connect()
	if err != nil {
		log.Fatal(err)
	}
	return drv, db
}

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
	if dialect.MySQL != drv {
		return drv, nil, errors.New("DB_DSN environment variable is not set")
	}
	db, err := ConnectMysql(nil)
	if err != nil {
		return drv, nil, err
	}
	return drv, db, nil
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

func ConnectMysql(cfg *mysql.Config) (*sql.DB, error) {
	if nil == cfg {
		var err error
		cfg, err = mysqlCfg()
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open(dialect.MySQL, cfg.FormatDSN())
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf(
				"Failed to open MySQL connection: %s", err,
			),
		)
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)
	db.SetConnMaxLifetime(time.Hour)
	return db, nil
}
