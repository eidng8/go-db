package db

import (
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/eidng8/go-utils"
	"github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

var (
	errTest   = errors.New("test error")
	okTxFunc  = func(tx *sql.Tx) (int, error) { return 123, nil }
	errTxFunc = func(tx *sql.Tx) (int, error) { return 123, errTest }
)

func setup(tb testing.TB) *sql.DB {
	require.Nil(tb, os.Setenv("DB_DRIVER", "sqlite3"))
	require.Nil(tb, os.Setenv("DB_DSN", ":memory:?_journal=WAL&_timeout=5000"))
	_, db, err := Connect()
	require.Nil(tb, err)
	//goland:noinspection SqlNoDataSourceInspection
	_, err = db.Exec("CREATE TABLE test (val TEXT)")
	require.Nil(tb, err)
	db.SetMaxOpenConns(1)
	return db
}

func Test_Connect_with_dsn(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "sqlite3"))
	require.Nil(t, os.Setenv("DB_DSN", ":memory:"))
	driver, conn, err := Connect()
	require.Nil(t, err)
	require.Equal(t, "sqlite3", driver)
	require.IsType(t, &sql.DB{}, conn)
}

func Test_Connect_returns_error_if_no_dsn(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "sqlite3"))
	require.Nil(t, os.Unsetenv("DB_DSN"))
	_, _, err := Connect()
	require.NotNil(t, err)
}

func Test_Connect_returns_error_if_wrong_driver(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "unknown"))
	require.Nil(t, os.Setenv("DB_DSN", ":memory:"))
	_, _, err := Connect()
	require.NotNil(t, err)
}

func Test_Connect_returns_error_if_no_driver(t *testing.T) {
	require.Nil(t, os.Unsetenv("DB_DRIVER"))
	require.Nil(t, os.Setenv("DB_DSN", ":memory:"))
	_, _, err := Connect()
	require.NotNil(t, err)
}

func Test_ConnectX_with_dsn(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "sqlite3"))
	require.Nil(t, os.Setenv("DB_DSN", ":memory:"))
	driver, conn := ConnectX()
	require.Equal(t, "sqlite3", driver)
	require.IsType(t, &sql.DB{}, conn)
}

func Test_ConnectX_panics(t *testing.T) {
	require.Nil(t, os.Unsetenv("DB_DRIVER"))
	require.Panics(t, func() { _, _ = ConnectX() })
}

func Test_Transaction_returns_cb_value(t *testing.T) {
	conn, mock, err := sqlmock.New()
	require.Nil(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer conn.Close()
	mock.ExpectBegin()
	mock.ExpectCommit()
	actual, err := Transaction(conn, okTxFunc)
	require.Nil(t, err)
	require.Equal(t, 123, actual)
}

func Test_Transaction_returns_err_if_begin_fails(t *testing.T) {
	conn, mock, err := sqlmock.New()
	require.Nil(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer conn.Close()
	mock.ExpectBegin().WillReturnError(errTest)
	actual, err := Transaction(conn, okTxFunc)
	require.ErrorIs(t, err, errTest)
	require.Zero(t, actual)
}

func Test_Transaction_rolls_back_and_returns_err_if_fn_fails(t *testing.T) {
	conn, mock, err := sqlmock.New()
	require.Nil(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer conn.Close()
	mock.ExpectBegin()
	mock.ExpectRollback()
	actual, err := Transaction(conn, errTxFunc)
	require.ErrorIs(t, err, errTest)
	require.Zero(t, actual)
}

func Test_Transaction_wraps_rolls_back_errors(t *testing.T) {
	conn, mock, err := sqlmock.New()
	require.Nil(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer conn.Close()
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(errTest)
	actual, err := Transaction(conn, errTxFunc)
	require.NotNil(t, err)
	require.Equal(t, "test error; test error", err.Error())
	require.Zero(t, actual)
}

func Test_Transaction_returns_commit_errors(t *testing.T) {
	conn, mock, err := sqlmock.New()
	require.Nil(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer conn.Close()
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errTest)
	actual, err := Transaction(conn, okTxFunc)
	require.ErrorIs(t, err, errTest)
	require.Zero(t, actual)
}

func Test_mysqlCfg_reads_env(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	cfg, err := mysqlCfg()
	require.Nil(t, err)
	require.Equal(
		t, &mysql.Config{
			User:                     "test_user",
			Passwd:                   "test_pass",
			Net:                      "test_protocol",
			Addr:                     "test_host",
			DBName:                   "test_name",
			Collation:                "test_collation",
			Loc:                      utils.ReturnOrPanic(time.LoadLocation("UTC")),
			AllowCleartextPasswords:  false,
			AllowFallbackToPlaintext: false,
			AllowNativePasswords:     true,
			AllowOldPasswords:        false,
			MultiStatements:          true,
			ParseTime:                true,
		}, cfg,
	)
}

func Test_mysqlCfg_returns_error_if_user_not_set(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Unsetenv("DB_USER"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	_, err := mysqlCfg()
	require.NotNil(t, err)
}

func Test_mysqlCfg_returns_error_if_password_not_set(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Unsetenv("DB_PASSWORD"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	_, err := mysqlCfg()
	require.NotNil(t, err)
}

func Test_mysqlCfg_returns_error_if_host_not_set(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Unsetenv("DB_HOST"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	_, err := mysqlCfg()
	require.NotNil(t, err)
}

func Test_mysqlCfg_returns_error_if_name_not_set(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Unsetenv("DB_NAME"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	_, err := mysqlCfg()
	require.NotNil(t, err)
}

func Test_mysqlCfg_returns_default_protocol(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Unsetenv("DB_PROTOCOL"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	cfg, err := mysqlCfg()
	require.Nil(t, err)
	require.Equal(t, "tcp", cfg.Net)
}

func Test_mysqlCfg_returns_default_collation(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Unsetenv("DB_COLLATION"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	cfg, err := mysqlCfg()
	require.Nil(t, err)
	require.Equal(t, "utf8mb4_unicode_ci", cfg.Collation)
}

func Test_mysqlCfg_returns_utc_if_timezone_not_set(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Unsetenv("DB_TIMEZONE"))
	cfg, err := mysqlCfg()
	require.Nil(t, err)
	require.Equal(t, utils.ReturnOrPanic(time.LoadLocation("")), cfg.Loc)
}

func Test_mysqlCfg_returns_error_if_timezone_error(t *testing.T) {
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "unknown"))
	_, err := mysqlCfg()
	require.NotNil(t, err)
}
