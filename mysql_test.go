package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/eidng8/go-utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func launchMysql() (testcontainers.Container, string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForLog("MySQL init process done. Ready for start up."),
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "test_pass",
			"MYSQL_DATABASE":      "test_db",
			"MYSQL_USER":          "test_user",
			"MYSQL_PASSWORD":      "test_pass",
		},
	}
	cntr, err := testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
	utils.PanicIfError(err)
	if nil != err {
		e := cntr.Terminate(ctx)
		if nil != e {
			panic(fmt.Errorf("%w\n%w", err, e))
		}
		panic(err)
	}
	mapped, err := cntr.MappedPort(
		ctx, utils.ReturnOrPanic(nat.NewPort("tcp", "3306")),
	)
	utils.PanicIfError(err)
	return cntr, mapped.Port()
}

func callMysqlTest(port string, f func(*testing.T, string)) func(*testing.T) {
	return func(t *testing.T) {
		f(t, port)
	}
}

func Test_Connect_returns_error_if_wrong_config(t *testing.T) {
	require.Nil(t, os.Unsetenv("DB_DSN"))
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql-test"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "abc://not-exist"))
	require.Nil(t, os.Setenv("DB_NAME", "test_name"))
	require.Nil(t, os.Setenv("DB_PROTOCOL", "test_protocol"))
	require.Nil(t, os.Setenv("DB_COLLATION", "test_collation"))
	require.Nil(t, os.Setenv("DB_TIMEZONE", "UTC"))
	sql.Register("mysql-test", &MockErrorDriver{t})
	_, _, err := Connect()
	require.NotNil(t, err)
}

func Test_Mysql(t *testing.T) {
	require.Nil(t, os.Unsetenv("DB_DSN"))
	require.Nil(t, os.Unsetenv("DB_PROTOCOL"))
	require.Nil(t, os.Unsetenv("DB_COLLATION"))
	require.Nil(t, os.Unsetenv("DB_TIMEZONE"))
	require.Nil(t, os.Setenv("DB_DRIVER", "mysql"))
	require.Nil(t, os.Setenv("DB_USER", "test_user"))
	require.Nil(t, os.Setenv("DB_PASSWORD", "test_pass"))
	require.Nil(t, os.Setenv("DB_HOST", "test_host"))
	require.Nil(t, os.Setenv("DB_NAME", "test_db"))
	ctx := context.Background()
	cntr, port := launchMysql()
	defer utils.PanicIfError(cntr.Terminate(ctx))
	t.Run("connects to mysql", callMysqlTest(port, doConnectToMysql))
	t.Run(
		"connects to mysql returns error if wrong env config",
		callMysqlTest(port, doConnectToMysqlReturnsError),
	)
}

func doConnectToMysql(t *testing.T, port string) {
	_, _, err := Connect()
	require.Nil(t, err)
}

func doConnectToMysqlReturnsError(t *testing.T, port string) {
	require.Nil(t, os.Unsetenv("DB_USER"))
	_, _, err := Connect()
	require.NotNil(t, err)
}

type MockErrorDriver struct{ t *testing.T }

func (m *MockErrorDriver) OpenConnector(name string) (driver.Connector, error) {
	return nil, errTest
}

func (m *MockErrorDriver) Open(name string) (driver.Conn, error) {
	return nil, errTest
}
