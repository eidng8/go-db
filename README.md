# go-db

A tiny module for lazy me, to connect database.

This module use a predefined set of environment variables to connect databases using `database/sql`.

##  Usage

Just call `Connect()` or `ConnectX()` to establish database connections.

```golang
import (
    "database/sql"
    "os"
    "github.com/eidng8/go-db"

    "<your-package>/ent"
)

func getEntClient() *ent.Client {
    return ent.NewClient(ent.Driver(entsql.OpenDB(db.ConnectX())))
}
```

### Environment Variables

#### DB_DRIVER

REQUIRED and cannot be empty. Determines what kind of database to connect. Can be any driver supported by `database/sql`, such as `mysql`, `sqlite3`, `pgx`, etc. Remember to import proper driver module to your package.

#### DB_DSN

REQUIRED for connections other than MySQL. A complete DSN string to be used to establish connection to database. When set, this variable is passed directly to `sql.Open()`. For MySQL, if this variable is not set, variables below will be used to configure the connection.

#### Variables specific to MySQL

These variables are used to configure the connection to MySQL, if `DB_DSN` is not set.

##### DB_USER

REQUIRED and cannot be empty. Determines the user name to connect to database.

##### DB_PASSWORD

REQUIRED and cannot be empty. Determines the password to connect to database.

##### DB_HOST

REQUIRED and cannot be empty. Determines the host to connect to.

##### DB_NAME

REQUIRED and cannot be empty. Determines the database name to connect to.

##### DB_PROTOCOL

OPTIONAL and defaults to `tcp`.

##### DB_COLLATION

OPTIONAL and defaults to `utf8mb4_unicode_ci`.

##### DB_TIMEZONE

OPTIONAL and defaults to `UTC`.

##### more configurations

When using `Connect()` or `ConnectX()` with environment variables, some configurations are also set:

```golang
mysql.Config{
    // .....
    AllowCleartextPasswords:  false,
    AllowFallbackToPlaintext: false,
    AllowNativePasswords:     true,
    AllowOldPasswords:        false,
    MultiStatements:          true,
    ParseTime:                true,
    // .....
}
```

Besides using `Connect()` or `ConnectX()`, `ConnectMysql()` can also be called with `mysql.Config` pointer:

```golang
cfg := mysql.Config{
    // You detailed configuration .....
}
ConnectMysql(cfg)
```

No matter which function is used to establish the connection, the following 3 settings are also called:

```golang
db.SetMaxIdleConns(10)
db.SetMaxOpenConns(100)
db.SetConnMaxLifetime(time.Hour)
```
