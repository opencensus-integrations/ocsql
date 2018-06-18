# ocsql
OpenCensus SQL database driver wrapper.

Add an ocsql wrapper to your existing database code to instrument the
interactions with the database.

## initialize

To use ocsql with your application, register an ocsql wrapper of a database
driver as shown below.

Example:
```go
import (
    "github.com/basvanbeek/ocsql"
    _ "github.com/mattn/go-sqlite3"
)

var (
    driverName string
    err        error
    db         *sql.DB
)

// Register our ocsql wrapper for the provided SQLite3 driver.
driverName, err = ocsql.Register("sqlite3", ocsql.WithOptions(ocsql.TraceAll))
if err != nil {
    log.Fatalf("unable to register our ocsql driver: %v\n", err)
}

// Connect to a SQLite3 database using the ocsql driver wrapper.
db, err = sql.Open(driverName, "resource.db")
```

If not wanting to use the named driver magic used by ocsql.Register, an
alternative way to bootstrap the ocsql wrapper exists.

Example:
```go
import (
    "github.com/basvanbeek/ocsql"
    sqlite3 "github.com/mattn/go-sqlite3"
)

var (
    driver driver.Driver
    err    error
    db     *sql.DB
)

// Explicitly wrap the SQLite3 driver with ocsql
driver = ocsql.Wrap(&sqlite3.SQLiteDriver{})

// Register our ocsql wrapper as a database driver
sql.Register("ocsql-sqlite3", driver)

// Connect to a SQLite3 database using the ocsql driver wrapper
db, err = sql.Open("ocsql-sqlite3", "resource.db")
```

## context

To really take advantage of ocsql, all database calls should be made using the
*Context methods. Failing to do so will result in many orphaned ocsql traces
if the `AllowRoot` TraceOption is set to true. By default AllowRoot is disabled
and will result in ocsql not tracing the database calls if context or parent
spans are missing.

| Old            | New                   |
|----------------|-----------------------|
| *DB.Begin      | *DB.BeginTx           |
| *DB.Exec       | *DB.ExecContext       |
| *DB.Ping       | *DB.PingContext       |
| *DB.Prepare    | *DB.PrepareContext    |
| *DB.Query      | *DB.QueryContext      |
| *DB.QueryRow   | *DB.QueryRowContext   |
|                |                       |
| *Stmt.Exec     | *Stmt.ExecContext     |
| *Stmt.Query    | *Stmt.QueryContext    |
| *Stmt.QueryRow | *Stmt.QueryRowContext |
|                |                       |
| *Tx.Exec       | *Tx.ExecContext       |
| *Tx.Prepare    | *Tx.PrepareContext    |
| *Tx.Query      | *Tx.QueryContext      |
| *Tx.QueryRow   | *Tx.QueryRowContext   |

Example:
```go

func (s *svc) GetDevice(ctx context.Context, id int) (*Device, error) {
    // assume we have instrumented our service transports and ctx holds a span.
    var device Device
    if err := s.db.QueryRowContext(
        ctx, "SELECT * FROM device WHERE id = ?", id,
    ).Scan(&device); err != nil {
        return nil, err
    }
    return device
}
```
