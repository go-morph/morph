package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/go-morph/morph/drivers"
	"github.com/go-morph/morph/models"

	_ "github.com/mattn/go-sqlite3"
)

var driverName = "sqlite3"
const defaultMigrationMaxSize = 10 * 1 << 20 // 10 MB
var defaultConfig = &Config{
	MigrationsTable:        "schema_migrations",
	StatementTimeoutInSecs: 5,
	MigrationMaxSize:       defaultMigrationMaxSize,
}

// add here any custom driver configuration
var configParams = []string{
	"x-migration-max-size",
	"x-migrations-table",
	"x-statement-timeout",
}

func init() {
	drivers.Register("sqlite3", &sqlite3{})
}

type Config struct {
	MigrationsTable string
	StatementTimeoutInSecs int
	MigrationMaxSize       int
	// filename        string
}

type sqlite3 struct {
	conn *sql.Conn
	db *sql.DB
	config *Config
	locked int32
}

func WithInstance(dbInstance *sql.DB, config *Config) (drivers.Driver, error) {
	driverConfig := mergeConfigs(config, defaultConfig)

	conn, err := dbInstance.Conn(context.Background())
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "grabbing_connection", OrigErr: err, Message: "failed to grab connection to the database"}
	}

	return &sqlite3{
		conn: conn,
		config: driverConfig,
	}, nil
}

// ToDo: check and remove all comments
func (driver *sqlite3) Open(connURL string) (drivers.Driver, error) {
	customParams, err := drivers.ExtractCustomParams(connURL, configParams)
	if err != nil {
		return nil, &drivers.AppError{Driver: driverName, OrigErr: err, Message: "failed to parse custom parameters from url"}
	}

	sanitizedConnURL, err := drivers.RemoveParamsFromURL(connURL, configParams)
	if err != nil {
		return nil, &drivers.AppError{Driver: driverName, OrigErr: err, Message: "failed to sanitize url from custom parameters"}
	}

	driverConfig, err := mergeConfigWithParams(customParams, defaultConfig)
	if err != nil {
		return nil, &drivers.AppError{Driver: driverName, OrigErr: err, Message: "failed to merge custom params to driver config"}
	}

	fmt.Println("=========================================")
	fmt.Println("Opening sqlite db", sanitizedConnURL)
	fmt.Println("=========================================")

	db, err := sql.Open(driverName, sanitizedConnURL)
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "opening_connection", OrigErr: err, Message: "failed to open connection with the database"}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "grabbing_connection", OrigErr: err, Message: "failed to grab connection to the database"}
	}

	driver.db = db
	driver.config = driverConfig
	driver.conn = conn

	return driver, nil
}

func (driver *sqlite3) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	return driver.conn.PingContext(ctx)
}

// ToDo: swap driver for s
func (driver *sqlite3) Close() error {
	if driver.conn != nil {
		if err := driver.conn.Close(); err != nil {
			return &drivers.DatabaseError{
				OrigErr: err,
				Driver: driverName,
				Message: "failed to close database connection",
				Command: "sqlite3_conn_close",
				Query:   nil,
			}
		}
	}

	if driver.db != nil {
		if err := driver.db.Close(); err != nil {
			return &drivers.DatabaseError{
				OrigErr: err,
				Driver: driverName,
				Message: "failed to close database",
				Command: "sqlite3_db_close",
				Query:   nil,
			}
		}
	}

	driver.db = nil
	driver.conn = nil
	return nil
}

func (driver *sqlite3) Lock() error {
	if atomic.LoadInt32(&driver.locked) != 0 {
		return &drivers.DatabaseError{
			OrigErr: errors.New("driver is marked as locked"),
			Driver:  driverName,
			Message: "database already locked",
			Command: "lock_sqlite3_database",
		}
	}

	atomic.StoreInt32(&driver.locked, 1)
	return nil
}

func (driver *sqlite3) Unlock() error {
	if atomic.LoadInt32(&driver.locked) != 1 {
		return &drivers.DatabaseError{
			OrigErr: errors.New("driver is marked as unlocked"),
			Driver:  driverName,
			Message: "database already unlocked",
			Command: "unlock_sqlite3_database",
		}
	}

	atomic.StoreInt32(&driver.locked, 0)
	return nil
}

func (driver *sqlite3) CreateSchemaTableIfNotExists() (err error) {
	if driver.conn == nil {
		return &drivers.AppError{
			OrigErr: errors.New("driver has no connection established"),
			Message: "database connection is missing",
			Driver:  driverName,
		}
	}

	if err = driver.Lock(); err != nil {
		return err
	}
	defer func() {
		// If we saw no error prior to unlocking and unlocking returns an error we need to
		// assign the unlocking error to err
		if unlockErr := driver.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	createTableIfNotExistsQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (version bigint not null primary key, name varchar not null)", driver.config.MigrationsTable)
	if _, err = driver.conn.ExecContext(ctx, createTableIfNotExistsQuery); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed while executing query",
			Command: "sqlite3_create_migrations_table_if_not_exists",
			Query:   []byte(createTableIfNotExistsQuery),
		}
	}

	return nil
}

func (driver *sqlite3) Apply(migration *models.Migration) (err error) {
	if err = driver.Lock(); err != nil {
		return
	}
	defer func() {
		// If we saw no error prior to unlocking and unlocking returns an error we need to
		// assign the unlocking error to err
		if unlockErr := driver.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	query, readErr := migration.Query()
	if readErr != nil {
		return &drivers.AppError{
			OrigErr: readErr,
			Driver:  driverName,
			Message: fmt.Sprintf("failed to read migration query: %s", migration.Name),
		}
	}
	defer migration.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	transaction, err := driver.conn.BeginTx(ctx, nil)
	if err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "error while opening a transaction to the database",
			Command: "sqlite3_begin_transaction",
		}
	}

	if err = executeQuery(transaction, query); err != nil {
		return err
	}

	if err = executeQuery(transaction, driver.addMigrationQuery(migration)); err != nil {
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "error while committing a transaction to the database",
			Command: "sqlite3_commit_transaction",
		}
	}

	return nil
}

func (driver *sqlite3) AppliedMigrations() (migrations []*models.Migration, err error) {
	if err = driver.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		// If we saw no error prior to unlocking and unlocking returns an error we need to
		// assign the unlocking error to err
		if unlockErr := driver.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	query := fmt.Sprintf("SELECT version, name FROM %s", driver.config.MigrationsTable)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	var appliedMigrations []*models.Migration
	var version uint32
	var name string

	rows, err := driver.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to fetch applied migrations",
			Command: "sqlite3_select_applied_migrations",
			Query:   []byte(query),
		}
	}

	for rows.Next() {
		if err := rows.Scan(&version, &name); err != nil {
			return nil, &drivers.DatabaseError{
				OrigErr: err,
				Driver:  driverName,
				Message: "failed to scan applied migration row",
				Command: "sqlite3_scan_applied_migrations",
			}
		}

		appliedMigrations = append(appliedMigrations, &models.Migration{
			Name:    name,
			Version: version,
		})
	}

	return appliedMigrations, nil
}

func (driver *sqlite3) addMigrationQuery(migration *models.Migration) string {
	return fmt.Sprintf("INSERT INTO %s (version, name) VALUES (%d, '%s')", driver.config.MigrationsTable, migration.Version, migration.Name)
}

func mergeConfigs(config *Config, defaultConfig *Config) *Config {
	if config.MigrationsTable == "" {
		config.MigrationsTable = defaultConfig.MigrationsTable
	}

	if config.StatementTimeoutInSecs == 0 {
		config.StatementTimeoutInSecs = defaultConfig.StatementTimeoutInSecs
	}

	if config.MigrationMaxSize == 0 {
		config.MigrationMaxSize = defaultConfig.MigrationMaxSize
	}

	return config
}

func mergeConfigWithParams(params map[string]string, config *Config) (*Config, error) {
	var err error

	for _, configKey := range configParams {
		if v, ok := params[configKey]; ok {
			switch configKey {
			case "x-migration-max-size":
				if config.MigrationMaxSize, err = strconv.Atoi(v); err != nil {
					return nil, errors.New(fmt.Sprintf("failed to cast config param %s of %s", configKey, v))
				}
			case "x-migrations-table":
				config.MigrationsTable = v
			case "x-statement-timeout":
				if config.StatementTimeoutInSecs, err = strconv.Atoi(v); err != nil {
					return nil, errors.New(fmt.Sprintf("failed to cast config param %s of %s", configKey, v))
				}
			}
		}
	}

	return config, nil
}

func executeQuery(transaction *sql.Tx, query string) error {
	if _, err := transaction.Exec(query); err != nil {
		if txErr := transaction.Rollback(); txErr != nil {
			err = errors.Wrap(errors.New(err.Error()+txErr.Error()), "failed to execute query in migration transaction")

			return &drivers.DatabaseError{
				OrigErr: err,
				Driver:  driverName,
				Command: "sqlite3_rollback_transaction",
			}
		}

		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to execute migration",
			Command: "sqlite3_executing_query",
			Query:   []byte(query),
		}
	}

	return nil
}
