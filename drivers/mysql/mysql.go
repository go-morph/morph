// Initial code generated by generator.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/go-morph/morph/drivers"
	"github.com/go-morph/morph/models"
	_ "github.com/go-sql-driver/mysql"
)

const driverName = "mysql"
const defaultMigrationMaxSize = 10 * 1 << 20 // 10 MB
var defaultConfig = &Config{
	MigrationsTable:        "db_migrations",
	StatementTimeoutInSecs: 60,
	MigrationMaxSize:       defaultMigrationMaxSize,
}

// add here any custom driver configuration
var configParams = []string{
	"x-migration-max-size",
	"x-migrations-table",
	"x-statement-timeout",
}

type Config struct {
	MigrationsTable        string
	StatementTimeoutInSecs int
	MigrationMaxSize       int
	databaseName           string
	closeDBonClose         bool
}

type mysql struct {
	conn   *sql.Conn
	db     *sql.DB
	config *Config
}

func WithInstance(dbInstance *sql.DB, config *Config) (drivers.Driver, error) {
	driverConfig := mergeConfigs(config, defaultConfig)

	conn, err := dbInstance.Conn(context.Background())
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "grabbing_connection", OrigErr: err, Message: "failed to grab connection to the database"}
	}

	if driverConfig.databaseName, err = currentDatabaseNameFromDB(conn, driverConfig); err != nil {
		return nil, err
	}

	return &mysql{config: driverConfig, conn: conn, db: dbInstance}, nil
}

func Open(connURL string) (drivers.Driver, error) {
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

	db, err := sql.Open(driverName, sanitizedConnURL)
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "opening_connection", OrigErr: err, Message: "failed to open connection with the database"}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, &drivers.DatabaseError{Driver: driverName, Command: "grabbing_connection", OrigErr: err, Message: "failed to grab connection to the database"}
	}

	if driverConfig.databaseName, err = extractDatabaseNameFromURL(sanitizedConnURL); err != nil {
		return nil, &drivers.AppError{Driver: driverName, OrigErr: err, Message: "failed to extract database name from connection url"}
	}

	driverConfig.closeDBonClose = true

	return &mysql{
		conn:   conn,
		db:     db,
		config: driverConfig,
	}, nil
}

func (driver *mysql) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	return driver.conn.PingContext(ctx)
}

func (driver *mysql) Close() error {
	if driver.conn != nil {
		if err := driver.conn.Close(); err != nil {
			return &drivers.DatabaseError{
				OrigErr: err,
				Driver:  driverName,
				Message: "failed to close database connection",
				Command: "mysql_conn_close",
				Query:   nil,
			}
		}
	}

	if driver.db != nil && driver.config.closeDBonClose {
		if err := driver.db.Close(); err != nil {
			return &drivers.DatabaseError{
				OrigErr: err,
				Driver:  driverName,
				Message: "failed to close database",
				Command: "mysql_db_close",
				Query:   nil,
			}
		}
		driver.db = nil
	}

	driver.conn = nil
	return nil
}

func (driver *mysql) Lock() error {
	aid, err := drivers.GenerateAdvisoryLockID(driver.config.databaseName, driver.config.MigrationsTable)
	if err != nil {
		return err
	}

	// This will wait until the lock can be acquired or until the statement timeout has reached.
	query := fmt.Sprintf("SELECT GET_LOCK(?, %d)", driver.config.StatementTimeoutInSecs)
	var success bool
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	if err := driver.conn.QueryRowContext(ctx, query, aid).Scan(&success); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to obtain advisory lock due to error",
			Command: "lock_migrations_table",
			Query:   []byte(query),
		}
	}

	if !success {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to obtain advisory lock, potentially due to timeout",
			Command: "lock_migrations_table",
			Query:   []byte(query),
		}
	}

	return nil
}

func (driver *mysql) Unlock() error {
	aid, err := drivers.GenerateAdvisoryLockID(driver.config.databaseName, driver.config.MigrationsTable)
	if err != nil {
		return err
	}

	query := `SELECT RELEASE_LOCK(?)`
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	if _, err := driver.conn.ExecContext(ctx, query, aid); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to unlock advisory lock",
			Command: "unlock_migrations_table",
			Query:   []byte(query),
		}
	}

	return nil
}

func (driver *mysql) createSchemaTableIfNotExists() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	createTableIfNotExistsQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (Version bigint(20) NOT NULL, Name varchar(64) NOT NULL, PRIMARY KEY (Version)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", driver.config.MigrationsTable)
	if _, err = driver.conn.ExecContext(ctx, createTableIfNotExistsQuery); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed while executing query",
			Command: "create_migrations_table_if_not_exists",
			Query:   []byte(createTableIfNotExistsQuery),
		}
	}

	return nil
}

func (driver *mysql) Apply(migration *models.Migration, saveVersion bool) (err error) {
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

	if _, err := driver.conn.ExecContext(ctx, query); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed when applying migration",
			Command: "apply_migration",
			Query:   []byte(query),
		}
	}

	updateVersionContext, cancel := context.WithTimeout(context.Background(), time.Duration(driver.config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	if !saveVersion {
		return nil
	}

	updateVersionQuery := driver.addMigrationQuery(migration)
	if _, err := driver.conn.ExecContext(updateVersionContext, updateVersionQuery); err != nil {
		return &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed when updating migrations table with the new version",
			Command: "update_version",
			Query:   []byte(updateVersionQuery),
		}
	}

	return nil
}

func (driver *mysql) AppliedMigrations() (migrations []*models.Migration, err error) {
	if driver.conn == nil {
		return nil, &drivers.AppError{
			OrigErr: errors.New("driver has no connection established"),
			Message: "database connection is missing",
			Driver:  driverName,
		}
	}

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

	if err := driver.createSchemaTableIfNotExists(); err != nil {
		return nil, err
	}

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
			Command: "select_applied_migrations",
			Query:   []byte(query),
		}
	}

	for rows.Next() {
		if err := rows.Scan(&version, &name); err != nil {
			return nil, &drivers.DatabaseError{
				OrigErr: err,
				Driver:  driverName,
				Message: "failed to scan applied migration row",
				Command: "scan_applied_migrations",
			}
		}

		appliedMigrations = append(appliedMigrations, &models.Migration{
			Name:      name,
			Version:   version,
			Direction: models.Up,
		})
	}

	return appliedMigrations, nil
}

func currentDatabaseNameFromDB(conn *sql.Conn, config *Config) (string, error) {
	query := "SELECT DATABASE()"

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.StatementTimeoutInSecs)*time.Second)
	defer cancel()

	var databaseName string
	if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
		return "", &drivers.DatabaseError{
			OrigErr: err,
			Driver:  driverName,
			Message: "failed to fetch database name",
			Command: "current_database",
			Query:   []byte(query),
		}
	}

	return databaseName, nil
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

func (driver *mysql) addMigrationQuery(migration *models.Migration) string {
	if migration.Direction == models.Down {
		return fmt.Sprintf("DELETE FROM %s WHERE (Version=%d AND NAME='%s')", driver.config.MigrationsTable, migration.Version, migration.Name)
	}
	return fmt.Sprintf("INSERT INTO %s (Version, Name) VALUES (%d, '%s')", driver.config.MigrationsTable, migration.Version, migration.Name)
}

func (driver *mysql) SetConfig(key string, value interface{}) error {
	if driver.config != nil {
		switch key {
		case "StatementTimeoutInSecs":
			n, ok := value.(int)
			if ok {
				driver.config.StatementTimeoutInSecs = n
				return nil
			}
			return fmt.Errorf("incorrect value type for %s", key)
		case "MigrationsTable":
			n, ok := value.(string)
			if ok {
				driver.config.MigrationsTable = n
				return nil
			}
			return fmt.Errorf("incorrect value type for %s", key)
		}
	}

	return fmt.Errorf("incorrect key name %q", key)
}
