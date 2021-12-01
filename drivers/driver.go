package drivers

import (
	"fmt"
	"sync"

	"github.com/go-morph/morph/models"
)

var driversMu sync.RWMutex
var registeredDrivers = make(map[string]Driver)

type Driver interface {
	Ping() error
	Close() error
	Lock() error
	Unlock() error
	Apply(migration *models.Migration, saveVersion bool) error
	AppliedMigrations() ([]*models.Migration, error)
	SetConfig(key string, value interface{}) error
}

type db interface {
	Open(connURL string) (Driver, error)
}

type DriverOption func(Driver)

func SetMigrationTableName(name string) DriverOption {
	return func(d Driver) {
		_ = d.SetConfig("MigrationsTable", name)
	}
}

func SetSatementTimeoutInSeconds(n int) DriverOption {
	return func(d Driver) {
		_ = d.SetConfig("StatementTimeoutInSecs", n)
	}
}

func Connect(connectionURL, driverName string, options ...DriverOption) (Driver, error) {
	driversMu.RLock()
	driver, ok := registeredDrivers[driverName]
	driversMu.RUnlock()

	if !ok {
		return nil, &AppError{
			OrigErr: nil,
			Driver:  driverName,
			Message: "unsupported driver found",
		}
	}

	d, ok := driver.(db)
	if !ok {
		return nil, &AppError{
			OrigErr: nil,
			Driver:  driverName,
			Message: "unsupported driver implementation",
		}
	}

	connectedDriver, err := d.Open(connectionURL)
	if err != nil {
		return nil, err
	}

	return connectedDriver, nil
}

func Register(driverName string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	registeredDrivers[driverName] = driver
}

func GetDriver(driverName string) (Driver, error) {
	driversMu.Lock()
	defer driversMu.Unlock()

	driver, ok := registeredDrivers[driverName]
	if !ok {
		return nil, fmt.Errorf("driver %q not found", driverName)
	}

	return driver, nil
}
