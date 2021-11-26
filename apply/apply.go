package apply

import (
	"github.com/go-morph/morph"
	"github.com/go-morph/morph/models"
	"github.com/go-morph/morph/sources"
	"github.com/spf13/cobra"
)

func Migrate(dsn, source, driverName, path string) error {
	src, err := sources.Open(source, path)
	if err != nil {
		return err
	}
	defer src.Close()

	engine, err := morph.NewFromConnURL(dsn, src, driverName)
	if err != nil {
		return err
	}

	return engine.ApplyAll(models.Up)
}

func Up(limit int, arge cobra.PositionalArgs) (int, error) {
	// TODO:
	// this function needs to accept a number as an argument to apply N-migrations upwards

	// 1. get current version from the migrations table
	// 2. check if we have new versions in the migrations and it is in the range of limit
	// 3. if the limit is smaller thane zero, run all the ramaining migrations?
	// 4. handle failure (maybe rollback the last errored migration script with its `down` script?)
	// and return number of successful
	return -1, nil
}

func Down(limit int, arge cobra.PositionalArgs) (int, error) {
	// TODO:
	// opposiste of Up
	return -1, nil
}
