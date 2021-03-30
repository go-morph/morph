package file

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/go-morph/morph/models"
	"github.com/go-morph/morph/sources"
)

func init() {
	sources.Register("file", &File{})
}

type File struct {
	url        string
	path       string
	migrations []*models.Migration
}

func (f *File) Open(sourceURL string) (sources.Source, error) {
	uri, err := url.Parse(sourceURL)
	if err != nil {
		return nil, err
	}

	// host might be "." for relative URLs like file://./migrations
	p := uri.Host + uri.Path

	// if no path provided, default to current directory
	if len(p) == 0 {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		p = wd
	}

	// make path absolute if required
	if p[0:1] == "." {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		p = abs
	}

	nf := &File{
		url:  sourceURL,
		path: p,
	}

	if err := nf.readMigrations(); err != nil {
		return nil, fmt.Errorf("cannot read migrations in path %q: %w", p, err)
	}

	return nf, nil
}

func (f *File) readMigrations() error {
	info, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("file %q is not a directory", info.Name())
	}

	migrations := []*models.Migration{}
	walkerr := filepath.Walk(f.path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}

		m := &models.Migration{Bytes: file, Name: path}
		migrations = append(migrations, m)
		return nil
	})
	if walkerr != nil {
		return walkerr
	}

	f.migrations = migrations
	return nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) Migrations() []*models.Migration {
	return f.migrations
}
