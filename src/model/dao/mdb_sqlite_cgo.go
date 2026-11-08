//go:build sqlite_cgo

package dao

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// sqliteFileDSN uses mattn/go-sqlite3 parameters. That driver applies
// _busy_timeout, _journal_mode, and _synchronous in Driver.Open for every
// connection. It does not understand modernc's _pragma names.
func sqliteFileDSN(path string) string {
	return fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&_synchronous=NORMAL", path, SQLiteBusyTimeoutMS)
}

func openDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(sqliteFileDSN(dsn)), cfg)
	if err != nil {
		return nil, err
	}
	return db, nil
}
