//go:build !sqlite_cgo

package dao

import (
	"fmt"

	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
)

// sqliteFileDSN uses modernc.org/sqlite _pragma parameters. They run inside
// Driver.Open, so every pooled connection inherits them. The mattn names
// _busy_timeout and _journal_mode are ignored by modernc v1.47.0.
func sqliteFileDSN(path string) string {
	return fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path, SQLiteBusyTimeoutMS)
}

func openDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(sqliteFileDSN(dsn)), cfg)
	if err != nil {
		return nil, err
	}
	return db, nil
}
