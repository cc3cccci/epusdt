package dao

import (
	"database/sql"

	"gorm.io/gorm"
)

// SQLiteBusyTimeoutMS is applied to every new SQLite connection.
// busy_timeout is per connection. A single PRAGMA on the pool does not
// carry over to connections opened later.
const SQLiteBusyTimeoutMS = 5000

// OpenConfiguredSQLite opens a business or runtime SQLite database with the
// production per-connection pragmas and pool limits.
func OpenConfiguredSQLite(path string, cfg *gorm.Config, maxOpenConns int) (*gorm.DB, error) {
	db, err := openDB(path, cfg)
	if err != nil {
		return nil, err
	}
	if _, err = configureSQLite(db, maxOpenConns); err != nil {
		return nil, err
	}
	return db, nil
}

func configureSQLite(db *gorm.DB, maxOpenConns int) (*sql.DB, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	if maxOpenConns <= 0 {
		maxOpenConns = 1
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(1)

	// journal_mode, synchronous, and busy_timeout are set in Driver.Open via
	// sqliteFileDSN. Repeating them here would only configure one pooled connection.
	if err := sqlDB.Ping(); err != nil {
		return nil, err
	}

	return sqlDB, nil
}
