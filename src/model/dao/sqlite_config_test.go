package dao

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestPooledSQLiteConnectionsInheritBusyTimeout(t *testing.T) {
	db, err := OpenConfiguredSQLite(filepath.Join(t.TempDir(), "runtime.db"), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{SingularTable: true},
		Logger:         logger.Default.LogMode(logger.Silent),
	}, 2)
	if err != nil {
		t.Fatalf("open configured sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql DB: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})

	ctx := context.Background()
	first, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("first connection: %v", err)
	}
	defer first.Close()
	second, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("second connection: %v", err)
	}
	defer second.Close()

	for name, conn := range map[string]*sql.Conn{"first": first, "second": second} {
		busy := pragmaInt(t, conn, "PRAGMA busy_timeout")
		if busy != SQLiteBusyTimeoutMS {
			t.Fatalf("%s busy_timeout = %d, want %d", name, busy, SQLiteBusyTimeoutMS)
		}
		syncMode := pragmaInt(t, conn, "PRAGMA synchronous")
		if syncMode != 1 {
			t.Fatalf("%s synchronous = %d, want 1 (NORMAL)", name, syncMode)
		}
		journal := pragmaText(t, conn, "PRAGMA journal_mode")
		if !strings.EqualFold(journal, "wal") {
			t.Fatalf("%s journal_mode = %q, want wal", name, journal)
		}
	}
}

func pragmaInt(t *testing.T, conn *sql.Conn, query string) int64 {
	t.Helper()
	var value int64
	if err := conn.QueryRowContext(context.Background(), query).Scan(&value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}

func pragmaText(t *testing.T, conn *sql.Conn, query string) string {
	t.Helper()
	var value string
	if err := conn.QueryRowContext(context.Background(), query).Scan(&value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}
