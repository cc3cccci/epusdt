package data

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestCleanupExpiredTransactionLocksUnderSQLiteContention(t *testing.T) {
	dir := t.TempDir()
	cfg := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{SingularTable: true},
		Logger:         logger.Default.LogMode(logger.Silent),
	}
	runtimeDB, err := dao.OpenConfiguredSQLite(filepath.Join(dir, "runtime.db"), cfg, 2)
	if err != nil {
		t.Fatalf("open runtime sqlite: %v", err)
	}
	mainDB, err := dao.OpenConfiguredSQLite(filepath.Join(dir, "main.db"), cfg, 1)
	if err != nil {
		t.Fatalf("open main sqlite: %v", err)
	}
	previousRuntime := dao.RuntimeDB
	previousMain := dao.Mdb
	dao.RuntimeDB = runtimeDB
	dao.Mdb = mainDB
	t.Cleanup(func() {
		dao.RuntimeDB = previousRuntime
		dao.Mdb = previousMain
		closeGorm(t, runtimeDB)
		closeGorm(t, mainDB)
	})
	if err := runtimeDB.AutoMigrate(&mdb.TransactionLock{}); err != nil {
		t.Fatalf("migrate transaction_lock: %v", err)
	}
	if err := mainDB.AutoMigrate(&mdb.Orders{}); err != nil {
		t.Fatalf("migrate orders: %v", err)
	}
	order := &mdb.Orders{
		TradeId:        "trade-keep",
		OrderId:        "order-keep",
		Status:         mdb.StatusWaitPay,
		ActualAmount:   1.25,
		Token:          "USDT",
		Network:        "tron",
		ReceiveAddress: "TKeepAddress",
	}
	if err := mainDB.Create(order).Error; err != nil {
		t.Fatalf("create order: %v", err)
	}

	sqlDB, err := runtimeDB.DB()
	if err != nil {
		t.Fatalf("runtime sql DB: %v", err)
	}
	// Keep the second connection open after it is returned. Cleanup must wait
	// on an already-open connection; opening one while the write lock is held
	// can block inside sqlite3_open before busy_timeout is applied.
	sqlDB.SetMaxIdleConns(2)

	activeExpires := seedTransactionLocks(t)
	if err := CleanupExpiredTransactionLocks(); err != nil {
		t.Fatalf("cleanup without contention: %v", err)
	}
	assertLockExpiry(t, activeExpires, false)
	assertOrderUntouched(t)

	ctx := context.Background()
	holder, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("holder connection: %v", err)
	}
	defer holder.Close()
	spare, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("spare connection: %v", err)
	}
	assertBusyTimeout(t, holder)
	assertBusyTimeout(t, spare)
	if err := spare.Close(); err != nil {
		t.Fatalf("return spare connection: %v", err)
	}

	activeExpires = seedTransactionLocks(t)
	if _, err := holder.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("begin short hold: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- CleanupExpiredTransactionLocks()
	}()
	select {
	case err := <-errCh:
		_, _ = holder.ExecContext(ctx, "ROLLBACK")
		t.Fatalf("cleanup returned while the writer still held the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := holder.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit short hold: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("cleanup after short contention: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not finish after the writer released the lock")
	}
	assertLockExpiry(t, activeExpires, false)
	assertOrderUntouched(t)

	activeExpires = seedTransactionLocks(t)
	if _, err := holder.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("begin long hold: %v", err)
	}
	start := time.Now()
	go func() {
		errCh <- CleanupExpiredTransactionLocks()
	}()
	var cleanupErr error
	select {
	case cleanupErr = <-errCh:
	case <-time.After(8 * time.Second):
		_, _ = holder.ExecContext(ctx, "ROLLBACK")
		t.Fatal("cleanup did not return within 8s while the writer held the lock")
	}
	elapsed := time.Since(start)
	if _, err := holder.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback long hold: %v", err)
	}
	if cleanupErr == nil {
		t.Fatal("cleanup error = nil, want SQLITE_BUSY while the writer holds the lock")
	}
	if !sqliteBusyError(cleanupErr) {
		t.Fatalf("cleanup error = %v, want SQLITE_BUSY", cleanupErr)
	}
	if elapsed < 4*time.Second || elapsed > 8*time.Second {
		t.Fatalf("busy cleanup elapsed = %s, want between 4s and 8s", elapsed)
	}
	assertLockExpiry(t, activeExpires, true)
	assertOrderUntouched(t)

	if err := CleanupExpiredTransactionLocks(); err != nil {
		t.Fatalf("cleanup after the writer rolled back: %v", err)
	}
	assertLockExpiry(t, activeExpires, false)
	assertOrderUntouched(t)
}

func seedTransactionLocks(t *testing.T) time.Time {
	t.Helper()
	if err := dao.RuntimeDB.Where("id > ?", 0).Delete(&mdb.TransactionLock{}).Error; err != nil {
		t.Fatalf("clear locks: %v", err)
	}
	activeExpires := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	rows := []mdb.TransactionLock{
		{
			Network: "tron", Address: "addr-expired", Token: "USDT",
			AmountScaled: 100, AmountPrecision: 2, AmountText: "1.00",
			TradeId: "trade-expired", ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
		{
			Network: "tron", Address: "addr-active", Token: "USDT",
			AmountScaled: 200, AmountPrecision: 2, AmountText: "2.00",
			TradeId: "trade-active", ExpiresAt: activeExpires,
		},
	}
	if err := dao.RuntimeDB.Create(&rows).Error; err != nil {
		t.Fatalf("seed locks: %v", err)
	}
	return activeExpires
}

func assertLockExpiry(t *testing.T, activeExpires time.Time, wantExpired bool) {
	t.Helper()
	var locks []mdb.TransactionLock
	if err := dao.RuntimeDB.Order("trade_id asc").Find(&locks).Error; err != nil {
		t.Fatalf("list locks: %v", err)
	}
	got := map[string]mdb.TransactionLock{}
	for _, lock := range locks {
		got[lock.TradeId] = lock
	}
	expired, hasExpired := got["trade-expired"]
	if wantExpired {
		if !hasExpired {
			t.Fatal("expired lock was removed while cleanup failed")
		}
		if expired.AmountScaled != 100 || expired.Address != "addr-expired" {
			t.Fatalf("expired lock changed: %+v", expired)
		}
	} else if hasExpired {
		t.Fatalf("expired lock remains: %+v", expired)
	}
	active, ok := got["trade-active"]
	if !ok {
		t.Fatal("active lock was removed")
	}
	if active.AmountScaled != 200 || active.Address != "addr-active" || active.Token != "USDT" {
		t.Fatalf("active lock changed: %+v", active)
	}
	if !active.ExpiresAt.Equal(activeExpires) && active.ExpiresAt.UTC().Truncate(time.Second) != activeExpires {
		t.Fatalf("active expires_at = %s, want %s", active.ExpiresAt, activeExpires)
	}
	if len(got) != 1+boolToInt(wantExpired) {
		t.Fatalf("locks = %+v", got)
	}
}

func assertOrderUntouched(t *testing.T) {
	t.Helper()
	var order mdb.Orders
	if err := dao.Mdb.Where("trade_id = ?", "trade-keep").Take(&order).Error; err != nil {
		t.Fatalf("reload order: %v", err)
	}
	if order.Status != mdb.StatusWaitPay || order.ActualAmount != 1.25 || order.ReceiveAddress != "TKeepAddress" || order.Token != "USDT" {
		t.Fatalf("order changed: status=%d amount=%v address=%s token=%s", order.Status, order.ActualAmount, order.ReceiveAddress, order.Token)
	}
}

func assertBusyTimeout(t *testing.T, conn *sql.Conn) {
	t.Helper()
	var busy int64
	if err := conn.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&busy); err != nil {
		t.Fatalf("pragma busy_timeout: %v", err)
	}
	if busy != int64(dao.SQLiteBusyTimeoutMS) {
		t.Fatalf("busy_timeout = %d, want %d", busy, dao.SQLiteBusyTimeoutMS)
	}
}

func sqliteBusyError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "sqlite_busy")
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func closeGorm(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	if err != nil {
		t.Errorf("db handle: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		t.Errorf("close db: %v", err)
	}
}
