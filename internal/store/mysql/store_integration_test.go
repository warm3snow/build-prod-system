// EXP-03 集成测试：并发下单不超卖、幂等重放、同键冲突、库存守恒。
// 需要真实 MySQL：TEST_MYSQL_DSN 未设置时跳过。
// 运行方式：
//
//	kubectl -n order-lab port-forward svc/mysql 13306:3306 &
//	TEST_MYSQL_DSN='flash:flash@tcp(127.0.0.1:13306)/flash?parseTime=true' go test ./internal/store/mysql/ -run 'TestConcurrency|TestIdempotency' -v
package mysql

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/warm3snow/build-prod-system/internal/event"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testDB 连接真实 MySQL；DSN 未设置时跳过测试。
func testDB(t *testing.T) (*gorm.DB, *Store) {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; skipping integration test")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)
	store := New(db)
	if err := store.InitSchema(context.Background()); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return db, store
}

// resetSku 重置指定 SKU 库存并清理指定用户的测试数据。
func resetSku(t *testing.T, db *gorm.DB, sku string, stock int, userIDs ...string) {
	t.Helper()
	if err := db.Model(&Inventory{}).Where("sku = ?", sku).Update("stock", stock).Error; err != nil {
		t.Fatalf("reset stock: %v", err)
	}
	if len(userIDs) > 0 {
		if err := db.Where("user_id IN ?", userIDs).Delete(&Idempotency{}).Error; err != nil {
			t.Fatalf("clean idempotency: %v", err)
		}
		if err := db.Where("user_id IN ?", userIDs).Delete(&OrderModel{}).Error; err != nil {
			t.Fatalf("clean orders: %v", err)
		}
	}
}

func countOrders(t *testing.T, db *gorm.DB, userID string) int64 {
	t.Helper()
	var n int64
	if err := db.Model(&OrderModel{}).Where("user_id = ?", userID).Count(&n).Error; err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return n
}

// TestConcurrentNoOversell 库存 100，并发 200 下单：
// 恰好 100 成功、100 库存不足，库存为 0，订单数 100（守恒）。
func TestConcurrentNoOversell(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userPrefix := uniquePrefix("exp03-u")
	resetSku(t, db, sku, 100)

	var wg sync.WaitGroup
	var mu sync.Mutex
	success, outOfStock, other := 0, 0, 0
	const workers = 200
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			userID := fmt.Sprintf("%s-%d", userPrefix, i%20)
			_, err := store.CreateOrder(ctx, userID, sku, "", fmt.Sprintf("key-%d", i), hashFor(userID, sku), event.TraceContext{})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				success++
			case err == ErrOutOfStock:
				outOfStock++
			default:
				other++
			}
		}(i)
	}
	wg.Wait()

	if success != 100 {
		t.Errorf("expected 100 success, got %d", success)
	}
	if outOfStock != 100 {
		t.Errorf("expected 100 out-of-stock, got %d", outOfStock)
	}
	if other != 0 {
		t.Errorf("unexpected other errors: %d", other)
	}
	stock, err := store.GetStock(context.Background(), sku)
	if err != nil {
		t.Fatalf("get stock: %v", err)
	}
	if stock != 0 {
		t.Errorf("expected stock 0, got %d", stock)
	}
	var userIDs []string
	for i := 0; i < 20; i++ {
		userIDs = append(userIDs, fmt.Sprintf("%s-%d", userPrefix, i))
	}
	var orders int64
	if err := db.Model(&OrderModel{}).Where("user_id IN ?", userIDs).Count(&orders).Error; err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orders != 100 {
		t.Errorf("expected 100 orders, got %d", orders)
	}
}

// TestConcurrentIdempotencyHeavy 同一幂等键 200 并发高压：仅 1 个创建，其余全部重放，库存只扣 1。
// 覆盖 insert-first 幂等 + 死锁重试在极端同键竞争下的行为。
func TestConcurrentIdempotencyHeavy(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp03-idem-heavy")
	resetSku(t, db, sku, 200, userID)

	const workers = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	created, replayed, other := 0, 0, 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			o, err := store.CreateOrder(ctx, userID, sku, "", "heavy-same-key", hashFor(userID, sku), event.TraceContext{})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && !o.Replayed:
				created++
			case err == nil && o.Replayed:
				replayed++
			default:
				other++
			}
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Errorf("expected exactly 1 created order, got %d", created)
	}
	if replayed != workers-1 {
		t.Errorf("expected %d replays, got %d", workers-1, replayed)
	}
	if other != 0 {
		t.Errorf("unexpected errors: %d", other)
	}
	if n := countOrders(t, db, userID); n != 1 {
		t.Errorf("expected 1 order row, got %d", n)
	}
	stock, err := store.GetStock(context.Background(), sku)
	if err != nil {
		t.Fatalf("get stock: %v", err)
	}
	if stock != 199 {
		t.Errorf("expected stock 199 (deducted once), got %d", stock)
	}
}

// TestConcurrentIdempotency 同一幂等键并发 50 次：只创建 1 个订单，其余为重放或冲突，库存只扣 1。
func TestConcurrentIdempotency(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp03-idem")
	resetSku(t, db, sku, 50, userID)

	var wg sync.WaitGroup
	var mu sync.Mutex
	created, replayed, other := 0, 0, 0
	const workers = 50
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			o, err := store.CreateOrder(ctx, userID, sku, "", "same-key", hashFor(userID, sku), event.TraceContext{})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && !o.Replayed:
				created++
			case err == nil && o.Replayed:
				replayed++
			default:
				other++
			}
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Errorf("expected exactly 1 created order, got %d", created)
	}
	if replayed != workers-1 {
		t.Errorf("expected %d replays, got %d", workers-1, replayed)
	}
	if other != 0 {
		t.Errorf("unexpected errors: %d", other)
	}
	if n := countOrders(t, db, userID); n != 1 {
		t.Errorf("expected 1 order row, got %d", n)
	}
	stock, err := store.GetStock(context.Background(), sku)
	if err != nil {
		t.Fatalf("get stock: %v", err)
	}
	if stock != 49 {
		t.Errorf("expected stock 49 (deducted once), got %d", stock)
	}
}

// TestIdempotencyConflict 同键不同参数拒绝，不扣库存。
func TestIdempotencyConflict(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp03-conf")
	resetSku(t, db, sku, 10, userID)

	ctx := context.Background()
	if _, err := store.CreateOrder(ctx, userID, sku, "", "conf-key", hashFor(userID, sku), event.TraceContext{}); err != nil {
		t.Fatalf("first order: %v", err)
	}
	_, err := store.CreateOrder(ctx, userID, "P2", "", "conf-key", hashFor(userID, "P2"), event.TraceContext{})
	if err != ErrIdempotencyConf {
		t.Fatalf("expected ErrIdempotencyConf, got %v", err)
	}
	if n := countOrders(t, db, userID); n != 1 {
		t.Errorf("expected 1 order row, got %d", n)
	}
	stock, err := store.GetStock(context.Background(), sku)
	if err != nil {
		t.Fatalf("get stock: %v", err)
	}
	if stock != 9 {
		t.Errorf("expected stock 9, got %d", stock)
	}
}

// TestOutOfStockRollback 库存不足时不产生孤儿订单/幂等记录。
func TestOutOfStockRollback(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp03-oos")
	resetSku(t, db, sku, 0, userID)

	_, err := store.CreateOrder(context.Background(), userID, sku, "", "oos-key", hashFor(userID, sku), event.TraceContext{})
	if err != ErrOutOfStock {
		t.Fatalf("expected ErrOutOfStock, got %v", err)
	}
	if n := countOrders(t, db, userID); n != 0 {
		t.Errorf("expected 0 order rows, got %d", n)
	}
	var idem int64
	if err := db.Model(&Idempotency{}).Where("user_id = ?", userID).Count(&idem).Error; err != nil {
		t.Fatalf("count idempotency: %v", err)
	}
	if idem != 0 {
		t.Errorf("expected 0 idempotency rows, got %d", idem)
	}
}

// hashFor 测试侧参数哈希（store 层不关心哈希算法，只要求同参数稳定）。
func hashFor(userID, sku string) string {
	sum := sha256.Sum256([]byte(userID + "|" + sku + "|\x01"))
	return fmt.Sprintf("%x", sum[:])
}

// uniquePrefix 生成带纳秒时间戳的唯一前缀，隔离共享库中的测试数据。
func uniquePrefix(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}
