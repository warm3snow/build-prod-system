// EXP-09 集成测试：Outbox 与订单同事务、Inbox 去重副作用唯一、投递状态机与对账。
// 需要真实 MySQL：TEST_MYSQL_DSN 未设置时跳过。
package mysql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/warm3snow/build-prod-system/internal/event"
)

// TestOutboxWrittenWithOrder 下单事务提交 == 事件提交；重放不产生新事件。
func TestOutboxWrittenWithOrder(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp09-obx")
	resetSku(t, db, sku, 10, userID)

	ctx := context.Background()
	trace := event.TraceContext{RequestID: "rid-test", TraceParent: "00-abc-def-01"}

	o, err := store.CreateOrder(ctx, userID, sku, "", "obx-key", hashFor(userID, sku), trace)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if o.Replayed {
		t.Fatal("expected new order")
	}

	var rows []OutboxEvent
	if err := db.Where("order_id = ?", o.ID).Find(&rows).Error; err != nil {
		t.Fatalf("load outbox: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 outbox event, got %d", len(rows))
	}
	row := rows[0]
	if row.Status != StatusPending {
		t.Errorf("expected PENDING, got %s", row.Status)
	}
	if row.EventID == "" || len(row.EventID) != 36 {
		t.Errorf("expected uuid event id, got %q", row.EventID)
	}
	if row.RequestID != "rid-test" || row.TraceParent != "00-abc-def-01" {
		t.Errorf("trace context not persisted: %+v", row)
	}
	var e event.OrderCreated
	if err := json.Unmarshal([]byte(row.Payload), &e); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if e.EventID != row.EventID || e.OrderID != o.ID || e.SKU != sku {
		t.Errorf("payload mismatch: %+v", e)
	}

	// 同幂等键重放：不产生新订单，也不产生新事件。
	o2, err := store.CreateOrder(ctx, userID, sku, "", "obx-key", hashFor(userID, sku), trace)
	if err != nil {
		t.Fatalf("replay order: %v", err)
	}
	if !o2.Replayed {
		t.Fatal("expected replay")
	}
	var n int64
	if err := db.Model(&OutboxEvent{}).Where("order_id = ?", o.ID).Count(&n).Error; err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 1 {
		t.Errorf("replay must not add event, got %d", n)
	}
}

// TestInboxDedupSideEffectOnce 同一事件重复消费：副作用恰好一次。
func TestInboxDedupSideEffectOnce(t *testing.T) {
	db, store := testDB(t)
	userID := uniquePrefix("exp09-inbox")

	e := event.OrderCreated{
		EventID:   event.NewID(),
		OrderID:   1, // 与真实订单无外键关联，仅验证去重与副作用语义
		UserID:    userID,
		SKU:       "P1",
		CreatedAt: time.Now(),
	}
	ctx := context.Background()

	dup1, err := store.ProcessInboxEvent(ctx, e)
	if err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if dup1 {
		t.Fatal("first consume must not be dup")
	}
	var notifs int64
	if err := db.Model(&OrderNotification{}).Where("order_id = ?", e.OrderID).Count(&notifs).Error; err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if notifs != 1 {
		t.Fatalf("expected 1 notification, got %d", notifs)
	}

	dup2, err := store.ProcessInboxEvent(ctx, e)
	if err != nil {
		t.Fatalf("second consume: %v", err)
	}
	if !dup2 {
		t.Fatal("second consume must be dup")
	}
	if err := db.Model(&OrderNotification{}).Where("order_id = ?", e.OrderID).Count(&notifs).Error; err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if notifs != 1 {
		t.Errorf("side effect must stay unique, got %d", notifs)
	}
}

// TestOutboxStateMachine PENDING → SENT；尝试耗尽 → DEAD；重放 → PENDING。
func TestOutboxStateMachine(t *testing.T) {
	db, store := testDB(t)
	userID := uniquePrefix("exp09-sm")
	resetSku(t, db, "P1", 5, userID)

	ctx := context.Background()
	o, err := store.CreateOrder(ctx, userID, "P1", "", "sm-key", hashFor(userID, "P1"), event.TraceContext{})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	var row OutboxEvent
	if err := db.Where("order_id = ?", o.ID).First(&row).Error; err != nil {
		t.Fatalf("load outbox: %v", err)
	}

	// PENDING 可见
	pending, err := store.FetchPendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("fetch pending: %v", err)
	}
	if len(pending) != 1 || pending[0].EventID != row.EventID {
		t.Fatalf("expected 1 pending event, got %+v", pending)
	}

	// 标记 SENT 后从 PENDING 消失
	if err := store.MarkOutboxSent(ctx, row.EventID); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	pending, err = store.FetchPendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("fetch pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after sent, got %d", len(pending))
	}

	// DEAD 状态机：另建事件，尝试耗尽
	o2, err := store.CreateOrder(ctx, userID, "P1", "", "sm-key-2", hashFor(userID, "P1"), event.TraceContext{})
	if err != nil {
		t.Fatalf("create order 2: %v", err)
	}
	var row2 OutboxEvent
	if err := db.Where("order_id = ?", o2.ID).First(&row2).Error; err != nil {
		t.Fatalf("load outbox 2: %v", err)
	}
	dead := false
	for i := 0; i < 3; i++ {
		dead, err = store.MarkOutboxFailed(ctx, row2.ID, "kafka unavailable", 3)
		if err != nil {
			t.Fatalf("mark failed: %v", err)
		}
	}
	if !dead {
		t.Fatal("expected DEAD after max attempts")
	}
	var check OutboxEvent
	if err := db.First(&check, row2.ID).Error; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if check.Status != StatusDead {
		t.Errorf("expected DEAD, got %s", check.Status)
	}

	// 显式重放：DEAD → PENDING
	if n, err := store.RequeueDead(ctx); err != nil || n != 1 {
		t.Fatalf("requeue dead: n=%d err=%v", n, err)
	}
	if err := db.First(&check, row2.ID).Error; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if check.Status != StatusPending {
		t.Errorf("expected PENDING after requeue, got %s", check.Status)
	}
}

// TestOutboxPerOrderReconcile 对账不变量：新订单数 == outbox 事件数（含 SENT）。
func TestOutboxPerOrderReconcile(t *testing.T) {
	db, store := testDB(t)
	const sku = "P1"
	userID := uniquePrefix("exp09-rc")
	resetSku(t, db, sku, 20, userID)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := store.CreateOrder(ctx, userID, sku, "", "rc-key-"+string(rune('a'+i)), hashFor(userID, sku), event.TraceContext{}); err != nil {
			t.Fatalf("create order %d: %v", i, err)
		}
	}
	var orders int64
	if err := db.Model(&OrderModel{}).Where("user_id = ?", userID).Count(&orders).Error; err != nil {
		t.Fatalf("count orders: %v", err)
	}
	var events int64
	if err := db.Model(&OutboxEvent{}).Where("user_id = ?", userID).Count(&events).Error; err != nil {
		t.Fatalf("count events: %v", err)
	}
	if orders != 10 || events != 10 {
		t.Errorf("reconcile broken: orders=%d events=%d", orders, events)
	}
}
