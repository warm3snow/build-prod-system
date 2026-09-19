// EXP-09 Outbox/Inbox：订单事件与后置副作用的持久化层。
//
// 可靠性边界（ADR-018）：
//   - Outbox 与订单在同一事务提交：订单提交 == 事件提交，无双写缺口；
//   - Relay at-least-once：仅在 Kafka 确认后标记 SENT，确认前崩溃允许重发；
//   - Inbox 与后置副作用同一事务：重复投递由 event_id 唯一键去重，副作用唯一；
//   - 坏消息不静默跳过：attempts 达到上限标记 DEAD，显式失败、可重放。
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/warm3snow/build-prod-system/internal/event"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EventStatus Outbox 事件投递状态机。
// PENDING → SENT（确认）；PENDING → DEAD（超过最大尝试，显式失败可重放）。
type EventStatus string

const (
	StatusPending EventStatus = "PENDING"
	StatusSent    EventStatus = "SENT"
	StatusDead    EventStatus = "DEAD"
)

// OutboxEvent 订单事件 Outbox。写入与订单同事务；Relay 轮询 PENDING 并投递。
type OutboxEvent struct {
	ID          int64       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	EventID     string      `gorm:"column:event_id;uniqueIndex;size:36;not null" json:"event_id"`
	OrderID     int64       `gorm:"column:order_id;index:idx_outbox_order;not null" json:"order_id"`
	UserID      string      `gorm:"column:user_id;size:64;not null" json:"user_id"`
	SKU         string      `gorm:"column:sku;size:64;not null" json:"sku"`
	Payload     string      `gorm:"column:payload;type:json;not null" json:"payload"`
	RequestID   string      `gorm:"column:request_id;size:64" json:"request_id"`
	TraceParent string      `gorm:"column:trace_parent;size:128" json:"trace_parent"`
	Status      EventStatus `gorm:"column:status;size:16;not null;index:idx_outbox_status_id,priority:1" json:"status"`
	Attempts    int         `gorm:"column:attempts;not null;default:0" json:"attempts"`
	LastError   string      `gorm:"column:last_error;size:512" json:"last_error"`
	CreatedAt   time.Time   `gorm:"column:created_at;not null;index:idx_outbox_status_id,priority:2" json:"created_at"`
	SentAt      *time.Time  `gorm:"column:sent_at" json:"sent_at"`
}

func (OutboxEvent) TableName() string { return "outbox_events" }

// InboxEvent 消费去重记录。event_id 主键：重复投递第二次插入触发 1062，跳过副作用。
type InboxEvent struct {
	EventID   string    `gorm:"column:event_id;primaryKey;size:36" json:"event_id"`
	OrderID   int64     `gorm:"column:order_id;index:idx_inbox_order;not null" json:"order_id"`
	UserID    string    `gorm:"column:user_id;size:64;not null" json:"user_id"`
	SKU       string    `gorm:"column:sku;size:64;not null" json:"sku"`
	Payload   string    `gorm:"column:payload;type:json;not null" json:"payload"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"created_at"`
}

func (InboxEvent) TableName() string { return "inbox_events" }

// OrderNotification 后置副作用（订单已受理通知，模拟真实通知/投影任务）。
// 不变量：每个订单恰好一行（order_id 唯一），由 Inbox 去重保证。
type OrderNotification struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	OrderID   int64     `gorm:"column:order_id;uniqueIndex;not null" json:"order_id"`
	UserID    string    `gorm:"column:user_id;index:idx_notif_user;size:64;not null" json:"user_id"`
	Message   string    `gorm:"column:message;size:128;not null" json:"message"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"created_at"`
}

func (OrderNotification) TableName() string { return "order_notifications" }

// writeOutbox 在下单事务内写入事件（调用方已持有 tx）。
// 重放订单不产生新事件：事件数 == 新订单数，逐事件对账的前提。
func writeOutbox(tx *gorm.DB, orderID int64, e event.OrderCreated) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	row := OutboxEvent{
		EventID:     e.EventID,
		OrderID:     orderID,
		UserID:      e.UserID,
		SKU:         e.SKU,
		Payload:     string(payload),
		RequestID:   e.RequestID,
		TraceParent: e.TraceParent,
		Status:      StatusPending,
		CreatedAt:   e.CreatedAt,
	}
	if err := tx.Create(&row).Error; err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// FetchPendingOutbox 按 ID 升序取 PENDING 事件（最老优先，保证投递顺序与下单顺序一致）。
func (s *Store) FetchPendingOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	var rows []OutboxEvent
	if err := s.db.WithContext(ctx).
		Where("status = ?", StatusPending).
		Order("id ASC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("fetch pending outbox: %w", err)
	}
	return rows, nil
}

// MarkOutboxSent 标记单个事件已投递（Kafka 确认后调用）。按 event_id 幂等更新。
func (s *Store) MarkOutboxSent(ctx context.Context, eventID string) error {
	now := time.Now()
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("event_id = ? AND status = ?", eventID, StatusPending).
		Updates(map[string]any{"status": StatusSent, "sent_at": now}).Error; err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	return nil
}

// MarkOutboxSentBatch 批量标记事件已投递（单个事务，1 次 fsync）。
// 逐条 autocommit 的 UPDATE 每条一次 fsync，在 200 TPS 投递速率下会与下单事务
// 竞争 redo log fsync（EXP-09 实测 handler commit 排队），批量后 fsync 归组。
// 只按主键 id 定位：不带 status 条件，避免优化器走 idx_outbox_status_id 二级索引
// 大范围扫描加 next-key 锁——EXP-09 实测该锁与下单 INSERT 的新行冲突，
// UPDATE 堆积等锁 2s+ 超时。纯主键点更新锁范围最小（仅行锁）。
// 标记幂等：行由调用方刚从 PENDING 集合取出，重复标记 SENT 无害。
func (s *Store) MarkOutboxSentBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&OutboxEvent{}).
			Where("id IN ?", ids).
			Updates(map[string]any{"status": StatusSent, "sent_at": now}).Error
	})
	if err != nil {
		return fmt.Errorf("batch mark outbox sent: %w", err)
	}
	return nil
}

// MarkOutboxFailed 记录一次投递失败：attempts+1；达到 maxAttempts 时标记 DEAD。
// 返回 dead=true 表示该事件已进入显式失败状态（保留记录、可重放，不静默跳过）。
func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, errMsg string, maxAttempts int) (bool, error) {
	var row OutboxEvent
	if err := s.db.WithContext(ctx).First(&row, id).Error; err != nil {
		return false, fmt.Errorf("load outbox event: %w", err)
	}
	row.Attempts++
	row.LastError = truncate(errMsg, 512)
	if row.Attempts >= maxAttempts {
		row.Status = StatusDead
	} else {
		row.Status = StatusPending // 保持待投递，下一轮重试
	}
	if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
		return false, fmt.Errorf("mark outbox failed: %w", err)
	}
	return row.Status == StatusDead, nil
}

// RequeueDead 将 DEAD 事件重置为 PENDING（显式重放入口，供实验手册与运维使用）。
func (s *Store) RequeueDead(ctx context.Context) (int64, error) {
	res := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("status = ?", StatusDead).
		Updates(map[string]any{"status": StatusPending, "attempts": 0, "last_error": ""})
	if res.Error != nil {
		return 0, fmt.Errorf("requeue dead outbox: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// CountPendingOutbox 快速统计 PENDING 事件数（EXP-10 下单反压水位采样用）。
// 走 idx_outbox_status_id 索引的 COUNT，无锁、低开销。
func (s *Store) CountPendingOutbox(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("status = ?", StatusPending).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count pending outbox: %w", err)
	}
	return n, nil
}

// OutboxStats 积压统计：PENDING/DEAD 数量与最老 PENDING 年龄（告警与对账用）。
type OutboxStats struct {
	Pending          int64
	Dead             int64
	OldestPendingAge time.Duration
}

func (s *Store) OutboxStats(ctx context.Context) (OutboxStats, error) {
	var st OutboxStats
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("status = ?", StatusPending).Count(&st.Pending).Error; err != nil {
		return st, fmt.Errorf("count pending outbox: %w", err)
	}
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("status = ?", StatusDead).Count(&st.Dead).Error; err != nil {
		return st, fmt.Errorf("count dead outbox: %w", err)
	}
	// Find（而非 First）：空表是常态，避免每次采集都打 record not found 日志。
	var oldest []OutboxEvent
	if err := s.db.WithContext(ctx).Where("status = ?", StatusPending).
		Order("id ASC").Limit(1).Find(&oldest).Error; err != nil {
		return st, fmt.Errorf("oldest pending outbox: %w", err)
	}
	if len(oldest) > 0 {
		st.OldestPendingAge = time.Since(oldest[0].CreatedAt)
	}
	return st, nil
}

// ProcessInboxBatch 批量消费事务（EXP-10）：N 条事件的 Inbox 去重 + 后置副作用
// 在单个事务内完成（1 次 fsync，突破每事务 fsync 的 ~250/s 串行墙）。
// 用 INSERT IGNORE 幂等：重复 event_id 被忽略（new 计数只含真正插入的行），
// 副作用（notification）与 Inbox 同事务同批，唯一性保持。
// 返回 new/dup 计数；失败（DB 不可用）整批回滚，调用方整批重试（幂等安全）。
func (s *Store) ProcessInboxBatch(ctx context.Context, events []event.OrderCreated) (newCount, dupCount int, err error) {
	if len(events) == 0 {
		return 0, 0, nil
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		rows := make([]InboxEvent, 0, len(events))
		notifs := make([]OrderNotification, 0, len(events))
		now := time.Now()
		for _, e := range events {
			payload, err := json.Marshal(e)
			if err != nil {
				return fmt.Errorf("marshal event: %w", err)
			}
			rows = append(rows, InboxEvent{
				EventID:   e.EventID,
				OrderID:   e.OrderID,
				UserID:    e.UserID,
				SKU:       e.SKU,
				Payload:   string(payload),
				CreatedAt: now,
			})
			notifs = append(notifs, OrderNotification{
				OrderID:   e.OrderID,
				UserID:    e.UserID,
				Message:   "order accepted",
				CreatedAt: now,
			})
		}
		// INSERT IGNORE：重复 event_id 静默忽略（返回受影响行数 = 新插入数）。
		res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows)
		if res.Error != nil {
			return fmt.Errorf("batch insert inbox: %w", res.Error)
		}
		newCount = int(res.RowsAffected)
		dupCount = len(events) - newCount
		// 通知副作用：对整批做 INSERT IGNORE（order_id 唯一）。
		// dup 事件的通知历史上已随 Inbox 同事务提交，此处被唯一键忽略；
		// 与逐条路径语义一致：Inbox 存在 ⇒ 通知存在（同事务），dup 不产生新副作用。
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&notifs).Error; err != nil {
			return fmt.Errorf("batch insert notifications: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return newCount, dupCount, nil
}

// ProcessInboxEvent 消费事务：Inbox 去重 + 后置副作用，同一事务提交。
// 返回 dup=true 表示 event_id 已消费过（重复投递），本次不产生副作用；
// 位点提交由调用方（Consumer）在事务成功后执行。
func (s *Store) ProcessInboxEvent(ctx context.Context, e event.OrderCreated) (bool, error) {
	dup := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		row := InboxEvent{
			EventID:   e.EventID,
			OrderID:   e.OrderID,
			UserID:    e.UserID,
			SKU:       e.SKU,
			Payload:   string(payload),
			CreatedAt: time.Now(),
		}
		if err := tx.Create(&row).Error; err != nil {
			if isDuplicate(err) {
				dup = true
				return nil // 重复投递：跳过副作用，事务仍成功
			}
			return fmt.Errorf("insert inbox: %w", err)
		}
		notif := OrderNotification{
			OrderID:   e.OrderID,
			UserID:    e.UserID,
			Message:   "order accepted",
			CreatedAt: time.Now(),
		}
		// 冲突兜底：Inbox 已去重，理论上不会冲突；OnConflict 仅防御异常历史数据。
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&notif).Error; err != nil {
			return fmt.Errorf("insert notification: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return dup, nil
}

// truncate 截断错误消息，避免超长错误撑爆 last_error 列。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
