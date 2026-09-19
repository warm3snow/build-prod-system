// Package event 定义订单域事件（EXP-09 Outbox 模式）。
// 事件在下单事务内与订单、库存、幂等一同提交，Relay 负责可靠投递到 Kafka；
// Consumer 以 Inbox 去重后产生后置副作用。事件 ID 全程唯一，是逐事件对账的锚点。
package event

import (
	"time"

	"github.com/google/uuid"
)

// OrderCreated 订单创建事件：下单事务内写入 Outbox 的负载。
// 业务正确性事实（库存、订单）以 MySQL 为准，事件仅用于异步后置任务。
type OrderCreated struct {
	EventID     string    `json:"event_id"`
	OrderID     int64     `json:"order_id"`
	UserID      string    `json:"user_id"`
	SKU         string    `json:"sku"`
	RequestID   string    `json:"request_id,omitempty"`
	TraceParent string    `json:"trace_parent,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// TraceContext 关联上下文：贯穿 HTTP → Outbox → Kafka → Consumer 的完整处理。
// RequestID 由 HTTP 中间件生成；TraceParent 透传 W3C traceparent 头（预留 OTel 接入）。
type TraceContext struct {
	RequestID   string
	TraceParent string
}

// NewID 生成唯一事件 ID（UUID v4）。写入 Outbox 时生成，
// 是 Kafka 消息 key、Inbox 去重键与逐事件对账的唯一标识。
// Go 1.20+ 的 crypto/rand 读取不返回错误，uuid.NewString 无需错误分支。
func NewID() string {
	return uuid.NewString()
}
