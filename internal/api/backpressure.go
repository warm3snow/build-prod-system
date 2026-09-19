// EXP-10 下单反压：Outbox 积压达到高水位时拒绝新下单。
//
// 原理：事件链路积压（PENDING）的增长 = 下单速率 - 投递速率。若投递侧持续落后，
// 仅靠 Kafka 队列吸收会让积压无限增长（无界内存/磁盘）。下单前检查积压水位，
// 超过预算时明确拒绝（503 backlog_limited），而不是静默接受后资源耗尽。
// 采样滞后 ≤ 采样间隔（500ms），水位是软预算——允许少量超调。
package api

import (
	"context"
	"sync/atomic"
	"time"
)

// Backpressure 积压水位采样器：后台周期采样 Outbox PENDING 计数。
// order-api 进程内每 500ms 一次 COUNT 查询（索引计数，低开销），
// 下单路径读取原子值，不额外访问 DB。
type Backpressure struct {
	store interface {
		CountPendingOutbox(ctx context.Context) (int64, error)
	}
	limit   int64
	pending atomic.Int64
}

// NewBackpressure 创建采样器。limit <= 0 表示不启用（反压实验开关）。
func NewBackpressure(store interface {
	CountPendingOutbox(ctx context.Context) (int64, error)
}, limit int64) *Backpressure {
	return &Backpressure{store: store, limit: limit}
}

// Run 周期采样循环，随 ctx 取消退出。
func (b *Backpressure) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			n, err := b.store.CountPendingOutbox(sampleCtx)
			cancel()
			if err != nil {
				// 采样失败保持上一值：反压不因采样抖动误放行（保守方向）。
				continue
			}
			b.pending.Store(n)
		}
	}
}

// Enabled 反压是否启用。
func (b *Backpressure) Enabled() bool { return b.limit > 0 }

// OverLimit 当前积压是否超过水位。未启用时永远 false。
func (b *Backpressure) OverLimit() bool {
	return b.limit > 0 && b.pending.Load() >= b.limit
}

// Pending 当前采样的积压数（观察用）。
func (b *Backpressure) Pending() int64 { return b.pending.Load() }
