// Package relay 实现 Outbox → Kafka 的可靠投递（EXP-09）。
//
// 语义（ADR-018）：
//   - 仅在 Kafka 确认后标记 SENT；确认失败/超时 = 未知结果，保持 PENDING 重试；
//   - 重复投递是合法路径（at-least-once），由 Consumer Inbox 去重；
//   - 投递失败 attempts+1，达到上限标记 DEAD（显式失败，可 SQL 重放），不静默跳过；
//   - Kafka 不可用时轮询持续重试，Outbox 积压有指标与告警；业务下单不依赖 Kafka。
package relay

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/mq/kafka"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

// Config Relay 运行参数。
type Config struct {
	BatchSize    int
	PollInterval time.Duration
	MaxAttempts  int
}

// Relay 轮询 Outbox 并投递到 Kafka。
type Relay struct {
	store  *mysql.Store
	prod   *kafka.Producer
	log    *slog.Logger
	cfg    Config
	stopCh chan struct{}
	doneCh chan struct{}
}

func New(store *mysql.Store, prod *kafka.Producer, log *slog.Logger, cfg Config) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 100
	}
	return &Relay{
		store:  store,
		prod:   prod,
		log:    log,
		cfg:    cfg,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Run 轮询循环；Stop 后返回。使用独立后台循环避免与优雅退出竞争。
func (r *Relay) Run() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.drainOnce()
		case <-statsTicker.C:
			r.updateStats()
		}
	}
}

// updateStats 采集 Outbox 积压水位（告警与看板）。
func (r *Relay) updateStats() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := r.store.OutboxStats(ctx)
	if err != nil {
		RecordRelayError("stats")
		r.log.Warn("outbox stats", "err", err)
		return
	}
	SetOutboxGauges(st.Pending, st.Dead, st.OldestPendingAge)
}

// drainOnce 处理当前积压的一批：
//  1. 逐条解析 payload（坏消息单独走 MarkOutboxFailed → DEAD，不阻塞整批）；
//  2. 合法消息一次性批量发送（WriteMessages 单次刷盘，避免逐条等待 BatchTimeout）；
//  3. 批量确认成功 → 逐条标记 SENT；失败 → 整批保持 PENDING 重试（at-least-once，
//     重复投递由 Consumer Inbox 去重；Kafka 抖动不消耗 attempts）。
func (r *Relay) drainOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := r.store.FetchPendingOutbox(ctx, r.cfg.BatchSize)
	if err != nil {
		RecordRelayError("fetch")
		r.log.Error("fetch pending outbox", "err", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	start := time.Now()
	dead := 0

	// 1. 解析 payload，分离坏消息与可发送消息。
	msgs := make([]kafka.BatchMessage, 0, len(rows))
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		var e event.OrderCreated
		if err := json.Unmarshal([]byte(row.Payload), &e); err != nil {
			isDead, derr := r.store.MarkOutboxFailed(ctx, row.ID, err.Error(), r.cfg.MaxAttempts)
			if derr != nil {
				RecordRelayError("mark_failed")
				r.log.Error("mark outbox failed", "event_id", row.EventID, "err", derr)
				continue
			}
			if isDead {
				dead++
				RecordRelayDead()
				r.log.Error("outbox event dead", "event_id", row.EventID, "attempts", row.Attempts+1, "err", err)
			}
			RecordRelayFailed()
			continue
		}
		msgs = append(msgs, kafka.BatchMessage{
			Key:       e.EventID,
			Value:     []byte(row.Payload),
			RequestID: e.RequestID,
		})
		ids = append(ids, row.ID)
	}
	if len(msgs) == 0 {
		return
	}

	// 2. 批量发送：一次 WriteMessages，batch 满载立即刷盘。
	if err := r.prod.ProduceBatch(ctx, msgs); err != nil {
		// Kafka 不可用/确认失败：整批保持 PENDING，下一轮重试（部分已写入会形成
		// 重复投递，由 Inbox 去重）。不逐条 MarkOutboxFailed：Kafka 抖动不消耗 attempts。
		RecordRelayBatchFailed()
		r.log.Warn("produce batch failed, keeping pending", "size", len(msgs), "err", err)
		ObserveRelayBatch(time.Since(start), len(rows), 0, len(msgs), dead)
		return
	}

	// 3. 确认后批量标记 SENT（单事务 1 次 fsync，主键定位）。
	if err := r.store.MarkOutboxSentBatch(ctx, ids); err != nil {
		// 标记失败：已发送但未标记，合法未知状态，下一轮重发，Inbox 去重兜底。
		RecordRelayError("mark_sent")
		r.log.Error("batch mark outbox sent", "size", len(ids), "err", err)
		ObserveRelayBatch(time.Since(start), len(rows), 0, len(msgs), dead)
		return
	}
	RecordRelaySentBatch(len(ids))
	ObserveRelayBatch(time.Since(start), len(rows), len(ids), 0, dead)
}

// Stop 请求停止轮询并等待本轮退出。
func (r *Relay) Stop() {
	close(r.stopCh)
	<-r.doneCh
}
