// Package consumer 实现 Kafka → Inbox 去重 → 后置副作用的消费链路（EXP-09）。
//
// 可靠性语义（ADR-018）：
//   - 保序处理：单分区 + 单 goroutine 逐条处理，一条消息事务成功后提交该条位点，
//     不越过尚未完成的消息；
//   - Inbox 去重与后置业务修改在同一 MySQL 事务，业务提交后才提交消费位点；
//   - 重复投递（Relay 重发、消费后位点未提交）由 Inbox 唯一键吸收，副作用唯一；
//   - 处理失败（如 DB 不可用）不提交位点，等待重试，不静默跳过。
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/mq/kafka"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

// Config 消费者运行参数。
type Config struct {
	// RetryBackoff 处理失败（DB 不可用等）后重试间隔；失败消息不提交位点，保序等待。
	RetryBackoff time.Duration
}

// Processor 单线程消费处理器（保序主线）。
type Processor struct {
	store *mysql.Store
	cons  *kafka.Consumer
	log   *slog.Logger
	cfg   Config
}

func New(store *mysql.Store, cons *kafka.Consumer, log *slog.Logger, cfg Config) *Processor {
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}
	return &Processor{store: store, cons: cons, log: log, cfg: cfg}
}

// Run 消费循环：Fetch → Process → Commit，直至 ctx 取消（优雅退出）。
func (p *Processor) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := p.cons.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // 退出信号
			}
			RecordProcessError("fetch")
			p.log.Error("fetch message", "err", err)
			time.Sleep(p.cfg.RetryBackoff)
			continue
		}
		p.handle(ctx, msg)
	}
}

// handle 处理单条消息：业务事务成功（含重复识别）后提交位点。
func (p *Processor) handle(ctx context.Context, msg kafkago.Message) {
	var e event.OrderCreated
	if err := json.Unmarshal(msg.Value, &e); err != nil {
		// 无法解析的消息：记录显式失败日志与指标，提交位点防止永久阻塞
		// （内容错误不影响其他消息；消息保留在 Kafka，可重放修复）。
		RecordProcessError("parse")
		p.log.Error("parse event, skipping", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		if err := p.cons.Commit(ctx, msg); err != nil {
			RecordProcessError("commit")
			p.log.Error("commit after parse error", "err", err)
		}
		return
	}

	start := time.Now()
	var dup bool
	for {
		d, err := p.store.ProcessInboxEvent(ctx, e)
		if err == nil {
			dup = d
			if dup {
				RecordProcessed("dup")
			} else {
				RecordProcessed("new")
				ObservePostProcessLatency(time.Since(e.CreatedAt))
			}
			break
		}
		// 业务事务失败（如 DB 不可用）：不提交位点，循环重试同一消息（保序：不越过失败消息）。
		RecordProcessError("txn")
		p.log.Error("process event", "event_id", e.EventID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.RetryBackoff):
		}
	}

	// 业务事务已提交，此刻才提交消费位点。
	if err := p.cons.Commit(ctx, msg); err != nil {
		RecordProcessError("commit")
		// 位点未提交：重启后从旧位点重读 → Inbox 去重兜底，副作用不重复。
		p.log.Warn("commit offset", "event_id", e.EventID, "err", err)
		return
	}
	p.log.Info("event processed",
		"event_id", e.EventID, "order_id", e.OrderID, "dup", dup,
		"request_id", e.RequestID, "trace_parent", e.TraceParent,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}
