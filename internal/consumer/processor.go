// Package consumer 实现 Kafka → Inbox 去重 → 后置副作用的消费链路（EXP-09 建立，EXP-10 并发化）。
//
// 可靠性语义（ADR-018/020）：
//   - Inbox 去重与后置业务修改在同一 MySQL 事务，业务提交后才提交消费位点；
//   - 重复投递（Relay 重发、消费后位点未提交）由 Inbox 唯一键吸收，副作用唯一；
//   - EXP-10 并发化：N 个 worker 并发处理消息（无业务顺序依赖，Inbox 全局去重），
//     位点按分区 watermark 顺序提交——不越过未完成/失败消息（保序边界保留）；
//   - 在途消息有界（MaxInflight）：下游（DB）变慢时 worker 阻塞 → 队列满 → 暂停 fetch，
//     消费压力随下游自动收缩（反压）；
//   - 处理失败（DB 不可用）不提交位点，循环重试；毒丸消息（解析失败）跳过并提交。
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/mq/kafka"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

// Config 消费者运行参数。
type Config struct {
	// Concurrency 并发 worker 数；每个 worker 一个独立处理循环与 DB 事务。
	Concurrency int
	// MaxInflight 在途消息上限（含处理中与待处理）：有界队列即反压。
	MaxInflight int
	// BatchSize worker 攒批大小：一批事件一个事务（1 次 fsync），
	// 突破逐条事务的 fsync 串行墙（EXP-10 实测逐条 ~250/s 天花板）。
	BatchSize int
	// BatchWait 攒批等待上限：队列不满时最多等这么久即处理当前批。
	BatchWait time.Duration
	// RetryBackoff 处理失败（DB 不可用等）后重试间隔。
	RetryBackoff time.Duration
}

// Processor 并发消费处理器：reader（单）→ 有界队列 → N workers → 顺序位点提交。
type Processor struct {
	store *mysql.Store
	cons  *kafka.Consumer
	log   *slog.Logger
	cfg   Config
}

func New(store *mysql.Store, cons *kafka.Consumer, log *slog.Logger, cfg Config) *Processor {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = cfg.Concurrency * 2
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.BatchWait <= 0 {
		cfg.BatchWait = 10 * time.Millisecond
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}
	return &Processor{store: store, cons: cons, log: log, cfg: cfg}
}

// Run 启动 reader + workers + committer，直至 ctx 取消。
func (p *Processor) Run(ctx context.Context) {
	workCh := make(chan kafkago.Message, p.cfg.MaxInflight)
	doneCh := make(chan kafkago.Message, p.cfg.MaxInflight)

	var wg sync.WaitGroup
	// reader：单 goroutine fetch，队列满时自然阻塞（反压）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(workCh)
		for {
			msg, err := p.cons.Fetch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				RecordProcessError("fetch")
				p.log.Error("fetch message", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(p.cfg.RetryBackoff):
				}
				continue
			}
			select {
			case workCh <- msg:
				AddInflight(1)
			case <-ctx.Done():
				return
			}
		}
	}()

	// workers：攒批并发处理（一批一个事务），完成后逐条发 doneCh。
	for i := 0; i < p.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			batch := make([]kafkago.Message, 0, p.cfg.BatchSize)
			timer := time.NewTimer(p.cfg.BatchWait)
			defer timer.Stop()
			flush := func() {
				if len(batch) == 0 {
					return
				}
				p.processBatch(ctx, workerID, batch)
				for _, m := range batch {
					AddInflight(-1)
					select {
					case doneCh <- m:
					case <-ctx.Done():
						return
					}
				}
				batch = batch[:0]
			}
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-workCh:
					if !ok {
						flush() // 队列关闭，处理残余
						return
					}
					batch = append(batch, msg)
					if len(batch) >= p.cfg.BatchSize {
						flush()
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						timer.Reset(p.cfg.BatchWait)
					}
				case <-timer.C:
					flush()
					timer.Reset(p.cfg.BatchWait)
				}
			}
		}(i)
	}

	// committer：按分区 watermark 顺序推进水位，合并提交位点。
	// 3 分区交错完成时连续段常仅 1-2 条，逐段提交的 Kafka 往返成为串行点
	//（EXP-10 实测提交 3310 次/4616 条、每条 74ms）；改为 100ms 攒批 flush，
	// 提交延迟 ≤ 100ms（崩溃重读窗口由 Inbox 去重兜底）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		committer := newCommitter(p.cons, p.log)
		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				committer.flush(ctx) // 退出前尽量提交已推进水位
				return
			case msg := <-doneCh:
				committer.markDone(msg)
			case <-flushTicker.C:
				committer.flush(ctx)
			}
		}
	}()

	wg.Wait()
}

// processBatch 处理一批消息：解析 → 批量事务（Inbox 去重 + 副作用）→ 指标。
// 失败（DB 不可用）整批循环重试：批内消息位点均不提交（watermark 拦住）。
// 毒丸消息从批中剔除单独跳过（不阻塞整批）。
func (p *Processor) processBatch(ctx context.Context, workerID int, batch []kafkago.Message) {
	events := make([]event.OrderCreated, 0, len(batch))
	for _, msg := range batch {
		var e event.OrderCreated
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			// 毒丸消息：记录显式失败，位点照常提交（内容错误不影响其他消息；
			// 消息保留在 Kafka，可重放修复）。
			RecordProcessError("parse")
			p.log.Error("parse event, skipping", "partition", msg.Partition, "offset", msg.Offset, "err", err)
			continue
		}
		events = append(events, e)
	}
	if len(events) == 0 {
		return
	}

	start := time.Now()
	for {
		newCount, dupCount, err := p.store.ProcessInboxBatch(ctx, events)
		if err == nil {
			RecordProcessedN("new", newCount)
			RecordProcessedN("dup", dupCount)
			for _, e := range events {
				ObservePostProcessLatency(time.Since(e.CreatedAt))
			}
			p.log.Info("batch processed",
				"worker", workerID, "size", len(events), "new", newCount, "dup", dupCount,
				"duration_ms", time.Since(start).Milliseconds(),
			)
			return
		}
		// 业务事务失败（如 DB 不可用）：整批循环重试。
		RecordProcessError("txn")
		p.log.Error("process batch", "worker", workerID, "size", len(events), "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.RetryBackoff):
		}
	}
}

// committer 按分区维护 watermark：只有「已提交位点之后的连续完成段」才被提交，
// 不越过任何未完成消息。提交段攒批合并 flush（EXP-10：逐段提交的 Kafka 往返
// 是串行点，3 分区交错时平均每段仅 1-2 条）。
type committer struct {
	cons     *kafka.Consumer
	log      *slog.Logger
	mu       sync.Mutex
	state    map[int]*partitionState
	toCommit []kafkago.Message // 已推进水位、等待 flush 的提交段（跨分区合并）
}

type partitionState struct {
	nextToCommit int64                     // 下一个待提交 offset（该 offset 尚未完成）
	done         map[int64]kafkago.Message // 已完成但被未完成 offset 拦住的后续消息
}

func newCommitter(cons *kafka.Consumer, log *slog.Logger) *committer {
	return &committer{cons: cons, log: log, state: make(map[int]*partitionState)}
}

// markDone 记录一条消息完成，推进该分区水位；提交由 flush 攒批执行。
func (c *committer) markDone(msg kafkago.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state[msg.Partition]
	if st == nil {
		st = &partitionState{done: make(map[int64]kafkago.Message)}
		c.state[msg.Partition] = st
	}
	if st.nextToCommit == 0 {
		// 分区首次出现：水位从当前 offset 起（消费组位点由 Kafka 侧管理，
		// 首次提交前的重读窗口由 Inbox 去重兜底）。
		st.nextToCommit = msg.Offset
	}
	if msg.Offset < st.nextToCommit {
		return // 重复完成（如重启前已处理），忽略
	}
	st.done[msg.Offset] = msg

	// 收集从 nextToCommit 起的连续完成段，进入合并提交队列。
	for {
		m, ok := st.done[st.nextToCommit]
		if !ok {
			break
		}
		c.toCommit = append(c.toCommit, m)
		delete(st.done, st.nextToCommit)
		st.nextToCommit++
	}
}

// flush 把攒批的提交段一次性提交（跨分区合并为单次 Kafka 往返）。
func (c *committer) flush(ctx context.Context) {
	c.mu.Lock()
	if len(c.toCommit) == 0 {
		c.mu.Unlock()
		return
	}
	toCommit := c.toCommit
	c.toCommit = nil
	c.mu.Unlock()

	if err := c.cons.Commit(ctx, toCommit...); err != nil {
		RecordProcessError("commit")
		// 提交失败：失败段放回队列头部，下一轮 flush 重试。
		// 最坏情况 Kafka 侧位点落后，重启后从旧位点重读，Inbox 去重兜底。
		c.mu.Lock()
		c.toCommit = append(toCommit, c.toCommit...)
		c.mu.Unlock()
		c.log.Warn("commit watermark", "count", len(toCommit), "err", err)
		return
	}
	c.log.Info("watermark committed", "count", len(toCommit))
}
