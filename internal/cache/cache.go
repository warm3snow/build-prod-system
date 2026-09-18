// Package cache 提供 Cache Aside 模式的 Redis 查询缓存（EXP-07/08）。
//
// EXP-07 一致性边界（不变）：
//   - 只缓存可重建的读查询：商品信息与展示库存。扣库存与订单写入仍以 MySQL 事务为准，
//     不依据缓存库存保证下单成功。
//   - 读路径允许有限陈旧：最坏陈旧窗口 = 主 TTL + 失效失败时的兜底窗口。
//   - 写路径：MySQL 事务提交后由 handler 调用 Invalidate 删除缓存键；
//     删除失败时尝试把 TTL 截短到 1s，最终靠主 TTL 兜底，保证陈旧有界。
//   - 不存在对象使用短期负缓存（独立键），避免缓存穿透。
//
// EXP-08 有界回源（缓存失效保护，本实验新增）：
//   - 热点 Key 过期时，进程内请求合并（singleflight）把同一 Key 的并发 miss
//     合并为一次 DB 回源；合并范围仅限单进程，多副本下每个副本各回源一次。
//   - 回源具备独立超时（BackfillTimeout）、并发上限（信号量）与有界等待
//     （BackfillAcquireTimeout）；超限时对允许陈旧的读查询返回本地旧值（stale），
//     无旧值可用时明确拒绝（503 + backfill_overloaded/timeout/error 分类）。
//   - 本地旧值库（有界内存 map）从 Redis 命中与回源成功中刷新；
//     Redis 故障时优先直接返回旧值，不把缓存流量转嫁给数据库。
//
// Redis 是可选依赖：不可用时读降级（旧值 > 有界回源），写只记录指标，不阻塞核心路径。
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

// 键前缀：负缓存使用独立键，与正缓存分离，避免"值缺失"与"负缓存"的解析歧义。
const (
	keyProduct = "cache:product:" // cache:product:{sku} → Product JSON
	keyStock   = "cache:stock:"   // cache:stock:{sku}   → 库存十进制整数
	keyNeg     = "cache:neg:"     // cache:neg:{sku}     → "1"（商品不存在）
)

// ReadStatus 一次读请求的最终分类，同时用于 X-Cache 响应头与指标 label。
type ReadStatus int

const (
	StatusHit      ReadStatus = iota // 正缓存命中
	StatusNeg                         // 负缓存命中（对象确认不存在）
	StatusFresh                       // 未命中，回源成功（本次打了 DB）
	StatusNegFresh                    // 未命中，回源确认对象不存在（本次打了 DB，与负缓存命中区分）
	StatusStale                       // 回源受限/Redis 故障，返回本地旧值（仅允许陈旧的查询）
	StatusRejected                    // 回源受限且无旧值，调用方应明确拒绝（503）

	// statusMiss 仅作为 readCache 的内部信号（缓存未命中，需回源），不返回给调用方。
	statusMiss
)

// String 输出 X-Cache 响应头与指标 label 使用的状态名。
// StatusFresh 沿用 EXP-07 的 "miss" 语义（本次确实回源了）。
func (s ReadStatus) String() string {
	switch s {
	case StatusHit:
		return "hit"
	case StatusNeg:
		return "neg"
	case StatusStale:
		return "stale"
	case StatusRejected:
		return "reject"
	default:
		return "miss"
	}
}

// RejectCode 解释 StatusRejected 的原因，写入 503 响应体 code 字段。
type RejectCode string

const (
	RejectOverload RejectCode = "backfill_overloaded" // 回源额度已满，有界等待超时
	RejectTimeout  RejectCode = "backfill_timeout"    // 回源执行超过独立超时
	RejectError    RejectCode = "backfill_error"      // DB 回源出错（Redis 故障期间无旧值可用）
)

var (
	cacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_hits_total", Help: "缓存命中次数，按操作类型分类。"},
		[]string{"op"},
	)
	cacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_misses_total", Help: "缓存未命中（需回源）次数，按操作类型分类。"},
		[]string{"op"},
	)
	cacheNegativeHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_negative_hits_total", Help: "负缓存命中次数，按操作类型分类。"},
		[]string{"op"},
	)
	cacheErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_errors_total", Help: "Redis 操作失败次数，按操作类型（get/set/del/expire）分类。"},
		[]string{"op"},
	)
	cacheInvalidateTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_invalidate_total", Help: "缓存失效（写路径删除）次数，按结果分类。"},
		[]string{"result"},
	)

	// EXP-08 有界回源指标。
	// wanted 与 executed 之差即请求合并（singleflight）吸收的重复回源数。
	cacheBackfillWantedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_backfill_wanted_total", Help: "需要回源的请求数（进入回源路径的 miss），按操作类型分类。"},
		[]string{"op"},
	)
	cacheBackfillExecutedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_backfill_executed_total", Help: "实际执行的 DB 回源次数，按操作类型分类。与 wanted 的差值即被合并的请求数。"},
		[]string{"op"},
	)
	cacheBackfillOutcomeTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_backfill_outcome_total", Help: "回源结果分类计数（fresh/neg/stale/rejected）。"},
		[]string{"op", "result"},
	)
	cacheBackfillInflight = promauto.NewGauge(
		prometheus.GaugeOpts{Name: "cache_backfill_inflight", Help: "当前占用回源额度的在途回源数。"},
	)
	cacheStaleHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "cache_stale_hits_total", Help: "本地旧值库命中次数（降级服务），按操作类型分类。"},
		[]string{"op"},
	)
	cacheStaleEntries = promauto.NewGauge(
		prometheus.GaugeOpts{Name: "cache_stale_entries", Help: "本地旧值库当前条目数。"},
	)
	// EXP-08：失效队列（异步失效）。队列满丢弃后陈旧由主 TTL 兜底。
	cacheInvalidateQueueLen = promauto.NewGauge(
		prometheus.GaugeOpts{Name: "cache_invalidate_queue_len", Help: "异步失效队列当前积压数。"},
	)
)

// Config 缓存客户端配置。EXP-08 起改为结构体参数，便于后续实验继续扩展。
type Config struct {
	RDB    *redis.Client
	TTL    time.Duration // 正缓存 TTL
	NegTTL time.Duration // 负缓存 TTL

	OpTimeout time.Duration // 单次 Redis 操作超时

	// EXP-07 陈旧窗口测量的实验开关，生产配置必须为 0。
	FillDelay       time.Duration // 回源后 SET 前延迟，放大"旧值回填"竞态窗口
	InvalidateDelay time.Duration // 下单提交后 DEL 前延迟，放大"提交→失效"窗口

	// EXP-08 有界回源。
	CoalesceEnabled      bool          // 进程内请求合并（singleflight）开关，供对照实验
	BackfillMaxConcur    int           // 回源并发上限（必须在 DB 连接预算内）
	BackfillAcquireTO    time.Duration // 等待回源额度的最长时间（有界等待）
	BackfillTimeout      time.Duration // 单次回源的独立超时
	StaleMaxEntries      int           // 本地旧值库最大条目数
	StaleTTL             time.Duration // 本地旧值可服务时长
	InvalidateQueueSize  int           // 异步失效队列容量；满则丢弃（陈旧由 TTL 兜底）
}

// Client 缓存客户端。所有 Redis 操作带超时，失败只记录指标并返回错误，由调用方降级。
type Client struct {
	rdb     *redis.Client
	ttl     time.Duration
	negTTL  time.Duration
	timeout time.Duration

	fillDelay     time.Duration
	invalidateDly time.Duration

	coalesce    bool
	backfillSem chan struct{} // 回源并发信号量：容量 = BackfillMaxConcur
	acquireTO   time.Duration
	backfillTO  time.Duration
	sf          singleflight.Group // 进程内请求合并；范围仅限单进程
	stale       *staleStore

	invQueue chan string // 异步失效队列（EXP-08）：下单提交后投递，worker 消费
}

// New 构造缓存客户端。零值字段取课程默认（与 deploy/k8s 中 env 默认一致）。
func New(cfg Config) *Client {
	ttl, negTTL, timeout := cfg.TTL, cfg.NegTTL, cfg.OpTimeout
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if negTTL <= 0 {
		negTTL = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	if cfg.BackfillMaxConcur <= 0 {
		cfg.BackfillMaxConcur = 12
	}
	if cfg.BackfillAcquireTO <= 0 {
		cfg.BackfillAcquireTO = 200 * time.Millisecond
	}
	if cfg.BackfillTimeout <= 0 {
		cfg.BackfillTimeout = time.Second
	}
	if cfg.StaleMaxEntries <= 0 {
		cfg.StaleMaxEntries = 1024
	}
	if cfg.StaleTTL <= 0 {
		cfg.StaleTTL = 60 * time.Second
	}
	if cfg.InvalidateQueueSize <= 0 {
		cfg.InvalidateQueueSize = 1024
	}
	c := &Client{
		rdb: cfg.RDB, ttl: ttl, negTTL: negTTL, timeout: timeout,
		fillDelay: cfg.FillDelay, invalidateDly: cfg.InvalidateDelay,
		coalesce: cfg.CoalesceEnabled, backfillSem: make(chan struct{}, cfg.BackfillMaxConcur),
		acquireTO: cfg.BackfillAcquireTO, backfillTO: cfg.BackfillTimeout,
		stale: newStaleStore(cfg.StaleMaxEntries, cfg.StaleTTL),
	}
	if cfg.RDB != nil {
		c.invQueue = make(chan string, cfg.InvalidateQueueSize)
		go c.invalidateWorker()
	}
	return c
}

// Close 优雅关闭：停掉失效 worker。失效队列内剩余条目不再处理（陈旧由 TTL 兜底）。
func (c *Client) Close() {
	if c.invQueue != nil {
		close(c.invQueue)
	}
}

// opCtx 返回带超时的上下文，保证单次 Redis 操作不会无限挂起。
func (c *Client) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// readCache 读 Redis 并完成负缓存检查，返回（状态，值）：
//   - StatusHit：val 为原始字符串（JSON 或十进制整数），由调用方解析；
//   - StatusStale：Redis 故障且本地旧值可用，val 为旧值对象（any）；
//   - StatusNeg / statusMiss：val 为 nil。
//
// Redis 故障（GET 报错）时：优先返回本地旧值（stale），不把缓存流量转嫁给 DB；
// 无旧值才继续走有界回源路径（statusMiss）。
func (c *Client) readCache(ctx context.Context, key, negKey, op string) (ReadStatus, any) {
	opCtx, cancel := c.opCtx(ctx)
	v, err := c.rdb.Get(opCtx, key).Result()
	cancel()
	if err == nil {
		cacheHitsTotal.WithLabelValues(op).Inc()
		return StatusHit, v
	}
	if err != redis.Nil {
		cacheErrorsTotal.WithLabelValues("get").Inc()
		if s, ok := c.stale.get(key); ok {
			cacheStaleHitsTotal.WithLabelValues(op).Inc()
			return StatusStale, s
		}
		return statusMiss, nil
	}
	// 未命中：检查负缓存。
	opCtx, cancel = c.opCtx(ctx)
	neg, err := c.rdb.Exists(opCtx, negKey).Result()
	cancel()
	if err == nil && neg > 0 {
		cacheNegativeHitsTotal.WithLabelValues(op).Inc()
		return StatusNeg, nil
	}
	cacheMissesTotal.WithLabelValues(op).Inc()
	return statusMiss, nil
}

// GetOrLoadProduct 读商品：缓存命中/负命中/旧值降级直接返回；
// miss 时在请求合并 + 并发上限 + 独立超时保护下回源 DB。
// load 由调用方提供（handler 闭包 store.GetProduct），便于保持缓存层与存储层解耦。
func (c *Client) GetOrLoadProduct(ctx context.Context, sku string, load func(context.Context) (mysql.Product, error)) (mysql.Product, ReadStatus, RejectCode) {
	key := keyProduct + sku
	st, val := c.readCache(ctx, key, keyNeg+sku, "product")
	switch st {
	case StatusHit:
		var p mysql.Product
		if json.Unmarshal([]byte(val.(string)), &p) == nil {
			cacheStaleEntries.Set(float64(c.stale.put(key, p)))
			return p, StatusHit, ""
		}
		// 值损坏：按未命中处理，回源并覆盖。
	case StatusStale:
		return val.(mysql.Product), StatusStale, ""
	case StatusNeg:
		return mysql.Product{}, StatusNeg, ""
	}
	out := c.doBackfill(ctx, key, keyNeg+sku, "product", func(bctx context.Context) (any, bool, error) {
		p, err := load(bctx)
		if err != nil {
			if errors.Is(err, mysql.ErrNotFound) {
				return nil, false, nil // DB 确认不存在 → 负缓存
			}
			return nil, true, err
		}
		return p, true, nil
	})
	p, _ := out.val.(mysql.Product)
	return p, out.st, out.code
}

// GetOrLoadStock 读展示库存，语义与 GetOrLoadProduct 一致。
func (c *Client) GetOrLoadStock(ctx context.Context, sku string, load func(context.Context) (int, error)) (int, ReadStatus, RejectCode) {
	key := keyStock + sku
	st, val := c.readCache(ctx, key, keyNeg+sku, "stock")
	switch st {
	case StatusHit:
		if n, err := strconv.Atoi(val.(string)); err == nil {
			cacheStaleEntries.Set(float64(c.stale.put(key, n)))
			return n, StatusHit, ""
		}
	case StatusStale:
		return val.(int), StatusStale, ""
	case StatusNeg:
		return 0, StatusNeg, ""
	}
	out := c.doBackfill(ctx, key, keyNeg+sku, "stock", func(bctx context.Context) (any, bool, error) {
		n, err := load(bctx)
		if err != nil {
			if errors.Is(err, mysql.ErrNotFound) {
				return nil, false, nil
			}
			return nil, true, err
		}
		return n, true, nil
	})
	n, _ := out.val.(int)
	return n, out.st, out.code
}

// backfillOutcome 回源路径的最终结果。
type backfillOutcome struct {
	val  any
	st   ReadStatus
	code RejectCode
}

// doBackfill 有界回源入口：
//  1. 进程内请求合并（可关闭做对照）：同一 Key 的并发 miss 只执行一次 load；
//  2. 回源额度：并发上限 + 有界等待，超限降级（旧值或拒绝）；
//  3. 独立超时：回源整体受 BackfillTimeout 约束。
func (c *Client) doBackfill(ctx context.Context, key, negKey, op string, load func(context.Context) (any, bool, error)) backfillOutcome {
	cacheBackfillWantedTotal.WithLabelValues(op).Inc()
	if !c.coalesce {
		return c.runBounded(ctx, key, negKey, op, load)
	}
	// 合并的 leader 不随单个请求取消而中止（请求断开不应打断共享回填），
	// 超时仍由 runBounded 内部的独立 deadline 保证。
	ch := c.sf.DoChan(key, func() (any, error) {
		defer c.sf.Forget(key)
		return c.runBounded(context.WithoutCancel(ctx), key, negKey, op, load), nil
	})
	select {
	case r := <-ch:
		return r.Val.(backfillOutcome)
	case <-ctx.Done():
		// 等待合并结果期间请求已断开：不白等，走旧值/拒绝路径。
		return c.staleOrReject(key, op, RejectOverload)
	}
}

// runBounded 实际执行一次有界回源：信号量限流 + 独立超时 + 负缓存/回填。
func (c *Client) runBounded(ctx context.Context, key, negKey, op string, load func(context.Context) (any, bool, error)) backfillOutcome {
	// 1. 有界等待回源额度：额度满则最多等 AcquireTO，超时降级。
	select {
	case c.backfillSem <- struct{}{}:
		defer func() {
			<-c.backfillSem
			cacheBackfillInflight.Set(float64(len(c.backfillSem)))
		}()
		cacheBackfillInflight.Set(float64(len(c.backfillSem)))
	case <-ctx.Done():
		return c.staleOrReject(key, op, RejectOverload)
	case <-time.After(c.acquireTO):
		return c.staleOrReject(key, op, RejectOverload)
	}

	// 2. 独立回源超时：与 Redis 操作超时解耦，DB 慢不会无限占用额度。
	bctx, cancel := context.WithTimeout(ctx, c.backfillTO)
	defer cancel()

	cacheBackfillExecutedTotal.WithLabelValues(op).Inc()
	v, found, err := load(bctx)
	if err != nil {
		code := RejectError
		if errors.Is(err, context.DeadlineExceeded) {
			code = RejectTimeout
		}
		return c.staleOrReject(key, op, code)
	}
	if !found {
		// DB 确认不存在：写短期负缓存防穿透（失败只记指标）。
		// 本次请求确实回源了，状态为 StatusNegFresh（X-Cache: miss），
		// 与后续请求的负缓存命中（StatusNeg，X-Cache: neg）区分。
		opCtx, cancel := c.opCtx(ctx)
		if e := c.rdb.Set(opCtx, negKey, "1", c.negTTL).Err(); e != nil {
			cacheErrorsTotal.WithLabelValues("set").Inc()
		}
		cancel()
		cacheBackfillOutcomeTotal.WithLabelValues(op, "neg").Inc()
		return backfillOutcome{st: StatusNegFresh}
	}

	// 3. 回填 Redis（SET 失败只记指标，不影响响应）+ 刷新本地旧值库。
	if c.fillDelay > 0 {
		select {
		case <-bctx.Done():
		case <-time.After(c.fillDelay):
		}
	}
	c.fillBack(ctx, key, v)
	cacheBackfillOutcomeTotal.WithLabelValues(op, "fresh").Inc()
	return backfillOutcome{val: v, st: StatusFresh}
}

// staleOrReject 回源受限时的降级路径：有本地旧值返回 stale，否则明确拒绝。
func (c *Client) staleOrReject(key, op string, code RejectCode) backfillOutcome {
	if v, ok := c.stale.get(key); ok {
		cacheStaleHitsTotal.WithLabelValues(op).Inc()
		cacheBackfillOutcomeTotal.WithLabelValues(op, "stale").Inc()
		return backfillOutcome{val: v, st: StatusStale}
	}
	cacheBackfillOutcomeTotal.WithLabelValues(op, "rejected").Inc()
	return backfillOutcome{st: StatusRejected, code: code}
}

// fillBack 回源成功后写入正缓存（Cache Aside 读路径回填）。
func (c *Client) fillBack(ctx context.Context, key string, v any) {
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	switch val := v.(type) {
	case mysql.Product:
		b, err := json.Marshal(val)
		if err != nil {
			return
		}
		if err := c.rdb.Set(opCtx, key, b, c.ttl).Err(); err != nil {
			cacheErrorsTotal.WithLabelValues("set").Inc()
		}
	case int:
		if err := c.rdb.Set(opCtx, key, strconv.Itoa(val), c.ttl).Err(); err != nil {
			cacheErrorsTotal.WithLabelValues("set").Inc()
		}
	}
	cacheStaleEntries.Set(float64(c.stale.put(key, v)))
}

// Invalidate 写路径缓存失效（Cache Aside：先提交 MySQL 事务，再删除缓存）。
//
// EXP-08 起为异步投递：下单事务提交后不再同步等待 Redis，避免 Redis 故障时
// 把超时（100ms×2）叠加到下单延迟上（场景 B 实测：200 TPS 下单叠加失效超时后
// 拉长 P1 行锁持有 → DB 连接池耗尽 → 全链路失败率 21.9%）。
// 队列有界；满则丢弃并计 dropped，陈旧由主 TTL 兜底（与 EXP-07 兜底策略一致）。
func (c *Client) Invalidate(ctx context.Context, sku string) {
	if c.invQueue == nil {
		c.delKeys(ctx, sku) // 兜底：无队列（如缓存禁用前的调用）时同步执行
		return
	}
	select {
	case c.invQueue <- sku:
		cacheInvalidateQueueLen.Set(float64(len(c.invQueue)))
	default:
		cacheInvalidateTotal.WithLabelValues("dropped").Inc()
	}
}

// invalidateWorker 消费失效队列执行删除。单 worker 保持失效顺序且不放大并发。
func (c *Client) invalidateWorker() {
	for sku := range c.invQueue {
		cacheInvalidateQueueLen.Set(float64(len(c.invQueue)))
		c.delKeys(context.Background(), sku)
	}
}

// delKeys 执行实际删除：商品正缓存 + 库存正缓存 + 负缓存。
// 失败恢复策略：DEL 失败 → EXPIRE 1s 截短；EXPIRE 也失败 → 主 TTL 兜底。
func (c *Client) delKeys(ctx context.Context, sku string) {
	if c.invalidateDly > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.invalidateDly):
		}
	}
	keys := []string{keyProduct + sku, keyStock + sku, keyNeg + sku}
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	n, err := c.rdb.Del(opCtx, keys...).Result()
	if err != nil {
		cacheErrorsTotal.WithLabelValues("del").Inc()
		// DEL 失败：对每个键尝试 EXPIRE 1s，把陈旧窗口截短到兜底 TTL。
		ok := true
		for _, k := range keys {
			if e := c.rdb.Expire(opCtx, k, time.Second).Err(); e != nil {
				cacheErrorsTotal.WithLabelValues("expire").Inc()
				ok = false
			}
		}
		if !ok {
			cacheInvalidateTotal.WithLabelValues("error").Inc()
			return
		}
		cacheInvalidateTotal.WithLabelValues("degraded").Inc()
		return
	}
	_ = n
	cacheInvalidateTotal.WithLabelValues("ok").Inc()
}

// staleStore 本地旧值库：仅在回源受限/Redis 故障时服务，让降级请求不碰 DB。
// 有界：条目数上限 + 每条目 TTL；刷新节流：剩余寿命 > TTL/2 时跳过写入，减少锁竞争。
// 滑动续期（EXP-08 场景 B）：命中时把过期时间顺延一个 TTL——故障期间被持续读取的
// Key 不会"同步过期"后一起挤向 DB，避免故障期二次拒绝风暴（旧值陈旧度仍以
// 上一次成功回源/命中为起点，上界 = 2×TTL，对展示类查询可接受）。
type staleStore struct {
	mu      sync.Mutex
	entries map[string]staleEntry
	max     int
	ttl     time.Duration
}

type staleEntry struct {
	val any
	exp time.Time
}

func newStaleStore(max int, ttl time.Duration) *staleStore {
	return &staleStore{entries: make(map[string]staleEntry), max: max, ttl: ttl}
}

// get 读取未过期旧值；命中时滑动续期一个 TTL，过期条目顺带清理。
// 返回 (值, 是否命中)。
func (s *staleStore) get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || time.Now().After(e.exp) {
		delete(s.entries, key)
		return nil, false
	}
	e.exp = time.Now().Add(s.ttl) // 滑动续期
	s.entries[key] = e
	return e.val, true
}

// put 写入/刷新旧值，返回当前条目数。容量满时驱逐任意一个条目（map 遍历序随机）。
func (s *staleStore) put(key string, v any) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && time.Until(e.exp) > s.ttl/2 {
		return len(s.entries) // 刷新节流：剩余寿命充足，跳过写锁
	}
	if len(s.entries) >= s.max {
		for k := range s.entries {
			delete(s.entries, k)
			break
		}
	}
	s.entries[key] = staleEntry{val: v, exp: time.Now().Add(s.ttl)}
	return len(s.entries)
}
