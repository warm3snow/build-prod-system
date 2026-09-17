// Package cache 提供 Cache Aside 模式的 Redis 查询缓存（EXP-07）。
//
// 一致性边界（与 docs/roadmap.md 第 2 节一致）：
//   - 只缓存可重建的读查询：商品信息与展示库存。扣库存与订单写入仍以 MySQL 事务为准，
//     不依据缓存库存保证下单成功。
//   - 读路径允许有限陈旧：最坏陈旧窗口 = 主 TTL + 失效失败时的兜底窗口。
//   - 写路径：MySQL 事务提交后由 handler 调用 Invalidate 删除缓存键；
//     删除失败时尝试把 TTL 截短到 1s，最终靠主 TTL 兜底，保证陈旧有界。
//   - 不存在对象使用短期负缓存（独立键），避免缓存穿透，也避免与正缓存值解析歧义。
//
// Redis 是可选依赖：不可用时读降级回源 DB、写只记录指标，不阻塞核心路径。
package cache

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

// 键前缀：负缓存使用独立键，与正缓存分离，避免"值缺失"与"负缓存"的解析歧义。
const (
	keyProduct = "cache:product:" // cache:product:{sku} → Product JSON
	keyStock   = "cache:stock:"   // cache:stock:{sku}   → 库存十进制整数
	keyNeg     = "cache:neg:"     // cache:neg:{sku}     → "1"（商品不存在）
)

// HitStatus 缓存查询结果分类。
type HitStatus int

const (
	HitMiss     HitStatus = iota // 未命中，需要回源
	HitPositive                  // 正缓存命中
	HitNegative                  // 负缓存命中（对象确认不存在）
)

// String 输出指标 label 与 X-Cache 响应头使用的状态名。
func (s HitStatus) String() string {
	switch s {
	case HitPositive:
		return "hit"
	case HitNegative:
		return "neg"
	default:
		return "miss"
	}
}

var (
	cacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "缓存命中次数，按操作类型分类。",
		},
		[]string{"op"},
	)
	cacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "缓存未命中（需回源）次数，按操作类型分类。",
		},
		[]string{"op"},
	)
	cacheNegativeHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_negative_hits_total",
			Help: "负缓存命中次数（对象确认不存在），按操作类型分类。",
		},
		[]string{"op"},
	)
	cacheErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_errors_total",
			Help: "Redis 操作失败次数，按操作类型（get/set/del/expire）分类。",
		},
		[]string{"op"},
	)
	cacheInvalidateTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_invalidate_total",
			Help: "缓存失效（写路径删除）次数，按结果分类。",
		},
		[]string{"result"},
	)
)

// Client 缓存客户端。所有 Redis 操作带超时，失败只记录指标并返回错误，由调用方降级。
type Client struct {
	rdb            *redis.Client
	ttl            time.Duration // 正缓存 TTL
	negTTL         time.Duration // 负缓存 TTL
	timeout        time.Duration // 单次 Redis 操作超时
	fillDelay      time.Duration // 实验开关：回源后 SET 前的延迟，用于放大旧值回填竞态窗口
	invalidateDly  time.Duration // 实验开关：下单提交后 DEL 前的延迟，用于放大提交→失效窗口
}

// New 构造缓存客户端。
// fillDelay 与 invalidateDelay 是 EXP-07 陈旧窗口测量的实验开关，生产配置应为 0。
func New(rdb *redis.Client, ttl, negTTL, timeout, fillDelay, invalidateDelay time.Duration) *Client {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if negTTL <= 0 {
		negTTL = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	return &Client{
		rdb: rdb, ttl: ttl, negTTL: negTTL, timeout: timeout,
		fillDelay: fillDelay, invalidateDly: invalidateDelay,
	}
}

// opCtx 返回带超时的上下文，保证单次 Redis 操作不会无限挂起。
func (c *Client) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// GetProduct 读商品缓存：返回商品、命中状态；HitMiss 表示需要回源。
// Redis 错误按未命中处理（降级回源），指标单独计数。
func (c *Client) GetProduct(ctx context.Context, sku string) (mysql.Product, HitStatus) {
	var p mysql.Product
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	v, err := c.rdb.Get(opCtx, keyProduct+sku).Result()
	if err == nil {
		if err := json.Unmarshal([]byte(v), &p); err == nil {
			cacheHitsTotal.WithLabelValues("product").Inc()
			return p, HitPositive
		}
		// 值损坏按未命中处理：回源并覆盖。
	} else if err != redis.Nil {
		cacheErrorsTotal.WithLabelValues("get").Inc()
		return p, HitMiss
	}
	// 未命中或值损坏：检查负缓存。
	neg, err := c.rdb.Exists(opCtx, keyNeg+sku).Result()
	if err == nil && neg > 0 {
		cacheNegativeHitsTotal.WithLabelValues("product").Inc()
		return p, HitNegative
	}
	cacheMissesTotal.WithLabelValues("product").Inc()
	return p, HitMiss
}

// SetProduct 回源成功后写入正缓存（Cache Aside 读路径回填）。
// 写入失败不影响响应，靠下次请求继续回源。
func (c *Client) SetProduct(ctx context.Context, sku string, p mysql.Product) {
	if c.fillDelay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.fillDelay):
		}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return
	}
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	if err := c.rdb.Set(opCtx, keyProduct+sku, b, c.ttl).Err(); err != nil {
		cacheErrorsTotal.WithLabelValues("set").Inc()
	}
}

// SetProductMissing 商品确认不存在时写入短期负缓存，避免缓存穿透。
func (c *Client) SetProductMissing(ctx context.Context, sku string) {
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	if err := c.rdb.Set(opCtx, keyNeg+sku, "1", c.negTTL).Err(); err != nil {
		cacheErrorsTotal.WithLabelValues("set").Inc()
	}
}

// GetStock 读展示库存缓存：返回库存值与命中状态；HitMiss 表示需要回源。
func (c *Client) GetStock(ctx context.Context, sku string) (int, HitStatus) {
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	v, err := c.rdb.Get(opCtx, keyStock+sku).Result()
	if err == nil {
		if n, err := strconv.Atoi(v); err == nil {
			cacheHitsTotal.WithLabelValues("stock").Inc()
			return n, HitPositive
		}
	} else if err != redis.Nil {
		cacheErrorsTotal.WithLabelValues("get").Inc()
		return 0, HitMiss
	}
	cacheMissesTotal.WithLabelValues("stock").Inc()
	return 0, HitMiss
}

// SetStock 回源成功后写入库存缓存。
func (c *Client) SetStock(ctx context.Context, sku string, stock int) {
	if c.fillDelay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.fillDelay):
		}
	}
	opCtx, cancel := c.opCtx(ctx)
	defer cancel()
	if err := c.rdb.Set(opCtx, keyStock+sku, stock, c.ttl).Err(); err != nil {
		cacheErrorsTotal.WithLabelValues("set").Inc()
	}
}

// Invalidate 写路径缓存失效（Cache Aside：先提交 MySQL 事务，再删除缓存）。
// 删除范围：商品正缓存 + 库存正缓存 + 负缓存（商品创建场景下负缓存可能遮蔽新对象）。
// 失效失败恢复策略：
//  1. DEL 失败 → 尝试 EXPIRE key 1s 截短陈旧窗口；
//  2. EXPIRE 也失败 → 主 TTL（默认 60s）兜底，陈旧窗口有界。
func (c *Client) Invalidate(ctx context.Context, sku string) {
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
