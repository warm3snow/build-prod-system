// Package ratelimit 提供令牌桶限流器（EXP-11 准入控制）。
//
// 主线选择令牌桶：允许短突发（burst）的同时限制平均到达率（rate），
// 是 k6 开放模型（constant-arrival-rate）下可对照、可解释的限流策略。
// 固定窗口、漏桶等算法比较为课程选做（roadmap EXP-11）。
//
// 预算口径（EXP-11 冻结）：
//   - 单实例限流：RATE_LIMIT_RPS 是单 Pod 预算；N 副本时集群总预算 = N × 单 Pod 预算
//     （无跨 Pod 协调；需要精确全局配额时应在入口层再限一次，本课程入口为 k8s Service，
//     只声明各层预算关系，不做分布式令牌桶）。
//   - 限流拒绝返回 429（sla.md：保护性拒绝，不计系统错误率，单独统计）。
package ratelimit

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tokensAvailable = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ratelimit_tokens_available",
			Help: "令牌桶当前可用令牌数。",
		},
	)
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ratelimit_requests_total",
			Help: "经过限流器的请求数，按结果（allowed/limited）分类。",
		},
		[]string{"result"},
	)
)

// TokenBucket 经典令牌桶：按 rate 速率补充令牌，桶容量 burst。
// 并发安全（互斥锁），下单路径每次请求一次 Allow，开销可忽略。
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // 令牌补充速率（个/秒）
	burst  float64 // 桶容量（允许的突发大小）
	tokens float64 // 当前令牌数
	last   time.Time
}

// New 构造令牌桶。rate <= 0 表示禁用（Allow 永远放行，对照实验开关）。
func New(rate, burst float64) *TokenBucket {
	if burst <= 0 {
		burst = rate // 默认 burst = 1 秒的量
	}
	return &TokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// Allow 尝试取一枚令牌。rate <= 0 时永远放行。
func (b *TokenBucket) Allow() bool {
	if b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		tokensAvailable.Set(b.tokens)
		requestsTotal.WithLabelValues("allowed").Inc()
		return true
	}
	tokensAvailable.Set(b.tokens)
	requestsTotal.WithLabelValues("limited").Inc()
	return false
}

// Rate 返回限流速率（观察用）。
func (b *TokenBucket) Rate() float64 { return b.rate }
