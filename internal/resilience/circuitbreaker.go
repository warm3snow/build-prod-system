// Package resilience 提供非关键依赖的熔断与资源隔离（EXP-12）。
//
// 熔断器：三态状态机（closed / open / half-open）。
//   - closed：正常放行，连续失败达到阈值后打开；
//   - open：直接拒绝（快速失败，不消耗下游资源），冷却期后进入 half-open；
//   - half-open：只放行有限探测请求（探测并发 1），探测成功关闭、失败重新打开。
//
// 语义边界（课程口径）：
//   - 熔断只作用于「允许降级的非关键依赖」（商品附加信息）；核心数据校验
//     （库存事务、幂等约束）永远走 MySQL 真实路径，不存在"熔断式假成功"。
//   - 熔断按「失败计数」而非「失败率」：依赖从正常突然转坏时能最快反应；
//     失败率模型需要窗口统计，对本课程的单依赖场景过度设计。
package resilience

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	circuitState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dep_circuit_state",
			Help: "依赖熔断器当前状态（0=closed, 1=open, 2=half-open）。",
		},
		[]string{"dep"},
	)
	circuitTransitions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dep_circuit_transitions_total",
			Help: "熔断器状态迁移次数，按迁移类型（open/half-open/closed）分类。",
		},
		[]string{"dep", "to"},
	)
)

type cbState int32

const (
	StateClosed   cbState = 0
	StateOpen     cbState = 1
	StateHalfOpen cbState = 2
)

// CircuitBreaker 简单三态熔断器。并发安全。
type CircuitBreaker struct {
	name string

	mu         sync.Mutex
	state      cbState
	consecFail int
	openedAt   time.Time

	// 配置
	failThreshold int           // 连续失败阈值
	openDuration  time.Duration // open 冷却时长
	// half-open 探测并发控制：探测窗口内最多 1 个在途请求
	probeInFlight atomic.Int64
}

// Config 熔断器参数。零值取课程默认。
type Config struct {
	Name          string
	FailThreshold int
	OpenDuration  time.Duration
}

// NewCircuitBreaker 构造熔断器。初始 closed。
func NewCircuitBreaker(cfg Config) *CircuitBreaker {
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 5
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 10 * time.Second
	}
	cb := &CircuitBreaker{
		name:          cfg.Name,
		state:         StateClosed,
		failThreshold: cfg.FailThreshold,
		openDuration:  cfg.OpenDuration,
	}
	circuitState.WithLabelValues(cfg.Name).Set(float64(StateClosed))
	return cb
}

// Allow 判断请求是否放行：
//   - closed：放行；
//   - open 且冷却未到：拒绝；
//   - open 且冷却已到：转 half-open，只放行 1 个探测请求；
//   - half-open：最多 1 个在途探测，其余拒绝（限制探测并发，防止恢复洪峰）。
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	switch cb.state {
	case StateClosed:
		cb.mu.Unlock()
		return true
	case StateOpen:
		if time.Since(cb.openedAt) < cb.openDuration {
			cb.mu.Unlock()
			return false
		}
		cb.setStateLocked(StateHalfOpen)
	}
	// half-open：探测并发上限 1
	cb.mu.Unlock()
	return cb.probeInFlight.CompareAndSwap(0, 1)
}

// ReportResult 上报一次请求结果（仅在请求被放行后调用）。
// 连续失败达到阈值 → open（记录打开时间）；成功 → closed。
func (cb *CircuitBreaker) ReportResult(ok bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateHalfOpen {
		// half-open 探测结束，释放探测名额
		cb.probeInFlight.Store(0)
	}
	switch cb.state {
	case StateClosed:
		if !ok {
			cb.consecFail++
			if cb.consecFail >= cb.failThreshold {
				cb.setStateLocked(StateOpen)
				cb.openedAt = time.Now()
				cb.consecFail = 0
			}
		} else {
			cb.consecFail = 0
		}
	case StateHalfOpen:
		if ok {
			cb.consecFail = 0
			cb.setStateLocked(StateClosed)
		} else {
			cb.setStateLocked(StateOpen)
			cb.openedAt = time.Now()
		}
	}
}

// State 当前状态（观测）。
func (cb *CircuitBreaker) State() cbState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) setStateLocked(s cbState) {
	cb.state = s
	circuitState.WithLabelValues(cb.name).Set(float64(s))
	to := "closed"
	switch s {
	case StateOpen:
		to = "open"
	case StateHalfOpen:
		to = "half-open"
	}
	circuitTransitions.WithLabelValues(cb.name, to).Inc()
}
