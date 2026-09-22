// 非关键依赖客户端：资源隔离（有界连接/并发 + 独立超时）＋ 熔断 ＋ 业务降级。
//
// 隔离语义（EXP-12）：
//   - 并发上限（信号量）＋连接上限（MaxConnsPerHost）双层有界：
//     慢依赖最多占用 PoolSize 个并发槽位与连接，不会耗尽进程 goroutine/内存/连接预算；
//   - 有界等待：拿不到槽位时最多等 AcquireTimeout，超时直接降级（不排队堆积）；
//   - 独立超时：单次依赖调用受 Timeout 约束（远小于写路径 1s 总预算，EXP-11 子预算）；
//   - 熔断：连续失败 FailThreshold 次打开，OpenDuration 冷却后半开探测（并发 1），
//     恢复后关闭。熔断打开时请求直接降级，不触碰下游。
//
// 降级语义：依赖不可用/超时/熔断 → 返回 (nil, Degraded)，
// 调用方省略附加信息但仍返回 200（X-Dep: degraded）；核心数据校验不在此路径。
//
// 对照实验：Isolation=false 时退化为「无隔离」客户端（无限并发、独立超时不生效、
// 无熔断），用于场景 C1 展示慢依赖对共享资源的消耗。
package resilience

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Degraded 依赖降级哨兵错误：附加信息不可得，调用方应省略并标记 degraded。
var Degraded = errors.New("dependency degraded")

var (
	depRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dep_requests_total",
			Help: "非关键依赖调用结果分类（ok/degraded/timeout/error/circuit_open）。",
		},
		[]string{"dep", "result"},
	)
	depInflight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dep_inflight",
			Help: "非关键依赖当前在途调用数（受隔离池约束）。",
		},
		[]string{"dep"},
	)
	depLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dep_latency_seconds",
			Help:    "非关键依赖调用耗时分布（成功调用）。",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2},
		},
		[]string{"dep"},
	)
)

// DepConfig 依赖客户端配置。
type DepConfig struct {
	Name         string        // 指标 label（如 "product-extra"）
	BaseURL      string        // 依赖服务地址
	Timeout      time.Duration // 单次调用独立超时
	PoolSize     int           // 并发/连接上限（0 = 无隔离，对照实验）
	AcquireTO    time.Duration // 等待槽位的最长时间（有界等待）
	Isolation    bool          // 是否启用隔离+熔断（对照实验开关）
	FailThreshold int          // 熔断连续失败阈值
	OpenDuration time.Duration // 熔断冷却时长
}

// Client 非关键依赖客户端。
type Client struct {
	name     string
	baseURL  string
	httpc    *http.Client
	sem      chan struct{}
	acquireT time.Duration
	cb       *CircuitBreaker
	isolation bool
	inflight atomic.Int64
}

// NewClient 构造依赖客户端。Isolation=false 时 sem/cb 不生效（无隔离对照）。
func NewClient(cfg DepConfig) *Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{Timeout: 1 * time.Second}).DialContext,
	}
	if cfg.PoolSize > 0 {
		transport.MaxConnsPerHost = cfg.PoolSize
		transport.MaxIdleConnsPerHost = cfg.PoolSize
	}
	c := &Client{
		name:      cfg.Name,
		baseURL:   cfg.BaseURL,
		httpc:     &http.Client{Transport: transport},
		acquireT:  cfg.AcquireTO,
		isolation: cfg.Isolation,
	}
	if cfg.Timeout > 0 {
		c.httpc.Timeout = cfg.Timeout
	}
	if cfg.Isolation && cfg.PoolSize > 0 {
		c.sem = make(chan struct{}, cfg.PoolSize)
		c.cb = NewCircuitBreaker(Config{
			Name:          cfg.Name,
			FailThreshold: cfg.FailThreshold,
			OpenDuration:  cfg.OpenDuration,
		})
	}
	return c
}

// GetExtra 获取 SKU 附加信息；依赖不可用时返回 Degraded（调用方省略字段并标记）。
func (c *Client) GetExtra(ctx context.Context, sku string) (map[string]any, error) {
	if c.sem != nil {
		// 有界等待槽位：拿不到就降级，不在进程内排队堆积（排队 = 无限 goroutine）。
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
		case <-time.After(c.acquireT):
			c.record("acquire_timeout")
			return nil, Degraded
		case <-ctx.Done():
			c.record("acquire_timeout")
			return nil, Degraded
		}
	}
	if c.cb != nil && !c.cb.Allow() {
		c.record("circuit_open")
		return nil, Degraded
	}
	c.inflight.Add(1)
	depInflight.WithLabelValues(c.name).Set(float64(c.inflight.Load()))
	defer func() {
		c.inflight.Add(-1)
		depInflight.WithLabelValues(c.name).Set(float64(c.inflight.Load()))
	}()

	start := time.Now()
	// 依赖调用继承请求 context：请求总预算到期（EXP-11）时自动取消，
	// 不再出现「响应白写」的无用工作（C1 对照中 6000 次 2s 调用在客户端
	// 已断连后仍继续执行，正是缺了这一行）。
	req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/extra/"+sku, nil)
	if rerr != nil {
		c.record("error")
		if c.cb != nil {
			c.cb.ReportResult(false)
		}
		return nil, Degraded
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeoutErr(err) {
			c.record("timeout")
		} else {
			c.record("error")
		}
		if c.cb != nil {
			c.cb.ReportResult(false)
		}
		return nil, Degraded
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.record("error")
		if c.cb != nil {
			c.cb.ReportResult(false)
		}
		return nil, Degraded
	}
	var out struct {
		Extra map[string]any `json:"extra"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.record("error")
		if c.cb != nil {
			c.cb.ReportResult(false)
		}
		return nil, Degraded
	}
	c.record("ok")
	depLatency.WithLabelValues(c.name).Observe(time.Since(start).Seconds())
	if c.cb != nil {
		c.cb.ReportResult(true)
	}
	return out.Extra, nil
}

// State 熔断器状态（观测用；无熔断时返回 closed）。
func (c *Client) State() cbState {
	if c.cb == nil {
		return StateClosed
	}
	return c.cb.State()
}

func (c *Client) record(result string) {
	depRequestsTotal.WithLabelValues(c.name, result).Inc()
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// String 实现 fmt.Stringer（调试用）。
func (c *Client) String() string { return fmt.Sprintf("dep-client(%s)", c.name) }
