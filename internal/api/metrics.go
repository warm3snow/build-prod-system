// RED 指标：Rate / Errors / Duration，按 route 与 status 分类。
// 业务指标（订单数、库存拒绝）在 EXP-09 前后随业务演进补充。
package api

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP 请求总数，按 route 与 status 分类。",
		},
		[]string{"route", "method", "status"},
	)
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP 请求耗时分布。",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"route"},
	)
	ordersCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_created_total",
			Help: "成功创建订单总数。",
		},
	)
	ordersRejectedOutOfStock = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_rejected_out_of_stock_total",
			Help: "因库存不足拒绝的订单请求总数。",
		},
	)
	ordersReplayedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_replayed_total",
			Help: "幂等重放返回既有订单的次数。",
		},
	)
	ordersRejectedBacklog = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_rejected_backlog_total",
			Help: "因事件积压超过预算水位被拒绝的下单次数（EXP-10 反压）。",
		},
	)
	// EXP-11 准入控制：限流/在途上限/总时间预算三类拒绝分别计数，
	// 拒绝分类可见是过载实验的验收点（不能全部混在 5xx 里）。
	httpRateLimitedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "http_rate_limited_total",
			Help: "被令牌桶限流拒绝的请求数（429 rate_limited）。",
		},
	)
	httpOverloadedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "http_overloaded_total",
			Help: "因在途请求超过上限被拒绝的请求数（503 overloaded）。",
		},
	)
	httpDeadlineExceededTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "http_deadline_exceeded_total",
			Help: "超过总时间预算被取消的请求数（504 deadline_exceeded）。",
		},
	)
	httpInflight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_inflight",
			Help: "当前在途请求数（受 MAX_INFLIGHT 约束）。",
		},
	)
	// EXP-12：非关键依赖降级计数（降级响应仍为 200，需单独指标可见）。
	depDegradedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dep_degraded_total",
			Help: "商品附加信息降级响应次数（200 + degraded=true）。",
		},
	)
	// EXP-15：坏版本注入计数（BAD_MODE 生效的可观测证据，与发布门禁联动）。
	badReleaseTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "release_bad_mode_total",
			Help: "坏版本注入拦截的下单请求数（BAD_MODE 开关触发）。",
		},
	)
	)

// MetricsMiddleware 记录 RED 指标。放在路由匹配之后，确保 route 已归一化。
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		httpRequestsTotal.WithLabelValues(route, c.Request.Method, strconv.Itoa(c.Writer.Status())).Inc()
		httpRequestDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	}
}

// RecordOrderCreated 成功创建订单（非重放）时调用。
func RecordOrderCreated() { ordersCreatedTotal.Inc() }

// RecordOrderRejected 库存不足拒绝时调用。
func RecordOrderRejected() { ordersRejectedOutOfStock.Inc() }

// RecordOrderReplayed 幂等重放时调用。
func RecordOrderReplayed() { ordersReplayedTotal.Inc() }

// RecordOrderRejectedBacklog 积压反压拒绝时调用。
func RecordOrderRejectedBacklog() { ordersRejectedBacklog.Inc() }

// RecordRateLimited 限流拒绝（429）时调用。
func RecordRateLimited() { httpRateLimitedTotal.Inc() }

// RecordInflightRejected 在途上限拒绝（503 overloaded）时调用。
func RecordInflightRejected() { httpOverloadedTotal.Inc() }

// RecordDeadlineExceeded 总时间预算耗尽（504）时调用。
func RecordDeadlineExceeded() { httpDeadlineExceededTotal.Inc() }

// RecordDepDegraded 非关键依赖降级响应（200 + degraded）时调用。
func RecordDepDegraded() { depDegradedTotal.Inc() }

// RecordBadRelease 坏版本注入拦截时调用（EXP-15 发布门禁证据）。
func RecordBadRelease() { badReleaseTotal.Inc() }
