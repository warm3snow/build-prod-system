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
