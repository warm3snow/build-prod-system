// Package api 提供 order-api 的 HTTP 处理层（Gin 实现）。
// 错误码映射遵守 docs/design/sla.md：
// 库存不足/幂等冲突 → 409（业务拒绝，不计系统错误率）；未知商品 → 404；内部错误 → 500。
package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/warm3snow/build-prod-system/internal/cache"
	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/resilience"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

type Store interface {
	Ping(ctx context.Context) error
	GetProduct(ctx context.Context, sku string) (mysql.Product, error)
	GetStock(ctx context.Context, sku string) (int, error)
	CreateOrder(ctx context.Context, userID, sku, idemKey, paramHash string, trace event.TraceContext) (*mysql.CreatedOrder, error)
	GetLatestOrder(ctx context.Context, userID string) (*mysql.CreatedOrder, error)
	GetOrdersByCursor(ctx context.Context, userID string, beforeCreatedAt *time.Time, beforeID *int64, limit int) ([]mysql.CreatedOrder, bool, error)
	CountPendingOutbox(ctx context.Context) (int64, error)
}

// traceContextKey gin context 键：请求关联上下文（request_id / traceparent）。
const traceContextKey = "exp09.trace"

// Server HTTP 处理层。cch 为 nil 或缓存禁用时走纯 MySQL 路径（EXP-07 无缓存对照组）。
// bp 为 nil 或未启用时不施加积压反压（EXP-10 对照实验）。
// adm 承载 EXP-11 准入控制（限流/在途上限/总时间预算），任一防线关闭即跳过。
// dep 为非关键依赖客户端（EXP-12 商品附加信息）；nil 时 /extra 返回 501。
type Server struct {
	store Store
	cch   *cache.Client
	log   *slog.Logger
	bp    *Backpressure
	adm   *admissionState
	dep   *resilience.Client
}

func NewServer(store Store, cch *cache.Client, log *slog.Logger, bp *Backpressure, adm Admission, dep *resilience.Client) *Server {
	return &Server{store: store, cch: cch, log: log, bp: bp, adm: newAdmissionState(adm), dep: dep}
}

// Routes 返回带日志与指标中间件的 Gin 路由。
// 中间件顺序（由外向内）：恢复 → 指标/日志 → 总时间预算 → 限流 → 在途上限 → 业务。
// 限流与在途拒绝同样落入 RED 指标（status 分类），拒绝分类可观测。
func (s *Server) Routes() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), MetricsMiddleware(), withLogging(s.log))
	r.Use(s.adm.DeadlineMiddleware(), s.adm.RateLimitMiddleware(), s.adm.InFlightMiddleware())
	r.GET("/healthz", s.healthz)
	r.GET("/readyz", s.readyz)
	r.GET("/api/products/:sku", s.getProduct)
	r.GET("/api/products/:sku/stock", s.getStock)
	r.GET("/api/products/:sku/extra", s.getProductExtra)
	r.POST("/api/orders", s.createOrder)
	r.GET("/api/orders", s.listOrders)
	r.GET("/api/orders/latest", s.getLatestOrder)
	return r
}

// healthz 存活探针：只检查进程本身，不依赖下游。
func (s *Server) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz 就绪探针：检查 DB 可达；DB 短暂故障时不摘流由实验 EXP-08/11 再迭代。
func (s *Server) readyz(c *gin.Context) {
	if err := s.store.Ping(c.Request.Context()); err != nil {
		writeErr(c, http.StatusServiceUnavailable, "db_unavailable", "database not ready")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// getProduct Cache Aside 读路径（EXP-08 起带请求合并与有界回源）：
// 命中/负命中直接返回；miss 时在 cache 层的合并+并发+超时保护下回源；
// 回源受限时对允许陈旧的商品查询返回本地旧值（stale），无旧值明确拒绝（503）。
// 响应头 X-Cache 标记本次读取状态（hit/neg/miss/stale/reject/off）。
func (s *Server) getProduct(c *gin.Context) {
	sku := c.Param("sku")
	if s.cch != nil {
		p, st, code := s.cch.GetOrLoadProduct(c.Request.Context(), sku,
			func(ctx context.Context) (mysql.Product, error) { return s.store.GetProduct(ctx, sku) })
		c.Header("X-Cache", st.String())
		switch st {
		case cache.StatusHit, cache.StatusFresh, cache.StatusStale:
			c.JSON(http.StatusOK, p)
		case cache.StatusNeg, cache.StatusNegFresh:
			writeErr(c, http.StatusNotFound, "not_found", "resource not found")
		default: // StatusRejected：回源受限且无旧值，明确拒绝而非伪装成功
			writeErr(c, http.StatusServiceUnavailable, string(code), "cache backfill degraded")
		}
		return
	}
	p, err := s.store.GetProduct(c.Request.Context(), sku)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	c.Header("X-Cache", "off")
	c.JSON(http.StatusOK, p)
}

// getProductExtra 商品附加信息（EXP-12 非关键依赖）：依赖可用时返回附加信息；
// 依赖慢/失败/熔断时降级——省略附加信息但保持 200（X-Dep: degraded）。
// 该路径独立于下单：库存事务与幂等约束不经过依赖，不存在"降级为假成功"。
func (s *Server) getProductExtra(c *gin.Context) {
	sku := c.Param("sku")
	if s.dep == nil {
		writeErr(c, http.StatusNotImplemented, "dep_not_configured", "dependency client not configured")
		return
	}
	extra, err := s.dep.GetExtra(c.Request.Context(), sku)
	if err != nil {
		RecordDepDegraded()
		c.Header("X-Dep", "degraded")
		c.JSON(http.StatusOK, gin.H{"sku": sku, "extra": nil, "degraded": true})
		return
	}
	c.Header("X-Dep", "ok")
	c.JSON(http.StatusOK, gin.H{"sku": sku, "extra": extra, "degraded": false})
}

// getStock 展示库存查询，允许缓存有限陈旧（下单仍以 MySQL 事务为准）。
func (s *Server) getStock(c *gin.Context) {
	sku := c.Param("sku")
	if s.cch != nil {
		stock, st, code := s.cch.GetOrLoadStock(c.Request.Context(), sku,
			func(ctx context.Context) (int, error) { return s.store.GetStock(ctx, sku) })
		c.Header("X-Cache", st.String())
		switch st {
		case cache.StatusHit, cache.StatusFresh, cache.StatusStale:
			c.JSON(http.StatusOK, gin.H{"sku": sku, "stock": stock})
		case cache.StatusNeg, cache.StatusNegFresh:
			writeErr(c, http.StatusNotFound, "not_found", "resource not found")
		default: // StatusRejected
			writeErr(c, http.StatusServiceUnavailable, string(code), "cache backfill degraded")
		}
		return
	}
	stock, err := s.store.GetStock(c.Request.Context(), sku)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	c.Header("X-Cache", "off")
	c.JSON(http.StatusOK, gin.H{"sku": sku, "stock": stock})
}

type createOrderReq struct {
	UserID string `json:"user_id" binding:"required"`
	SKU    string `json:"sku" binding:"required"`
	Qty    int    `json:"qty"`
}

func (s *Server) createOrder(c *gin.Context) {
	key := c.GetHeader("Idempotency-Key")
	if key == "" {
		writeErr(c, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header required")
		return
	}
	var req createOrderReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeErr(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	if req.UserID == "" || req.SKU == "" || req.Qty != 1 {
		writeErr(c, http.StatusBadRequest, "bad_request", "user_id/sku required and qty must be 1")
		return
	}
	paramHash := hashParams(req.UserID, req.SKU, req.Qty)

	// EXP-10 积压反压：事件链路积压超过预算水位时拒绝新下单（503 backlog_limited）。
	// 检查在事务之前、不扣库存；客户端可用原幂等键在水位回落后重试。
	if s.bp != nil && s.bp.OverLimit() {
		RecordOrderRejectedBacklog()
		writeErr(c, http.StatusServiceUnavailable, "backlog_limited",
			"event backlog over budget, order temporarily rejected")
		return
	}

	// 关联上下文：request_id（中间件生成）+ traceparent（W3C，预留 OTel），
	// 随事件贯穿 Outbox → Kafka → Consumer，实现 HTTP/Relay/Consumer 全链路关联。
	trace := traceFromContext(c)
	o, err := s.store.CreateOrder(c.Request.Context(), req.UserID, req.SKU, key, paramHash, trace)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	// Cache Aside：MySQL 事务已提交，此刻才失效缓存；重放订单不改变库存，无需失效。
	// 失效失败由 cache 层兜底（EXPIRE 1s → 主 TTL），不影响下单响应。
	if !o.Replayed && s.cch != nil {
		s.cch.Invalidate(c.Request.Context(), req.SKU)
	}
	status := http.StatusCreated
	if o.Replayed {
		status = http.StatusOK
		RecordOrderReplayed()
	} else {
		RecordOrderCreated()
	}
	c.JSON(status, o)
}

// listOrders 游标分页订单列表（ADR-011）。
// 查询参数：user_id、limit（默认 10，最大 50）、cursor_created_at、cursor_id。
// 响应包含 next_cursor_created_at / next_cursor_id / has_more。
func (s *Server) listOrders(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		writeErr(c, http.StatusBadRequest, "bad_request", "user_id query required")
		return
	}
	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 50 {
			limit = n
		}
	}
	var beforeCreatedAt *time.Time
	var beforeID *int64
	if v := c.Query("cursor_created_at"); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			beforeCreatedAt = &t
		}
	}
	if v := c.Query("cursor_id"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			beforeID = &n
		}
	}
	orders, hasMore, err := s.store.GetOrdersByCursor(c.Request.Context(), userID, beforeCreatedAt, beforeID, limit)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	resp := gin.H{"orders": orders, "has_more": hasMore}
	if len(orders) > 0 {
		last := orders[len(orders)-1]
		resp["next_cursor_created_at"] = last.CreatedAt.Format(time.RFC3339Nano)
		resp["next_cursor_id"] = last.ID
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) getLatestOrder(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		writeErr(c, http.StatusBadRequest, "bad_request", "user_id query required")
		return
	}
	o, err := s.store.GetLatestOrder(c.Request.Context(), userID)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	c.JSON(http.StatusOK, o)
}

func handleStoreErr(c *gin.Context, err error) {
	switch {
	case errIsDeadline(err):
		// EXP-11 总时间预算耗尽：明确归类为超时（504），不伪装成 500。
		// context 取消已把无用工作（DB 语句/回源）终止，这里只负责如实上报。
		RecordDeadlineExceeded()
		writeErr(c, http.StatusGatewayTimeout, "deadline_exceeded", "request deadline exceeded")
	case errors.Is(err, mysql.ErrNotFound):
		writeErr(c, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, mysql.ErrOutOfStock):
		RecordOrderRejected()
		writeErr(c, http.StatusConflict, "out_of_stock", "insufficient stock")
	case errors.Is(err, mysql.ErrIdempotencyConf):
		writeErr(c, http.StatusConflict, "idempotency_conflict", "same key with different params")
	default:
		writeErr(c, http.StatusInternalServerError, "internal_error", "internal error")
	}
}

func hashParams(userID, sku string, qty int) string {
	h := sha256.New()
	h.Write([]byte(userID))
	h.Write([]byte("|"))
	h.Write([]byte(sku))
	h.Write([]byte("|"))
	h.Write([]byte{byte(qty)})
	return hex.EncodeToString(h.Sum(nil))
}

type errBody struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

func writeErr(c *gin.Context, status int, code, msg string) {
	c.JSON(status, errBody{Code: code, Error: msg})
}

// withLogging 结构化请求日志：method、path、status、耗时、请求 ID（EXP-04 扩展指标）。
// EXP-09：把关联上下文（request_id/traceparent）放入 gin context，
// 供下单路径写入 Outbox 事件，贯穿全链路。
func withLogging(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		rid := c.GetHeader("X-Request-Id")
		if rid == "" {
			rid = "rid-" + hex.EncodeToString(randBytes(8))
		}
		c.Set(traceContextKey, event.TraceContext{
			RequestID:   rid,
			TraceParent: c.GetHeader("traceparent"),
		})
		c.Next()
		log.Info("http_request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"request_id", rid,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}

// traceFromContext 读取请求关联上下文；缺失时降级为空（事件仍写入，仅少关联信息）。
func traceFromContext(c *gin.Context) event.TraceContext {
	if v, ok := c.Get(traceContextKey); ok {
		if t, ok := v.(event.TraceContext); ok {
			return t
		}
	}
	return event.TraceContext{}
}

// randBytes 生成随机字节；失败时降级为时间戳派生，保证日志中间件不 panic。
func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		b = []byte(time.Now().Format("20060102150405.000000000"))
	}
	return b
}
