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
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

type Store interface {
	Ping(ctx context.Context) error
	GetProduct(ctx context.Context, sku string) (mysql.Product, error)
	GetStock(ctx context.Context, sku string) (int, error)
	CreateOrder(ctx context.Context, userID, sku, idemKey, paramHash string) (*mysql.CreatedOrder, error)
	GetLatestOrder(ctx context.Context, userID string) (*mysql.CreatedOrder, error)
	GetOrdersByCursor(ctx context.Context, userID string, beforeCreatedAt *time.Time, beforeID *int64, limit int) ([]mysql.CreatedOrder, bool, error)
}

type Server struct {
	store Store
	log   *slog.Logger
}

func NewServer(store Store, log *slog.Logger) *Server {
	return &Server{store: store, log: log}
}

// Routes 返回带日志与指标中间件的 Gin 路由。
func (s *Server) Routes() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), MetricsMiddleware(), withLogging(s.log))
	r.GET("/healthz", s.healthz)
	r.GET("/readyz", s.readyz)
	r.GET("/api/products/:sku", s.getProduct)
	r.GET("/api/products/:sku/stock", s.getStock)
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

func (s *Server) getProduct(c *gin.Context) {
	sku := c.Param("sku")
	p, err := s.store.GetProduct(c.Request.Context(), sku)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
	c.JSON(http.StatusOK, p)
}

func (s *Server) getStock(c *gin.Context) {
	sku := c.Param("sku")
	stock, err := s.store.GetStock(c.Request.Context(), sku)
	if err != nil {
		handleStoreErr(c, err)
		return
	}
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
	o, err := s.store.CreateOrder(c.Request.Context(), req.UserID, req.SKU, key, paramHash)
	if err != nil {
		handleStoreErr(c, err)
		return
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
func withLogging(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		rid := c.GetHeader("X-Request-Id")
		if rid == "" {
			rid = "rid-" + hex.EncodeToString(randBytes(8))
		}
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

// randBytes 生成随机字节；失败时降级为时间戳派生，保证日志中间件不 panic。
func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		b = []byte(time.Now().Format("20060102150405.000000000"))
	}
	return b
}
