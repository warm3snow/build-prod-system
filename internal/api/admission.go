// EXP-11 准入控制与时间预算：有界准入（限流 + 在途上限）+ 总截止时间约束。
//
// 三层防线（由外向内）：
//  1. 限流（429 rate_limited）：令牌桶按速率拒绝，超载给出明确、立即的反馈；
//  2. 在途上限（503 overloaded）：同一时刻正在处理的请求有硬上限，
//     防止排队无限增长（HTTP 层没有显式队列，超过在途上限即拒绝）；
//  3. 总截止时间（504 deadline_exceeded）：每个请求携带总时间预算，下游
//     各层（DB/缓存/依赖）的子预算由总预算推导，超时后 context 取消无用工作。
//
// 预算关系（EXP-11 冻结，与 deploy/k8s 中 env 一致）：
//   - 读路径总预算 500ms（SLO P99 ≤ 200ms 的 2.5 倍余量）；
//   - 写路径总预算 1s（SLO P99 ≤ 500ms 的 2 倍余量）；
//   - 子预算：Redis 操作 100ms、回源 1s（读路径由总预算先行截断）、
//     DB 语句 2s（DSN readTimeout/writeTimeout，总预算先到先截断）。
//   - 多副本关系：单实例限流预算 × 副本数 = 集群总预算（无跨 Pod 协调，
//     EXP-13 按副本数同步核算；需要全局精确配额应在入口层再限）。
package api

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/warm3snow/build-prod-system/internal/ratelimit"
)

// Admission 准入控制配置。RateLimiter 为 nil 或 MaxInflight <= 0 时对应防线关闭
//（对照实验开关）。预算 <= 0 表示不施加总截止时间。
type Admission struct {
	RateLimiter *ratelimit.TokenBucket
	MaxInflight int
	ReadBudget  time.Duration
	WriteBudget time.Duration
}

type admissionState struct {
	limiter  *ratelimit.TokenBucket
	maxIn    int
	inflight chan struct{}
	cur      atomic.Int64
	readB    time.Duration
	writeB   time.Duration
}

func newAdmissionState(a Admission) *admissionState {
	s := &admissionState{
		limiter: a.RateLimiter,
		maxIn:   a.MaxInflight,
		readB:   a.ReadBudget,
		writeB:  a.WriteBudget,
	}
	if s.maxIn > 0 {
		s.inflight = make(chan struct{}, s.maxIn)
	}
	return s
}

// requestBudgetKey gin context 键：本请求的总时间预算（由 route 决定）。
const requestBudgetKey = "exp11.budget"

// DeadlineMiddleware 施加总截止时间：读/写路由分别使用不同预算。
// 预算到期后 context 自动取消，下游 DB/Redis 操作随之终止（无用工作被取消）。
func (s *admissionState) DeadlineMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		budget := s.readB
		if isWriteRoute(c.FullPath()) {
			budget = s.writeB
		}
		if budget <= 0 {
			c.Next()
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), budget)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Set(requestBudgetKey, budget)
		c.Next()
		// 处理完成但预算已耗尽且未写出响应：明确归类为 deadline_exceeded，
		// 不把超时伪装成 200，也不让下游慢请求无解释地挂着。
		if ctx.Err() != nil && c.Writer.Written() == false && !c.IsAborted() {
			RecordDeadlineExceeded()
			writeErr(c, http.StatusGatewayTimeout, "deadline_exceeded", "request deadline exceeded")
		}
	}
}

// RateLimitMiddleware 令牌桶限流：拒绝返回 429 rate_limited（保护性拒绝，
// 不计系统错误率；客户端应减速或稍后重试，且不应与 5xx 同样激进重试）。
// 探针豁免：/healthz /readyz 不消耗令牌——过载时探针必须保持 2xx，
// 否则 kubelet 会把「限流保护」误判为「进程不健康」而重启 Pod
//（EXP-11 实测：探针被限流 → liveness 失败 → 过载中 Pod 被 SIGTERM 重启，
// 二次放大了过载；修复后过载全程 0 重启）。
func (s *admissionState) RateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if isProbeRoute(c.FullPath()) || s.limiter == nil || s.limiter.Allow() {
			c.Next()
			return
		}
		RecordRateLimited()
		writeErr(c, http.StatusTooManyRequests, "rate_limited", "request rate over budget")
		c.Abort()
	}
}

// InFlightMiddleware 在途请求上限：无队列，超过上限立即拒绝（503 overloaded）。
// 这是比限流更强的「绝对并发」约束：即使令牌桶有突发余量，
// 同时处理中的请求也不会超过进程容量预算（goroutine/DB 连接/内存有界）。
// 探针豁免：探针路径不占用在途额度（与限流豁免同理，且探针本身瞬时完成）。
func (s *admissionState) InFlightMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.inflight == nil || isProbeRoute(c.FullPath()) {
			c.Next()
			return
		}
		select {
		case s.inflight <- struct{}{}:
			s.cur.Add(1)
			httpInflight.Set(float64(s.cur.Load()))
			defer func() {
				<-s.inflight
				s.cur.Add(-1)
				httpInflight.Set(float64(s.cur.Load()))
			}()
			c.Next()
		default:
			RecordInflightRejected()
			writeErr(c, http.StatusServiceUnavailable, "overloaded", "too many in-flight requests")
			c.Abort()
		}
	}
}

// isWriteRoute 写路径（下单）使用更宽的总预算；其余 /api 路由按读预算。
func isWriteRoute(fullPath string) bool {
	return fullPath == "/api/orders"
}

// isProbeRoute 健康探针路径：不受限流/在途上限约束（总时间预算对探针同样不适用）。
func isProbeRoute(fullPath string) bool {
	return fullPath == "/healthz" || fullPath == "/readyz"
}

// errIsDeadline 判断错误是否由预算/超时引起（分类统计与 504 映射共用）。
func errIsDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
