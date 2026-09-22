// dependency-simulator：可控依赖模拟器（EXP-12 非关键依赖）。
//
// 提供商品附加信息（推荐语/描述）查询，模拟一个非关键下游服务：
//   - GET /extra/:sku → {"sku":..., "extra":{"recommendation":"...", "generated_at":...}}
//   - POST /control {"mode":"ok"|"slow"|"fail","latency_ms":N,"fail_rate":0.5}
//     slow：每次响应固定延迟 latency_ms；fail：以 fail_rate 概率返回 500；
//   - GET /state  → 当前控制状态
//   - GET /healthz
//
// 用途：order-api 通过 /api/products/:sku/extra 调用它；模拟器可以被实验
// 实时切换到慢响应/持续失败，观察共享资源消耗、隔离与熔断行为。
// 这是实验组件，不承载真实业务数据。
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type state struct {
	mu        sync.RWMutex
	mode      string // ok | slow | fail
	latencyMs int64
	failRate  float64
}

var (
	cur     state
	calls   atomic.Int64
	latency atomic.Int64 // 累计延迟微秒（观测）
)

type controlReq struct {
	Mode      string  `json:"mode"`
	LatencyMs int64   `json:"latency_ms"`
	FailRate  float64 `json:"fail_rate"`
}

func main() {
	cur = state{mode: "ok", latencyMs: 0, failRate: 0}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/state", func(w http.ResponseWriter, _ *http.Request) {
		cur.mu.RLock()
		defer cur.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode": cur.mode, "latency_ms": cur.latencyMs, "fail_rate": cur.failRate,
			"calls": calls.Load(), "total_latency_us": latency.Load(),
		})
	})
	mux.HandleFunc("/control", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req controlReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		cur.mu.Lock()
		if req.Mode != "" {
			cur.mode = req.Mode
		}
		if req.LatencyMs > 0 {
			cur.latencyMs = req.LatencyMs
		}
		if req.FailRate > 0 {
			cur.failRate = req.FailRate
		}
		if req.Mode == "ok" {
			cur.latencyMs, cur.failRate = 0, 0
		}
		cur.mu.Unlock()
		log.Info("control applied", "mode", req.Mode, "latency_ms", req.LatencyMs, "fail_rate", req.FailRate)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/extra/", func(w http.ResponseWriter, r *http.Request) {
		sku := r.URL.Path[len("/extra/"):]
		calls.Add(1)
		cur.mu.RLock()
		mode, lat, fr := cur.mode, cur.latencyMs, cur.failRate
		cur.mu.RUnlock()

		if mode == "slow" && lat > 0 {
			start := time.Now()
			time.Sleep(time.Duration(lat) * time.Millisecond)
			latency.Add(time.Since(start).Microseconds())
		}
		if mode == "fail" {
			// 确定性伪随机：以 fail_rate 概率失败（简单近似，实验用途）
			if float64(calls.Load()%1000)/1000.0 < fr {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"simulated failure"}`))
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sku": sku,
			"extra": map[string]any{
				"recommendation": "Flash 配件推荐：USB-C 快充线",
				"generated_at":   time.Now().UTC().Format(time.RFC3339),
			},
		})
	})

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	log.Info("dependency-simulator listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
