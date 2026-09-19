// Relay 指标（EXP-09）：投递结果、积压水位、错误与批次耗时。
// 积压指标同时供告警（PrometheusRule）与对账使用。
package relay

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	relaySentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_sent_total",
		Help: "Relay 已获 Kafka 确认并标记 SENT 的事件数。",
	})
	relayFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_failed_total",
		Help: "Relay 投递失败次数（保持 PENDING 重试）。",
	})
	relayBatchFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_batch_failed_total",
		Help: "Relay 批量发送失败次数（Kafka 不可用/确认失败，整批保持 PENDING）。",
	})
	relayDeadTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_dead_total",
		Help: "超过最大尝试次数被标记 DEAD 的事件数（显式失败，可重放）。",
	})
	relayErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_errors_total",
		Help: "Relay 内部错误，按操作分类（fetch/mark_sent/mark_failed）。",
	}, []string{"op"})
	relayBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "relay_batch_duration_seconds",
		Help:    "单批投递耗时分布。",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})
	outboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_backlog",
		Help: "Outbox 中 PENDING 事件数（积压水位）。",
	})
	outboxOldestPendingAge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_pending_age_seconds",
		Help: "最老 PENDING 事件年龄（秒），衡量投递时效。",
	})
	outboxDead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_dead",
		Help: "Outbox 中 DEAD 事件数（显式失败待重放）。",
	})
)

func RecordRelaySent()           { relaySentTotal.Inc() }
func RecordRelaySentBatch(n int) { relaySentTotal.Add(float64(n)) }
func RecordRelayFailed()         { relayFailedTotal.Inc() }
func RecordRelayBatchFailed()    { relayBatchFailedTotal.Inc() }
func RecordRelayDead()           { relayDeadTotal.Inc() }
func RecordRelayError(op string) { relayErrorsTotal.WithLabelValues(op).Inc() }

// ObserveRelayBatch 记录批次结果；outcome 取 ok/partial（含失败）。
func ObserveRelayBatch(d time.Duration, total, sent, failed, dead int) {
	outcome := "ok"
	if failed > 0 || dead > 0 {
		outcome = "partial"
	}
	relayBatchDuration.WithLabelValues(outcome).Observe(d.Seconds())
}

// SetOutboxGauges 更新积压水位（由 Relay 周期采集或调用方轮询后调用）。
func SetOutboxGauges(pending, dead int64, oldestAge time.Duration) {
	outboxBacklog.Set(float64(pending))
	outboxDead.Set(float64(dead))
	outboxOldestPendingAge.Set(oldestAge.Seconds())
}
