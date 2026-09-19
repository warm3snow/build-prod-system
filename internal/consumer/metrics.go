// Consumer 指标（EXP-09）：处理结果（新/重复）、错误、业务完成延迟与 Kafka Lag。
package consumer

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	processedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_processed_total",
		Help: "消费处理事件数，按结果分类（new=产生副作用，dup=Inbox 去重命中）。",
	}, []string{"result"})
	processErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_process_errors_total",
		Help: "消费处理错误，按阶段分类（fetch/txn/parse/commit）。",
	}, []string{"op"})
	postProcessLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "order_post_process_latency_seconds",
		Help:    "业务完成延迟：订单创建 → 后置副作用提交。",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, math.Inf(1)},
	})
	kafkaLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "消费组 Lag（最新位点 - 已提交位点）。",
	})
	inflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_inflight",
		Help: "在途消息数（已取出未完成）；有界队列水位即反压信号。",
	})
)

func RecordProcessed(result string) { processedTotal.WithLabelValues(result).Inc() }
func RecordProcessedN(result string, n int) {
	processedTotal.WithLabelValues(result).Add(float64(n))
}
func RecordProcessError(op string)              { processErrorsTotal.WithLabelValues(op).Inc() }
func ObservePostProcessLatency(d time.Duration) { postProcessLatency.Observe(d.Seconds()) }
func SetKafkaLag(lag int64)                     { kafkaLag.Set(float64(lag)) }
func AddInflight(delta int)                     { inflight.Add(float64(delta)) }
