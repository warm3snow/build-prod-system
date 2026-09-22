// Package mysql 的存储层指标。
// EXP-11：事务重试计数——重试放大率的分子（下游事务执行次数 vs 业务请求数），
// 用于验证「重试只发生在明确可重试错误上，且受次数与总预算约束」。
package mysql

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var txRetriesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "mysql_tx_retries_total",
		Help: "因死锁（1213）/锁等待（1205）触发的事务重试次数（EXP-11 重试放大观测）。",
	},
)

// RecordTxRetry 事务因可重试错误被重试时调用。
func RecordTxRetry() { txRetriesTotal.Inc() }
