// dbstats 暴露 database/sql 连接池指标（EXP-06 用于连接池 A/B 实验）。
// 采用纯 collector 实现：拉取时实时读取 sql.DB.Stats()，避免与 promauto 双重注册冲突。
package observability

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

// RegisterDBStats 注册连接池指标 collector。
func RegisterDBStats(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil {
		return
	}
	prometheus.DefaultRegisterer.MustRegister(newDBCollector(sqlDB))
}

type dbCollector struct {
	db     *sql.DB
	open   *prometheus.Desc
	inUse  *prometheus.Desc
	wait   *prometheus.Desc
	waitMs *prometheus.Desc
}

func newDBCollector(sqlDB *sql.DB) *dbCollector {
	return &dbCollector{
		db: sqlDB,
		open: prometheus.NewDesc("db_pool_open_connections",
			"当前打开的数据库连接数", nil, nil),
		inUse: prometheus.NewDesc("db_pool_in_use_connections",
			"正在使用的数据库连接数", nil, nil),
		wait: prometheus.NewDesc("db_pool_wait_count_total",
			"因连接池耗尽而等待的累计次数", nil, nil),
		waitMs: prometheus.NewDesc("db_pool_wait_duration_seconds_total",
			"连接池等待累计时长（秒）", nil, nil),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.open
	ch <- c.inUse
	ch <- c.wait
	ch <- c.waitMs
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(c.open, prometheus.GaugeValue, float64(s.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(s.InUse))
	ch <- prometheus.MustNewConstMetric(c.wait, prometheus.CounterValue, float64(s.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.waitMs, prometheus.CounterValue, s.WaitDuration.Seconds())
}
