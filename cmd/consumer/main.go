// consumer：订单事件消费进程（EXP-09）。
// 从 Kafka 消费订单事件，Inbox 去重与后置副作用（订单受理通知）在同一 MySQL 事务；
// 事务提交后才提交消费位点。保序主线：单 goroutine 逐条处理，不越过失败消息。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/warm3snow/build-prod-system/internal/config"
	"github.com/warm3snow/build-prod-system/internal/consumer"
	"github.com/warm3snow/build-prod-system/internal/mq/kafka"
	"github.com/warm3snow/build-prod-system/internal/observability"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg := config.Load()

	// 独立连接池：消费事务低频小查询，小池足够（EXP-06 连接预算表预留）。
	db, err := gorm.Open(gormmysql.Open(cfg.DBDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		log.Error("open db", "err", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Error("get sql db", "err", err)
		os.Exit(1)
	}
	sqlDB.SetMaxOpenConns(cfg.ConsumerDBMaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.ConsumerDBMaxIdleConns)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	observability.RegisterDBStats(db)
	store := mysql.New(db)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	if err := store.Ping(ctx); err != nil {
		log.Error("ping db", "err", err)
		os.Exit(1)
	}
	cancel()

	cons := kafka.NewConsumer(
		strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaTopic, cfg.ConsumerGroup,
	)
	defer cons.Close()
	log.Info("consumer started", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.ConsumerGroup)

	// Kafka Lag 周期采集。
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			consumer.SetKafkaLag(cons.Lag())
		}
	}()

	proc := consumer.New(store, cons, log, consumer.Config{RetryBackoff: cfg.ConsumerRetryBackoff})
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go proc.Run(runCtx)

	metricsSrv := &http.Server{
		Addr:    cfg.ConsumerMetricsAddr,
		Handler: observability.MetricsHandler(),
	}
	go func() {
		log.Info("consumer metrics listening", "addr", cfg.ConsumerMetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listen", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	// 优雅退出：先停止消费循环，再关闭 reader（Fetch 阻塞解除）。
	runCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics shutdown", "err", err)
	}
	log.Info("consumer stopped")
}
