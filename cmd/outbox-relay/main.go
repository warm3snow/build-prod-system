// outbox-relay：Outbox → Kafka 可靠投递进程（EXP-09）。
// 轮询 MySQL Outbox 中的 PENDING 事件，获得 Kafka 确认后标记 SENT；
// 确认失败保持 PENDING 重试（at-least-once），重复投递由 Consumer Inbox 去重。
// Kafka 不可用不影响下单：事件在事务内已与订单一同提交，Kafka 恢复后自动排空。
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
	"github.com/warm3snow/build-prod-system/internal/mq/kafka"
	"github.com/warm3snow/build-prod-system/internal/observability"
	"github.com/warm3snow/build-prod-system/internal/relay"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg := config.Load()

	// 独立连接池：Relay 轮询+标记是低频小查询，小池足够（EXP-06 连接预算表预留）。
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
	sqlDB.SetMaxOpenConns(cfg.RelayDBMaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.RelayDBMaxIdleConns)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	observability.RegisterDBStats(db)
	store := mysql.New(db)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	if err := store.Ping(ctx); err != nil {
		log.Error("ping db", "err", err)
		os.Exit(1)
	}
	cancel()

	// Kafka 就绪前持续重试：Relay 存活不依赖 Kafka（下单亦然），
	// Kafka 稍后可用自动衔接投递；期间 Outbox 事件保持 PENDING 积压。
	var prod *kafka.Producer
	for {
		prod, err = kafka.NewProducer(strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaTopic, cfg.KafkaPartitions)
		if err == nil {
			break
		}
		log.Warn("kafka not ready, retrying", "brokers", cfg.KafkaBrokers, "err", err)
		time.Sleep(5 * time.Second)
	}
	defer prod.Close()
	log.Info("kafka connected", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)

	r := relay.New(store, prod, log, relay.Config{
		BatchSize:    cfg.RelayBatchSize,
		PollInterval: cfg.RelayPollInterval,
		MaxAttempts:  cfg.OutboxMaxAttempts,
	})
	go r.Run()

	metricsSrv := &http.Server{
		Addr:    cfg.RelayMetricsAddr,
		Handler: observability.MetricsHandler(),
	}
	go func() {
		log.Info("relay metrics listening", "addr", cfg.RelayMetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listen", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	r.Stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics shutdown", "err", err)
	}
	log.Info("outbox-relay stopped")
}
