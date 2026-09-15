// order-api：Flash Order 最小订单服务（EXP-02）。
// 关注点：MySQL 持久化、事务写路径、探针、优雅退出、结构化日志。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/warm3snow/build-prod-system/internal/api"
	"github.com/warm3snow/build-prod-system/internal/config"
	"github.com/warm3snow/build-prod-system/internal/observability"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	gin.SetMode(gin.ReleaseMode)

	cfg := config.Load()

	// 数据库连接池：EXP-02 先用保守默认，EXP-06 基于实测调整并形成预算。
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
	// 连接池参数由 env 配置（EXP-06 做 A/B 实验，最终值写入连接预算）
	sqlDB.SetMaxOpenConns(cfg.DBMaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.DBMaxIdleConns)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	observability.RegisterDBStats(db)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	store := mysql.New(db)
	if err := store.InitSchema(ctx); err != nil {
		log.Error("init schema", "err", err)
		os.Exit(1)
	}
	if err := store.Ping(ctx); err != nil {
		log.Error("ping db", "err", err)
		os.Exit(1)
	}
	cancel()

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      api.NewServer(store, log).Routes(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	// metrics 独立端口：抓取流量不与业务请求混合，避免污染 RED 指标。
	metricsSrv := &http.Server{
		Addr:    ":2112",
		Handler: observability.MetricsHandler(),
	}
	go func() {
		log.Info("metrics listening", "addr", ":2112")
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listen", "err", err)
			os.Exit(1)
		}
	}()

	go func() {
		log.Info("order-api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	// 优雅退出：SIGTERM 后停止接收新请求，等待在途请求完成。
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
		_ = srv.Close()
	}
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics shutdown", "err", err)
	}
	log.Info("order-api stopped")
}
