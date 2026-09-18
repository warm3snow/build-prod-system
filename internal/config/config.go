// Package config 从环境变量读取服务配置，提供实验可复现的默认值。
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	DBDSN           string
	PingTimeout     time.Duration
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	DBMaxOpenConns  int
	DBMaxIdleConns  int

	// Redis 缓存（EXP-07）。Redis 是可选依赖：连接失败时降级为无缓存模式。
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisTimeout  time.Duration
	RedisPoolSize int
	CacheEnabled  bool
	CacheTTL      time.Duration
	NegCacheTTL   time.Duration
	// 实验开关（EXP-07 陈旧窗口测量用，生产配置必须为 0）：
	InvalidateDelay time.Duration // 下单提交后延迟 DEL，放大"提交→失效"窗口
	FillDelay       time.Duration // 回源后延迟 SET，放大"旧值回填"竞态窗口

	// 有界回源（EXP-08）：热点过期合并、回源并发/超时上限、超限降级（旧值/拒绝）。
	CoalesceEnabled      bool          // 进程内请求合并（singleflight），对照实验可关闭
	BackfillMaxConcur    int           // 回源并发上限，必须在 DB 连接预算内
	BackfillAcquireTO    time.Duration // 等待回源额度的最长时间（有界等待）
	BackfillTimeout      time.Duration // 单次回源的独立超时
	StaleMaxEntries      int           // 本地旧值库最大条目数
	StaleTTL             time.Duration // 本地旧值可服务时长
	InvalidateQueueSize  int           // 异步失效队列容量；满则丢弃（陈旧由 TTL 兜底）
}

func Load() Config {
	return Config{
		HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
		DBDSN:           getEnv("DB_DSN", "flash:flash@tcp(127.0.0.1:3306)/flash?parseTime=true&timeout=2s&readTimeout=2s&writeTimeout=2s"),
		PingTimeout:     getDur("DB_PING_TIMEOUT", 2*time.Second),
		ShutdownTimeout: getDur("SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:     getDur("HTTP_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    getDur("HTTP_WRITE_TIMEOUT", 10*time.Second),
		DBMaxOpenConns:  getInt("DB_MAX_OPEN_CONNS", 25),
		DBMaxIdleConns:  getInt("DB_MAX_IDLE_CONNS", 10),

		RedisAddr:       getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:         getInt("REDIS_DB", 0),
		RedisTimeout:    getDur("REDIS_TIMEOUT", 100*time.Millisecond),
		RedisPoolSize:   getInt("REDIS_POOL_SIZE", 128),
		CacheEnabled:    getBool("CACHE_ENABLED", false),
		CacheTTL:        getDur("CACHE_TTL", 60*time.Second),
		NegCacheTTL:     getDur("NEG_CACHE_TTL", 5*time.Second),
		InvalidateDelay: time.Duration(getInt("CACHE_INVALIDATE_DELAY_MS", 0)) * time.Millisecond,
		FillDelay:       time.Duration(getInt("CACHE_FILL_DELAY_MS", 0)) * time.Millisecond,

		CoalesceEnabled:      getBool("CACHE_COALESCE_ENABLED", true),
		BackfillMaxConcur:    getInt("BACKFILL_MAX_CONCURRENCY", 12),
		BackfillAcquireTO:    getDur("BACKFILL_ACQUIRE_TIMEOUT", 200*time.Millisecond),
		BackfillTimeout:      getDur("BACKFILL_TIMEOUT", time.Second),
		StaleMaxEntries:      getInt("STALE_MAX_ENTRIES", 1024),
		StaleTTL:             getDur("STALE_TTL", 60*time.Second),
		InvalidateQueueSize:  getInt("INVALIDATE_QUEUE_SIZE", 1024),
	}
}

func getBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
