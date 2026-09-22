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
	CoalesceEnabled     bool          // 进程内请求合并（singleflight），对照实验可关闭
	BackfillMaxConcur   int           // 回源并发上限，必须在 DB 连接预算内
	BackfillAcquireTO   time.Duration // 等待回源额度的最长时间（有界等待）
	BackfillTimeout     time.Duration // 单次回源的独立超时
	StaleMaxEntries     int           // 本地旧值库最大条目数
	StaleTTL            time.Duration // 本地旧值可服务时长
	InvalidateQueueSize int           // 异步失效队列容量；满则丢弃（陈旧由 TTL 兜底）

	// 可靠事件（EXP-09）：Outbox → Kafka → Inbox 去重 → 后置副作用。
	KafkaBrokers    string // 逗号分隔的 Kafka broker 列表
	KafkaTopic      string // 订单事件主题
	KafkaPartitions int    // 主题分区数（EXP-10 冻结 3；分区只增不减）
	ConsumerGroup   string // 消费组 ID

	// outbox-relay
	RelayBatchSize      int           // 每轮投递批次大小
	RelayPollInterval   time.Duration // 轮询间隔（也决定投递延迟下界）
	OutboxMaxAttempts   int           // 最大投递尝试次数，超过标记 DEAD
	RelayMetricsAddr    string        // Relay metrics 监听地址
	RelayDBMaxOpenConns int           // Relay 独立连接池上限（EXP-06 预算预留）
	RelayDBMaxIdleConns int

	// consumer
	ConsumerRetryBackoff   time.Duration // 处理失败（DB 不可用）重试间隔
	ConsumerMetricsAddr    string        // Consumer metrics 监听地址
	ConsumerDBMaxOpenConns int           // Consumer 独立连接池上限（EXP-06 预算预留）
	ConsumerDBMaxIdleConns int
	ConsumerConcurrency    int           // 并发 worker 数（EXP-10）
	ConsumerMaxInflight    int           // 在途消息上限（有界队列 = 反压）
	ConsumerBatchSize      int           // worker 攒批大小（一批一个事务，1 次 fsync）
	ConsumerBatchWait      time.Duration // 攒批等待上限

	// Outbox 积压预算（EXP-10 反压）：PENDING 超过该水位时 order-api 拒绝新下单
	//（503 backlog_limited），防止积压无限增长。
	OutboxBacklogLimit int

	// 准入控制与时间预算（EXP-11）：
	//   RateLimitRPS：单实例令牌桶速率（0 = 关闭，对照实验开关）；
	//   RateLimitBurst：桶容量（允许的短突发）；
	//   MaxInflight：同时处理中的请求硬上限（0 = 关闭）；
	//   ReadBudget / WriteBudget：读/写路径总截止时间（0 = 不施加）。
	RateLimitRPS    float64
	RateLimitBurst  float64
	MaxInflight     int
	ReadBudget      time.Duration
	WriteBudget     time.Duration

	// 非关键依赖（EXP-12 商品附加信息）：资源隔离 + 熔断 + 降级。
	DepSimAddr       string        // 依赖模拟器地址；空 = 不启用依赖客户端
	DepTimeout       time.Duration // 单次依赖调用独立超时（写路径 1s 总预算的子预算）
	DepPoolSize      int           // 依赖调用并发/连接上限（0 = 无隔离，对照实验）
	DepAcquireTO     time.Duration // 等待依赖槽位的最长时间（有界等待）
	DepIsolation     bool          // 隔离+熔断总开关（对照实验可关闭）
	DepCBFailThresh  int           // 熔断连续失败阈值
	DepCBOpenDur     time.Duration // 熔断冷却时长
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

		CoalesceEnabled:     getBool("CACHE_COALESCE_ENABLED", true),
		BackfillMaxConcur:   getInt("BACKFILL_MAX_CONCURRENCY", 12),
		BackfillAcquireTO:   getDur("BACKFILL_ACQUIRE_TIMEOUT", 200*time.Millisecond),
		BackfillTimeout:     getDur("BACKFILL_TIMEOUT", time.Second),
		StaleMaxEntries:     getInt("STALE_MAX_ENTRIES", 1024),
		StaleTTL:            getDur("STALE_TTL", 60*time.Second),
		InvalidateQueueSize: getInt("INVALIDATE_QUEUE_SIZE", 1024),

		KafkaBrokers:    getEnv("KAFKA_BROKERS", "kafka.order-lab.svc.cluster.local:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "orders"),
		KafkaPartitions: getInt("KAFKA_PARTITIONS", 3),
		ConsumerGroup:   getEnv("CONSUMER_GROUP", "order-consumer"),

		RelayBatchSize:      getInt("RELAY_BATCH_SIZE", 100),
		RelayPollInterval:   getDur("RELAY_POLL_INTERVAL", 500*time.Millisecond),
		OutboxMaxAttempts:   getInt("OUTBOX_MAX_ATTEMPTS", 100),
		RelayMetricsAddr:    getEnv("RELAY_METRICS_ADDR", ":2113"),
		RelayDBMaxOpenConns: getInt("RELAY_DB_MAX_OPEN_CONNS", 4),
		RelayDBMaxIdleConns: getInt("RELAY_DB_MAX_IDLE_CONNS", 2),

		ConsumerRetryBackoff:   getDur("CONSUMER_RETRY_BACKOFF", time.Second),
		ConsumerMetricsAddr:    getEnv("CONSUMER_METRICS_ADDR", ":2114"),
		ConsumerDBMaxOpenConns: getInt("CONSUMER_DB_MAX_OPEN_CONNS", 16),
		ConsumerDBMaxIdleConns: getInt("CONSUMER_DB_MAX_IDLE_CONNS", 8),
		ConsumerConcurrency:    getInt("CONSUMER_CONCURRENCY", 8),
		ConsumerMaxInflight:    getInt("CONSUMER_MAX_INFLIGHT", 16),
		ConsumerBatchSize:      getInt("CONSUMER_BATCH_SIZE", 50),
		ConsumerBatchWait:      getDur("CONSUMER_BATCH_WAIT", 10*time.Millisecond),

		OutboxBacklogLimit: getInt("OUTBOX_BACKLOG_LIMIT", 200000),

		RateLimitRPS:    getFloat("RATE_LIMIT_RPS", 0),
		RateLimitBurst:  getFloat("RATE_LIMIT_BURST", 0),
		MaxInflight:     getInt("MAX_INFLIGHT", 0),
		ReadBudget:      getDur("READ_BUDGET", 0),
		WriteBudget:     getDur("WRITE_BUDGET", 0),

		DepSimAddr:      getEnv("DEP_SIM_ADDR", ""),
		DepTimeout:      getDur("DEP_TIMEOUT", 200*time.Millisecond),
		DepPoolSize:     getInt("DEP_POOL_SIZE", 8),
		DepAcquireTO:    getDur("DEP_ACQUIRE_TIMEOUT", 50*time.Millisecond),
		DepIsolation:    getBool("DEP_ISOLATION", true),
		DepCBFailThresh: getInt("DEP_CB_FAIL_THRESHOLD", 5),
		DepCBOpenDur:    getDur("DEP_CB_OPEN_DURATION", 10*time.Second),
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

func getFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
