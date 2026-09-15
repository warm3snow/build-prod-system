// Package config 从环境变量读取服务配置，提供实验可复现的默认值。
package config

import (
	"os"
	"time"
)

type Config struct {
	HTTPAddr        string
	DBDSN           string
	PingTimeout     time.Duration
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
		DBDSN:           getEnv("DB_DSN", "flash:flash@tcp(127.0.0.1:3306)/flash?parseTime=true&timeout=2s&readTimeout=2s&writeTimeout=2s"),
		PingTimeout:     getDur("DB_PING_TIMEOUT", 2*time.Second),
		ShutdownTimeout: getDur("SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:     getDur("HTTP_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    getDur("HTTP_WRITE_TIMEOUT", 10*time.Second),
	}
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
