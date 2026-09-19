// event-replay：后置任务突发回放工具（EXP-10）。
// 直接批量注入 Outbox PENDING 事件（不经下单接口），模拟「独立数据集的
// 有效订单事件」突发。回放吞吐不计为真实下单 TPS（课程口径）。
//
// 回放事件使用负数订单域（order_id < 0）与 replay- 用户前缀：
// 与真实订单（>0）在 orders/notifications 唯一键上互不干扰，对账按域区分。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/store/mysql"
)

func main() {
	var (
		dsn   = flag.String("dsn", os.Getenv("DB_DSN"), "MySQL DSN（或 DB_DSN env）")
		count = flag.Int("count", 0, "注入事件数")
		batch = flag.Int("batch", 500, "每批 INSERT 行数")
	)
	flag.Parse()
	if *dsn == "" {
		*dsn = "flash:flash@tcp(127.0.0.1:3306)/flash?parseTime=true&timeout=5s&readTimeout=10s&writeTimeout=10s"
	}
	if *count <= 0 {
		log.Fatal("usage: event-replay -count N [-batch 500] [-dsn ...]")
	}

	db, err := gorm.Open(gormmysql.Open(*dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("sql db: %v", err)
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(4)

	ctx := context.Background()
	start := time.Now()
	injected := 0
	// 负数订单域起点：回放事件用负数 order_id（不与真实订单冲突）。
	// 已有负数时继续递减；尚无负数时从 -1 开始。
	var minOrderID int64
	db.Model(&mysql.OutboxEvent{}).Select("COALESCE(MIN(order_id),0)").Scan(&minOrderID)
	nextID := minOrderID - 1
	if nextID > 0 {
		nextID = -1
	}

	for injected < *count {
		n := *batch
		if injected+n > *count {
			n = *count - injected
		}
		rows := make([]mysql.OutboxEvent, 0, n)
		for i := 0; i < n; i++ {
			e := event.OrderCreated{
				EventID:   event.NewID(),
				OrderID:   nextID,
				UserID:    "replay-bot",
				SKU:       "P1",
				CreatedAt: time.Now(),
			}
			row := mysql.OutboxEvent{
				EventID:   e.EventID,
				OrderID:   nextID,
				UserID:    e.UserID,
				SKU:       e.SKU,
				Payload:   mustJSON(e),
				Status:    mysql.StatusPending,
				CreatedAt: e.CreatedAt,
			}
			rows = append(rows, row)
			nextID--
		}
		if err := db.WithContext(ctx).Create(&rows).Error; err != nil {
			log.Fatalf("insert batch at %d: %v", injected, err)
		}
		injected += n
		if injected%(*batch*20) == 0 || injected == *count {
			fmt.Printf("injected %d/%d (%.1fs)\n", injected, *count, time.Since(start).Seconds())
		}
	}
	fmt.Printf("done: %d events injected in %.1fs (%.0f/s)\n",
		*count, time.Since(start).Seconds(), float64(*count)/time.Since(start).Seconds())
}

func mustJSON(e event.OrderCreated) string {
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return string(b)
}
