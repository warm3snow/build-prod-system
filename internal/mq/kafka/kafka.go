// Package kafka 封装订单事件的 Kafka 生产/消费（EXP-09，segmentio/kafka-go）。
//
// 投递语义（ADR-018）：
//   - Producer：acks=all 等待确认后返回；确认失败 = 未知结果，Relay 允许重试；
//   - Consumer：手动提交位点。主线保序处理：单分区 + 单 goroutine 逐条处理，
//     一条消息的事务完成后才提交该条位点，不越过尚未完成的消息。
package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	// DefaultTopic 订单事件主题：单分区（EXP-09 保序主线；EXP-10 再评估分区扩展）。
	DefaultTopic = "orders"
	// DefaultGroup 消费组：仅 consumer 一个成员。
	DefaultGroup = "order-consumer"
)

// Producer 订单事件生产者。所有消息带 key=event_id：
// 保序 + 重试时消息可定位；EXP-10 扩分区后仍保证同 Key 有序。
type Producer struct {
	w *kafka.Writer
}

// NewProducer 创建生产者并确保主题存在（幂等创建）。
// EXP-10：分区数由调用方传入（KAFKA_PARTITIONS）；已存在主题分区数不足时
// 返回错误（分区只增不减，实验对照用不同主题名或手动 alter，见 EXP-10-manual）。
// EXP-17：RF/minISR 由调用方传入（HA 档 3/2；单 Broker 档 1/0=不设置）。
func NewProducer(brokers []string, topic string, partitions, rf, minISR int) (*Producer, error) {
	if partitions <= 0 {
		partitions = 1
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		// 批量写模型：BatchSize 100 满载立即刷盘；尾批等待 BatchTimeout 后刷盘。
		// 默认 BatchTimeout=1s 会导致串行逐条写入每条等 1s（EXP-09 场景 A 实测
		// ~1 msg/s 的根因），这里压到 50ms：投递延迟 P99 仍远低于 2s SLO。
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		// 发送超时：超时视为未知结果（消息可能已被 broker 持久化），
		// 由 Relay 重试 + Inbox 去重兜底。
		WriteTimeout: 2 * time.Second,
		MaxAttempts:  3,
	}
	if err := ensureTopic(brokers, topic, partitions, rf, minISR); err != nil {
		_ = w.Close()
		return nil, err
	}
	return &Producer{w: w}, nil
}

// ProduceBatch 批量发送事件并等待 Kafka 确认（单次 WriteMessages，一次刷盘）。
// 返回 nil == 全部获得确认；错误 == 未确认或部分确认（整批保持 PENDING 重试，
// 重复投递由 Consumer Inbox 去重）。
func (p *Producer) ProduceBatch(ctx context.Context, msgs []BatchMessage) error {
	km := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		km = append(km, kafka.Message{
			Key:   []byte(m.Key),
			Value: m.Value,
			Headers: []kafka.Header{
				{Key: "event_id", Value: []byte(m.Key)},
				{Key: "request_id", Value: []byte(m.RequestID)},
			},
		})
	}
	return p.w.WriteMessages(ctx, km...)
}

// BatchMessage 批量发送的消息条目。
type BatchMessage struct {
	Key       string // event_id：Kafka 消息 key
	Value     []byte // 事件 payload
	RequestID string // 关联 ID（Kafka header）
}

func (p *Producer) Close() error { return p.w.Close() }

// Consumer 手动位点消费者。保序主线：调用方单 goroutine 逐条 Fetch → Process → Commit。
type Consumer struct {
	r *kafka.Reader
}

// NewConsumer 创建消费组消费者。新消费组无提交位点时从最早消息开始
// （kafka-go 默认 FirstOffset），保证部署前积压的事件不被跳过。
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	return &Consumer{r: r}
}

// Fetch 读取下一条消息；返回 io.EOF 表示消费组已关闭。
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.r.FetchMessage(ctx)
}

// Commit 提交位点。仅在消息对应的事务提交成功后调用。
func (c *Consumer) Commit(ctx context.Context, msgs ...kafka.Message) error {
	return c.r.CommitMessages(ctx, msgs...)
}

// Lag 当前消费延迟（分区最新位点 - 已提交位点）。
func (c *Consumer) Lag() int64 { return c.r.Lag() }

func (c *Consumer) Close() error { return c.r.Close() }

// ensureTopic 幂等创建主题：分区/副本由参数给定（EXP-17：RF 与 min.insync.replicas
// 显式传入，不依赖 broker 默认——多副本投递语义必须落在 topic 配置上）。
// 主题已存在且分区数小于目标时返回错误——分区只增不减，
// 实验对照需要重建主题（Kafka 事件可从 Outbox 重放，见 EXP-10-manual）。
func ensureTopic(brokers []string, topic string, partitions, rf, minISR int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get kafka controller: %w", err)
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer cc.Close()

	// 已存在主题的分区数校验（只增不减的约束）。
	parts, err := cc.ReadPartitions(topic)
	if err == nil && len(parts) > 0 {
		if got := len(parts); got < partitions {
			return fmt.Errorf("topic %s exists with %d partitions < desired %d; "+
				"recreate topic or set KAFKA_PARTITIONS accordingly", topic, got, partitions)
		}
		return nil // 分区数已满足
	}

	// kafka-go 的 CreateTopics 对 TopicAlreadyExists（36）幂等跳过，不返回错误。
	tc := kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: rf}
	if minISR > 0 {
		tc.ConfigEntries = append(tc.ConfigEntries, kafka.ConfigEntry{
			ConfigName: "min.insync.replicas", ConfigValue: strconv.Itoa(minISR),
		})
	}
	if err := cc.CreateTopics(tc); err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	return nil
}
