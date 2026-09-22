// Package mysql 提供 Flash Order 的 MySQL 访问层（GORM 实现）。
// EXP-02 目标：数据库持久化、事务写路径、幂等存储的最小可用实现。
package mysql

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"
	"github.com/warm3snow/build-prod-system/internal/event"
	"github.com/warm3snow/build-prod-system/internal/order"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrOutOfStock      = errors.New("out of stock")
	ErrIdempotencyConf = errors.New("idempotency conflict")
	ErrInternal        = errors.New("internal error")
)

// Product 商品。gorm tag 负责建表结构，json tag 保持 API 契约。
type Product struct {
	SKU        string `gorm:"column:sku;primaryKey;size:64" json:"sku"`
	Name       string `gorm:"column:name;not null;size:128;index:idx_products_name" json:"name"`
	PriceCents int64  `gorm:"column:price_cents;not null" json:"price_cents"`
}

func (Product) TableName() string { return "products" }

// Inventory 单 SKU 库存。不变量：stock 永远 >= 0。
type Inventory struct {
	Sku   string `gorm:"column:sku;primaryKey;size:64" json:"sku"`
	Stock int    `gorm:"column:stock;not null;check:stock >= 0" json:"stock"`
}

func (Inventory) TableName() string { return "inventory" }

// OrderModel 订单持久化模型。
type OrderModel struct {
	ID             int64             `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	UserID         string            `gorm:"column:user_id;not null;size:64;index:idx_orders_user_created,priority:1" json:"user_id"`
	Sku            string            `gorm:"column:sku;not null;size:64" json:"sku"`
	Qty            int               `gorm:"column:qty;not null" json:"qty"`
	UnitPriceCents int64             `gorm:"column:unit_price_cents;not null" json:"unit_price_cents"`
	Status         order.OrderStatus `gorm:"column:status;not null;size:16" json:"status"`
	CreatedAt      time.Time         `gorm:"column:created_at;not null;index:idx_orders_user_created,priority:2" json:"-"`
}

func (OrderModel) TableName() string { return "orders" }

// Idempotency 幂等记录，(user_id, idem_key) 复合主键保证同键唯一。
type Idempotency struct {
	UserID    string    `gorm:"column:user_id;primaryKey;size:64" json:"user_id"`
	IdemKey   string    `gorm:"column:idem_key;primaryKey;size:128" json:"idem_key"`
	OrderID   int64     `gorm:"column:order_id;not null;index:idx_idem_order" json:"order_id"`
	ParamHash string    `gorm:"column:param_hash;not null;size:64" json:"param_hash"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"-"`
}

func (Idempotency) TableName() string { return "idempotency" }

// Store 数据访问层。
type Store struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Store {
	return &Store{db: db}
}

// InitSchema 用 AutoMigrate 幂等建表，并预置种子数据（OnConflict DoNothing 等价 INSERT IGNORE）。
// 后续实验通过增量迁移演进，见 ADR-004。
func (s *Store) InitSchema(ctx context.Context) error {
	if err := s.db.WithContext(ctx).AutoMigrate(
		&Product{}, &Inventory{}, &OrderModel{}, &Idempotency{},
		&OutboxEvent{}, &InboxEvent{}, &OrderNotification{},
	); err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	seeds := []Product{
		{SKU: "P1", Name: "Flash Phone Case", PriceCents: 9900},
		{SKU: "P2", Name: "Flash USB Cable", PriceCents: 1900},
	}
	for _, p := range seeds {
		if err := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{DoNothing: true}).Create(&p).Error; err != nil {
			return fmt.Errorf("seed product %s: %w", p.SKU, err)
		}
	}
	stocks := []Inventory{{Sku: "P1", Stock: 1000}, {Sku: "P2", Stock: 1000}}
	for _, inv := range stocks {
		if err := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{DoNothing: true}).Create(&inv).Error; err != nil {
			return fmt.Errorf("seed inventory %s: %w", inv.Sku, err)
		}
	}
	return nil
}

// Ping 健康检查。
func (s *Store) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// GetProduct 查询商品；未找到返回 ErrNotFound。
func (s *Store) GetProduct(ctx context.Context, sku string) (Product, error) {
	var p Product
	if err := s.db.WithContext(ctx).Where("sku = ?", sku).First(&p).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return p, ErrNotFound
		}
		return p, fmt.Errorf("get product: %w", err)
	}
	return p, nil
}

// GetStock 查询库存；未找到返回 ErrNotFound。
func (s *Store) GetStock(ctx context.Context, sku string) (int, error) {
	var inv Inventory
	if err := s.db.WithContext(ctx).Where("sku = ?", sku).First(&inv).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("get stock: %w", err)
	}
	return inv.Stock, nil
}

// CreatedOrder 下单事务结果。
type CreatedOrder struct {
	ID             int64     `json:"id"`
	UserID         string    `json:"user_id"`
	SKU            string    `json:"sku"`
	Qty            int       `json:"qty"`
	UnitPriceCents int64     `json:"unit_price_cents"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	Replayed       bool      `json:"replayed"`
}

// CreateOrder 在单个事务内完成：幂等占位 → 扣库存 → 建订单 → 写 Outbox 事件。
// 采用 insert-first 幂等：先 INSERT (user_id, idem_key) 占位，
// 同键并发由唯一键（复合主键）串行化，避免 SELECT ... FOR UPDATE 在空记录上的 gap lock 死锁。
// 死锁（1213）与锁等待（1205）通过有限重试自动恢复。
// EXP-09：新订单在事务内写入 Outbox（事件 ID 唯一），重放订单不产生新事件；
// trace 携带关联上下文（request_id/trace_parent），贯穿 Relayer 与 Consumer。
func (s *Store) CreateOrder(ctx context.Context, userID, sku string, idemKey, paramHash string, trace event.TraceContext) (*CreatedOrder, error) {
	var out *CreatedOrder
	err := withTxRetry(ctx, s.db, func(tx *gorm.DB) error {
		// 1. 幂等占位（含参数哈希）：同键第二次插入触发 1062，判定重放或冲突。
		dup, err := tryInsertIdem(tx, userID, idemKey, paramHash)
		if err != nil {
			return err
		}
		if dup {
			// 重放读不加锁：幂等行一旦提交即不可变，普通读即可；
			// FOR UPDATE 会与占位持有者形成锁序反转（inventory → idempotency）死锁。
			var idem Idempotency
			if err := tx.Where("user_id = ? AND idem_key = ?", userID, idemKey).
				First(&idem).Error; err != nil {
				return fmt.Errorf("load idempotency: %w", err)
			}
			if idem.ParamHash != paramHash {
				return ErrIdempotencyConf
			}
			o, err := getOrderByID(ctx, tx, idem.OrderID)
			if err != nil {
				return err
			}
			o.Replayed = true
			out = o
			return nil
		}

		// 2. 读取商品价格：业务模型中价格不可变（无改价接口，EXP-01 冻结），
		// 普通读即可，无需 FOR UPDATE 占用 P1 商品行热点锁（EXP-09 实测该锁
		// 在 200 TPS 单 SKU 热点下放大排队）。
		var p Product
		if err := tx.Where("sku = ?", sku).First(&p).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			return fmt.Errorf("get price: %w", err)
		}

		// 3. 建订单，并回填幂等记录的 order_id（同一事务）。
		o := OrderModel{
			UserID:         userID,
			Sku:            sku,
			Qty:            1,
			UnitPriceCents: p.PriceCents,
			Status:         order.StatusCreated,
		}
		if err := tx.Create(&o).Error; err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if err := tx.Model(&Idempotency{}).
			Where("user_id = ? AND idem_key = ?", userID, idemKey).
			Update("order_id", o.ID).Error; err != nil {
			return fmt.Errorf("backfill idempotency: %w", err)
		}

		// 4. Outbox 事件：与订单同事务提交（EXP-09）。
		// 事件 ID 在事务内生成，是 Kafka 消息 key 与 Inbox 去重键。
		if err := writeOutbox(tx, o.ID, event.OrderCreated{
			EventID:     event.NewID(),
			OrderID:     o.ID,
			UserID:      userID,
			SKU:         sku,
			RequestID:   trace.RequestID,
			TraceParent: trace.TraceParent,
			CreatedAt:   o.CreatedAt,
		}); err != nil {
			return err
		}

		// 5. 条件更新扣库存：放在事务最后，stock > 0 才允许扣减，避免超卖。
		// EXP-09 把热点行锁（inventory）的持有窗口压缩到「1 条 UPDATE + COMMIT」：
		// 此前顺序（库存第 2 步）在 200 TPS 单 SKU 热点下与 outbox INSERT 叠加，
		// 锁队列爆炸（实测下单 P50 5.7s、连接池等待 22 万次）。
		// 锁顺序保持全局一致（idempotency → … → inventory），无死锁序反转。
		res := tx.Model(&Inventory{}).
			Where("sku = ? AND stock > 0", sku).
			UpdateColumn("stock", gorm.Expr("stock - 1"))
		if res.Error != nil {
			return fmt.Errorf("deduct stock: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrOutOfStock
		}
		out = toCreatedOrder(o, false)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// tryInsertIdem 尝试插入幂等占位记录（含参数哈希，提交后即可判定重放/冲突）。
// 返回 dup=true 表示该键已存在（并发或历史提交），由调用方判定重放/冲突。
func tryInsertIdem(tx *gorm.DB, userID, idemKey, paramHash string) (bool, error) {
	err := tx.Create(&Idempotency{UserID: userID, IdemKey: idemKey, ParamHash: paramHash}).Error
	if err == nil {
		return false, nil
	}
	if isDuplicate(err) {
		return true, nil
	}
	return false, fmt.Errorf("insert idempotency: %w", err)
}

// withTxRetry 在死锁/锁等待时有限重试事务（EXP-03 冻结：最多 3 次）。
// EXP-11 安全重试口径：只在明确可重试错误（1213 死锁 / 1205 锁等待）上重试，
// 指数退避＋随机抖动（避免并发重试同步撞击热点行），重试总时长受调用方
// context 总预算约束（预算耗尽即停止，重试不能突破端到端截止时间）。
func withTxRetry(ctx context.Context, db *gorm.DB, fn func(tx *gorm.DB) error) error {
	const maxRetries = 3
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// 退避基期 25ms × 2^(attempt-1)，抖动 ±50%：attempt=1 → 12.5~37.5ms，
			// attempt=2 → 25~75ms，attempt=3 → 50~150ms。总等待上界 ~260ms，
			// 远小于写路径 1s 总预算，重试不会自我放大。
			base := 25 * time.Millisecond * (1 << (attempt - 1))
			jitter := time.Duration(rand.Int64N(int64(base))) // [0, base)
			backoff := base/2 + jitter
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		lastErr = db.WithContext(ctx).Transaction(fn)
		if lastErr == nil || !isRetryable(lastErr) {
			return lastErr
		}
		RecordTxRetry()
	}
	return lastErr
}

// isRetryable 判断 MySQL 死锁（1213）与锁等待超时（1205）。
func isRetryable(err error) bool {
	var me *sqldriver.MySQLError
	return errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205)
}

// GetOrdersByCursor 游标分页查询用户订单列表（ADR-011）。
// 锚点：(created_at, id) 双字段，避免同时间戳下的重复/遗漏。
// 返回 limit 条，按 created_at DESC, id DESC 排序；hasMore 表示是否还有下一页。
func (s *Store) GetOrdersByCursor(ctx context.Context, userID string, beforeCreatedAt *time.Time, beforeID *int64, limit int) ([]CreatedOrder, bool, error) {
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	q := s.db.WithContext(ctx).Where("user_id = ?", userID)
	if beforeCreatedAt != nil && beforeID != nil {
		q = q.Where(
			"(created_at < ? OR (created_at = ? AND id < ?))",
			*beforeCreatedAt, *beforeCreatedAt, *beforeID,
		)
	}
	var rows []OrderModel
	if err := q.Order("created_at DESC, id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("list orders: %w", err)
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	out := make([]CreatedOrder, 0, len(rows))
	for _, o := range rows {
		out = append(out, *toCreatedOrder(o, false))
	}
	return out, hasMore, nil
}

// GetLatestOrder 查询用户最新订单（EXP-02 简化版，EXP-06 做索引优化）。
func (s *Store) GetLatestOrder(ctx context.Context, userID string) (*CreatedOrder, error) {
	var o OrderModel
	if err := s.db.WithContext(ctx).Where("user_id = ?", userID).
		Order("created_at DESC, id DESC").First(&o).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get latest order: %w", err)
	}
	return toCreatedOrder(o, false), nil
}

func getOrderByID(ctx context.Context, db *gorm.DB, id int64) (*CreatedOrder, error) {
	var o OrderModel
	if err := db.WithContext(ctx).Where("id = ?", id).First(&o).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get order by id: %w", err)
	}
	return toCreatedOrder(o, false), nil
}

func toCreatedOrder(o OrderModel, replayed bool) *CreatedOrder {
	return &CreatedOrder{
		ID:             o.ID,
		UserID:         o.UserID,
		SKU:            o.Sku,
		Qty:            o.Qty,
		UnitPriceCents: o.UnitPriceCents,
		Status:         string(o.Status),
		CreatedAt:      o.CreatedAt,
		Replayed:       replayed,
	}
}

// isDuplicate 判断 MySQL 唯一键冲突（1062）。
func isDuplicate(err error) bool {
	var me *sqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}
