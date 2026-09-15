// Package order 定义 Flash Order 的核心领域类型与业务不变量。
// 这是 EXP-01 的可编程契约，后续实验的数据库表与接口语义都以此为准。
package order

import "fmt"

// 错误码：与 sla.md 的错误分类对应。
const (
	NoErr ErrCode = iota
	ErrOutOfStock
	ErrIdempotencyConflict
)

// ErrCode 业务错误码。
type ErrCode int

// Failure 业务失败：错误码＋可读信息。
type Failure struct {
	Code ErrCode
	Msg  string
}

func (f Failure) Error() string { return f.Msg }

// SKU 商品库存单元。
type SKU string

// Inventory 单 SKU 的库存。
// 不变量：Stock 永远 >= 0；初始值 = Stock + 已提交订单数。
type Inventory struct {
	Sku   SKU
	Stock int
}

// UnitPrice 单价，单位：分。后续如需货币运算，必须避免浮点。
type UnitPrice int64

// Order 订单（单 SKU、单件）。
type Order struct {
	ID               int64
	Sku              SKU
	Qty              int
	UserID           string
	IdempotencyKey   string
	Status           OrderStatus
	UnitPriceAtOrder UnitPrice
}

// OrderStatus 订单状态机：终态不因重复消息倒退。
type OrderStatus string

const (
	StatusCreated OrderStatus = "CREATED"
)

// PlaceOrderParams 下单参数。
type PlaceOrderParams struct {
	UserID string
	Sku    SKU
	Qty    int
	// IdempotencyKey 幂等键：同一 (UserID, Key) 的重复提交返回同一结果；
	// 同键但参数不同视为冲突。
	IdempotencyKey string
}

// PlaceOrderResult 下单结果。
type PlaceOrderResult struct {
	Order    *Order
	Err      ErrCode
	ErrMsg   string
	Replayed bool // 幂等重放返回既有结果
}

// Stock 查询库存。
func (inv Inventory) StockFor() int { return inv.Stock }

// TryPlaceOrder 尝试下单：扣库存、建订单、记录幂等，视为一个原子业务步骤。
//
// 规则（EXP-01 冻结）：
//  1. qty 必须为 1，否则返回 ErrOutOfStock（本实验阶段用错误码表示参数违规）。
//  2. 库存不足返回 ErrOutOfStock，库存与订单不变。
//  3. 同 (UserID, IdempotencyKey) 重复提交返回既有订单（幂等重放）。
//  4. 同键但参数不同返回 ErrIdempotencyConflict。
func TryPlaceOrder(
	inv Inventory,
	ordersByKey map[string]Order,
	p PlaceOrderParams,
	nextOrderID *int64,
) (Inventory, PlaceOrderResult) {
	if p.Qty != 1 {
		return inv, PlaceOrderResult{Err: ErrOutOfStock, ErrMsg: fmt.Sprintf("unsupported qty %d", p.Qty)}
	}
	key := p.UserID + "/" + p.IdempotencyKey
	if prev, ok := ordersByKey[key]; ok {
		if prev.Sku != p.Sku {
			return inv, PlaceOrderResult{Err: ErrIdempotencyConflict, ErrMsg: "same key with different params"}
		}
		return inv, PlaceOrderResult{Order: &prev, Replayed: true}
	}
	if inv.Sku != p.Sku {
		return inv, PlaceOrderResult{Err: ErrOutOfStock, ErrMsg: "unknown sku"}
	}
	if inv.Stock < 1 {
		return inv, PlaceOrderResult{Err: ErrOutOfStock, ErrMsg: "out of stock"}
	}
	*nextOrderID++
	o := Order{
		ID:             *nextOrderID,
		Sku:            p.Sku,
		Qty:            1,
		UserID:         p.UserID,
		IdempotencyKey: p.IdempotencyKey,
		Status:         StatusCreated,
	}
	ordersByKey[key] = o
	inv.Stock--
	return inv, PlaceOrderResult{Order: &o}
}
