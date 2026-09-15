package order

import "testing"

// TestStockNeverNegative 验证库存守恒与不超卖。
func TestStockNeverNegative(t *testing.T) {
	inv := Inventory{Sku: "P1", Stock: 10}
	orders := map[string]Order{}
	var nextID int64

	for i := 0; i < 4; i++ {
		var res PlaceOrderResult
		inv, res = TryPlaceOrder(inv, orders, PlaceOrderParams{
			UserID: "u1", Sku: "P1", Qty: 1, IdempotencyKey: "k-" + string(rune('a'+i)),
		}, &nextID)
		if res.Err != NoErr {
			t.Fatalf("order %d failed: %v", i, res.ErrMsg)
		}
	}
	if inv.Stock != 6 {
		t.Fatalf("expected stock 6, got %d", inv.Stock)
	}
	// 库存守恒：初始 10 = 剩余 6 + 订单 4
	if 10 != inv.Stock+len(orders) {
		t.Fatalf("stock conservation violated: %d != %d+%d", 10, inv.Stock, len(orders))
	}
	// 超卖拒绝
	inv2, res := TryPlaceOrder(Inventory{Sku: "P1", Stock: 0}, orders, PlaceOrderParams{
		UserID: "u9", Sku: "P1", Qty: 1, IdempotencyKey: "k9",
	}, &nextID)
	if res.Err != ErrOutOfStock {
		t.Fatalf("expected out of stock, got %v", res.Err)
	}
	if inv2.Stock != 0 {
		t.Fatalf("stock should be unchanged, got %d", inv2.Stock)
	}
}

// TestIdempotentReplay 同一幂等键重复提交返回同一订单。
func TestIdempotentReplay(t *testing.T) {
	inv := Inventory{Sku: "P1", Stock: 3}
	orders := map[string]Order{}
	var nextID int64
	p := PlaceOrderParams{UserID: "u1", Sku: "P1", Qty: 1, IdempotencyKey: "k-1"}

	inv, first := TryPlaceOrder(inv, orders, p, &nextID)
	if first.Err != NoErr || first.Replayed {
		t.Fatalf("first order unexpected: %+v", first)
	}
	inv, second := TryPlaceOrder(inv, orders, p, &nextID)
	if second.Err != NoErr || !second.Replayed {
		t.Fatalf("expected idempotent replay, got %+v", second)
	}
	if first.Order.ID != second.Order.ID {
		t.Fatalf("replay returned different order: %d vs %d", first.Order.ID, second.Order.ID)
	}
	if inv.Stock != 2 {
		t.Fatalf("replay must not deduct stock twice, got %d", inv.Stock)
	}
}

// TestIdempotencyConflict 同键不同参数必须拒绝。
func TestIdempotencyConflict(t *testing.T) {
	inv := Inventory{Sku: "P1", Stock: 3}
	orders := map[string]Order{}
	var nextID int64

	_, _ = TryPlaceOrder(inv, orders, PlaceOrderParams{
		UserID: "u1", Sku: "P1", Qty: 1, IdempotencyKey: "k-1",
	}, &nextID)
	_, res := TryPlaceOrder(inv, orders, PlaceOrderParams{
		UserID: "u1", Sku: "P2", Qty: 1, IdempotencyKey: "k-1",
	}, &nextID)
	if res.Err != ErrIdempotencyConflict {
		t.Fatalf("expected conflict, got %v", res.Err)
	}
}
