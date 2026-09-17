// EXP-07 缓存预热：对热点 SKU 集合（P1..P100）的商品与库存各请求一次，
// 在正式压测前填充缓存，制造"热缓存"工况。
// 用法：k6 run --vus 10 tests/load/cache-warmup.js（单次遍历，几秒内完成）
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const SKU_COUNT = parseInt(__ENV.SKU_COUNT || '100', 10);

export const options = {
  vus: 10,
  iterations: SKU_COUNT * 2,
};

export default function () {
  // 每 VU 轮流取 SKU：偶数迭代查商品，奇数迭代查库存，保证全部 SKU 覆盖。
  const n = __ITER * 10 + __VU; // 展开为线性下标，避免重复
  const sku = `P${(n % SKU_COUNT) + 1}`;
  const path = Math.floor(n / SKU_COUNT) % 2 === 0
    ? `/api/products/${sku}`
    : `/api/products/${sku}/stock`;
  const res = http.get(`${BASE_URL}${path}`);
  check(res, { 'warmup 2xx': (x) => x.status === 200 });
}
