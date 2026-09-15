// EXP-04 容量基线：阶梯提升到达率，找到满足 SLO 的最大稳定 QPS。
// 开放模型（ramping-arrival-rate）：施压端不因服务变慢而减少请求。
// 请求混合 8:1:1（商品/库存查询 80%、下单 10%、订单查询 10%）。
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 50, duration: '1m' },
        { target: 100, duration: '1m' },
        { target: 200, duration: '1m' },
        { target: 400, duration: '1m' },
        { target: 800, duration: '1m' },
        { target: 1000, duration: '1m' },
      ],
    },
  },
  thresholds: {
    'http_req_failed{kind:read}': ['rate<0.01'],
    'http_req_failed{kind:write}': ['rate<0.01'],
    'http_req_duration{kind:read}': ['p(99)<200'],
    'http_req_duration{kind:write}': ['p(99)<500'],
  },
};

export default function () {
  const r = Math.random();
  if (r < 0.8) {
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const path = r < 0.4 ? `/api/products/${sku}` : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`, { tags: { kind: 'read' } });
    check(res, { 'read 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.9) {
    const key = `base-${__VU}-${__ITER}`;
    // 写压测热点 SKU：库存由压测前重置为大值，避免库存耗尽让 409 淹没真实写延迟
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `base-u-${__VU}`, sku: 'P1', qty: 1 }),
      {
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        tags: { kind: 'write' },
      }
    );
    // 201 新建 / 200 重放 / 409 库存不足或冲突均为业务可接受，5xx 才算系统错误
    check(res, { 'order accepted': (x) => [200, 201, 409].includes(x.status) });
    return;
  }
  // VU 1 一定会产生订单（写流），用固定用户保证查询有结果；查询本身允许 404。
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=base-u-1`, { tags: { kind: 'read' } });
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}
