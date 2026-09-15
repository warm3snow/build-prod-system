// k6 冒烟脚本（EXP-01 骨架，EXP-02 部署后可运行，EXP-04 冻结完整负载档）。
// 固定到达率使用开放模型，避免服务变慢时压测端同步减少施压（coordinated omission）。
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    smoke_open_model: {
      executor: 'constant-arrival-rate',
      rate: 10,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 20,
      maxVUs: 20,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<500'],
  },
};

// 请求混合：80% 读（商品/库存查询）/ 10% 下单 / 10% 订单查询（见 experiment-parameters.md）。
export default function () {
  const r = Math.random();
  if (r < 0.8) {
    const sku = Math.floor(Math.random() * 10000) + 1;
    const path = r < 0.4
      ? `/api/products/${sku}`
      : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`);
    check(res, { 'read ok': (r2) => r2.status === 200 });
    return;
  }
  if (r < 0.9) {
    // 幂等键：同一 VU 内按迭代号生成，重试必须复用同一 key。
    const key = `load-${__VU}-${__ITER}`;
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `load-${__VU}`, sku: 'P1', qty: 1 }),
      { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } }
    );
    // 201 新建、200 幂等重放、409 库存不足或冲突，均属业务可接受；5xx 才算系统错误。
    check(res, { 'order accepted': (r2) => [200, 201, 409].includes(r2.status) });
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=load-${__VU}`);
  check(res, { 'order query ok': (r2) => r2.status === 200 });
}
