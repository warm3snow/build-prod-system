// EXP-13 固定副本容量曲线：固定到达率 × 8:1:1 混合，测量各副本数下的成功吞吐与 SLO。
// 不带客户端重试（容量测量口径：到达率 → 分类结果，重试会污染「成功业务量」统计）。
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || '60s';

export const options = {
  scenarios: {
    fixed: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 400,
      maxVUs: 3000,
    },
  },
  thresholds: {
    dropped_iterations: ['count==0'],
    'http_req_duration{kind:read}': ['p(99)<200'],
    'http_req_duration{kind:write}': ['p(99)<500'],
  },
};

export default function () {
  const r = Math.random();
  // READ_ONLY=1：纯读容量测量（隔离写路径瓶颈，验证「读扩容有效」）
  if (__ENV.READ_ONLY === '1') {
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const res = http.get(`${BASE_URL}/api/products/${sku}`, { tags: { kind: 'read' } });
    check(res, { 'read 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.8) {
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const path = r < 0.4 ? `/api/products/${sku}` : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`, { tags: { kind: 'read' } });
    check(res, { 'read 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.9) {
    const key = `fr-${__VU}-${__ITER}`;
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `fr-u-${__VU}`, sku: 'P1', qty: 1 }),
      {
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        tags: { kind: 'write' },
      }
    );
    check(res, { 'order accepted': (x) => [200, 201, 409].includes(x.status) });
    // EXP-18 成功响应账本：客户端已收到成功响应的订单逐单记录（200 重放与 201 首发
    // 指向同一 order_id，对账器按 id 去重）。输出经 nohup 日志落盘，事后提取到
    // 故障域外——「成功订单不丢」核验的判定性证据。
    if (__ENV.LEDGER === '1' && (res.status === 200 || res.status === 201)) {
      const o = res.json();
      console.log(`LEDGER|${o.id}|fr-u-${__VU}|P1|${key}|${Date.now()}`);
    }
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=fr-u-1`, {
    tags: { kind: 'read' },
  });
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}
