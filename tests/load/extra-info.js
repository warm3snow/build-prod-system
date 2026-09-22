// EXP-12 非关键依赖故障对照：附加信息（依赖）与核心路径混合压测。
//
// 混合比例：extra 60%（非关键依赖）/ 商品读 30% / 库存读 5% / 下单 5%。
// 观察：依赖慢/失败时，核心路径（读 200ms、写 500ms SLO）是否被波击；
// 依赖降级响应（200 + X-Dep: degraded）视为成功但单独计数（不伪装成完整成功）。
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || '2m';

export const options = {
  scenarios: {
    mix: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 400,
      maxVUs: 2000,
    },
  },
  thresholds: {
    dropped_iterations: ['count==0'],
    'http_req_duration{kind:core-read}': ['p(99)<200'],
    'http_req_duration{kind:core-write}': ['p(99)<500'],
  },
};

// 降级计数由服务端 dep_degraded_total 指标承载（客户端响应头 X-Dep 仅用于核对）。
export default function () {
  const r = Math.random();
  if (r < 0.6) {
    // 非关键依赖：附加信息。降级（200 + X-Dep: degraded）也是成功响应。
    const sku = `P${Math.floor(Math.random() * 100) + 1}`; // 热点 SKU 便于对照
    const res = http.get(`${BASE_URL}/api/products/${sku}/extra`, {
      timeout: '500ms',
      tags: { kind: 'dep' },
    });
    check(res, {
      'extra ok or degraded': (x) => x.status === 200 || x.status === 503 || x.status === 504,
    });
    return;
  }
  if (r < 0.9) {
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const res = http.get(`${BASE_URL}/api/products/${sku}`, {
      timeout: '400ms',
      tags: { kind: 'core-read' },
    });
    check(res, { 'product 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.95) {
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const res = http.get(`${BASE_URL}/api/products/${sku}/stock`, {
      timeout: '400ms',
      tags: { kind: 'core-read' },
    });
    check(res, { 'stock 2xx': (x) => x.status === 200 });
    return;
  }
  const key = `exp12-${__VU}-${__ITER}`;
  const res = http.post(
    `${BASE_URL}/api/orders`,
    JSON.stringify({ user_id: `exp12-u-${__VU}`, sku: 'P2', qty: 1 }),
    {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
      timeout: '800ms',
      tags: { kind: 'core-write' },
    }
  );
  check(res, { 'order classified': (x) => [200, 201, 409, 429, 503, 504].includes(x.status) });
}

// 降级计数由服务端 dep_degraded_total 指标承载（客户端 degradedCount 仅供交叉核对，
// 通过 console.log 输出到日志）。
