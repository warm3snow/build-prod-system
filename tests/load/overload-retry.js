// EXP-11 过载与安全重试：固定到达率超载 + 客户端有界重试。
//
// 客户端重试口径（EXP-11 冻结，与课程安全重试约定一致）：
//   - 只对可重试结果重试：网络超时（status 0）/ 5xx；429/409/400 等保护性/业务拒绝不重试；
//   - 幂等键跨重试不变（原键重试，由服务端幂等吸收重复提交）；
//   - 最多 2 次重试，指数退避 50ms×2^n + 随机抖动，总重试时长有上限；
//   - 读请求不重试（读重试放大高、收益低，课程口径：只对幂等写操作重试）。
//
// 重试放大率口径：http_reqs（实际发出）/ iterations（到达率调度次数）。
// 到达率（RATE）是开放模型：施压端不因服务变慢而减少请求，dropped_iterations 必须为 0。
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const RATE = Number(__ENV.RATE || 3000);
const DURATION = __ENV.DURATION || '2m';
// RETRY=0 关闭客户端重试（对照实验：观察无重试时的失败形态）。
const RETRY = __ENV.RETRY !== '0';
// 写请求客户端超时（场景 B 需要 > 服务端写预算 1s，让 504 真实到达客户端分类）。
const WRITE_TIMEOUT = __ENV.WRITE_TIMEOUT || '800ms';

export const options = {
  scenarios: {
    overload: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 800,
      maxVUs: 2000,
    },
  },
  thresholds: {
    dropped_iterations: ['count==0'], // 压测机必须真实发出全部迭代，否则到达率口径失真
  },
};

function postOrder(key) {
  return http.post(
    `${BASE_URL}/api/orders`,
    JSON.stringify({ user_id: `ov-${__VU}`, sku: 'P1', qty: 1 }),
    {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
      timeout: WRITE_TIMEOUT,
      tags: { kind: 'write' },
    }
  );
}

export default function () {
  const key = `exp11-${__VU}-${__ITER}`;
  const r = Math.random();
  if (r < 0.8) {
    // 读路径：不重试（只对幂等写操作启用客户端重试）。
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const path = r < 0.4 ? `/api/products/${sku}` : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`, { timeout: '400ms', tags: { kind: 'read' } });
    check(res, { 'read 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.9) {
    let res = postOrder(key);
    if (RETRY) {
      for (let attempt = 1; attempt <= 2; attempt++) {
        const retryable = res.status >= 500 || res.status === 0;
        if (!retryable) break;
        // 指数退避 + 抖动：50ms×2^(attempt-1) × U[0.5,1.5]
        sleep(0.05 * Math.pow(2, attempt - 1) * (0.5 + Math.random()));
        res = postOrder(key);
      }
    }
    // 最终结果允许：201 新建 / 200 重放 / 409 业务拒绝 / 429 限流 / 503 反压或过载 / 504 超时
    check(res, {
      'order final classified': (x) => [200, 201, 409, 429, 503, 504].includes(x.status),
    });
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=ov-1`, {
    timeout: '400ms',
    tags: { kind: 'read' },
  });
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}
