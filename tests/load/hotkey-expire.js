// EXP-08 热点过期对照：读流量集中到少量热点 SKU（默认 P1..P5），
// 靠自然 TTL 过期制造"过期风暴"，观察请求合并（singleflight）对回源次数的压缩。
//
// 与 cache-product.js 的区别：
//   - 纯读（无下单），避免下单 DEL 失效干扰"自然过期"这一单一变量；
//   - SKU 集合极小（5 个），过期瞬间同一 Key 的并发 miss 足够多，合并效果可测。
//
// 环境变量：
//   BASE_URL 服务地址；RATE 到达率（默认 2000）；DURATION 时长（默认 3m）；HOT 热点数（默认 5）
//
// 对照方法（见 EXP-08-manual）：
//   CACHE_COALESCE_ENABLED=true/false 各跑一轮，对比 X-Cache miss 计数
//   与 cache_backfill_wanted_total / cache_backfill_executed_total。
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const RATE = parseInt(__ENV.RATE || '2000', 10);
const DURATION = __ENV.DURATION || '3m';
const HOT = parseInt(__ENV.HOT || '5', 10);

const cHit = new Counter('cache_hits');
const cNeg = new Counter('cache_neg');
const cMiss = new Counter('cache_misses');
const cStale = new Counter('cache_stale');
const cReject = new Counter('cache_reject');

export const options = {
  scenarios: {
    constant: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 800,
      maxVUs: 2000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

export default function () {
  const sku = `P${Math.floor(Math.random() * HOT) + 1}`;
  const path = Math.random() < 0.5
    ? `/api/products/${sku}`
    : `/api/products/${sku}/stock`;
  const res = http.get(`${BASE_URL}${path}`);
  check(res, { 'read 2xx': (x) => x.status === 200 });
  count(res);
}

function count(res) {
  switch (res.headers['X-Cache']) {
    case 'hit':
      cHit.add(1);
      break;
    case 'neg':
      cNeg.add(1);
      break;
    case 'stale':
      cStale.add(1);
      break;
    case 'reject':
      cReject.add(1);
      break;
    default:
      cMiss.add(1);
  }
}
