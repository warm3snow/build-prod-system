// EXP-07 缓存对照压测：固定热点 SKU 集合（默认 P1..P100），8:1:1 读写混合。
// 与 constant-2000 的区别：SKU 从大范围随机收敛到固定小集合，保证热缓存高命中。
// 通过 X-Cache 响应头统计命中/未命中，直接观察缓存收益与冷热差异。
//
// 环境变量：
//   BASE_URL  服务地址（默认集群内 DNS）
//   RATE      到达率 QPS（默认 2000）
//   DURATION  时长（默认 3m）
//   SKU_COUNT 热点 SKU 数量（默认 100）
//
// 冷缓存对照：运行前 FLUSHDB（见 EXP-07-manual）；热缓存对照：先跑 cache-warmup.js。
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const RATE = parseInt(__ENV.RATE || '2000', 10);
const DURATION = __ENV.DURATION || '3m';
const SKU_COUNT = parseInt(__ENV.SKU_COUNT || '100', 10);

const cacheHit = new Counter('cache_hits');
const cacheNeg = new Counter('cache_neg');
const cacheMiss = new Counter('cache_misses');
const cacheOff = new Counter('cache_off');

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
  const r = Math.random();
  if (r < 0.8) {
    const sku = `P${Math.floor(Math.random() * SKU_COUNT) + 1}`;
    const path = r < 0.4 ? `/api/products/${sku}` : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`);
    check(res, { 'read 2xx': (x) => x.status === 200 });
    countCache(res);
    return;
  }
  if (r < 0.9) {
    const key = `cache-${__VU}-${__ITER}`;
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `cache-u-${__VU}`, sku: 'P1', qty: 1 }),
      { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } }
    );
    check(res, { 'order accepted': (x) => [200, 201, 409].includes(x.status) });
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=cache-u-1`);
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}

function countCache(res) {
  switch (res.headers['X-Cache']) {
    case 'hit':
      cacheHit.add(1);
      break;
    case 'neg':
      cacheNeg.add(1);
      break;
    case 'off':
      cacheOff.add(1);
      break;
    default:
      cacheMiss.add(1);
  }
}
