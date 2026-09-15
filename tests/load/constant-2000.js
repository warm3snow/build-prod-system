// EXP-05 恒定 2000 QPS 负载：让 order-api CPU 持续饱和，供 pprof 抓热点。
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';

export const options = {
  scenarios: {
    constant: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '3m',
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
    const sku = `P${Math.floor(Math.random() * 10000) + 1}`;
    const path = r < 0.4 ? `/api/products/${sku}` : `/api/products/${sku}/stock`;
    const res = http.get(`${BASE_URL}${path}`);
    check(res, { 'read 2xx': (x) => x.status === 200 });
    return;
  }
  if (r < 0.9) {
    const key = `hot-${__VU}-${__ITER}`;
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `hot-u-${__VU}`, sku: 'P1', qty: 1 }),
      { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } }
    );
    check(res, { 'order accepted': (x) => [200, 201, 409].includes(x.status) });
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=hot-u-1`);
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}
