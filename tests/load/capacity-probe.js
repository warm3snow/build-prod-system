// EXP-04 容量上限探测：从 1000 QPS 快速爬升到 5000 QPS，定位首个瓶颈。
// 注意：经 kubectl port-forward 施压时，端口转发本身可能先成为瓶颈，结果需对照 Pod 指标。
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';

export const options = {
  scenarios: {
    probe: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 4000,
      stages: [
        { target: 1000, duration: '30s' },
        { target: 2000, duration: '30s' },
        { target: 3000, duration: '30s' },
        { target: 4000, duration: '30s' },
        { target: 5000, duration: '30s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
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
    const key = `probe-${__VU}-${__ITER}`;
    const res = http.post(
      `${BASE_URL}/api/orders`,
      JSON.stringify({ user_id: `probe-u-${__VU}`, sku: 'P1', qty: 1 }),
      { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } }
    );
    check(res, { 'order accepted': (x) => [200, 201, 409].includes(x.status) });
    return;
  }
  const res = http.get(`${BASE_URL}/api/orders/latest?user_id=probe-u-1`);
  check(res, { 'latest 2xx/404': (x) => [200, 404].includes(x.status) });
}
