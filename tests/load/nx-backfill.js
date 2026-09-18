// EXP-08 场景 D：负缓存对不存在 ID 的回源效果。
// 对 NX-1..NX-N 每个不存在 SKU 请求两次：第一次回源（404 miss，写负缓存），
// 第二次负缓存命中（404 neg，不打 DB）。验证负缓存把"穿透风暴"限制为每键一次回源。
//
// 环境变量：BASE_URL、NX_COUNT（默认 1000）
// 对照：统计 DB Com_select 增量应 ≈ NX_COUNT（每键一次回源），
// 且 backfill executed ≈ wanted/2（第二次全部被负缓存吸收）。
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';
const NX_COUNT = parseInt(__ENV.NX_COUNT || '1000', 10);

const cMiss = new Counter('cache_misses');
const cNeg = new Counter('cache_neg');
const cReject = new Counter('cache_reject');

export const options = {
  scenarios: {
    shared: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: NX_COUNT * 2,
    },
  },
};

export default function () {
  // 前 NX_COUNT 次迭代是第一轮（每键第一次），后 NX_COUNT 次是第二轮。
  const n = __ITER < NX_COUNT ? __ITER : __ITER - NX_COUNT;
  const sku = `NX-${n + 1}`;
  const res = http.get(`${BASE_URL}/api/products/${sku}`);
  check(res, { '404 as expected': (x) => x.status === 404 });
  switch (res.headers['X-Cache']) {
    case 'neg':
      cNeg.add(1);
      break;
    case 'reject':
      cReject.add(1);
      break;
    default:
      cMiss.add(1);
  }
}
