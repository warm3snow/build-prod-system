// EXP-08 场景 D 聚焦实验：100 个不同 NZ key、100 VU 并发、单轮请求。
// 每个 key 只有一次请求，不存在同 key 合并——观察 wanted/executed 是否为 1:1，
// 用于区分"负缓存吸收"与"请求合并"各自对回源压缩的贡献。
import http from 'k6/http';

const BASE_URL = __ENV.BASE_URL || 'http://order-api.order-lab.svc.cluster.local:8080';

export const options = {
  scenarios: {
    s: {
      executor: 'shared-iterations',
      vus: 100,
      iterations: 100,
    },
  },
};

export default function () {
  http.get(`${BASE_URL}/api/products/NZ-${__ITER + 1}`);
}
