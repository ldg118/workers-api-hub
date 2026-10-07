# Workers API Hub

**Workers API Hub** 是跑在 Cloudflare Workers 上的单文件 ES Module：把 Cloudflare Workers AI 账号池与第三方 OpenAI 兼容渠道（OpenRouter、Gemini 免费层等）统一转换成 **OpenAI / Anthropic 双协议**接口。零依赖，前端全内联。

> **适用场景**：不想买 VPS / 不想维护服务器，但需要代理海外模型（国内无法直连 OpenRouter / Gemini / OpenAI）。Workers 免费额度够开发用，免运维。

## 界面预览

**登录落地页**

![登录落地页](picture/landing-dark.png)

**管理面板 · 数据看板（第三方渠道调用统计）**

![数据看板](picture/dashboard-dark-1.png)

**管理面板 · 概览（CF 账号池用量总览）**

![概览](picture/dashboard-dark-2.png)

---

## ✅ 快速部署（3 步）

### 1. 准备 Cloudflare 账号

创建 3 个东西（都是免费额度够开发）：
- **Worker**：Workers & Pages → Create → Workers
- **KV Namespace**：Workers → Storage & Databases → Create namespace（命名空间名填 `KV`）
- **D1 Database**（可选）：不用配额组 / 看板统计可以不建；命名空间名填 `DB`

### 2. 绑定与配置

| 类型 | 名称 | 必填 | 说明 |
|---|---|---|---|
| KV Namespace | `KV` | ✅ | 主配置、配额调度冷却、缓存；未绑会显示自带错误页 |
| 环境变量（Text） | `ADMIN_PASSWORD` | ✅ | 后台 `/admin` 登录密码；未设会显示自带错误页 |
| D1 Database | `DB` | ⚠️ | 用配额组或看板统计时必须绑；不绑的话配额组里设了次数上限的成员会报错拒绝请求，看板不显示。纯反代场景可以不绑 |

### 3. 上传部署

Workers & Pages → Pages → Create → Direct Upload → 填项目名 → Create project → 直接把 `_worker.js` 拖到上传区域 → 选环境（生产/预览）→ 保存并部署。

> ⚠️ **别用 Workers → Quick Edit 粘贴** `_worker.js`，文件 10000+ 行容易截断导致报 `Error 1101`。上面这条路径没这问题。

### 部署后自检

部署完打开 `https://<你的域名>/version`，返回的 `build` 值要和代码里 `BUILD_ID` 对得上——对不上就是部署没生效，白排查。

---

## 功能说明

### 1. 双协议网关
- 把所有上游统一收敛为 **OpenAI 兼容接口**（`/v1/chat/completions`、`/v1/models` 等）。
- 同时提供 **Anthropic 协议**转换（消息格式、流式 SSE 在两端互转），客户端可任选其一接入。
- 支持流式与非流式；内置 SSE 心跳保活以规避 Cloudflare 边缘的流式超时。

### 2. 多上游接入
- **第三方渠道反代**（主要用途）：填写 OpenAI 兼容端点的 baseUrl + API Key 即可，如 OpenRouter、Gemini 免费层。
- **Cloudflare Workers AI 账号池**（`@cf/`）：多账号随机容错。

### 3. 模型路由与命名空间
请求模型名按前缀分派，互不冲突：

| 写法 | 含义 |
|---|---|
| `@cf/<模型>` / `cf/<模型>` | 走 CF 账号池 |
| `provider:<渠道>/<模型>` 或 `<渠道>/<模型>` | 直指某渠道的某模型 |
| `TT:<组名>` | 走配额组（见下） |
| 映射表里的自定义名 | 由模型映射决定落点 |
| 裸模型名 | 若设了「默认渠道」则原样转发；否则明确报错 |

- 未知模型一律显式返回 400，不会静默回落到任何默认模型。

### 4. 模型映射
把客户端使用的模型名映射到上游真实模型（具体模型 / 配额组 / CF 模型均可）。无效映射会在管理面板标红并显式报错。

### 5. 调用配额组（负载均衡）`TT:<组名>`
将一个组内的多个上游成员当作虚拟模型对外暴露，自动做负载均衡、故障冷却、换成员重试、会话粘性（可选）。

### 6. Gemini 原生适配
Gemini 原生协议已适配，支持 tools（函数调用）与流式。不走其 OpenAI 兼容端点，而是调用原生接口并做协议转换。

### 7. 管理面板（内置 SPA）
访问 `/admin` 登录后使用，按任务分组：
- **概览**：CF 账号池用量查询与刷新；
- **代理接入**：API 密钥管理、第三方渠道配置；
- **路由**：模型映射、调用配额组。

### 8. 统计看板（D1）
绑定 D1 后自动记录每渠道 / 每模型的请求数、成功失败、延迟、token 用量，按小时聚合。未绑 D1 时看板不显示，代理照常。

### 9. 鉴权与安全
后台 `/admin` 由 `ADMIN_PASSWORD` 保护。可在管理面板生成子密钥，与系统默认密钥隔离。

---

## 常见排障

- **显示自带错误页**（KV 缺失 / 密码未设）→ 绑定没配全，按「快速部署 → 绑定与配置」补齐。
- **配额组报错"统计不到已用次数"** → 有次数上限的组没绑 D1，必须绑。
- **`/` 返回 404** → Worker 路由没配到这个域名/路径。
- **`origin_bad_gateway` 502**（带 `cloudflare_error:true`）→ CF 边缘生成，上游响应无效；长流式注意 CF Free 约 100s 源站超时。
- **上游 4xx 原样透出**（带 `Access-Control-Allow-Origin`）= Worker 返回；只有 `CF-RAY` + `Cache-Control:private` = CF 裸错误页。

---

<details>
<summary><strong>附录：高级配置 & 内部细节（开发/排障参考）</strong></summary>

一般不用动，留着给以后自己排查用。

### 可选环境变量（默认值合理）

Worker → Settings → Variables → 添加同名变量覆盖；不设就走代码内置默认值。都不是密钥，标记「未加密」即可。

| 变量 | 默认 | 说明 |
|---|---|---|
| `THIRD_PARTY_EST_COST_PER_1K` | `0.011` | 看板"估算成本"单价（美元/千 token），纯示意 |
| `COOLDOWN_CACHE_MS` | `5000` | 配额调度冷却表的 isolate 内缓存时长 |
| `QUOTA_QUERY_CACHE_MS` | `5000` | 配额分流 D1 查询的 isolate 内缓存时长 |

### KV 内部 key 清单（代码自己管理，别手动写）

| Key | 说明 |
|---|---|
| `config` | 主配置（accounts / apiKeys / providers / customModelMap / quotaGroups / 系统密钥轮换状态等） |
| `cooldowns` | 配额调度的成员冷却表 |
| `cache_usage_summary` | CF 账号池用量汇总缓存 |
| `cache_usage_details` | CF 账号池各账号用量明细缓存 |

### D1 `stats` 表

首次调用自动建表，按小时桶聚合。主键 `(day, provider_id, model)`，字段涵盖请求数、成功失败、延迟、token 用量、探测调用分开计数。

### Gemini 内部实现

tools → functionDeclarations 转换、工具参数 schema 白名单清洗、thoughtSignature 用于工具调用回放、原生流式回包转 OpenAI 格式。

### 看板统计口径

- 小时桶（`2026-10-07T13`）而非纯日期；
- 探测调用（probe）计入真实调用配额；
- 平均延迟 = 本小时桶内总耗时 / 总请求数；
- 成功调用记首字节时间（TTFB），失败记全程耗时；
- 老表自动补列：首次调用跑 ALTER TABLE ADD COLUMN。

</details>

---

## 参考项目

本项目在开发过程中参考了以下开源项目，特此致谢：

- [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api)
- [cmliussss2024/WorkersAI2API](https://github.com/cmliussss2024/WorkersAI2API)
