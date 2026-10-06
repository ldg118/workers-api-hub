# EdgeLLM Gateway 部署说明

单文件 Cloudflare Worker `_worker.js`：把 Workers AI / 第三方 OpenAI 兼容渠道转成 **OpenAI / Anthropic 双协议**接口。零依赖，前端全内联，**部署 = 把这一个文件部署到 Cloudflare**。当前主用途是第三方渠道反向代理（OpenRouter、Gemini 免费层）。

> 版本号：当前 `BUILD_ID = 2026-10-06.60`，每次改动 +1，部署后 `curl /version` 核对。

## 功能说明

本网关除基础的反代转发外，还内置了以下完整功能模块：

### 1. 双协议网关
- 把所有上游统一收敛为 **OpenAI 兼容接口**（`/v1/chat/completions`、`/v1/models` 等）。
- 同时提供 **Anthropic 协议**转换（消息格式、流式 SSE 在两端互转），客户端可任选其一接入。
- 支持流式（Server-Sent Events）与非流式；内置 SSE 心跳保活以规避 Cloudflare 边缘的流式超时。

### 2. 多上游接入
- **第三方渠道反代**（主要用途）：填写 OpenAI 兼容端点的 baseUrl + API Key 即可，如 OpenRouter、Gemini 免费层。
- **Cloudflare Workers AI 账号池**（`@cf/`）：多账号随机容错，本项目当前基本未启用。

### 3. 模型路由与命名空间
请求模型名按前缀分派，互不冲突：

| 写法 | 含义 |
|---|---|
| `@cf/<模型>` / `cf/<模型>` | 走 CF 账号池 |
| `provider:<渠道>/<模型>` 或 `<渠道>/<模型>` | 直指某渠道的某模型 |
| `TT:<组名>` | 走配额组（见下） |
| 映射表里的自定义名 | 由模型映射决定落点 |
| 裸模型名 | 若设了「默认渠道」则原样转发到该渠道；否则明确报错 |

- 未知模型一律显式返回 400，不会静默回落到任何默认模型。

### 4. 模型映射
- 在「模型映射」里把**客户端使用的模型名**映射到**上游真实模型**（具体模型 / 配额组 / CF 模型均可）。
- 无效映射会在管理面板标红并显式报错，不会悄悄失效。

### 5. 调用配额组（负载均衡）`TT:<组名>`
将一个组内的多个上游成员（不同 key / 不同模型）当作一个虚拟模型对外暴露，自动做：
- **负载均衡**：按本小时用量 + 在途请求挑选最闲成员；
- **冷却**：429 / 5xx / 超时等错误自动冷却一段时间再启用；
- **换成员重试**：单次请求在组内自动换成员至多 3 次；
- **会话粘性**（可选）：同一会话固定落在同一成员；
- **周期重置**：按小时桶切分用量窗口，支持「当地 / UTC / 北京时间」三种重置时刻写法。

### 6. Gemini 原生适配层
- Gemini 不走其 OpenAI 兼容端点（对 tools 支持不全），而是直接调用原生 `generateContent` 并自己做协议转换。
- 处理 tools → `functionDeclarations` 转换、工具参数 schema 的白名单式清洗、工具调用回放所需的 `thoughtSignature`、以及原生流式回包转 OpenAI 格式。

### 7. 管理面板（内置 SPA）
访问 `/admin` 登录后使用，按任务分组：
- **概览**（CF 账号池相关，本项目基本不用的部分）；
- **代理接入**：API 密钥管理、第三方渠道配置；
- **路由**：模型映射、调用配额组。

### 8. 统计看板（D1，可选）
- 绑定 D1 `DB` 后记录每渠道 / 每模型的请求数、成功失败、延迟、token 用量。
- 统计按**小时桶**切分，使非 UTC 零点周期边界准确；探测调用（probe）计入配额用量。
- 未绑定 D1 时优雅降级，代理照常、仅看板不显示。

### 9. 鉴权与安全
- 后台 `/admin` 由 `ADMIN_PASSWORD` 保护。
- 「接入信息 → API 密钥」可生成**你自己的子密钥**，与系统默认密钥隔离；建议部署后重新生成系统默认密钥并替换为强密钥。

## 必要绑定

| 类型 | 名称 | 用途 |
|---|---|---|
| KV | `KV` | 单键 `config`（主配置）+ 单键 `cooldowns`（调度冷却） |
| 环境变量 | `ADMIN_PASSWORD` | 后台登录密码，必须设 |
| D1 | `DB`（库名 `stats`） | 调用统计，可选；但设了用量上限的配额组必须绑 |

## 部署

**实际可用路径：Pages Functions**（Git 集成或 `wrangler pages deploy`，入口文件名必须恰好是 `_worker.js`）。Pages Direct Upload 不支持 Functions，访问 `/` 会恒定 404。

> 注：Workers 平台实测会返回 1101（Worker threw exception），暂不可用，请走上面的 Pages 路径。

## 部署后自检

1. 版本：`curl https://<域名>/version` → 返回 `BUILD_ID`。
2. 后台：访问 `/admin`，用 `ADMIN_PASSWORD` 登录。
3. 配渠道：第三方渠道填 OpenRouter / Gemini；模型映射把客户端名映射到上游；可加 `TT:<组名>` 配额组。
4. 拿密钥：在「接入信息 → API 密钥」生成你自己的密钥（系统默认密钥建议重新生成换成强密钥）。

## 常见排障

- **404** → 请求没进 Worker（部署 / 路由 / 域名问题）。
- **自带错误页**（缺 KV / 没设密码）→ 绑定没配，按上补齐。
- **502 `origin_bad_gateway`**（带 `cloudflare_error:true`）→ CF 边缘生成，上游响应无效；长流式注意 CF Free 约 100s 源站超时。
- **上游 4xx 原样透出**（带 `Access-Control-Allow-Origin`）= Worker 返回；只有 `CF-RAY` + `Cache-Control:private` = CF 裸错误页。

## 参考项目

本项目在开发过程中参考了以下开源项目，特此致谢：

- [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api)
- [cmliussss2024/WorkersAI2API](https://github.com/cmliussss2024/WorkersAI2API)
