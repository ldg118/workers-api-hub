# EdgeLLM Gateway 部署说明

单文件 Cloudflare Worker `_worker.js`：把 Workers AI / 第三方 OpenAI 兼容渠道转成 **OpenAI / Anthropic 双协议**接口。零依赖，前端全内联，**部署 = 把这一个文件部署到 Cloudflare**。当前主用途是第三方渠道反向代理（OpenRouter、Gemini 免费层）。

> 版本号：当前 `BUILD_ID = 2026-10-06.60`，每次改动 +1，部署后 `curl /version` 核对。

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
