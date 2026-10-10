# Workers API Hub

**Workers API Hub** 是跑在 Cloudflare Workers 上的单文件 ES Module：把 Cloudflare Workers AI 账号池与第三方 OpenAI 兼容渠道（OpenRouter、Gemini 免费层等）统一转换成 **OpenAI / Anthropic 双协议**接口。零依赖，前端全内联。

> **适用场景**：不想买 VPS / 不想维护服务器，但需要代理海外模型（国内无法直连 OpenRouter / Gemini / OpenAI）。Workers 免费额度够开发用，免运维。

## ✨ 功能汇总

- 📡 多上游统一：支持 Cloudflare AI、OpenRouter、Claude、OpenAI、Gemini、Grok 等
- 🔁 双协议输出：所有渠道自动转 OpenAI / Anthropic 双协议接口
- 🏷️ 智能路由：按模型前缀分流，配额组自动负载均衡、故障冷却
- 📋 模型规格：自动返回 context length、定价、架构等完整规格，支持渠道手填单价兜底
- 🛠️ 管理面板：内置 SPA，渠道配置、密钥管理、模型映射可视化
- 📊 统计看板：D1 自动记录请求、延迟、Token 用量（可选）
- 🔒 内置鉴权：支持用户名+密码双因子、会话超时、登录限流、子密钥隔离

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
| KV Namespace | `KV` | ✅ | 主配置、冷却、缓存全存在这 |
| 环境变量 | `ADMIN_PASSWORD` | ✅ | 后台登录密码 |
| 环境变量 | `ADMIN_USERNAME` | ❌ | 可选，设了则用户名+密码双因子登录 |
| D1 Database | `DB` | ⚠️ | 用配额组或看板必须绑；纯反代可不绑 |

### 3. 上传部署

Workers & Pages → Pages → Create → Direct Upload → 填项目名 → Create project → 直接把 `_worker.js` 拖到上传区域 → 选环境（生产/预览）→ 保存并部署。

> ⚠️ **别用 Workers → Quick Edit 粘贴** `_worker.js`，文件 10000+ 行容易截断导致报 `Error 1101`。上面这条路径没这问题。

### 部署后自检

部署完打开 `https://<你的域名>/version`，返回的 `build` 值要和代码里 `BUILD_ID` 对得上——对不上就是部署没生效，白排查。

---

## 功能说明

### 1. 多上游统一
- **第三方渠道反代**：填写 OpenAI 兼容端点的 baseUrl + API Key 即可，如 OpenRouter、Grok、各 OpenAI 兼容接口。
- **Cloudflare Workers AI 账号池**（`@cf/`）：多账号随机容错。
- **Gemini 原生适配**：不走 OpenAI 兼容端点，直接调原生接口做协议转换，支持 tools（函数调用）与流式。

### 2. 双协议输出
- OpenAI 兼容接口（流式 + 非流式），内置 SSE 心跳保活以规避 Cloudflare 边缘的流式超时。
- Anthropic 协议转换（消息格式、流式 SSE 在两端互转），客户端可任选其一接入。

### 3. 智能路由
请求模型名按前缀分派，互不冲突：

| 写法 | 含义 |
|---|---|
| `@cf/<模型>` / `cf/<模型>` | 走 CF 账号池 |
| `provider:<渠道>/<模型>` 或 `<渠道>/<模型>` | 直指某渠道的某模型 |
| `TT:<组名>` | 走配额组 |
| 映射表里的自定义名 | 由模型映射决定落点 |
| 裸模型名 | 若设了「默认渠道」则原样转发；否则明确报错 |

- 未知模型一律显式返回 400，不会静默回落到任何默认模型。
- **模型映射**：把客户端使用的模型名映射到上游真实模型（具体模型 / 配额组 / CF 模型均可）。无效映射会在管理面板标红并显式报错。
- **调用配额组** `TT:<组名>`：将一个组内的多个上游成员当作虚拟模型对外暴露，自动做负载均衡、故障冷却、换成员重试、会话粘性（可选）；组级上游冷却时长可按组覆盖全局默认（小组成员少时建议调短）；成员支持拖拽排序定优先级，并可按组开启「长流直通」（见 §8）。

### 4. 模型规格
自动返回上游的完整规格（上下文长度、定价、架构、支持参数等）。上游没提供定价时，可在渠道里手填单价兜底。

### 5. 管理面板
访问 `/admin` 登录后使用，按任务分组：
- **概览**：CF 账号池用量查询与刷新；
- **代理接入**：API 密钥管理、第三方渠道配置；
- **路由**：模型映射、调用配额组。

### 6. 统计看板（D1）
绑定 D1 后自动记录每渠道 / 每模型的请求数、成功失败、延迟、token 用量，按小时聚合。未绑 D1 时看板不显示，代理照常。

### 7. 鉴权与安全
- 后台 `/admin` 由 `ADMIN_PASSWORD` 保护；可选 `ADMIN_USERNAME` 开启双因子登录
- 会话有超时保护，登录失败有频率限制，防穷举
- 可在管理面板生成子密钥，与系统默认密钥隔离

### 8. 长流直通（免费档长输出保护）
Cloudflare 免费档单请求 CPU 上限 10ms，超长输出（几分钟、上 MB 的流）会被掐断（错误面板显示「已超出 CPU 时间限制」）。**渠道编辑弹窗、配额组弹窗、模型映射行内**都有 **「长流直通」** 开关（三处任一命中即生效；映射是单线路专属通道，适合只给某条长任务映射单独开）：

- 开启后流式响应**不逐 chunk 读取**，由运行时原生直管，CPU 趋近零（实测 3.4MB / 1.1 万 chunk / 15 分钟的流全程仅 ~18ms CPU）；
- 代价：流内 `model` 字段不回写（客户端看到上游原名）、这几条的 token 统计记 0；**次数统计、冷却、负载均衡完全不受影响**；
- 只对 OpenAI 协议端点的透传流生效；Gemini 原生适配层的流必须逐 chunk 转协议，直通对它无效；
- 适用场景：agent 长任务、超长文本生成。普通聊天用不到，默认关闭。

---

## 常见排障

- **显示自带错误页**（KV 缺失 / 密码未设）→ 绑定没配全，按「快速部署 → 绑定与配置」补齐。
- **配额组报错"统计不到已用次数"** → 有次数上限的组没绑 D1，必须绑。
- **`/` 返回 404** → Worker 路由没配到这个域名/路径。
- **`origin_bad_gateway` 502**（带 `cloudflare_error:true`）→ CF 边缘生成，上游响应无效；长流式注意 CF Free 约 100s 源站超时。
- **长输出中途断流 / 错误面板出现「已超出 CPU 时间限制」** → 免费档 10ms CPU 被超长流耗尽。给对应渠道或配额组开启「长流直通」（见功能说明 §8）。
- **上游 4xx 原样透出**（带 `Access-Control-Allow-Origin`）= Worker 返回；只有 `CF-RAY` + `Cache-Control:private` = CF 裸错误页。

---

<details>
<summary><strong>附录：高级配置 & 内部细节（开发/排障参考）</strong></summary>

一般不用动，留着给以后自己排查用。

### 可选环境变量（默认值合理）

Worker → Settings → Variables → 添加同名变量覆盖；不设就走代码内置默认值。都不是密钥，标记「未加密」即可。

| 变量 | 默认 | 说明 |
|---|---|---|
| `THIRD_PARTY_EST_COST_PER_1K` | `0.0005` | 看板"估算成本"全局兜底单价（美元/千 token），纯示意。仅当渠道没手填价、上游也没带回真实价时才用；2026-10-10 由 0.011 下调（旧值对 Gemini 免费层类场景高估 10~30 倍）。渠道弹窗可手填单价，两格填 0 = 免费渠道（成本按 0 计） |
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

- [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api) 借鉴 Gemini 适配
- [cmliussss2024/WorkersAI2API](https://github.com/cmliussss2024/WorkersAI2API) 基于其主体框架开发

---

## ⚠️ 免责声明

本项目按"原样"提供，作者**不对本项目的稳定性、安全性、准确性、适用性**做任何明示或暗示的担保。本项目仅用于学习研究。

### 风险提示

1. **上游第三方渠道不可控**：OpenRouter、Google Gemini、xAI Grok、OpenAI、Anthropic 等外部服务的 API 格式、定价、可用性、服务条款随时可能变更，本项目无法保证与上述服务的持续兼容。因上游变更导致的调用失败、超支、合规问题，作者不承担任何责任。

2. **Cloudflare 免费额度有限**：Workers / KV / D1 均有每日配额上限，配额耗尽将导致服务中断。生产使用需自行评估升级付费方案。

3. **安全责任自负**：部署时请务必为 `ADMIN_PASSWORD` 设置强密码，并建议启用 `ADMIN_USERNAME` 双因子登录。若因弱密码、密钥泄露或配置不当导致的任何损失，作者不承担任何责任。

4. **合规自负**：使用者需自行遵守 OpenAI / Anthropic / Google / xAI / Cloudflare 等上游的服务条款，以及本人所在国家/地区的法律法规。本项目仅做协议转换与分发，不对通过本项目产生的任何 AI 生成内容承担责任。

5. **数据与隐私**：本项目将上游返回的模型输出原样转发给调用方，不对内容进行二次处理、审查或存储（除用户显式开启 D1 统计）。使用者需自行评估所处理内容的合规性。
