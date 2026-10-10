/**
 * Workers API Hub
 * Cloudflare Workers 上的多上游 API 网关：把第三方 OpenAI 兼容渠道（OpenRouter、Gemini 等）
 * 与 Cloudflare Workers AI 账号池统一转换成 OpenAI / Anthropic 双协议接口；
 * 支持负载均衡、调用配额调度、故障冷却与自动切换，并自带可视化管理面板。
 */

// 构建标识：部署后 curl /version（或看落地页页脚）核对线上版本。
// Direct Upload 没有版本概念，防「部署了没生效」的白跑排查。
// ★★ 硬约定：**每次改动 _worker.js 都要把版本号 +1**（日期变了就换新日期，**序号继续递增、不重置**）。
//    格式固定 `YYYY-MM-DD.N`。验证脚本会拦下格式不对的值，但「有没有 +1」只能靠自觉 ——
//    曾经因为版本号没变，本地/线上分不清哪个是哪版，白排查了一整轮。
const BUILD_ID = '2026-10-10.152';

// 系统默认密钥自动轮换参数
const ROTATE_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000; // 轮换周期：7 天
const ROTATE_GRACE_MS = 24 * 60 * 60 * 1000;        // 旧密钥宽限期：轮换后 24h 内仍有效，避免正在飞的请求被拒
function genApiKey() {
	return 'sk-wa-' + crypto.randomUUID().replace(/-/g, '');
}

// 默认模型映射表（左边是客户端请求的模型名，右边是 Cloudflare 上对应的真实模型）
// 内置模型表。键统一用 cf/<短名> 形式 —— 客户端配置里一眼就能看出这个模型来自
// Cloudflare 账号池，和第三方渠道的「渠道名/模型名」调用形式保持一致。
// 老的裸短名（glm-4.7-flash）继续可用，由 resolveRoute 里的一段兼容逻辑兜住。
const DEFAULT_MODEL_MAP = {
	// 对话 / 文本生成模型（保留：免费额度内、消耗 low 档；已移除 kimi-k2.6 等 high、gpt-oss-120b 及全部向量嵌入模型）
	// 按「每百万 token 输入+输出」的 Neuron 消耗从低到高排列：gemma(36,364) < glm(41,900) < gpt-oss-20b(45,455)
	'cf/gemma-4-26b-a4b-it': '@cf/google/gemma-4-26b-a4b-it',
	'cf/glm-4.7-flash': '@cf/zai-org/glm-4.7-flash',
	'cf/gpt-oss-20b': '@cf/openai/gpt-oss-20b'
};

// 各 CF 模型的额度消耗档位（low / mid / high）—— 只用于界面提示，不参与路由判断。
// 依据 Cloudflare Workers AI 定价页（2026-10-01 更新），按「输入 + 输出」每百万 token 的 Neuron 估算：
//   low ≤ 50,000 ／ mid ≤ 150,000 ／ high > 150,000
// 免费额度是 10,000 Neurons/天（Free 与 Paid 计划相同，超出才计费）。
// CF 调价时同步改这里，并跑 _verify_providers.mjs 的 [16] 基线断言。
const MODEL_COST_TIER = {
	'@cf/google/gemma-4-26b-a4b-it': 'low',        //  9,091 +  27,273
	'@cf/zai-org/glm-4.7-flash': 'low',            //  5,500 +  36,400
	'@cf/openai/gpt-oss-20b': 'low'               // 18,182 +  27,273
};

// 上游「首字节」等待上限（毫秒）。只约束拿到响应头这一步，流式开始后不再计时。
// 取值略小于 CF 边缘的后端等待上限，好让我们先于边缘超时、回一条可读的错误。
const PROVIDER_TIMEOUT_MS = 90000;

// 统一「首字节超时」请求封装：PROVIDER_TIMEOUT_MS 内未拿到响应头则中止请求；
// 一拿到响应头立即清除计时器（流式可继续慢慢流，不被误杀）。所有上游调用
// （CF 账号池 / 第三方 / Gemini 原生）共用这一份，避免四处重复
// AbortController + clearTimeout 且行为漂移。返回 { response, ttfb }；超时时
// 抛出 err.isTimeout=true（上层据此格式化各自的报错），其他错误原样上抛。
async function fetchWithTtfb(url, init = {}) {
	const startedAt = Date.now();
	const timeoutCtl = new AbortController();
	const ttfbTimer = setTimeout(() => timeoutCtl.abort(), PROVIDER_TIMEOUT_MS);
	try {
		// 强制上游不压缩（2026-10-10，Opt3 系列）：Workers 运行时对上游 gzip/br 响应的**逐 chunk 解压
		// CPU 记在 worker 头上** —— 免费档 10ms 下这正是长流杀手：实测推理模型 6k 个 300B 小 chunk
		// 累计 ~2000ms CPU 被 exceededResources 杀掉，而大块少 chunk 的流（Gemini）没事。
		// 关掉压缩后运行时零解压；上游→CF 是机房间直连，多传的原始字节可忽略。
		// （个别上游无视 identity 仍回 gzip 时，行为与从前相同，不会更差。）
		const headers = { ...(init.headers || {}), 'Accept-Encoding': 'identity' };
		const response = await fetch(url, { ...init, headers, signal: timeoutCtl.signal });
		clearTimeout(ttfbTimer);
		return { response, ttfb: Date.now() - startedAt };
	} catch (e) {
		clearTimeout(ttfbTimer);
		if (timeoutCtl.signal.aborted) {
			const err = new Error(`upstream did not respond within ${Math.round(PROVIDER_TIMEOUT_MS / 1000)}s (timeout)`);
			err.isTimeout = true;
			throw err;
		}
		throw e;
	}
}

// 流式响应保活心跳间隔（毫秒）。连续对话时上游生成慢、首字节可能等很久，
// 期间一个字节都不发的话 Cloudflare 边缘会判「响应不完整」并回 502。
// 定期发一个 SSE 注释帧（以 ":" 开头，SSE 规范规定客户端应忽略）即可保活，对输出无影响。
const SSE_HEARTBEAT_MS = 15000;

// Anthropic thinking 块的占位 signature（2026-10-09，NVIDIA NIM 适配）。
// 真实的 thinking signature 是 Anthropic 服务端签的加密串，用于服务端校验「思考块回传」；
// 本代理的思考内容来自上游的 reasoning_content（OpenAI 侧字段，无签名概念），且请求方向
// 转换时客户端回传的 thinking 块会被直接丢弃 —— 签名永远不会被任何一方校验，占位即可。
// 客户端（如 Claude Code）只把它当不透明字符串携带，不做本地校验。
const THINKING_SIG_PLACEHOLDER = 'gateway-thinking-placeholder';

// 上下文溢出钳制重试的安全余量（token 数，2026-10-09，NVIDIA NIM 适配）。
// 钳制后的 max_tokens = 上限 - 消息 - 余量。余量不必大：两次请求的消息完全相同、
// 上游 tokenizer 计数一致，纯粹防边界抖动。
const CONTEXT_RETRY_MARGIN = 512;

// 从上游 400 错误文本里抠出「上下文上限 + 消息 token 数」（OpenAI / NIM 系措辞）：
// "This model's maximum context length is 131072 tokens. However, you requested
//  135072 tokens (125000 in the messages, 10072 in the completion)."
// 解析不出（别家的 400）就返回 null —— 绝不在没把握时瞎改参数重试。
function parseContextOverflow(errText) {
	const text = String(errText || '');
	const mMax = text.match(/maximum context length is (\d+)/i);
	const mBreak = text.match(/\((\d+) in the messages?, (\d+) in the completion\)/i);
	if (!mMax || !mBreak) return null;
	const max = Number(mMax[1]);
	const prompt = Number(mBreak[1]);
	if (!Number.isFinite(max) || !Number.isFinite(prompt) || max <= 0 || prompt < 0) return null;
	return { max, prompt };
}

// Gemini 原生适配层总开关（2026-10-05）：true = googleapis 系渠道走原生 generateContent（含工具调用）；
// false = 全部回落旧的 OpenAI 兼容端点 + 工具历史折文本（一行降级，见「Gemini 原生适配层」）。
const GEMINI_NATIVE_ENABLED = true;

export default {
	async fetch(request, env, ctx) {
		// 1. 检查是否绑定了 KV 存储
		if (!env.KV) {
			return handleKVError(request);
		}

		// 2. 检查是否配置了 ADMIN_PASSWORD 环境变量
		if (!env.ADMIN_PASSWORD) {
			return handlePasswordError(request);
		}

		// 处理跨域预检请求（OPTIONS）
		if (request.method === 'OPTIONS') {
			return new Response(null, {
				headers: {
					'Access-Control-Allow-Origin': '*',
					'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, DELETE',
					'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-api-key'
				}
			});
		}

		const url = new URL(request.url);

		// 2. OpenAI 兼容的代理接口（/v1/ 开头）
		if (url.pathname.startsWith('/v1/')) {
			const response = await handleV1Proxy(request, env, ctx);
			return addCORSHeaders(response);
		}

		// 3. 后台管理面板的 API 接口（/api/ 开头）
		if (url.pathname.startsWith('/api/')) {
			const response = await handleDashboardApi(request, env, ctx);
			return addCORSHeaders(response);
		}

		// 4. 后台管理面板页面
		if (url.pathname === '/admin' || url.pathname === '/admin/') {
			const isLoggedIn = await verifyAdminCookie(request, env);
			if (isLoggedIn) {
				return handleAdminPage(request, env, ctx);
			} else {
				// 未登录则跳转到首页（登录页）
				return new Response(null, {
					status: 302,
					headers: { 'Location': '/' }
				});
			}
		}

		// 5. 首页 / 登录页
		if (url.pathname === '/') {
			return handleLandingPage(request, env, ctx);
		}

		// robots.txt 支持，用于屏蔽搜索引擎爬虫
		if (url.pathname === '/robots.txt') {
			return new Response('User-agent: *\nDisallow: /', {
				headers: { 'Content-Type': 'text/plain; charset=utf-8' }
			});
		}

		// 版本标识（公开）：部署后 curl /version 核对线上是否为新版
		if (url.pathname === '/version') {
			return addCORSHeaders(new Response(JSON.stringify({ build: BUILD_ID }), { headers: { 'Content-Type': 'application/json' } }));
		}

		// 6. 其他路径一律返回 404
		return new Response('404 Not Found', { status: 404 });
	}
};

// 工具函数：给响应加上跨域（CORS）响应头
function addCORSHeaders(response) {
	const newResponse = new Response(response.body, response);
	newResponse.headers.set('Access-Control-Allow-Origin', '*');
	newResponse.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, DELETE');
	newResponse.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-api-key');
	return newResponse;
}

// 工具函数：计算字符串的 SHA-256 哈希值
async function sha256(message) {
	const msgBuffer = new TextEncoder().encode(message);
	const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
	const hashArray = Array.from(new Uint8Array(hashBuffer));
	return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// ----------------------------------------------------
// KV 读写相关工具函数
// （把所有配置合并存到一个 'config' 键里，一次读取就能拿到全部配置，省 KV 读次数）
// ----------------------------------------------------
const memoryCache = {
	config: null,
	configExpiry: 0
};
const CACHE_TTL_MS = 60000; // 内存缓存有效期：1 分钟

// 配置默认值：单一事实来源。getAppConfig 重建对象时以 ...DEFAULT_CONFIG 打底，
// 任何「新增配置字段」只要加到这里就会被自动带上，从结构上消灭「重建时漏字段」类 bug
// （曾导致 systemApiKey 被丢、依赖它的客户端每 ~60s 周期性 401）。
const DEFAULT_CONFIG = {
	accounts: [],
	apiKeys: [],
	customModelMap: {},
	providers: [],
	cfPoolEnabled: false,
	defaultProviderId: '',
	quotaGroups: [],
	systemApiKey: null,
	systemApiKeyCreatedAt: null,
	systemKeyRotationEnabled: false,
	systemKeyRotatedAt: null,
	systemApiKeyPrev: null,
	hiddenModels: [],
	// 逐条模型映射的独立开关：这里存的是「已停用」的映射源名（不删映射本身，随时可再打开）
	disabledMappings: [],
	// 长流直通的映射名单（2026-10-10）：映射是「单线路直走」的专属通道，这里的源名请求走直通
	// （不逐 chunk 读流，防免费档 CPU 10ms 掐断超长输出；代价：流内 model 不回写、token 统计记 0）。
	// 与渠道级 / 配额组级开关是「或」的关系，任一命中即生效。
	fastPassMappings: [],
	// 「不要注入 stream_options」的 (渠道|上游模型) 名单：某些端点会因不认识该字段而整条 4xx。
	// 首次被拒后自动记入，之后对该模型不再注入（参考 nim-proxy 的做法）。
	noUsageInject: []
};

async function getAppConfig(env) {
	const now = Date.now();
	if (memoryCache.config && now < memoryCache.configExpiry) {
		return memoryCache.config;
	}

	const raw = await env.KV.get('config');
	let parsed = { ...DEFAULT_CONFIG, customModelMap: { ...DEFAULT_MODEL_MAP } };

	if (raw) {
		try {
			const data = JSON.parse(raw);
			const accounts = Array.isArray(data.accounts) ? data.accounts : [];
			parsed = {
				...DEFAULT_CONFIG,
				accounts,
				apiKeys: Array.isArray(data.apiKeys) ? data.apiKeys : [],
				customModelMap: (data.customModelMap && typeof data.customModelMap === 'object') ? data.customModelMap : { ...DEFAULT_MODEL_MAP },
				providers: Array.isArray(data.providers) ? data.providers : [],
				// 隐藏模型名单：用户在看板逐行「隐藏」后落入此处（B 方案）
				hiddenModels: Array.isArray(data.hiddenModels) ? data.hiddenModels : [],
				// 已停用的模型映射源名（逐条开关）；老配置缺省 = 全部启用（零迁移）
				disabledMappings: Array.isArray(data.disabledMappings) ? data.disabledMappings : [],
				// 长流直通的映射名单；老配置缺省 = 空（零迁移）
				fastPassMappings: Array.isArray(data.fastPassMappings) ? data.fastPassMappings : [],
				// 不注入 stream_options 的 (渠道|模型) 名单；老配置缺省 = 空（全部照常注入）
				noUsageInject: Array.isArray(data.noUsageInject) ? data.noUsageInject : [],
				// 未显式设置过时：有 CF 账号就沿用「开启」（不静默改变现有部署的行为），
				// 一个账号都没有则视为纯第三方模式，默认关闭账号池
				cfPoolEnabled: typeof data.cfPoolEnabled === 'boolean' ? data.cfPoolEnabled : accounts.length > 0,
				defaultProviderId: typeof data.defaultProviderId === 'string' ? data.defaultProviderId : '',
				// 配额组：一组「同一用途的候选模型 + 各自的配额上限」（按次数或 token），按负载自动分摊、报错自动换成员
				quotaGroups: Array.isArray(data.quotaGroups) ? data.quotaGroups : [],
				// 系统默认密钥：自动轮换相关字段（老配置缺省时按安全默认值补齐）
				systemKeyRotationEnabled: typeof data.systemKeyRotationEnabled === 'boolean' ? data.systemKeyRotationEnabled : false,
				systemKeyRotatedAt: data.systemKeyRotatedAt || null,
				systemApiKeyPrev: data.systemApiKeyPrev || null,
				systemApiKey: data.systemApiKey || null,
				systemApiKeyCreatedAt: data.systemApiKeyCreatedAt || null
			};
		} catch (e) { }
	}
	// ⚠️ KV 读不到 config 时**绝不回写**：以前这里会 put 一份默认配置做「首次初始化」，
	// 但那意味着一次瞬时读空（KV 最终一致 / 抖动）就会把整份配置（渠道 / 密钥 / 配额组）
	// 覆盖成默认 —— 损失极大。默认配置本就是 parsed 的初值，直接返回即可；
	// 真正的落盘交给首次 saveAppConfig（如 ensureSystemKey 生成系统密钥时）。

	memoryCache.config = parsed;
	memoryCache.configExpiry = now + CACHE_TTL_MS;
	return parsed;
}

async function saveAppConfig(env, config) {
	await env.KV.put('config', JSON.stringify(config));
	memoryCache.config = config;
	memoryCache.configExpiry = Date.now() + CACHE_TTL_MS;
}

async function getAccounts(env) {
	const config = await getAppConfig(env);
	return config.accounts;
}

async function saveAccounts(env, accounts) {
	const config = await getAppConfig(env);
	config.accounts = accounts;
	await saveAppConfig(env, config);
	await env.KV.delete('cache_usage_summary'); // 清除用量统计的缓存
}

// 系统默认密钥的维护：首次安装自动替换为随机值（消除硬编码常量泄露风险）+ 开启后每 7 天惰性轮换。
// 写入频率极低（安装一次 / 每 7 天一次），且会同步刷新内存缓存，无性能负担。
async function ensureSystemKey(env) {
	const config = await getAppConfig(env);
	const now = Date.now();
	let changed = false;

	if (!config.systemApiKey) {
		// 首次安装：用随机密钥替换写在代码里的出厂常量
		config.systemApiKey = genApiKey();
		config.systemApiKeyCreatedAt = new Date().toISOString();
		config.systemKeyRotatedAt = now;
		config.systemApiKeyPrev = null;
		changed = true;
	} else if (config.systemKeyRotationEnabled && config.systemKeyRotatedAt && (now - config.systemKeyRotatedAt) >= ROTATE_INTERVAL_MS) {
		// 轮换周期到了：旧密钥进入宽限期（24h 内仍有效），生成新密钥
		config.systemApiKeyPrev = config.systemApiKey;
		config.systemApiKey = genApiKey();
		config.systemApiKeyCreatedAt = new Date().toISOString();
		config.systemKeyRotatedAt = now;
		changed = true;
	}

	// 宽限期过后清掉旧密钥（仅清理，不算变更触发写）
	if (config.systemApiKeyPrev && config.systemKeyRotatedAt && (now - config.systemKeyRotatedAt) >= ROTATE_GRACE_MS) {
		config.systemApiKeyPrev = null;
		changed = true;
	}

	if (changed) await saveAppConfig(env, config);
	return config;
}

async function getApiKeys(env) {
	await ensureSystemKey(env);
	const config = await getAppConfig(env);
	const keys = (config.apiKeys || []).map(k => ({ ...k }));
	const items = [{
		id: '__system__',
		name: '系统默认密钥',
		key: config.systemApiKey,
		system: true,
		createdAt: config.systemApiKeyCreatedAt || null,
		// 给前端展示轮换状态
		rotationEnabled: !!config.systemKeyRotationEnabled,
		nextRotationAt: (config.systemKeyRotationEnabled && config.systemKeyRotatedAt) ? config.systemKeyRotatedAt + ROTATE_INTERVAL_MS : null
	}, ...keys];
	return items;
}

async function getCustomModelMap(env) {
	const config = await getAppConfig(env);
	return config.customModelMap;
}

async function saveCustomModelMap(env, map) {
	const config = await getAppConfig(env);
	config.customModelMap = map;
	await saveAppConfig(env, config);
}

// ---- 「不再注入 stream_options」的本地记忆 ----
// 某些 OpenAI 兼容端点不认识 stream_options，会直接 4xx 拒掉整条请求 → 该模型的流式从此永远失败。
// 对策（参考 nim-proxy）：撞到这种拒绝时去掉注入重试一次，并把该 (渠道|上游模型) 记入名单，之后不再注入。
// 注意：只在**错误正文点名了 stream_options/include_usage** 时才重试 —— 不盲目重试，
// 因为对 40 RPM 这类免费档来说，多打一次上游就是少一次真额度。
function usageInjectKeyOf(providerId, model) {
	return String(providerId) + '|' + String(model || '');
}

function isUsageInjectDisabled(config, providerId, model) {
	const list = (config && Array.isArray(config.noUsageInject)) ? config.noUsageInject : [];
	return list.indexOf(usageInjectKeyOf(providerId, model)) !== -1;
}

async function markUsageInjectDisabled(env, providerId, model) {
	if (!env || !providerId) return;
	const config = await getAppConfig(env);
	const set = new Set((Array.isArray(config.noUsageInject) ? config.noUsageInject : []).map(String));
	const key = usageInjectKeyOf(providerId, model);
	if (set.has(key)) return;   // 已在名单里：不再产生 KV 写
	set.add(key);
	config.noUsageInject = [...set];
	await saveAppConfig(env, config);
}

// 第三方渠道（OpenAI 兼容的上游 API）读写
// 结构与 accounts 平级，存在同一个 config 键里
async function getProviders(env) {
	const config = await getAppConfig(env);
	return config.providers || [];
}

async function saveProviders(env, providers) {
	const config = await getAppConfig(env);
	config.providers = providers;
	await saveAppConfig(env, config);
}

// 双向同步隐藏名单：toAdd 并入 config.hiddenModels（去重）；toRemove 从中移除（仅当已存在）。
// 用于「从渠道移除模型→自动隐藏；重新加回渠道→自动恢复可见」的对称逻辑。
// 调用方需自行完成跨渠道过滤（toAdd 只传确已不在任何渠道的孤儿；toRemove 传本次加回的模型）。
async function applyHiddenModelChanges(env, toAdd, toRemove) {
	if ((!toAdd || !toAdd.length) && (!toRemove || !toRemove.length)) return false;
	const config = await getAppConfig(env);
	const hidden = new Set(Array.isArray(config.hiddenModels) ? config.hiddenModels.map(String) : []);
	let changed = false;
	const addSet = new Set((toAdd || []).map(String).filter(Boolean));
	const remSet = new Set((toRemove || []).map(String).filter(Boolean));
	for (const m of addSet) { if (!hidden.has(m)) { hidden.add(m); changed = true; } }
	for (const m of remSet) { if (hidden.has(m)) { hidden.delete(m); changed = true; } }
	if (changed) {
		config.hiddenModels = [...hidden];
		await saveAppConfig(env, config);
	}
	return changed;
}

// 配额调度总开关（负载均衡 / 成员冷却 / 换成员重试）。默认开；关掉即回到旧行为。
async function isQuotaSchedulingOn(env) {
	const config = await getAppConfig(env);
	return config.quotaScheduling !== false;
}

async function saveQuotaScheduling(env, enabled) {
	const config = await getAppConfig(env);
	config.quotaScheduling = enabled !== false;
	await saveAppConfig(env, config);
}

async function getQuotaGroups(env) {
	const config = await getAppConfig(env);
	return config.quotaGroups || [];
}

async function saveQuotaGroups(env, groups) {
	const config = await getAppConfig(env);
	config.quotaGroups = groups;
	await saveAppConfig(env, config);
}

// ----------------------------------------------------
// 配额组的用量统计
//
// 直接复用 D1 里的 stats 表：req 字段就是「那天、那个渠道、那个模型被调用了多少次」，
// 所以配额判断不需要新表、不需要新埋点。
// 返回 Map（key = JSON.stringify([providerId, model])）→ 已用次数；
// 返回 null 表示「查不到」（未绑 D1 或查询失败）—— 调用方必须显式处理，不能当成 0。
// ----------------------------------------------------
// ----------------------------------------------------
// 配额周期：重置时区与时刻
// ----------------------------------------------------
// 刻意用「固定 UTC 偏移」而不是 IANA 时区名 —— Cloudflare 运行时的内嵌时区数据
// 更新可能滞后（IANA 邮件列表里有实际案例），固定偏移的行为完全可预测。
// 代价：美国夏令时一年要手动改两次（3 月第二个周日开始、11 月第一个周日结束）。
const RESET_TZ_PRESETS = {
	utc:    { label: 'UTC',                        offset: 0 },
	cn:     { label: '北京时间 UTC+8',              offset: 480 },
	jp:     { label: '东京 UTC+9',                  offset: 540 },
	pt_dst: { label: '美国太平洋（夏令时）UTC-7',    offset: -420 },
	pt_std: { label: '美国太平洋（冬令时）UTC-8',    offset: -480 },
	et_dst: { label: '美国东部（夏令时）UTC-4',      offset: -240 },
	et_std: { label: '美国东部（冬令时）UTC-5',      offset: -300 },
	custom: { label: '自定义偏移',                  offset: null }
};

// 组配置 → 时区偏移（分钟）
function resolveResetOffset(group) {
	const key = group && group.resetTz ? String(group.resetTz) : 'utc';
	if (key === 'custom') {
		const v = Number(group && group.resetTzOffset);
		if (!Number.isFinite(v)) return 0;
		return Math.max(-840, Math.min(840, Math.round(v)));
	}
	// 用 hasOwnProperty 取值：避免 key 命中 Object.prototype 成员（constructor/toString…）取到函数
	const preset = Object.prototype.hasOwnProperty.call(RESET_TZ_PRESETS, key) ? RESET_TZ_PRESETS[key] : null;
	return preset && preset.offset !== null ? preset.offset : 0;
}

// 'HH:MM' → 当天第几分钟（0..1439）。解析不出来一律当 0 点。
function parseResetTime(text) {
	const parts = String(text == null ? '' : text).trim().split(':');
	if (parts.length !== 2) return 0;
	const h = Number(parts[0]);
	const mi = Number(parts[1]);
	if (!Number.isInteger(h) || !Number.isInteger(mi)) return 0;
	if (h < 0 || h > 23 || mi < 0 || mi > 59) return 0;
	return h * 60 + mi;
}

// 规范成 'HH:MM'，解析不出来一律 00:00（GET 会回显存下来的值，界面看到的就是真实生效的）
function normalizeResetTime(text) {
	const min = parseResetTime(text);
	return String(Math.floor(min / 60)).padStart(2, '0') + ':' + String(min % 60).padStart(2, '0');
}

// 统计桶的时间键（小时级），如 2026-10-04T07
function statsBucket(ms) {
	return new Date(ms).toISOString().slice(0, 13);
}

// 把 UTC 毫秒格式化成北京时间，仅用于展示与报错文案
function fmtBeijing(ms) {
	return new Date(Number(ms) + 480 * 60000).toISOString().slice(0, 16).replace('T', ' ');
}

// 同一个时间戳按「任意时区偏移」渲染 —— 用来把重置时刻同时显示成 UTC / 北京时间 / 组自己时区，
// 免得用户把同一个瞬间的三个写法误当成三个不同的时间。
function fmtInOffset(ms, offsetMin) {
	return new Date(Number(ms) + Number(offsetMin || 0) * 60000).toISOString().slice(0, 16).replace('T', ' ');
}

// 某年某月「第 day 日」的本地基准毫秒（该月没这一天时落到当月最后一天）。
// ⚠️ 绝不能直接写 Date.UTC(y, m, 31) —— 短月会**进位到次月**（2 月 31 日 → 3 月 3 日），
// 周期窗口会整段错位却完全不报错。必须显式与「当月最后一天」取 min。
function monthAnchorLocal(y, m, day, resetMin) {
	const lastDay = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
	const dd = Math.min(Math.max(1, Math.floor(day) || 1), lastDay);
	return Date.UTC(y, m, dd) + resetMin * 60000;
}

// 当前配额的周期窗口，返回统计桶的半开区间 [startKey, endKey)
function quotaWindow(group, nowMs) {
	const offsetMin = resolveResetOffset(group);
	const resetMin = parseResetTime(group && group.resetLocalTime);
	const isMonth = !!(group && group.period === 'month');
	// 月内重置日：1-31。老组没有该字段 → 缺省 1 → 行为与改造前完全一致（零迁移）。
	const resetDay = Math.max(1, Math.min(31, Math.floor(Number(group && group.resetDay)) || 1));
	// 先把时间平移到该时区的「墙上时间」，之后一律用 UTC getter 读 —— 等价于读本地时间，
	// 但不依赖运行时的本机时区设置（Workers 恒为 UTC）。
	const localMs = Number(nowMs) + offsetMin * 60000;
	const d = new Date(localMs);
	let startLocal;
	if (isMonth) {
		// 本月「第 resetDay 日」的基准时刻；还没到就把窗口退回上个月（上月同样按 resetDay 取）
		startLocal = monthAnchorLocal(d.getUTCFullYear(), d.getUTCMonth(), resetDay, resetMin);
		if (startLocal > localMs) startLocal = monthAnchorLocal(d.getUTCFullYear(), d.getUTCMonth() - 1, resetDay, resetMin);
	} else {
		// 该时区当天的基准时刻；还没到就退回昨天
		startLocal = Math.floor(localMs / 86400000) * 86400000 + resetMin * 60000;
		if (startLocal > localMs) startLocal -= 86400000;
	}
	const sd = new Date(startLocal);
	const endLocal = isMonth
		? monthAnchorLocal(sd.getUTCFullYear(), sd.getUTCMonth() + 1, resetDay, resetMin)
		: startLocal + 86400000;
	const startMs = startLocal - offsetMin * 60000;
	const endMs = endLocal - offsetMin * 60000;
	return {
		startMs: startMs,
		endMs: endMs,
		offsetMin: offsetMin,
		resetMin: resetMin,
		// 桶是小时级：非整点边界向外取整成整点。宁可多算一点（提前切换），
		// 也不要少算 —— 少算会超发上游额度，那才是真的坏事。
		startKey: statsBucket(Math.floor(startMs / 3600000) * 3600000),
		endKey: statsBucket(Math.ceil(endMs / 3600000) * 3600000)
	};
}

// 组的重置基准的人类可读描述，界面直接用
function describeReset(group) {
	const resetDay = Math.max(1, Math.min(31, Math.floor(Number(group && group.resetDay)) || 1));
	const period = group && group.period === 'month' ? ('每月 ' + resetDay + ' 日') : '每天';
	const time = String((group && group.resetLocalTime) || '00:00');
	const key = group && group.resetTz ? String(group.resetTz) : 'utc';
	let tz;
	if (key === 'custom') {
		const off = resolveResetOffset(group);
		const abs = Math.abs(off);
		tz = '自定义 UTC' + (off < 0 ? '-' : '+')
			+ String(Math.floor(abs / 60)).padStart(2, '0') + ':' + String(abs % 60).padStart(2, '0');
	} else {
		const preset = Object.prototype.hasOwnProperty.call(RESET_TZ_PRESETS, key) ? RESET_TZ_PRESETS[key] : null;
		tz = preset ? preset.label : 'UTC';
	}
	return period + ' ' + time + ' · ' + tz;
}

// 从一个用量条目里按计量单位取值。条目形如 {count, tokens}（见 queryQuotaUsage 的 SQL）。
// unit==='token' → token 总量；其它（含缺省）→ 请求条数。条目缺失时返回 0。
function quotaMeterOf(entry, unit) {
	if (!entry) return 0;
	return unit === 'token' ? (Number(entry.tokens) || 0) : (Number(entry.count) || 0);
}

// 查一组配额成员的「本周期已用量」。周期边界由组自己的重置时区/时刻决定。
// 返回 Map<JSON[providerId, model], {count, tokens}>；返回 null 表示算不出来（未绑 D1 / 查询失败）——
// 调用方必须区分 null 和 0。两种计量一次查出（只多一列、往返次数不变），由成员自己的 unit 决定用哪个。
async function queryQuotaUsage(env, group, members) {
	if (!env.DB) return null;
	await ensureStatsTable(env);
	const ids = [...new Set((members || []).map(m => String(m.providerId)))];
	if (!ids.length) return new Map();
	const placeholders = ids.map(() => '?').join(',');
	const win = quotaWindow(group, Date.now());
	// 进程内短缓存：同一周期内结果基本不变，5s 陈旧对分流无影响（见 QUOTA_QUERY_CACHE_MS）
	const cacheKey = 'qusage:' + (group ? group.id : '') + '|' + win.startKey + '|' + win.endKey + '|' + ids.slice().sort().join(',');
	const now = Date.now();
	const ttl = quotaQueryCacheMs(env);
	if (ttl > 0) {
		const hit = quotaQueryCache.get(cacheKey);
		if (hit) {
			if (now < hit.expiry) return hit.value;
			quotaQueryCache.delete(cacheKey); // 过期项顺手清，避免 Map 无限膨胀
		}
	}
	// req + probe_req：把「测试连通 / 测模型」这类探测调用也算进配额。
	// 上游是按总请求数算额度的，测试同样消耗它 —— 不合并的话会出现
	// 「测试把额度用掉了，配额却还显示有剩余」的假象。
	const sql = `SELECT provider_id, model,
	     COALESCE(SUM(req + probe_req), 0) AS used_count,
	     COALESCE(SUM(tokens), 0) AS used_tokens
	   FROM stats
	   WHERE day >= ? AND day < ? AND provider_id IN (${placeholders})
	   GROUP BY provider_id, model`;
	try {
		const { results } = await env.DB.prepare(sql).bind(win.startKey, win.endKey, ...ids).all();
		const out = new Map();
		for (const r of results || []) {
			out.set(JSON.stringify([String(r.provider_id), String(r.model)]), {
				count: Number(r.used_count) || 0,
				tokens: Number(r.used_tokens) || 0
			});
		}
		// 只缓存成功结果，失败(null)不缓存，避免瞬时故障被放大
		if (ttl > 0) quotaQueryCache.set(cacheKey, { expiry: now + ttl, value: out });
		return out;
	} catch (e) {
		return null;
	}
}

// ---------------- 配额调度基础设施（2026-10-06 新增，参考 sub2api 的账号调度） ----------------
// 目标：把「按顺序取第一个未满」改成**真正的负载均衡**，并解决
//   「上游 429 了、配额页却显示还有余额」——原因是只按日/月桶计数，
//   一分钟内把同一个 key 打爆（RPM 超限）时配额根本看不出来。
// 做法：① 选号时同时看「本小时用量」和「本周期用量」，谁闲用谁；
//       ② 上游报错（429/5xx/超时）把成员**临时踢出**（冷却）；
//       ③ 一次请求内自动换成员重试（有上限、带排除名单）。

const QUOTA_COOLDOWN_429_MS = 60000; // 429 限流：默认冷却 60s（上游给了 retry_after 就取更长的）。
// 429 冷却时长可用 env.COOLDOWN_429_MS 覆盖（2026-10-09，NVIDIA NIM 适配）：
// 免费档限流多为 RPM 级（如 NVIDIA 40 RPM），撞限流后 1~2s 就能继续，
// 固定 60s 对这类上游偏保守 —— 设 2000~5000 更贴合。仅影响「写冷却」时长，
// retry_after 语义不变（仍取两者较长）。
function cooldown429Ms(env) {
	const v = Number(env && env.COOLDOWN_429_MS);
	return Number.isFinite(v) && v >= 1000 ? v : QUOTA_COOLDOWN_429_MS;
}
const QUOTA_COOLDOWN_ERR_MS = 30000; // 5xx / 超时 / 连不上：冷却 30s
// 上游「模型级并发已满」（NIM：`ResourceExhausted: Worker local total request limit reached (32/32)`）。
// 与 429 限流、与「额度耗尽」都不同：它是**瞬时拥塞**（几十秒内随生成结束自愈），
// 而且该上限**按模型、跨 key 共享**（换渠道也未必躲得开，只有换到别家基础设施才可能有用）。
// ⚠️ 必须排在「额度耗尽」判定**之前**：否则正文里的 ResourceExhausted 会命中那条
//   `/resource_exhausted/i`，被误判成额度耗尽 → 白关 30 分钟。
// 可用 env.COOLDOWN_WORKER_EXHAUST_MS 覆盖（默认 20s：够让一批生成跑完，又不至于长期闲着）。
const QUOTA_COOLDOWN_WORKER_MS = 20000;
function cooldownWorkerExhaustMs(env) {
	const v = Number(env && env.COOLDOWN_WORKER_EXHAUST_MS);
	return Number.isFinite(v) && v >= 1000 ? v : QUOTA_COOLDOWN_WORKER_MS;
}
// 组级「上游冷却时长」覆盖（2026-10-10）：配额组可以自带 cooldownSec（秒），留空/0 = 用全局默认。
// 为什么需要它：全局值是**一个数**，而「成员少的组」和「成员多的组」对冷却时长的容忍度正好相反 ——
//   冷却中的成员会被 pickQuotaMember **直接跳过**，组越小越容易整组同时进冷却（那就是请求直接失败）；
//   所以小组合该调短、大组合无所谓（这类错误上游是秒回、不产生 token 计费，重试很便宜）。
// ⚠️ 只覆盖两类「可配置的短冷却」：上游拥塞（worker 池挤满）与 429 限流。
//    401/403 与 5xx / 超时的 30s **不覆盖**：那两类缩短没有任何好处（key 错了/上游崩了，早试也是白试）。
// 非法值（负数/非数字）在这里一律当「不覆盖」—— 校验在保存接口做（那里会显式 400）。
function quotaCoolOverrideMs(group) {
	const n = Number(group && group.cooldownSec);
	return Number.isFinite(n) && n >= 1 ? Math.floor(n) * 1000 : 0;
}
// 判定上游是否在说「我不认识 stream_options / include_usage」。
// 只有正文点名了这个字段才做「去掉注入重试一次」，避免对普通 400 盲目重试（白白多打一次上游额度）。
const USAGE_FIELD_REJECTED_RE = /stream_options|include_usage/i;
// 「额度耗尽」类错误（402 余额不足 / 429+insufficient_quota / RESOURCE_EXHAUSTED）：
// 这类失败**不会**在几十秒后自愈 —— 要等配额周期重置（一天/一月）。若仍按 60s 冷却，
// 调度器会反复回头撞同一堵墙：每次都白失败一次、冷却期内还用不了它。
// 故给一个远长于瞬时错误的冷却。⚠️ QUOTA_COOLDOWN_MAX_MS 必须 >= 此值，
// 否则 setCooldown 里的 Math.min 会把它截断回上限。
const QUOTA_COOLDOWN_QUOTA_MS = 30 * 60 * 1000; // 额度/余额耗尽：冷却 30 分钟
const QUOTA_COOLDOWN_MAX_MS = 1800000; // 冷却上限 30 分钟（防被上游的超长 retry_after 卡死）
const QUOTA_MAX_SWITCH = 3; // 一次请求内最多尝试几个成员（含第一个）
// 可用 env.QUOTA_MAX_SWITCH 覆盖（2~10）。注意代价：每多试一个成员 = 多一次上游往返，
// 若失败是「超时」型，最坏情况会成倍拉长这个请求的等待时间 —— 所以默认保守取 3。
function quotaMaxSwitch(env) {
	const v = Number(env && env.QUOTA_MAX_SWITCH);
	return Number.isFinite(v) && v >= 2 ? Math.min(10, Math.floor(v)) : QUOTA_MAX_SWITCH;
}
// 瞬时基础设施错误（5xx / 超时 / 连不上）的「原地重试」次数。
// 现实依据（2026-10-06 直连 Google 实测）：gemini-3.1-flash-lite 会间歇性返回
// 503「This model is currently experiencing high demand. Spikes in demand are usually temporary.」
// —— 5 次里挂 1 次；重试一次基本就过。
// 只在「没有别的成员可换」时才用（非配额组请求，或第一个成员且无备选），避免放大故障时的请求量。
const PROVIDER_TRANSIENT_RETRY = 1;
const PROVIDER_TRANSIENT_RETRY_DELAY_MS = 500; // 重试前小睡一下，给上游喘息
const QUOTA_HOUR_WEIGHT = 3; // 选号打分里「本小时已用比例」的权重（越大越避免短时扎堆）
// 选号打分里「在途并发」的权重。打分主体已改成按「已用/上限」比例（量纲无关），
// 而在途是个绝对计数，故折算成一个小比例项：1 个在途 ≈ 多用 5% 额度。
const QUOTA_BUSY_WEIGHT = 0.05;
const COOLDOWN_KV_KEY = 'cooldowns';

// 冷却表：KV 存（跨 isolate 生效），isolate 内短缓存（省 KV 读）。
// 缓存时长可用 env.COOLDOWN_CACHE_MS 覆盖（0 = 每次都读 KV，验证脚本与排障用）。
let cooldownCache = { at: 0, map: null };

// 配额分流用的两次 D1 读数（本周期用量 / 本小时用量）也做 isolate 内短缓存。
// 配额计数本身就是近似/最终一致，3~5s 的陈旧对分流判断无影响，却能把
// 「每条走配额组的请求都打 2 次 D1」降成「每 5s 最多 2 次」，直接砍掉请求延迟。
// 注意：null（D1 瞬错退化值）绝不进缓存，否则一次故障会被放大成 5s 内全部选号失败。
const QUOTA_QUERY_CACHE_MS = 5000; // 默认 5s；可用 env.QUOTA_QUERY_CACHE_MS 覆盖（0 = 每次都读 D1，验证脚本用）
const quotaQueryCache = new Map(); // key -> { expiry, value }

function cooldownKeyOf(providerId, model) {
	return String(providerId) + '|' + String(model || '');
}

function cooldownCacheMs(env) {
	const v = Number(env && env.COOLDOWN_CACHE_MS);
	return Number.isFinite(v) && v >= 0 ? v : 5000;
}

function quotaQueryCacheMs(env) {
	const v = Number(env && env.QUOTA_QUERY_CACHE_MS);
	return Number.isFinite(v) && v >= 0 ? v : QUOTA_QUERY_CACHE_MS;
}

async function getCooldowns(env) {
	const now = Date.now();
	const ttl = cooldownCacheMs(env);
	if (ttl > 0 && cooldownCache.map && now - cooldownCache.at < ttl) return cooldownCache.map;
	let map = {};
	try {
		const raw = env.KV ? await env.KV.get(COOLDOWN_KV_KEY) : null;
		if (raw) map = JSON.parse(raw) || {};
	} catch (e) { map = {}; }
	// 把「因落盘节流还没写进 KV」的条目并回来：否则每次 KV 重读（缓存过期）都会把它们抹掉，
	// 节流就从「少写几次」变成「丢冷却」。只并这些条目 —— 其它语义（如管理员手动清空）不受影响。
	if (cooldownWrite.unpersisted.size && cooldownCache.map) {
		for (const k of cooldownWrite.unpersisted) {
			const lv = cooldownCache.map[k];
			if (lv) map[k] = lv;
		}
	}
	// 顺手清掉已过期的，免得这个键无限膨胀
	for (const k of Object.keys(map)) {
		if (!(Number(map[k] && map[k].until) > now)) delete map[k];
	}
	cooldownCache = { at: now, map };
	return map;
}

// cooldowns 的 KV 落盘节流。
// 背景：KV 免费档只有 1000 写/天，而上游**每次失败**都会把整张冷却表落盘一次 ——
// 上游故障 + 客户端重连重试时，这个键会被瞬间刷爆（同一键还额外受平台「1 写/秒」限制）。
// 但**同一 isolate 内的冷却判定用的是内存 map**（cooldownCache），KV 落盘只为「跨 isolate / 重启后仍可见」，
// 所以突发时少落几次盘几乎没有代价：本次该跳过谁，照样跳过。
// 规则：① 两次落盘至少间隔 COOLDOWN_KV_MIN_MS；
//      ② COOLDOWN_KV_WINDOW_MS 窗口内新增冷却达到 COOLDOWN_KV_BURST_N 次 → 暂停落盘 COOLDOWN_KV_PAUSE_MS
//         （暂停期间内存照常生效，暂停结束后由下一次状态变更补写一次最新状态）。
// 整体可用 env.COOLDOWN_KV_THROTTLE=0 关闭（验证脚本断言「每次失败都落盘」时会关掉）。
const COOLDOWN_KV_MIN_MS = 3000;
const COOLDOWN_KV_WINDOW_MS = 15000;
const COOLDOWN_KV_BURST_N = 3;
const COOLDOWN_KV_PAUSE_MS = 120000;
const cooldownWrite = { lastAt: 0, winStart: 0, winFails: 0, pausedUntil: 0, unpersisted: new Set() };

function cooldownKvThrottleOn(env) {
	const v = env && env.COOLDOWN_KV_THROTTLE;
	if (v === undefined || v === null || v === '') return true;
	return !(v === 0 || v === '0' || v === false || v === 'false');
}

// 记一次「新增冷却」（= 一次上游失败），够密就进入暂停
function noteCooldownFailure() {
	const now = Date.now();
	if (now - cooldownWrite.winStart > COOLDOWN_KV_WINDOW_MS) {
		cooldownWrite.winStart = now;
		cooldownWrite.winFails = 0;
	}
	cooldownWrite.winFails++;
	if (cooldownWrite.winFails >= COOLDOWN_KV_BURST_N) {
		cooldownWrite.pausedUntil = now + COOLDOWN_KV_PAUSE_MS;
		cooldownWrite.winStart = now;
		cooldownWrite.winFails = 0;
	}
}

// 落盘。force=true 跳过节流 —— 只有 clearCooldown 会用它：它仅在真有条目时才走到这里，本身就低频且有界。
async function persistCooldowns(env, map, force) {
	if (!env.KV) return false;
	const now = Date.now();
	if (!force && cooldownKvThrottleOn(env)) {
		if (now < cooldownWrite.pausedUntil) return false;                  // 暂停中
		if (now - cooldownWrite.lastAt < COOLDOWN_KV_MIN_MS) return false;   // 间隔太近
	}
	try {
		await env.KV.put(COOLDOWN_KV_KEY, JSON.stringify(map));
		cooldownWrite.lastAt = now;
		cooldownWrite.unpersisted.clear();   // 整张表已落盘，之前的「未落盘」标记不再需要
		return true;
	} catch (e) { return false; }   // 冷却落盘失败不影响主流程
}

async function setCooldown(env, providerId, model, ms, reason) {
	const key = cooldownKeyOf(providerId, model);
	const map = await getCooldowns(env);
	const until = Date.now() + Math.max(1000, Math.min(QUOTA_COOLDOWN_MAX_MS, Number(ms) || 0));
	const prev = map[key];
	// 已在冷却里且剩余时间更长 → 不缩短
	if (prev && Number(prev.until) > until) return prev;
	map[key] = { until, reason: String(reason || '').slice(0, 120) };
	cooldownCache = { at: Date.now(), map };
	// 标记「可能被节流挡住不落盘」的条目：getCooldowns 在 KV 重读时会把它们并回来，
	// 否则一次缓存过期就把刚设的冷却凭空抹掉 —— 节流会变成「丢冷却」。
	cooldownWrite.unpersisted.add(key);
	noteCooldownFailure();
	await persistCooldowns(env, map);
	return map[key];
}

async function clearCooldown(env, providerId, model) {
	const key = cooldownKeyOf(providerId, model);
	const map = await getCooldowns(env);
	if (!map[key]) return false;
	delete map[key];
	cooldownWrite.unpersisted.delete(key);
	cooldownCache = { at: Date.now(), map };
	// force：解除冷却不节流（有条目才会走到这里，天然低频；且它是「故障恢复」的信号，应当立刻可见）
	await persistCooldowns(env, map, true);
	return true;
}

// 把上游失败分类：值不值得「换个成员重试」+ 冷却多久。
// transient = 瞬时基础设施错误（5xx / 超时 / 连不上）→ 值得**原地再试一次**；
//             429 / 401 / 403 / 402 不算（原地重试没用，得换成员或等冷却）。
// 冷却时长分两档：瞬时错误 30s / 429 限流 60s；**额度耗尽类 30 分钟**（不会自愈，见常量注释）。
// coolOverrideMs：配额组的组级冷却覆盖（毫秒；0/缺省 = 用全局）。见 quotaCoolOverrideMs。
//   只作用于「上游拥塞」与「429 限流」两档；retry_after 仍照旧取更长者（上游明确说了等多久，得尊重）。
function classifyUpstreamFailure(result, env, coolOverrideMs) {
	const s = Number((result && (result.upstreamStatus || result.status)) || 0);
	const msg = String((result && result.error) || '');
	const coolOv = Number(coolOverrideMs) > 0 ? Number(coolOverrideMs) : 0;
	// ★ 最先判「模型级 worker 池挤满」（2026-10-09，参考 nim-proxy）：
	//   NIM 除 40 RPM 外还有**每模型 worker 并发上限**，报错正文含 `ResourceExhausted:
	//   Worker local total request limit reached (32/32)`。若落到下面的 `/resource_exhausted/i`
	//   会被误判成「额度/余额已耗尽」→ 白关 30 分钟；而它其实是**几十秒自愈的瞬时拥塞**。
	//   给短冷却；**不设 transient** —— 循环里的原地重试只睡 0.5s，对拥塞没意义，只白烧一次上游额度。
	if (/worker local total request limit|worker[^.]*request limit reached/i.test(msg)) {
		return {
			retryable: true,
			transient: false,
			coolMs: coolOv || cooldownWorkerExhaustMs(env),   // 组里填了就按组的来（见 quotaCoolOverrideMs）
			reason: '上游模型级并发已满（worker 池挤满，稍后自愈）'
		};
	}
	let out;
	// ★ 额度/余额耗尽优先判定：这类失败**必须换成员**，且要**长冷却**。
	//   · 402 Payment Required —— OpenRouter 等「余额不足」走这个码；
	//   · 429 但正文是额度语义 —— OpenAI 的 insufficient_quota、Gemini 的 RESOURCE_EXHAUSTED
	//     都复用 429，光看状态码会误判成「限流 60s」，于是 60s 后又去撞（额度根本没恢复）。
	//   注意：402 在改造前落进 `s >= 400` 分支 → retryable:false → **不换成员**，是明确的盲区。
	const quotaExhausted = /insufficient_quota|resource_exhausted|exceeded your current quota|quota exceeded|billing/i.test(msg);
	if (s === 402 || (s === 429 && quotaExhausted)) {
		out = { retryable: true, transient: false, coolMs: QUOTA_COOLDOWN_QUOTA_MS, reason: `上游 ${s}（额度/余额已耗尽）` };
	}
	// 429 限流同样吃组级覆盖（RPM 级限流在小组里也会把整组关 60s，正是用户要调短的那类）
	else if (s === 429) out = { retryable: true, transient: false, coolMs: coolOv || cooldown429Ms(env), reason: '上游 429 限流' };
	else if (s === 401 || s === 403) out = { retryable: true, transient: false, coolMs: QUOTA_COOLDOWN_ERR_MS, reason: `上游 ${s}（key 无效 / 无权限）` };
	else if (s >= 500) out = { retryable: true, transient: true, coolMs: QUOTA_COOLDOWN_ERR_MS, reason: `上游 ${s}` };
	else if (s >= 400) out = { retryable: false, transient: false, coolMs: 0, reason: `上游 ${s}（请求本身有问题，换成员也没用）` };
	else if (/timeout/i.test(msg)) out = { retryable: true, transient: true, coolMs: QUOTA_COOLDOWN_ERR_MS, reason: '上游超时' };
	else if (/connection error/i.test(msg)) out = { retryable: true, transient: true, coolMs: QUOTA_COOLDOWN_ERR_MS, reason: '上游连接失败' };
	else out = { retryable: false, transient: false, coolMs: 0, reason: '' };
	// 上游给了 retry_after 就取更长的冷却（429 常见），但不超过上限
	if (out.retryable) {
		const m = /"retry_after"\s*:\s*(\d+)/.exec(msg);
		if (m) out.coolMs = Math.min(QUOTA_COOLDOWN_MAX_MS, Math.max(out.coolMs, Number(m[1]) * 1000));
	}
	return out;
}

// 当前小时桶的用量（stats.day 形如 2026-10-04T07）——把请求在一小时内摊开，
// 这正是「配额还有余额却 429」的解药：不靠精确限流，靠不扎堆。
// 与 queryQuotaUsage 同口径：一次查出 count + tokens 两个维度，由成员自己的 unit 决定用哪个。
async function queryQuotaHourUsage(env, members) {
	if (!env.DB) return null;
	await ensureStatsTable(env);
	const ids = [...new Set((members || []).map(m => String(m.providerId)))];
	if (!ids.length) return new Map();
	const placeholders = ids.map(() => '?').join(',');
	const hourKey = new Date().toISOString().slice(0, 13);
	// 进程内短缓存：小时桶内数据在 5s 内几乎不变（见 QUOTA_QUERY_CACHE_MS）
	const cacheKey = 'qhour:' + hourKey + '|' + ids.slice().sort().join(',');
	const now = Date.now();
	const ttl = quotaQueryCacheMs(env);
	if (ttl > 0) {
		const hit = quotaQueryCache.get(cacheKey);
		if (hit) {
			if (now < hit.expiry) return hit.value;
			quotaQueryCache.delete(cacheKey); // 过期项顺手清，避免 Map 无限膨胀
		}
	}
	const sql = `SELECT provider_id, model,
	     COALESCE(SUM(req + probe_req), 0) AS used_count,
	     COALESCE(SUM(tokens), 0) AS used_tokens
	   FROM stats
	   WHERE day = ? AND provider_id IN (${placeholders})
	   GROUP BY provider_id, model`;
	try {
		const { results } = await env.DB.prepare(sql).bind(hourKey, ...ids).all();
		const out = new Map();
		for (const r of results || []) {
			out.set(JSON.stringify([String(r.provider_id), String(r.model)]), {
				count: Number(r.used_count) || 0,
				tokens: Number(r.used_tokens) || 0
			});
		}
		// 只缓存成功结果，失败(null)不缓存，避免瞬时故障被放大
		if (ttl > 0) quotaQueryCache.set(cacheKey, { expiry: now + ttl, value: out });
		return out;
	} catch (e) {
		return null; // 算不出来就退化成不带小时维度，别让选号整个挂掉
	}
}

// FNV-1a：会话散列（稳定的确定性哈希，用来做「会话粘性」选号）
function hashString(s) {
	let h = 2166136261;
	const str = String(s == null ? '' : s);
	for (let i = 0; i < str.length; i++) {
		h ^= str.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
}

// 会话标识：取第一条 user 消息的前 256 字符 —— 多轮对话里它恒定不变
function sessionKeyOf(payload) {
	const msgs = (payload && Array.isArray(payload.messages)) ? payload.messages : [];
	for (const m of msgs) {
		if (m && m.role === 'user') {
			const t = typeof m.content === 'string'
				? m.content
				: (Array.isArray(m.content) ? m.content.map(p => (p && p.text) || '').join('') : '');
			if (t) return 's' + hashString(t.slice(0, 256)).toString(36);
		}
	}
	return '';
}

// isolate 内的在途计数：同一时刻尽量别把并发的请求都压到同一个成员身上
const inflightByMember = new Map();
function inflightInc(key) { inflightByMember.set(key, (inflightByMember.get(key) || 0) + 1); }
function inflightDec(key) {
	const v = (inflightByMember.get(key) || 0) - 1;
	if (v > 0) inflightByMember.set(key, v); else inflightByMember.delete(key);
}

// 按组内顺序找出第一个「配额未满」的成员。
// 返回 { ok: true, providerId, model } 或 { ok: false, error }
async function pickQuotaMember(group, env, opts) {
	opts = opts || {};
	const excluded = opts.exclude instanceof Set ? opts.exclude : new Set();
	const inflight = opts.inflight instanceof Map ? opts.inflight : null;
	// 调度总开关关掉时：不看冷却、不看负载、不粘性 —— 完全回到旧行为（按组内顺序取第一个未满）
	const sched = opts.scheduling !== false;
	const providers = await getProviders(env);
	const cooldowns = sched ? await getCooldowns(env) : {};
	const nowMs = Date.now();
	const skipped = [];
	const usable = [];
	for (const m of group.members || []) {
		if (m.status === 'disabled') continue;
		const key = cooldownKeyOf(m.providerId, m.model);
		if (excluded.has(key)) { skipped.push(`「${m.model}」本次已试过`); continue; }
		const p = providers.find(x => x.id === m.providerId);
		if (!p) { skipped.push(`成员「${m.model}」引用的渠道已被删除`); continue; }
		if (p.status === 'disabled') { skipped.push(`「${p.name}」渠道已停用`); continue; }
		const cd = cooldowns[key];
		if (cd && Number(cd.until) > nowMs) {
			skipped.push(`「${p.name}/${m.model}」冷却中（还剩 ${Math.ceil((Number(cd.until) - nowMs) / 1000)}s：${cd.reason || '上游报错'}）`);
			continue;
		}
		usable.push({ ...m, providerName: p.name, _key: key });
	}

	if (!usable.length) {
		return {
			ok: false,
			error: `配额组「${group.name}」当前没有可用成员。` + (skipped.length ? '（' + skipped.join('；') + '）' : '')
		};
	}

	const period = group.period === 'month' ? 'month' : 'day';
	const hasLimit = usable.some(m => Number(m.limit) > 0);
	let usage = null;
	if (hasLimit) {
		usage = await queryQuotaUsage(env, group, usable);
		if (!usage) {
			return {
				ok: false,
				error: `配额组「${group.name}」的成员设了上限，但统计不到已用量 —— `
					+ '请在 Worker 上绑定 D1 数据库（变量名 DB）。未绑定时无法安全地按配额分流。'
			};
		}
	}
	// 本小时用量：把请求在一小时内摊开（拿不到就退化成只看本周期，不报错）。调度关掉时不查。
	const hourUsage = sched ? await queryQuotaHourUsage(env, usable) : null;

	// 计量单位：'token' = 按 token 总量，其它（缺省）= 按请求条数。
	// 旧配置不带这个字段 → 一律当 'count'，语义与改造前完全一致（零迁移）。
	const unitOf = (m) => (m.unit === 'token' ? 'token' : 'count');
	// 打分的分母在下面「定层」之后再算（只按本层算）——层内成员共用同一个分母，层间没有可比性。

	const rows = [];
	for (const m of usable) {
		const k = JSON.stringify([String(m.providerId), String(m.model)]);
		const limit = Number(m.limit) || 0;
		const unit = unitOf(m);
		const used = usage ? quotaMeterOf(usage.get(k), unit) : 0;
		if (limit > 0 && used >= limit) continue; // 本周期已满 → 硬性剔除
		const hour = hourUsage ? quotaMeterOf(hourUsage.get(k), unit) : 0;
		const busy = inflight ? (inflight.get(m._key) || 0) : 0;
		rows.push({ m, limit, unit, used, hour, busy, rnd: Math.random() });
	}

	if (!rows.length) {
		const detail = usable.map(m => {
			const limit = Number(m.limit) || 0;
			const unit = unitOf(m);
			if (limit <= 0) return `${m.model} 不限`;
			const used = usage
				? quotaMeterOf(usage.get(JSON.stringify([String(m.providerId), String(m.model)])), unit)
				: 0;
			return `${m.providerName}/${m.model} ${used}/${limit}${unit === 'token' ? ' token' : ' 次'}`;
		}).join('，');
		const win = quotaWindow(group, Date.now());
		return {
			ok: false,
			error: `配额组「${group.name}」${period === 'month' ? '本月' : '今日'}配额已用完（${detail}）。`
				+ `下次重置：${fmtBeijing(win.endMs)}（北京时间；基准 ${describeReset(group)}）。`
		};
	}

	// ---- 组内优先级（2026-10-09）----
	// 语义：数字越小越优先（default 1）。**只在「当前可用的最高那一层」里选人，层内依旧是原来的负载均衡**
	// —— 两者正交、互补，不是二选一。高层成员被冷却 / 停用 / 已满时它就不在 rows 里了，
	// 这一层自然塌到下一层，这就是「优先用 A，A 挂了才用 B」的兜底。
	const prioOf = (m) => {
		const n = Math.floor(Number(m && m.priority));
		return Number.isFinite(n) && n > 0 ? n : 1;
	};

	// 调度总开关关闭 → 不做负载均衡 / 不换成员 / 不冷却；但**优先级仍生效**
	// （按「优先级 → 配置顺序」取第一个未满）。全部同优先级时 = 原样按配置顺序 = 与改造前完全一致
	//（rows 保持组内顺序，而 sort 是稳定的）。
	if (!sched) {
		const first = rows.slice().sort((a, b) => prioOf(a.m) - prioOf(b.m))[0];
		return { ok: true, providerId: first.m.providerId, model: first.m.model, used: first.used, limit: first.limit };
	}

	// 定层：只保留可用的最高优先级那一层作为候选（全部没填 priority 时 tierPrio=1、tierRows===rows）
	const tierPrio = rows.reduce((mn, r) => Math.min(mn, prioOf(r.m)), Infinity);
	const tierRows = rows.filter(r => prioOf(r.m) === tierPrio);
	const tierKeys = new Set(tierRows.map(r => r.m._key));
	const tierUsable = usable.filter(m => tierKeys.has(m._key));
	// 打分的分母：有限成员用自己的 limit；「不限」成员（limit<=0）用**本层**最大 limit 做分母，
	// 好让不限成员彼此仍可比。全层都不限时为 0 → 小数点退化为绝对量比较（= 改造前行为）。
	const maxLimit = tierUsable.reduce((mx, m) => Math.max(mx, Number(m.limit) || 0), 0);

	// 会话粘性：同一会话尽量落到同一个成员 —— 多轮对话 / 工具调用更稳（保住上游 Prompt 缓存）。
	// 用「组内配置顺序 + 会话散列」确定性计算，不需要存状态；该成员不可用时自动回落均衡。
	// ★ 默认开启（opt-out）：只有显式设为 false 才关闭；未设置一律视为开。
	if (opts.sessionKey && group.sticky !== false) {
		const want = tierUsable[hashString(opts.sessionKey) % tierUsable.length];
		const hit = tierRows.find(r => r.m._key === want._key);
		if (hit) return { ok: true, providerId: hit.m.providerId, model: hit.m.model, used: hit.used, limit: hit.limit, sticky: true };
	}

	// 负载均衡排序：按「消耗比例」而非绝对量 —— 组内成员可能一个按次数、一个按 token，
	// 上限也各不相同（50 万 vs 500 万），绝对量根本不可比。
	//   score = 本小时消耗比例 × 权重 + 本周期消耗比例 + 在途并发 × 小权重
	//   · 有限成员：比例 = 已用 / 自己的上限
	//   · 不限成员：本周期比例恒为 0（优先——先把不限额度的用掉），小时比例用组内最大上限做分母
	//   · 全组都不限：小时比例退化为绝对量，与改造前的 (hour+busy)*3 → used 行为等价
	const scoreOf = (r) => {
		const usedRatio = r.limit > 0 ? r.used / r.limit : 0;
		const hourRatio = r.limit > 0 ? r.hour / r.limit
			: (maxLimit > 0 ? r.hour / maxLimit : r.hour);
		return hourRatio * QUOTA_HOUR_WEIGHT + usedRatio + r.busy * QUOTA_BUSY_WEIGHT;
	};
	tierRows.sort((a, b) => scoreOf(a) - scoreOf(b) || a.rnd - b.rnd);
	const pick = tierRows[0];
	return { ok: true, providerId: pick.m.providerId, model: pick.m.model, used: pick.used, limit: pick.limit };
}

// 运行模式：是否启用 Cloudflare 账号池 / 默认第三方渠道
async function getCfPoolEnabled(env) {
	const config = await getAppConfig(env);
	return config.cfPoolEnabled !== false;
}

async function saveRuntimeSettings(env, { cfPoolEnabled, defaultProviderId }) {
	const config = await getAppConfig(env);
	if (typeof cfPoolEnabled === 'boolean') config.cfPoolEnabled = cfPoolEnabled;
	if (defaultProviderId !== undefined) config.defaultProviderId = defaultProviderId;
	await saveAppConfig(env, config);
}

// ----------------------------------------------------
// 管理员身份验证（同时支持 Cookie 和 Authorization 请求头）
// ----------------------------------------------------
// 登录令牌 = "<sha256(管理员密码)>.<过期时间戳>"。
// 登录 cookie 设 Max-Age=14400（4 小时）→ 持久化写盘：关窗口/关掉整个浏览器后，
// 4 小时内重开仍是登录态；超过 4 小时（无论浏览器开没开）两端同时失效、强制重登。
// 令牌内的 exp 与 Max-Age 对齐，避免出现「cookie 还在、服务器已拒」。
// 旧的「裸哈希」cookie（无有效期）一律视为过期 → 强制重新登录。
const ADMIN_SESSION_TTL_MS = 4 * 60 * 60 * 1000; // 最长登录有效期：4 小时

// 站点图标（favicon）：复刻落地页 .logo-icon —— 渐变圆角方块 + 白字 AI。
// 内联 SVG data-URI，零外部请求；浏览器标签页/收藏夹显示用。
const FAVICON_LINK = `<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0' stop-color='%236366f1'/%3E%3Cstop offset='.5' stop-color='%23a855f7'/%3E%3Cstop offset='1' stop-color='%23ec4899'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='64' height='64' rx='14' fill='url(%23g)'/%3E%3Ctext x='32' y='43' font-family='Arial,sans-serif' font-size='28' font-weight='700' fill='%23fff' text-anchor='middle'%3EAI%3C/text%3E%3C/svg%3E">`;

function makeAdminSessionToken(hash) {
	return hash + '.' + (Date.now() + ADMIN_SESSION_TTL_MS);
}

// 解析会话令牌：返回 { hash, exp }；格式不对（含旧的无有效期格式）返回 null
function parseAdminSessionToken(token) {
	if (typeof token !== 'string' || !token) return null;
	const i = token.lastIndexOf('.');
	if (i < 0) return null;
	const hash = token.slice(0, i);
	const exp = Number(token.slice(i + 1));
	if (!hash || !Number.isFinite(exp)) return null;
	return { hash, exp };
}

// 管理员凭据。可选环境变量 ADMIN_USERNAME：设置后登录需**同时**填对用户名+密码，
// 穷举要同时命中两个秘密，破解成本大幅上升；未设置则退回「仅密码」（向后兼容）。
function adminCredentials(env) {
	const password = env && env.ADMIN_PASSWORD ? String(env.ADMIN_PASSWORD).trim() : '';
	const username = env && env.ADMIN_USERNAME ? String(env.ADMIN_USERNAME).trim() : '';
	return { username, password, requireUsername: !!username };
}

// 会话令牌的哈希源：配了用户名就纳入 → 改用户名或密码任一都会让旧登录态失效
async function adminTokenHash(env) {
	const { username, password } = adminCredentials(env);
	return sha256(username ? (username + ':' + password) : password);
}

// ----------------------------------------------------
// 登录失败限流（isolate 内存 + 冷却）
// ----------------------------------------------------
// 说明：仅本 isolate 生效 —— 单个/少量来源的穷举会被直接锁死；分布式来源理论上可绕过，
// 但那已远超个人网关的威胁模型，且正常用户几乎无感（错几次等一会儿即可）。
const LOGIN_MAX_FAILS = 5;               // 窗口内失败达到该次数 → 锁定
const LOGIN_WINDOW_MS = 10 * 60 * 1000;  // 失败计数窗口：10 分钟
const LOGIN_LOCK_MS = 5 * 60 * 1000;     // 锁定时长：5 分钟
const loginFailures = new Map();         // ip -> { count, firstAt, until }

function clientIpOf(request) {
	const ip = request.headers.get('CF-Connecting-IP')
		|| request.headers.get('X-Forwarded-For')
		|| 'unknown';
	return String(ip).split(',')[0].trim() || 'unknown';
}

function loginLockLeft(ip) {
	const rec = loginFailures.get(ip);
	if (!rec || !rec.until) return 0;
	const left = rec.until - Date.now();
	return left > 0 ? left : 0;
}

function noteLoginFailure(ip) {
	const now = Date.now();
	let rec = loginFailures.get(ip);
	if (!rec || (now - rec.firstAt) > LOGIN_WINDOW_MS) rec = { count: 0, firstAt: now, until: 0 };
	rec.count++;
	if (rec.count >= LOGIN_MAX_FAILS) rec.until = now + LOGIN_LOCK_MS;
	loginFailures.set(ip, rec);
	// 防内存膨胀：条目过多时清掉已过期/已过窗口的
	if (loginFailures.size > 5000) {
		for (const [k, v] of loginFailures) {
			if ((!v.until || now >= v.until) && (now - v.firstAt) > LOGIN_WINDOW_MS) loginFailures.delete(k);
		}
	}
}

function clearLoginFailures(ip) {
	loginFailures.delete(ip);
}

async function checkAdminAuth(request, env) {
	const cred = adminCredentials(env);
	if (!cred.password) return false; // 还没配置管理员密码
	const expectedHash = await adminTokenHash(env);

	// 1. 先从 Cookie 里取登录令牌（浏览器访问时走这里）—— 必须带有效期且未过期
	const cookies = request.headers.get('Cookie') || '';
	const cookieMatch = cookies.match(/admin_token=([^;]+)/);
	if (cookieMatch) {
		const parsed = parseAdminSessionToken(cookieMatch[1]);
		return !!parsed && parsed.hash === expectedHash && Date.now() < parsed.exp;
	}

	// 2. Cookie 里没有的话，再从 Authorization 请求头里取（API 工具调用时走这里）
	//    裸哈希保持兼容（脚本/工具直传）；若带有效期则一并校验
	const authHeader = request.headers.get('Authorization');
	if (authHeader && authHeader.startsWith('Bearer ')) {
		const raw = authHeader.substring(7).trim();
		const parsed = parseAdminSessionToken(raw);
		if (parsed) return parsed.hash === expectedHash && Date.now() < parsed.exp;
		return raw === expectedHash;
	}

	return false;
}

// 校验管理员的登录 Cookie（用于页面访问的权限判断）
async function verifyAdminCookie(request, env) {
	const cookies = request.headers.get('Cookie') || '';
	const cookieMatch = cookies.match(/admin_token=([^;]+)/);
	if (!cookieMatch) return false;

	const cred = adminCredentials(env);
	if (!cred.password) return false;

	const expectedHash = await adminTokenHash(env);
	const parsed = parseAdminSessionToken(cookieMatch[1]);
	return !!parsed && parsed.hash === expectedHash && Date.now() < parsed.exp;
}

// ----------------------------------------------------
// 代理接口的鉴权工具函数
// ----------------------------------------------------
async function checkProxyAuth(request, env) {
	// 触发首次安装自动更新 / 惰性轮换，并刷新配置缓存
	await getApiKeys(env);
	const config = await getAppConfig(env);
	const now = Date.now();

	// 收集当前有效的密钥集合：手动密钥需未过期；系统默认密钥永远有效；
	// 轮换宽限期内的旧系统密钥也临时有效（避免轮换瞬间正在飞的请求被拒）。
	const valid = new Set();
	for (const k of (config.apiKeys || [])) {
		if (!k.expiresAt || k.expiresAt > now) valid.add(k.key);
	}
	if (config.systemApiKey) valid.add(config.systemApiKey);
	if (config.systemApiKeyPrev && config.systemKeyRotatedAt && (now - config.systemKeyRotatedAt) < ROTATE_GRACE_MS) {
		valid.add(config.systemApiKeyPrev);
	}

	if (valid.size === 0) return false; // 系统默认密钥永远存在，正常不会走到这

	// 先检查 x-api-key 头
	const xApiKey = request.headers.get('x-api-key');
	if (xApiKey && valid.has(xApiKey)) {
		return true;
	}

	// 再检查 Authorization: Bearer 头
	const authHeader = request.headers.get('Authorization');
	if (authHeader && authHeader.startsWith('Bearer ')) {
		return valid.has(authHeader.substring(7));
	}

	return false;
}

// ----------------------------------------------------
// 用量统计的缓存工具函数
// ----------------------------------------------------
async function getCachedSummary(env) {
	const cached = await env.KV.get('cache_usage_summary');
	if (cached) {
		try {
			const data = JSON.parse(cached);
			if (Date.now() - data.timestamp < 300000) { // 缓存有效期 5 分钟
				return data;
			}
		} catch (e) { }
	}
	return null;
}

async function setCachedSummary(env, summaryData) {
	const data = {
		...summaryData,
		timestamp: Date.now()
	};
	await env.KV.put('cache_usage_summary', JSON.stringify(data));
}

async function refreshAccountsUsage(env, accounts, limit = 20) {
	const cachedDetailsRaw = await env.KV.get('cache_usage_details');
	let cacheMap = {};
	if (cachedDetailsRaw) {
		try {
			cacheMap = JSON.parse(cachedDetailsRaw) || {};
		} catch (e) {
			cacheMap = {};
		}
	}

	// 按最后更新的时间戳升序排序（时间戳为 0 或不存在的最先更新）
	const sortedAccounts = [...accounts].sort((a, b) => {
		const tA = cacheMap[a.id]?.timestamp || 0;
		const tB = cacheMap[b.id]?.timestamp || 0;
		return tA - tB;
	});

	const accountsToUpdate = sortedAccounts.slice(0, limit);

	const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
	sevenDaysAgo.setUTCHours(0, 0, 0, 0);
	const startSevenDays = sevenDaysAgo.toISOString().split('.')[0] + 'Z';

	const todayUTC = new Date();
	todayUTC.setUTCHours(0, 0, 0, 0);
	const startToday = todayUTC.toISOString().split('.')[0] + 'Z';

	const promises = accountsToUpdate.map(async (account) => {
		try {
			const [todayGroups, historyGroups] = await Promise.all([
				queryGraphQL(account.accountId, account.apiToken, startToday),
				queryGraphQL(account.accountId, account.apiToken, startSevenDays)
			]);
			const todayParsed = processAnalytics(todayGroups);
			const historyParsed = processAnalytics(historyGroups);

			cacheMap[account.id] = {
				status: 'active',
				error: null,
				usageToday: todayParsed.todayTotalNeurons,
				modelsToday: todayParsed.todayModels,
				history: historyParsed.history,
				timestamp: Date.now()
			};
		} catch (e) {
			console.error(`Error querying GraphQL for ${account.name}:`, e);
			cacheMap[account.id] = {
				status: 'error',
				error: e.message,
				usageToday: cacheMap[account.id]?.usageToday || 0,
				modelsToday: cacheMap[account.id]?.modelsToday || [],
				history: cacheMap[account.id]?.history || [],
				timestamp: Date.now() // 即使出错也更新时间戳，以便其他账号轮转刷新
			};
		}
	});

	await Promise.all(promises);
	await env.KV.put('cache_usage_details', JSON.stringify(cacheMap));
	return cacheMap;
}

// ----------------------------------------------------
// Cloudflare GraphQL 用量分析查询
// ----------------------------------------------------
async function queryGraphQL(accountId, apiToken, startDateTime) {
	const query = `
		query GetAIUsage($accountId: String!, $start: String!) {
			viewer {
				accounts(filter: { accountTag: $accountId }) {
					aiInferenceAdaptiveGroups(
						filter: { datetime_geq: $start }
						limit: 1000
					) {
						count
						sum {
							totalNeurons
						}
						dimensions {
							date
							modelId
						}
					}
				}
			}
		}
	`;
	const response = await fetch(`https://api.cloudflare.com/client/v4/graphql`, {
		method: 'POST',
		headers: {
			'Authorization': `Bearer ${apiToken}`,
			'Content-Type': 'application/json'
		},
		body: JSON.stringify({
			query,
			variables: {
				accountId,
				start: startDateTime
			}
		})
	});

	if (!response.ok) {
		throw new Error(`GraphQL API error: ${response.statusText}`);
	}

	const result = await response.json();
	if (result.errors && result.errors.length > 0) {
		throw new Error(result.errors[0].message);
	}

	return result?.data?.viewer?.accounts?.[0]?.aiInferenceAdaptiveGroups || [];
}

function processAnalytics(groups) {
	const todayStr = new Date().toISOString().split('T')[0];

	let todayTotalNeurons = 0;
	const todayModelsMap = {};
	const historyMap = {};

	// 先把最近 7 天的历史数据全部初始化为 0
	for (let i = 6; i >= 0; i--) {
		const d = new Date(Date.now() - i * 24 * 60 * 60 * 1000);
		const dStr = d.toISOString().split('T')[0];
		historyMap[dStr] = 0;
	}

	for (const group of groups) {
		const date = group.dimensions.date;
		const model = group.dimensions.modelId;
		const neurons = group.sum.totalNeurons || 0;
		const count = group.count || 0;

		if (date === todayStr) {
			todayTotalNeurons += neurons;
			if (!todayModelsMap[model]) {
				todayModelsMap[model] = { model, neurons: 0, requests: 0 };
			}
			todayModelsMap[model].neurons += neurons;
			todayModelsMap[model].requests += count;
		}

		if (historyMap[date] !== undefined) {
			historyMap[date] += neurons;
		}
	}

	const todayModels = Object.values(todayModelsMap).sort((a, b) => b.neurons - a.neurons);
	const history = Object.keys(historyMap)
		.sort()
		.map(date => ({ date, neurons: historyMap[date] }));

	return {
		todayTotalNeurons,
		todayModels,
		history
	};
}

// ----------------------------------------------------
// OpenAI 兼容代理接口（/v1/）的处理函数
// ----------------------------------------------------
async function handleV1Proxy(request, env, ctx) {
	const url = new URL(request.url);

	// 1. 校验调用密钥（API Key）
	if (!await checkProxyAuth(request, env)) {
		// /v1/messages 返回 Anthropic 格式错误，其他路径返回 OpenAI 格式
		if (url.pathname === '/v1/messages') {
			return new Response(JSON.stringify({
				type: 'error',
				error: {
					type: 'authentication_error',
					message: 'Invalid x-api-key or Authorization header.'
				}
			}), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
		return new Response(JSON.stringify({
			error: {
				message: "Incorrect or missing API key. Configure keys in the dashboard.",
				type: "invalid_request_error",
				param: null,
				code: "invalid_api_key"
			}
		}), { status: 401, headers: { 'Content-Type': 'application/json' } });
	}

	// 2. 获取模型列表接口（/v1/models）
	if (url.pathname === '/v1/models' && request.method === 'GET') {
		const config = await getAppConfig(env);
		const cfEnabled = config.cfPoolEnabled !== false;
		const customMap = config.customModelMap || {};

		// 账号池关闭时不再列出 CF 模型：既丢掉 CF 预设，也过滤掉存量配置里指向 @cf/ 的映射
		// （KV 首次初始化时会把 DEFAULT_MODEL_MAP 写进 customModelMap，不过滤会漏出来）
		let combinedMap;
		if (cfEnabled) {
			combinedMap = { ...DEFAULT_MODEL_MAP, ...customMap };
		} else {
			combinedMap = {};
			for (const key of Object.keys(customMap)) {
				const target = customMap[key];
				// 账号池关闭时，`@cf/...` 与 `cf/<短名>` 两种写法都不可用（resolveRoute 会直接 400），
				// 一律不列 —— 否则模型列表会「广告」出调不通的模型（列表里显示 cloudflare、实际调用报错）。
				if (typeof target === 'string' && !target.startsWith('@cf/') && !target.startsWith('cf/')) {
					combinedMap[key] = target;
				}
			}
		}
		// 逐条映射开关：被停用的映射不列出来（列了却调不通 = 误导客户端）
		{
			const off = new Set(Array.isArray(config.disabledMappings) ? config.disabledMappings.map(String) : []);
			if (off.size) for (const k of Object.keys(combinedMap)) if (off.has(k)) delete combinedMap[k];
		}
		const providers = (config.providers || []).filter(p => p.status !== 'disabled');

		// owned_by 标明真实来源：客户端拉模型列表时能一眼看出这个模型走哪条上游。
		// （以前这里是随手写的占位值 meta/openai，跟实际来源毫无关系）
		// 存量配置里可能残留指向 @cf/ 的裸名映射（老版内置表写进 KV 的）。它们和 cf/<短名>
		// 完全等价，列出来只是噪音 —— 但用户自己起的别名要保留，所以只过滤"值等于内置预设"的。
		const isLegacyPresetAlias = (key, value) =>
			!key.startsWith('cf/') && typeof value === 'string' && value.startsWith('@cf/')
			&& DEFAULT_MODEL_MAP['cf/' + key] === value;

		// 根据映射目标推导真实来源，而不是统一标 cloudflare（含第三方渠道映射时尤其重要）
		const ownedByOf = (target) => {
			if (typeof target !== 'string') return 'unknown';
			if (target.startsWith('@cf/') || target.startsWith('cf/')) return 'cloudflare';
			if (target.startsWith('TT:')) return 'quota-group';
			const ch = target.startsWith('provider:') ? target.slice('provider:'.length).split('/')[0] : target.split('/')[0];
			return ch || 'unknown';
		};
		// 规格解析：把「调用名」映射回 (渠道, 上游模型名)，好去缓存里取规格。CF 模型没有规格来源，返回 null。
		const providersById = new Map(providers.map(p => [p.id, p]));
		const specTargetOf = (target) => {
			if (typeof target !== 'string') return null;
			const t = target.trim();
			if (t.startsWith('@cf/') || t.startsWith('cf/')) return null;
			if (t.startsWith('TT:')) {
				const g = (config.quotaGroups || []).find(x => x.name === t.slice(3).trim());
				if (!g) return null;
				const members = g.members || [];
				const mem = members.find(x => x.status !== 'disabled') || members[0];
				if (!mem) return null;
				const p = providersById.get(mem.providerId);
				return p ? { provider: p, model: mem.model } : null;
			}
			const parsed = parseMappingTarget(t, providers);
			if (!parsed) return null;
			const p = providersById.get(parsed.providerId);
			return p ? { provider: p, model: parsed.model } : null;
		};

		// 从缓存读某渠道某模型的规格（尚未预热到则 null）
		const specOf = (provider, model) => {
			const hit = modelInfoCache.get('models:' + provider.id);
			const details = hit && hit.data && hit.data.details;
			if (!Array.isArray(details)) return null;
			const want = String(model).toLowerCase();
			return details.find(d => String(d.id).toLowerCase() === want) || null;
		};

		// 把规格摊平成 OpenRouter 风格字段（客户端认哪个用哪个；不认识的字段会被忽略，无害）。
		// 上游不提供价格时回落到渠道「手填单价」，换算成 美元/token 与 OpenRouter 口径一致。
		const specFieldsOf = (spec, provider) => {
			const out = {};
			if (spec) {
				if (spec.name) out.name = spec.name;
				if (spec.description) out.description = spec.description;
				if (spec.contextLength) { out.context_length = spec.contextLength; out.max_input_tokens = spec.contextLength; }
				if (spec.maxOutput) {
					out.max_output_tokens = spec.maxOutput;
					out.top_provider = { max_completion_tokens: spec.maxOutput };
				}
				if (spec.inputModalities || spec.outputModalities) {
					out.architecture = {};
					if (spec.inputModalities) out.architecture.input_modalities = spec.inputModalities;
					if (spec.outputModalities) out.architecture.output_modalities = spec.outputModalities;
				}
				if (spec.supportedParameters) out.supported_parameters = spec.supportedParameters;
				if (spec.pricing && typeof spec.pricing === 'object') out.pricing = spec.pricing;
			}
			if (!out.pricing && provider && provider.pricing) {
				const pin = Number(provider.pricing.inputPer1M);
				const pout = Number(provider.pricing.outputPer1M);
				// 显式 0/0（免费渠道）也是合法定价，应透出（prompt/completion = 0），不能靠真值短路漏掉。
				if (Number.isFinite(pin) || Number.isFinite(pout)) {
					out.pricing = {
						prompt: (Number.isFinite(pin) ? pin : 0) / 1000000,
						completion: (Number.isFinite(pout) ? pout : 0) / 1000000
					};
				}
			}
			return out;
		};

		const modelsData = [];
		const providersToWarm = [];
		const pushModel = (id, ownedBy, st) => {
			modelsData.push(Object.assign({
				id,
				object: 'model',
				created: 1686935000,
				owned_by: ownedBy
			}, specFieldsOf(st ? specOf(st.provider, st.model) : null, st ? st.provider : null)));
			if (st) providersToWarm.push(st.provider);
		};

		for (const id of Object.keys(combinedMap)) {
			if (isLegacyPresetAlias(id, combinedMap[id])) continue;
			pushModel(id, ownedByOf(combinedMap[id]), specTargetOf(combinedMap[id]));
		}

		// 第三方渠道的模型也一并暴露，id 用「渠道名/模型名」形式，可直接拿来调用
		const knownIds = new Set(Object.keys(combinedMap));
		for (const provider of providers) {
			for (const m of provider.models || []) {
				const id = `${provider.name}/${m}`;
				if (!m || knownIds.has(id)) continue;
				knownIds.add(id);
				pushModel(id, provider.name, { provider, model: m });
			}
		}

		// 配额组名同样列出来，客户端可直接调用（实际走组内第一个未满配额的成员）
		for (const g of config.quotaGroups || []) {
			if (g.status === 'disabled' || !g.name || knownIds.has('TT:' + g.name)) continue;
			knownIds.add('TT:' + g.name);
			pushModel('TT:' + g.name, 'quota-group', specTargetOf('TT:' + g.name));
		}

		// 给本次列出、但缓存缺失/过期的渠道丢一次后台预热（不阻塞本次响应；没 waitUntil 就跳过）
		const warmed = new Set();
		for (const p of providersToWarm) {
			if (warmed.has(p.id)) continue;
			warmed.add(p.id);
			warmProviderSpecs(p, ctx);
		}

		return new Response(JSON.stringify({
			object: 'list',
			data: modelsData
		}), { headers: { 'Content-Type': 'application/json' } });
	}

	// 3. 对话补全 / 文本补全 接口
	if ((url.pathname === '/v1/chat/completions' || url.pathname === '/v1/completions') && request.method === 'POST') {
		return handleCompletions(request, env, url.pathname, ctx);
	}

	// 4. Anthropic Messages API 接口（/v1/messages）
	if (url.pathname === '/v1/messages' && request.method === 'POST') {
		return handleMessages(request, env, ctx);
	}


	return new Response(JSON.stringify({
		error: { message: `Path not found: ${url.pathname}`, type: "invalid_request_error" }
	}), { status: 404, headers: { 'Content-Type': 'application/json' } });
}

// ----------------------------------------------------
// 上游调用层：Cloudflare 账号池 + 第三方渠道
// 两类上游都收敛成同一种返回格式，下游的流透传 / 协议转换不需要感知差异。
// 返回格式：{ success: true, data } | { success: true, stream } | { success: false, error }
// ----------------------------------------------------

// Cloudflare 账号池调用：随机打散 + 逐个重试
async function callAccountPool(cfPayload, env, stream) {
	const accounts = await getAccounts(env);
	const activeAccounts = accounts.filter(a => a.status === 'active');
	if (activeAccounts.length === 0) {
		return { success: false, error: "No active Cloudflare accounts configured. Add them in the WebUI." };
	}

	const shuffledAccounts = [...activeAccounts].sort(() => Math.random() - 0.5);
	let lastError = null;
	// 上游 HTTP 状态：4xx 要原样透出（CF 只替换 5xx 响应体），同时供调用方的失败分类使用
	// —— 以前这里完全不回状态，客户端把 CF 的 400/404 一律看成 502，也无法区分「该不该换账号重试」。
	let lastStatus = 0;

	for (const account of shuffledAccounts) {
		// 只对「拿到响应头」设超时，一拿到就撤销 —— 流式开始后可以慢慢流。
		// 免得 CF 边缘等超时后丢一条含糊的 origin_bad_gateway 502。
		let cfResponse;
		let ttfb = 0;
		try {
			const _r = await fetchWithTtfb(
				`https://api.cloudflare.com/client/v4/accounts/${account.accountId}/ai/v1/chat/completions`,
				{
					method: 'POST',
					headers: {
						'Authorization': `Bearer ${account.apiToken}`,
						'Content-Type': 'application/json',
					},
					body: JSON.stringify(cfPayload),
				}
			);
			cfResponse = _r.response;
			ttfb = _r.ttfb;

			if (cfResponse.ok) {
				if (stream) {
					return { success: true, stream: cfResponse.body, ttfb };
				} else {
					const cfJson = await cfResponse.json();
					// CF Workers AI 偶发以 200 + { success:false, errors:[...] } 返回业务错误
					// （模型被限流 / 配额耗尽等），不能只看 HTTP 状态码就当成功——
					// 否则会被当成功透传、统计计成 ok，还把错误响应当正常结果返回给客户端。
					if (cfJson && cfJson.success === false) {
						const errMsg = (cfJson.errors && cfJson.errors[0] && cfJson.errors[0].message)
							|| (cfJson.messages && cfJson.messages[0])
							|| (typeof cfJson.message === 'string' ? cfJson.message : null)
							|| JSON.stringify(cfJson).slice(0, 300);
						lastError = `CF API returned success:false: ${errMsg}`;
						// 落到循环末尾继续尝试下一个账号（与 !ok 分支一致）
					} else {
						return { success: true, data: cfJson };
					}
				}
			} else {
				const errorText = await cfResponse.text();
				lastStatus = cfResponse.status;
				lastError = `CF API returned ${cfResponse.status}: ${errorText}`;
			}
		} catch (e) {
			lastError = (e && e.isTimeout)
				? `CF API did not respond within ${Math.round(PROVIDER_TIMEOUT_MS / 1000)}s (timeout)`
				: `Connection error: ${e.message}`;
		}
	}

	return {
		success: false,
		error: `All Cloudflare accounts failed. Last error: ${lastError}`,
		upstreamStatus: lastStatus || 0,
		status: (lastStatus >= 400 && lastStatus < 500) ? lastStatus : 502
	};
}

// 第三方渠道调用：上游必须是 OpenAI 兼容端点，响应格式与 CF 一致，可直接透传
// ----------------------------------------------------
// 第三方渠道调用统计（D1）
// CF 账号池的用量来自 Cloudflare 官方账单，不需要自建统计；
// 这里**只记第三方渠道**，两者在看板上是独立的两块。
// ----------------------------------------------------

const STATS_TABLE_SQL = `CREATE TABLE IF NOT EXISTS stats (
	day TEXT NOT NULL,
	provider_id TEXT NOT NULL,
	provider_name TEXT NOT NULL,
	model TEXT NOT NULL,
	req INTEGER NOT NULL DEFAULT 0,
	ok INTEGER NOT NULL DEFAULT 0,
	fail INTEGER NOT NULL DEFAULT 0,
	ms_total INTEGER NOT NULL DEFAULT 0,
	probe_req INTEGER NOT NULL DEFAULT 0,
	probe_ok INTEGER NOT NULL DEFAULT 0,
	probe_fail INTEGER NOT NULL DEFAULT 0,
	probe_ms_total INTEGER NOT NULL DEFAULT 0,
	tokens INTEGER NOT NULL DEFAULT 0,
	reasoning_tokens INTEGER NOT NULL DEFAULT 0,
	input_tokens INTEGER NOT NULL DEFAULT 0,
	output_tokens INTEGER NOT NULL DEFAULT 0,
	last_ms INTEGER NOT NULL DEFAULT 0,
	last_at TEXT,
	PRIMARY KEY (day, provider_id, model)
)`;

let statsTableReady = false;
let statsTableRetryAfter = 0;

// 首次用到时自动建表 —— 用户只需建库 + 绑 DB 变量，不必手动跑 SQL
async function ensureStatsTable(env) {
	if (statsTableReady || !env.DB) return;
	if (Date.now() < statsTableRetryAfter) return;
	try {
		await env.DB.prepare(STATS_TABLE_SQL).run();
		// 老表补列：测试/探测调用单独计数。
		// ★ 先 PRAGMA 查一次现有列，只 ALTER 缺的那些（2026-10-08 优化）：
		//   旧实现无条件跑 10 条 ALTER，列已存在时**每条都必然抛错** → 冷启动白花 10 次 D1 往返。
		//   新表由 STATS_TABLE_SQL 一次建全 → 这里 0 条 ALTER，冷路径只剩 1×CREATE + 1×PRAGMA。
		const wantCols = [
			['probe_req', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_ok', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_fail', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_ms_total', 'INTEGER NOT NULL DEFAULT 0'],
			['tokens', 'INTEGER NOT NULL DEFAULT 0'],
			['reasoning_tokens', 'INTEGER NOT NULL DEFAULT 0'],
			// 成本估算要按输入/输出各自单价算，故分开存（老数据为 0 → 成本侧自动回落粗估）
			['input_tokens', 'INTEGER NOT NULL DEFAULT 0'],
			['output_tokens', 'INTEGER NOT NULL DEFAULT 0'],
			['last_ms', 'INTEGER NOT NULL DEFAULT 0'],
			['last_at', 'TEXT']
		];
		let have = null;
		try {
			const info = await env.DB.prepare('PRAGMA table_info(stats)').all();
			if (info && Array.isArray(info.results)) have = new Set(info.results.map(r => String(r.name)));
		} catch (_) { have = null; } // PRAGMA 不可用 → 退回逐条试着加（老行为，仍正确）
		for (const [col, def] of wantCols) {
			if (have && have.has(col)) continue;
			try {
				await env.DB.prepare(`ALTER TABLE stats ADD COLUMN ${col} ${def}`).run();
			} catch (e) { /* 列已存在 */ }
		}
		statsTableReady = true;
	} catch (e) {
		// 建表失败（权限 / 瞬时故障）：5 分钟内不重试，免得每个请求都白跑一次 DDL
		statsTableRetryAfter = Date.now() + 300000;
	}
}

// 记录一次渠道调用。统计是旁路：
//   - 不阻塞响应（交给 ctx.waitUntil）
//   - 失败只丢这一条计数，绝不影响代理本身
// isProbe = true 表示这是「测试连通 / 测模型」这类探测调用 —— 单独计数（probe_*），
// 但配额判断会把两者相加，因为上游是按总请求数算额度的，测试同样消耗它。
function recordProviderCall(env, ctx, provider, model, ok, ms, isProbe, tokens, reasoningTokens, inputTokens, outputTokens) {
	if (!env.DB) return;
	// 桶是「小时」而不是「天」：配额组的重置时刻可以不在 UTC 零点（如 Gemini 是太平洋午夜
	// = UTC 07:00），只有小时级的桶才能把周期边界切准。列名仍叫 day（SQLite 改主键成本高），
	// 值形如 2026-10-04T07 —— 老数据是纯日期串，字符串比较下仍能被看板的 >= 范围查询覆盖。
	const day = new Date().toISOString().slice(0, 13);
	const probe = isProbe ? 1 : 0;
	const msRounded = Math.max(0, Math.round(Number(ms) || 0));
	const tokensRounded = Math.max(0, Math.round(Number(tokens) || 0));
	const reasoningRounded = Math.max(0, Math.round(Number(reasoningTokens) || 0));
	const inputRounded = Math.max(0, Math.round(Number(inputTokens) || 0));
	const outputRounded = Math.max(0, Math.round(Number(outputTokens) || 0));
	const task = (async () => {
		await ensureStatsTable(env);
		await env.DB.prepare(
			`INSERT INTO stats (day, provider_id, provider_name, model,
			                    req, ok, fail, ms_total,
			                    probe_req, probe_ok, probe_fail, probe_ms_total, tokens, reasoning_tokens,
			                    input_tokens, output_tokens,
			                    last_ms, last_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(day, provider_id, model) DO UPDATE SET
			   provider_name = excluded.provider_name,
			   req = req + excluded.req,
			   ok = ok + excluded.ok,
			   fail = fail + excluded.fail,
			   ms_total = ms_total + excluded.ms_total,
			   probe_req = probe_req + excluded.probe_req,
			   probe_ok = probe_ok + excluded.probe_ok,
			   probe_fail = probe_fail + excluded.probe_fail,
			   probe_ms_total = probe_ms_total + excluded.probe_ms_total,
			   tokens = tokens + excluded.tokens,
			   reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens,
			   input_tokens = input_tokens + excluded.input_tokens,
			   output_tokens = output_tokens + excluded.output_tokens,
			   last_ms = CASE WHEN (excluded.ok = 1 OR excluded.probe_ok = 1) THEN excluded.last_ms ELSE last_ms END,
			   last_at = CASE WHEN (excluded.ok = 1 OR excluded.probe_ok = 1) THEN excluded.last_at ELSE last_at END`
		).bind(
			day,
			String(provider.id),
			String(provider.name),
			String(model || ''),
			probe ? 0 : 1,
			probe ? 0 : (ok ? 1 : 0),
			probe ? 0 : (ok ? 0 : 1),
			probe ? 0 : msRounded,
			probe ? 1 : 0,
			probe ? (ok ? 1 : 0) : 0,
			probe ? (ok ? 0 : 1) : 0,
			probe ? msRounded : 0,
			probe ? 0 : tokensRounded,
			probe ? 0 : reasoningRounded,
			probe ? 0 : inputRounded,
			probe ? 0 : outputRounded,
			// 最近一次 = 覆盖写：转发和探测都算（后台「重测」的延迟要能立刻反映到看板）
			msRounded,
			new Date().toISOString()
		).run();
	})().catch(() => { });
	if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(task);
}

// 非流式响应里的 token 数（OpenAI 与 Gemini 适配层产出的都是标准 usage 字段）。
// 额外抽 reasoning_tokens（思考 token），兼容三种上游写法：
//   OpenAI o 系列 → usage.reasoning_tokens；或 usage.completion_tokens_details.reasoning_tokens
//   Anthropic    → usage.output_tokens_details.reasoning_tokens
//   Gemini 原生   → 由 geminiResponseToOpenAI 直接塞进 usage.reasoning_tokens
function usageOf(result) {
	const u = result && result.data && result.data.usage;
	if (!u) return { tokens: 0, reasoningTokens: 0, inputTokens: 0, outputTokens: 0 };
	const tokens = Number(u.total_tokens || u.totalTokens || 0) || 0;
	let reasoning = 0;
	if (u.reasoning_tokens) reasoning = Number(u.reasoning_tokens) || 0;
	else if (u.completion_tokens_details && u.completion_tokens_details.reasoning_tokens) reasoning = Number(u.completion_tokens_details.reasoning_tokens) || 0;
	else if (u.output_tokens_details && u.output_tokens_details.reasoning_tokens) reasoning = Number(u.output_tokens_details.reasoning_tokens) || 0;
	// 输入/输出分开记（成本估算按各自单价算）；缺字段留 0，成本侧自动回落粗估
	const inputTokens = Number(u.prompt_tokens || u.input_tokens || 0) || 0;
	const outputTokens = Number(u.completion_tokens || u.output_tokens || 0) || 0;
	return { tokens, reasoningTokens: reasoning, inputTokens, outputTokens };
}

// 流式响应探针：只旁路观察，**不改动任何字节**。
// 上游（OpenAI 透传 / Gemini 原生适配层）产出的都是标准 OpenAI chunk，
// 末尾的 usage 块里有 total_tokens；流结束时回调一次，用来补记 token 统计。
function withUsageTap(stream, onUsage) {
	const reader = stream.getReader();
	const decoder = new TextDecoder();
	let buf = '';
	let maxTotal = 0;
	let maxReasoning = 0;
	let maxInput = 0;
	let maxOutput = 0;
	// ★ 一次性闸门（2026-10-09）：落库回调只许跑一次。
	//   客户端中途断开时，cancel() 与背景循环收尾的 finally 都会跑到 —— 实测各调一次
	//   （见 _verify_stats.mjs「中断流只记一次」），于是请求数 +2、token 翻倍。
	//   两者都改走 fire()，谁先到谁生效，后到的被忽略。
	let fired = false;
	const fire = () => {
		if (fired) return;
		fired = true;
		if (!onUsage) return;   // Opt3 后此函数只服务转换路径；没挂回调 = 纯包装，直接跳过
		try {
			onUsage({ tokens: maxTotal, reasoningTokens: maxReasoning, inputTokens: maxInput, outputTokens: maxOutput });
		} catch (e) { /* 统计失败绝不影响请求 */ }
	};
	return new ReadableStream({
		// 铁律：Worker 上必须 start 主动排空，绝不用裸 pull（见 MEMORY.md 流式约定）
		start(controller) {
			(async () => {
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) break;
						try {
							buf += decoder.decode(value, { stream: true });
							let nl;
							while ((nl = buf.indexOf('\n')) !== -1) {
								const line = buf.slice(0, nl);
								buf = buf.slice(nl + 1);
								if (line.indexOf('data:') !== 0) continue;
								const s = line.slice(5).trim();
								if (!s || s === '[DONE]') continue;
								if (s.indexOf('tokens') === -1) continue; // Opt1: 只含 token 字段的行才跑正则（内容增量块几乎都不含，省掉 ~95% 正则执行）
								const m = /"total_tokens"\s*:\s*(\d+)/.exec(s);
								if (m) maxTotal = Math.max(maxTotal, Number(m[1]) || 0);
								const mr = /"reasoning_tokens"\s*:\s*(\d+)/.exec(s);
								if (mr) maxReasoning = Math.max(maxReasoning, Number(mr[1]) || 0);
								const mi = /"prompt_tokens"\s*:\s*(\d+)/.exec(s);
								if (mi) maxInput = Math.max(maxInput, Number(mi[1]) || 0);
								const mo = /"completion_tokens"\s*:\s*(\d+)/.exec(s);
								if (mo) maxOutput = Math.max(maxOutput, Number(mo[1]) || 0);
							}
							if (buf.length > 200000) buf = buf.slice(-4096); // 防异常上游把缓冲撑爆
						} catch (e) { /* 探测失败不影响转发 */ }
						controller.enqueue(value);
					}
					try { controller.close(); } catch (e) { }
				} catch (e) {
					try { controller.error(e); } catch (_) { }
				} finally {
					fire();
				}
			})();
		},
		cancel(reason) {
			try { reader.cancel(reason); } catch (e) { }
			// 回调必须与 start 收尾一致（同一个对象形状、同一个 fire 闸门），
			// 否则调用方读 uo.tokens 恒为 undefined → 中断的流 token 记 0
			fire();
		},
	});
}

// 落地页公开汇总接口（/api/public/provider-stats）的 isolate 内缓存，30s TTL，避免公开端点被刷穿 D1
let publicProviderSummaryCache = null;

// 管理看板统计的 isolate 内短缓存（默认 8s；env.STATS_CACHE_MS 可覆盖，0=关闭，验证脚本用 0）。
// 面板「第三方渠道 / 数据看板」来回切会反复拉同一份数据 —— 缓存后几乎瞬开、不再打 D1（2026-10-08）。
const statsCache = new Map();
function statsCacheMs(env) {
	const v = Number(env && env.STATS_CACHE_MS);
	return Number.isFinite(v) && v >= 0 ? v : 8000;
}

// 第三方渠道统计：总览 + 按渠道（含各模型明细）
// 数据全部来自本代理埋点，与 CF 官方账单是两条独立链路
async function queryProviderStats(env, range, includeInactive = false) {
	if (!env.DB) return { enabled: false, reason: 'no-db' };
	const cacheMs = statsCacheMs(env);
	const cacheKey = 'stats:' + range + ':' + (includeInactive ? 1 : 0);
	if (cacheMs > 0) {
		const hit = statsCache.get(cacheKey);
		if (hit) { if (Date.now() < hit.expiry) return hit.value; statsCache.delete(cacheKey); }
	}
	await ensureStatsTable(env);
	// 第三方渠道「估算成本」全局粗估单价（美元 / 千 token），仅当渠道没手填价、也没上游真实价时兜底。
	// 2026-10-10 由 0.011（$11/百万）下调到 0.0005（$0.5/百万）：旧值对「主力走 Gemini 免费层 / Flash」
	// 场景系统性高估 10~30 倍（那类模型真实价 ~$0.1~0.4/百万，免费层甚至为 0）。要更准优先手填渠道单价，
	// 或设环境变量 THIRD_PARTY_EST_COST_PER_1K 覆盖。纯示意估算，非真实账单。
	const estRate = Number(env && env.THIRD_PARTY_EST_COST_PER_1K) || 0.0005;

	// 单价来源优先级：① 渠道手填单价 → ② 上游 /models 缓存里的真实价（OpenRouter 等）→ ③ 全局粗估 estRate。
	// 手填价让 Gemini / Agnes 这类上游不公开价格的渠道也能算得比较准。
	const providersForPricing = await getProviders(env);
	// 已停用/已删除的渠道不计入看板：删渠道只清映射、不动 stats 表，历史行仍留着；
	// 这里在查询侧过滤，today/7d/all 一律默认隐藏。审计时传 includeInactive=1 才显示。
	// 注意：若读不到渠道配置（haveConfig=false，如 KV 暂不可读），则不隐藏任何行——
	// 宁可多显示、也不要把真实数据静默藏掉。
	const provList = providersForPricing || [];
	const haveConfig = provList.length > 0;
	const providerIds = new Set(provList.map(p => String(p.id)));
	const disabledIds = new Set(provList.filter(p => p.status === 'disabled').map(p => String(p.id)));
	const isProviderVisible = (pid) => includeInactive || !haveConfig
		|| (providerIds.has(String(pid)) && !disabledIds.has(String(pid)));

	// 显式隐藏的模型（B 方案）：用户在看板逐行「隐藏」后落入 config.hiddenModels（模型名全局隐藏）。
	// 与渠道隐藏同构逻辑——默认藏、includeInactive=1（含已停用渠道/隐藏模型）时一并显示，便于审计恢复。
	const cfgHidden = await getAppConfig(env);
	const hiddenModels = new Set(Array.isArray(cfgHidden.hiddenModels) ? cfgHidden.hiddenModels.map(String) : []);
	const isModelVisible = (m) => includeInactive || !hiddenModels.has(String(m));
	const manualPriceOf = (pid) => {
		const p = (providersForPricing || []).find(x => x.id === String(pid));
		const pr = p && p.pricing;
		// 2026-10-10：pricing 里至少一个字段是显式数字（含 0）＝手填生效。
		// 0/0 = 免费渠道 → 成本按 0 算，不回落上游价 / 粗估；负数按 0 处理，防负成本。
		const field = (v) => {
			if (v === undefined || v === null || v === '') return null;
			const n = Number(v);
			return Number.isFinite(n) && n >= 0 ? n : null;
		};
		const inN = pr ? field(pr.inputPer1M) : null;
		const outN = pr ? field(pr.outputPer1M) : null;
		if (pr && typeof pr === 'object' && (inN !== null || outN !== null)) {
			return { inputPer1M: inN || 0, outputPer1M: outN || 0 };
		}
		return null;
	};
	const upstreamPriceOf = (pid, model) => {
		const hit = modelInfoCache.get('models:' + String(pid));
		const details = hit && hit.data && hit.data.details;
		if (!Array.isArray(details)) return null;
		const d = details.find(x => x.id === String(model));
		if (!d || !d.pricing) return null;
		// 上游 pricing 是「美元 / token」（OpenRouter 风格），换算成 /百万 tokens
		const inputPer1M = (Number(d.pricing.prompt) || 0) * 1000000;
		const outputPer1M = (Number(d.pricing.completion) || 0) * 1000000;
		if (!inputPer1M && !outputPer1M) return null;
		return { inputPer1M, outputPer1M };
	};
	// 有单价且这行有入/出拆分 → 按各自单价精确算；否则（无价 / 老数据没拆）沿用原来的「总 token × 粗估」
	const costOf = (pid, model, o) => {
		const inputTokens = Number(o.inputTokens) || 0;
		const outputTokens = Number(o.outputTokens) || 0;
		const total = Number(o.tokens) || 0;
		const price = manualPriceOf(pid) || upstreamPriceOf(pid, model);
		// 显式 0/0（手填「免费渠道」）→ 成本恒 0：连没拆入/出的老数据也不吃粗估。
		// upstreamPriceOf 不可能返回 0/0，这支短路只会被手填价触发。
		if (price && !(price.inputPer1M > 0) && !(price.outputPer1M > 0)) return 0;
		if (price && (inputTokens + outputTokens) > 0) {
			return Math.round((inputTokens / 1000000 * price.inputPer1M + outputTokens / 1000000 * price.outputPer1M) * 100) / 100;
		}
		return Math.round(total / 1000 * estRate * 100) / 100;
	};

	const today = new Date().toISOString().slice(0, 10);
	let sinceDay = null;
	if (range === 'today') sinceDay = today;
	else if (range === '7d') sinceDay = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);

	// ★ 单次 D1 查询同时喂三块（rows / trend / today）—— 原来 3 条串行往返是「切 tab 慢」的主因（2026-10-08）。
	//   窗口取三者的并集：today/7d 都只需近 7 天（趋势固定 7 天窗口），all 需全部 → 不设 day 过滤。
	//   三块聚合（按 渠道+模型 / 按 模型+日 / 今日按模型）改在 JS 里算 —— 行数 = 渠道×日×模型，很小。
	const weekStart = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);
	const dayList = [];
	for (let i = 6; i >= 0; i--) dayList.push(new Date(Date.now() - i * 86400000).toISOString().slice(0, 10));
	const mergedSince = sinceDay ? weekStart : null; // 有 range 过滤时 weekStart ≤ sinceDay，覆盖三者
	const rawStmt = env.DB.prepare(
		'SELECT provider_id, provider_name, model, substr(day,1,10) AS d, req, ok, fail, ms_total AS msTotal,'
		+ ' probe_req AS probeReq, probe_ok AS probeOk, probe_fail AS probeFail, probe_ms_total AS probeMsTotal,'
		+ ' tokens, reasoning_tokens AS reasoningTokens, input_tokens AS inputTokens, output_tokens AS outputTokens,'
		+ ' last_at AS lastAt, last_ms AS lastMs FROM stats'
		+ (mergedSince ? ' WHERE day >= ?' : '')
	);
	const rawRows = ((mergedSince ? await rawStmt.bind(mergedSince).all() : await rawStmt.all()).results) || [];

	// 三块聚合桶
	const rowAgg = new Map();        // provider_id|model → rows 行
	const trendByModel = new Map();  // model → { [day]: tokens }
	const todayAgg = new Map();      // model → { model, tokens, req, probeReq }
	for (const r of rawRows) {
		// 可见性过滤（与原三处 filter 等价；includeInactive 时两个判断恒真）
		if (!isProviderVisible(r.provider_id) || !isModelVisible(r.model)) continue;
		const pid = String(r.provider_id);
		const d = String(r.d);
		// ① rows：受 range 限制
		if (!sinceDay || d >= sinceDay) {
			const k = pid + '\u0000' + String(r.model);
			let cur = rowAgg.get(k);
			if (!cur) {
				cur = {
					provider_id: pid, provider_name: r.provider_name, model: r.model,
					req: 0, ok: 0, fail: 0, msTotal: 0, probeReq: 0, probeOk: 0, probeFail: 0, probeMsTotal: 0,
					tokens: 0, reasoningTokens: 0, inputTokens: 0, outputTokens: 0, lastAt: null, lastMs: 0
				};
				rowAgg.set(k, cur);
			}
			cur.req += r.req || 0; cur.ok += r.ok || 0; cur.fail += r.fail || 0;
			cur.msTotal += r.msTotal || 0; cur.probeReq += r.probeReq || 0; cur.probeOk += r.probeOk || 0;
			cur.probeFail += r.probeFail || 0; cur.probeMsTotal += r.probeMsTotal || 0;
			cur.tokens += r.tokens || 0; cur.reasoningTokens += r.reasoningTokens || 0;
			cur.inputTokens += r.inputTokens || 0; cur.outputTokens += r.outputTokens || 0;
			// 等价原 SQL 的「MAX(last_at) + 裸列 last_ms」：取 last_at 最新那一行的 last_ms
			if (r.lastAt && (!cur.lastAt || String(r.lastAt) >= String(cur.lastAt))) { cur.lastAt = r.lastAt; cur.lastMs = r.lastMs || 0; }
		}
		// ② 趋势 / ③ 今日：固定近 7 天窗口
		if (d >= weekStart) {
			let tm = trendByModel.get(r.model);
			if (!tm) { tm = {}; trendByModel.set(r.model, tm); }
			tm[d] = (tm[d] || 0) + (r.tokens || 0);
			if (d === today) {
				let ta = todayAgg.get(r.model);
				if (!ta) { ta = { model: r.model, tokens: 0, req: 0, probeReq: 0 }; todayAgg.set(r.model, ta); }
				ta.tokens += r.tokens || 0; ta.req += r.req || 0; ta.probeReq += r.probeReq || 0;
			}
		}
	}
	const rows = [...rowAgg.values()].sort((a, b) => (b.req || 0) - (a.req || 0));
	// 今日按模型合并回一行（同名模型不能按渠道拆成多条 —— .84 回归）
	const todayRows = [...todayAgg.values()].sort((a, b) => (b.tokens - a.tokens) || (b.req - a.req));

	// 把「渠道 + 模型」的扁平行聚成两层结构
	const byProvider = new Map();
	for (const r of rows) {
		const key = String(r.provider_id);
		if (!byProvider.has(key)) {
			byProvider.set(key, { id: key, name: r.provider_name, req: 0, ok: 0, fail: 0, msTotal: 0, probeReq: 0, probeOk: 0, probeFail: 0, probeMsTotal: 0, tokens: 0, reasoningTokens: 0, inputTokens: 0, outputTokens: 0, costEst: 0, models: [] });
		}
		const p = byProvider.get(key);
		p.req += r.req || 0;
		p.ok += r.ok || 0;
		p.fail += r.fail || 0;
		p.msTotal += r.msTotal || 0;
		p.probeReq += r.probeReq || 0;
		p.probeOk += r.probeOk || 0;
		p.probeFail += r.probeFail || 0;
		p.probeMsTotal += r.probeMsTotal || 0;
		p.tokens += r.tokens || 0;
		p.reasoningTokens += r.reasoningTokens || 0;
		p.inputTokens += r.inputTokens || 0;
		p.outputTokens += r.outputTokens || 0;
		// 渠道级「最近一次」= 各模型里时刻最新的那个
		if (r.lastAt && (!p.lastAt || r.lastAt > p.lastAt)) { p.lastAt = r.lastAt; p.lastMs = r.lastMs || 0; }
		if (r.provider_name) p.name = r.provider_name;
		// 成本按「模型」逐条算（单价是模型级的），渠道成本 = 各模型成本之和
		const modelCost = costOf(r.provider_id, r.model, r);
		p.costEst += modelCost;
		p.models.push({
			model: r.model,
			hidden: hiddenModels.has(String(r.model)),
			// 与 shape 同口径：请求数/成功失败/平均延迟并入探测（.47）
			req: (r.req || 0) + (r.probeReq || 0),
			ok: (r.ok || 0) + (r.probeOk || 0),
			fail: (r.fail || 0) + (r.probeFail || 0),
			avgMs: ((r.req || 0) + (r.probeReq || 0)) ? Math.round(((r.msTotal || 0) + (r.probeMsTotal || 0)) / ((r.req || 0) + (r.probeReq || 0))) : 0,
			lastMs: r.lastMs || 0,
			lastAt: r.lastAt || null,
			tokens: r.tokens || 0,
			reasoningTokens: r.reasoningTokens || 0,
			inputTokens: r.inputTokens || 0,
			outputTokens: r.outputTokens || 0,
			costEst: modelCost,
			probeReq: r.probeReq || 0,
			probeOk: r.probeOk || 0,
			probeFail: r.probeFail || 0,
			probeAvgMs: r.probeReq ? Math.round((r.probeMsTotal || 0) / r.probeReq) : 0
		});
	}

	const shape = (o, costOverride) => ({
		// req/ok/fail/avgMs 已并入探测调用（probe_*），与配额面板、占比环形图口径一致（.47）
		req: (o.req || 0) + (o.probeReq || 0),
		ok: (o.ok || 0) + (o.probeOk || 0),
		fail: (o.fail || 0) + (o.probeFail || 0),
		okRate: ((o.req || 0) + (o.probeReq || 0)) ? Math.round(((o.ok || 0) + (o.probeOk || 0)) / ((o.req || 0) + (o.probeReq || 0)) * 1000) / 10 : 0,
		avgMs: ((o.req || 0) + (o.probeReq || 0)) ? Math.round(((o.msTotal || 0) + (o.probeMsTotal || 0)) / ((o.req || 0) + (o.probeReq || 0))) : 0,
		tokens: o.tokens || 0,
		reasoningTokens: o.reasoningTokens || 0,
		inputTokens: o.inputTokens || 0,
		outputTokens: o.outputTokens || 0,
		costEst: costOverride != null ? costOverride : Math.round((o.tokens || 0) / 1000 * estRate * 100) / 100,
		probeReq: o.probeReq || 0,
		probeOk: o.probeOk || 0,
		probeFail: o.probeFail || 0,
		probeAvgMs: o.probeReq ? Math.round((o.probeMsTotal || 0) / o.probeReq) : 0
	});

	const providerList = [...byProvider.values()];
	const totalCostEst = Math.round(providerList.reduce((s, p) => s + (p.costEst || 0), 0) * 100) / 100;

	// summary 由过滤后的 providerList 聚合，与明细口径一致（含已停用开关）
	const summaryAgg = providerList.reduce((a, p) => {
		a.req += p.req || 0; a.ok += p.ok || 0; a.fail += p.fail || 0;
		a.msTotal += p.msTotal || 0; a.probeReq += p.probeReq || 0; a.probeOk += p.probeOk || 0;
		a.probeFail += p.probeFail || 0; a.probeMsTotal += p.probeMsTotal || 0;
		a.tokens += p.tokens || 0; a.reasoningTokens += p.reasoningTokens || 0;
		a.inputTokens += p.inputTokens || 0; a.outputTokens += p.outputTokens || 0;
		return a;
	}, { req: 0, ok: 0, fail: 0, msTotal: 0, probeReq: 0, probeOk: 0, probeFail: 0, probeMsTotal: 0, tokens: 0, reasoningTokens: 0, inputTokens: 0, outputTokens: 0 });

	const out = {
		enabled: true,
		range,
		sinceDay,
		today,
		summary: shape(summaryAgg, totalCostEst),
		providers: providerList.map(p => ({ id: p.id, name: p.name, ...shape(p, p.costEst), lastMs: p.lastMs || 0, lastAt: p.lastAt || null, models: p.models })),
		trend: {
			days: dayList,
			series: [...trendByModel.entries()].map(([model, byDay]) => ({ model, data: dayList.map(d => byDay[d] || 0) }))
		},
		todayByModel: todayRows.map(r => ({ model: r.model, tokens: r.tokens || 0, req: (r.req || 0) + (r.probeReq || 0) }))
	};
	if (cacheMs > 0) statsCache.set(cacheKey, { expiry: Date.now() + cacheMs, value: out });
	return out;
}

// ============================================================
// Gemini 原生适配层（2026-10-05，参考 Wei-Shaw/sub2api 的 antigravity 包）
// 背景：Google 的 OpenAI 兼容端点（…/v1beta/openai）对工具调用支持不完整 ——
//   历史含 role:"tool" 会静默断流、flash-lite 会把工具调用当文本吐出来。
// 做法：Gemini 渠道不再走兼容端点，改走**原生 generateContent**，自己做
//   OpenAI ⇄ Gemini 的协议转换（messages / tools / functionCall / 流式）。
// 开关：provider.geminiNative === false 可强制回落旧（兼容端点）路径。
// 产出的是**标准 OpenAI 对象 / SSE**，因此下游 passthroughStream（含模型名回写、
//   补 [DONE]）、withSseHeartbeat、anthropicStreamTransform 全部原样复用，协议层零改动。
// 移植自 sub2api 的原生坑位（其 schema_cleaner / request_transformer 实证）：
//   ① toolConfig 必须始终带；② 工具 schema 要先清理；③ 空 schema 补小写 type 的 object。
// ============================================================

function isGeminiProvider(provider) {
	if (!GEMINI_NATIVE_ENABLED) return false;
	if (!provider || provider.geminiNative === false) return false;
	if (provider.geminiNative === true) return true;
	return /generativelanguage\.googleapis\.com/i.test(String(provider.baseUrl || ''));
}

// …/v1beta/openai → …/v1beta（原生要的是 models/{m}:generateContent）
function geminiNativeBase(baseUrl) {
	return String(baseUrl || '').replace(/\/+$/, '').replace(/\/openai$/i, '');
}

// Gemini 的 function 参数 schema 很挑：展开 $ref/$defs、剔掉不支持的字段、保证是「小写 type 的 object」
// ★ Gemini 的 Schema 只接受 JSON-Schema 的一个**子集** —— 这里用**白名单**：
//   只保留官方 Schema 支持的字段，其余一律丢掉。
//   踩过的坑（2026-10-06 用户实际报错）：客户端工具 schema 里带了 `exclusiveMinimum`，
//   而旧实现是「黑名单」（列到的才删）→ 漏掉它 → 上游直接 400
//   「Invalid JSON payload received. Unknown name "exclusiveMinimum" at
//     tools[0].function_declarations[3].parameters.properties[2].value」。
//   白名单能一劳永逸挡住未来任何未知关键字（exclusiveMinimum/Maximum、const、oneOf…）。
const GEMINI_SCHEMA_KEEP = new Set([
	'type', 'format', 'title', 'description', 'nullable', 'default',
	'items', 'minItems', 'maxItems', 'enum', 'properties', 'required',
	'minProperties', 'maxProperties', 'minimum', 'maximum',
	'minLength', 'maxLength', 'pattern', 'example', 'anyOf', 'propertyOrdering',
]);
// 这些键的**值是数据、不是 schema** —— 原样保留，绝不下钻清洗
// （否则 default:{a:1} / example:{...} 会被当成 schema 洗成空对象）
const GEMINI_SCHEMA_RAW_VALUE_KEYS = new Set(['default', 'example', 'enum']);

function cleanGeminiSchema(schema) {
	// ★ 数组也必须是「非法输入」：typeof [] === 'object' 会骗过守卫，数组 walk 出来再 JSON 化
	//   会丢属性 → functionDeclarations[].parameters 变数组 → Gemini 400。
	if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return { type: 'object', properties: {} };
	const defs = Object.assign({}, schema.$defs || {}, schema.definitions || {});
	const walk = (node) => {
		if (Array.isArray(node)) return node.map(walk);
		if (!node || typeof node !== 'object') return node;
		const out = {};
		for (const k of Object.keys(node)) {
			const v = node[k];
			if (v === undefined) continue;
			if (k === '$ref') {
				const refName = String(v).split('/').pop();
				if (defs[refName] && typeof defs[refName] === 'object') Object.assign(out, walk(defs[refName]));
				continue;
			}
			// ★ 白名单：不在名单里的键一律丢掉（这才是挡住 exclusiveMinimum 的关键）
			if (!GEMINI_SCHEMA_KEEP.has(k)) continue;
			// ★ properties 是「任意属性名 → schema」的映射：属性名不是关键字，
			//   不能过白名单（否则 city/value 这些名字会被当成未知关键字删掉，schema 变空）。
			if (k === 'properties') {
				const props = {};
				if (v && typeof v === 'object' && !Array.isArray(v)) {
					for (const pk of Object.keys(v)) props[pk] = walk(v[pk]);
				}
				out.properties = props;
				continue;
			}
			if (k === 'type') {
				// OpenAI 允许 type: ['string','null']；Gemini 只认单个类型 + nullable
				if (Array.isArray(v)) {
					const arr = v.filter(t => String(t == null ? '' : t).toLowerCase() !== 'null');
					if (arr.length !== v.length) out.nullable = true;
					out.type = String(arr[0] == null ? 'object' : arr[0]).toLowerCase();
				} else if (v != null) {
					out.type = String(v).toLowerCase();
				}
				continue;
			}
			if (GEMINI_SCHEMA_RAW_VALUE_KEYS.has(k)) { out[k] = v; continue; }
			out[k] = walk(v);
		}
		return out;
	};
	const cleaned = walk(schema);
	if (!cleaned.type) cleaned.type = 'object';
	cleaned.type = String(cleaned.type).toLowerCase();
	if (cleaned.type === 'object' && (!cleaned.properties || typeof cleaned.properties !== 'object')) {
		cleaned.properties = {};
	}
	return cleaned;
}

function openaiContentToText(content) {
	if (content == null) return '';
	if (typeof content === 'string') return content;
	if (Array.isArray(content)) {
		return content.map(p => (typeof p === 'string' ? p : (p && typeof p.text === 'string' ? p.text : ''))).filter(Boolean).join('');
	}
	return String(content);
}

function openaiContentToParts(content) {
	if (Array.isArray(content)) {
		const parts = [];
		for (const p of content) {
			if (typeof p === 'string') { parts.push({ text: p }); continue; }
			if (!p || typeof p !== 'object') continue;
			if (typeof p.text === 'string') { parts.push({ text: p.text }); continue; }
			if (p.type === 'image_url' && p.image_url && typeof p.image_url.url === 'string') {
				const m = /^data:([^;]+);base64,(.+)$/i.exec(p.image_url.url);
				if (m) parts.push({ inlineData: { mimeType: m[1], data: m[2] } });
				// 非 data URI 的图片 URL（http(s)://…）在这里被有意跳过：Gemini 原生只接受
				// inlineData 或经 Files API 上传得到的 fileData.fileUri，**不能**直抓任意公网
				// 图片地址。硬转 fileData 只会让上游 400，所以保持丢弃（不发坏请求）。
			}
		}
		return parts.length ? parts : [{ text: '' }];
	}
	return [{ text: openaiContentToText(content) }];
}

// 从 OpenAI 侧的 tool_call 里捞出 Gemini 的 thoughtSignature
// （回程时我们把它放在 extra_content.google.thought_signature，客户端原样回带即可命中）
function extractThoughtSignature(tc) {
	if (!tc || typeof tc !== 'object') return '';
	const ex = tc.extra_content || tc.extraContent;
	const g = ex && typeof ex === 'object' ? (ex.google || ex.Google) : null;
	const cands = [
		g && (g.thought_signature || g.thoughtSignature),
		tc.thought_signature, tc.thoughtSignature,
	];
	for (const c of cands) if (typeof c === 'string' && c) return c;
	return '';
}

// ★ Gemini 3 回放 functionCall 时必须带 thoughtSignature，否则上游直接 400：
//   「Function call is missing a thought_signature in functionCall parts」。
//   客户端（OpenAI 协议）没有这个字段、一般也不会原样回带 → 取不到真实签名时
//   用官方哨兵值跳过校验。sub2api 实证 + 2026-10-06 直连 Google 实测：哨兵值 → 200 正常回答。
const GEMINI_DUMMY_THOUGHT_SIGNATURE = 'skip_thought_signature_validator';

// reasoning_effort → Gemini 思考档位（2026-10-09，修「客户端关思考对 Gemini 无效」）。
// 手册口径（ai.google.dev/gemini-api/docs/generate-content/thinking，2026-10-09 查证）：
//   · Gemini 3 系用 thinkingLevel（minimal/low/medium/high）——**不能完全关闭**：
//     3.1 Pro 关不了，3 Flash / Flash-Lite 也只到 minimal（最接近零的档）。
//   · Gemini 2.5 系用 thinkingBudget（0=关；Pro 关不了、最小 128；-1=动态）。
// none/off 是「客户端想关」的语义：3 系压到 minimal，2.5 flash 给 0、2.5 Pro 给最小 128。
const GEMINI_EFFORT_TO_LEVEL = { none: 'minimal', off: 'minimal', minimal: 'minimal', low: 'low', medium: 'medium', high: 'high' };
const GEMINI_EFFORT_TO_BUDGET_25 = { none: 0, off: 0, minimal: 1024, low: 2048, medium: 8192, high: 16384 };

// OpenAI Chat Completions 请求 → Gemini generateContent 请求体
function buildGeminiRequest(payload) {
	const messages = Array.isArray(payload.messages) ? payload.messages : [];
	// tool_call_id → 函数名（tool 结果要标出处，否则 Gemini 不认 functionResponse）
	const nameById = {};
	for (const m of messages) {
		if (m && m.role === 'assistant' && Array.isArray(m.tool_calls)) {
			for (const tc of m.tool_calls) if (tc && tc.id) nameById[tc.id] = (tc.function && tc.function.name) || '';
		}
	}
	const contents = [];
	let systemText = '';
	for (const m of messages) {
		if (!m || typeof m !== 'object') continue;
		const role = m.role;
		if (role === 'system' || role === 'developer') {
			const t = openaiContentToText(m.content);
			if (t) systemText += (systemText ? '\n\n' : '') + t;
		} else if (role === 'user') {
			contents.push({ role: 'user', parts: openaiContentToParts(m.content) });
		} else if (role === 'assistant') {
			const parts = [];
			const t = openaiContentToText(m.content);
			if (t) parts.push({ text: t });
			if (Array.isArray(m.tool_calls)) {
				for (const tc of m.tool_calls) {
					let args = {};
					try { args = JSON.parse((tc.function && tc.function.arguments) || '{}'); } catch (_) { args = {}; }
					// ★ functionCall.args 与 functionResponse.response 同理：必须是对象(Struct)。
					//   typeof [] === 'object' 同样能骗过守卫 → 数组 args 透传 → 上游 400。
					if (!args || typeof args !== 'object' || Array.isArray(args)) args = {};
					parts.push({
						functionCall: { name: (tc.function && tc.function.name) || '', args },
						// ★ 必须带 thoughtSignature（取不到真实签名就用哨兵值，否则 Gemini 3 报 400）
						thoughtSignature: extractThoughtSignature(tc) || GEMINI_DUMMY_THOUGHT_SIGNATURE,
					});
				}
			}
			if (parts.length) contents.push({ role: 'model', parts });
		} else if (role === 'tool' || role === 'function') {
			const name = nameById[m.tool_call_id] || m.name || '';
			// 工具结果 → functionResponse.response。★ Gemini 该字段必须是 JSON 对象(Struct)：
			//   客户端可能回传「JSON 数组字符串」（OpenAI content-parts，如
			//   '[{"type":"text","text":"x"}]'），JSON.parse 后是数组；typeof [] === 'object'
			//   骗过了旧守卫 → 数组原样透传 → 上游 400：
			//   「... function_response: Proto field is not repeating, cannot start list」。
			//   故：数组一律包成 { result: ... }（数组嵌一层即为合法 Struct Value）。
			let resp = null;
			if (typeof m.content === 'string') {
				try { resp = JSON.parse(m.content); } catch (_) { resp = null; }
			} else if (m.content && typeof m.content === 'object') {
				resp = m.content;
			}
			if (Array.isArray(resp)) {
				const txt = openaiContentToText(resp);
				resp = txt ? { result: txt } : { result: resp };
			} else if (!resp || typeof resp !== 'object') {
				resp = { result: openaiContentToText(m.content) };
			}
			contents.push({ role: 'user', parts: [{ functionResponse: { name, response: resp } }] });
		}
	}
	const out = { contents };
	if (systemText) out.systemInstruction = { parts: [{ text: systemText }] };

	// tools → functionDeclarations（含 schema 清理）
	if (Array.isArray(payload.tools) && payload.tools.length) {
		const funcs = [];
		for (const t of payload.tools) {
			const fn = t && t.type === 'function' ? t.function : (t && t.function ? t.function : null);
			if (!fn || !fn.name) continue;
			funcs.push({ name: fn.name, description: fn.description || '', parameters: cleanGeminiSchema(fn.parameters) });
		}
		if (funcs.length) out.tools = [{ functionDeclarations: funcs }];
	}

	// toolConfig —— sub2api 实证：**必须始终带**（上游没它直接拒）
	let mode = 'AUTO';
	const choice = payload.tool_choice;
	if (choice === 'none') mode = 'NONE';
	else if (choice === 'required' || (choice && typeof choice === 'object')) mode = 'ANY';
	out.toolConfig = { functionCallingConfig: { mode } };

	// generationConfig
	const gc = {};
	if (typeof payload.temperature === 'number') gc.temperature = payload.temperature;
	if (typeof payload.top_p === 'number') gc.topP = payload.top_p;
	const maxTokens = typeof payload.max_tokens === 'number' ? payload.max_tokens
		: (typeof payload.max_completion_tokens === 'number' ? payload.max_completion_tokens : null);
	if (maxTokens != null) gc.maxOutputTokens = maxTokens;
	if (Array.isArray(payload.stop) && payload.stop.length) gc.stopSequences = payload.stop.slice(0, 5);
	// reasoning_effort → 思考档位（映射依据见 GEMINI_EFFORT_TO_LEVEL 注释）。
	// 只认已知档位名，未知值忽略（不加 thinkingConfig，模型走自己的默认档）。
	// includeThoughts:true 让思考摘要随 parts（thought:true）回吐 —— 下方转发链要用。
	const effort = typeof payload.reasoning_effort === 'string' ? payload.reasoning_effort.trim().toLowerCase() : '';
	if (effort && GEMINI_EFFORT_TO_LEVEL[effort] != null) {
		const m = String(payload.model || '');
		if (/^gemini-2/i.test(m)) {
			let budget = GEMINI_EFFORT_TO_BUDGET_25[effort];
			// 2.5 Pro 不支持关闭：none/off 的 0 档抬到最小 128（手册：128~32768，不能 disable）
			if (/pro/i.test(m) && budget < 128) budget = 128;
			gc.thinkingConfig = { thinkingBudget: budget, includeThoughts: true };
		} else {
			gc.thinkingConfig = { thinkingLevel: GEMINI_EFFORT_TO_LEVEL[effort], includeThoughts: true };
		}
	}
	if (Object.keys(gc).length) out.generationConfig = gc;
	return out;
}

// Gemini generateContent 响应 → OpenAI chat.completion
function geminiResponseToOpenAI(gj, modelName) {
	const cand = (gj && Array.isArray(gj.candidates) && gj.candidates[0]) || {};
	const parts = (cand.content && Array.isArray(cand.content.parts)) ? cand.content.parts : [];
	let text = '';
	let reasoning = '';
	const toolCalls = [];
	for (const p of parts) {
		// 思考摘要 part（thought:true）→ reasoning_content（2026-10-09 起不再丢弃）：
		// OpenAI 客户端直接可见；Anthropic 路径经 reasoning_content→thinking 管道自动成思考块。
		if (p && p.thought === true) {
			if (typeof p.text === 'string' && p.text) reasoning += p.text;
			continue;
		}
		if (p && typeof p.text === 'string' && p.text) text += p.text;
		if (p && p.functionCall) {
			const tc = {
				id: 'call_' + String(Math.random()).slice(2, 11),
				type: 'function',
				function: {
					name: p.functionCall.name || '',
					arguments: JSON.stringify(p.functionCall.args || {}),
				},
			};
			// 带上签名：客户端原样回带时，buildGeminiRequest 能把它还回上游
			if (p.thoughtSignature) tc.extra_content = { google: { thought_signature: p.thoughtSignature } };
			toolCalls.push(tc);
		}
	}
	const message = { role: 'assistant', content: text || null };
	if (reasoning) message.reasoning_content = reasoning;
	if (toolCalls.length) message.tool_calls = toolCalls;
	let finish = 'stop';
	if (toolCalls.length) finish = 'tool_calls';
	else if (String(cand.finishReason || '').toUpperCase() === 'MAX_TOKENS') finish = 'length';
	const um = (gj && gj.usageMetadata) || {};
	return {
		id: 'chatcmpl-' + Date.now().toString(36) + String(Math.random()).slice(2, 6),
		object: 'chat.completion',
		created: Math.floor(Date.now() / 1000),
		model: modelName,
		choices: [{ index: 0, message, finish_reason: finish }],
		usage: {
			prompt_tokens: um.promptTokenCount || 0,
			completion_tokens: um.candidatesTokenCount || 0,
			total_tokens: um.totalTokenCount || ((um.promptTokenCount || 0) + (um.candidatesTokenCount || 0)),
			reasoning_tokens: um.thoughtsTokenCount || 0,
		},
	};
}

// Gemini :streamGenerateContent?alt=sse → 标准 OpenAI SSE（不补 [DONE]，由 passthroughStream 负责）
function geminiStreamToOpenAI(body, modelName) {
	const reader = body.getReader();
	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	const id = 'chatcmpl-' + Date.now().toString(36) + String(Math.random()).slice(2, 6);
	const created = Math.floor(Date.now() / 1000);
	const mkChunk = (delta, finish) => ({
		id, object: 'chat.completion.chunk', created, model: modelName,
		choices: [{ index: 0, delta, finish_reason: finish == null ? null : finish }],
	});
	let buffer = '';
	let roleSent = false;
	let toolIndex = -1;
	// ★ 不在中途发 finish / usage：
	//   ① 上游可能把 finishReason 放在「还没有 functionCall 的中间块」上 → 早发会把 finish_reason 误判成 stop；
	//   ② usage 可能出现在多个块里 → 早发会重复（线上实测到重复 usage 块）。
	//   统一等整条流读完，按最终状态发一次。
	let upstreamFinish = '';
	let lastUsage = null;

	return new ReadableStream({
		// 铁律：Worker 上必须 start 主动排空，绝不用裸 pull（见 MEMORY.md 流式约定）
		start(controller) {
			(async () => {
				const emit = (obj) => { try { controller.enqueue(encoder.encode('data: ' + JSON.stringify(obj) + '\n\n')); return true; } catch (_) { return false; } };
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) break;
						buffer += decoder.decode(value, { stream: true });
						let nl;
						while ((nl = buffer.indexOf('\n')) !== -1) {
							const line = buffer.slice(0, nl).replace(/\r$/, '').trim();
							buffer = buffer.slice(nl + 1);
							if (!line || line.startsWith(':')) continue;
							if (!line.startsWith('data:')) continue;
							const payloadStr = line.slice(5).trim();
							if (!payloadStr || payloadStr === '[DONE]') continue;
							let gj;
							try { gj = JSON.parse(payloadStr); } catch (_) { continue; }
							const cand = (gj && Array.isArray(gj.candidates) && gj.candidates[0]) || {};
							const parts = (cand.content && Array.isArray(cand.content.parts)) ? cand.content.parts : [];
						for (const p of parts) {
							// 思考摘要 part（thought:true）→ reasoning_content delta（2026-10-09 起不再丢弃）。
							// 正文 content 仍绝不混思考；Anthropic 路径经管道自动转成 thinking 块。
							if (p && p.thought === true) {
								if (typeof p.text === 'string' && p.text) {
									if (!roleSent) { roleSent = true; emit(mkChunk({ role: 'assistant', content: '' }, null)); }
									emit(mkChunk({ reasoning_content: p.text }, null));
								}
								continue;
							}
							if (p && typeof p.text === 'string' && p.text) {
									if (!roleSent) { roleSent = true; emit(mkChunk({ role: 'assistant', content: '' }, null)); }
									emit(mkChunk({ content: p.text }, null));
								}
								if (p && p.functionCall) {
									if (!roleSent) { roleSent = true; emit(mkChunk({ role: 'assistant', content: null }, null)); }
									toolIndex++;
									const tcDelta = {
										index: toolIndex,
										id: 'call_' + String(Math.random()).slice(2, 11),
										type: 'function',
										function: { name: p.functionCall.name || '', arguments: JSON.stringify(p.functionCall.args || {}) },
									};
									if (p.thoughtSignature) tcDelta.extra_content = { google: { thought_signature: p.thoughtSignature } };
									emit(mkChunk({ tool_calls: [tcDelta] }, null));
								}
							}
							if (cand.finishReason) upstreamFinish = String(cand.finishReason).toUpperCase();
							if (gj && gj.usageMetadata) lastUsage = gj.usageMetadata;
						}
					}
					// 流读完，按最终状态发一次 finish_reason，再发一次 usage（顺序同 OpenAI：finish → usage）
					emit(mkChunk({}, upstreamFinish === 'MAX_TOKENS' ? 'length' : (toolIndex >= 0 ? 'tool_calls' : 'stop')));
					if (lastUsage) {
						emit({
							id, object: 'chat.completion.chunk', created, model: modelName, choices: [],
						usage: {
							prompt_tokens: lastUsage.promptTokenCount || 0,
							completion_tokens: lastUsage.candidatesTokenCount || 0,
							total_tokens: lastUsage.totalTokenCount || 0,
							reasoning_tokens: lastUsage.thoughtsTokenCount || 0,
						},
						});
					}
					try { controller.close(); } catch (_) { }
				} catch (e) {
					// 上游断流：透出错误事件再正常收尾（同 passthroughStream 的做法，避免边缘 502）
					try { controller.enqueue(encoder.encode('data: ' + JSON.stringify({ error: { message: String((e && e.message) || e), type: 'server_error', code: 'upstream_stream_error' } }) + '\n\n')); } catch (_) { }
					try { controller.close(); } catch (_) { }
				}
			})();
		},
		cancel(reason) { try { reader.cancel(reason); } catch (_) { } },
	});
}

// 真正调用 Gemini 原生端点（含 TTFB 超时，语义同 callProvider）
async function callGeminiNative(provider, payload, stream) {
	const base = geminiNativeBase(provider.baseUrl);
	const model = String(payload.model || '').replace(/^models\//i, '');
	if (!base || !model) {
		return { success: false, error: `Gemini provider "${provider.name}" is missing baseUrl or model.` };
	}
	const method = stream ? 'streamGenerateContent' : 'generateContent';
	const url = `${base}/models/${model}:${method}${stream ? '?alt=sse' : ''}`;
	const body = buildGeminiRequest(payload);

	try {
		const { response: upstream, ttfb } = await fetchWithTtfb(url, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json', 'x-goog-api-key': provider.apiKey || '' },
			body: JSON.stringify(body),
		});
		if (!upstream.ok) {
			const errorText = await upstream.text();
			// ★ 上游 4xx 原样透出体外：CF 边缘**只会替换 5xx 的响应体**，4xx 能带着真实报错到客户端
			//   （2026-10-06 实测：Worker 返回 400 → CF 原样透传 body；返回 502 → 被 CF 换成裸页面）。
			//   否则「Google 说 thought_signature 缺失」这种关键信息会被 CF 的 502 盖掉，无从排查。
			const status = (upstream.status >= 400 && upstream.status < 500) ? upstream.status : 502;
			return { success: false, status, error: `Provider "${provider.name}" returned ${upstream.status}: ${errorText}` };
		}
		if (stream) return { success: true, stream: geminiStreamToOpenAI(upstream.body, model), ttfb };
		return { success: true, data: geminiResponseToOpenAI(await upstream.json(), model), ttfb };
	} catch (e) {
		const isTimeout = !!(e && e.isTimeout);
		return {
			success: false,
			error: isTimeout
				? `Provider "${provider.name}" did not respond within ${Math.round(PROVIDER_TIMEOUT_MS / 1000)}s (upstream timeout).`
				: `Provider "${provider.name}" connection error: ${e.message}`,
		};
	}
}

async function callProvider(provider, payload, stream, env) {
	// Gemini 渠道走原生适配层（见上方「Gemini 原生适配层」）
	if (isGeminiProvider(provider)) return callGeminiNative(provider, payload, stream);

	const baseUrl = String(provider.baseUrl || '').replace(/\/+$/, '');
	if (!baseUrl) {
		return { success: false, error: `Provider "${provider.name}" has no baseUrl configured.` };
	}

	// 只对「拿到响应头」这一步设超时，一拿到就撤销计时器 ——
	// 流式响应拿到头之后可以慢慢流多久都行，不会被这个计时器误杀。
	// 目的：上游长时间不回时，回一条我们自己能读懂的错误（本代理的 JSON），
	// 而不是让 Cloudflare 边缘等超时后丢一条含糊的 origin_bad_gateway 502 给客户端。
	try {
		// 本地/无鉴权的 OpenAI 兼容端点可以不填 Key，此时不带 Authorization 头
		const headers = { 'Content-Type': 'application/json' };
		if (provider.apiKey) headers['Authorization'] = `Bearer ${provider.apiKey}`;

		// OpenAI 系默认不在流里回 usage，导致 Anthropic 客户端侧 message_delta.usage 恒为 0、
		// 本代理的流式 token 统计也拿不到。显式请求 include_usage，上游在最后一条 chunk 带 usage，
		// 由 withUsageTap 捕获补记统计、anthropicStreamTransform 转成 Anthropic 的 usage 事件。
		// ⚠️ 但有些端点根本不认这个字段、会整条 4xx 拒掉 → 那个模型的流式从此永远失败。
		// 故先查「不再注入」名单：被拒过的 (渠道|模型) 直接不注入（见 markUsageInjectDisabled）。
		// 统一延迟口径：拿到响应头即测 TTFB（首字节），流式/非流式都记这个值
		const cfg = env ? await getAppConfig(env) : null;
		const injectUsage = !!stream && !isUsageInjectDisabled(cfg, provider.id, payload && payload.model);
		const sendPayload = injectUsage
			? { ...payload, stream_options: { include_usage: true } }
			: payload;

		// 单次上游调用（错误归一也在这里）。抽成闭包是因为「上下文溢出」时可能要钳 max_tokens 再打一次。
		const attempt = async (p) => {
			const { response: upstream, ttfb } = await fetchWithTtfb(`${baseUrl}/chat/completions`, {
				method: 'POST',
				headers,
				body: JSON.stringify(p),
			});

			if (!upstream.ok) {
				const errorText = await upstream.text();
				// upstreamStatus 供「失败分类」用（429/5xx 可换成员重试，400 不可）；
				// status 是给客户端的：4xx 原样透出（CF 只替换 5xx 响应体），其余压成 502。
				return {
					success: false,
					upstreamStatus: upstream.status,
					status: (upstream.status >= 400 && upstream.status < 500) ? upstream.status : 502,
					error: `Provider "${provider.name}" returned ${upstream.status}: ${errorText}`,
					errorText,
				};
			}

			if (stream) {
				// passthrough 标记：本响应是上游原始字节（未在 worker 内转换）——
				// 只有这类流才有资格走「长流直通」（原生直管，JS 不逐 chunk 读）。Gemini 原生 /
				// CF 账号池的流是在 worker 内转换出来的，没有这个标记。
				return { success: true, stream: upstream.body, ttfb, passthrough: true };
			}
			return { success: true, data: await upstream.json(), ttfb };
		};

		let result = await attempt(sendPayload);

		// ① stream_options 被上游拒掉 → 去掉注入重试一次，并把该 (渠道|模型) 记入「不再注入」名单。
		//    不然这个模型的**每一次**流式请求都要白撞一次 4xx（参考 nim-proxy 的做法）。
		//    只在错误正文点名了 stream_options/include_usage 时触发，不盲目重试普通 400。
		if (injectUsage && !result.success
			&& result.upstreamStatus >= 400 && result.upstreamStatus < 500
			&& USAGE_FIELD_REJECTED_RE.test(result.errorText || '')) {
			const retry = await attempt(payload);
			if (retry.success) {
				await markUsageInjectDisabled(env, provider.id, payload && payload.model);
				console.warn('[usage-inject] 上游拒绝 stream_options，已对 ' + usageInjectKeyOf(provider.id, payload && payload.model) + ' 关闭注入');
			}
			result = retry;
		}

		// ② 上下文溢出钳制重试（2026-10-09，NVIDIA NIM 适配）：上游按精确 tokenizer 计数，
		// 长会话 + 大 max_tokens 预算会撞 400「maximum context length」。若**消息本身放得下**、
		// 只是输出预算太奢侈，把 max_tokens 钳到（上限 - 消息 - 余量）重打一次，对客户端透明；
		// 消息本身就超限则无解（只能靠客户端精简会话），维持原样透出。只重试一次。
		if (!result.success && result.upstreamStatus === 400) {
			const clamp = parseContextOverflow(result.errorText || '');
			if (clamp) {
				const newMax = clamp.max - clamp.prompt - CONTEXT_RETRY_MARGIN;
				// 原预算已经比钳制值小却还 400 → 问题不在 max_tokens，瞎重试没意义
				if (newMax >= 64 && (payload.max_tokens == null || payload.max_tokens > newMax)) {
					result = await attempt({ ...sendPayload, max_tokens: newMax });
				}
			}
		}

		return result;
	} catch (e) {
		const isTimeout = !!(e && e.isTimeout);
		return {
			success: false,
			error: isTimeout
				? `Provider "${provider.name}" did not respond within ${Math.round(PROVIDER_TIMEOUT_MS / 1000)}s (upstream timeout).`
				: `Provider "${provider.name}" connection error: ${e.message}`
		};
	}
}

// ----------------------------------------------------
// Gemini 渠道的工具历史降级（v3 自然语言版）：assistant.tool_calls + role:"tool" → 折进 user 消息
// Google 的 OpenAI 兼容端点（generativelanguage.googleapis.com/v1beta/openai）
// 对历史里出现 role:"tool" 或 assistant.tool_calls 的请求会「静默断流」——返回 200 后
// 流在字节零就死（或直接 502），不给任何报错（2026-10-05 抓包实锤，见 .workbuddy/memory）。
// 实践中踩过的两个坑（都已实证）：
// 1. tools 数组保留不动 —— 模型发起工具调用那段本来就通（实测 200），只有历史消息需要转换；
// 2. 折叠文本不能用协议式标记（"[系统记录：…][工具执行结果]…"）—— flash-lite 会模仿格式、
//    把工具调用/输出当正文复述（甚至编造），而不是真正回答。v3 用自然语言包裹
//    （「（工具 X 的输出：…）」），并在发生折叠时往系统提示追加一条行为规则。
// 转换是有损的（模型看到的是文本而非结构化结果），但 agent 流程实测可用。
// ----------------------------------------------------
function convertToolMessagesToText(payload) {
	const src = Array.isArray(payload.messages) ? payload.messages : [];
	// tool_call_id → 函数名，结果标注出处用
	const nameById = new Map();
	for (const m of src) {
		if (m && m.role === 'assistant' && Array.isArray(m.tool_calls)) {
			for (const tc of m.tool_calls) {
				if (tc && tc.id) nameById.set(String(tc.id), (tc.function && tc.function.name) || m.name || 'tool');
			}
		}
	}
	const out = [];
	let pending = ''; // 相邻的工具输出攒着，并入下一条 user 消息
	let folded = false;
	const flush = (tail) => {
		if (!pending) return;
		out.push({ role: 'user', content: tail ? pending + '\n\n' + tail : pending });
		pending = '';
		folded = true;
	};
	for (const m of src) {
		if (m && m.role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
			continue; // 调用记录不再单独成句——结果自带出处标注，避免给模型可模仿的模式
		}
		if (m && m.role === 'tool') {
			const fn = nameById.get(String(m.tool_call_id)) || m.name || 'tool';
			const body = typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? '');
			pending += (pending ? '\n\n' : '') + `（工具 ${fn} 的输出：\n${body}\n）`;
			continue;
		}
		if (pending && m && m.role === 'user' && typeof m.content === 'string') {
			out.push({ role: 'user', content: pending + '\n\n' + m.content });
			pending = '';
			folded = true;
			continue;
		}
		flush('以上是工具的输出，请基于它继续回答。');
		out.push(m);
	}
	flush('以上是工具的输出，请基于它继续回答。');
	if (folded) {
		// 折叠发生过才注入：明确行为规则，防小模型模仿/编造工具输出格式
		const s = out.find(m => m && m.role === 'system' && typeof m.content === 'string');
		if (s) s.content += '\n\n（注：历史中的工具调用与结果已由系统转为普通文本随用户消息提供。'
			+ '需要新数据时正常发起工具调用即可；不要自行编造工具输出，也不要复述工具输出的包装格式。）';
	}
	payload.messages = out;
	return payload;
}

// 按路由结果把请求分发到对应上游
async function callUpstream(route, payload, env, stream, ctx) {
	// 路由本身失败（模型没配置、账号池已关闭等）——属于配置错误，交给调用方按 400 返回
	if (route.kind === 'error') {
		return { success: false, error: route.error, status: 400 };
	}

	if (route.kind !== 'provider') {
		// 必须用 route.model 覆盖 payload.model：映射表解析出来的 @cf/... 全路径才是 CF 认的模型名，
		// 漏了这一步会把映射「之前」的短名原样发给 CF（映射表界面看着正常，实际完全不生效）
		const cfPayload = { ...payload, model: route.model };
		// CF AI 的 OpenAI 兼容端点和 Google 一个毛病：历史里出现 role:"tool"/assistant.tool_calls
		// 就静默断流（2026-10-05 C4 实锤）—— 同一味药，工具历史降级成纯文本
		convertToolMessagesToText(cfPayload);
		return callAccountPool(cfPayload, env, stream);
	}

	const providers = await getProviders(env);
	const provider = providers.find(p => p.id === route.providerId);
	if (!provider) {
		return { success: false, error: `Third-party provider "${route.providerId}" not found. It may have been deleted.` };
	}
	if (provider.status === 'disabled') {
		return { success: false, error: `Third-party provider "${provider.name}" is disabled.` };
	}

	// 按渠道构造本次要发出去的 payload（Google 兼容端点的「工具历史折文本」降级只能对具体渠道判断）
	const preparePayload = (prov, model) => {
		const p = { ...payload, model };
		if (/googleapis\.com/i.test(String(prov.baseUrl || '')) && !isGeminiProvider(prov)) convertToolMessagesToText(p);
		return p;
	};

	// 配额组：一次请求内允许「换成员重试」（429/5xx/超时 → 冷却该成员并换下一个）。
	// 普通渠道只试一次；但**瞬时错误**（5xx/超时/连不上）允许原地重试一次 ——
	// 上游偶发过载很常见（实测 Google 会间歇性 503），重试一次基本就过。
	// 总开关关掉时：不换成员、不冷却、也不重试（回到最早的行为）。
	const sched = await isQuotaSchedulingOn(env);
	// 组级冷却覆盖：整个请求只读一次组配置（换成员时复用，顺带省掉循环里的重复读取）
	const groupCfg = route.quotaGroupId
		? (((await getQuotaGroups(env)) || []).find(x => x.id === route.quotaGroupId) || null)
		: null;
	const coolOverride = quotaCoolOverrideMs(groupCfg);
	const tried = new Set();
	const switchBudget = (route.quotaGroupId && sched) ? quotaMaxSwitch(env) : 1;
	const sessionKey = route.quotaGroupId ? sessionKeyOf(payload) : '';
	let transientBudget = sched ? PROVIDER_TRANSIENT_RETRY : 0;
	let switchedAny = false;
	let lastCls = null;
	let curProvider = provider;
	let curModel = route.model;
	let result = null;
	let tries = 0;
	const hardCap = switchBudget + PROVIDER_TRANSIENT_RETRY + 1; // 兜底上限，绝不无限循环

	while (tries < hardCap) {
		if (tries > 0) {
			// 先试「换成员」
			let switched = false;
			if (tries < switchBudget && route.quotaGroupId && sched) {
				const g = groupCfg;   // 本请求开头已读过一次，这里复用（同一请求内配置不会变）
				if (g) {
					const next = await pickQuotaMember(g, env, { exclude: tried, inflight: inflightByMember, sessionKey, scheduling: sched });
					const np = next.ok ? providers.find(p => p.id === next.providerId) : null;
					if (np) {
						curProvider = np;
						curModel = next.model;
						switched = true;
						switchedAny = true;
					}
				}
			}
			if (!switched) {
				// 没有别的成员可换 → 只有「瞬时错误」且还没换过成员时，才原地再试一次
				const canTransient = !!(lastCls && lastCls.transient && transientBudget > 0 && !switchedAny);
				if (!canTransient) break;
				transientBudget--;
				await new Promise((res) => setTimeout(res, PROVIDER_TRANSIENT_RETRY_DELAY_MS));
			}
		}
		tries++;
		const memberKey = cooldownKeyOf(curProvider.id, curModel);
		tried.add(memberKey);
		inflightInc(memberKey);
		const startedAt = Date.now();
		let r;
		try {
			r = await callProvider(curProvider, preparePayload(curProvider, curModel), stream, env);
		} finally {
			// 用 finally 保证 inc/dec 配平：入参表达式若同步抛错，旧写法会跳过 dec → 在途计数永久 +1
			inflightDec(memberKey);
		}
		const elapsed = Date.now() - startedAt;

		if (r.success) {
			// 成功 → 解除该成员的冷却（可能只是偶发抖动）。
			// ⚠️ 必须与下面「失败即写冷却」对称：以前这里只对**配额组**路由清、而写冷却却对
			// **所有**路由生效，于是一条直连请求的偶发失败会给该成员留下冷却、之后直连成功也
			// 解不掉；若该成员同时被某配额组引用，就会被 pickQuotaMember 静默跳过。
			// clearCooldown 查不到条目时是空操作，且共用 isolate 冷却缓存，不会给每次成功都加 KV 写。
			if (sched) await clearCooldown(env, curProvider.id, curModel);
			// 延迟统一记 TTFB（首字节），流式/非流式口径一致（见 callProvider/callGeminiNative/callAccountPool）
			const okMs = (r.ttfb != null) ? r.ttfb : elapsed;
			if (stream && r.stream) {
				// 长流直通（2026-10-10）：上游原始透传流 + 渠道/配额组/映射任一勾了 streamFastPass →
				// 交给响应层原生直管（JS 不逐 chunk 读），免费档 CPU 10ms 下超长输出不再被掐。
				// 代价：流内 model 不回写、token 统计没有（计次在这里立即落库；冷却/选号都在
				// 流开始前已完成，不受影响）。Gemini 原生 / CF 账号池的流在 worker 内转换，不适用。
				// 映射名单的 key 是客户端请求的原始模型名（= 映射源名，单线路直走的专属通道）。
				const fpCfg = await getAppConfig(env);
				const fastPass = r.passthrough === true && (
					(groupCfg && groupCfg.streamFastPass === true) ||
					(curProvider && curProvider.streamFastPass === true) ||
					(Array.isArray(fpCfg.fastPassMappings) && fpCfg.fastPassMappings.indexOf(payload.model) !== -1)
				);
				if (fastPass) {
					recordProviderCall(env, ctx, curProvider, curModel, true, okMs, false, 0, 0, 0, 0);
					r.fastPass = true;
				} else {
					// 流式：token 要等流走完才知道，交给探针在结束时落库。
					// Opt3（2026-10-10）：不再在这里包一层 withUsageTap（那会给每条流多加一趟
					// decode/split/encode —— 免费档 CPU 10ms 下长流杀手，实测 1.8MB 流被 exceededResources）。
					// 改成把回调挂到 result 上，由最终构造响应流的消费方融合：
					//   - OpenAI 透传路径（handleCompletions）→ passthroughStream(., ., tap) 单趟完成
					//   - Anthropic 转换路径 → withUsageTap(anthropicStreamTransform(...), tap)（转换本身要解析，保持外包）
					r.usageTapFn = (uo) => {
						recordProviderCall(env, ctx, curProvider, curModel, true, okMs, false, uo.tokens, uo.reasoningTokens, uo.inputTokens, uo.outputTokens);
					};
				}
			} else {
				const uo = usageOf(r);
				recordProviderCall(env, ctx, curProvider, curModel, true, okMs, false, uo.tokens, uo.reasoningTokens, uo.inputTokens, uo.outputTokens);
			}
			result = r;
			break;
		}

		// 失败：先落统计，再判断该不该冷却 / 换下一个 / 原地重试
		recordProviderCall(env, ctx, curProvider, curModel, false, elapsed, false, 0);
		lastCls = classifyUpstreamFailure(r, env, coolOverride);
		if (lastCls.retryable && sched) await setCooldown(env, curProvider.id, curModel, lastCls.coolMs, lastCls.reason);
		result = r;
		if (!lastCls.retryable) break; // 400 这类是请求本身的问题，换成员 / 重试都一样
	}

	if (!result) {
		return { success: false, error: `Provider "${provider.name}" is not available right now.` };
	}

	return result;
}

// 兼容入口：按请求体里的 model 解析路由后发起调用
async function callOpenAICompatibleAPI(cfPayload, env, stream, ctx) {
	const route = await resolveRoute(cfPayload.model, env, sessionKeyOf(cfPayload));
	return callUpstream(route, cfPayload, env, stream, ctx);
}

// ----------------------------------------------------
// 模型路由：请求的模型名 → 走哪一类上游
// ----------------------------------------------------

// 把映射值 / 模型名解析成第三方渠道路由
// 支持两种写法：provider:<渠道id或渠道名>/<上游模型名>，以及 <渠道名>/<上游模型名>
// allowShorthand=false 时只认 provider: 前缀，避免映射表里的普通模型名被误判
function parseProviderTarget(target, providers, allowShorthand = false) {
	if (typeof target !== 'string') return null;

	let providerKey = null;
	let modelName = null;
	const text = target.trim();

	if (text.startsWith('provider:')) {
		const rest = text.slice('provider:'.length);
		const slash = rest.indexOf('/');
		if (slash === -1) return null;
		providerKey = rest.slice(0, slash).trim();
		modelName = rest.slice(slash + 1).trim();
	} else if (allowShorthand) {
		const slash = text.indexOf('/');
		if (slash <= 0) return null;
		providerKey = text.slice(0, slash).trim();
		modelName = text.slice(slash + 1).trim();
	} else {
		return null;
	}

	if (!providerKey || !modelName) return null;

	const provider = providers.find(p => p.id === providerKey || p.name === providerKey);
	if (!provider) return null;

	return { kind: 'provider', providerId: provider.id, providerName: provider.name, model: modelName };
}

// 解析映射目标。严格要求 provider: 前缀，但写漏了前缀、且能按「渠道名/模型名」解析出来时也认。
// 很多人会直接写 渠道名/模型名（因为调用时就是这么写的），静默失效是最糟糕的结果。
function parseMappingTarget(target, providers) {
	return parseProviderTarget(target, providers, false)
		|| parseProviderTarget(target, providers, true);
}

// 校验映射表：返回 { 客户端模型名: 问题描述 }
// 本版（带账号池）的映射目标有两类合法写法：@cf/...（走账号池）和 provider:<渠道>/<模型>（走第三方渠道）。
// 写错的映射如果只是静默跳过，用户会看到"配了却不生效"，所以要把问题显式暴露出来。
function findInvalidMappings(map, providers, cfEnabled) {
	const problems = {};
	const names = providers.map(p => p.name).join('、') || '（还没有添加任何渠道）';

	for (const [source, target] of Object.entries(map || {})) {
		if (typeof target !== 'string' || !target.trim()) {
			problems[source] = '目标为空';
			continue;
		}
		const text = target.trim();

		// 配额组引用 TT:<组名>：合法写法（组是否存在由运行时 resolveRoute 校验）
		if (text.startsWith('TT:')) continue;

		// Cloudflare 模型路径：账号池启用时合法；关闭时该条不生效
		if (text.startsWith('@cf/')) {
			if (!cfEnabled) problems[source] = 'Cloudflare 账号池已关闭，这条映射不会生效';
			continue;
		}

		// cf/<短名> 也合法（与 /v1/models 列出的名字一致，复制粘贴即可用）
		if (text.startsWith('cf/')) {
			if (!cfEnabled) problems[source] = 'Cloudflare 账号池已关闭，这条映射不会生效';
			continue;
		}

		// 第三方渠道写法（含写漏 provider: 前缀的宽容写法）
		if (parseMappingTarget(text, providers)) continue;

		// 到此：既非 CF 路径、也非可识别的第三方渠道 → 一律视为无效配置，显式报错，
		// 不再静默回落 CF 账号池（与 resolveRoute 一致：裸模型名不会生效）
		if (text.includes('/')) {
			const key = text.startsWith('provider:')
				? text.slice('provider:'.length).split('/')[0]
				: text.split('/')[0];
			problems[source] = '找不到渠道「' + key + '」，当前渠道：' + names;
		} else {
			problems[source] = '无效目标「' + text + '」：裸模型名不会生效，请写成 cf/<短名>（走账号池）或 provider:<渠道名>/<模型名>（走第三方）';
		}
	}
	return problems;
}

// 解析请求的模型名，决定这次请求走哪个上游
// 返回 { kind: 'cf', model } / { kind: 'provider', providerId, model } / { kind: 'error', error }
//
// 账号池启用时（默认部署）：@cf/ 直传 > 映射表 > 渠道名简写 > 默认渠道 > 未知模型显式报错（不静默回落）
// 账号池关闭时（纯第三方反代）：映射表(渠道) > 渠道名简写 > 默认渠道（模型名原样转发）
async function resolveRoute(model, env, sessionKey) {
	const requested = typeof model === 'string' ? model.trim() : '';
	const config = await getAppConfig(env);
	const cfEnabled = config.cfPoolEnabled !== false;
	const providers = config.providers || [];

	// 1. CF 原生路径只在账号池启用时直传
	if (requested.startsWith('@cf/')) {
		if (cfEnabled) return { kind: 'cf', model: requested };
		return {
			kind: 'error',
			error: `Model "${requested}" requires the Cloudflare account pool, which is currently disabled.`
		};
	}

	// 2. 映射表命中。账号池关闭时不合并 CF 预设映射，避免 CF 预设名字劫持第三方模型。
	// 用「null 原型」对象：否则当 requested 命中 Object.prototype 成员（toString / constructor /
	// __proto__ / hasOwnProperty …）时会取到函数/对象而非 undefined，随后 mapped.startsWith 抛
	// TypeError（未捕获 → 500），违反「未知模型一律显式 400」的约定。
	const combinedMap = Object.assign(Object.create(null),
		cfEnabled ? DEFAULT_MODEL_MAP : null,
		config.customModelMap || {});
	let mapped = combinedMap[requested];
	let mappedKey = mapped ? requested : null;

	// 兼容老写法：内置表已统一叫 cf/<短名>，客户端填的裸短名（glm-4.7-flash）继续认
	if (!mapped && !requested.includes('/')) {
		const aliasKey = 'cf/' + requested;
		const aliasVal = combinedMap[aliasKey];
		if (aliasVal) { mapped = aliasVal; mappedKey = aliasKey; }
	}

	// 逐条映射开关（2026-10-09）：这条映射被停用 → 显式 400。
	// 刻意**不**静默回落到默认渠道 —— 那会「答非所问」，是最难排查的一类问题。
	if (mapped && mappedKey && (config.disabledMappings || []).indexOf(mappedKey) !== -1) {
		return {
			kind: 'error',
			error: `Model "${requested}" 的映射已被停用（到「模型映射」里重新开启，或换用其它模型名）。`
		};
	}

	// 映射写了但解析不出渠道时记下来，别让它静默失效
	let badMapping = null;
	if (mapped) {
		// 映射目标指向配额组：TT:<组名> —— 实际走哪个成员由组内顺序 + 已用次数决定
		if (typeof mapped === 'string' && mapped.startsWith('TT:')) {
			const groupName = mapped.slice('TT:'.length).trim();
			const g = (config.quotaGroups || []).find(x => x.name === groupName && x.status !== 'disabled');
			if (!g) {
				return { kind: 'error', error: `映射目标 "TT:${groupName}" 找不到可用的配额组，请到「调用配额」里核对组名。` };
			}
			const picked = await pickQuotaMember(g, env, { sessionKey, scheduling: config.quotaScheduling !== false });
			if (!picked.ok) return { kind: 'error', error: picked.error };
			return { kind: 'provider', providerId: picked.providerId, model: picked.model, quotaGroupId: g.id };
		}

		// 宽容解析：写漏 provider: 前缀但能认出渠道的也直接生效（保存时同样会自动补全）
		const providerRoute = parseMappingTarget(mapped, providers);
		if (providerRoute) return providerRoute;

		if (mapped.startsWith('@cf/')) {
			// 指向 CF：账号池启用时透传；关闭时该条不生效，继续往下走（页面上已有整体提示）
			if (cfEnabled) return { kind: 'cf', model: mapped };
		} else if (mapped.startsWith('cf/')) {
			// 映射值也允许写 cf/<短名>（与 /v1/models 列出的名字一致，直接复制粘贴就能用）。
			// 优先用映射表把短名解析成规范的 @cf/<org>/<model> 全路径；
			// 表里查不到（自定义 CF 模型）就按 cf/<short> → @cf/<short> 兜底。
			const viaTable = combinedMap[mapped];
			const resolved = (typeof viaTable === 'string' && viaTable.startsWith('@cf/'))
				? viaTable
				: '@cf/' + mapped.slice(3);
			if (cfEnabled) return { kind: 'cf', model: resolved };
			badMapping = mapped;
		} else {
			// 既不是 TT:、也不是可识别的第三方渠道（含写漏 provider: 前缀的宽容写法）、
			// 也不是 CF 路径（@cf/ 或 cf/）→ 一律视为无效配置，显式报错。
			// 不再把裸模型名静默回落到 CF 账号池，否则会出现"配了却不生效 / 答非所问"
			// 且极难排查，违反"不静默回落任何 CF 模型"的硬约定。
			if (mapped.includes('/')) {
				const key = mapped.startsWith('provider:')
					? mapped.slice('provider:'.length).split('/')[0]
					: mapped.split('/')[0];
				return {
					kind: 'error',
					error: `Mapping target "${mapped}" cannot be resolved to a provider. Unknown channel "${key}". Use provider:<provider-name>/<upstream-model>.`
				};
			}
			return {
				kind: 'error',
				error: `Mapping target "${mapped}" is invalid: a mapping value must be "TT:<group>", "@cf/...", "cf/<short>", or "provider:<channel>/<model>". Bare model names are not allowed.`
			};
		}
	}

	// 2b. cf/ 前缀：映射表里没配过，就直接当 @cf/ 全路径转发。
	//     这样任何 CF 模型都能写成 cf/openai/gpt-oss-20b 来调用，不必先配映射。
	//     必须排在「渠道名/模型名」简写之前，否则会被当成叫 cf 的第三方渠道。
	if (requested.startsWith('cf/')) {
		if (cfEnabled) return { kind: 'cf', model: '@cf/' + requested.slice(3) };
		return {
			kind: 'error',
			error: `Model "${requested}" requires the Cloudflare account pool, which is currently disabled.`
		};
	}

	// 2c. 配额组显式前缀 TT:<组名>：配额组唯一合法的「按组调用」入口。
	//     用显式前缀彻底杜绝「裸组名」与映射键 / CF 预设短名抢同一命名空间（冲突 C）。
	if (requested.startsWith('TT:')) {
		const groupName = requested.slice('TT:'.length).trim();
		const g = (config.quotaGroups || []).find(x => x.name === groupName && x.status !== 'disabled');
		if (!g) return { kind: 'error', error: `Quota group "${groupName}" not found or disabled.` };
		const picked = await pickQuotaMember(g, env, { sessionKey, scheduling: config.quotaScheduling !== false });
		if (!picked.ok) return { kind: 'error', error: picked.error };
		return { kind: 'provider', providerId: picked.providerId, model: picked.model, quotaGroupId: g.id };
	}

	// 3. 「渠道名/模型名」简写
	const shorthand = parseProviderTarget(requested, providers, true);
	if (shorthand) return shorthand;

	// 4. 默认渠道：模型名原样转发，这是纯反代模式的主要入口
	const defaultProviderId = config.defaultProviderId;
	if (defaultProviderId) {
		const fallbackProvider = providers.find(p => p.id === defaultProviderId && p.status !== 'disabled');
		if (fallbackProvider) {
			return { kind: 'provider', providerId: fallbackProvider.id, model: requested };
		}
	}

	// 5. 兜底：未知模型 / 解析不出的映射一律显式报错，绝不静默回落到任何默认模型。
	//    之前「未知模型偷偷换成某个内置模型」会让调用方答非所问且极难排查；
	//    现在统一返回明确错误，由调用方决定如何配置（映射 / 默认渠道 / 显式 provider:）。
	if (badMapping) {
		return {
			kind: 'error',
			error: `Model "${requested}" has a mapping target "${badMapping}" that cannot be resolved to a provider. `
				+ `Use provider:<provider-name>/<upstream-model>, e.g. provider:gemini/gemini-3.8-flash.`
		};
	}

	return {
		kind: 'error',
		error: `Model "${requested}" is not configured: no mapping, quota group, default provider, or known channel matched. `
			+ `Add a mapping in the dashboard, call it as "provider:<provider-name>/<upstream-model>", or set a default provider.`
	};
}

// 对话补全 / 文本补全 的代理处理函数
async function handleCompletions(request, env, pathname, ctx) {
	let body;
	try {
		body = await request.json();
	} catch (e) {
		return new Response(JSON.stringify({ error: { message: "Invalid JSON body", type: "invalid_request_error" } }), { status: 400 });
	}

	const { model, messages, prompt, stream } = body;

	if (pathname === '/v1/chat/completions' && !messages) {
		return new Response(JSON.stringify({ error: { message: "messages field is required", type: "invalid_request_error" } }), { status: 400 });
	}
	if (pathname === '/v1/completions' && !prompt) {
		return new Response(JSON.stringify({ error: { message: "prompt field is required", type: "invalid_request_error" } }), { status: 400 });
	}

	// 构造发往上游的请求体。model 保持客户端传入的原始名字，
	// 由 callOpenAICompatibleAPI → resolveRoute 决定实际走 CF 账号池还是第三方渠道
	const cfPayload = {
		model: model,
		messages: pathname === '/v1/chat/completions' ? messages : [{ role: 'user', content: prompt }],
		stream: !!stream,
	};

	const passthroughFields = [
		'temperature', 'max_tokens', 'top_p', 'n',
		'stop', 'presence_penalty', 'frequency_penalty',
		'logprobs', 'top_logprobs', 'seed', 'user',
		'tools', 'tool_choice', 'parallel_tool_calls',
		'response_format',
		// 以下几项以前被静默丢弃：新模型用 max_completion_tokens 取代 max_tokens、
		// 推理模型用 reasoning_effort、少数上游用 logit_bias —— 客户端发了却收不到会很难排查。
		'max_completion_tokens', 'reasoning_effort', 'logit_bias',
	];
	for (const field of passthroughFields) {
		if (body[field] !== undefined) cfPayload[field] = body[field];
	}

	const result = await callOpenAICompatibleAPI(cfPayload, env, stream, ctx);

	if (!result.success) {
		// 路由/配置类错误用 400（模型没配置、账号池已关闭），上游故障仍用 502
		const status = result.status || 502;
		return new Response(JSON.stringify({
			error: { message: result.error, type: status === 400 ? "invalid_request_error" : "server_error" }
		}), { status, headers: { 'Content-Type': 'application/json' } });
	}

	if (stream) {
		const sseHeaders = {
			'Content-Type': 'text/event-stream',
			// ⚠️ 不再手动设置 Connection / Transfer-Encoding：
			// 这两个是 hop-by-hop 头，由 Workers 运行时自己负责分帧，手动设会被忽略、
			// 还会扰乱它的缓冲判断（社区实锤会导致长流式请求被判「代码 hang」→ 502）。
			// 改成提示边缘「不要缓冲我」的正确姿势：
			'Cache-Control': 'no-cache, no-transform',
			'X-Accel-Buffering': 'no',
		};
		// 长流直通：上游字节原样交给运行时直管（JS 不逐 chunk 读，CPU 趋近零）。
		// 流内 model 不回写、无 token 统计 —— 换取免费档下超长输出不被 CPU 限制掐断。
		if (result.fastPass) return new Response(result.stream, { headers: sseHeaders });
		// Opt3：tap 回调（callUpstream 挂在 result.usageTapFn 上）与心跳都融合进透传层 ——
		// 单层完成 model 替换 + token 抽取 + keep-alive，外加批量冲刷省 enqueue 次数。
		const transformedStream = passthroughStream(result.stream, model, result.usageTapFn);
		return new Response(transformedStream, { headers: sseHeaders });
	} else {
		const cfJson = result.data;
		if (cfJson.model !== undefined) cfJson.model = model;
		return new Response(JSON.stringify(cfJson), {
			headers: { 'Content-Type': 'application/json' },
		});
	}
}

// ----------------------------------------------------
// Anthropic Messages API → OpenAI Chat Completions 格式转换
// ----------------------------------------------------
function convertAnthropicToOpenAI(anthropicBody) {
	const openaiBody = {};

	// model 直接映射
	openaiBody.model = anthropicBody.model;

	// max_tokens 直接映射
	if (anthropicBody.max_tokens !== undefined) {
		openaiBody.max_tokens = anthropicBody.max_tokens;
	}

	// stream 直接映射
	if (anthropicBody.stream !== undefined) {
		openaiBody.stream = anthropicBody.stream;
	}

	// temperature 直接映射
	if (anthropicBody.temperature !== undefined) {
		openaiBody.temperature = anthropicBody.temperature;
	}

	// top_p 直接映射
	if (anthropicBody.top_p !== undefined) {
		openaiBody.top_p = anthropicBody.top_p;
	}

	// stop_sequences → stop
	if (anthropicBody.stop_sequences !== undefined) {
		openaiBody.stop = anthropicBody.stop_sequences;
	}

	// Anthropic thinking → reasoning_effort（2026-10-09）：Claude Code 等客户端的思考开关
	// 以前在这里被静默丢弃 —— 现在能传到下游：Gemini 原生路径映射成 thinkingLevel/thinkingBudget，
	// 第三方渠道按白名单透传。预算折档：≤2048 low、≤8192 medium、其余 high；disabled → none。
	const th = anthropicBody.thinking;
	if (th && typeof th === 'object') {
		if (th.type === 'disabled') {
			openaiBody.reasoning_effort = 'none';
		} else if (th.type === 'enabled' && Number(th.budget_tokens) > 0) {
			const b = Number(th.budget_tokens);
			openaiBody.reasoning_effort = b <= 2048 ? 'low' : (b <= 8192 ? 'medium' : 'high');
		}
	}

	// 构建 OpenAI 格式的 messages 数组
	const openaiMessages = [];

	// Anthropic system 字段 → OpenAI system role message (插入到 messages 最前面)
	if (anthropicBody.system) {
		let systemContent = '';
		if (typeof anthropicBody.system === 'string') {
			systemContent = anthropicBody.system;
		} else if (Array.isArray(anthropicBody.system)) {
			// system 为数组格式：[{type: "text", text: "..."}, ...]
			for (const block of anthropicBody.system) {
				if (block.type === 'text' && block.text) {
					systemContent += block.text + '\n';
				}
			}
			systemContent = systemContent.trim();
		}
		if (systemContent) {
			openaiMessages.push({ role: 'system', content: systemContent });
		}
	}

	// 转换 messages
	for (const msg of anthropicBody.messages) {
		const role = msg.role;
		const content = msg.content;

		// Anthropic 的 content 可能是字符串或数组
		if (typeof content === 'string') {
			openaiMessages.push({ role, content });
		} else if (Array.isArray(content)) {

			// assistant 消息：text 和 tool_use 需合并为一条消息（Bug #4）
			if (role === 'assistant') {
				let textContent = '';
				const toolCalls = [];

				for (const block of content) {
					if (block.type === 'text') {
						textContent += block.text || '';
					} else if (block.type === 'tool_use') {
						toolCalls.push({
							id: block.id,
							type: 'function',
							function: {
								name: block.name,
								arguments: JSON.stringify(block.input || {})
							}
						});
					}
				}

				// 只有确实带 tool_calls 时才允许 content: null（OpenAI 的合法形态）；
				// 否则空内容会产出「content: null 且无 tool_calls」的非法消息。
				const assistantMsg = { role: 'assistant', content: textContent || (toolCalls.length > 0 ? null : '') };
				if (toolCalls.length > 0) {
					assistantMsg.tool_calls = toolCalls;
				}
				openaiMessages.push(assistantMsg);
				continue;
			}

			// user 消息：先处理 tool_result，再处理 text/image（Bug #5）
			if (role === 'user') {
				// 先处理 tool_result 块
				for (const block of content) {
					if (block.type === 'tool_result') {
						let resultContent = '';
						if (typeof block.content === 'string') {
							resultContent = block.content;
						} else if (Array.isArray(block.content)) {
							for (const c of block.content) {
								if (c.type === 'text' && c.text) {
									resultContent += c.text;
								}
							}
						}
						const toolMsg = {
							role: 'tool',
							tool_call_id: block.tool_use_id,
							content: resultContent
						};
						if (block.name) toolMsg.name = block.name;
						openaiMessages.push(toolMsg);
					}
				}

				// 再处理剩余的 text 和 image 块
				const openaiContentParts = [];
				for (const block of content) {
					if (block.type === 'text') {
						openaiContentParts.push({ type: 'text', text: block.text || '' });
					} else if (block.type === 'image') {
						// Anthropic image source → OpenAI image_url
						const source = block.source || {};
						let imageUrl = '';
						if (source.type === 'url' && source.url) {
							// URL 类型图片（Bug #3）
							imageUrl = source.url;
						} else if (source.data) {
							const mediaType = source.media_type || 'image/png';
							imageUrl = `data:${mediaType};base64,${source.data}`;
						}
						if (imageUrl) {
							openaiContentParts.push({
								type: 'image_url',
								image_url: { url: imageUrl }
							});
						}
					}
				}

				if (openaiContentParts.length > 0) {
					openaiMessages.push({ role: 'user', content: openaiContentParts });
				}
				continue;
			}

			// 兜底：其他角色只处理 text 块
			const openaiContentParts = [];
			for (const block of content) {
				if (block.type === 'text') {
					openaiContentParts.push({ type: 'text', text: block.text || '' });
				}
			}
			if (openaiContentParts.length > 0) {
				openaiMessages.push({ role, content: openaiContentParts });
			}
		}
	}

	// 确保第一条消息是 user（OpenAI 要求第一条消息必须是 user 或 system）
	// 如果第一条是 assistant（来自 Anthropic 的多轮 tool calling），在它前面插入一条占位 user 消息
	const firstNonSystemMsg = openaiMessages.find(m => m.role !== 'system');
	// 首条只要不是 user 就补占位：除了 assistant（多轮 tool calling），首条 user 消息**只含
	// tool_result** 时会转出 role:'tool'，形成 system→tool 开头，部分上游直接 400。
	if (firstNonSystemMsg && firstNonSystemMsg.role !== 'user') {
		// 找到 system 消息后的位置，插入一条空的 user 消息
		const systemCount = openaiMessages.filter(m => m.role === 'system').length;
		openaiMessages.splice(systemCount, 0, {
			role: 'user',
			content: '_'
		});
	}

	openaiBody.messages = openaiMessages;

	// tools 字段转换：Anthropic 格式 → OpenAI 格式
	if (anthropicBody.tools && Array.isArray(anthropicBody.tools)) {
		openaiBody.tools = anthropicBody.tools.map(tool => ({
			type: 'function',
			function: {
				name: tool.name,
				description: tool.description || '',
				parameters: tool.input_schema || {}
			}
		}));
	}

	// tool_choice 转换
	if (anthropicBody.tool_choice) {
		const tc = anthropicBody.tool_choice;
		if (tc.type === 'auto') {
			openaiBody.tool_choice = 'auto';
		} else if (tc.type === 'any') {
			openaiBody.tool_choice = 'required';
		} else if (tc.type === 'tool' && tc.name) {
			openaiBody.tool_choice = { type: 'function', function: { name: tc.name } };
		}
	}

	return openaiBody;
}

// ----------------------------------------------------
// OpenAI Chat Completion 响应 → Anthropic Messages 格式转换
// ----------------------------------------------------
function convertOpenAIToAnthropic(openaiResponse, originalModel) {
	const choice = openaiResponse.choices?.[0] || {};
	const message = choice.message || {};

	const anthropicResponse = {
		id: `msg_${crypto.randomUUID()}`,
		type: 'message',
		role: 'assistant',
		content: [],
		model: originalModel,
		stop_reason: null,
		stop_sequence: null,
		usage: {
			input_tokens: openaiResponse.usage?.prompt_tokens || 0,
			output_tokens: openaiResponse.usage?.completion_tokens || 0
		}
	};

	// reasoning_content → thinking 块（NVIDIA NIM / DeepSeek 系思考模型）。
	// signature 用占位值：本代理在请求方向会丢弃客户端回传的 thinking 块（转换器只认
	// text/tool_use/tool_result/image），签名永远不会被校验，占位即可满足协议形状。
	if (message.reasoning_content) {
		anthropicResponse.content.push({
			type: 'thinking',
			thinking: String(message.reasoning_content),
			signature: THINKING_SIG_PLACEHOLDER
		});
	}

	// 文本内容 → text block
	if (message.content) {
		anthropicResponse.content.push({
			type: 'text',
			text: message.content
		});
	}

	// tool_calls → tool_use blocks
	if (message.tool_calls && Array.isArray(message.tool_calls)) {
		for (const tc of message.tool_calls) {
			let inputObj = {};
			try {
				inputObj = typeof tc.function.arguments === 'string'
					? JSON.parse(tc.function.arguments)
					: tc.function.arguments;
			} catch (_) {
				inputObj = {};
			}
			// Anthropic tool_use.input 规范要求对象：数组/标量一律归一化（否则严格 SDK 解析异常）
			if (!inputObj || typeof inputObj !== 'object' || Array.isArray(inputObj)) inputObj = {};
			anthropicResponse.content.push({
				type: 'tool_use',
				id: tc.id,
				name: tc.function.name,
				input: inputObj
			});
		}
	}

	// finish_reason → stop_reason 映射
	const finishReason = choice.finish_reason;
	if (finishReason === 'stop') {
		anthropicResponse.stop_reason = 'end_turn';
	} else if (finishReason === 'tool_calls') {
		anthropicResponse.stop_reason = 'tool_use';
	} else if (finishReason === 'length') {
		anthropicResponse.stop_reason = 'max_tokens';
	} else {
		anthropicResponse.stop_reason = finishReason || 'end_turn';
	}

	return anthropicResponse;
}

// ----------------------------------------------------
// OpenAI 错误响应 → Anthropic 错误格式转换
// ----------------------------------------------------
function convertOpenAIErrorToAnthropic(openaiError, status) {
	// 按上游 HTTP 状态码映射 Anthropic 错误类型：以前恒为 api_error，会把 400 这类
	// 客户端自身的错误标成服务端错误，误导客户端的重试策略。
	const s = Number(status) || 0;
	let type = 'api_error';
	if (s === 400 || s === 404 || s === 422) type = 'invalid_request_error';
	else if (s === 401) type = 'authentication_error';
	else if (s === 403) type = 'permission_error';
	else if (s === 429) type = 'rate_limit_error';
	else if (s === 413) type = 'request_too_large';
	return {
		type: 'error',
		error: {
			type,
			message: openaiError?.error?.message || openaiError?.message || 'Unknown error'
		}
	};
}

// ----------------------------------------------------
// Anthropic /v1/messages 路由处理函数
// ----------------------------------------------------
async function handleMessages(request, env, ctx) {
	// 认证由 handleV1Proxy 的 checkProxyAuth 统一处理（支持 x-api-key + Bearer）

	// 解析请求体
	let anthropicBody;
	try {
		anthropicBody = await request.json();
	} catch (e) {
		return new Response(JSON.stringify({
			type: 'error',
			error: { type: 'invalid_request_error', message: 'Invalid JSON body.' }
		}), { status: 400, headers: { 'Content-Type': 'application/json' } });
	}

	// 基本参数校验
	if (!anthropicBody.messages || !Array.isArray(anthropicBody.messages)) {
		return new Response(JSON.stringify({
			type: 'error',
			error: { type: 'invalid_request_error', message: 'messages field is required and must be an array.' }
		}), { status: 400, headers: { 'Content-Type': 'application/json' } });
	}
	if (!anthropicBody.max_tokens) {
		return new Response(JSON.stringify({
			type: 'error',
			error: { type: 'invalid_request_error', message: 'max_tokens is required.' }
		}), { status: 400, headers: { 'Content-Type': 'application/json' } });
	}

	// 模型名先原样透传，实际走 CF 还是第三方渠道由 resolveRoute 决定
	const model = anthropicBody.model;

	// Anthropic → OpenAI 格式转换
	const openaiBody = convertAnthropicToOpenAI(anthropicBody);
	openaiBody.model = model;

	const stream = !!anthropicBody.stream;

	const result = await callOpenAICompatibleAPI(openaiBody, env, stream, ctx);

	if (!result.success) {
		// 尝试解析 CF 错误详情
		let errorDetail;
		try {
			if (result.error && result.error.includes('CF API returned')) {
				const match = result.error.match(/CF API returned \d+: (.+)/);
				if (match) {
					errorDetail = JSON.parse(match[1]);
				}
			}
		} catch (_) { }

		const status = result.status || 502;
		const anthropicError = convertOpenAIErrorToAnthropic(
			errorDetail || { message: result.error },
			status
		);
		return new Response(JSON.stringify(anthropicError), {
			status,
			headers: { 'Content-Type': 'application/json' }
		});
	}

	if (stream) {
		// 流式：转换流。Opt3：token 抽取仍外包一层 withUsageTap（转换本身必须解析每个 chunk，
		// 省不掉），但回调从 callUpstream 的 result.usageTapFn 拿；没挂 = 纯包装不落库。
		const anthropicStream = withUsageTap(anthropicStreamTransform(result.stream, model), result.usageTapFn);
		const transformedStream = withSseHeartbeat(anthropicStream);
		return new Response(transformedStream, {
			headers: {
				'Content-Type': 'text/event-stream',
				// ⚠️ 不再手动设置 Connection / Transfer-Encoding：
				// 这两个是 hop-by-hop 头，由 Workers 运行时自己负责分帧，手动设会被忽略、
				// 还会扰乱它的缓冲判断（社区实锤会导致长流式请求被判「代码 hang」→ 502）。
				// 改成提示边缘「不要缓冲我」的正确姿势：
				'Cache-Control': 'no-cache, no-transform',
				'X-Accel-Buffering': 'no',
			},
		});
	} else {
		// 非流式：转换响应
		const openaiResponse = result.data;
		const anthropicResponse = convertOpenAIToAnthropic(openaiResponse, model);
		return new Response(JSON.stringify(anthropicResponse), {
			headers: { 'Content-Type': 'application/json' },
		});
	}
}

// ----------------------------------------------------
// Anthropic SSE 流式转换
// 将 OpenAI SSE 格式实时转换为 Anthropic SSE 格式
// ----------------------------------------------------
function anthropicStreamTransform(upstreamBody, modelName) {
	const reader = upstreamBody.getReader();
	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	let buffer = '';
	let messageId = `msg_${crypto.randomUUID()}`;
	let contentBlockIndex = -1;  // 首次递增后从 0 开始（Bug #8）
	let currentToolCallId = null;
	let currentToolName = null;
	let currentToolArgs = '';
	let streamStarted = false;
	let blockStopSent = false;  // 跟踪最后一个 content block 是否已发送 stop（Bug #2）
	let thinkingOpen = false;   // thinking 块是否开着（2026-10-09 reasoning_content 适配）
	let inputTokens = 0;
	let outputTokens = 0;
	let finalFinish = '';       // 上游最后一个 finish_reason（把 length 映射成 max_tokens，与非流式口径一致）
	let finalSent = false;      // 收尾事件是否已发（[DONE] 或流自然结束 → 只发一次）

	return new ReadableStream({
		// ⚠️ 主动排空（eager drain），理由同 passthroughStream：
		// Workers 上 pull 驱动可能永远等不到下游需求 → 流不产出 → 被判 hang → 502。
		start(controller) {
			// 急切 message_start（2026-10-09，NVIDIA NIM 适配）：大模型首 token 可达 3~8s
			// 甚至更久（Nemotron Ultra 253B 的 TTFT 是设计如此）。先把 message_start 发出去，
			// 客户端（Claude Code 等）立刻有消息对象可渲染，而不是干等上游首字节。
			// 此刻 usage 必然为 0 —— 真实值由收尾的 message_delta 补，口径与原先一致。
			// 注意：发完要同步置 streamStarted，否则首个内容 delta 会重复发一次 message_start。
			sendMessageStart(controller);
			streamStarted = true;
			(async () => {
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) {
							if (buffer.trim()) {
								buffer = processLines(buffer, controller);
							}
							// ⚠️ 关键兜底：不少上游**根本不发 `data: [DONE]`**（尤其 Gemini 原生路径，
							// 见 geminiStreamToOpenAI：它刻意不产 [DONE]，补 [DONE] 的 passthroughStream
							// 只挂在 OpenAI 路径上）。以前只认 [DONE] 才收尾 → 这类上游走 /v1/messages
							// 时客户端永远等不到 message_delta / message_stop → SDK 报「流在 message_stop
							// 前结束」或直接挂起。故流自然结束时若无收尾，必须补发。
							if (!finalSent) sendFinalEvent(controller);
							controller.close();
							break;
						}

						buffer += decoder.decode(value, { stream: true });
						buffer = processLines(buffer, controller);
					}
				} catch (e) {
					// 上游断流：按 Anthropic 协议发 error 事件再正常收尾 ——
					// 让客户端看到真实原因，而不是让边缘拿「不完整响应」去回 502（2026-10-05）
					try {
						controller.enqueue(encoder.encode(`event: error\ndata: {"type":"error","error":{"type":"api_error","message":${JSON.stringify(String(e && e.message || e))}}}\n\n`));
						controller.close();
					} catch (_) { }
				}
			})();
		},
		cancel() {
			try { reader.cancel(); } catch (_) { }
		},
	});

	function processLines(data, controller) {
		const lines = data.split('\n');
		const remaining = lines.pop();

		for (const line of lines) {
			const trimmed = line.trim();
			if (!trimmed) continue;

			if (trimmed.startsWith('data: ')) {
				const dataStr = trimmed.slice(6);
				if (dataStr === '[DONE]') {
					// 发送最终事件
					sendFinalEvent(controller);
					continue;
				}

				try {
					const chunk = JSON.parse(dataStr);

					// ⚠️ usage 必须**先于** choices 判定处理：OpenAI 规范里 stream_options.include_usage
					// 的 usage 放在最后一个 `choices: []` 的包里（本代理在 callProvider 里强制要了该参数，
					// geminiStreamToOpenAI 也这么发）——先 `if (!choice) continue` 会整包丢掉，
					// 导致 message_delta.usage 里的 input/output tokens 恒为 0。
					if (chunk.usage) {
						inputTokens = chunk.usage.prompt_tokens || 0;
						outputTokens = chunk.usage.completion_tokens || 0;
					}

					const choice = chunk.choices?.[0];
					if (!choice) continue;

					const delta = choice.delta || {};

				// 处理 tool_calls delta
				if (delta.tool_calls && Array.isArray(delta.tool_calls)) {
					for (const tc of delta.tool_calls) {
						if (tc.id) {
							// 新的 tool_call 开始。**任何**已打开但未关闭的内容块都要先收掉 ——
							// 不能只判断「上一个工具块」：若此刻开着的是文本块（index 0），直接就
							// contentBlockIndex++ 到 1 并 content_block_start，会发出非法的 SSE 序列
							// （index 0 永远没有 content_block_stop）。反向 tool→text 已有对称处理。
							// 若开着的是 thinking 块，收尾前要先补 signature_delta（协议形状）。
							if (!blockStopSent && contentBlockIndex >= 0) {
								if (thinkingOpen) {
									sendThinkingSignature(controller);
									thinkingOpen = false;
								}
								sendContentBlockStop(controller);
								blockStopSent = true;
							}
							currentToolCallId = tc.id;
							currentToolName = tc.function?.name || '';
							currentToolArgs = '';
							contentBlockIndex++;
							blockStopSent = false;

							sendContentBlockStart(controller, 'tool_use');
						}

						if (tc.function?.arguments) {
							currentToolArgs += tc.function.arguments;
							// 发送 tool_use 的 input_json_delta
							sendToolUseDelta(controller, tc.function.arguments);
						}
					}
				} else if (delta.reasoning_content || delta.reasoning) {
					// 思考增量（NVIDIA NIM / DeepSeek 系：reasoning_content）→ thinking 块。
					// message_start 已急切发过，这里只管开块 / 续流。
					if (!thinkingOpen) {
						// 从其他块切进 thinking：先收掉当前块（罕见，但保持对称），并清掉工具
						// 状态 —— 否则后续文本 delta 会走 tool→text 分支，对已收掉的块重复发 stop。
						if (contentBlockIndex >= 0 && !blockStopSent) {
							sendContentBlockStop(controller);
							blockStopSent = true;
						}
						if (currentToolCallId) {
							currentToolCallId = null;
							currentToolName = null;
							currentToolArgs = '';
						}
						contentBlockIndex++;
						blockStopSent = false;
						sendContentBlockStart(controller, 'thinking');
						thinkingOpen = true;
					}
					sendThinkingDelta(controller, String(delta.reasoning_content || delta.reasoning));
				} else if (delta.content) {
					// 文本内容 delta（message_start 已急切发过：首个内容只负责开块）
					if (thinkingOpen) {
						// thinking → text：先补 signature_delta 再收块，然后开新的 text 块
						sendThinkingSignature(controller);
						sendContentBlockStop(controller);
						blockStopSent = true;
						thinkingOpen = false;
						contentBlockIndex++;
						sendContentBlockStart(controller, 'text');
						blockStopSent = false;
					} else if (contentBlockIndex < 0) {
						// 首个内容块（原先靠 streamStarted 兜着，急切发送后改看块下标）
						contentBlockIndex++;
						sendContentBlockStart(controller, 'text');
						blockStopSent = false;
					}

					// 如果之前有 tool_call 在进行中，先结束
					if (currentToolCallId) {
						sendContentBlockStop(controller);
						blockStopSent = true;
						currentToolCallId = null;
						currentToolName = null;
						currentToolArgs = '';

						// 开始新的 text block
						contentBlockIndex++;
						sendContentBlockStart(controller, 'text');
						blockStopSent = false;
					}

					sendTextDelta(controller, delta.content);
				}

					// 检查 finish_reason
					if (choice.finish_reason) {
						finalFinish = choice.finish_reason;
						if (currentToolCallId && currentToolArgs) {
							// 发送最终的 tool_use input（内部会置 blockStopSent，避免收尾时重复发）
							sendToolUseFinalInput(controller);
						}
					}
				} catch (_) {
					// 忽略解析错误
				}
			}
		}
		return remaining;
	}

	function sendMessageStart(controller) {
		const event = {
			type: 'message_start',
			message: {
				id: messageId,
				type: 'message',
				role: 'assistant',
				content: [],
				model: modelName,
				stop_reason: null,
				stop_sequence: null,
				usage: { input_tokens: inputTokens, output_tokens: outputTokens }
			}
		};
		controller.enqueue(encoder.encode(`event: message_start\ndata: ${JSON.stringify(event)}\n\n`));
	}

	function sendContentBlockStart(controller, blockType) {
		const event = {
			type: 'content_block_start',
			index: contentBlockIndex,
			content_block: blockType === 'tool_use'
				? { type: 'tool_use', id: currentToolCallId, name: currentToolName, input: {} }
				: blockType === 'thinking'
					? { type: 'thinking', thinking: '' }
					: { type: 'text', text: '' }
		};
		controller.enqueue(encoder.encode(`event: content_block_start\ndata: ${JSON.stringify(event)}\n\n`));
	}

	function sendTextDelta(controller, text) {
		const event = {
			type: 'content_block_delta',
			index: contentBlockIndex,
			delta: { type: 'text_delta', text }
		};
		controller.enqueue(encoder.encode(`event: content_block_delta\ndata: ${JSON.stringify(event)}\n\n`));
	}

	function sendThinkingDelta(controller, text) {
		const event = {
			type: 'content_block_delta',
			index: contentBlockIndex,
			delta: { type: 'thinking_delta', thinking: text }
		};
		controller.enqueue(encoder.encode(`event: content_block_delta\ndata: ${JSON.stringify(event)}\n\n`));
	}

	// thinking 块收尾前必须补 signature_delta（协议形状；占位值，见 THINKING_SIG_PLACEHOLDER 注释）
	function sendThinkingSignature(controller) {
		const event = {
			type: 'content_block_delta',
			index: contentBlockIndex,
			delta: { type: 'signature_delta', signature: THINKING_SIG_PLACEHOLDER }
		};
		controller.enqueue(encoder.encode(`event: content_block_delta\ndata: ${JSON.stringify(event)}\n\n`));
	}

	function sendToolUseDelta(controller, argsDelta) {
		const event = {
			type: 'content_block_delta',
			index: contentBlockIndex,
			delta: { type: 'input_json_delta', partial_json: argsDelta }
		};
		controller.enqueue(encoder.encode(`event: content_block_delta\ndata: ${JSON.stringify(event)}\n\n`));
	}

	function sendToolUseFinalInput(controller) {
		// 发送最终的 content_block_stop
		controller.enqueue(encoder.encode(`event: content_block_stop\ndata: ${JSON.stringify({
			type: 'content_block_stop',
			index: contentBlockIndex
		})}\n\n`));
		blockStopSent = true;
	}

	function sendContentBlockStop(controller) {
		controller.enqueue(encoder.encode(`event: content_block_stop\ndata: ${JSON.stringify({
			type: 'content_block_stop',
			index: contentBlockIndex
		})}\n\n`));
	}

	function sendFinalEvent(controller) {
		if (finalSent) return;   // 只发一次（[DONE] 与「流自然结束」两条路径都会调到这里）
		finalSent = true;

		// 整条流没有任何内容块（空回复 / 上游只回 usage）时，必须先补 message_start，
		// 否则会先发 message_delta / message_stop —— 协议顺序非法，客户端会直接报错。
		if (!streamStarted) {
			sendMessageStart(controller);
			streamStarted = true;
		}

		// 有内容块且尚未收尾 → 补 content_block_stop；无内容块时 contentBlockIndex 为 -1，不能发（index:-1 非法）
		if (!blockStopSent && contentBlockIndex >= 0) {
			// thinking 块还开着（上游只出了思考、没有正文/工具）→ 先补 signature 再收
			if (thinkingOpen) {
				sendThinkingSignature(controller);
				thinkingOpen = false;
			}
			sendContentBlockStop(controller);
			blockStopSent = true;
		}

		// stop_reason 与非流式 convertOpenAIToAnthropic 对齐：length→max_tokens、tool_calls→tool_use
		let stopReason = 'end_turn';
		if (finalFinish === 'length') stopReason = 'max_tokens';
		else if (finalFinish === 'tool_calls' || currentToolCallId) stopReason = 'tool_use';

		const event = {
			type: 'message_delta',
			delta: {
				stop_reason: stopReason,
				stop_sequence: null
			},
			// usage 在流末尾才拿得到，故 message_start 里的 input_tokens 只能是 0；
			// 这里补上真实的 input/output，避免客户端看到的输入 token 恒为 0。
			usage: { input_tokens: inputTokens || 0, output_tokens: outputTokens || 0 }
		};
		controller.enqueue(encoder.encode(`event: message_delta\ndata: ${JSON.stringify(event)}\n\n`));

		controller.enqueue(encoder.encode(`event: message_stop\ndata: ${JSON.stringify({
			type: 'message_stop'
		})}\n\n`));
	}
}

// 从上游错误正文里抠出人能读的那句话（各家 JSON 结构不同，尽量都兜住）
function extractUpstreamMessage(errText) {
	try {
		const obj = JSON.parse(String(errText || ''));
		const candidates = [
			obj?.error?.message,
			obj?.error?.detail,
			obj?.message,
			obj?.detail,
			obj?.error_description
		];
		for (const c of candidates) {
			if (typeof c === 'string' && c.trim()) return c.trim();
		}
		// OpenAI 风格：[{ error: { message } }]
		if (Array.isArray(obj) && obj[0]?.error?.message) return String(obj[0].error.message);
	} catch (_) { }
	return '';
}

// 从上游错误正文里提取"应该改用哪个模型"的建议
// 例：Google —— "This model models/gemini-2.5-flash is no longer available to new users.
//      Please update your code to use models/gemini-3.8-flash for the latest features"
function extractSuggestedModel(errText) {
	const text = String(errText || '');
	let m = text.match(/use\s+(?:the\s+)?models\/([A-Za-z0-9._:-]+)/i);
	if (m) return m[1];
	m = text.match(/did you mean[^'"]*['"]([A-Za-z0-9._:/-]+)['"]/i);
	if (m) return m[1];
	return '';
}

// 探测单个模型是否可用：连通性 + 模型名有效性
// 只发一句 ping，刻意不带 max_tokens（思考型模型给太小的输出预算会被上游直接拒绝）
// Gemini 渠道的连通性探测：走**原生 generateContent**，与 callProvider→callGeminiNative 同一条路。
// 否则「测试」打的是 OpenAI 兼容端点、实际调用走原生，两者不一致：
//   ① 原生路径坏了（thought_signature / schema 清洗）测试却是绿的（假阳性）；
//   ② baseUrl 未带 /openai 时测试打 …/v1beta/chat/completions 会 404（假阴性）。
async function probeGeminiNative(provider, model, timeoutMs) {
	const base = geminiNativeBase(provider.baseUrl);
	const m = String(model || '').replace(/^models\//i, '');
	const endpoint = (base && m) ? `${base}/models/${m}:generateContent` : '';
	const startedAt = Date.now();
	if (!endpoint) {
		return { model, ok: false, status: 0, elapsed: 0, endpoint, native: true, error: '缺少 baseUrl 或模型名' };
	}
	try {
		const res = await fetch(endpoint, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json', 'x-goog-api-key': provider.apiKey || '' },
			body: JSON.stringify({
				contents: [{ role: 'user', parts: [{ text: 'ping' }] }],
				generationConfig: { maxOutputTokens: 16 }
			}),
			signal: AbortSignal.timeout(timeoutMs)
		});
		const elapsed = Date.now() - startedAt;
		if (res.ok) {
			const data = await res.json().catch(() => null);
			const reply = (((data && data.candidates && data.candidates[0] && data.candidates[0].content
				&& data.candidates[0].content.parts) || [])
				.map(p => (p && p.text) || '').join('')).slice(0, 80);
			return { model, ok: true, status: res.status, elapsed, endpoint, native: true, reply };
		}
		const errText = await res.text();
		return {
			model, ok: false, status: res.status, elapsed, endpoint, native: true,
			error: `HTTP ${res.status}: ${errText.slice(0, 400)}`,
			upstreamMessage: extractUpstreamMessage(errText),
			suggestedModel: extractSuggestedModel(errText),
			// native=true：不要给出「要填 …/v1beta/openai」那条对原生渠道是错误建议的提示
			hint: buildProviderTestHint(res.status, provider.baseUrl, errText, model, true)
		};
	} catch (e) {
		const isTimeout = e && e.name === 'TimeoutError';
		return {
			model, ok: false, status: 0, timedOut: isTimeout, elapsed: Date.now() - startedAt, endpoint, native: true,
			error: isTimeout ? `超时（${Math.round(timeoutMs / 1000)} 秒内没有响应）` : `连接失败: ${e.message}`,
			hint: isTimeout
				? '上游长时间没返回。思考型模型本身较慢，可以稍后重试；若多次超时，检查该渠道是否可用。'
				: '检查 Base URL 是否可达（域名拼写、是否需要走代理），以及该地址是否对公网开放。'
		};
	}
}

async function probeProviderModel(baseUrl, apiKey, model, timeoutMs = 25000, provider = null) {
	// 测试必须跟真实调用走同一条路：Gemini 渠道实际走原生 generateContent（见 callProvider）
	const asProvider = provider || { baseUrl, apiKey };
	if (isGeminiProvider(asProvider)) return probeGeminiNative({ ...asProvider, baseUrl, apiKey }, model, timeoutMs);

	const endpoint = String(baseUrl || '').replace(/\/+$/, '') + '/chat/completions';
	const headers = { 'Content-Type': 'application/json' };
	if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;
	const startedAt = Date.now();

	try {
		const res = await fetch(endpoint, {
			method: 'POST',
			headers,
			body: JSON.stringify({ model, messages: [{ role: 'user', content: 'ping' }] }),
			signal: AbortSignal.timeout(timeoutMs)
		});
		const elapsed = Date.now() - startedAt;

		if (res.ok) {
			const data = await res.json().catch(() => null);
			return {
				model,
				ok: true,
				status: res.status,
				elapsed,
				endpoint,
				reply: String(data?.choices?.[0]?.message?.content || '').slice(0, 80)
			};
		}

		const errText = await res.text();
		return {
			model,
			ok: false,
			status: res.status,
			elapsed,
			endpoint,
			error: `HTTP ${res.status}: ${errText.slice(0, 400)}`,
			upstreamMessage: extractUpstreamMessage(errText),
			suggestedModel: extractSuggestedModel(errText),
			hint: buildProviderTestHint(res.status, baseUrl, errText, model)
		};
	} catch (e) {
		const isTimeout = e && e.name === 'TimeoutError';
		return {
			model,
			ok: false,
			status: 0,
			timedOut: isTimeout,
			elapsed: Date.now() - startedAt,
			endpoint,
			error: isTimeout ? `超时（${Math.round(timeoutMs / 1000)} 秒内没有响应）` : `连接失败: ${e.message}`,
			hint: isTimeout
				? '上游长时间没返回。思考型模型本身较慢，可以稍后重试；若多次超时，检查该渠道是否可用。'
				: '检查 Base URL 是否可达（域名拼写、是否需要走代理），以及该地址是否对公网开放。'
		};
	}
}

// 把常见失败翻译成可操作的提示，避免"测试不通但不知道为什么"
// 优先级：输入本身的硬事实 → 上游正文里的明确线索 → baseUrl 路径事实 → 状态码兜底
function buildProviderTestHint(status, baseUrl, errText, model, native = false) {
	const lowerBase = String(baseUrl || '').toLowerCase();
	const lowerErr = String(errText || '').toLowerCase();

	// --- 输入本身的硬事实，最先检查 ---
	// 模型 ID 里不可能有空格，出现空格基本是把产品显示名（"Gemini 3.8 Flash"）当 ID 填了
	if (/\s/.test(String(model || ''))) {
		return '模型名里不能有空格，要填 API 里的完整 ID（例如 gemini-3.8-flash），不能填 "Gemini 3.8 Flash" 这种显示名。'
			+ '拿不准就点上面的「从上游拉取模型列表」。';
	}

	// --- 上游正文里的明确线索，优先 ---
	// 「模型已下线 / 已对新用户关闭」——地址和密钥都是对的，唯一的问题就是模型名
	if (lowerErr.includes('no longer available') || lowerErr.includes('update your code to use') || lowerErr.includes('is deprecated') || lowerErr.includes('has been retired')) {
		const suggested = extractSuggestedModel(errText);
		return suggested
			? `模型名已过期（地址和密钥都没问题）。上游建议改用 ${suggested}，点右侧「换用它」即可替换。`
			: '模型名已过期，地址和密钥都没问题。请到上游文档换一个当前可用的模型名。';
	}
	if (lowerErr.includes('user location is not supported')) {
		return 'Google 判定请求来源地区不受支持 —— 这是 Cloudflare 边缘节点出口地区的问题，不是你的配置错。可以改用其他渠道，或把该渠道指向一个不受限的中转。';
	}
	if (lowerErr.includes('api key not valid') || lowerErr.includes('api_key_invalid') || lowerErr.includes('invalid api key')) {
		return 'API Key 无效。Gemini 要用 Google AI Studio（aistudio.google.com）创建的 key（一般以 AIza 开头），GCP/Vertex 的服务账号凭证不能用于这个端点。';
	}

	// --- Base URL 本身就是硬事实，比正文里的"模型不存在"更值得先修 ---
	// （地址少一层时上游也常回"model not found"，照正文提示会把人带偏）
	// native 渠道（走原生 generateContent）不需要 /openai —— 这条提示只对「被迫落回 OpenAI 兼容端点」的渠道成立
	if (!native && lowerBase.includes('generativelanguage.googleapis.com') && !lowerBase.includes('/openai')) {
		return 'Base URL 少了一层：Gemini 的原生端点不是 OpenAI 格式，要填 https://generativelanguage.googleapis.com/v1beta/openai。';
	}
	if (lowerBase.includes('api.anthropic.com') && !/\/v1\/?$/.test(lowerBase)) {
		return 'Claude 的 OpenAI 兼容端点基地址是 https://api.anthropic.com/v1，路径写多或写少都会 404。';
	}

	// --- 再看正文里的模型名 / 配额类线索 ---
	if (lowerErr.includes('resource_exhausted') || lowerErr.includes('quota') || lowerErr.includes('rate limit') || lowerErr.includes('too many requests')) {
		return '触及配额或限流：确认该 key 的免费额度没用完、没被每分钟请求数限制。';
	}
	if (lowerErr.includes('not found for api version') || lowerErr.includes('is not supported for') || lowerErr.includes('unknown model') || lowerErr.includes('model not found') || lowerErr.includes('does not exist')) {
		return '上游不认识这个模型名。要填 API 里的完整 ID（不要带 models/ 前缀、注意大小写与命名空间），拿不准就点「从上游拉取模型列表」。';
	}
	if (lowerErr.includes('invalid model') || (lowerErr.includes('model') && status === 400)) {
		return '上游认为模型名不合法。检查是否混入了显示名、空格或中文，拿不准就点「从上游拉取模型列表」。';
	}
	if (lowerErr.includes('location') || lowerErr.includes('region')) {
		return '上游提示地区/区域限制，通常与出口地区有关，不是本地配置问题。';
	}

	// --- 最后按状态码兜底 ---
	if (status === 404) {
		return '404 有两种可能：① Base URL 路径不对（只填到 /v1、/v1beta/openai、/api/v3 为止，别带 /chat/completions）；② 这个模型名在该账号下不存在或已下线。请以上面的原始错误正文为准。';
	}
	if (status === 401 || status === 403) {
		return '密钥无效或没有该模型的权限，确认 API Key 是否正确、是否已开通对应模型。';
	}
	if (status === 400) {
		return '上游拒绝了请求，最常见是模型名不对。请填写服务商文档里的完整模型 ID（注意大小写与命名空间前缀，如 openai/gpt-4o、deepseek-ai/DeepSeek-V3），或用「从上游拉取模型列表」。';
	}
	if (status === 429) {
		return '被限流或额度不足，稍后重试或检查账户余额。';
	}
	if (status >= 500) {
		return '上游服务端异常，通常不是你的配置问题，稍后重试。';
	}
	if (lowerErr.includes('model')) {
		return '错误信息里提到了 model，优先核对模型名。';
	}
	return '';
}

// ----------------------------------------------------
// 模型规格元信息（上下文窗口 / 最大输出 / 价格 / 能力）
// 不同上游 /models 的字段名各不相同，这里归一化成统一视图：
//   OpenRouter 风格：context_length / pricing / architecture.input_modalities / top_provider.max_completion_tokens
//   Google 原生风格：inputTokenLimit / outputTokenLimit / displayName / supportedGenerationMethods
//   通用 OpenAI 风格：多数只回 id（拿不到的字段一律留空，前端显示「—」）
// ----------------------------------------------------
const MODEL_INFO_CACHE_MS = 5 * 60 * 1000;
const modelInfoCache = new Map(); // providerId -> { at, data }

function firstVal(...vals) {
	for (const v of vals) if (v !== undefined && v !== null && v !== '') return v;
	return undefined;
}
function numOrNull(v) {
	const n = Number(v);
	return Number.isFinite(n) && n > 0 ? n : null;
}
// 渠道自定义单价（美元 / 百万 tokens）。undefined = 本次未提交（编辑时保留原值）；null = 清空；对象 = 设置。
// 2026-10-10：0/0 是合法的显式值 = 「免费渠道」（看板成本按 0 算）；两字段都缺/无效仍视为清空；负数按「该字段未填」。
function sanitizeProviderPricing(raw) {
	if (raw === undefined) return undefined;
	if (!raw || typeof raw !== 'object') return null;
	const field = (v) => {
		if (v === undefined || v === null || v === '') return null;
		const n = Number(v);
		return Number.isFinite(n) && n >= 0 ? n : null;
	};
	const inputPer1M = field(raw.inputPer1M);
	const outputPer1M = field(raw.outputPer1M);
	if (inputPer1M === null && outputPer1M === null) return null;
	return { inputPer1M: inputPer1M || 0, outputPer1M: outputPer1M || 0 };
}
function normalizeModelSpec(m) {
	if (typeof m === 'string') {
		return {
			id: m.replace(/^models\//, '').trim(), name: '', description: '',
			contextLength: null, maxOutput: null, pricing: null,
			inputModalities: null, outputModalities: null, supportedParameters: null, supportedMethods: null
		};
	}
	if (!m || typeof m !== 'object') return null;
	const arch = (m.architecture && typeof m.architecture === 'object') ? m.architecture : {};
	const tp = (m.top_provider && typeof m.top_provider === 'object') ? m.top_provider : {};
	const pricing = (m.pricing && typeof m.pricing === 'object') ? m.pricing : null;
	const id = String(firstVal(m.id, m.name, m.model) || '').replace(/^models\//, '').trim();
	if (!id) return null;
	const displayName = firstVal(m.displayName, m.display_name);
	return {
		id,
		name: displayName ? String(displayName) : ((typeof m.name === 'string' && m.name !== id) ? m.name : ''),
		description: String(firstVal(m.description) || ''),
		contextLength: numOrNull(firstVal(m.context_length, m.contextLength, m.inputTokenLimit, m.input_token_limit, tp.context_length, m.context_window)),
		maxOutput: numOrNull(firstVal(tp.max_completion_tokens, m.max_completion_tokens, m.max_output_tokens, m.outputTokenLimit, m.output_token_limit)),
		pricing,
		inputModalities: Array.isArray(arch.input_modalities) ? arch.input_modalities : (Array.isArray(m.input_modalities) ? m.input_modalities : null),
		outputModalities: Array.isArray(arch.output_modalities) ? arch.output_modalities : null,
		supportedParameters: Array.isArray(m.supported_parameters) ? m.supported_parameters : null,
		supportedMethods: Array.isArray(m.supportedGenerationMethods) ? m.supportedGenerationMethods : null
	};
}

// Gemini 原生 /models —— OpenAI 兼容层只回 id，原生才有 inputTokenLimit / outputTokenLimit / displayName。
// 失败静默返回 null：它只是「补充」，不该让整个拉取失败。
async function fetchGeminiNativeModels(provider) {
	const base = geminiNativeBase(provider.baseUrl);
	if (!base) return null;
	try {
		const res = await fetch(base + '/models', {
			headers: { 'x-goog-api-key': provider.apiKey || '' },
			signal: AbortSignal.timeout(20000)
		});
		if (!res.ok) return null;
		const data = await res.json().catch(() => null);
		const list = Array.isArray(data && data.models) ? data.models : [];
		return list.map(normalizeModelSpec).filter(d => d && d.id);
	} catch (e) { return null; }
}

// 拉取并归一化某渠道的模型规格（打上游 {baseUrl}/models，Gemini 再补一手原生 /models）。
// 两处共用：后台「模型规格」弹窗（要详细错误）与 /v1/models 的后台预热（只看成败）。
// 返回 { ok:true, payload } 或 { ok:false, ...失败细节 }；成功时由调用方写入 modelInfoCache。
async function buildModelSpecPayload(provider) {
	const baseUrl = String(provider.baseUrl || '').replace(/\/+$/, '');
	const headers = { 'Content-Type': 'application/json' };
	if (provider.apiKey) headers['Authorization'] = `Bearer ${provider.apiKey}`;
	const endpoint = baseUrl + '/models';

	try {
		const res = await fetch(endpoint, { method: 'GET', headers, signal: AbortSignal.timeout(20000) });
		if (!res.ok) {
			const errText = await res.text();
			return { ok: false, endpoint, status: res.status, errText };
		}

		const data = await res.json().catch(() => null);
		const rawList = Array.isArray(data?.data) ? data.data
			: Array.isArray(data?.models) ? data.models
				: Array.isArray(data) ? data : [];

		// 归一化每个模型的规格（各家字段名不同，统一成一套；拿不到的留空）
		const details = rawList.map(normalizeModelSpec).filter(d => d && d.id);
		const models = details.map(d => d.id);

		// Gemini 渠道再补一手原生 /models：OpenAI 兼容层只回 id，原生才有 inputTokenLimit / outputTokenLimit 等
		if (isGeminiProvider(provider)) {
			const native = await fetchGeminiNativeModels(provider);
			if (Array.isArray(native) && native.length) {
				const byId = new Map();
				for (const d of details) byId.set(d.id.toLowerCase(), d);
				for (const n of native) {
					const base = byId.get(n.id.toLowerCase());
					if (base) {
						// 只补兼容层缺的字段，不覆盖已有值
						for (const k of Object.keys(n)) {
							if ((base[k] === undefined || base[k] === null || base[k] === '') && n[k] !== undefined && n[k] !== null) base[k] = n[k];
						}
					} else {
						details.push(n);
						models.push(n.id);
					}
				}
			}
		}

		return { ok: true, payload: { success: true, endpoint, count: models.length, models, details } };
	} catch (e) {
		return { ok: false, endpoint, transport: true, error: `连接失败: ${e.message}` };
	}
}

// /v1/models 的后台预热：把渠道规格悄悄拉进 modelInfoCache。
// ① 只在有 ctx.waitUntil 时执行 —— 响应已经发出，绝不让客户端等上游；
// ② 已有新鲜缓存则跳过；③ 同一渠道在缓存窗口内最多尝试一次，避免上游故障时被反复打。
const modelSpecWarmAt = new Map(); // providerId -> 上次预热尝试时间
function warmProviderSpecs(provider, ctx) {
	if (!ctx || typeof ctx.waitUntil !== 'function') return;
	const key = 'models:' + provider.id;
	const hit = modelInfoCache.get(key);
	if (hit && Date.now() - hit.at < MODEL_INFO_CACHE_MS) return;
	const last = modelSpecWarmAt.get(provider.id) || 0;
	if (Date.now() - last < MODEL_INFO_CACHE_MS) return;
	modelSpecWarmAt.set(provider.id, Date.now());

	const task = (async () => {
		const result = await buildModelSpecPayload(provider);
		if (result.ok) modelInfoCache.set(key, { at: Date.now(), data: result.payload });
	})().catch(() => { });
	ctx.waitUntil(task);
}


// 透传 CF /ai/v1/chat/completions 返回的 SSE 流
// CF 返回的本来就是标准 OpenAI 的 SSE 格式，我们只把模型名改一下，
// 这样 tool_calls、finish_reason、reasoning_content、usage 等字段都能原样保留。
// SSE 心跳包装：在「已经开始输出、但下游暂时没有新数据」的空档里，
// 周期性往外发一个 SSE 注释帧（":" 开头，规范要求客户端忽略），
// 让 Cloudflare 边缘始终能看到数据在流动，避免长请求被判定为「响应不完整」而回 502。
// 一旦内层结束（done）或出错，就停止心跳并如实结束/抛出，不吞掉真实错误。
function withSseHeartbeat(innerStream, intervalMs = SSE_HEARTBEAT_MS) {
	const reader = innerStream.getReader();
	const encoder = new TextEncoder();
	const PING = encoder.encode(': keep-alive\n\n');

	return new ReadableStream({
		start(controller) {
			let closed = false;
			// 立刻先发一帧：连续对话时上游首字节可能很久，先把「还活着」告诉边缘
			try { controller.enqueue(PING); } catch (_) { }

			const timer = setInterval(() => {
				if (closed) return;
				try { controller.enqueue(PING); } catch (_) { }
			}, Math.max(1000, Number(intervalMs) || SSE_HEARTBEAT_MS));

			const finish = () => { if (!closed) { closed = true; clearInterval(timer); } };

			(async () => {
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) break;
						if (!closed) controller.enqueue(value);
					}
					if (!closed) { controller.close(); }
				} catch (e) {
					if (!closed) {
						// 兜底：内层流异常时也透出错误事件再收尾（正常情况下内层已自行处理）
						try {
							controller.enqueue(encoder.encode(`data: {"error":{"message":${JSON.stringify(String(e && e.message || e))},"type":"server_error","code":"upstream_stream_error"}}\n\n`));
							controller.close();
						} catch (_) { }
					}
				} finally {
					finish();
				}
			})();
		},
		cancel(reason) {
			try { reader.cancel(reason); } catch (_) { }
		}
	});
}

// 流式透传里只替换 model 字段（定点替换，避免整段 JSON.parse+stringify）。上游 model 恒为字符串，值内不含转义引号。
const MODEL_FIELD_RE = /("model"\s*:\s*)"[^"]*"/;

// Opt3（2026-10-10）：把原 withUsageTap（token 抽取层）+ withSseHeartbeat（心跳层）全部融合进透传层。
// 为什么：免费档 CPU 10ms 下长流是杀手（实测 ~5978-chunk / 1.8MB 流被 exceededResources 杀掉）。
// 两轮实测锁定了真凶 —— 成本在**每层 hop 的 read+enqueue**（~330µs/chunk，文本处理只占零头）：
//   .147（tap+透传 2 层）与 .148（合并层+心跳 2 层）死亡点完全相同（~6k chunks / 2000ms CPU）。
// 所以这一版：① 三层合一 —— 1 次 read + 最多 1 次 enqueue/hop；
//   ② **批量冲刷** —— 攒满 FLUSH_BYTES 或上游停顿 15ms 才 enqueue 一次，把几千次 enqueue
//   压到几十次（1.8MB ≈ 55 次）。聊天场景每 chunk 最多多 15ms 延迟，客户端渲染无感。
// onUsage：流结束/中断时回调一次 token 统计（fire-once 语义）；不传 = 纯透传。
// ⚠️ 阈值全部用字面量：验证脚本会把本函数单独抽出 eval，引用外部常量会 ReferenceError。
//    （32KB 攒批 / 15ms 凑批等待 / 心跳 10s —— 想调就改这里的字面量）
function passthroughStream(upstreamBody, modelName, onUsage, heartbeatMs) {
	const reader = upstreamBody.getReader();
	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	let buffer = '';
	let pending = '';   // 待冲刷的输出（攒批）
	// —— 原 withUsageTap 的统计状态（内联） ——
	let maxTotal = 0;
	let maxReasoning = 0;
	let maxInput = 0;
	let maxOutput = 0;
	// ★ 一次性闸门：客户端中途断开时，cancel() 与收尾 finally 都会跑到，只许落库一次。
	let fired = false;
	const fire = () => {
		if (fired) return;
		fired = true;
		if (!onUsage) return;
		try {
			onUsage({ tokens: maxTotal, reasoningTokens: maxReasoning, inputTokens: maxInput, outputTokens: maxOutput });
		} catch (e) { /* 统计失败绝不影响请求 */ }
	};

	return new ReadableStream({
		// ⚠️ 用「主动排空」（eager drain）而不是 pull(controller)。
		// 旧写法是 pull 驱动的（由运行时按下游需求来调用），但在 Workers 上
		// Response body 的反压不一定能可靠地传回用户侧的 ReadableStream ——
		// pull 可能**永远不被调用**（甚至一次都没有）→ 一个字节都流不出去 →
		// 运行时判定「Worker 代码 hang」→ CF 边缘回 502 origin_bad_gateway。
		// 这正是「短对话/测试正常、长对话与连续对话必挂」的成因：
		// 短回复恰好在一个 pull 周期内读完，长的需要反复 pull 就死。
		// 改成在 start 里开一个独立循环主动读干上游，完全不依赖下游需求。
		start(controller) {
			let timer = null;
			let closed = false;
			const PING = encoder.encode(': keep-alive\n\n');
			// 心跳（原 withSseHeartbeat 内联）：立刻先发一帧，告诉边缘「还活着」
			try { controller.enqueue(PING); } catch (_) { }
			timer = setInterval(() => {
				if (closed) return;
				try { controller.enqueue(PING); } catch (_) { }
			}, Math.max(1000, Number(heartbeatMs) || 10000));
			const finish = () => { if (!closed) { closed = true; clearInterval(timer); } };

			(async () => {
				try {
					let scanFrom = 0;   // buffer 中已扫描过的位置（一轮 compact 一次，不再逐行 slice）
					const flush = () => {
						if (!pending) return;
						controller.enqueue(encoder.encode(pending));
						pending = '';
					};
					while (true) {
						// 攒批：够 32KB 先冲再读；手里有半批 → read 和 15ms 定时器赛跑，
						// 上游停顿就先冲已有的（保延迟）；data 先到就用 race 里已完成的那次 read，绝不重读。
						let value, done;
						if (pending.length >= 32768) {
							flush();
							({ value, done } = await reader.read());
						} else if (pending.length > 0) {
							const readp = reader.read();
							let tickHandle;
							const tickp = new Promise(res => { tickHandle = setTimeout(() => res(null), 15); });
							let res = await Promise.race([readp.then(v => ({ v })), tickp]);
							clearTimeout(tickHandle);
							if (res === null) {
								flush();   // 上游停顿 → 先把已有的发出去
								res = { v: await readp };   // 继续等同一个 read（不丢数据、不重复读）
							}
							({ value, done } = res.v);
						} else {
							({ value, done } = await reader.read());
						}
						if (done) {
							// 把缓冲区里剩下的内容输出掉
							if (buffer.slice(scanFrom).trim()) {
								processLines(buffer.slice(scanFrom), 0);
							}
							flush();
							controller.enqueue(encoder.encode('data: [DONE]\n\n'));
							controller.close();
							break;
						}

						buffer += decoder.decode(value, { stream: true });
						scanFrom = processLines(buffer, scanFrom);
						if (scanFrom > 0) {
							buffer = buffer.slice(scanFrom);   // 每 chunk 只 compact 一次
							scanFrom = 0;
						}
					}
				} catch (e) {
					// 上游断流：发一条错误事件再正常收尾 —— 让客户端看到真实原因，
					// 而不是让边缘拿「不完整响应」去回 502（2026-10-05）。错误后不补 [DONE]，避免被当成正常结束
					try {
						controller.enqueue(encoder.encode(`data: {"error":{"message":${JSON.stringify(String(e && e.message || e))},"type":"server_error","code":"upstream_stream_error"}}\n\n`));
						controller.close();
					} catch (_) { }
				} finally {
					finish();
					fire();
				}
			})();
		},
		cancel(reason) {
			try { reader.cancel(reason); } catch (_) { }
			fire();
		},
	});

	// 扫描 data.slice(from) 里的完整行：model 定点替换 + token 抽取，攒进 pending（由调用方批量冲刷）。
	// 返回未处理行的起始下标。
	function processLines(data, from) {
		let pos = from;
		while (true) {
			const nl = data.indexOf('\n', pos);
			if (nl === -1) break;
			let line = data.slice(pos, nl);
			pos = nl + 1;
			if (line.endsWith('\r')) line = line.slice(0, -1);   // CRLF 上游；等价原 trim() 但零分配
			if (line.length === 0) continue;
			if (line.startsWith('data: ')) {
				const dataStr = line.slice(6);
				if (dataStr === '[DONE]') continue;
				try {
					// Opt2: 只定点替换 model 字段，其余字节原样透传 —— 避免每个 chunk 都 JSON.parse+JSON.stringify。
					// 函数式 replacer 规避替换串里的 $ 注入；无 model 字段的行 replace 无匹配、原串返回（零分配）。
					const outStr = dataStr.replace(MODEL_FIELD_RE, (_, p1) => p1 + JSON.stringify(modelName));
					pending += 'data: ' + outStr + '\n\n';
				} catch (_) {
					// 解析不了的行，按原样转发
					pending += line + '\n';
				}
				// Opt1: 只含 token 字段的行才跑 4 条正则（内容增量块几乎都不含，省掉 ~95% 正则执行）
				if (dataStr.indexOf('tokens') !== -1) {
					const m = /"total_tokens"\s*:\s*(\d+)/.exec(dataStr);
					if (m) maxTotal = Math.max(maxTotal, Number(m[1]) || 0);
					const mr = /"reasoning_tokens"\s*:\s*(\d+)/.exec(dataStr);
					if (mr) maxReasoning = Math.max(maxReasoning, Number(mr[1]) || 0);
					const mi = /"prompt_tokens"\s*:\s*(\d+)/.exec(dataStr);
					if (mi) maxInput = Math.max(maxInput, Number(mi[1]) || 0);
					const mo = /"completion_tokens"\s*:\s*(\d+)/.exec(dataStr);
					if (mo) maxOutput = Math.max(maxOutput, Number(mo[1]) || 0);
				}
			} else {
				// 非 data 开头的 SSE 行（注释、事件等），原样转发
				pending += line + '\n';
			}
		}
		return pos;
	}
}

// ----------------------------------------------------
// 后台管理面板的 API 接口处理函数
// ----------------------------------------------------
async function handleDashboardApi(request, env, ctx) {
	const url = new URL(request.url);
	const method = request.method;

	// 1. 查询初始化状态（密码通过环境变量配置，所以这里永远返回已初始化）
	if (url.pathname === '/api/auth/status' && method === 'GET') {
		const cred = adminCredentials(env);
		return new Response(JSON.stringify({
			isSetup: true,
			requireUsername: cred.requireUsername
		}), { headers: { 'Content-Type': 'application/json' } });
	}

	// 2. 设置首个管理员密码（已停用，改由环境变量 ADMIN_PASSWORD 配置）
	if (url.pathname === '/api/auth/setup' && method === 'POST') {
		return new Response(JSON.stringify({ error: 'Setup is handled via environment variable ADMIN_PASSWORD' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
	}

	// 3. 登录（直接比对凭据，不读写 KV，既快又省钱）
	if (url.pathname === '/api/auth/login' && method === 'POST') {
		const body = await request.json().catch(() => ({}));
		const cred = adminCredentials(env);
		if (!cred.password) {
			return new Response(JSON.stringify({ error: '管理员密码未配置（需设置环境变量 ADMIN_PASSWORD）' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
		}
		// 限流：同一来源连续失败过多 → 先锁一段时间
		const ip = clientIpOf(request);
		const lockLeft = loginLockLeft(ip);
		if (lockLeft > 0) {
			const secs = Math.ceil(lockLeft / 1000);
			return new Response(JSON.stringify({ error: `尝试过于频繁，请 ${secs} 秒后再试` }), {
				status: 429,
				headers: { 'Content-Type': 'application/json', 'Retry-After': String(secs) }
			});
		}
		const okUser = cred.requireUsername ? (String(body.username || '') === cred.username) : true;
		const okPass = typeof body.password === 'string' && body.password === cred.password;
		if (okUser && okPass) {
			clearLoginFailures(ip);
		// 持久 cookie（Max-Age=14400 = 4 小时）→ 关窗口/关浏览器 4 小时内重开仍登录；
		// 令牌内 exp 同时放宽到 4 小时，与 Max-Age 对齐，杜绝「cookie 在、服务器已拒」
		const token = makeAdminSessionToken(await adminTokenHash(env));
		return new Response(JSON.stringify({ success: true }), {
			headers: {
				'Content-Type': 'application/json',
				'Set-Cookie': `admin_token=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=14400`
			}
		});
		}
		noteLoginFailure(ip);
		// 用户名/密码错误一律返回同一句话，避免泄露是哪一项错
		return new Response(JSON.stringify({ error: cred.requireUsername ? '用户名或密码不正确' : '密码不正确' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
	}

	// 4. 退出登录
	if (url.pathname === '/api/auth/logout' && method === 'POST') {
		return new Response(JSON.stringify({ success: true }), {
			headers: {
				'Content-Type': 'application/json',
				'Set-Cookie': `admin_token=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`
			}
		});
	}

	// 5. 公开的用量汇总（首页未登录时也能看到）
	if (url.pathname === '/api/usage/summary') {
		if (method === 'GET') {
			const cached = await getCachedSummary(env);
			if (cached) {
				return new Response(JSON.stringify(cached), { headers: { 'Content-Type': 'application/json' } });
			}

			const accounts = await getAccounts(env);
			if (accounts.length === 0) {
				return new Response(JSON.stringify({
					totalNeuronsToday: 0,
					totalAccounts: 0,
					totalLimit: 0,
					usagePercentage: 0,
					needUpdate: false
				}), { headers: { 'Content-Type': 'application/json' } });
			}

			// 读取缓存的卡片明细来检查更新时间
			const cachedDetailsRaw = await env.KV.get('cache_usage_details');
			let cacheMap = {};
			if (cachedDetailsRaw) {
				try {
					cacheMap = JSON.parse(cachedDetailsRaw) || {};
				} catch (e) { }
			}

			// 判断是否有任意一个账号的更新时间超过了 20 分钟 (20 * 60 * 1000)
			const now = Date.now();
			const hasOutdated = accounts.some(account => {
				const lastUpdated = cacheMap[account.id]?.timestamp || 0;
				return (now - lastUpdated) > 20 * 60 * 1000;
			});

			// 计算当前缓存中的汇总数据和模型占比
			let totalNeuronsToday = 0;
			let modelsToday = {};
			accounts.forEach(account => {
				const cachedItem = cacheMap[account.id];
				if (cachedItem) {
					if (cachedItem.usageToday) {
						totalNeuronsToday += cachedItem.usageToday;
					}
					if (cachedItem.modelsToday) {
						cachedItem.modelsToday.forEach(m => {
							modelsToday[m.model] = (modelsToday[m.model] || 0) + m.neurons;
						});
					}
				}
			});

			const formattedModelsToday = Object.keys(modelsToday).map(model => ({
				model,
				neurons: modelsToday[model]
			}));

			const totalLimit = accounts.length * 10000;
			const usagePercentage = totalLimit > 0 ? parseFloat(((totalNeuronsToday / totalLimit) * 100).toFixed(2)) : 0;

			const summary = {
				totalNeuronsToday,
				totalAccounts: accounts.length,
				totalLimit,
				usagePercentage,
				modelsToday: formattedModelsToday,
				needUpdate: hasOutdated
			};

			await setCachedSummary(env, summary);
			return new Response(JSON.stringify(summary), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			// 刷新用量是管理员操作（触发 20 个账号的 GraphQL 查询 + KV 写）：
			// 必须鉴权，否则任何人都能匿名刷这个接口烧配额（P0 修复 2026-10-05）
			if (!(await checkAdminAuth(request, env))) {
				return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
			}
			const accounts = await getAccounts(env);
			if (accounts.length === 0) {
				return new Response(JSON.stringify({
					totalNeuronsToday: 0,
					totalAccounts: 0,
					totalLimit: 0,
					usagePercentage: 0,
					modelsToday: [],
					needUpdate: false
				}), { headers: { 'Content-Type': 'application/json' } });
			}

			// 刷新最老数据的 20 个账号
			const cacheMap = await refreshAccountsUsage(env, accounts, 20);

			// 计算最新总量和模型占比
			let totalNeuronsToday = 0;
			let modelsToday = {};
			accounts.forEach(account => {
				const cachedItem = cacheMap[account.id];
				if (cachedItem) {
					if (cachedItem.usageToday) {
						totalNeuronsToday += cachedItem.usageToday;
					}
					if (cachedItem.modelsToday) {
						cachedItem.modelsToday.forEach(m => {
							modelsToday[m.model] = (modelsToday[m.model] || 0) + m.neurons;
						});
					}
				}
			});

			const formattedModelsToday = Object.keys(modelsToday).map(model => ({
				model,
				neurons: modelsToday[model]
			}));

			const totalLimit = accounts.length * 10000;
			const usagePercentage = totalLimit > 0 ? parseFloat(((totalNeuronsToday / totalLimit) * 100).toFixed(2)) : 0;

			const summary = {
				totalNeuronsToday,
				totalAccounts: accounts.length,
				totalLimit,
				usagePercentage,
				modelsToday: formattedModelsToday,
				needUpdate: false
			};

			await setCachedSummary(env, summary);
			return new Response(JSON.stringify(summary), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 10b. 第三方渠道调用统计 · 落地页公开汇总（本代理埋点，与 CF 官方账单独立）
	// 只暴露「今日总请求数 + 整体成功率 + Token」，不含渠道/模型明细（2026-10-06）。
	// ⚠️ 必须放在下方 checkAdminAuth 鉴权门之前 —— 否则未登录拿 401，落地页「第三方渠道 · 今日」卡片不显示（.54 修过一次）
	if (url.pathname === '/api/public/provider-stats' && method === 'GET') {
		const now = Date.now();
		if (!publicProviderSummaryCache || now - publicProviderSummaryCache.t > 30000) {
			const q = await queryProviderStats(env, 'today');
			const s = q.summary || {};
			// .47 起 shape 的 req/ok 已含探测，不再叠加 probeReq/probeOk（否则双重计数）
			const total = s.req || 0;
			const okSum = s.ok || 0;
			publicProviderSummaryCache = {
				t: now,
				data: {
					enabled: q.enabled === true,
					total,
					rate: total ? Math.round(okSum / total * 1000) / 10 : null,
					tokens: s.tokens || 0
				}
			};
		}
		return new Response(JSON.stringify(publicProviderSummaryCache.data), { headers: { 'Content-Type': 'application/json' } });
	}

	// --------------------------------------------------
	// 下面这些都是需要登录后才能访问的接口
	// --------------------------------------------------
	const isAuthorized = await checkAdminAuth(request, env);
	if (!isAuthorized) {
		return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
	}

	if (url.pathname === '/api/accounts') {
		if (method === 'GET') {
			const accounts = await getAccounts(env);
			return new Response(JSON.stringify(accounts), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const { id, name, accountId, apiToken } = await request.json();
			if (!accountId || !apiToken) {
				return new Response(JSON.stringify({ error: 'AccountId and ApiToken are required' }), { status: 400 });
			}

			let accounts = await getAccounts(env);
			if (id) {
				// 编辑已有账号
				accounts = accounts.map(a => {
					if (a.id === id) {
						const updatedToken = (apiToken.includes('...') || apiToken === '********') ? a.apiToken : apiToken;
						return { ...a, name: name || a.name, accountId, apiToken: updatedToken };
					}
					return a;
				});
			} else {
				// 新增账号
				accounts.push({
					id: crypto.randomUUID(),
					name: name || 'CF Account',
					accountId,
					apiToken,
					status: 'active'
				});
			}
			await saveAccounts(env, accounts);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'DELETE') {
			const { id } = await request.json();
			let accounts = await getAccounts(env);
			accounts = accounts.filter(a => a.id !== id);
			await saveAccounts(env, accounts);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 7. 测试账号是否能正常连接
	if (url.pathname === '/api/accounts/test' && method === 'POST') {
		const { id, accountId, apiToken } = await request.json();
		let targetAccountId = accountId;
		let targetApiToken = apiToken;

		if (id) {
			const accounts = await getAccounts(env);
			const acc = accounts.find(a => a.id === id);
			if (acc) {
				if (!targetAccountId) targetAccountId = acc.accountId;
				if (!targetApiToken || targetApiToken.includes('...') || targetApiToken === '********') {
					targetApiToken = acc.apiToken;
				}
			}
		}

		if (!targetAccountId || !targetApiToken) {
			return new Response(JSON.stringify({ success: false, error: 'Account info not found' }), { status: 400 });
		}

		const [readResult, analyticsResult] = await Promise.all([
			// 1. Workers AI > Read
			(async () => {
				try {
					const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${targetAccountId}/ai/models/search?limit=1`, {
						method: 'GET',
						headers: {
							'Authorization': `Bearer ${targetApiToken}`,
							'Content-Type': 'application/json'
						}
					});
					const data = await res.json();
					if (res.ok && data.success !== false) {
						return { success: true };
					}
					return { success: false, error: data.errors?.[0]?.message || `HTTP ${res.status}` };
				} catch (e) {
					return { success: false, error: e.message };
				}
			})(),
			// 2. Account Analytics > Read
			(async () => {
				try {
					const query = `
						query GetAIUsage($accountId: String!, $start: String!) {
							viewer {
								accounts(filter: { accountTag: $accountId }) {
									aiInferenceAdaptiveGroups(
										filter: { datetime_geq: $start }
										limit: 1
									) {
										count
									}
								}
							}
						}
					`;
					const todayUTC = new Date();
					todayUTC.setUTCHours(0, 0, 0, 0);
					const startToday = todayUTC.toISOString().split('.')[0] + 'Z';

					const res = await fetch(`https://api.cloudflare.com/client/v4/graphql`, {
						method: 'POST',
						headers: {
							'Authorization': `Bearer ${targetApiToken}`,
							'Content-Type': 'application/json'
						},
						body: JSON.stringify({
							query,
							variables: {
								accountId: targetAccountId,
								start: startToday
							}
						})
					});
					const data = await res.json();
					if (res.ok && !data.errors && data.data?.viewer?.accounts) {
						return { success: true };
					}
					return { success: false, error: data.errors?.[0]?.message || `HTTP ${res.status}` };
				} catch (e) {
					return { success: false, error: e.message };
				}
			})()
		]);

		const allSuccess = readResult.success && analyticsResult.success;
		let overallError = null;
		if (!allSuccess) {
			const failedPerms = [];
			if (!readResult.success) failedPerms.push(`Workers AI > Read (${readResult.error})`);
						if (!analyticsResult.success) failedPerms.push(`Account Analytics > Read (${analyticsResult.error})`);
			overallError = failedPerms.join('; ');
		}

		return new Response(JSON.stringify({
			success: allSuccess,
			error: overallError,
			permissions: {
				workersAiRead: readResult,
								accountAnalyticsRead: analyticsResult
			}
		}), { headers: { 'Content-Type': 'application/json' } });
	}

	// 8. 登录后看到的详细用量统计
	if (url.pathname === '/api/accounts/usage' && method === 'GET') {
		const accounts = await getAccounts(env);
		if (accounts.length === 0) {
			return new Response(JSON.stringify([]), { headers: { 'Content-Type': 'application/json' } });
		}

		// 刷新最老数据的20个账号
		const cacheMap = await refreshAccountsUsage(env, accounts, 20);

		// 构建完整结果列表，若没有缓存数据则标为 pending
		const results = accounts.map(account => {
			const cached = cacheMap[account.id];
			return {
				id: account.id,
				name: account.name,
				accountId: account.accountId,
				status: cached ? cached.status : 'pending',
				error: cached ? cached.error : undefined,
				usageToday: cached ? cached.usageToday : 0,
				modelsToday: cached ? cached.modelsToday : [],
				history: cached ? cached.history : [],
				lastUpdated: cached ? cached.timestamp : 0
			};
		});

		return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
	}

	// 9. 代理接口用的自定义 API 密钥管理
	if (url.pathname === '/api/keys') {
		if (method === 'GET') {
			const keys = await getApiKeys(env);
			return new Response(JSON.stringify(keys), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const body = await request.json();
			const config = await getAppConfig(env);

			// 系统默认密钥：重新生成/自定义（删不掉的保底 Key）
			if (body.system === true) {
				const newKey = (typeof body.key === 'string' && body.key.trim()) ? body.key.trim() : genApiKey();
				config.systemApiKey = newKey;
				config.systemApiKeyCreatedAt = new Date().toISOString();
				// 手动更换 = 立即切换：重置轮换计时（否则旧 systemKeyRotatedAt 若已超期，
				// 下一次 ensureSystemKey 会把刚设的密钥立刻再轮换一次），并清掉旧密钥宽限期。
				config.systemKeyRotatedAt = Date.now();
				config.systemApiKeyPrev = null;
				await saveAppConfig(env, config);
				return new Response(JSON.stringify({ success: true, key: newKey }), { headers: { 'Content-Type': 'application/json' } });
			}

			const { name, key, expiresInDays } = body;
			if (!name) {
				return new Response(JSON.stringify({ error: 'Name is required' }), { status: 400 });
			}

			const generatedKey = key || genApiKey();
			// expiresInDays: 正整数 = N 天后过期；0 / 缺省 / 非数字 = 永久有效
			const expiresAt = (typeof expiresInDays === 'number' && expiresInDays > 0)
				? Date.now() + expiresInDays * 86400000
				: null;
			const keys = config.apiKeys || [];
			keys.push({
				id: crypto.randomUUID(),
				name,
				key: generatedKey,
				createdAt: new Date().toISOString(),
				expiresAt
			});
			config.apiKeys = keys;
			await saveAppConfig(env, config);
			return new Response(JSON.stringify({ success: true, key: generatedKey }), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'DELETE') {
			const { id } = await request.json();
			// 系统默认密钥不可删除
			if (id === '__system__') {
				return new Response(JSON.stringify({ error: '系统默认密钥不可删除' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}
			const config = await getAppConfig(env);
			let keys = config.apiKeys || [];
			keys = keys.filter(k => k.id !== id);
			config.apiKeys = keys;
			await saveAppConfig(env, config);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 9b. 系统默认密钥自动轮换开关
	if (url.pathname === '/api/system-key-rotation') {
		if (method === 'GET') {
			await ensureSystemKey(env);
			const config = await getAppConfig(env);
			return new Response(JSON.stringify({
				enabled: !!config.systemKeyRotationEnabled,
				rotatedAt: config.systemKeyRotatedAt || null,
				nextRotationAt: (config.systemKeyRotationEnabled && config.systemKeyRotatedAt)
					? config.systemKeyRotatedAt + ROTATE_INTERVAL_MS
					: null
			}), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const { enabled } = await request.json();
			if (typeof enabled !== 'boolean') {
				return new Response(JSON.stringify({ error: 'enabled must be boolean' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}
			const config = await getAppConfig(env);
			config.systemKeyRotationEnabled = enabled;
			// 开启轮换时把轮换起点重置为「现在」，语义统一为「从开启之日起每 N 天轮换」。
			// 以前只判 `!config.systemKeyRotatedAt`，但 ensureSystemKey 在**首次安装**就会写入它，
			// 所以那个条件永远不成立 → 装好 7 天后再打开开关，下一次请求就立即轮换一次，
			// 与本意「避免一开启就立即轮换」相反。
			if (enabled) config.systemKeyRotatedAt = Date.now();
			await saveAppConfig(env, config);
			return new Response(JSON.stringify({ success: true, enabled }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	if (url.pathname === '/api/provider-stats' && method === 'GET') {
		// 渠道/模型级明细仅管理员可见（2026-10-06 补鉴权：此前任何知道 URL 的人都能查）
		if (!(await checkAdminAuth(request, env))) {
			return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
		const raw = url.searchParams.get('range') || 'today';
		const range = ['today', '7d', 'all'].includes(raw) ? raw : 'today';
		const incRaw = url.searchParams.get('includeInactive');
		const includeInactive = incRaw === '1' || incRaw === 'true';
		return new Response(JSON.stringify(await queryProviderStats(env, range, includeInactive)), { headers: { 'Content-Type': 'application/json' } });
	}

	// 10. 模型设置和映射
	if (url.pathname === '/api/settings') {
		if (method === 'GET') {
			const customMap = await getCustomModelMap(env);
			const config = await getAppConfig(env);
			const cfEnabled = config.cfPoolEnabled !== false;
			// 一并返回哪几条格式有问题，以及账号池是否启用（界面按去向分组时要显示生效状态）
			const invalid = findInvalidMappings(customMap, config.providers || [], cfEnabled);
			return new Response(JSON.stringify({
				customModelMap: customMap,
				invalid,
				cfPoolEnabled: cfEnabled,
				// 逐条映射开关（已停用的源名），界面据此渲染每行的小开关
				disabledMappings: Array.isArray(config.disabledMappings) ? config.disabledMappings : [],
				// 长流直通的映射名单（界面据此渲染每行的「直通」开关）
				fastPassMappings: Array.isArray(config.fastPassMappings) ? config.fastPassMappings : []
			}), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const { customModelMap } = await request.json();
			if (!customModelMap || typeof customModelMap !== 'object') {
				return new Response(JSON.stringify({ error: 'Invalid customModelMap payload' }), { status: 400 });
			}

			const config = await getAppConfig(env);
			const providers = config.providers || [];
			const cfEnabled = config.cfPoolEnabled !== false;

			// 写漏 provider: 前缀、但能按「渠道名/模型名」解析出来的，自动补全并回报，
			// 免得用户看着映射配好了、实际却路由到了别的地方
			const normalized = {};
			const autoFixed = {};
			for (const [source, target] of Object.entries(customModelMap)) {
				const text = typeof target === 'string' ? target.trim() : '';
				if (text && !text.startsWith('provider:') && !text.startsWith('@cf/')
					&& parseProviderTarget(text, providers, true)) {
					normalized[source] = 'provider:' + text;
					autoFixed[source] = normalized[source];
				} else {
					normalized[source] = target;
				}
			}

			await saveCustomModelMap(env, normalized);

			// 顺带清掉「已停用 / 直通」名单里已不存在的源名（删映射 / 改名后别留孤儿开关状态）。
			// 注意：**不动**仍然存在的映射的开关状态 —— 用户停用/勾直通的那些保存后应保持原样。
			{
				const cfg2 = await getAppConfig(env);
				const prevOff = Array.isArray(cfg2.disabledMappings) ? cfg2.disabledMappings : [];
				const prunedOff = prevOff.filter(s => Object.prototype.hasOwnProperty.call(normalized, s));
				const prevFp = Array.isArray(cfg2.fastPassMappings) ? cfg2.fastPassMappings : [];
				const prunedFp = prevFp.filter(s => Object.prototype.hasOwnProperty.call(normalized, s));
				if (prunedOff.length !== prevOff.length || prunedFp.length !== prevFp.length) {
					cfg2.disabledMappings = prunedOff;
					cfg2.fastPassMappings = prunedFp;
					await saveAppConfig(env, cfg2);
				}
			}

			// 保存本身照常成功（避免用户卡在一条错映射上连删都删不掉），
			// 但把问题显式回传，界面会立刻提示
			const problems = findInvalidMappings(normalized, providers, cfEnabled);
			const warnings = Object.entries(problems).map(([s, p]) => '「' + s + '」：' + p);
			return new Response(JSON.stringify({ success: true, autoFixed, warnings, invalid: problems }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 10a. 单条模型映射的启用 / 停用（映射表每行的小开关）。
	//      只改 config.disabledMappings —— 不删映射本身；停用后 resolveRoute 命中该源名会显式 400，
	//      且 /v1/models 不再列出（避免「列出来却调不通」）。
	if (url.pathname === '/api/mappings/status' && method === 'POST') {
		const body = await request.json().catch(() => ({}));
		const source = String(body.source || '').trim();
		if (!source) {
			return new Response(JSON.stringify({ error: 'source is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
		}
		const enabled = body.enabled !== false;
		const config = await getAppConfig(env);
		const set = new Set(Array.isArray(config.disabledMappings) ? config.disabledMappings.map(String) : []);
		if (enabled) set.delete(source); else set.add(source);
		config.disabledMappings = [...set];
		await saveAppConfig(env, config);
		return new Response(JSON.stringify({ success: true, disabledMappings: config.disabledMappings }),
			{ headers: { 'Content-Type': 'application/json' } });
	}

	// 10b. 单条映射的「长流直通」开关（2026-10-10）。映射是单线路专属通道，勾上后该源名的
	//      流式请求不逐 chunk 读取（防免费档 CPU 掐超长输出）。与渠道级 / 组级开关「或」的关系。
	if (url.pathname === '/api/mappings/fastpass' && method === 'POST') {
		const body = await request.json().catch(() => ({}));
		const source = String(body.source || '').trim();
		if (!source) {
			return new Response(JSON.stringify({ error: 'source is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
		}
		const enabled = body.enabled !== false;
		const config = await getAppConfig(env);
		const set = new Set(Array.isArray(config.fastPassMappings) ? config.fastPassMappings.map(String) : []);
		if (enabled) set.add(source); else set.delete(source);
		config.fastPassMappings = [...set];
		await saveAppConfig(env, config);
		return new Response(JSON.stringify({ success: true, fastPassMappings: config.fastPassMappings }),
			{ headers: { 'Content-Type': 'application/json' } });
	}

	// 11a. 统计看板「隐藏模型」名单（B 方案：显式隐藏，与「含已停用渠道」同一套审计逻辑）
	if (url.pathname === '/api/hidden-models') {
		if (!(await verifyAdminCookie(request, env))) {
			return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
		if (method === 'POST') {
			const { model, action } = await request.json();
			if (!model || (action !== 'add' && action !== 'remove')) {
				return new Response(JSON.stringify({ error: 'Invalid payload' }), { status: 400 });
			}
			const key = String(model).trim();
			if (!key) return new Response(JSON.stringify({ error: 'Empty model' }), { status: 400 });
			const config = await getAppConfig(env);
			const set = new Set(Array.isArray(config.hiddenModels) ? config.hiddenModels.map(String) : []);
			if (action === 'add') set.add(key);
			else set.delete(key);
			config.hiddenModels = [...set];
			await saveAppConfig(env, config);
			return new Response(JSON.stringify({ success: true, hiddenModels: config.hiddenModels }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 批量隐藏「孤儿模型」：统计里有、但已不在任何渠道 models 列表中的模型。
	// 只针对默认可见的活跃渠道（status !== 'disabled'）；已隐藏的跳过；数据不删，仅视图层过滤。
	if (url.pathname === '/api/hidden-models/cleanup') {
		if (!(await verifyAdminCookie(request, env))) {
			return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
		if (method === 'POST') {
			const config = await getAppConfig(env);
			const providers = config.providers || [];
			const hidden = new Set(Array.isArray(config.hiddenModels) ? config.hiddenModels.map(String) : []);
			const allModels = new Set();
			providers.forEach(p => (p.models || []).forEach(m => allModels.add(String(m))));
			// 仅对默认可见的活跃渠道做判定（禁用/删除渠道在默认视图本就不可见，不处理）
			const visibleProv = new Map(providers.filter(p => p.status !== 'disabled').map(p => [String(p.id), p]));
			const toHide = [];
			if (env.DB) {
				const rows = await env.DB.prepare(`SELECT DISTINCT provider_id, model FROM stats`).all();
				for (const r of (rows.results || [])) {
					const prov = visibleProv.get(String(r.provider_id));
					if (!prov) continue;
					const m = String(r.model);
					if (allModels.has(m)) continue; // 仍在某渠道 models 中 → 非孤儿
					if (hidden.has(m)) continue; // 已隐藏 → 跳过
					toHide.push(m);
				}
			}
			if (toHide.length) {
				toHide.forEach(m => hidden.add(m));
				config.hiddenModels = [...hidden];
				await saveAppConfig(env, config);
			}
			return new Response(JSON.stringify({ success: true, hidden: toHide }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 11. 运行模式：是否启用 Cloudflare 账号池 / 默认第三方渠道
	if (url.pathname === '/api/runtime') {
		if (method === 'GET') {
			const config = await getAppConfig(env);
			return new Response(JSON.stringify({
				cfPoolEnabled: config.cfPoolEnabled !== false,
				defaultProviderId: config.defaultProviderId || '',
				accounts: (config.accounts || []).length
			}), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const { cfPoolEnabled, defaultProviderId } = await request.json();
			if (cfPoolEnabled !== undefined && typeof cfPoolEnabled !== 'boolean') {
				return new Response(JSON.stringify({ error: 'cfPoolEnabled must be a boolean' }), { status: 400 });
			}
			await saveRuntimeSettings(env, {
				cfPoolEnabled,
				defaultProviderId: defaultProviderId === undefined ? undefined : String(defaultProviderId)
			});
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 12. 第三方渠道管理（OpenAI 兼容的上游 API）
	if (url.pathname === '/api/providers') {
		if (method === 'GET') {
			const providers = await getProviders(env);
			return new Response(JSON.stringify(providers), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const body = await request.json();
			const { id, name, baseUrl, apiKey, models, status, geminiNative } = body;
			// 渠道自定义单价（美元 / 百万 tokens）；undefined = 未提交、null = 清空、对象 = 设置
			const pricing = sanitizeProviderPricing(body.pricing);
			// 长流直通（2026-10-10）：流式响应不做逐 chunk 读取，防免费档 CPU 10ms 掐断超长输出。
			// 代价：流内 model 不回写、token 统计记 0（计次统计不受影响）。勾选 = true，未勾 = false。
			const fastPassFlag = body.streamFastPass === true;
			// Gemini 原生协议开关：true = 强制走原生 / false = 强制走旧兼容端点 / 其它(含未传) = 按 baseUrl 自动判断。
			// 用 undefined 表示「自动」—— saveProviders 走 JSON，undefined 的键会被丢掉，所以老配置零迁移。
			const gnFlag = geminiNative === true ? true : (geminiNative === false ? false : undefined);
			if (!name || !baseUrl) {
				return new Response(JSON.stringify({ error: 'Name and baseUrl are required' }), { status: 400 });
			}

			// 模型列表同时兼容数组和多行文本
			const modelList = Array.isArray(models)
				? models.map(s => String(s).trim()).filter(Boolean)
				: String(models || '').split(/[\n,]/).map(s => s.trim()).filter(Boolean);

		let providers = await getProviders(env);
		const oldProvider = id ? providers.find(p => p.id === id) : null;
		if (id) {
				let found = false;
				providers = providers.map(p => {
					if (p.id !== id) return p;
					found = true;
					// 编辑时密钥留空或仍是掩码，则保留原值
					const nextKey = (!apiKey || apiKey.includes('...') || apiKey === '********') ? p.apiKey : apiKey;
					// 模型列表变了就顺手清掉「已不在列表里」的健康记录。
					// 不清的话：删掉一个坏模型后，改天同名模型再加回来，会看到一条过期的红点/过期建议，
					// 让人以为新加的也是坏的（而在健康面板里直接删模型正是为了不混淆）。
					const nextHealth = p.modelHealth
						? Object.fromEntries(Object.entries(p.modelHealth).filter(([k]) => modelList.includes(k)))
						: p.modelHealth;
					return {
						...p,
						name,
						baseUrl: String(baseUrl).replace(/\/+$/, ''),
						apiKey: nextKey,
						models: modelList,
					status: status || p.status || 'active',
					geminiNative: gnFlag,
					streamFastPass: fastPassFlag,
					modelHealth: nextHealth
					};
				});
				if (!found) {
					return new Response(JSON.stringify({ error: 'Provider not found' }), { status: 404 });
				}
			} else {
				providers.push({
					id: crypto.randomUUID(),
					name,
					type: 'openai',
					baseUrl: String(baseUrl).replace(/\/+$/, ''),
					apiKey: apiKey || '',
					models: modelList,
				status: status || 'active',
				geminiNative: gnFlag,
				streamFastPass: fastPassFlag,
					createdAt: new Date().toISOString()
				});
			}

			// 渠道自定义单价：undefined = 本次未提交（保留原值）；null = 显式清空；对象 = 设置。
			if (pricing !== undefined) {
				const targetId = id || (providers[providers.length - 1] || {}).id;
				const pricingTarget = providers.find(p => p.id === targetId);
				if (pricingTarget) {
					if (pricing) pricingTarget.pricing = pricing;
					else delete pricingTarget.pricing;
				}
			}

			// 编辑渠道的模型列表时双向同步隐藏名单：
			//  · 从本渠道移除、且已不在任何渠道 models 列表中的模型 → 自动加入隐藏名单
			//  · 本次重新加回本渠道的模型（之前被自动隐藏过）→ 从隐藏名单移除，恢复可见
			let hiddenToAdd = [], hiddenToRemove = [];
			if (oldProvider) {
				const oldModels = oldProvider.models || [];
				const allNewModels = new Set();
				providers.forEach(p => (p.models || []).forEach(m => allNewModels.add(String(m))));
				hiddenToAdd = oldModels.filter(m => !modelList.includes(m))
					.filter(m => !allNewModels.has(String(m)));
				hiddenToRemove = modelList.filter(m => !oldModels.includes(m));
			}
			await saveProviders(env, providers);
			if (hiddenToAdd.length || hiddenToRemove.length) {
				await applyHiddenModelChanges(env, hiddenToAdd, hiddenToRemove);
			}
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'DELETE') {
			const { id } = await request.json();
			const config = await getAppConfig(env);
			let providers = config.providers || [];
			const target = providers.find(p => p.id === id);
			providers = providers.filter(p => p.id !== id);
			// 渠道删除后：其独有模型（不在任何剩余渠道 models 列表中）自动加入隐藏名单
			let hiddenToAdd = [];
			if (target && target.models && target.models.length) {
				const remainingModels = new Set();
				providers.forEach(p => (p.models || []).forEach(m => remainingModels.add(String(m))));
				hiddenToAdd = target.models.filter(m => !remainingModels.has(String(m)));
			}
			await saveProviders(env, providers);
			if (hiddenToAdd.length) await applyHiddenModelChanges(env, hiddenToAdd, []);

			// 删掉的正是默认渠道时，一并清空默认指向，避免留下一台指向空处的默认路由
			if (config.defaultProviderId === id) {
				await saveRuntimeSettings(env, { defaultProviderId: '' });
			}

			// 顺带清掉指向该渠道的模型映射，避免留下坏死映射
			// 映射值可能写成 provider:<渠道id>/... 也可能写成 provider:<渠道名>/...，两种都要清
			let removedMappings = 0;
			if (target) {
				const prefixes = [`provider:${target.id}/`, `provider:${target.name}/`];
				const map = await getCustomModelMap(env);
				let changed = false;
				for (const key of Object.keys(map)) {
					if (typeof map[key] === 'string' && prefixes.some(p => map[key].includes(p))) {
						delete map[key];
						changed = true;
						removedMappings++;
					}
				}
				if (changed) await saveCustomModelMap(env, map);
			}

			// 顺带清掉配额组里指向该渠道的成员，避免留下「渠道已删除」的死成员
			// （运行时 pickQuotaMember 会标 providerMissing，但界面上仍会显示为一条无效成员）
			const groups = config.quotaGroups || [];
			let removedMembers = 0;
			if (target && groups.length) {
				for (const g of groups) {
					if (!Array.isArray(g.members)) continue;
					const before = g.members.length;
					g.members = g.members.filter(m => m.providerId !== id && m.providerId !== target.name);
					removedMembers += before - g.members.length;
				}
				if (removedMembers) await saveQuotaGroups(env, groups);
			}

			return new Response(JSON.stringify({ success: true, removedMappings, removedMembers }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 12a. 渠道「启用 / 停用」轻量切换（列表卡片上的外置开关用）。
	//      只改 status 一个字段，不碰 name/baseUrl/models/geminiNative —— 避免整条渠道回写时丢字段。
	if (url.pathname === '/api/providers/status' && method === 'POST') {
		const body = await request.json().catch(() => ({}));
		const id = String(body.id || '');
		const status = body.status === 'disabled' ? 'disabled' : 'active';
		let providers = await getProviders(env);
		const idx = providers.findIndex(p => p.id === id);
		if (idx === -1) {
			return new Response(JSON.stringify({ error: 'Provider not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
		}
		providers[idx] = { ...providers[idx], status };
		await saveProviders(env, providers);
		return new Response(JSON.stringify({ success: true, status }), { headers: { 'Content-Type': 'application/json' } });
	}

	// 12. 配额组：一组候选模型 + 各自的配额上限（按次数或 token），按负载自动分摊、报错自动换成员
	if (url.pathname === '/api/quota-groups') {
		if (method === 'GET') {
			// 配置与冷却表互不依赖 → 并行读（原来串行 = 2 次往返）
			const [config, cooldowns] = await Promise.all([getAppConfig(env), getCooldowns(env)]);
			const groups = config.quotaGroups || [];
			const providers = config.providers || [];
			const nowMs = Date.now();

			// 每个组可以有自己的重置时区/时刻，窗口不同 —— 所以逐组查用量，不能合并成两次查询
			const groupsOut = [];
			for (const g of groups) {
				// 本周期用量 + 本小时用量互不依赖 → 并行（原来串行 = 2 次 D1 往返）
				const [map, hourMap] = await Promise.all([
					queryQuotaUsage(env, g, g.members || []),
					queryQuotaHourUsage(env, g.members || []),
				]);
				const win = quotaWindow(g, nowMs);
				groupsOut.push({
					...g,
					resetTz: g.resetTz || 'utc',
					resetLocalTime: g.resetLocalTime || '00:00',
					resetLabel: describeReset(g),
					nextResetAt: win.endMs,
					nextResetText: fmtBeijing(win.endMs),
					// 同一个瞬间在「组自己时区」里的写法：界面并排显示，说清两个数是同一时刻
					nextResetLocalText: fmtInOffset(win.endMs, win.offsetMin),
					sticky: g.sticky !== false,
					members: (g.members || []).map(m => {
						const p = providers.find(x => x.id === m.providerId);
						const k = JSON.stringify([String(m.providerId), String(m.model)]);
						const cd = cooldowns[cooldownKeyOf(m.providerId, m.model)];
						const cdLeft = (cd && Number(cd.until) > nowMs) ? Math.ceil((Number(cd.until) - nowMs) / 1000) : 0;
						return {
							...m,
							providerName: p ? p.name : '',
							providerMissing: !p,
							providerDisabled: !!(p && p.status === 'disabled'),
							// null 表示统计不可用（未绑 D1），界面要区分「0」和「算不出来」
							// 成员的 unit 决定取哪个口径：'token' 取 token 总量，否则取请求条数
							used: map ? quotaMeterOf(map.get(k), m.unit) : null,
							hourUsed: hourMap ? quotaMeterOf(hourMap.get(k), m.unit) : null,
							cooldownLeft: cdLeft,
							cooldownReason: cdLeft ? (cd.reason || '上游报错') : '',
							limit: Number(m.limit) || 0
						};
					})
				});
			}

			return new Response(JSON.stringify({
				groups: groupsOut,
				d1: !!env.DB,
				scheduling: config.quotaScheduling !== false,
				today: new Date().toISOString().slice(0, 10),
				nowBeijing: fmtBeijing(nowMs)
			}), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'POST') {
			const body = await request.json();
			const name = String(body.name || '').trim();
			if (!name) {
				return new Response(JSON.stringify({ error: '组名不能为空' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}
			if (name.includes('/')) {
				return new Response(JSON.stringify({ error: '组名不能包含 "/"（会和「渠道名/模型名」的调用形式混淆）' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}
			if (name.startsWith('TT:')) {
				return new Response(JSON.stringify({ error: '组名不能带 "TT:" 前缀（调用时已自动添加，直接填自定义组名即可）' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}

			// 成员净化：① limit 非空又非数字时**显式报错**（以前 `Number(x)||0` 会把它静默变成
			// 0 = 「不限次数」）；② 按 (providerId, model) 去重（重复行共享同一用量键，会让调度
			// 白跑、界面出现重复卡片）。
			// 注意：**不**拒绝指向已删渠道的成员 —— 那是本项目的既定设计：这类成员在运行时被
			// pickQuotaMember 跳过，并在配额面板上标「渠道已删除」，让用户自己决定怎么处理。
			const rawMembers = Array.isArray(body.members) ? body.members : [];
			const seenMemberKeys = new Set();
			const members = [];
			for (const m of rawMembers) {
				const providerId = String(m.providerId || '').trim();
				const model = String(m.model || '').trim();
				if (!providerId || !model) continue;   // 界面新增但未填的行：直接丢弃
				let limit = 0;
				const rawLimit = m.limit;
				if (rawLimit !== undefined && rawLimit !== null && rawLimit !== '') {
					const n = Number(rawLimit);
					if (!Number.isFinite(n) || n < 0) {
						return new Response(JSON.stringify({
							error: `成员「${model}」的配额上限不是合法数字：${JSON.stringify(rawLimit)}（留空或 0 表示不限）`
						}), { status: 400, headers: { 'Content-Type': 'application/json' } });
					}
					limit = Math.floor(n);
				}
				// 组内优先级：>=1 的整数，数字越小越先用；留空/缺省 = 1（与旧配置行为一致）。
				// 与「上限」同样处理：填了非法值就**显式报错**，不静默兜底成 1（否则用户以为改生效了）。
				let priority = 1;
				const rawPrio = m.priority;
				if (rawPrio !== undefined && rawPrio !== null && rawPrio !== '') {
					const pn = Number(rawPrio);
					if (!Number.isFinite(pn) || pn < 1) {
						return new Response(JSON.stringify({
							error: `成员「${model}」的优先级不是合法数字：${JSON.stringify(rawPrio)}（留空或 1 表示最高优先级，数字越大越靠后）`
						}), { status: 400, headers: { 'Content-Type': 'application/json' } });
					}
					priority = Math.floor(pn);
				}
				const memberKey = providerId + '\u0000' + model;
				if (seenMemberKeys.has(memberKey)) continue;   // 同渠道同模型只保留第一条
				seenMemberKeys.add(memberKey);
				// 计量单位只认白名单 'token'，其余（含缺省）一律 'count' —— 兼容旧配置语义
				const unit = m.unit === 'token' ? 'token' : 'count';
				members.push({ providerId, model, limit, unit, priority, status: m.status === 'disabled' ? 'disabled' : 'active' });
			}

			if (!members.length) {
				return new Response(JSON.stringify({ error: '至少需要一个有效成员（渠道 + 模型名）' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}

			let groups = await getQuotaGroups(env);
			if (groups.some(g => g.name === name && g.id !== body.id)) {
				return new Response(JSON.stringify({ error: `已有同名的配额组：${name}` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}

			// 组级「上游冷却时长」（秒）：留空/0 = 跟随全局默认（拥塞 20s / 限流 60s）。
			// 与「上限 / 优先级」同一套规矩：填了非法值就**显式报错**，不静默兜底 —— 否则
			// 用户以为调短了、实际还在用 20s，正好踩在这功能最要命的场景上（小组成员少）。
			let cooldownSec = 0;
			const rawCool = body.cooldownSec;
			if (rawCool !== undefined && rawCool !== null && rawCool !== '') {
				const cn = Number(rawCool);
				if (!Number.isFinite(cn) || cn < 0 || cn > 600) {
					return new Response(JSON.stringify({
						error: `上游冷却不是合法的秒数：${JSON.stringify(rawCool)}（留空或 0 表示跟随全局默认，可填 1~600）`
					}), { status: 400, headers: { 'Content-Type': 'application/json' } });
				}
				cooldownSec = Math.floor(cn);
			}

			// 重置基准：时区预设 + 该时区的本地时刻。老组没有这两个字段 → 落到 UTC 零点，
			// 行为与改造前一致（零迁移）。
			const resetTzKey = String(body.resetTz || '');
			const payload = {
				name,
				period: body.period === 'month' ? 'month' : 'day',
				status: body.status === 'disabled' ? 'disabled' : 'active',
				resetTz: Object.prototype.hasOwnProperty.call(RESET_TZ_PRESETS, resetTzKey) ? resetTzKey : 'utc',
				resetLocalTime: normalizeResetTime(body.resetLocalTime),
				resetTzOffset: Math.max(-840, Math.min(840, Math.round(Number(body.resetTzOffset) || 0))),
				// 月内重置日（仅 period=month 有意义）：缺省 1；非数字回落到 1，数字按 1-31 截断
				resetDay: Math.max(1, Math.min(31, Math.floor(Number(body.resetDay)) || 1)),
				// 会话粘性：同一会话尽量固定同一个成员（多轮/工具调用更稳）；默认开（未设置即视为开）
				sticky: body.sticky !== false,
				// 长流直通（2026-10-10）：组内成员走 OpenAI 透传时，流式响应不逐 chunk 读取，
				// 防免费档 CPU 10ms 掐断超长输出。代价：token 统计记 0（计次/冷却/选号不受影响）。
				streamFastPass: body.streamFastPass === true,
				// 组级上游冷却（秒）：0 = 跟随全局默认。老组没有这个字段 → 0，行为与改造前一致（零迁移）
				cooldownSec,
				members
			};

			if (body.id) {
				let found = false;
				groups = groups.map(g => {
					if (g.id !== body.id) return g;
					found = true;
					return { ...g, ...payload };
				});
				if (!found) {
					return new Response(JSON.stringify({ error: '配额组不存在' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
				}
			} else {
				groups.push({ id: crypto.randomUUID(), ...payload, createdAt: new Date().toISOString() });
			}

			await saveQuotaGroups(env, groups);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'DELETE') {
			const { id } = await request.json();
			let groups = await getQuotaGroups(env);
			groups = groups.filter(g => g.id !== id);
			await saveQuotaGroups(env, groups);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 12c. 配额组「启用 / 停用」轻量切换（组卡片头部的外置开关用）。
	//      只改该组的 status；停用后 TT:<组名> 与指向它的映射都会显式报「组未启用」，不再参与选号。
	if (url.pathname === '/api/quota-groups/status' && method === 'POST') {
		const body = await request.json().catch(() => ({}));
		const id = String(body.id || '');
		const status = body.status === 'disabled' ? 'disabled' : 'active';
		let groups = await getQuotaGroups(env);
		const idx = groups.findIndex(g => g.id === id);
		if (idx === -1) {
			return new Response(JSON.stringify({ error: '配额组不存在' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
		}
		groups[idx] = { ...groups[idx], status };
		await saveQuotaGroups(env, groups);
		return new Response(JSON.stringify({ success: true, status }), { headers: { 'Content-Type': 'application/json' } });
	}

	// 12b. 配额调度总开关（负载均衡 / 成员冷却 / 换成员重试）+ 手动解除冷却
	if (url.pathname === '/api/quota-scheduling') {
		if (method === 'GET') {
			const config = await getAppConfig(env);
			const cds = await getCooldowns(env);
			const now = Date.now();
			const list = Object.keys(cds)
				.filter(k => Number(cds[k] && cds[k].until) > now)
				.map(k => ({
					key: k,
					left: Math.ceil((Number(cds[k].until) - now) / 1000),
					reason: String(cds[k].reason || '')
				}));
			return new Response(JSON.stringify({
				enabled: config.quotaScheduling !== false,
				cooldowns: list
			}), { headers: { 'Content-Type': 'application/json' } });
		}
		if (method === 'POST') {
			const body = await request.json().catch(() => ({}));
			if (body.clearCooldowns === true) {
				cooldownCache = { at: Date.now(), map: {} };
				// 管理员手动清空：把落盘节流状态也一并复位，否则接下来几分钟的冷却可能被节流挡住不落盘
				cooldownWrite.pausedUntil = 0;
				cooldownWrite.winStart = 0;
				cooldownWrite.winFails = 0;
				cooldownWrite.lastAt = 0;
				cooldownWrite.unpersisted.clear();
				if (env.KV) { try { await env.KV.delete(COOLDOWN_KV_KEY); } catch (e) { /* 忽略 */ } }
			}
			if (body.enabled !== undefined) await saveQuotaScheduling(env, body.enabled !== false);
			const config = await getAppConfig(env);
			return new Response(JSON.stringify({ success: true, enabled: config.quotaScheduling !== false }),
				{ headers: { 'Content-Type': 'application/json' } });
		}
	}

	// 13. 第三方渠道连通性 / 模型可用性测试
	// 传 model 测单个；传 models 数组则逐个测（并发受限），结果写回渠道的 modelHealth 供界面展示
	if (url.pathname === '/api/providers/test' && method === 'POST') {
		let { id, baseUrl, apiKey, model, models } = await request.json();
		let targetBaseUrl = baseUrl;
		let targetKey = apiKey;
		let provider = null;

		if (id) {
			const config = await getAppConfig(env);
			provider = (config.providers || []).find(x => x.id === id);
			if (provider) {
				if (!targetBaseUrl) targetBaseUrl = provider.baseUrl;
				if (!targetKey || targetKey.includes('...') || targetKey === '********') targetKey = provider.apiKey;
			}
		}

		if (!targetBaseUrl) {
			return new Response(JSON.stringify({ success: false, error: 'Base URL 不能为空' }), { status: 400 });
		}

		// 待测模型：显式传入的数组 > 单个 model > 渠道已配置的模型列表
		let list = (Array.isArray(models) && models.length)
			? models.map(m => String(m).trim()).filter(Boolean)
			: (model ? [String(model).trim()] : ((provider && provider.models) || []).slice());
		if (!list.length) {
			return new Response(JSON.stringify({ success: false, error: '请先填写至少一个模型名，测试需要指定模型' }), { status: 400 });
		}

		// 单次最多测 12 个，避免一次打太多上游请求
		const MAX_MODELS = 12;
		const truncated = list.length > MAX_MODELS;
		list = list.slice(0, MAX_MODELS);

		// 并发限 4：思考型模型单个可能十几秒，全并发容易被上游限流
		const CONCURRENCY = 4;
		// 测试要跟真实调用同路：Gemini 渠道走原生（isGeminiProvider 判定），其余走 OpenAI 兼容端点
		const probeTarget = provider
			? { ...provider, baseUrl: targetBaseUrl, apiKey: targetKey }
			: { baseUrl: targetBaseUrl, apiKey: targetKey, name: '未保存的渠道' };
		const results = [];
		for (let i = 0; i < list.length; i += CONCURRENCY) {
			const batch = list.slice(i, i + CONCURRENCY);
			const settled = await Promise.all(batch.map(m => probeProviderModel(targetBaseUrl, targetKey, m, undefined, probeTarget)));
			results.push(...settled);
		}

		// 探测调用也要计数：它真实消耗上游额度（Gemini 这类按请求数算免费额度尤其明显），
		// 而且必须计入配额 —— 否则会出现「测试把额度用掉了，配额却还显示有剩余」的假象。
		// 记在 probe_* 列里，转发统计（req/ok/fail）不受影响。
		const probeProvider = provider || {
			id: 'adhoc:' + String(targetBaseUrl).replace(/\/+$/, ''),
			name: '未保存的渠道'
		};
		results.forEach(r => {
			// 超时探测不计延迟（.48）：25 秒超时是「没拿到响应」，计入平均会把渠道延迟拉爆；
			// 失败次数照记（probe_fail），非超时失败（如 429）的耗时保留——那是真实链路耗时。
			recordProviderCall(env, ctx, probeProvider, r.model, r.ok, r.timedOut ? 0 : r.elapsed, true);
		});

		// 测的是「渠道已保存的配置」时才回写健康状态，避免把临时改动当成正式结果记下来
		const isStoredConfig = provider
			&& (!baseUrl || baseUrl === provider.baseUrl)
			&& (!apiKey || apiKey === provider.apiKey);
		if (isStoredConfig) {
			const at = new Date().toISOString();
			const health = Object.assign({}, provider.modelHealth || {});
			results.forEach(r => {
				health[r.model] = {
					ok: r.ok,
					status: r.status || 0,
					error: r.ok ? '' : r.error,
					upstreamMessage: r.ok ? '' : (r.upstreamMessage || ''),
					suggestedModel: r.ok ? '' : (r.suggestedModel || ''),
					elapsed: r.elapsed,
					at
				};
			});
			provider.modelHealth = health;
			const config = await getAppConfig(env);
			await saveProviders(env, (config.providers || []).map(p => p.id === provider.id ? provider : p));
		}

		const nativeMode = isGeminiProvider(probeTarget);
		return new Response(JSON.stringify({
			success: results.every(r => r.ok),
			endpoint: nativeMode
				? geminiNativeBase(targetBaseUrl) + '/models/{model}:generateContent'
				: String(targetBaseUrl).replace(/\/+$/, '') + '/chat/completions',
			native: nativeMode,
			truncated,
			results
		}), { headers: { 'Content-Type': 'application/json' } });
	}

	// 13. 从上游拉取可用模型列表（省得手抄模型名，也避免抄到已过期的名字）
	if (url.pathname === '/api/providers/models' && method === 'GET') {
		const id = url.searchParams.get('id');
		const providers = await getProviders(env);
		const provider = providers.find(p => p.id === id);
		if (!provider) {
			return new Response(JSON.stringify({ success: false, error: 'Provider not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
		}

		// 5 分钟进程内缓存：规格元信息不常变，避免反复打开弹窗就反复打上游（?refresh=1 强制刷新）
		const modelsCacheKey = 'models:' + provider.id;
		if (url.searchParams.get('refresh') !== '1') {
			const hit = modelInfoCache.get(modelsCacheKey);
			if (hit && Date.now() - hit.at < MODEL_INFO_CACHE_MS) {
				return new Response(JSON.stringify(Object.assign({ cached: true }, hit.data)), { headers: { 'Content-Type': 'application/json' } });
			}
		}

		// 复用统一实现（与 /v1/models 的后台预热同一份代码，避免两处各写一遍）
		const result = await buildModelSpecPayload(provider);
		if (!result.ok) {
			const body = result.transport
				? {
					success: false,
					endpoint: result.endpoint,
					error: result.error,
					hint: '并非所有上游都提供 /models 接口，遇到这种就只能手动填模型名。'
				}
				: {
					success: false,
					endpoint: result.endpoint,
					error: `HTTP ${result.status}: ${result.errText.slice(0, 300)}`,
					upstreamMessage: extractUpstreamMessage(result.errText),
					hint: '并非所有上游都提供 /models 接口，遇到这种就只能手动填模型名。'
				};
			return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } });
		}

		modelInfoCache.set(modelsCacheKey, { at: Date.now(), data: result.payload });
		return new Response(JSON.stringify(result.payload), { headers: { 'Content-Type': 'application/json' } });
	}

	return new Response(JSON.stringify({ error: 'Endpoint not found' }), { status: 404 });
}

// ----------------------------------------------------
// 前端页面处理函数（按页面拆分）
// ----------------------------------------------------

// 共享 UI 片段：三页内联前端抽出的公共常量，避免逐页重复、并保证主题一致。
// ⚠️ error/access 是刻意独立的「红色主题」，值与其它页不同，不在共享范围内，切勿并入。
const COMMON_CSS_RESET = `
		* {
			box-sizing: border-box;
			margin: 0;
			padding: 0;
		}
`;

// landing 与 admin 逐值核对后完全一致的暗色 / 亮色主题变量（2026-10-06 抽离）
const COMMON_THEME_VARS = `
			--bg-color: #0b0f19;
			--card-bg: rgba(30, 41, 59, 0.45);
			--border-color: rgba(255, 255, 255, 0.08);
			--text-main: #f8fafc;
			--text-muted: #94a3b8;
			--primary-gradient: linear-gradient(135deg, #6366f1 0%, #a855f7 50%, #ec4899 100%);
			--accent-color: #a855f7;
			--input-bg: rgba(15, 23, 42, 0.6);
			--input-border: rgba(255, 255, 255, 0.1);
			--input-text: #f8fafc;
			--btn-secondary-bg: rgba(255, 255, 255, 0.06);
			--btn-secondary-hover: rgba(255, 255, 255, 0.12);
			--btn-secondary-text: #f8fafc;
			--modal-overlay-bg: rgba(8, 10, 18, 0.6);
			--glass-blur: 20px;
			--card-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
`;

const COMMON_THEME_VARS_LIGHT = `
			--bg-color: #f1f5f9;
			--card-bg: rgba(255, 255, 255, 0.7);
			--border-color: rgba(0, 0, 0, 0.06);
			--text-main: #0f172a;
			--text-muted: #64748b;
			--primary-gradient: linear-gradient(135deg, #4f46e5 0%, #9333ea 50%, #db2777 100%);
			--accent-color: #9333ea;
			--input-bg: rgba(241, 245, 249, 0.8);
			--input-border: rgba(0, 0, 0, 0.08);
			--input-text: #0f172a;
			--btn-secondary-bg: rgba(0, 0, 0, 0.04);
			--btn-secondary-hover: rgba(0, 0, 0, 0.08);
			--btn-secondary-text: #0f172a;
			--modal-overlay-bg: rgba(241, 245, 249, 0.5);
			--glass-blur: 20px;
			--card-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.07);
`;

// 三页完全相同的 Toast 助手（landing / admin 此前各定义一份，已逐字核对一致）
const COMMON_TOAST_JS = `
		function showToast(message, type = 'success') {
			let container = document.querySelector('.toast-container');
			if (!container) {
				container = document.createElement('div');
				container.className = 'toast-container';
				document.body.appendChild(container);
			}

			const toast = document.createElement('div');
			toast.className = \`toast toast-\${type}\`;
			
			let iconSvg = '';
			if (type === 'success') {
				iconSvg = \`<svg class="toast-icon" style="color: #ffffff;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>\`;
			} else if (type === 'error') {
				iconSvg = \`<svg class="toast-icon" style="color: #ffffff;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>\`;
			} else {
				iconSvg = \`<svg class="toast-icon" style="color: #ffffff;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>\`;
			}

			toast.innerHTML = \`\${iconSvg}<span>\${message}</span>\`;
			container.appendChild(toast);

			toast.offsetHeight; // trigger reflow
			toast.classList.add('show');

			setTimeout(() => {
				toast.classList.remove('show');
				setTimeout(() => toast.remove(), 400);
			}, 3000);
		}
`;

// 1. 首页 / 登录页
// 页面响应的缓存头：private（仅本人/管理员可见）+ no-cache（每次用 If-None-Match 校验）+ ETag。
// ★ 不用 max-age：页面内容含「登录态 + 运行模式(CF开关) + BUILD_ID」，必须每次校验；
//   校验命中即回 304，省掉整页重下（2026-10-08：面板页 ~205KB，是「加载慢」的主因）。
function htmlCacheHeaders(etag) {
	return {
		'Content-Type': 'text/html; charset=utf-8',
		'ETag': etag,
		'Cache-Control': 'private, no-cache',
	};
}

// If-None-Match 命中判定（浏览器可能带多个候选，按逗号拆开逐个比）
function etagMatches(request, etag) {
	const inm = request.headers.get('If-None-Match');
	if (!inm) return false;
	return inm.split(',').some(s => s.trim() === etag || s.trim() === '*');
}

async function handleLandingPage(request, env, ctx) {
	// ⚠️ 模板里（悬浮按钮 / 登录弹窗）引用了 isLoggedIn，删了这里就会 Error 1101（2026-10-05 踩过）
	const isLoggedIn = await verifyAdminCookie(request, env);
	// 配了 ADMIN_USERNAME 时登录框多一个「用户名」；未配置则只显示密码框
	const requireUsername = adminCredentials(env).requireUsername;
	// 账号池关闭时首页不显示 CF 用量看板（纯第三方反代模式下那些数字恒为 0）
	const cfEnabled = await getCfPoolEnabled(env);

	// 内容指纹：随 构建号 / 登录态 / 用户名模式 / 运行模式 变
	const pageEtag = '"lp-' + BUILD_ID + '-' + (isLoggedIn ? 'in' : 'out') + '-' + (requireUsername ? 'u' : 'nu') + '-' + (cfEnabled ? 'cf' : 'ncf') + '"';
	if (etagMatches(request, pageEtag)) return new Response(null, { status: 304, headers: htmlCacheHeaders(pageEtag) });

	const html = `<!DOCTYPE html>
<head>
	<meta charset="UTF-8">
	<meta name="robots" content="noindex, nofollow">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Workers API Hub</title>
	${FAVICON_LINK}
	<link rel="preconnect" href="https://fonts.googleapis.com">
	<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
	<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
	<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
	<style>
		:root {
			${COMMON_THEME_VARS}
			--orb-1-color: rgba(99, 102, 241, 0.15);
			--orb-2-color: rgba(236, 72, 153, 0.12);
		}

		:root[data-theme="light"] {
			${COMMON_THEME_VARS_LIGHT}
			--orb-1-color: rgba(99, 102, 241, 0.08);
			--orb-2-color: rgba(236, 72, 153, 0.06);
		}

		${COMMON_CSS_RESET}

		body {
			font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
			background-color: var(--bg-color);
			color: var(--text-main);
			min-height: 100vh;
			display: flex;
			flex-direction: column;
			justify-content: center;
			align-items: center;
			padding: 20px;
			overflow-x: hidden;
			position: relative;
		}

		h1, h2, h3 {
			font-family: 'Outfit', sans-serif;
		}

		/* Utility Hidden Class */
		.hidden {
			display: none !important;
		}

		/* Dynamic Background Orbs */
		.bg-orbs-container {
			position: fixed;
			top: 0;
			left: 0;
			width: 100%;
			height: 100%;
			z-index: -1;
			overflow: hidden;
			pointer-events: none;
		}

		.bg-orb {
			position: absolute;
			border-radius: 50%;
			filter: blur(100px);
			animation: float 25s infinite alternate ease-in-out;
		}

		.bg-orb-1 {
			top: -10%;
			left: -10%;
			width: 50vw;
			height: 50vw;
			background: var(--orb-1-color);
			animation-duration: 20s;
		}

		.bg-orb-2 {
			bottom: -10%;
			right: -10%;
			width: 60vw;
			height: 60vw;
			background: var(--orb-2-color);
			animation-duration: 30s;
			animation-delay: -5s;
		}

		@keyframes float {
			0% {
				transform: translate(0, 0) scale(1);
			}
			50% {
				transform: translate(5%, 10%) scale(1.1);
			}
			100% {
				transform: translate(-5%, -5%) scale(0.9);
			}
		}

		.action-btn-group {
			position: fixed;
			top: 20px;
			right: 20px;
			display: flex;
			flex-direction: row;
			gap: 12px;
			z-index: 1000;
		}

		.floating-btn {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			color: var(--text-main);
			width: 44px;
			height: 44px;
			border-radius: 12px;
			display: flex;
			align-items: center;
			justify-content: center;
			cursor: pointer;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
			z-index: 1000;
			outline: none;
		}
		
		.floating-btn:hover {
			transform: translateY(-2px);
			border-color: rgba(168, 85, 247, 0.4);
			box-shadow: 0 8px 20px rgba(168, 85, 247, 0.15);
		}

	.dashboard-container {
		max-width: 1000px;
			width: 100%;
			display: flex;
			flex-direction: column;
			gap: 28px;
			z-index: 10;
		}

		.dashboard-grid {
			display: grid;
			grid-template-columns: 1fr 1.6fr 1.3fr;
			gap: 20px;
			width: 100%;
		}

		/* CF 池关闭时只显示第三方卡：单列全宽 */
		.dashboard-grid.provider-only {
			grid-template-columns: 1fr;
		}

		/* 第三方汇总卡：内容靠上（与 CF 卡标题位置一致；JS 用 style.display='' 恢复显示，样式须放类里） */
		#public-provider-card {
			display: flex;
			flex-direction: column;
			justify-content: flex-start;
		}

		.public-chart-wrapper {
			position: relative;
			height: 150px;
			width: 100%;
			display: flex;
			align-items: center;
			justify-content: center;
			gap: 16px;
			overflow: hidden;
		}

		.public-chart-wrapper canvas {
			max-width: 100% !important;
		}

		@media (max-width: 768px) {
			.dashboard-grid {
				grid-template-columns: 1fr !important;
			}
			.public-chart-wrapper {
				flex-direction: column !important;
				height: auto !important;
				padding: 10px 0;
				gap: 20px !important;
			}
			.public-chart-wrapper > div:first-child {
				width: 160px !important;
				height: 160px !important;
			}
			.public-chart-wrapper > div:nth-child(2) {
				width: 100% !important;
				height: auto !important;
				align-items: center !important;
			}
			#public-chart-legend {
				width: 100%;
				display: grid !important;
				grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
				gap: 8px !important;
				max-height: none !important;
			}
		}

		@keyframes fadeInUp {
			from {
				opacity: 0;
				transform: translateY(24px);
			}
			to {
				opacity: 1;
				transform: translateY(0);
			}
		}

		.animate-fade-in-up {
			opacity: 0;
			animation: fadeInUp 0.8s cubic-bezier(0.16, 1, 0.3, 1) forwards;
		}

		.delay-1 {
			animation-delay: 0.15s;
		}

		.delay-2 {
			animation-delay: 0.3s;
		}

		@keyframes spin {
			to { transform: rotate(360deg); }
		}

		.spinner {
			display: inline-block;
			width: 16px;
			height: 16px;
			border: 2px solid rgba(168, 85, 247, 0.2);
			border-radius: 50%;
			border-top-color: var(--accent-color);
			animation: spin 1s linear infinite;
		}

		#public-chart-legend {
			scrollbar-width: none;
			-ms-overflow-style: none;
		}
		#public-chart-legend::-webkit-scrollbar {
			display: none;
		}

		.login-header {
			display: flex;
			flex-direction: row;
			align-items: center;
			justify-content: center;
			gap: 16px;
			margin-bottom: 8px;
		}

		.logo-icon {
			width: 46px;
			height: 46px;
			border-radius: 12px;
			background: var(--primary-gradient);
			display: flex;
			align-items: center;
			justify-content: center;
			font-weight: bold;
			color: white;
			font-size: 22px;
			font-family: 'Outfit', sans-serif;
			box-shadow: 0 4px 14px rgba(168, 85, 247, 0.25);
		}

		.logo-text {
			font-size: 24px;
			font-weight: 700;
			letter-spacing: -0.5px;
			background: var(--primary-gradient);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
		}

		.stat-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 18px;
			padding: 26px;
			display: flex;
			flex-direction: column;
			gap: 14px;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1), box-shadow 0.3s, border-color 0.3s;
			min-width: 0;
			overflow: hidden;
		}

		.stat-card:hover {
			transform: translateY(-4px);
			border-color: rgba(168, 85, 247, 0.3);
			box-shadow: 0 12px 30px rgba(168, 85, 247, 0.1);
		}

		.stat-title {
			font-size: 14px;
			color: var(--text-muted);
			font-weight: 500;
		}

		.stat-value {
			font-size: 36px;
			font-weight: 700;
			font-family: 'Outfit', sans-serif;
			overflow-wrap: anywhere;
			word-break: break-word;
		}

		.progress-container {
			width: 100%;
			height: 8px;
			background-color: rgba(255, 255, 255, 0.06);
			border-radius: 4px;
			overflow: hidden;
			position: relative;
		}

		:root[data-theme="light"] .progress-container {
			background-color: rgba(0, 0, 0, 0.05);
		}

		.progress-bar {
			height: 100%;
			background: var(--primary-gradient);
			border-radius: 4px;
			width: 0%;
			transition: width 1.2s cubic-bezier(0.34, 1.56, 0.64, 1);
			position: relative;
			overflow: hidden;
		}

		.progress-bar::after {
			content: '';
			position: absolute;
			top: 0;
			left: 0;
			right: 0;
			bottom: 0;
			background: linear-gradient(
				90deg,
				rgba(255, 255, 255, 0) 0%,
				rgba(255, 255, 255, 0.2) 50%,
				rgba(255, 255, 255, 0) 100%
			);
			animation: progress-shimmer 2s infinite linear;
			background-size: 200% 100%;
		}

		@keyframes progress-shimmer {
			0% { background-position: -200% 0; }
			100% { background-position: 200% 0; }
		}

		.section-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 18px;
			padding: 28px;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			display: flex;
			flex-direction: column;
			gap: 20px;
		}

		.section-title {
			font-size: 18px;
			font-weight: 600;
			display: flex;
			align-items: center;
			gap: 8px;
		}

		.form-group {
			display: flex;
			flex-direction: column;
			gap: 8px;
		}

		.form-group label {
			font-size: 13px;
			font-weight: 500;
			color: var(--text-muted);
		}

		input {
			background-color: var(--input-bg);
			border: 1px solid var(--input-border);
			color: var(--input-text);
			padding: 12px 16px;
			border-radius: 10px;
			outline: none;
			font-size: 14px;
			transition: all 0.3s ease;
			backdrop-filter: blur(10px);
		}

		input:focus {
			border-color: var(--accent-color);
			box-shadow: 0 0 0 3px rgba(168, 85, 247, 0.2);
			background-color: rgba(15, 23, 42, 0.8);
		}

		:root[data-theme="light"] input:focus {
			background-color: rgba(255, 255, 255, 0.95);
		}

		.btn {
			display: inline-flex;
			align-items: center;
			justify-content: center;
			padding: 12px 20px;
			border-radius: 10px;
			font-weight: 600;
			font-size: 14px;
			cursor: pointer;
			border: none;
			transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
			text-decoration: none;
		}

		.btn-primary {
			background-color: rgba(139, 124, 246, 0.16);
			color: #c4b5fd;
			border: 1px solid rgba(139, 124, 246, 0.35);
		}

		.btn-primary:hover {
			background-color: rgba(139, 124, 246, 0.26);
		}

		.btn-primary:active {
			transform: translateY(1px);
		}

		:root[data-theme="light"] .btn-primary {
			background-color: #eeedfe;
			color: #534ab7;
			border-color: #cecbf6;
		}

		:root[data-theme="light"] .btn-primary:hover {
			background-color: #e2dffb;
		}

		.btn-secondary {
			background-color: var(--btn-secondary-bg);
			color: var(--btn-secondary-text);
			border: 1px solid var(--border-color);
		}

		.btn-secondary:hover {
			background-color: var(--btn-secondary-hover);
			transform: translateY(-1px);
		}

		/* Modal Styling */
		.modal-overlay {
			position: fixed;
			top: 0;
			left: 0;
			right: 0;
			bottom: 0;
			background-color: var(--modal-overlay-bg);
			backdrop-filter: blur(0px);
			-webkit-backdrop-filter: blur(0px);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 1000;
			opacity: 0;
			pointer-events: none;
			transition: opacity 0.3s ease, backdrop-filter 0.3s ease, -webkit-backdrop-filter 0.3s ease;
		}

		.modal-overlay.active {
			opacity: 1;
			pointer-events: auto;
			backdrop-filter: blur(8px);
			-webkit-backdrop-filter: blur(8px);
		}

		.modal-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 20px;
			width: 100%;
			max-width: 400px;
			max-height: calc(100vh - 48px);
			overflow-y: auto;
			padding: 32px;
			box-shadow: 0 20px 50px rgba(0, 0, 0, 0.4);
			display: flex;
			flex-direction: column;
			gap: 20px;
			transform: scale(0.9) translateY(20px);
			transition: transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), background-color 0.3s;
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
		}

		.modal-overlay.active .modal-card {
			transform: scale(1) translateY(0);
		}

		.modal-header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			border-bottom: 1px solid var(--border-color);
			padding-bottom: 16px;
		}

		.modal-header h3 {
			font-size: 18px;
			font-weight: 600;
		}

		.close-btn {
			background: none;
			border: none;
			color: var(--text-muted);
			cursor: pointer;
			padding: 4px;
			display: flex;
			align-items: center;
			justify-content: center;
			outline: none;
			transition: color 0.2s;
		}

		.close-btn:hover {
			color: var(--text-main);
		}

		/* Toast Notification */
		.toast-container {
			position: fixed;
			top: 24px;
			right: 24px;
			display: flex;
			flex-direction: column;
			gap: 10px;
			z-index: 9999;
			pointer-events: none;
		}

		.toast {
			min-width: 260px;
			padding: 14px 20px;
			border-radius: 12px;
			box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
			font-size: 14px;
			font-weight: 600;
			display: flex;
			align-items: center;
			gap: 12px;
			backdrop-filter: blur(15px);
			-webkit-backdrop-filter: blur(15px);
			transform: translateY(-20px);
			opacity: 0;
			transition: all 0.4s cubic-bezier(0.16, 1, 0.3, 1);
			pointer-events: auto;
		}

		.toast.show {
			transform: translateY(0);
			opacity: 1;
		}

		.toast-icon {
			width: 20px;
			height: 20px;
			flex-shrink: 0;
		}

		.toast-success {
			background-color: #10b981 !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-success .toast-icon, .toast-success span {
			color: #ffffff !important;
		}

		.toast-error {
			background-color: #ef4444 !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-error .toast-icon, .toast-error span {
			color: #ffffff !important;
		}
		
		.toast-warning {
			background-color: #f59e0b !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-warning .toast-icon, .toast-warning span {
			color: #ffffff !important;
		}
	</style>
</head>
<body>

	<!-- Dynamic Background Orbs -->
	<div class="bg-orbs-container">
		<div class="bg-orb bg-orb-1"></div>
		<div class="bg-orb bg-orb-2"></div>
	</div>

	<!-- Floating Action Buttons -->
	<div class="action-btn-group">
		<button class="floating-btn" onclick="toggleTheme()" title="切换日间/夜间模式">
			<svg class="theme-icon-sun" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none; width: 20px; height: 20px;">
				<circle cx="12" cy="12" r="4" />
				<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
			</svg>
			<svg class="theme-icon-moon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width: 20px; height: 20px;">
				<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z" />
			</svg>
		</button>
		<button class="floating-btn" onclick="openLoginModal()" title="${isLoggedIn ? '管理后台' : '管理员登录'}" style="background: var(--primary-gradient); color: white; border: none;">
			${isLoggedIn ? `
				<!-- User Check Icon -->
				<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width: 20px; height: 20px;">
					<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
					<circle cx="9" cy="7" r="4" />
					<polyline points="16 11 18 13 22 9" />
				</svg>
			` : `
				<!-- User Icon -->
				<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width: 20px; height: 20px;">
					<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
					<circle cx="12" cy="7" r="4" />
				</svg>
			`}
		</button>
	</div>

	<div class="dashboard-container">
		<div class="login-header animate-fade-in-up" style="margin-bottom: 24px;">
			<div class="logo-icon">AI</div>
			<span class="logo-text">Workers API Hub</span>
		</div>

		<div class="dashboard-grid${cfEnabled ? '' : ' provider-only'}">
			<!-- Public stats widget（CF 池卡：池关闭时隐藏） -->
			<div class="stat-card animate-fade-in-up delay-1" style="${cfEnabled ? '' : 'display: none;'}justify-content: space-between;">
				<div>
					<div class="stat-title" style="margin-bottom: 10px;">今日用量汇总</div>
					<div style="display: flex; align-items: baseline; gap: 4px;">
						<div class="stat-value" id="public-neurons" style="font-size: 30px; background: var(--primary-gradient); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 800; display: inline-block;">0</div>
						<span style="font-size: 14px; color: var(--text-muted); font-weight: 500; font-family: 'Outfit', sans-serif;">Neurons</span>
					</div>
				</div>
				
				<div style="margin-top: 16px;">
					<div class="progress-container">
						<div class="progress-bar" id="public-progress" style="width: 0%;"></div>
					</div>
					<div style="display: flex; justify-content: space-between; font-size: 11px; color: var(--text-muted); margin-top: 8px;">
						<span id="public-limit-desc">总限额: 0 Neurons</span>
						<span id="public-percent-desc" style="font-weight: 600; color: var(--accent-color);">0.00%</span>
					</div>
				</div>
			</div>
			<!-- Public model chart widget -->
			<div class="stat-card animate-fade-in-up delay-2" id="public-models-card" style="padding: 20px; display: ${cfEnabled ? 'flex' : 'none'}; flex-direction: column; justify-content: center;">
				<!-- Chart and custom legend container -->
				<div class="public-chart-wrapper" id="public-chart-wrapper" style="display: none; height: 150px; width: 100%; flex-direction: row; align-items: center; justify-content: space-between; gap: 16px;">
					<!-- Left column: Chart (fixed 140x140, fits the narrow 3-col card) -->
					<div style="position: relative; height: 140px; width: 140px; flex-shrink: 0; display: flex; align-items: center; justify-content: center;">
						<canvas id="publicModelsChart"></canvas>
					</div>
					<!-- Right column: Legend list -->
					<div style="flex: 1; display: flex; flex-direction: column; justify-content: center; min-width: 0; align-self: stretch; height: 140px;">
						<div id="public-chart-legend" style="flex: 1; display: flex; flex-direction: column; gap: 6px; min-width: 0; max-height: 130px; overflow-y: auto; padding-right: 4px;"></div>
					</div>
				</div>
				
				<!-- Loading / Empty Placeholder -->
				<div id="public-chart-placeholder" style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 190px; width: 100%; color: var(--text-muted); font-size: 13px; gap: 12px;">
					<span class="spinner" style="width: 24px; height: 24px; border-width: 2.5px;"></span>
					<span>正在载入数据...</span>
				</div>
			</div>
			<!-- Public provider stats widget：第三方渠道今日汇总（总量+成功率，不点名渠道；不受 CF 池开关影响） -->
			<div class="stat-card animate-fade-in-up delay-3" id="public-provider-card" style="padding: 20px; display: none;">
			<div class="stat-title" style="margin-bottom: 12px;">第三方渠道 · 今日</div>
			<div style="flex: 1; display: flex; align-items: center; justify-content: center;">
				<div style="display: flex; align-items: baseline; justify-content: center; gap: 20px; flex-wrap: wrap;">
					<div style="display: flex; align-items: baseline; gap: 4px;">
						<div class="stat-value" id="public-provider-tokens" style="font-size: 24px; background: var(--primary-gradient); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 800; display: inline-block;">0</div>
						<span style="font-size: 12px; color: var(--text-muted); font-weight: 500;">Token</span>
					</div>
					<div style="display: flex; align-items: baseline; gap: 4px;">
						<div class="stat-value" id="public-provider-rate" style="font-size: 24px; font-weight: 800; color: var(--accent-color); display: inline-block;">—</div>
						<span style="font-size: 12px; color: var(--text-muted); font-weight: 500;">成功率</span>
					</div>
				</div>
			</div>
		</div>
		</div>
	</div>

	<!-- 弹窗：管理员登录 / 后台快捷入口 -->
	<div class="modal-overlay" id="login-modal">
		<div class="modal-card">
			<div class="modal-header">
				<h3 id="modal-title">${isLoggedIn ? '管理面板入口' : '管理员登录'}</h3>
				<button onclick="closeLoginModal()" class="close-btn">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
						<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
					</svg>
				</button>
			</div>
			
			${isLoggedIn ? `
				<div style="text-align: center; display: flex; flex-direction: column; gap: 16px; margin-top: 10px;">
					<div style="font-size: 40px; margin-bottom: 8px;">🎉</div>
					<p style="font-size: 14px; color: var(--text-muted); line-height: 1.5;">您当前已登录管理员身份。</p>
					<a href="/admin" class="btn btn-primary" style="width: 100%; text-decoration: none; display: flex; align-items: center; justify-content: center; height: 42px;">进入后台管理面板</a>
					<button class="btn btn-secondary" onclick="submitLogout()" style="width: 100%; height: 42px;">安全退出</button>
				</div>
			` : `
				${requireUsername ? `
				<div class="form-group" style="margin-top: 10px;">
					<label for="login-username">用户名</label>
					<input type="text" id="login-username" placeholder="请输入用户名" autocomplete="username" onkeydown="if(event.key==='Enter')submitLogin()">
				</div>` : ''}
				<div class="form-group" style="margin-top: 10px;">
					<label for="login-password">管理员密码</label>
					<input type="password" id="login-password" placeholder="请输入管理员密码" autocomplete="current-password" onkeydown="if(event.key==='Enter')submitLogin()">
				</div>
				<div class="modal-footer" style="margin-top: 10px; display: flex; gap: 12px; justify-content: flex-end; width: 100%;">
					<button class="btn btn-secondary" onclick="closeLoginModal()" style="height: 38px;">取消</button>
					<button class="btn btn-primary" onclick="submitLogin()" style="height: 38px;">登录</button>
				</div>
			`}
		</div>
	</div>

	<script>
		// Toast Helper
		${COMMON_TOAST_JS}

		function initTheme() {
			const savedTheme = localStorage.getItem('theme');
			if (savedTheme) {
				document.documentElement.setAttribute('data-theme', savedTheme);
			} else {
				const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
				const defaultTheme = systemPrefersDark ? 'dark' : 'light';
				document.documentElement.setAttribute('data-theme', defaultTheme);
			}
			updateThemeIcons();
		}

		let publicModelsChartInstance = null;
		let lastPublicSummaryData = null;

		function toggleTheme() {
			const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
			const newTheme = currentTheme === 'light' ? 'dark' : 'light';
			document.documentElement.setAttribute('data-theme', newTheme);
			localStorage.setItem('theme', newTheme);
			updateThemeIcons();
			if (lastPublicSummaryData) {
				renderPublicSummary(lastPublicSummaryData);
			}
		}

		function updateThemeIcons() {
			const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
			const sunIcons = document.querySelectorAll('.theme-icon-sun');
			const moonIcons = document.querySelectorAll('.theme-icon-moon');
			if (currentTheme === 'light') {
				sunIcons.forEach(el => el.style.display = 'none');
				moonIcons.forEach(el => el.style.display = 'block');
			} else {
				sunIcons.forEach(el => el.style.display = 'block');
				moonIcons.forEach(el => el.style.display = 'none');
			}
		}

		function openLoginModal() {
			document.getElementById('login-modal').classList.add('active');
			const usrInput = document.getElementById('login-username');
			const pwdInput = document.getElementById('login-password');
			if (usrInput) usrInput.value = '';
			if (pwdInput) pwdInput.value = '';
			const first = usrInput || pwdInput;
			if (first) setTimeout(() => first.focus(), 100);
		}

		function closeLoginModal() {
			document.getElementById('login-modal').classList.remove('active');
		}

		initTheme();

		window.onload = function() {
			${cfEnabled ? 'loadPublicSummary();' : ''}
			loadPublicProviderStats();
		};

		async function loadPublicSummary() {
			try {
				const res = await fetch('/api/usage/summary');
				const data = await res.json();
				
				renderPublicSummary(data);

				// 匿名访客不再触发 POST 刷新：该接口已改为必须管理员鉴权（P0 修复 2026-10-05），
				// 匿名调用只会拿到 401 —— 旧代码在这里白跑一次请求且静默失败。
				// CF 用量的刷新交给已登录的管理面板。
			} catch (e) {
				console.error(e);
			}
		}

		async function loadPublicProviderStats() {
			try {
				const res = await fetch('/api/public/provider-stats');
				const data = await res.json();
				renderPublicProviderStats(data);
			} catch (e) {
				console.error(e);
			}
		}

		function renderPublicProviderStats(data) {
			const card = document.getElementById('public-provider-card');
			if (!card) return;
			if (!data || data.enabled !== true) { card.style.display = 'none'; return; }
			card.style.display = '';
			const rateEl = document.getElementById('public-provider-rate');
			const tokEl = document.getElementById('public-provider-tokens');
			if (rateEl) rateEl.innerText = data.total ? (data.rate + '%') : '—';
			if (tokEl) tokEl.innerText = Number(data.tokens || 0).toLocaleString();
		}

		function animateNumber(id, end, duration = 1200) {
			const obj = document.getElementById(id);
			if (!obj) return;
			let start = parseInt(obj.innerText.replace(/,/g, ''), 10);
			if (isNaN(start) || start <= 0) {
				start = end > 100 ? 100 : 0;
			}
			const range = end - start;
			if (range === 0) {
				obj.innerText = end.toLocaleString();
				return;
			}
			const startTime = performance.now();
			function update(currentTime) {
				const elapsed = currentTime - startTime;
				const progress = Math.min(elapsed / duration, 1);
				const easeProgress = 1 - Math.pow(2, -10 * progress);
				const current = Math.ceil(start + range * easeProgress);
				obj.innerText = current.toLocaleString();
				if (progress < 1) {
					requestAnimationFrame(update);
				} else {
					obj.innerText = end.toLocaleString();
				}
			}
			requestAnimationFrame(update);
		}

		function renderPublicSummary(data) {
			lastPublicSummaryData = data;
			const percent = Number(data.usagePercentage).toFixed(2);
			const roundedNeurons = Math.ceil(data.totalNeuronsToday);
			
			// 触发数字滚动的动效
			animateNumber('public-neurons', roundedNeurons, 1000);
			
			document.getElementById('public-progress').style.width = percent + '%';
			document.getElementById('public-limit-desc').innerText = '总限额: ' + Number(data.totalLimit).toLocaleString() + ' Neurons';
			document.getElementById('public-percent-desc').innerText = percent + '%';

			const wrapper = document.getElementById('public-chart-wrapper');
			const placeholder = document.getElementById('public-chart-placeholder');
			const legendContainer = document.getElementById('public-chart-legend');

			if (data.modelsToday && data.modelsToday.length > 0) {
				if (wrapper) wrapper.style.display = 'flex';
				if (placeholder) placeholder.style.display = 'none';

				// 按 Neurons 消耗数从大到小排序
				const sortedModelsToday = [...data.modelsToday].sort((a, b) => b.neurons - a.neurons);

				const labels = sortedModelsToday.map(m => m.model.split('/').pop());
				const chartData = sortedModelsToday.map(m => m.neurons);
				
				const isLight = document.documentElement.getAttribute('data-theme') === 'light';
				const textColor = isLight ? '#64748b' : '#94a3b8';
				const borderColor = isLight ? '#ffffff' : '#1e293b';
				
				const ctx = document.getElementById('publicModelsChart').getContext('2d');
				if (publicModelsChartInstance) {
					publicModelsChartInstance.destroy();
				}
				
				// 清空旧的 HTML Legend 标签
				if (legendContainer) legendContainer.innerHTML = '';

				publicModelsChartInstance = new Chart(ctx, {
					type: 'doughnut',
					data: {
						labels: labels,
						datasets: [{
							data: chartData,
							backgroundColor: ['#6366f1', '#a855f7', '#ec4899', '#10b981', '#f59e0b', '#3b82f6'],
							borderWidth: 2,
							borderColor: borderColor
						}]
					},
					options: {
						responsive: true,
						maintainAspectRatio: false,
						cutout: '70%',
						animation: {
							animateRotate: true,
							animateScale: true,
							duration: 1000,
							easing: 'easeOutQuart'
						},
						plugins: {
							legend: {
								display: false // 关闭原生图例，使用 HTML 图例
							}
						}
					}
				});

				// 动态且逐个淡入渲染模型说明 ID
				if (legendContainer) {
					const colors = ['#6366f1', '#a855f7', '#ec4899', '#10b981', '#f59e0b', '#3b82f6'];
					const total = chartData.reduce((a, b) => a + b, 0);
					
					labels.forEach((label, index) => {
						const val = chartData[index];
						const color = colors[index % colors.length];
						const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
						
						const item = document.createElement('div');
						item.style.display = 'flex';
						item.style.alignItems = 'center';
						item.style.gap = '8px';
						item.style.fontSize = '12px';
						item.style.color = textColor;
						item.style.opacity = '0';
						item.style.transform = 'translateX(10px)';
						item.style.transition = 'all 0.4s cubic-bezier(0.16, 1, 0.3, 1)';
						
						item.innerHTML = '<span style="width: 8px; height: 8px; border-radius: 50%; background-color: ' + color + '; flex-shrink: 0; margin-right: 2px;"></span>' +
							'<span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1; font-weight: 500;" title="' + label + '">' + label + '</span>' +
							'<span style="color: var(--text-muted); font-family: monospace; font-size: 11px; flex-shrink: 0; margin-left: 4px;">' + pct + '%</span>';
						
						legendContainer.appendChild(item);
						
						// 与环形图同时开始加载，依次淡入滑出
						setTimeout(() => {
							item.style.opacity = '1';
							item.style.transform = 'translateX(0)';
						}, index * 80);
					});
				}
			} else {
				if (wrapper) wrapper.style.display = 'none';
				if (placeholder) {
					placeholder.style.display = 'flex';
					placeholder.innerHTML = '<svg style="width: 32px; height: 32px; opacity: 0.5;" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
						'<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"></path>' +
						'<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"></path>' +
						'</svg><span>今日暂无消耗数据</span>';
				}
				if (publicModelsChartInstance) {
					publicModelsChartInstance.destroy();
					publicModelsChartInstance = null;
				}
			}
		}

		async function submitLogin() {
			const pwdEl = document.getElementById('login-password');
			const usrEl = document.getElementById('login-username');
			const password = pwdEl ? pwdEl.value : '';
			const username = usrEl ? usrEl.value : '';
			const res = await fetch('/api/auth/login', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ username, password })
			});
			if (res.ok) {
				showToast('登录成功！跳转中...');
				setTimeout(() => {
					window.location.href = '/admin';
				}, 600);
			} else {
				const data = await res.json();
				showToast('登录失败: ' + (data.error || '密码不正确！'), 'error');
			}
		}

		async function submitLogout() {
			const res = await fetch('/api/auth/logout', { method: 'POST' });
			if (res.ok) {
				showToast('安全退出成功');
				setTimeout(() => {
					window.location.reload();
				}, 600);
			}
		}
	</script>
	<footer style="text-align: center; padding: 120px 0 20px; font-size: 12px; color: var(--text-muted); opacity: 0.6; z-index: 10;">
		<a href="https://github.com/ldg118/workers-api-hub" target="_blank" rel="noopener noreferrer" style="color: inherit; text-decoration: none; border-bottom: 1px solid currentColor;">GitHub</a> · Workers API Hub · build ${BUILD_ID}
	</footer>
</body>
</html>`;

	return new Response(html, { headers: htmlCacheHeaders(pageEtag) });
}

// 2. 后台管理控制台页面
async function handleAdminPage(request, env, ctx) {
	// 运行模式决定渲染哪些界面：账号池关闭时（纯第三方反代）走「接入信息」作为落地页
	const cfEnabled = await getCfPoolEnabled(env);
	const defaultTab = cfEnabled ? 'overview' : 'access';

	// 内容指纹：随 构建号 / 运行模式 变（面板是登录后才可见的壳，数据全走 /api/*）
	const pageEtag = '"ad-' + BUILD_ID + '-' + (cfEnabled ? 'cf' : 'ncf') + '"';
	if (etagMatches(request, pageEtag)) return new Response(null, { status: 304, headers: htmlCacheHeaders(pageEtag) });

	// 配额组「重置基准」的时区**预设项**（点输入框右侧箭头展开，在下方平铺）。
	// 演变过程（2026-10-08，都是被实测逼出来的）：
	//   ① 原生 <datalist> —— 面板由浏览器渲染，**宽度不受 CSS 控制**，做不出「与输入框对齐」；
	//   ② 自建下拉「下方不够就向上弹」—— 结果盖住了上面的字段，看着乱；
	//   ③ 现在这版 —— 菜单**只向下展开**（内容区可滚动，展开后自动滚到可见），绝不遮挡。
	// · data-value 用可读 label —— 用户看到和打字的都是「北京时间 UTC+8」这种文本；
	// · data-key 供前端把文本反查回预设键；data-offset 供实时换算读；
	// · custom 不列 —— 自定义偏移改用「直接输入数字」表达（如 480 / -420）。
	const resetTzMenu = Object.keys(RESET_TZ_PRESETS).filter(function (k) { return k !== 'custom'; }).map(function (k) {
		const p = RESET_TZ_PRESETS[k];
		return '<button type="button" class="quota-combo-item" data-value="' + p.label + '" data-key="' + k + '" data-offset="' + (p.offset === null ? '' : p.offset) + '">' + p.label + '</button>';
	}).join('');

	const html = `<!DOCTYPE html>
<head>
	<meta charset="UTF-8">
	<meta name="robots" content="noindex, nofollow">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Workers API Hub Dashboard</title>
	${FAVICON_LINK}
	<link rel="preconnect" href="https://fonts.googleapis.com">
	<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
	<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
	<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
	<style>
		:root {
			${COMMON_THEME_VARS}
			--sidebar-bg: rgba(15, 23, 42, 0.6);
			--success-color: #10b981;
			--warning-color: #f59e0b;
			--danger-color: #ef4444;
			--sidebar-width: 260px;
			--sidebar-menu-hover: rgba(255, 255, 255, 0.04);
			--table-header-bg: rgba(0, 0, 0, 0.2);
			--section-item-bg: rgba(255, 255, 255, 0.02);
			--orb-1-color: rgba(99, 102, 241, 0.12);
			--orb-2-color: rgba(236, 72, 153, 0.08);
			/* Theme aware code tag tokens */
			--code-bg: rgba(0, 0, 0, 0.25);
			--code-color: #e9d5ff;
			--code-border: rgba(255, 255, 255, 0.04);
		}

		:root[data-theme="light"] {
			${COMMON_THEME_VARS_LIGHT}
			--sidebar-bg: rgba(255, 255, 255, 0.6);
			--success-color: #10b981;
			--warning-color: #f59e0b;
			--danger-color: #ef4444;
			--sidebar-menu-hover: rgba(0, 0, 0, 0.04);
			--table-header-bg: rgba(0, 0, 0, 0.03);
			--section-item-bg: rgba(0, 0, 0, 0.01);
			--orb-1-color: rgba(99, 102, 241, 0.06);
			--orb-2-color: rgba(236, 72, 153, 0.04);
			/* Theme aware code tag tokens */
			--code-bg: rgba(79, 70, 229, 0.07);
			--code-color: #4f46e5;
			--code-border: rgba(79, 70, 229, 0.15);
		}

		${COMMON_CSS_RESET}

		body {
			font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
			background-color: var(--bg-color);
			color: var(--text-main);
			min-height: 100vh;
			display: flex;
			flex-direction: column;
			overflow-x: hidden;
			position: relative;
		}

		/* Utility Hidden Class */
		.hidden {
			display: none !important;
		}

		/* Tab Content Transition — Cyberpunk Terminal Reveal */
		.tab-content {
			display: none;
			position: relative;
		}
		.tab-content.active {
			display: block;
			animation: cyberReveal 0.55s cubic-bezier(0.22, 1, 0.36, 1) both;
		}
		/* Scanline sweep overlay */
		.tab-content.active::before {
			content: '';
			position: absolute;
			top: 0;
			left: 0;
			right: 0;
			height: 2px;
			background: linear-gradient(90deg,
				transparent 0%,
				rgba(168, 85, 247, 0.1) 15%,
				rgba(168, 85, 247, 0.65) 40%,
				rgba(236, 72, 153, 0.85) 50%,
				rgba(168, 85, 247, 0.65) 60%,
				rgba(168, 85, 247, 0.1) 85%,
				transparent 100%
			);
			z-index: 100;
			pointer-events: none;
			animation: scanlineDrop 0.45s cubic-bezier(0.16, 1, 0.3, 1) forwards;
			box-shadow:
				0 0 15px rgba(168, 85, 247, 0.45),
				0 0 40px rgba(99, 102, 241, 0.2);
		}
		@keyframes scanlineDrop {
			0%   { top: 0; opacity: 0; }
			8%   { opacity: 1; }
			92%  { opacity: 1; }
			100% { top: 100%; opacity: 0; }
		}
		@keyframes cyberReveal {
			0% {
				opacity: 0;
				transform: translateY(10px) scale(0.97);
				filter: brightness(2.5) blur(1px);
			}
			18% {
				opacity: 0.35;
				filter: brightness(1.5) blur(0.3px);
			}
			35% {
				opacity: 0.7;
				transform: translateY(3px) scale(0.99);
				filter: brightness(1.15) blur(0);
			}
			60% {
				opacity: 0.9;
				transform: translateY(1px) scale(1);
				filter: brightness(1.03);
			}
			100% {
				opacity: 1;
				transform: translateY(0) scale(1);
				filter: brightness(1);
			}
		}

		/* Nav item — flowing gradient border + neon glow */
		.nav-item::after {
			content: '';
			position: absolute;
			left: 0;
			right: 0;
			bottom: 0;
			height: 2px;
			background: linear-gradient(90deg,
				#6366f1,
				#a855f7 20%,
				#ec4899 50%,
				#a855f7 80%,
				#6366f1
			);
			background-size: 200% 100%;
			transform: scaleX(0);
			transform-origin: center;
			transition: transform 0.4s cubic-bezier(0.22, 1, 0.36, 1);
		}
		.nav-item.active::after {
			transform: scaleX(1);
			animation: borderFlow 3s linear infinite;
		}
		@keyframes borderFlow {
			0%   { background-position: 200% 0; }
			100% { background-position: 0% 0; }
		}
		:root[data-theme="light"] .nav-item::after {
			background: linear-gradient(90deg,
				#4f46e5,
				#7c3aed 20%,
				#db2777 50%,
				#7c3aed 80%,
				#4f46e5
			);
			background-size: 200% 100%;
		}

		/* Dynamic Background Orbs */
		.bg-orbs-container {
			position: fixed;
			top: 0;
			left: 0;
			width: 100%;
			height: 100%;
			z-index: -1;
			overflow: hidden;
			pointer-events: none;
		}

		.bg-orb {
			position: absolute;
			border-radius: 50%;
			filter: blur(100px);
			animation: float 25s infinite alternate ease-in-out;
		}

		.bg-orb-1 {
			top: -10%;
			left: -10%;
			width: 50vw;
			height: 50vw;
			background: var(--orb-1-color);
			animation-duration: 20s;
		}

		.bg-orb-2 {
			bottom: -10%;
			right: -10%;
			width: 60vw;
			height: 60vw;
			background: var(--orb-2-color);
			animation-duration: 30s;
			animation-delay: -5s;
		}

		@keyframes float {
			0% { transform: translate(0, 0) scale(1); }
			100% { transform: translate(5%, 5%) scale(1.05); }
		}

		/* Sidebar Layout */
		.app-container {
			display: flex;
			min-height: 100vh;
			position: relative;
		}

		aside {
			width: var(--sidebar-width);
			background-color: var(--sidebar-bg);
			border-right: 1px solid var(--border-color);
			display: flex;
			flex-direction: column;
			padding: 30px 20px;
			position: fixed;
			top: 0;
			bottom: 0;
			left: 0;
			z-index: 100;
			/* 高度锁在视口内，滚动交给内部的 .nav-menu，避免内容多了溢出看不到底部 */
			overflow: hidden;
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1);
		}

		.logo-area {
			display: flex;
			align-items: center;
			gap: 12px;
			margin-bottom: 18px;
			padding-left: 8px;
			flex-shrink: 0;
		}

		.logo-icon {
			width: 38px;
			height: 38px;
			border-radius: 10px;
			background: var(--primary-gradient);
			display: flex;
			align-items: center;
			justify-content: center;
			font-weight: bold;
			color: white;
			font-size: 18px;
			font-family: 'Outfit', sans-serif;
			box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
		}

		.logo-text {
			font-size: 18px;
			font-weight: 700;
			font-family: 'Outfit', sans-serif;
			letter-spacing: -0.5px;
			background: var(--primary-gradient);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
		}

		.nav-menu {
			display: flex;
			flex-direction: column;
			gap: 8px;
			flex: 1 1 auto;
			/* min-height: 0 是关键：flex 子项默认 min-height:auto，不设成 0 的话
			   导航条目变多时（账号池启用会多出「概览」组）会把侧边栏整体撑高，
			   底部状态条被挤出视口、且滚不到 */
			min-height: 0;
			overflow-y: auto;
			scrollbar-width: thin;
			padding-right: 4px;
		}

		.nav-item {
			display: flex;
			align-items: center;
			gap: 12px;
			padding: 12px 16px;
			border-radius: 10px;
			cursor: pointer;
			font-size: 14px;
			font-weight: 500;
			color: var(--text-muted);
			transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
			position: relative;
			overflow: hidden;
		}

		.nav-item:hover {
			color: var(--text-main);
			background-color: var(--sidebar-menu-hover);
			transform: translateX(4px);
		}

		.nav-item.active {
			color: white;
			background: var(--primary-gradient);
			box-shadow:
				0 0 18px rgba(168, 85, 247, 0.35),
				0 0 40px rgba(99, 102, 241, 0.15),
				inset 0 1px 0 rgba(255, 255, 255, 0.1);
		}

		/* 侧边栏分组标题 */
		.nav-group-title {
			font-size: 11px;
			font-weight: 500;
			color: var(--text-muted);
			opacity: 0.75;
			padding: 16px 16px 6px;
			letter-spacing: 0.02em;
		}

		/* 运行模式：常驻侧边栏顶部（站点标题下方），紧凑一行，点击打开弹窗看详情 */
		.runtime-status {
			display: flex;
			align-items: center;
			gap: 8px;
			background-color: var(--section-item-bg);
			border: 1px solid var(--border-color);
			border-radius: 10px;
			padding: 9px 12px;
			cursor: pointer;
			transition: border-color 0.2s ease;
			margin-bottom: 22px;
			min-width: 0;
		}

		.runtime-status:hover {
			border-color: var(--accent-color);
		}

		.runtime-status-dot {
			width: 7px;
			height: 7px;
			border-radius: 50%;
			flex: none;
			background-color: var(--text-muted);
		}

		.runtime-status-value {
			font-size: 12px;
			color: var(--text-main);
			line-height: 1.5;
			flex: 1;
			min-width: 0;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}

		/* 账号池关闭时（纯第三方反代模式）隐藏 Cloudflare 相关界面。
		   用 CSS 而不是服务端不渲染，是为了让开关保存后能即时生效、不必整页刷新 */
		body.cf-off .cf-only {
			display: none !important;
		}

		#cf-off-warning {
			display: none;
		}

		body.cf-off #cf-off-warning {
			display: block !important;
		}

		.aside-footer {
			display: flex;
			flex-direction: column;
			gap: 12px;
			border-top: 1px solid var(--border-color);
			padding-top: 20px;
			/* 常驻底部：不随导航区滚动，也不被压缩 */
			flex-shrink: 0;
		}

		/* Main Content Area */
		main {
			flex: 1;
			margin-left: var(--sidebar-width);
			padding: 24px 40px 40px;
			min-width: 0;
			z-index: 10;
		}

		/* Card Grid & Stats */
		.card-grid {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
			gap: 20px;
		}

		.stat-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 18px;
			padding: 26px;
			display: flex;
			flex-direction: column;
			gap: 14px;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1), box-shadow 0.3s, border-color 0.3s;
			min-width: 0;
		}

		.stat-card:hover {
			transform: translateY(-4px);
			border-color: rgba(168, 85, 247, 0.3);
			box-shadow: 0 12px 30px rgba(168, 85, 247, 0.1);
		}

		.stat-title {
			font-size: 14px;
			color: var(--text-muted);
			font-weight: 500;
		}

		.stat-value {
			font-size: 32px;
			font-weight: 700;
			font-family: 'Outfit', sans-serif;
			overflow-wrap: anywhere;
			word-break: break-word;
		}

		.stat-desc {
			font-size: 12px;
			color: var(--text-muted);
		}

		.progress-container {
			width: 100%;
			height: 6px;
			background-color: rgba(255, 255, 255, 0.06);
			border-radius: 3px;
			overflow: hidden;
		}

		:root[data-theme="light"] .progress-container {
			background-color: rgba(0, 0, 0, 0.05);
		}

		.progress-bar {
			height: 100%;
			background: var(--primary-gradient);
			width: 0%;
			transition: width 1.2s cubic-bezier(0.34, 1.56, 0.64, 1);
		}

		/* Section Cards */
		.section-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 18px;
			padding: 30px;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			margin-bottom: 24px;
		}

		.section-note {
			margin-top: 6px;
			font-size: 13px;
			color: var(--text-muted);
			line-height: 1.5;
		}

		.access-endpoint-grid {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
			gap: 14px;
		}

		.access-endpoint-card {
			appearance: none;
			width: 100%;
			text-align: left;
			border: 1px solid var(--border-color);
			border-radius: 16px;
			padding: 18px 20px;
			background: linear-gradient(180deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.015));
			box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
			backdrop-filter: blur(14px);
			-webkit-backdrop-filter: blur(14px);
			transition: transform 0.25s cubic-bezier(0.16, 1, 0.3, 1), border-color 0.25s, box-shadow 0.25s, background 0.25s;
			cursor: default;
			color: inherit;
		}

		.access-endpoint-card:hover {
			transform: translateY(-2px);
			border-color: rgba(168, 85, 247, 0.28);
			box-shadow: 0 12px 30px rgba(168, 85, 247, 0.08);
		}

		.endpoint-badge {
			display: inline-flex;
			align-items: center;
			padding: 4px 10px;
			border-radius: 999px;
			background: rgba(168, 85, 247, 0.12);
			color: var(--accent-color);
			font-size: 12px;
			font-weight: 600;
			letter-spacing: 0.01em;
		}

		.endpoint-url {
			display: block;
			margin-top: 12px;
			font-size: 14px;
			line-height: 1.55;
			word-break: break-all;
			color: var(--text-main);
			text-decoration: underline;
			text-decoration-color: rgba(168, 85, 247, 0.5);
			text-underline-offset: 3px;
			cursor: pointer;
			background: transparent;
			border: none;
			padding: 0;
		}

		.endpoint-url:hover {
			color: var(--accent-color);
		}

		.section-header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			margin-bottom: 20px;
		}

		.section-title {
			font-size: 18px;
			font-weight: 600;
			font-family: 'Outfit', sans-serif;
			display: flex;
			align-items: center;
			gap: 8px;
		}

		/* Forms & Inputs */
		.form-group {
			display: flex;
			flex-direction: column;
			gap: 8px;
			margin-bottom: 16px;
		}

		.form-group label {
			font-size: 13px;
			font-weight: 500;
			color: var(--text-muted);
		}

		input {
			background-color: var(--input-bg);
			border: 1px solid var(--input-border);
			color: var(--input-text);
			padding: 12px 16px;
			border-radius: 10px;
			outline: none;
			font-size: 14px;
			transition: all 0.3s ease;
			backdrop-filter: blur(10px);
		}

		input:focus {
			border-color: var(--accent-color);
			box-shadow: 0 0 0 3px rgba(168, 85, 247, 0.2);
			background-color: rgba(15, 23, 42, 0.8);
		}

		:root[data-theme="light"] input:focus {
			background-color: rgba(255, 255, 255, 0.95);
		}

		/* 配额组的「预设下拉」——输入框右侧一个独立箭头，点开在下方平铺预设。
		   刻意**只向下展开**：向上弹会盖住相邻字段（2026-10-08 用户实测否掉）。
		   也刻意不用原生 <datalist>：它的面板宽度不受 CSS 控制，做不出与输入框对齐。 */
		.quota-combo {
			position: relative;
		}
		.quota-combo-row {
			display: flex;
			gap: 8px;
		}
		.quota-combo-row input {
			flex: 1 1 auto;
			min-width: 0;
		}
		.quota-combo-toggle {
			flex: none;
			width: 42px;
			border: 1px solid var(--input-border);
			border-radius: 10px;
			background-color: var(--input-bg);
			color: var(--text-muted);
			font-size: 12px;
			cursor: pointer;
		}
		.quota-combo-toggle:hover {
			border-color: var(--accent-color);
			color: var(--text-main);
		}
		.quota-combo-menu {
			display: none;
			position: absolute;
			left: 0;
			right: 0;              /* 覆盖整行（输入框 + 箭头），左右与字段对齐 */
			top: calc(100% + 4px); /* 只向下；不做向上翻转 */
			max-height: 200px;
			overflow-y: auto;
			/* 滚到面板顶/底后别再「穿透」给弹窗 —— 否则看着像在滑整个弹窗（2026-10-09） */
			overscroll-behavior: contain;
			padding: 4px;
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 10px;
			box-shadow: var(--card-shadow);
			z-index: 40;
			scrollbar-width: thin;
		}
		.quota-combo.open .quota-combo-menu {
			display: block;
		}
		.quota-combo-item {
			display: block;
			width: 100%;
			text-align: left;
			padding: 9px 12px;
			border: none;
			border-radius: 8px;
			background-color: transparent;
			color: var(--text-main);
			font-size: 13px;
			font-family: inherit;
			cursor: pointer;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}
		.quota-combo-item:hover {
			background-color: var(--section-item-bg);
		}
		/* 当前值对应的那项高亮 —— 免得"点开发现第一项就是现在这个"让人困惑 */
		.quota-combo-item.on {
			color: var(--accent-color);
		}

		/* 模型映射「目标模型」的自定义候选下拉。原生 datalist 的弹出面板不受 CSS 控制
		   （字体小、宽度窄、长模型名截断，2026-10-09 用户要求变大），改自建；
		   教训同配额「重置基准」：只向下展开、绝不向上翻。 */
		.map-combo {
			position: relative;
		}
		.map-combo-menu {
			display: none;
			position: absolute;
			left: 0;
			top: calc(100% + 4px);   /* 只向下；不做向上翻转 */
			width: max-content;      /* 面板随内容加宽，长模型名不再截断 */
			min-width: 100%;         /* 但至少与输入框对齐 */
			max-width: min(520px, calc(100vw - 64px));
			max-height: 320px;
			overflow-y: auto;
			/* 同配额预设面板：滚到头不再把滚动「穿透」给 .modal-body / 页面（2026-10-09） */
			overscroll-behavior: contain;
			padding: 4px;
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 10px;
			box-shadow: var(--card-shadow);
			z-index: 40;
			scrollbar-width: thin;
		}
		.map-combo.open .map-combo-menu {
			display: block;
		}
		.map-combo-item {
			display: block;
			width: 100%;
			text-align: left;
			padding: 10px 14px;
			border: none;
			border-radius: 8px;
			background-color: transparent;
			color: var(--text-main);
			font-size: 14px;
			font-family: inherit;
			cursor: pointer;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}
		.map-combo-item:hover {
			background-color: var(--section-item-bg);
		}
		/* 配额弹窗的成员行也用同款下拉；卡片内容窄（约 436px），面板加宽上限收窄，免得顶出横向滚动 */
		.quota-member-row .map-combo-menu {
			max-width: min(320px, calc(100vw - 64px));
		}

		/* ---- 成员行拖拽排序（2026-10-10，方案 C）----
		   拖拽只是「快速改优先级」的手势：拖完按新顺序回写每行的 priority 数字，
		   运行时语义不变（仍是「优先级定层 + 层内负载均衡」）。 */
		.quota-drag-handle {
			cursor: grab;
			user-select: none;
			-webkit-user-select: none;
			color: var(--text-muted);
			font-size: 15px;
			line-height: 1;
			text-align: center;
			letter-spacing: 1px;
		}
		.quota-drag-handle:active { cursor: grabbing; }
		.quota-member-row.dragging { opacity: .45; }
		/* 落点指示线：拖过某行上半 / 下半分别插到它前面 / 后面 */
		.quota-member-row.drop-above { box-shadow: 0 -2px 0 0 var(--accent-color); }
		.quota-member-row.drop-below { box-shadow: 0 2px 0 0 var(--accent-color); }

		/* Buttons */
		.btn {
			display: inline-flex;
			align-items: center;
			justify-content: center;
			padding: 10px 18px;
			border-radius: 10px;
			font-weight: 600;
			font-size: 14px;
			cursor: pointer;
			border: none;
			transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
			gap: 8px;
		}

		.btn-primary {
			background-color: rgba(139, 124, 246, 0.16);
			color: #c4b5fd;
			border: 1px solid rgba(139, 124, 246, 0.35);
		}

		.btn-primary:hover {
			background-color: rgba(139, 124, 246, 0.26);
		}

		:root[data-theme="light"] .btn-primary {
			background-color: #eeedfe;
			color: #534ab7;
			border-color: #cecbf6;
		}

		:root[data-theme="light"] .btn-primary:hover {
			background-color: #e2dffb;
		}

		.btn-danger {
			background-color: rgba(239, 68, 68, 0.12);
			color: #fca5a5;
			border: 1px solid rgba(239, 68, 68, 0.3);
		}

		.btn-danger:hover {
			background-color: rgba(239, 68, 68, 0.2);
		}

		:root[data-theme="light"] .btn-danger {
			background-color: #fcebeb;
			color: #a32d2d;
			border-color: #f7c1c1;
		}

		:root[data-theme="light"] .btn-danger:hover {
			background-color: #f9dcdc;
		}

		.btn-secondary {
			background-color: var(--btn-secondary-bg);
			color: var(--btn-secondary-text);
			border: 1px solid var(--border-color);
		}

		.btn-secondary:hover {
			background-color: var(--btn-secondary-hover);
			transform: translateY(-1px);
		}

		/* 统计栏「显示已隐藏」勾选框：复用 .btn 以获得与其他按钮一致的 hover/active 反馈 */
		.stats-toggle {
			font-weight: 600;
			cursor: pointer;
			user-select: none;
		}
		.stats-toggle.is-checked {
			border-color: var(--primary-color);
			color: var(--primary-color);
		}
		.stats-toggle input {
			margin: 0;
			cursor: pointer;
			accent-color: var(--primary-color);
		}

		.btn-success {
			background-color: var(--success-color);
			color: white;
			box-shadow: 0 4px 14px rgba(16, 185, 129, 0.3);
		}

		.btn-success:hover {
			background-color: #059669;
			transform: translateY(-2px);
			box-shadow: 0 6px 20px rgba(16, 185, 129, 0.5);
			opacity: 0.95;
		}

		.btn:disabled {
			opacity: 0.6;
			cursor: not-allowed;
			transform: none !important;
			box-shadow: none !important;
		}

		@keyframes spinner-border {
			to { transform: rotate(360deg); }
		}

		.spinner {
			display: inline-block;
			width: 12px;
			height: 12px;
			vertical-align: text-bottom;
			border: 2px solid currentColor;
			border-right-color: transparent;
			border-radius: 50%;
			animation: spinner-border .75s linear infinite;
		}

		@keyframes flash-green {
			0% {
				border-color: rgba(16, 185, 129, 0.8);
				box-shadow: 0 0 20px rgba(16, 185, 129, 0.35);
			}
			100% {
				border-color: var(--border-color);
				box-shadow: var(--card-shadow);
			}
		}

		.card-update-flash {
			animation: flash-green 2s cubic-bezier(0.25, 1, 0.5, 1);
		}

		/* Tables */
		table {
			width: 100%;
			border-collapse: collapse;
			text-align: left;
			font-size: 14px;
		}

		th {
			background-color: var(--table-header-bg);
			font-weight: 600;
			color: var(--text-muted);
			padding: 16px 20px;
			border-bottom: 1px solid var(--border-color);
		}

		td {
			padding: 16px 20px;
			border-bottom: 1px solid var(--border-color);
			color: var(--text-main);
		}

		tr:hover td {
			background-color: rgba(255, 255, 255, 0.01);
		}

		/* 映射表：按去向分组的标题行 */
		tr.mapping-group td {
			background-color: var(--section-item-bg, rgba(128, 128, 128, 0.06));
			font-size: 12.5px;
			font-weight: 600;
			color: var(--text-muted);
			padding: 10px 20px;
			cursor: pointer;
			user-select: none;
		}
		tr.mapping-group:hover td {
			background-color: var(--section-item-bg, rgba(128, 128, 128, 0.06));
		}
		tr.mapping-group .group-arrow {
			display: inline-block;
			width: 16px;
			font-size: 11px;
		}
		tr.mapping-group .badge {
			margin-left: 8px;
		}

		/* 说明区组件：把"一整段话"拆成"导语 + 对照卡 + 小字备注" */
		.hint-card {
			background-color: var(--section-item-bg);
			border: 1px solid var(--border-color);
			border-radius: 12px;
			padding: 12px 16px;
			margin-top: 14px;
		}
		/* 说明区标题行：整卡可点击折叠。下面有长表格的页面默认收起，腾出空间 */
		.hint-head {
			display: flex;
			align-items: center;
			gap: 8px;
			font-size: 12px;
			color: var(--text-muted);
			cursor: pointer;
			user-select: none;
		}
		.hint-head:hover {
			color: var(--text-main);
		}
		.hint-arrow {
			display: inline-block;
			width: 14px;
			font-size: 11px;
		}
		.hint-state {
			margin-left: auto;
			font-size: 11px;
			opacity: 0.75;
		}
		/* 展开时不再提示"点击收起"，箭头本身已经说明 */
		.hint-card:not(.collapsed) .hint-state {
			display: none;
		}
		.hint-body {
			margin-top: 12px;
		}
		.hint-card.collapsed .hint-body {
			display: none;
		}
		/* 配额组卡片：整行标题可点击折叠，收起时只留「组名 + 周期 + 成员数」一行，组多了好找 */
		.quota-group-card {
			border: 1px solid var(--border-color);
			border-radius: 12px;
			padding: 16px 18px;
			margin-bottom: 16px;
			background: var(--section-item-bg);
		}
		.quota-group-head {
			display: flex;
			align-items: center;
			gap: 10px;
			flex-wrap: wrap;
			cursor: pointer;
			user-select: none;
		}
		.quota-group-head:hover {
			color: var(--text-main);
		}
		.quota-group-arrow {
			display: inline-block;
			width: 14px;
			font-size: 11px;
			color: var(--text-muted);
		}
		.quota-group-body {
			display: none;
		}
		.quota-group-card.open .quota-group-body {
			display: block;
		}
		/* ---- 紧凑列表卡片（渠道 / 账号 / 密钥 共用；方案B：一行摘要 + 点开展开）---- */
		.list-stack {
			display: flex;
			flex-direction: column;
			gap: 10px;
			margin-top: 16px;
		}
		.list-card {
			border: 1px solid var(--border-color);
			border-radius: 12px;
			background: var(--section-item-bg);
			overflow: hidden;
		}
		.list-head {
			display: flex;
			align-items: center;
			gap: 10px;
			flex-wrap: wrap;
			padding: 12px 16px;
			cursor: pointer;
			user-select: none;
		}
		.list-head:hover {
			background: var(--table-header-bg);
		}
		.list-arrow {
			width: 12px;
			font-size: 10px;
			color: var(--text-muted);
			flex: none;
		}
		.list-title {
			font-weight: 600;
			font-size: 14px;
			color: var(--text-main);
		}
		.list-meta {
			font-size: 12px;
			color: var(--text-muted);
		}
		.list-spacer {
			margin-left: auto;
		}
		.list-body {
			display: none;
			padding: 4px 16px 14px 38px;
			border-top: 1px solid var(--border-color);
		}
		.list-card.open .list-body {
			display: block;
		}
		.list-row {
			display: flex;
			gap: 10px;
			align-items: flex-start;
			padding: 7px 0;
		}
		.list-label {
			flex: none;
			width: 72px;
			font-size: 12px;
			color: var(--text-muted);
			padding-top: 1px;
		}
		.list-url {
			font-family: monospace;
			font-size: 12px;
			color: var(--code-color);
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
			cursor: pointer;
			min-width: 0;
		}
		.list-url:hover {
			text-decoration: underline;
		}
		.chips {
			display: flex;
			flex-wrap: wrap;
			gap: 6px;
			min-width: 0;
		}
		.chip {
			font-size: 11.5px;
			font-family: monospace;
			line-height: 1.5;
			padding: 2px 8px;
			border-radius: 999px;
			background: var(--table-header-bg);
			color: var(--text-main);
			border: 1px solid var(--border-color);
			max-width: 100%;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}
		.chip-more {
			cursor: pointer;
			color: var(--accent-color);
		}
		.list-actions {
			display: flex;
			gap: 8px;
			flex-wrap: wrap;
			padding-top: 8px;
			/* 操作按钮统一靠右 —— 更符合「内容在左、操作在右」的习惯（2026-10-09 用户要求） */
			justify-content: flex-end;
		}
		/* 密钥展示框：比普通行内 code 长，看着像个字段而不是一个碎片；点击即复制 */
		.key-cell {
			display: inline-block;
			min-width: 220px;
			max-width: 100%;
			padding: 5px 12px;
			border: 1px solid var(--border-color);
			border-radius: 8px;
			background: var(--table-header-bg);
			font-size: 12.5px;
			cursor: pointer;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}
		.key-cell:hover {
			border-color: var(--accent-color);
		}
		/* 外置启用/停用开关（渠道卡片头 / 配额组卡片头共用） */
		.switch {
			position: relative;
			display: inline-flex;
			align-items: center;
			width: 40px;
			height: 22px;
			flex: none;
			cursor: pointer;
		}
		.switch input {
			position: absolute;
			opacity: 0;
			width: 0;
			height: 0;
			margin: 0;
		}
		.switch .switch-track {
			position: absolute;
			inset: 0;
			border-radius: 999px;
			background: var(--input-border);
			transition: background .2s;
		}
		.switch .switch-track::after {
			content: '';
			position: absolute;
			top: 3px;
			left: 3px;
			width: 16px;
			height: 16px;
			border-radius: 50%;
			background: #fff;
			transition: transform .2s;
			box-shadow: 0 1px 2px rgba(0, 0, 0, .3);
		}
		.switch input:checked + .switch-track {
			background: var(--success-color);
		}
		.switch input:checked + .switch-track::after {
			transform: translateX(18px);
		}
		.switch input:focus-visible + .switch-track {
			outline: 2px solid var(--accent-color);
			outline-offset: 2px;
		}
		.hint-grid {
			display: grid;
			grid-template-columns: 84px minmax(0, 1fr);
			gap: 10px 14px;
			align-items: center;
		}
		.hint-key {
			font-size: 12.5px;
			color: var(--text-muted);
		}
		.hint-val {
			font-family: monospace;
			font-size: 12.5px;
			color: var(--code-color);
			overflow-wrap: anywhere;
		}
		.hint-plain {
			font-size: 12.5px;
			color: var(--text-main);
		}
		.hint-foot {
			font-size: 12px;
			line-height: 1.6;
			color: var(--text-muted);
			margin-top: 10px;
		}

		code {
			font-family: monospace;
			background-color: var(--code-bg);
			padding: 4px 8px;
			border-radius: 6px;
			font-size: 13px;
			color: var(--code-color);
			border: 1px solid var(--code-border);
			transition: all 0.2s ease;
		}

		/* Badges */
		.badge {
			display: inline-flex;
			padding: 4px 8px;
			border-radius: 6px;
			font-size: 11px;
			font-weight: 600;
			white-space: nowrap;
		}

		.badge-success { background-color: rgba(16, 185, 129, 0.15); color: #10b981; }
		.badge-warning { background-color: rgba(245, 158, 11, 0.15); color: #f59e0b; }
		.badge-danger { background-color: rgba(239, 68, 68, 0.15); color: #ef4444; }
		.badge-info { background-color: rgba(59, 130, 246, 0.15); color: #3b82f6; }

		/* Charts */
		.charts-grid {
			display: grid;
			grid-template-columns: minmax(0, 1.5fr) minmax(0, 1fr);
			gap: 20px;
		}

		.chart-container {
			position: relative;
			height: 300px;
			width: 100%;
		}

		@media (max-width: 900px) {
			.charts-grid {
				grid-template-columns: minmax(0, 1fr);
			}
		}

		/* Modals */
		.modal-overlay {
			position: fixed;
			top: 0;
			left: 0;
			right: 0;
			bottom: 0;
			background-color: var(--modal-overlay-bg);
			backdrop-filter: blur(0px);
			-webkit-backdrop-filter: blur(0px);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 1000;
			opacity: 0;
			pointer-events: none;
			transition: opacity 0.3s ease, backdrop-filter 0.3s ease, -webkit-backdrop-filter 0.3s ease;
		}

		.modal-overlay.active {
			opacity: 1;
			pointer-events: auto;
			backdrop-filter: blur(8px);
			-webkit-backdrop-filter: blur(8px);
		}

		.modal-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 20px;
			width: 100%;
			max-width: 500px;
			max-height: calc(100vh - 40px);
			padding: 32px;
			box-shadow: 0 20px 50px rgba(0, 0, 0, 0.4);
			display: flex;
			flex-direction: column;
			gap: 20px;
			transform: scale(0.9) translateY(20px);
			transition: transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), background-color 0.3s;
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
		}

		/* 通用确认小卡片：替代原生 confirm() 的轻量弹窗 */
		.confirm-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 16px;
			width: 100%;
			max-width: 340px;
			padding: 24px 24px 20px;
			box-shadow: 0 12px 32px rgba(0, 0, 0, 0.35);
			display: flex;
			flex-direction: column;
			align-items: center;
			gap: 10px;
			text-align: center;
			transform: scale(0.92) translateY(12px);
			transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1), background-color 0.3s;
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
		}

		.modal-overlay.active .confirm-card {
			transform: scale(1) translateY(0);
		}

		/* 确认框必须永远盖在其它弹窗之上：它常在弹窗内部被调用（「测模型」弹窗里删模型、换用它）。
		   两边 z-index 都是 1000 时谁在上由 DOM 顺序决定，而 #confirm-modal 在 DOM 里排得更靠前 →
		   确认框被压在下面看不见，用户得先关掉上层弹窗才看得到（2026-10-09 实测反馈）。 */
		#confirm-modal {
			z-index: 2000;
		}

		.confirm-icon {
			font-size: 26px;
			line-height: 1;
		}

		.confirm-title {
			font-size: 15px;
			font-weight: 600;
			color: var(--text-main);
		}

		.confirm-text {
			font-size: 13px;
			color: var(--text-muted);
			line-height: 1.6;
		}

		.confirm-actions {
			display: flex;
			gap: 10px;
			margin-top: 8px;
		}

		.confirm-actions .btn {
			min-width: 84px;
			padding: 8px 16px;
			font-size: 13px;
			border-radius: 10px;
		}

		/* 弹窗内容区：头部与底部按钮固定，中间可滚动，避免长表单把按钮顶出屏幕 */
		.modal-body {
			flex: 1 1 auto;
			min-height: 0;
			overflow-y: auto;
			display: flex;
			flex-direction: column;
			gap: 16px;
			margin: 0 -6px;
			padding: 0 6px;
			scrollbar-width: thin;
		}

		/* 关键：flex 子项默认可压缩，而带 overflow 的元素自动最小尺寸为 0，
		   不加这条的话结果框/文本域会被压成一条细缝（踩过） */
		.modal-body > * {
			flex-shrink: 0;
		}

		.modal-body .form-group {
			margin-bottom: 0;
		}

		.modal-overlay.active .modal-card {
			transform: scale(1) translateY(0);
		}

		.modal-header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			border-bottom: 1px solid var(--border-color);
			padding-bottom: 16px;
		}

		.modal-header h3 {
			font-size: 18px;
			font-weight: 600;
		}

		.modal-footer {
			display: flex;
			justify-content: flex-end;
			gap: 12px;
			border-top: 1px solid var(--border-color);
			padding-top: 20px;
			margin-top: 10px;
		}

		/* Toast Notification (Green for success, Red for error, Orange for warning) */
		.toast-container {
			position: fixed;
			top: 24px;
			right: 24px;
			display: flex;
			flex-direction: column;
			gap: 10px;
			z-index: 9999;
			pointer-events: none;
		}

		.toast {
			min-width: 260px;
			padding: 14px 20px;
			border-radius: 12px;
			box-shadow: var(--card-shadow);
			font-size: 14px;
			font-weight: 600;
			display: flex;
			align-items: center;
			gap: 12px;
			backdrop-filter: blur(15px);
			-webkit-backdrop-filter: blur(15px);
			transform: translateY(-20px);
			opacity: 0;
			transition: all 0.4s cubic-bezier(0.16, 1, 0.3, 1);
			pointer-events: auto;
		}

		.toast.show {
			transform: translateY(0);
			opacity: 1;
		}

		.toast-icon {
			width: 20px;
			height: 20px;
			flex-shrink: 0;
		}

		.toast-success {
			background-color: #10b981 !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-success .toast-icon, .toast-success span {
			color: #ffffff !important;
		}

		.toast-error {
			background-color: #ef4444 !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-error .toast-icon, .toast-error span {
			color: #ffffff !important;
		}
		
		.toast-warning {
			background-color: #f59e0b !important;
			color: #ffffff !important;
			border: none !important;
		}
		.toast-warning .toast-icon, .toast-warning span {
			color: #ffffff !important;
		}

		/* Mobile Responsiveness */
		.mobile-header {
			display: none !important;
		}

		@media (max-width: 768px) {
			aside {
				transform: translateX(-100%);
			}
			aside.active {
				transform: translateX(0);
			}
			main {
				margin-left: 0;
				padding: 20px;
			}
			.mobile-header {
				display: flex !important;
			}
			.mobile-nav-toggle {
				background: none;
				border: none;
				color: var(--text-main);
				display: flex;
				align-items: center;
				gap: 6px;
				cursor: pointer;
				font-size: 14px;
				font-weight: 500;
			}
		}

		.public-chart-wrapper {
			position: relative;
			height: 190px;
			width: 100%;
			display: flex;
			align-items: center;
			justify-content: center;
			gap: 40px;
			overflow: hidden;
		}

		.public-chart-wrapper canvas {
			max-width: 100% !important;
		}

		#admin-chart-legend {
			scrollbar-width: none;
			-ms-overflow-style: none;
		}
		#admin-chart-legend::-webkit-scrollbar {
			display: none;
		}

		@media (max-width: 768px) {
			.public-chart-wrapper {
				flex-direction: column !important;
				height: auto !important;
				padding: 10px 0;
				gap: 20px !important;
			}
			.public-chart-wrapper > div:first-child {
				width: 160px !important;
				height: 160px !important;
			}
			.public-chart-wrapper > div:nth-child(2) {
				width: 100% !important;
				height: auto !important;
				align-items: center !important;
			}
		}
	</style>
</head>
<body${cfEnabled ? '' : ' class="cf-off"'}>
	<!-- Dynamic Background Orbs -->
	<div class="bg-orbs-container">
		<div class="bg-orb bg-orb-1"></div>
		<div class="bg-orb bg-orb-2"></div>
	</div>

	<!-- App Header for Mobile Toggle -->
	<div style="display: flex; justify-content: space-between; align-items: center; padding: 15px 20px; background-color: var(--sidebar-bg); border-bottom: 1px solid var(--border-color); z-index: 90;" class="mobile-header">
		<div class="logo-area" style="margin-bottom: 0;">
			<div class="logo-icon">AI</div>
			<span class="logo-text">Workers API Hub</span>
		</div>
		<div style="display: flex; align-items: center; gap: 12px;">
			<button class="mobile-nav-toggle" onclick="toggleSidebar()">
				<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path></svg>
				<span>菜单</span>
			</button>
		</div>
	</div>

	<div class="app-container">
		<!-- Sidebar -->
		<aside id="sidebar">
			<div class="logo-area">
				<div class="logo-icon">AI</div>
				<span class="logo-text">Workers API Hub</span>
			</div>

			<div class="runtime-status" onclick="openRuntimeModal()" title="点击修改运行模式">
				<span class="runtime-status-dot" id="runtime-status-dot"></span>
				<span class="runtime-status-value" id="runtime-status-text">载入中...</span>
			</div>
			
			<div class="nav-menu">
				<div class="nav-group-title cf-only">概览</div>
				<div class="nav-item${defaultTab === 'overview' ? ' active' : ''}" id="menu-overview" onclick="switchTab('overview')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path></svg>
					数据看板
				</div>

				<div class="nav-group-title">代理接入</div>
				<div class="nav-item${defaultTab === 'access' ? ' active' : ''}" id="menu-access" onclick="switchTab('access')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"></path></svg>
					接入信息
				</div>
				<div class="nav-item${defaultTab === 'providers' ? ' active' : ''}" id="menu-providers" onclick="switchTab('providers')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg>
					第三方渠道
				</div>

				<div class="nav-group-title">路由</div>
				<div class="nav-item" id="menu-quota" onclick="switchTab('quota')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"></path></svg>
					调用配额
				</div>
				<div class="nav-item" id="menu-settings" onclick="switchTab('settings')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
					模型映射
				</div>

				<div class="nav-group-title">设置</div>
				<div class="nav-item cf-only" id="menu-accounts" onclick="switchTab('accounts')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
					账号管理
				</div>
				<div class="nav-item" id="menu-theme" onclick="toggleTheme()">
					<svg class="theme-icon-sun" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none; width: 18px; height: 18px;">
						<circle cx="12" cy="12" r="4" />
						<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
					</svg>
					<svg class="theme-icon-moon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width: 18px; height: 18px;">
						<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z" />
					</svg>
					切换主题
				</div>
				<div class="nav-item" id="menu-logout" onclick="logout()">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path></svg>
					退出登录
				</div>
			</div>

			<div class="aside-footer">
				<div style="text-align: center; font-size: 11px; color: var(--text-muted); opacity: 0.55; padding-top: 4px;">
					<a href="https://github.com/ldg118/workers-api-hub" target="_blank" rel="noopener noreferrer" style="color: inherit; text-decoration: none; border-bottom: 1px solid currentColor;">GitHub</a> · Workers API Hub · <span title="${BUILD_ID}" style="cursor: default;">${BUILD_ID.split('.').pop()}</span>
				</div>
			</div>
		</aside>

		<!-- Main Workspace -->
		<main>
			<div id="auth-views" style="display: flex; flex-direction: column; gap: 30px; width: 100%;">
				
				<!-- TAB: Overview -->
				<div id="tab-overview" class="tab-content${defaultTab === 'overview' ? ' active' : ''}">
					<!-- 第三方渠道调用统计（与下方 CF 官方用量是两条独立数据源） -->
					<div class="section-card">
						<div class="section-header">
							<div class="section-title">第三方渠道调用</div>
							<div style="display: flex; align-items: center; gap: 8px;">
								<button class="btn btn-secondary" id="stats-range-today" onclick="setStatsRange('today')" style="padding: 6px 12px; font-size: 12px;">今日</button>
								<button class="btn btn-secondary" id="stats-range-7d" onclick="setStatsRange('7d')" style="padding: 6px 12px; font-size: 12px;">近 7 天</button>
								<button class="btn btn-secondary" id="stats-range-all" onclick="setStatsRange('all')" style="padding: 6px 12px; font-size: 12px;">全部</button>
								<button class="btn btn-secondary" id="stats-refresh" onclick="refreshProviderStats()" title="重新从 D1 读取统计" style="padding: 6px 12px; font-size: 12px;">↻ 刷新</button>
							<label class="btn btn-secondary stats-toggle" id="stats-include-inactive-wrap" style="font-size: 12px; padding: 6px 12px; gap: 5px; white-space: nowrap;">
								<input type="checkbox" id="stats-include-inactive" onchange="this.closest('label').classList.toggle('is-checked', this.checked); refreshProviderStats()"> 显示已隐藏
							</label>
							<button class="btn btn-secondary" id="stats-cleanup-orphans" onclick="cleanupOrphanModels()" title="把统计里有、但已不在任何渠道模型列表中的失效模型一次性隐藏" style="padding: 6px 12px; font-size: 12px;">隐藏失效模型</button>
							</div>
						</div>
						<div class="section-note">仅统计「经过本代理」的第三方渠道请求；与上游账单口径不同，仅供参考。</div>

						<div id="provider-stats-disabled" style="display: none; background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 14px 16px; border-radius: 12px; font-size: 13px; color: var(--warning-color); line-height: 1.6; margin-top: 14px;">
							<strong>统计未启用：</strong> 未绑定 D1 数据库。建库后在 Worker 设置里加 D1 绑定（变量名填 <code>DB</code>）并重新部署，首次调用会自动建表。
						</div>

						<div id="provider-stats-body" style="margin-top: 18px;">
						<div class="card-grid" style="margin-bottom: 18px;">
							<div class="stat-card">
								<div class="stat-title">Token 数</div>
								<div class="stat-value" id="stats-tokens">—</div>
								<div class="stat-desc" id="stats-reasoning">含思考 —</div>
							</div>
							<div class="stat-card">
								<div class="stat-title">成功率</div>
								<div class="stat-value" id="stats-okrate">—</div>
								<div class="stat-desc" id="stats-okfail"></div>
							</div>
							<div class="stat-card">
								<div class="stat-title">平均延迟</div>
								<div class="stat-value" id="stats-avgms">—</div>
								<div class="stat-desc">按请求数加权</div>
							</div>
							<div class="stat-card">
								<div class="stat-title">估算成本</div>
								<div class="stat-value" id="stats-cost">—</div>
								<div class="stat-desc">按 Token 粗估（非真实账单）</div>
							</div>
						</div>

						<!-- 图表：近 7 日逐模型 Token 走势 + 今日模型消耗占比（与 CF 侧图表同风格） -->
						<div class="charts-grid" style="margin-bottom: 18px;">
							<div class="section-card">
								<div class="section-title">过去 7 日消耗 Token（按模型）</div>
								<div class="chart-container">
									<canvas id="providerTrendChart"></canvas>
								</div>
							</div>
							<div class="section-card">
								<div class="section-title">今日模型消耗占比</div>
								<div class="public-chart-wrapper" style="display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 14px; height: auto; overflow: visible; padding: 12px 0;">
									<div id="provider-donut-wrapper" style="position: relative; height: 160px; width: 160px; flex-shrink: 0; display: flex; align-items: center; justify-content: center;">
										<canvas id="providerDonutChart"></canvas>
									</div>
									<div style="width: 100%; display: flex; flex-direction: row; flex-wrap: wrap; justify-content: center; gap: 6px 12px; min-width: 0;">
										<div id="provider-donut-legend" style="width: 100%; display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 6px 10px; min-width: 0;"></div>
									</div>
									<div id="provider-donut-placeholder" style="display: none; flex-direction: column; align-items: center; justify-content: center; height: 100%; width: 100%; color: var(--text-muted); font-size: 13px; gap: 12px; margin: auto;">
										<svg style="width: 32px; height: 32px; opacity: 0.5;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
											<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"></path>
											<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"></path>
										</svg>
										<span>今日暂无消耗数据</span>
									</div>
								</div>
							</div>
						</div>

						<table>
							<thead>
								<tr>
									<th>渠道</th>
									<th>请求</th>
									<th>成功 / 失败</th>
									<th>成功率</th>
									<th>平均延迟</th>
									<th>Token 数</th>
									<th>思考 Token</th>
									<th>估算成本</th>
								</tr>
							</thead>
							<tbody id="provider-stats-rows"></tbody>
						</table>
						</div>
					</div>

					<!-- CF 账号池概览小卡（池关闭时随 cf-only 一起隐藏） -->
					<div class="card-grid cf-only" style="margin-top: 24px;">
						<div class="stat-card">
							<div style="display: flex; justify-content: space-between; align-items: flex-start;">
								<div class="stat-title">今日总消耗量</div>
								<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: var(--accent-color); opacity: 0.85;">
									<path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
								</svg>
							</div>
							<div class="stat-value" id="stat-total-neurons">0</div>
							<div class="progress-container">
								<div class="progress-bar" id="stat-neurons-progress" style="width: 0%;"></div>
							</div>
							<div class="stat-desc" id="stat-neurons-desc">0 / 0 Neurons (0.00%)</div>
						</div>
						<div class="stat-card">
							<div style="display: flex; justify-content: space-between; align-items: flex-start;">
								<div class="stat-title">已绑定账号</div>
								<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: var(--accent-color); opacity: 0.85;">
									<path stroke-linecap="round" stroke-linejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
								</svg>
							</div>
							<div class="stat-value" id="stat-accounts-count">0</div>
							<div class="stat-desc">活跃中的 Cloudflare 账号数</div>
						</div>
						<div class="stat-card">
							<div style="display: flex; justify-content: space-between; align-items: flex-start;">
								<div class="stat-title">代理 API 密钥</div>
								<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: var(--accent-color); opacity: 0.85;">
									<path stroke-linecap="round" stroke-linejoin="round" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
								</svg>
							</div>
							<div class="stat-value" id="stat-keys-count">0</div>
							<div class="stat-desc">已配额调用 Key数</div>
						</div>
						<div class="stat-card">
							<div style="display: flex; justify-content: space-between; align-items: flex-start;">
								<div class="stat-title">节省成本 (估算)</div>
								<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: var(--accent-color); opacity: 0.85;">
									<path stroke-linecap="round" stroke-linejoin="round" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
								</svg>
							</div>
							<div class="stat-value" id="stat-cost-saving">$0.00</div>
							<div class="stat-desc">对比 OpenAI completions 同等额度价格</div>
						</div>
					</div>

					<!-- Charts -->
					<div class="charts-grid cf-only" style="margin-top: 24px;">
						<div class="section-card">
							<div class="section-title">过去 7 日消耗走势（Neurons）</div>
							<div class="chart-container">
								<canvas id="historyChart"></canvas>
							</div>
						</div>
						<div class="section-card">
							<div class="section-title">今日模型消耗占比</div>
							<div class="chart-container public-chart-wrapper" id="admin-chart-wrapper" style="display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 16px; height: auto; overflow: visible; padding: 12px 0;">
								<!-- Left: Chart -->
								<div id="admin-canvas-wrapper" style="position: relative; height: 220px; width: 220px; flex-shrink: 0; display: flex; align-items: center; justify-content: center;">
									<canvas id="modelsChart"></canvas>
								</div>
								<!-- Right: Legend -->
								<div id="admin-legend-wrapper" style="width: 100%; display: flex; flex-direction: row; flex-wrap: wrap; justify-content: center; gap: 6px 12px; min-width: 0;">
									<div id="admin-chart-legend" style="width: 100%; display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 6px 10px; min-width: 0;"></div>
								</div>
								<!-- Empty Placeholder -->
								<div id="admin-chart-placeholder" style="display: none; flex-direction: column; align-items: center; justify-content: center; height: 100%; width: 100%; color: var(--text-muted); font-size: 13px; gap: 12px; margin: auto;">
									<svg style="width: 32px; height: 32px; opacity: 0.5;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
										<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"></path>
										<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"></path>
									</svg>
									<span>今日暂无消耗数据</span>
								</div>
							</div>
						</div>
					</div>

					<!-- Detailed Accounts Usage Grid -->
					<div class="section-card cf-only" style="margin-top: 24px;">
						<div class="section-header">
							<div class="section-title">账号用量明细</div>
							<div style="display: flex; align-items: center; gap: 12px;">
								<span id="txt-last-updated" style="font-size: 12px; color: var(--text-muted); font-family: monospace;"></span>
								<button class="btn btn-secondary" id="btn-refresh-usage" onclick="loadUsageDetails(true)">刷新用量</button>
							</div>
						</div>
						<div id="accounts-usage-list" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px;">
							<!-- Individual account progress item -->
						</div>
					</div>
				</div>

				<!-- TAB: Accounts -->
				<div id="tab-accounts" class="tab-content cf-only">
					<div class="section-card">
						<div class="section-header">
							<div class="section-title">账号配置</div>
							<button class="btn btn-primary" onclick="openAddAccountModal()">
								<svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
								添加账号
							</button>
						</div>

						<div class="list-stack" id="accounts-list">
							<!-- Account cards -->
						</div>
					</div>
				</div>

				<!-- TAB: Access (endpoint info) -->
				<div id="tab-access" class="tab-content${defaultTab === 'access' ? ' active' : ''}">
					<div class="section-card" style="margin-bottom: 24px;">
						<div class="section-title">接入信息</div>
						<div class="section-note">把客户端接到这个代理上。OpenAI SDK 与 Anthropic Messages 两种格式都能直连，点击地址即可复制。</div>

						<div class="hint-card collapsed" id="access-hint">
							<div class="hint-head" onclick="toggleHint('access-hint')">
								<span class="hint-arrow">▾</span>
								<span>客户端要填三个值</span>
								<span class="hint-state">点击展开</span>
							</div>
							<div class="hint-body">
								<div class="hint-grid" style="grid-template-columns: 72px minmax(0, 1fr);">
									<span class="hint-key">Base URL</span>
									<span class="hint-plain" style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
										<code id="access-base-url" style="color:var(--text-main);">载入中…</code>
										<button type="button" class="btn btn-secondary" id="access-base-url-copy" style="padding:3px 9px; font-size:11px; border-radius:6px;" data-endpoint-url="" onclick="copyEndpointUrl(this.dataset.endpointUrl)">复制</button>
									</span>
									<span class="hint-key">API Key</span>
									<span class="hint-plain" style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
										<code id="access-key-value" style="color:var(--text-main);" title="当前密钥（已脱敏）">载入中…</code>
										<button type="button" class="btn btn-secondary" id="access-key-copy" style="padding:3px 9px; font-size:11px; border-radius:6px; display:none;" onclick="copyAccessKey()">复制</button>
										<span id="access-key-more" style="color:var(--text-muted); font-size:12px;"></span>
									</span>
									<span class="hint-key">模型名</span>
									<span class="hint-plain">你在「模型映射」里配过的名字</span>
								</div>
								<div class="hint-foot">Base URL 填到 <code>/v1</code> 为止，不要带 <code>/chat/completions</code>。</div>
							</div>
						</div>
						<div class="access-endpoint-grid" style="margin-top: 18px;">
							<div class="access-endpoint-card">
								<div class="endpoint-badge">OpenAI 兼容格式</div>
								<button type="button" class="endpoint-url" id="openai-endpoint-url" data-endpoint-url="" onclick="copyEndpointUrl(this.dataset.endpointUrl)">https://domain/v1/chat/completions</button>
							</div>
							<div class="access-endpoint-card">
								<div class="endpoint-badge">Anthropic 兼容格式</div>
								<button type="button" class="endpoint-url" id="anthropic-endpoint-url" data-endpoint-url="" onclick="copyEndpointUrl(this.dataset.endpointUrl)">https://domain/v1/messages</button>
							</div>
						</div>
						<div class="hint-foot" style="margin-top: 14px;">携带密钥时用 <code>Authorization: Bearer &lt;密钥&gt;</code> 或 <code>x-api-key</code> 请求头。</div>
					</div>

					<div class="section-card">
						<div class="section-header">
							<div class="section-title">API 密钥</div>
							<button class="btn btn-primary" onclick="openAddKeyModal()">
								<svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
								生成新密钥
							</button>
						</div>
						
						<div class="section-note">客户端调用本代理时用的凭证，跟上游是 Cloudflare 还是第三方渠道无关。系统已内置一个「系统默认密钥」（不可删除），作为防「误删光所有自定义密钥导致接口变公开」的安全兜底。建议保留至少一把自己的自定义密钥，并尽快把系统默认密钥改成你自己的强密钥。</div>

						<div style="background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 14px 16px; border-radius: 12px; font-size: 13px; color: var(--warning-color); line-height: 1.5; margin-top: 12px; margin-bottom: 10px;" id="no-key-warning" class="hidden">
							<strong>提示：</strong> 当前只有出厂「系统默认密钥」（仅作安全兜底，不建议日常使用）。请生成一把你自己的自定义密钥，并点击系统默认密钥的「重新生成」按钮换成一个只有你知道的强密钥。
						</div>

						<div style="display:flex; align-items:center; gap:12px; margin: 4px 0 14px; font-size:13px; flex-wrap:wrap;">
							<label style="display:flex; align-items:center; gap:8px; cursor:pointer; user-select:none;">
								<input type="checkbox" id="rotation-toggle" style="width:16px; height:16px; cursor:pointer;">
								<span>系统默认密钥每 7 天自动轮换（防泄露）</span>
							</label>
							<span id="rotation-next" style="color:var(--text-muted);"></span>
						</div>

						<div class="list-stack" id="keys-list">
							<!-- Keys cards -->
						</div>
					</div>

					<div class="hint-card collapsed" id="access-test-hint" style="margin-top: 24px;">
						<div class="hint-head" onclick="toggleHint('access-test-hint')">
							<span class="hint-arrow">▾</span>
							<span>快速自测</span>
							<span class="hint-state">点击展开</span>
						</div>
						<div class="hint-body">
							<div class="section-note">复制到终端执行，能拿到回复就说明链路是通的。</div>
							<pre id="access-curl-sample" style="margin-top: 16px; background-color: var(--section-item-bg); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px; font-size: 12px; line-height: 1.7; color: var(--text-main); overflow-x: auto; white-space: pre-wrap; word-break: break-all;">载入中...</pre>
						</div>
					</div>
				</div>

				<!-- TAB: Settings (Model Mapping) -->
				<div id="tab-settings" class="tab-content">
					<div class="section-card">
						<div class="section-title">模型映射</div>
						<div class="section-note">客户端请求的模型名，实际发给哪个上游模型。</div>

						<div class="hint-card collapsed" id="mapping-hint">
							<div class="hint-head" onclick="toggleHint('mapping-hint')">
								<span class="hint-arrow">▸</span>
								<span>目标值填什么，决定请求去哪</span>
								<span class="hint-state">点击展开</span>
							</div>
							<div class="hint-body">
								<div class="hint-grid">
									<span class="hint-key">Cloudflare</span>
									<span class="hint-val">@cf/meta/llama-3.1-8b-instruct</span>
									<span class="hint-key">第三方渠道</span>
									<span class="hint-val">provider:渠道名/模型名</span>
								</div>
								<div class="hint-foot">渠道写法可省略 <code>provider:</code> 前缀；模型名以 <code>@cf/</code> 开头时直接透传，不走映射。<br>徽标：<span class="badge badge-success">省额度</span> <span class="badge badge-warning">中等</span> <span class="badge badge-danger">较贵</span> 为消耗免费额度的档位（10,000 Neurons/天，悬停看区间）；</div>
							</div>
						</div>
						<div id="cf-off-warning" style="background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 14px 16px; border-radius: 12px; font-size: 13px; color: var(--warning-color); line-height: 1.6; margin-bottom: 20px;"><strong>注意：</strong> Cloudflare 账号池已关闭，下方所有指向 <code>@cf/</code> 的映射当前<b>不生效</b>（请求会继续落到默认渠道）。要重新启用请点侧边栏顶部的「运行模式」。</div>

						<div style="display: grid; grid-template-columns: 1fr 1.5fr auto; gap: 15px; background-color: var(--section-item-bg); padding: 20px; border-radius: 12px; border: 1px solid var(--border-color); margin-top: 10px;">
							<div class="form-group" style="margin-bottom: 0;">
								<label>请求模型名</label>
								<input type="text" id="map-source" placeholder="如: gpt-3.5-turbo">
							</div>
							<div class="form-group map-combo" style="margin-bottom: 0;">
								<label>目标模型</label>
								<input type="text" id="map-target" placeholder="下拉选已有模型，或直接手输" autocomplete="off">
								<div class="map-combo-menu" id="map-target-menu"></div>
							</div>
							<div style="display: flex; align-items: flex-end; gap: 10px; flex-wrap: wrap;">
								<button class="btn btn-primary" onclick="addMapping()" style="height: 45px;">添加/修改</button>
								<button class="btn btn-secondary" onclick="restorePresetMappings()" style="height: 45px;">预设映射</button>
							</div>
						</div>

						<table style="margin-top: 20px;">
							<thead>
								<tr>
									<th>客户端请求模型</th>
									<th>映射后的目标模型</th>
									<th>类型</th>
									<th style="width: 160px;">启用 / 操作</th>
								</tr>
							</thead>
							<tbody id="mappings-table-body">
								<!-- Mapping rows -->
							</tbody>
						</table>
					</div>
				</div>

				<!-- TAB: Quota Groups -->
				<div id="tab-quota" class="tab-content">
					<div class="section-card">
						<div class="section-header">
							<div class="section-title">调用配额</div>
							<button class="btn btn-primary" onclick="openQuotaModal()">
								<svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
								新建配额组
							</button>
						</div>
						<div class="section-note">把一组「同一用途的候选模型」放进配额组，每个成员设一个上限（每天或每月），单位可选<b>次数</b>或 <b>token</b>。默认按消耗比例<b>自动分摊</b>（谁用得少用谁），某个成员报错会<b>自动换下一个</b>并把它临时冷却。</div>
						<div class="hint-card collapsed" id="quota-hint">
							<div class="hint-head" onclick="toggleHint('quota-hint')">
								<span class="hint-arrow">▸</span>
								<span>怎么用配额组</span>
								<span class="hint-state">点击展开</span>
							</div>
							<div class="hint-body">
								<div class="hint-grid" style="grid-template-columns: 88px minmax(0, 1fr);">
									<span class="hint-key">直接调用</span>
									<span class="hint-val">组名</span>
									<span class="hint-key">配映射</span>
									<span class="hint-val">TT:组名</span>
								</div>
								<div class="hint-foot">已用量复用调用统计里的数据（按成员各自的单位：次数或 token），每个请求只多一次本地查询，<b>不会增加上游调用次数</b>。上限填 <code>0</code> 表示不限；<b>成员顺序只在消耗比例相同时才决定优先</b>（不是固定顺位）—— 正常是「谁这一小时用得少用谁」。<br>重置周期可填「每天」或「每月 N 日」（N 为 1-31，短月自动落到当月最后一天）；基准时区可直接打字选预设，也可填偏移分钟数（北京时间 = <code>480</code>）。<br>用它对齐上游的真实重置时间 —— 例如 Gemini 的每日配额在<b>太平洋时间午夜</b>重置：基准时区选「美国太平洋（夏令时）UTC-7」+ 重置时刻 <code>00:00</code> 即可（换算成 UTC 07:00 / 北京时间 15:00）。</div>
							</div>
						</div>
						<div id="quota-sched-bar" style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin-top: 16px; padding: 12px 16px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--section-item-bg);">
							<label style="display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text-main); cursor: pointer;">
								<input type="checkbox" id="quota-sched-toggle" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);" onchange="toggleQuotaScheduling(this.checked)">
								负载均衡调度（按用量自动分摊 · 失败自动换成员 · 出错成员临时冷却）
							</label>
							<span id="quota-sched-cooldowns" style="font-size: 12px; color: var(--text-muted);"></span>
							<button type="button" class="btn btn-secondary" id="quota-sched-clear" style="padding: 4px 10px; font-size: 11px; border-radius: 6px; display: none;" onclick="clearQuotaCooldowns()">解除全部冷却</button>
						</div>
						<div id="quota-d1-warning" class="hidden" style="background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 14px 16px; border-radius: 12px; font-size: 13px; color: var(--warning-color); line-height: 1.6; margin-top: 16px;">
							<strong>注意：</strong> 未绑定 D1 数据库（变量名 <code>DB</code>），配额组无法统计已用量（次数 / token）。设了上限的组会<b>直接报错</b>，不会静默放行。请到 Worker → Settings → Bindings 添加 D1。
						</div>
						<div id="quota-groups-list" style="margin-top: 20px;"></div>
					</div>
				</div>

				<!-- TAB: Third-party Providers -->
				<div id="tab-providers" class="tab-content${defaultTab === 'providers' ? ' active' : ''}">
					<div class="section-card">
						<div class="section-header">
							<div class="section-title">第三方渠道</div>
							<button class="btn btn-primary" onclick="openProviderModal()">
								<svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
								添加渠道
							</button>
						</div>
						<div class="section-note">接入任意 OpenAI 兼容端点（DeepSeek / Groq / OpenRouter / 自建 one-api 等），请求按模型名决定走哪一边。</div>

						<div class="hint-card collapsed" id="providers-hint">
							<div class="hint-head" onclick="toggleHint('providers-hint')">
								<span class="hint-arrow">▸</span>
								<span>让请求走这条渠道，两种方式</span>
								<span class="hint-state">点击展开</span>
							</div>
							<div class="hint-body">
								<div class="hint-grid" style="grid-template-columns: 72px minmax(0, 1fr);">
									<span class="hint-key">直接调用</span>
									<span class="hint-val">渠道名/模型名</span>
									<span class="hint-key">配映射</span>
									<span class="hint-val">provider:渠道名/模型名</span>
								</div>
								<div class="hint-foot">渠道消耗不计入 Cloudflare 的 Neuron 统计，不会出现在用量看板上。</div>
							</div>
						</div>

						<div class="list-stack" id="providers-list">
							<!-- Provider cards -->
						</div>
					</div>
				</div>

			</div>
		</main>
	</div>

	<!-- Modal: Add Cloudflare Account -->
	<div class="modal-overlay" id="account-modal">
		<div class="modal-card">
			<div class="modal-header">
				<h3 id="account-modal-title">添加 Cloudflare 账号</h3>
				<button onclick="closeAccountModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<input type="hidden" id="account-id-edit">
			<div class="form-group">
				<label for="account-name">账号别名 (如: 主账号 A)</label>
				<input type="text" id="account-name" placeholder="请输入备注名">
			</div>
			<div class="form-group">
				<label for="account-id">Account ID</label>
				<input type="text" id="account-id" placeholder="获取于 CF 控制台 Workers AI 页面" oninput="onAccountInfoChange()">
			</div>
			<div class="form-group">
				<label for="account-token">API Token (需要创建并赋予以下 3 个权限):</label>
				<div style="font-size: 12px; color: var(--text-muted); background: rgba(0,0,0,0.15); padding: 8px 12px; border-radius: 6px; margin-top: 4px; margin-bottom: 4px; line-height: 1.5; font-family: monospace;">
					• Workers AI &gt; Read <span id="perm-wa-read" style="margin-left: 8px;"></span><br>
					• Workers AI &gt; Edit <span id="perm-wa-edit" style="margin-left: 8px;"></span><br>
					• Account Analytics &gt; Read <span id="perm-aa-read" style="margin-left: 8px;"></span>
				</div>
				<input type="text" id="account-token" placeholder="CF 账号 API Token (会安全遮蔽保存)" oninput="onAccountInfoChange()">
			</div>
			
			<div id="test-result-alert" style="display: none; padding: 12px 16px; border-radius: 8px; font-size: 13px; font-weight: 500; word-break: break-word; overflow-wrap: break-word; max-height: 200px; overflow-y: auto; line-height: 1.6; border: 1px solid transparent;"></div>

			<div class="modal-footer">
				<button class="btn btn-success" onclick="testConnection()" id="btn-test-conn">测试连接</button>
				<button class="btn btn-primary" onclick="saveAccount()" id="btn-save-account" disabled>保存账号</button>
			</div>
		</div>
	</div>

	<!-- Modal: 通用确认小卡片（替代原生 confirm） -->
	<div class="modal-overlay" id="confirm-modal">
		<div class="confirm-card">
			<div class="confirm-icon" id="confirm-icon">⚠️</div>
			<div class="confirm-title" id="confirm-title">确认操作</div>
			<div class="confirm-text" id="confirm-text"></div>
			<div class="confirm-actions">
				<button class="btn btn-secondary" id="confirm-cancel-btn">取消</button>
				<button class="btn btn-primary" id="confirm-ok-btn">确定</button>
			</div>
		</div>
	</div>

	<!-- Modal: Add API Key -->
	<div class="modal-overlay" id="key-modal">
		<div class="modal-card">
			<div class="modal-header">
				<h3 id="key-modal-title">生成新 API 密钥</h3>
				<button onclick="closeKeyModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<div id="key-modal-form">
				<div class="form-group" style="margin-bottom: 16px;">
					<label for="key-name">密钥描述/使用客户端 (如: Cursor / NextChat)</label>
					<input type="text" id="key-name" placeholder="请输入描述名" style="width: 100%;">
				</div>
			<div class="form-group" style="margin-bottom: 16px;">
				<label for="key-val">API 密钥值 (可选，为空则随机生成 sk-wa-...)</label>
				<input type="text" id="key-val" placeholder="留空则随机生成密钥" style="width: 100%;">
			</div>
			<div class="form-group" style="margin-bottom: 16px;">
				<label for="key-expires">有效期（过期后自动失效，需重新生成）</label>
				<select id="key-expires" style="width: 100%; padding: 8px 10px; border-radius: 8px; border: 1px solid var(--border-color); background: var(--section-item-bg); color: var(--text-main); font-size: 13px;">
					<option value="0">永久有效</option>
					<option value="1">1 天</option>
					<option value="7">7 天</option>
					<option value="30">30 天</option>
				</select>
			</div>
				<div class="modal-footer" style="margin-top: 10px; display: flex; gap: 12px; justify-content: flex-end; width: 100%;">
					<button class="btn btn-secondary" onclick="closeKeyModal()">取消</button>
					<button class="btn btn-primary" onclick="saveKey()">生成密钥</button>
				</div>
			</div>
			<div id="key-modal-success" class="hidden" style="display: flex; flex-direction: column; gap: 16px;">
				<div style="text-align: center; color: var(--success-color); font-size: 40px; margin-bottom: 8px;">🎉</div>
				<p style="font-size: 14px; text-align: center; line-height: 1.6; color: var(--text-main);">
					密钥生成成功！请务必复制保存此密钥，关闭后将无法再次完整查看。
				</p>
				<div class="form-group">
					<label>API Key</label>
					<div style="display: flex; gap: 10px;">
						<input type="text" id="generated-key-val" readonly style="flex: 1; font-family: monospace;">
						<button class="btn btn-primary" onclick="copyGeneratedKey()">复制</button>
					</div>
				</div>
				<div class="modal-footer" style="margin-top: 10px; width: 100%;">
					<button class="btn btn-secondary" onclick="closeKeyModal()" style="width: 100%;">我已保存，关闭</button>
				</div>
			</div>
		</div>
	</div>

	<!-- Modal: Third-party Provider -->
	<div class="modal-overlay" id="provider-modal">
		<div class="modal-card" style="max-width: 540px; padding: 24px;">
			<div class="modal-header">
				<h3 id="provider-modal-title">添加第三方渠道</h3>
				<button onclick="closeProviderModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<input type="hidden" id="provider-id-edit">

			<div class="modal-body">
				<div class="form-group">
					<label for="provider-preset">快速预设（选一个自动填 Base URL 与示例模型）</label>
					<select id="provider-preset" onchange="applyProviderPreset(this.value)" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit;"></select>
				</div>

				<div class="form-group">
					<label for="provider-name">渠道名 (用于映射写法，建议用英文短名)</label>
					<input type="text" id="provider-name" placeholder="如: deepseek">
				</div>

				<div class="form-group">
					<label for="provider-base-url">Base URL (填到 /v1 或 /v1beta/openai 为止，不要带 /chat/completions)</label>
					<input type="text" id="provider-base-url" placeholder="如: https://api.deepseek.com/v1">
				</div>

				<div class="form-group">
					<label for="provider-api-key">API Key (编辑时留空表示不修改)</label>
					<input type="text" id="provider-api-key" placeholder="sk-...">
				</div>

				<div class="form-group">
					<label for="provider-models">模型列表 (每行一个，第一个用于连通性测试。模型名会过期，拿不准就从上游拉一份)</label>
					<textarea id="provider-models" rows="2" placeholder="deepseek-chat&#10;deepseek-reasoner" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit; resize: vertical;"></textarea>
					<div style="display: flex; align-items: center; gap: 10px; margin-top: 8px; flex-wrap: wrap;">
						<button type="button" class="btn btn-secondary" style="padding: 6px 12px; font-size: 12px; border-radius: 8px;" onclick="fetchUpstreamModels()" id="btn-fetch-models">从上游拉取模型列表</button>
						<span style="font-size: 12px; color: var(--text-muted);">已保存的渠道才能拉取</span>
					</div>
				</div>

				<div class="form-group">
					<label for="provider-status">状态</label>
					<select id="provider-status" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit;">
						<option value="active">启用</option>
						<option value="disabled">停用</option>
					</select>
				</div>

				<!-- 长流直通（2026-10-10）：免费档 CPU 10ms 会掐断超长输出（实测 1.8MB 流被 exceededResources），
				     直通 = 流式响应不逐 chunk 读取，由运行时原生直管，CPU 趋近零。
				     代价：流内 model 不回写、token 统计记 0（计次/冷却/负载均衡不受影响）。只对 OpenAI 协议端点生效。 -->
				<label style="display: flex; align-items: flex-start; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer; margin-top: 14px;">
					<input type="checkbox" id="provider-fastpass" style="width: 16px; height: 16px; padding: 0; margin: 2px 0 0; flex: none; accent-color: var(--accent-color);">
					<span>长流直通（防超长输出被 CF 掐断）—— 流式响应不逐 chunk 读取，代价：流内 model 不回写、token 统计缺失（次数统计不受影响）。只对 OpenAI 协议端点生效。</span>
				</label>

			<div class="form-group" style="margin-top: 14px;">
					<label for="provider-gemini-native">Gemini 协议（只对 googleapis 渠道有意义）</label>
					<select id="provider-gemini-native" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit;">
						<option value="auto">自动：地址含 googleapis 就走原生</option>
						<option value="on">强制走原生（工具调用更稳）</option>
						<option value="off">强制走 OpenAI 兼容端点</option>
					</select>
				</div>

				<div class="form-group" style="margin-top: 14px;">
					<label for="provider-price-input">计费单价（可选，用于估算成本）</label>
					<div style="display: flex; gap: 10px;">
						<input type="number" id="provider-price-input" step="0.0001" min="0" placeholder="输入价 $/百万 tokens">
						<input type="number" id="provider-price-output" step="0.0001" min="0" placeholder="输出价 $/百万 tokens">
					</div>
					<div class="section-note" style="margin-top: 6px;">留空 = 用上游价（OpenRouter 这类会自动带回）或全局粗估。Gemini / Agnes 这类上游不公开价格，手填后统计看板的「估算成本」更准。两格都填 <b>0</b> = 标记为「免费渠道」，看板成本按 0 计。</div>
				</div>

				<div id="provider-test-result" style="display: none;">
					<div id="provider-test-summary" style="padding: 12px 16px; border-radius: 10px; font-size: 13px; line-height: 1.7; white-space: pre-wrap; word-break: break-word; overflow-wrap: break-word; border: 1px solid transparent;"></div>
					<details id="provider-test-raw-wrap" style="display: none; margin-top: 10px;">
						<summary style="font-size: 12px; color: var(--text-muted); cursor: pointer;">查看原始响应</summary>
						<pre id="provider-test-raw" style="margin: 8px 0 0; padding: 10px 12px; border-radius: 10px; background-color: var(--section-item-bg); border: 1px solid var(--border-color); font-size: 11px; line-height: 1.6; white-space: pre-wrap; word-break: break-all; max-height: 30vh; overflow-y: auto;"></pre>
					</details>
				</div>
			</div>

			<div class="modal-footer">
				<button class="btn btn-success" onclick="testProvider()" id="btn-test-provider">测试连通</button>
				<button class="btn btn-primary" onclick="saveProvider()">保存渠道</button>
			</div>
		</div>
	</div>

	<!-- Modal: Provider Model Health -->
	<div class="modal-overlay" id="provider-health-modal">
		<div class="modal-card" style="max-width: 680px; padding: 24px;">
			<div class="modal-header">
				<h3 id="health-modal-title">模型可用性</h3>
				<button onclick="closeProviderHealthModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<div class="modal-body">
				<div class="section-note" id="health-endpoint"></div>
				<table>
					<thead>
						<tr>
							<th>模型</th>
							<th style="width: 76px;">状态</th>
							<th style="width: 76px;">耗时</th>
							<th>详情</th>
							<th style="width: 110px;">操作</th>
						</tr>
					</thead>
					<tbody id="health-table-body">
						<!-- Health rows -->
					</tbody>
				</table>
				<div class="section-note">逐个向该渠道发一句 ping。结果会记在渠道上，渠道列表里模型名前面的圆点即为最近一次结果（灰=未测、绿=正常、红=失败）。单次最多测 12 个模型。</div>
				<div style="background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 12px 14px; border-radius: 10px; font-size: 12.5px; color: var(--warning-color); line-height: 1.6; margin-top: 12px;"><strong>注意：</strong>测试会<b>真实调用上游</b> —— 既消耗上游额度，也会计入「调用配额」的已用量。上游是按总请求数算额度的，测试同样占额度，所以别频繁点。</div>
			</div>
			<div class="modal-footer">
				<button class="btn btn-secondary" onclick="closeProviderHealthModal()">关闭</button>
				<button class="btn btn-success" id="btn-test-all-models" onclick="testAllProviderModels()" title="会消耗上游额度，并计入调用配额">测试全部模型</button>
			</div>
		</div>
	</div>

	<!-- Modal: 模型规格（上游元信息：上下文 / 最大输出 / 价格 / 能力） -->
	<div class="modal-overlay" id="model-spec-modal">
		<div class="modal-card" style="max-width: 920px; padding: 24px;">
			<div class="modal-header">
				<h3 id="model-spec-title">模型规格</h3>
				<button onclick="closeModelSpecModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<div class="modal-body">
				<div class="section-note" id="model-spec-endpoint"></div>
				<div id="model-spec-content" style="max-height: 52vh; overflow: auto;"></div>
				<div class="section-note" style="margin-top: 10px;">规格来自上游 <code>/models</code> 接口，能否拿到取决于上游。<b>价格仅供参考</b>。<code>—</code> = 上游未提供该字段。</div>
			</div>
			<div class="modal-footer">
				<button class="btn btn-secondary" onclick="closeModelSpecModal()">关闭</button>
				<button class="btn btn-secondary" id="btn-spec-refresh" onclick="reloadModelSpecs()">重新拉取</button>
			</div>
		</div>
	</div>

	<!-- Modal: Runtime Mode -->
	<div class="modal-overlay" id="runtime-modal">
		<div class="modal-card" style="max-width: 480px;">
			<div class="modal-header">
				<h3>运行模式</h3>
				<button onclick="closeRuntimeModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>

			<div class="modal-body">
				<label style="display: flex; align-items: center; gap: 10px; font-size: 14px; cursor: pointer;">
					<input type="checkbox" id="cf-pool-toggle" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);">
					启用 Cloudflare 账号池
				</label>
				<div class="section-note">关闭后即为纯第三方反代模式：不再合并 CF 预设映射、模型列表不列 CF 模型，侧边栏「概览」分组一并隐藏。</div>

				<div class="form-group" style="margin-bottom: 0;">
					<label for="default-provider-select">默认渠道（未命中任何映射的模型名原样转发到这里，优先级高于 CF 兜底）</label>
					<select id="default-provider-select" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit;">
						<option value="">不设置</option>
					</select>
				</div>

				<div id="runtime-hint" style="font-size: 12px; color: var(--text-muted); line-height: 1.6;"></div>
			</div>

			<div class="modal-footer">
				<button class="btn btn-secondary" onclick="closeRuntimeModal()">取消</button>
				<button class="btn btn-primary" onclick="saveRuntimeMode()">保存</button>
			</div>
		</div>
	</div>


	<!-- Modal: Quota Group -->
	<div class="modal-overlay" id="quota-modal">
		<div class="modal-card" style="max-width: 760px; padding: 24px;">
			<div class="modal-header">
				<h3 id="quota-modal-title">新建配额组</h3>
				<button onclick="closeQuotaModal()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">
					<svg style="width: 20px; height: 20px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
				</button>
			</div>
			<input type="hidden" id="quota-id-edit">

			<div class="modal-body">
				<!-- 组名独占一行（原来和「重置」三件套挤在一个 2×2 网格里）；
				     标题直接用那句调用说明，不再另写「组名」二字（2026-10-10 用户要求） -->
				<div class="form-group">
					<label for="quota-name" id="quota-name-call">客户端调用时模型名填 <code id="quota-name-call-code" style="cursor: pointer;" onclick="copyQuotaCallName()" title="点击复制">TT:&lt;组名&gt;</code></label>
					<input type="text" id="quota-name" placeholder="如: gemini-free（不能含 / 和前缀 TT:）" oninput="updateQuotaNamePreview()">
				</div>

				<!-- ★ 成员区排在「重置周期/时刻/时区」**之前**（2026-10-10 用户要求两块对调）：
				     弹窗一打开就先看见成员表格，而不是先看一屏说明性字段 -->
				<div style="display: flex; align-items: center; justify-content: space-between;">
					<span style="font-size: 13px; color: var(--text-muted);">成员 —— 按消耗比例自动分摊（谁用得少用谁），某个成员报错会自动换下一个</span>
					<button class="btn btn-secondary" onclick="addQuotaMemberRow()" style="padding: 6px 12px; font-size: 12px;">添加成员</button>
				</div>

				<div style="display: grid; grid-template-columns: 22px 52px 1.2fr 1.5fr 78px 72px 72px 34px; gap: 8px; font-size: 12px; color: var(--text-muted);">
					<span title="拖动这一列可排序；也可直接改数字（越小越先用）">⠿</span><span title="数字越小越先用；同一层内仍然按负载均衡分摊">优先</span><span>渠道</span><span>上游模型名</span><span>上限</span><span>单位</span><span>状态</span><span></span>
				</div>
				<div id="quota-members" style="display: flex; flex-direction: column; gap: 10px;"></div>

				<!-- 重置基准三件套并成一行（组名搬走后不必再占 2×2）；
				     中间「重置时刻」给固定 200px，好让「重置时刻（该时区的本地时间）」这个长标签不折行
				     —— 标签一折行，本行三个输入框就不在同一水平线上了 -->
				<div style="display: grid; grid-template-columns: minmax(0, 1fr) 200px minmax(0, 1fr); gap: 16px;">
					<div class="form-group">
						<label for="quota-period">重置周期</label>
						<div class="quota-combo" id="quota-period-combo">
							<div class="quota-combo-row">
								<input type="text" id="quota-period" autocomplete="off"
									placeholder="留空 = 每天；或自定义日期"
									oninput="updateQuotaResetPreview()" onkeydown="quotaComboKey(event, 'quota-period')">
								<button type="button" class="quota-combo-toggle" title="展开预设"
									onclick="quotaComboToggle('quota-period')">▾</button>
							</div>
							<div class="quota-combo-menu" id="quota-period-menu">
								<button type="button" class="quota-combo-item" data-value="每天">每天</button>
								<button type="button" class="quota-combo-item" data-value="每月 1 日">每月 1 日</button>
								<button type="button" class="quota-combo-item" data-value="每月 15 日">每月 15 日</button>
								<button type="button" class="quota-combo-item" data-value="每月 25 日">每月 25 日</button>
							</div>
						</div>
					</div>
					<div class="form-group">
						<label for="quota-reset-time">重置时刻（该时区的本地时间）</label>
						<input type="time" id="quota-reset-time" value="00:00" onchange="updateQuotaResetPreview()">
					</div>
					<div class="form-group">
						<label for="quota-reset-tz">基准时区</label>
						<div class="quota-combo" id="quota-reset-tz-combo">
							<div class="quota-combo-row">
								<input type="text" id="quota-reset-tz" autocomplete="off"
									placeholder="留空 = UTC；或自定义偏移"
									oninput="updateQuotaResetPreview()" onkeydown="quotaComboKey(event, 'quota-reset-tz')">
								<button type="button" class="quota-combo-toggle" title="展开预设"
									onclick="quotaComboToggle('quota-reset-tz')">▾</button>
							</div>
							<div class="quota-combo-menu" id="quota-reset-tz-menu">${resetTzMenu}</div>
						</div>
					</div>
				</div>

				<div id="quota-reset-preview" style="font-size: 12px; color: var(--text-muted); line-height: 1.6;"></div>

				<label style="display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer;">
					<input type="checkbox" id="quota-status-active" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);">
					启用这个配额组
				</label>
				<label style="display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer; margin-top: 8px;">
					<input type="checkbox" id="quota-sticky" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);">
					会话粘性（默认开启；同一个会话尽量固定用同一个成员 —— 多轮对话 / 工具调用更稳，代价是分摊略不均）
				</label>

				<!-- 长流直通（2026-10-10）：免费档 CPU 10ms 会掐断超长输出。直通 = 组内成员的流式响应
				     不逐 chunk 读取（选号/冷却/计次照旧），代价：token 统计记 0。只对 OpenAI 协议端点生效。 -->
				<label style="display: flex; align-items: flex-start; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer;">
					<input type="checkbox" id="quota-fastpass" style="width: 16px; height: 16px; padding: 0; margin: 2px 0 0; flex: none; accent-color: var(--accent-color);">
					<span>长流直通（防超长输出被 CF 掐断）—— 流式响应不逐 chunk 读取，代价：token 统计记 0（次数/冷却/负载均衡不受影响）。只对 OpenAI 协议端点生效。</span>
				</label>

				<!-- 上游冷却放这里（而不是顶部）：它是「高级/次要」设置，放顶上会把「添加成员」顶到折叠线以下
				     —— 打开弹窗第一眼看不到正事（2026-10-10 用户反馈） -->
				<div class="form-group">
					<label for="quota-cooldown">上游冷却时长（秒）—— 填一个数，拥塞与 429 限流都按它</label>
					<input type="number" id="quota-cooldown" min="0" max="600" step="1" autocomplete="off"
						placeholder="留空或 0 = 各自默认（拥塞 20 秒 / 429 限流 60 秒）">
					<div style="font-size:11px; color: var(--text-muted); line-height: 1.5;">冷却中的成员不参与选号 —— 成员少的组建议调短（如填 5，则两类都 5 秒）。</div>
				</div>

				<div id="quota-modal-hint" style="font-size: 12px; color: var(--text-muted); line-height: 1.6;"></div>
			</div>

			<div class="modal-footer" style="display: flex; gap: 12px; justify-content: flex-end;">
				<button class="btn btn-secondary" onclick="closeQuotaModal()">取消</button>
				<button class="btn btn-primary" onclick="saveQuotaGroup()">保存配额组</button>
			</div>
		</div>
	</div>

	<script>
		let currentTab = '${defaultTab}';
		let historyChart = null;
		let modelsChart = null;
		const defaultMappings = ${JSON.stringify(DEFAULT_MODEL_MAP)};
		// 各 CF 模型的额度消耗档位（后端 MODEL_COST_TIER 注入），只用于表格里的徽标提示。
		// 注意：必须在 let customMappings 之前声明 —— _verify_mapping_ui.mjs 从那行开始抽代码段，
		// 而这里是模板内插语法，抽到沙箱里会变成非法 JS。
		const mappingCostTier = ${JSON.stringify(MODEL_COST_TIER)};
		let customMappings = {};
		let mappingInvalid = {};
		let mappingCfEnabled = true;
		// 逐条映射开关：已停用的源名集合（loadSettings 从 /api/settings 拿）
		let disabledMappings = new Set();
		// 长流直通的映射源名集合（loadSettings 从 /api/settings 拿）
		let fastPassMappings = new Set();
		// 分组折叠状态：cf=null 表示首次加载还没初始化（loadSettings 里按当前模式决定默认是否折叠，
		// 之后尊重用户手动展开）；provider 组默认收起。
		// 模型映射的两个分组（CF / 第三方渠道）**默认全部折叠**（2026-10-09 用户要求）。
		// 以前 cf 组会按「当前模式是否生效」自动展开，导致进页面就看到一长条 —— 现统一默认收起。
		const mappingGroupCollapsed = { cf: true, provider: true };
		let providersCache = [];
		let runtimeState = { cfPoolEnabled: true, defaultProviderId: '', accounts: 0 };
		let healthProviderId = null;
		let healthResults = {};

		// 主流服务商的 OpenAI 兼容端点。模型名是示例值（各家改名很勤），填进去后请按官方文档核对。
		const providerPresets = [
			// 模型名会随时间过期（Gemini 的 2.5 系列已对新用户关闭），填的是当前可用的示例值；
			// 拿不准就用弹窗里的「从上游拉取模型列表」
			{ key: 'gemini', label: 'Google Gemini（官方 OpenAI 兼容层）', baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai', name: 'gemini', models: ['gemini-3.8-flash'] },
			// 2026-10-09 NVIDIA NIM 适配配套：build.nvidia.com 免费托管档（40 RPM/每 key）。
			// key 是 nvapi- 开头，在 build.nvidia.com 免费领；模型名带斜杠是目录 ID 的正常形态，
			// 更多模型用弹窗里的「从上游拉取模型列表」一键补全。
			{ key: 'nvidia', label: 'NVIDIA NIM', baseUrl: 'https://integrate.api.nvidia.com/v1', name: 'nvidia', models: ['nvidia/llama-3.3-nemotron-super-49b-v1.5', 'z-ai/glm-5.1', 'qwen/qwen3-235b-a22b'] },
			{ key: 'claude', label: 'Anthropic Claude（官方 OpenAI 兼容层）', baseUrl: 'https://api.anthropic.com/v1', name: 'claude', models: ['claude-sonnet-4-5'] },
			{ key: 'openai', label: 'OpenAI', baseUrl: 'https://api.openai.com/v1', name: 'openai', models: ['gpt-4o', 'gpt-4o-mini'] },
			{ key: 'deepseek', label: 'DeepSeek', baseUrl: 'https://api.deepseek.com/v1', name: 'deepseek', models: ['deepseek-chat', 'deepseek-reasoner'] },
			{ key: 'openrouter', label: 'OpenRouter', baseUrl: 'https://openrouter.ai/api/v1', name: 'openrouter', models: ['openai/gpt-4o'] },
			{ key: 'groq', label: 'Groq', baseUrl: 'https://api.groq.com/openai/v1', name: 'groq', models: ['llama-3.3-70b-versatile'] },
			{ key: 'moonshot', label: 'Moonshot / Kimi', baseUrl: 'https://api.moonshot.cn/v1', name: 'moonshot', models: ['moonshot-v1-32k'] },
			{ key: 'zhipu', label: '智谱 GLM', baseUrl: 'https://open.bigmodel.cn/api/paas/v4', name: 'zhipu', models: ['glm-4-flash', 'glm-4-plus'] },
			{ key: 'dashscope', label: '通义千问 DashScope', baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1', name: 'qwen', models: ['qwen-plus', 'qwen-max'] },
			{ key: 'ark', label: '火山方舟 豆包', baseUrl: 'https://ark.cn-beijing.volces.com/api/v3', name: 'doubao', models: ['doubao-seed-1-6-250615'] },
			{ key: 'siliconflow', label: 'SiliconFlow 硅基流动', baseUrl: 'https://api.siliconflow.cn/v1', name: 'siliconflow', models: ['deepseek-ai/DeepSeek-V3'] },
			{ key: 'xai', label: 'xAI Grok', baseUrl: 'https://api.x.ai/v1', name: 'grok', models: ['grok-3-mini'] },
			{ key: 'mistral', label: 'Mistral', baseUrl: 'https://api.mistral.ai/v1', name: 'mistral', models: ['mistral-large-latest'] },
			{ key: 'custom', label: '自定义 / 其他 OpenAI 兼容端点', baseUrl: '', name: '', models: [] }
		];

		function initProviderPresets() {
			const select = document.getElementById('provider-preset');
			if (!select || select.options.length) return;
			select.innerHTML = '';
			const blank = document.createElement('option');
			blank.value = '';
			blank.textContent = '（请选择）';
			select.appendChild(blank);
			providerPresets.forEach(p => {
				const opt = document.createElement('option');
				opt.value = p.key;
				opt.textContent = p.label;
				select.appendChild(opt);
			});
		}

		function applyProviderPreset(key) {
			const preset = providerPresets.find(p => p.key === key);
			if (!preset) return;
			if (preset.baseUrl) document.getElementById('provider-base-url').value = preset.baseUrl;
			if (preset.models.length) {
				document.getElementById('provider-models').value = preset.models.join(String.fromCharCode(10));
			}
			const nameInput = document.getElementById('provider-name');
			if (!nameInput.value.trim() && preset.name) nameInput.value = preset.name;
		}

		// Toast Helper
		${COMMON_TOAST_JS}

		function renderUsageDetails(data) {
			let totalUsageToday = 0;
			let totalLimit = data.length * 10000;
			let historyData = {};
			let modelsToday = {};

			const usageList = document.getElementById('accounts-usage-list');

			// 记录刷新前已有卡片的最后更新时间戳
			const previousTimestamps = new Map();
			usageList.querySelectorAll('.section-card').forEach(card => {
				const id = card.dataset.id;
				const ts = parseInt(card.dataset.lastUpdated || '0', 10);
				if (id) previousTimestamps.set(id, ts);
			});

			usageList.innerHTML = '';

			if (data.length === 0) {
				usageList.innerHTML = '<div style="color: var(--text-muted); font-size:14px; text-align:center; padding: 20px; width: 100%;">没有绑定的账号，请前往“账号管理”添加账号。</div>';
				return;
			}

			data.forEach(account => {
				totalUsageToday += account.usageToday;

				// Percentage formatted to 2 decimal places
				const percentage = Math.min(100, Number(((account.usageToday / 10000) * 100).toFixed(2)));
				const warningClass = account.status === 'error' ? 'badge-danger' : (account.status === 'pending' ? 'badge-info' : (percentage >= 90 ? 'badge-warning' : 'badge-success'));
				const statusText = account.status === 'error' ? '连接异常' : (account.status === 'pending' ? '待刷新' : (percentage >= 100 ? '用尽 (10k)' : '正常运行'));
				
				// Usage rounded up (Math.ceil)
				const roundedUsage = Math.ceil(account.usageToday);
				
				const item = document.createElement('div');
				const isRefreshed = previousTimestamps.has(account.id) && previousTimestamps.get(account.id) !== account.lastUpdated;
				item.className = 'section-card' + (isRefreshed ? ' card-update-flash' : '');
				item.dataset.id = account.id;
				item.dataset.lastUpdated = account.lastUpdated || 0;
				item.style.padding = '20px';
				item.style.backgroundColor = 'rgba(255,255,255,0.01)';
				item.innerHTML = \`
					<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 12px; gap: 12px;">
						<div style="min-width: 0; flex: 1; display: flex; align-items: center; gap: 8px;">
							<strong style="font-size:15px; font-weight:600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 0 1 auto;" title="\${sen(account.name)}">\${sen(account.name)}</strong>
							<span style="font-size:12px; color: var(--text-muted); font-family: monospace; white-space: nowrap; flex-shrink: 0;">(\${account.accountId.substring(0,6)}...\${account.accountId.substring(account.accountId.length-4)})</span>
						</div>
						<span class="badge \${warningClass}" style="flex-shrink: 0;">\${statusText}</span>
					</div>
					<div class="progress-container">
						<div class="progress-bar" style="width: \${percentage}%;"></div>
					</div>
					<div style="display:flex; justify-content:space-between; font-size:12px; color: var(--text-muted); margin-top: 6px;">
						<span>今日已用: \${roundedUsage.toLocaleString()} / 10,000 Neurons</span>
						<span>\${percentage.toFixed(2)}%</span>
					</div>
					\${account.error ? \`<div style="color: var(--danger-color); font-size:11px; margin-top: 8px; background: rgba(239,68,68,0.08); padding: 8px 12px; border-radius: 6px; border: 1px solid rgba(239,68,68,0.12);">错误信息: \${sen(account.error)}</div>\` : ''}
				\`;
				usageList.appendChild(item);

				if (account.history) {
					account.history.forEach(h => {
						historyData[h.date] = (historyData[h.date] || 0) + h.neurons;
					});
				}

				if (account.modelsToday) {
					account.modelsToday.forEach(m => {
						modelsToday[m.model] = (modelsToday[m.model] || 0) + m.neurons;
					});
				}
			});

			// Top stats formatting (Usage rounded up, Percentage 2 decimals)
			const roundedTotalUsageToday = Math.ceil(totalUsageToday);
			document.getElementById('stat-total-neurons').innerText = roundedTotalUsageToday.toLocaleString();
			document.getElementById('stat-accounts-count').innerText = data.length;
			
			const overallPercentage = totalLimit > 0 ? Math.min(100, Number(((totalUsageToday / totalLimit) * 100).toFixed(2))) : 0;
			document.getElementById('stat-neurons-progress').style.width = overallPercentage + '%';
			document.getElementById('stat-neurons-desc').innerText = \`\${roundedTotalUsageToday.toLocaleString()} / \${totalLimit.toLocaleString()} Neurons (\${overallPercentage.toFixed(2)}%)\`;
			
			const costSaved = (totalUsageToday / 1000) * 0.011;
			document.getElementById('stat-cost-saving').innerText = '$' + costSaved.toFixed(2);

			const dates = Object.keys(historyData).sort();
			const neuronsData = dates.map(d => historyData[d]);
			renderHistoryChart(dates, neuronsData);

			const models = Object.keys(modelsToday);
			const modelsNeurons = models.map(m => modelsToday[m]);
			renderModelsChart(models, modelsNeurons);
		}

		let isRefreshingUsage = false;

		async function loadUsageDetails(isManual = false) {
			// 如果已经在刷新中，则直接返回，避免并发请求
			if (isRefreshingUsage) return;

			const now = Date.now();
			const lastFetchedRaw = localStorage.getItem('cache_usage_details_last_fetched');
			let lastFetched = lastFetchedRaw ? parseInt(lastFetchedRaw, 10) : 0;

			// 优先从浏览器 localStorage 读取并渲染上次缓存的数据
			const cachedDataRaw = localStorage.getItem('cache_accounts_usage');
			if (cachedDataRaw) {
				try {
					const cachedData = JSON.parse(cachedDataRaw);
					renderUsageDetails(cachedData);
				} catch (e) {
					console.error('Error parsing cached usage details:', e);
				}
			}
			const cachedKeysCount = localStorage.getItem('cache_keys_count');
			if (cachedKeysCount) {
				document.getElementById('stat-keys-count').innerText = cachedKeysCount;
			}

			// 更新文字显示
			updateLastUpdatedText(lastFetched);

			// 如果不是手动刷新，且最后更新时间在 15 分钟以内，则直接使用缓存，不发起 API 请求
			if (!isManual && lastFetched && (now - lastFetched) < 15 * 60 * 1000) {
				return;
			}

			const btn = document.getElementById('btn-refresh-usage');
			let originalBtnText = '';
			if (btn) {
				originalBtnText = btn.innerHTML;
				btn.disabled = true;
				btn.innerHTML = '<span class="spinner"></span> 刷新中...';
			}

			isRefreshingUsage = true;

			try {
				const res = await apiFetch('/api/accounts/usage');
				const data = await res.json();
				
				// 渲染最新的实时数据
				renderUsageDetails(data);
				
				// 保存/更新本地缓存
				localStorage.setItem('cache_accounts_usage', JSON.stringify(data));

				// 记录更新时间戳，并更新文字
				localStorage.setItem('cache_usage_details_last_fetched', now);
				updateLastUpdatedText(now);

				// 刷新并缓存 API 密钥数
				const keysRes = await apiFetch('/api/keys');
				const keys = await keysRes.json();
				document.getElementById('stat-keys-count').innerText = keys.length;
				localStorage.setItem('cache_keys_count', keys.length);

			} catch (e) {
				console.error(e);
			} finally {
				isRefreshingUsage = false;
				if (btn) {
					btn.disabled = false;
					btn.innerHTML = originalBtnText;
				}
			}
		}

		function updateLastUpdatedText(timestamp) {
			const label = document.getElementById('txt-last-updated');
			if (!label) return;
			if (!timestamp) {
				label.innerText = '从未更新';
				return;
			}
			const date = new Date(timestamp);
			const yyyy = date.getFullYear();
			const MM = String(date.getMonth() + 1).padStart(2, '0');
			const dd = String(date.getDate()).padStart(2, '0');
			const hh = String(date.getHours()).padStart(2, '0');
			const mm = String(date.getMinutes()).padStart(2, '0');
			const ss = String(date.getSeconds()).padStart(2, '0');
			label.innerText = '最后更新: ' + yyyy + '-' + MM + '-' + dd + ' ' + hh + ':' + mm + ':' + ss;
		}

		function initTheme() {
			const savedTheme = localStorage.getItem('theme');
			if (savedTheme) {
				document.documentElement.setAttribute('data-theme', savedTheme);
			} else {
				const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
				const defaultTheme = systemPrefersDark ? 'dark' : 'light';
				document.documentElement.setAttribute('data-theme', defaultTheme);
			}
			updateThemeIcons();
		}

		function toggleTheme() {
			const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
			const newTheme = currentTheme === 'light' ? 'dark' : 'light';
			document.documentElement.setAttribute('data-theme', newTheme);
			localStorage.setItem('theme', newTheme);
			updateThemeIcons();
			if (currentTab === 'overview') {
				loadUsageDetails();
			}
		}

		function updateThemeIcons() {
			const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
			const sunIcons = document.querySelectorAll('.theme-icon-sun');
			const moonIcons = document.querySelectorAll('.theme-icon-moon');
			if (currentTheme === 'light') {
				sunIcons.forEach(el => el.style.display = 'none');
				moonIcons.forEach(el => el.style.display = 'block');
			} else {
				sunIcons.forEach(el => el.style.display = 'block');
				moonIcons.forEach(el => el.style.display = 'none');
			}
		}

		initTheme();

		// ---------- 第三方渠道调用统计（与 CF 官方用量是两条独立数据源） ----------
		let statsRange = 'today';

		function setStatsRange(r) {
			statsRange = r;
			['today', '7d', 'all'].forEach(x => {
				const btn = document.getElementById('stats-range-' + x);
				if (!btn) return;
				btn.classList.toggle('btn-primary', x === r);
				btn.classList.toggle('btn-secondary', x !== r);
			});
			refreshProviderStats();
		}

		// 统计刷新：手动按钮与范围切换共用。进行中不叠加（D1 慢查询时避免请求堆积）。
		// 注意：不做自动轮询 —— 每次刷新都是一次 D1 查询，轮询会把免费额度烧掉；想看新数据点「↻ 刷新」。
		let statsRefreshing = false;
		async function refreshProviderStats() {
			if (statsRefreshing) return;
			statsRefreshing = true;
			const btn = document.getElementById('stats-refresh');
			if (btn) { btn.disabled = true; btn.textContent = '刷新中…'; }
			try {
				await loadProviderStats();
			} finally {
				statsRefreshing = false;
				if (btn) { btn.disabled = false; btn.textContent = '↻ 刷新'; }
			}
		}

		function setTextById(id, text) {
			const el = document.getElementById(id);
			if (el) el.innerText = text;
		}

		// 累计 Token 数自适应缩写（M/B/T/P/E）：自动选最合适单位，短数字（<1e6，即 ≤6 位）显示完整逗号格式，
		// 更长或累计到极大时自动缩写——开源后调用量大、累计值可能极巨，故单位多备两档（P=1e15 / E=1e18）。
		// 精确值仍可经 title 悬停查看。
		// 累计 Token 数万进制自然读数：7 位以内（<1e7，卡片可单行容纳）显示完整逗号格式，
		// 8 位起缩写（百万/千万/亿/十亿/…/百亿亿），系数恒 1.00~9.99，不撑破卡片。精确值仍可 title 悬停。
		function fmtTokenCompact(n) {
			n = Number(n) || 0;
			if (n < 1e7) return n.toLocaleString();
			const units = [
				['百亿亿', 1e18], ['十亿亿', 1e17], ['亿亿', 1e16],
				['千万亿', 1e15], ['百万亿', 1e14], ['十万亿', 1e13],
				['万亿', 1e12], ['千亿', 1e11], ['百亿', 1e10], ['十亿', 1e9],
				['亿', 1e8], ['千万', 1e7], ['百万', 1e6],
			];
			for (const [sym, div] of units) {
				if (n >= div) {
					const str = String(Number((n / div).toFixed(2)));
					return str + sym;
				}
			}
			return n.toLocaleString();
		}

		// 「最近一次延迟」的小字时间（本月-日 时:分，本地时区）
		function fmtStatsLastAt(iso) {
			const d = new Date(iso);
			if (isNaN(d.getTime())) return '';
			const pad = n => (n < 10 ? '0' + n : '' + n);
			return (d.getMonth() + 1) + '-' + d.getDate() + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
		}

		// 延迟单元格：平均（口径含探测）为主行，下方小字显示最近一次（每次调用覆盖，含「重测」）
		function statsLatencyCell(avgMs, req, lastMs, lastAt) {
			if (!req) return '—';
			const main = avgMs ? avgMs + ' ms' : '<span style="color: var(--danger-color);">超时</span>';
			if (!lastAt) return main;
			return main + '<div style="font-size:11px; color: var(--text-muted); margin-top:2px;">最近 ' + (lastMs || 0) + ' ms · ' + fmtStatsLastAt(lastAt) + '</div>';
		}

		function toggleStatsModels(pid) {
			document.querySelectorAll('.stats-model-row').forEach(tr => {
				if (tr.dataset.parent !== pid) return;
				tr.style.display = tr.style.display === 'none' ? '' : 'none';
			});
		}

		// 看板模型行「隐藏 / 恢复」（B 方案：显式隐藏名单落 config.hiddenModels）
		async function toggleHideModel(btn) {
			const pid = btn.dataset.pid, model = btn.dataset.model, action = btn.dataset.action;
			if (!model) return;
			try {
				const res = await apiFetch('/api/hidden-models', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ model, action })
				});
				const data = await res.json();
				if (!data.success) throw new Error(data.error || '操作失败');
				showToast(action === 'add' ? ('已隐藏模型：' + model) : ('已恢复模型：' + model));
				refreshProviderStats();
			} catch (e) {
				showToast('操作失败：' + e.message, 'error');
			}
		}

		// 批量隐藏孤儿模型：统计里有、但已不在任何渠道 models 列表中的失效模型
		async function cleanupOrphanModels() {
			try {
				const res = await apiFetch('/api/hidden-models/cleanup', { method: 'POST' });
				const data = await res.json();
				if (!data.success) throw new Error(data.error || '操作失败');
				const n = (data.hidden || []).length;
				showToast(n ? ('已隐藏 ' + n + ' 个孤儿模型') : '没有需要隐藏的孤儿模型');
				refreshProviderStats();
			} catch (e) {
				showToast('操作失败：' + e.message, 'error');
			}
		}

		let providerTrendChart = null;
		let providerDonutChart = null;
		const PROVIDER_CHART_COLORS = ['#6366f1', '#a855f7', '#ec4899', '#10b981', '#f59e0b', '#3b82f6'];

		// 近 7 日消耗 Token 走势：每个模型一条线
		function renderProviderTrendChart(trend) {
			const canvas = document.getElementById('providerTrendChart');
			if (!canvas) return;
			if (providerTrendChart) { providerTrendChart.destroy(); providerTrendChart = null; }
			const days = (trend && trend.days) || [];
			const series = (trend && trend.series) || [];
			const isLight = document.documentElement.getAttribute('data-theme') === 'light';
			const gridColor = isLight ? 'rgba(0, 0, 0, 0.05)' : 'rgba(255, 255, 255, 0.05)';
			const textColor = isLight ? '#64748b' : '#94a3b8';
			providerTrendChart = new Chart(canvas.getContext('2d'), {
				type: 'line',
				data: {
					labels: days,
					datasets: series.map((s, i) => ({
						label: (s.model || '(未记录)').split('/').pop(),
						data: s.data || [],
						borderColor: PROVIDER_CHART_COLORS[i % PROVIDER_CHART_COLORS.length],
						backgroundColor: PROVIDER_CHART_COLORS[i % PROVIDER_CHART_COLORS.length],
						borderWidth: 2,
						tension: 0.3,
						fill: false,
						pointRadius: 3,
						pointHoverRadius: 5
					}))
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					plugins: {
						legend: {
							display: series.length > 0,
							position: 'bottom',
							labels: { color: textColor, boxWidth: 12, padding: 12, font: { size: 11 } }
						}
					},
					scales: {
						y: { grid: { color: gridColor }, ticks: { color: textColor } },
						x: { grid: { display: false }, ticks: { color: textColor } }
					}
				}
			});
		}

		// 今日模型消耗占比：按 token 分块，图例 = 模型名 + 占比 + 请求数
		function renderProviderDonut(todayByModel) {
			const canvas = document.getElementById('providerDonutChart');
			const donutWrapper = document.getElementById('provider-donut-wrapper');
			const legendContainer = document.getElementById('provider-donut-legend');
			const placeholder = document.getElementById('provider-donut-placeholder');
			if (!canvas || !donutWrapper || !legendContainer || !placeholder) return;
			if (providerDonutChart) { providerDonutChart.destroy(); providerDonutChart = null; }
			legendContainer.innerHTML = '';

			const list = (todayByModel || []).filter(m => (m.tokens || 0) > 0 || (m.req || 0) > 0);
			if (!list.length) {
				donutWrapper.style.display = 'none';
				placeholder.style.display = 'flex';
				return;
			}
			donutWrapper.style.display = 'flex';
			placeholder.style.display = 'none';

			const isLight = document.documentElement.getAttribute('data-theme') === 'light';
			const textColor = isLight ? '#64748b' : '#94a3b8';
			const borderColor = isLight ? '#ffffff' : '#1e293b';
			const total = list.reduce((a, m) => a + (m.tokens || 0), 0);

			providerDonutChart = new Chart(canvas.getContext('2d'), {
				type: 'doughnut',
				data: {
					labels: list.map(m => (m.model || '(未记录)').split('/').pop()),
					datasets: [{
						data: list.map(m => m.tokens || 0),
						backgroundColor: list.map((_, i) => PROVIDER_CHART_COLORS[i % PROVIDER_CHART_COLORS.length]),
						borderWidth: 2,
						borderColor: borderColor
					}]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					cutout: '70%',
					animation: { animateRotate: true, animateScale: true, duration: 1000, easing: 'easeOutQuart' },
					plugins: { legend: { display: false } }
				}
			});

			list.forEach((m, index) => {
				const color = PROVIDER_CHART_COLORS[index % PROVIDER_CHART_COLORS.length];
				const pct = total > 0 ? ((m.tokens || 0) / total * 100).toFixed(1) : '0.0';
				const item = document.createElement('div');
			item.style.cssText = 'display:flex; flex-direction:column; gap:2px; min-width:0; font-size:12px; color:' + textColor + ';';
			item.innerHTML = '<span style="display:flex; align-items:center; gap:6px; min-width:0;">'
				+ '<span style="width: 8px; height: 8px; border-radius: 50%; background-color: ' + color + '; flex-shrink: 0;"></span>'
				+ '<span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 auto; min-width: 0; font-weight: 500;" title="' + sen(m.model || '(未记录)') + '">' + sen((m.model || '(未记录)').split('/').pop()) + '</span></span>'
				+ '<span style="color: var(--text-muted); font-family: monospace; font-size: 11px; padding-left: 14px;">' + pct + '% · ' + (m.req || 0) + '次</span>';
				legendContainer.appendChild(item);
			});
		}

		async function loadProviderStats() {
			const disabledEl = document.getElementById('provider-stats-disabled');
			const bodyEl = document.getElementById('provider-stats-body');
			try {
				const incEl = document.getElementById('stats-include-inactive');
			const inc = (incEl && incEl.checked) ? '1' : '0';
			const res = await apiFetch('/api/provider-stats?range=' + encodeURIComponent(statsRange) + '&includeInactive=' + inc);
				const data = await res.json();

				// 未绑定 D1：只提示，不报错、不影响代理
				if (!data.enabled) {
					if (disabledEl) disabledEl.style.display = 'block';
					if (bodyEl) bodyEl.style.display = 'none';
					return;
				}
				if (disabledEl) disabledEl.style.display = 'none';
				if (bodyEl) bodyEl.style.display = 'block';

			const s = data.summary || {};
			const rangeText = statsRange === 'today' ? '今日' : (statsRange === '7d' ? '近 7 天' : '全部');
			setTextById('stats-okrate', s.req ? s.okRate + '%' : '—');
			setTextById('stats-okfail', rangeText + ' · 成功 ' + (s.ok || 0) + ' / 失败 ' + (s.fail || 0));
			setTextById('stats-avgms', s.req ? (s.avgMs ? s.avgMs + ' ms' : '超时') : '—');
		setTextById('stats-tokens', fmtTokenCompact(s.tokens || 0));
		const tokEl = document.getElementById('stats-tokens');
		if (tokEl) tokEl.title = (s.tokens || 0).toLocaleString() + ' tokens（累计，悬停查看精确值）';
		setTextById('stats-reasoning', '含思考 ' + fmtTokenCompact(s.reasoningTokens || 0));
			setTextById('stats-cost', '$' + (s.costEst != null ? s.costEst.toFixed(2) : '0.00'));

			renderProviderTrendChart(data.trend);
			renderProviderDonut(data.todayByModel || []);

			const tbody = document.getElementById('provider-stats-rows');
			if (!tbody) return;
			const list = data.providers || [];
			if (!list.length) {
				tbody.innerHTML = '<tr><td colspan="8" style="text-align:center; color: var(--text-muted); padding:24px;">该时间段内没有渠道调用记录</td></tr>';
				return;
			}
			tbody.innerHTML = list.map(p => {
				const head = '<tr>'
					+ '<td><code style="cursor:pointer;" title="展开 / 收起模型明细" data-pid="' + sen(p.id) + '" onclick="toggleStatsModels(this.dataset.pid)">' + sen(p.name || p.id) + '</code></td>'
					+ '<td>' + (p.req || 0) + '</td>'
					+ '<td>' + (p.ok || 0) + ' / ' + (p.fail || 0) + '</td>'
				+ '<td>' + (p.req ? p.okRate + '%' : '—') + '</td>'
				+ '<td>' + statsLatencyCell(p.avgMs, p.req, p.lastMs, p.lastAt) + '</td>'
					+ '<td>' + (p.tokens || 0).toLocaleString() + '</td>'
					+ '<td>' + (p.reasoningTokens || 0).toLocaleString() + '</td>'
					+ '<td>$' + (p.costEst != null ? p.costEst.toFixed(2) : '0.00') + '</td>'
					+ '</tr>';
			const models = (p.models || []).map(m => {
				const isHidden = !!m.hidden;
				const btn = '<button data-pid="' + sen(p.id) + '" data-model="' + sen(m.model || '') + '" data-action="' + (isHidden ? 'remove' : 'add') + '" onclick="toggleHideModel(this)" style="font-size:10px; padding:1px 6px; margin-left:6px; border:1px solid var(--border-color); background:transparent; color:' + (isHidden ? 'var(--warning-color)' : 'var(--text-muted)') + '; border-radius:6px; cursor:pointer;">' + (isHidden ? '恢复' : '隐藏') + '</button>';
				return '<tr class="stats-model-row" data-parent="' + sen(p.id) + '" style="display:none;">'
					+ '<td style="padding-left:36px; font-size:12.5px; color: var(--text-muted);">' + sen(m.model || '(未记录)') + (isHidden ? ' <span style="color:var(--warning-color);font-size:11px;">[已隐藏]</span>' : '') + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.req || 0) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.ok || 0) + ' / ' + (m.fail || 0) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.req ? Math.round(m.ok / m.req * 1000) / 10 + '%' : '—') + '</td>'
					+ '<td style="font-size:12.5px;">' + statsLatencyCell(m.avgMs, m.req, m.lastMs, m.lastAt) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.tokens || 0).toLocaleString() + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.reasoningTokens || 0).toLocaleString() + '</td>'
					+ '<td style="font-size:12.5px;">$' + (m.costEst != null ? m.costEst.toFixed(2) : '0.00') + btn + '</td>'
					+ '</tr>';
			}).join('');
				return head + models;
			}).join('');
			} catch (e) {
				console.error(e);
				if (disabledEl) {
					disabledEl.style.display = 'block';
					disabledEl.innerText = '统计加载失败：' + e.message;
				}
			}
		}

		window.onload = function() {
			const baseUrl = window.location.origin + '/v1';
			const openaiUrl = window.location.origin + '/v1/chat/completions';
			const anthropicUrl = window.location.origin + '/v1/messages';
			// Base URL 也写真实域名（之前是硬编码的占位符，容易被照抄错）
			const baseUrlEl = document.getElementById('access-base-url');
			const baseUrlCopy = document.getElementById('access-base-url-copy');
			if (baseUrlEl) baseUrlEl.textContent = baseUrl;
			if (baseUrlCopy) baseUrlCopy.dataset.endpointUrl = baseUrl;
			const openaiUrlEl = document.getElementById('openai-endpoint-url');
			const anthropicUrlEl = document.getElementById('anthropic-endpoint-url');
			if (openaiUrlEl) {
				openaiUrlEl.dataset.endpointUrl = openaiUrl;
				openaiUrlEl.textContent = openaiUrl;
			}
			if (anthropicUrlEl) {
				anthropicUrlEl.dataset.endpointUrl = anthropicUrl;
				anthropicUrlEl.textContent = anthropicUrl;
			}
			initMapTargetCombo();
			initQuotaModelCombo();
			initQuotaDrag();
			loadRuntimeState();
			loadAccessSample();
			if (!document.body.classList.contains('cf-off')) {
				loadUsageDetails();
			}
			// 渠道统计两种模式都要加载（CF 段关闭时它仍然有内容）；默认看「今日」
			setStatsRange('today');
		};

		function toggleSidebar() {
			document.getElementById('sidebar').classList.toggle('active');
		}

		async function logout() {
			const res = await fetch('/api/auth/logout', { method: 'POST' });
			if (res.ok) {
				showToast('已安全退出登录');
				setTimeout(() => {
					window.location.href = '/';
				}, 800);
			}
		}

		function switchTab(tabName) {
			if (tabName === currentTab) return;
			// 账号池关闭时对应界面被 CSS 隐藏，这里兜一下，避免切到看不见的 Tab
			const menuEl = document.getElementById('menu-' + tabName);
			const tabEl = document.getElementById('tab-' + tabName);
			if (!menuEl || !tabEl) return;
			if (document.body.classList.contains('cf-off') && menuEl.classList.contains('cf-only')) return;

			currentTab = tabName;
			document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
			menuEl.classList.add('active');

			document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
			tabEl.classList.add('active');

			document.getElementById('sidebar').classList.remove('active');

			if (tabName === 'overview') {
				loadUsageDetails();
			} else if (tabName === 'accounts') {
				loadAccounts();
			} else if (tabName === 'settings') {
				loadSettings();
			} else if (tabName === 'providers') {
				loadProviders();
			} else if (tabName === 'access') {
				loadAccessSample();
			} else if (tabName === 'quota') {
				loadQuotaGroups();
			}
		}

		async function apiFetch(path, options = {}) {
			const res = await fetch(path, options);
			if (res.status === 401) {
				window.location.href = '/';
				throw new Error('Unauthorized');
			}
			return res;
		}


		function renderHistoryChart(labels, data) {
			if (historyChart) historyChart.destroy();
			const isLight = document.documentElement.getAttribute('data-theme') === 'light';
			const gridColor = isLight ? 'rgba(0, 0, 0, 0.05)' : 'rgba(255, 255, 255, 0.05)';
			const textColor = isLight ? '#64748b' : '#94a3b8';
			const ctx = document.getElementById('historyChart').getContext('2d');
			const gradient = ctx.createLinearGradient(0, 0, 0, 300);
			gradient.addColorStop(0, 'rgba(168, 85, 247, 0.35)');
			gradient.addColorStop(1, 'rgba(168, 85, 247, 0.00)');
			historyChart = new Chart(ctx, {
				type: 'line',
				data: {
					labels: labels,
					datasets: [{
						label: 'Neuron 消耗数',
						data: data,
						borderColor: '#a855f7',
						backgroundColor: gradient,
						borderWidth: 3,
						tension: 0.3,
						fill: true,
						pointBackgroundColor: '#a855f7',
						pointBorderColor: 'rgba(255, 255, 255, 0.8)',
						pointBorderWidth: 1.5,
						pointRadius: 4,
						pointHoverRadius: 6,
						pointHoverBorderWidth: 3
					}]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					plugins: { legend: { display: false } },
					scales: {
						y: { grid: { color: gridColor }, ticks: { color: textColor } },
						x: { grid: { display: false }, ticks: { color: textColor } }
					}
				}
			});
		}

		function renderModelsChart(labels, data) {
			if (modelsChart) modelsChart.destroy();
			
			const legendContainer = document.getElementById('admin-chart-legend');
			const canvasWrapper = document.getElementById('admin-canvas-wrapper');
			const legendWrapper = document.getElementById('admin-legend-wrapper');
			const placeholder = document.getElementById('admin-chart-placeholder');

			if (legendContainer) legendContainer.innerHTML = '';

			if (labels.length === 0) {
				if (canvasWrapper) canvasWrapper.style.display = 'none';
				if (legendWrapper) legendWrapper.style.display = 'none';
				if (placeholder) placeholder.style.display = 'flex';
				return;
			} else {
				if (canvasWrapper) canvasWrapper.style.display = 'flex';
				if (legendWrapper) legendWrapper.style.display = 'flex';
				if (placeholder) placeholder.style.display = 'none';
			}

			// Sort the model data descending by neurons
			const combined = labels.map((label, idx) => ({
				fullLabel: label,
				cleanLabel: label.split('/').pop(),
				value: data[idx]
			})).sort((a, b) => b.value - a.value);

			const sortedLabels = combined.map(x => x.cleanLabel);
			const sortedData = combined.map(x => x.value);

			const isLight = document.documentElement.getAttribute('data-theme') === 'light';
			const textColor = isLight ? '#64748b' : '#94a3b8';
			const borderColor = isLight ? '#ffffff' : '#1e293b';
			const ctx = document.getElementById('modelsChart').getContext('2d');
			
			modelsChart = new Chart(ctx, {
				type: 'doughnut',
				data: {
					labels: sortedLabels,
					datasets: [{
						data: sortedData,
						backgroundColor: ['#6366f1', '#a855f7', '#ec4899', '#10b981', '#f59e0b', '#3b82f6'],
						borderWidth: 2,
						borderColor: borderColor
					}]
				},
				options: {
					responsive: true,
					maintainAspectRatio: false,
					cutout: '70%',
					animation: {
						animateRotate: true,
						animateScale: true,
						duration: 1000,
						easing: 'easeOutQuart'
					},
					plugins: {
						legend: {
							display: false // 关闭原生图例，使用 HTML 自定义图例
						}
					}
				}
			});

			// Render Custom HTML Legend for Admin Page
			if (legendContainer) {
				const colors = ['#6366f1', '#a855f7', '#ec4899', '#10b981', '#f59e0b', '#3b82f6'];
				const total = sortedData.reduce((a, b) => a + b, 0);
				
				combined.forEach((itemData, index) => {
					const label = itemData.cleanLabel;
					const fullLabel = itemData.fullLabel;
					const val = itemData.value;
					const color = colors[index % colors.length];
					const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
					
				const item = document.createElement('div');
				item.style.display = 'flex';
				item.style.flexDirection = 'column';
				item.style.gap = '2px';
				item.style.minWidth = '0';
				item.style.fontSize = '12px';
				item.style.color = textColor;
				item.style.opacity = '0';
				item.style.transform = 'translateX(10px)';
				item.style.transition = 'all 0.4s cubic-bezier(0.16, 1, 0.3, 1)';

				item.innerHTML = '<span style="display: flex; align-items: center; gap: 8px; min-width: 0;">' +
					'<span style="width: 8px; height: 8px; border-radius: 50%; background-color: ' + color + '; flex-shrink: 0; margin-right: 2px;"></span>' +
					'<span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 auto; min-width: 0; font-weight: 500;" title="' + fullLabel + '">' + label + '</span></span>' +
					'<span style="color: var(--text-muted); font-family: monospace; font-size: 11px; padding-left: 18px;">' + pct + '%</span>';
					
					legendContainer.appendChild(item);
					
					// 与环形图同时开始加载，依次淡入滑出
					setTimeout(() => {
						item.style.opacity = '1';
						item.style.transform = 'translateX(0)';
					}, index * 80);
				});
			}
		}

		async function copyEndpointUrl(url) {
			if (!url) return;
			try {
				if (navigator.clipboard && window.isSecureContext) {
					await navigator.clipboard.writeText(url);
				} else {
					const input = document.createElement('input');
					input.value = url;
					input.style.position = 'fixed';
					input.style.opacity = '0';
					input.style.left = '-9999px';
					document.body.appendChild(input);
					input.select();
					document.execCommand('copy');
					document.body.removeChild(input);
				}
				showToast('已复制接入地址！');
			} catch (e) {
				console.error(e);
				showToast('复制失败，请手动复制 URL', 'error');
			}
		}

		async function loadAccounts() {
			try {
				const res = await apiFetch('/api/accounts');
				const accounts = await res.json();
				const list = document.getElementById('accounts-list');
				list.innerHTML = '';
				if (accounts.length === 0) {
					list.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 30px;">暂无配置的 Cloudflare 账号</div>';
					return;
				}
				accounts.forEach(acc => {
					const maskedToken = acc.apiToken.length > 8 ? acc.apiToken.substring(0, 4) + '...' + acc.apiToken.substring(acc.apiToken.length - 4) : '********';
					const maskedId = acc.accountId.length > 12 ? acc.accountId.substring(0, 6) + '...' + acc.accountId.substring(acc.accountId.length - 4) : '********';
					const card = document.createElement('div');
					card.className = 'list-card';
					card.innerHTML = '<div class="list-head" onclick="toggleListCard(this)">'
						+ '<span class="list-arrow">▸</span>'
						+ '<span class="list-title">' + sen(acc.name) + '</span>'
						+ '<span class="list-meta">' + sen(maskedId) + '</span>'
						+ '<span class="list-spacer"></span>'
						+ '<span class="list-meta">展开</span>'
						+ '</div>'
						+ '<div class="list-body">'
						+ '<div class="list-row"><span class="list-label">Account ID</span><code style="font-size:12px;">' + sen(maskedId) + '</code></div>'
						+ '<div class="list-row"><span class="list-label">API Token</span><code style="font-size:12px;">' + sen(maskedToken) + '</code></div>'
						+ '<div class="list-actions">'
						+ '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-acc-edit>编辑</button>'
						+ '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-acc-del>删除</button>'
						+ '</div>'
						+ '</div>';
					// 不把用户数据拼进内联 onclick（含单引号即破坏按钮 JS、含特殊构造还能注入）
					// 改用事件绑定；闭包直接捕获 acc，连查表都省了。
					card.querySelector('[data-acc-edit]').onclick = () => editAccount(acc);
					card.querySelector('[data-acc-del]').onclick = () => deleteAccount(acc.id);
					list.appendChild(card);
				});
			} catch (e) {
				console.error(e);
			}
		}

		function openAddAccountModal() {
			document.getElementById('account-modal-title').innerText = '添加 Cloudflare 账号';
			document.getElementById('account-id-edit').value = '';
			document.getElementById('account-name').value = '';
			document.getElementById('account-id').value = '';
			document.getElementById('account-token').value = '';
			document.getElementById('test-result-alert').style.display = 'none';
			document.getElementById('perm-wa-read').innerHTML = '';
			document.getElementById('perm-wa-edit').innerHTML = '';
			document.getElementById('perm-aa-read').innerHTML = '';
			document.getElementById('btn-save-account').disabled = true;
			document.getElementById('account-modal').classList.add('active');
		}

		function closeAccountModal() {
			document.getElementById('account-modal').classList.remove('active');
		}

		function editAccount(acc) {
			document.getElementById('account-modal-title').innerText = '编辑 Cloudflare 账号';
			document.getElementById('account-id-edit').value = acc.id;
			document.getElementById('account-name').value = acc.name;
			document.getElementById('account-id').value = acc.accountId;
			document.getElementById('account-token').value = acc.apiToken;
			document.getElementById('test-result-alert').style.display = 'none';
			document.getElementById('perm-wa-read').innerHTML = '';
			document.getElementById('perm-wa-edit').innerHTML = '';
			document.getElementById('perm-aa-read').innerHTML = '';
			document.getElementById('btn-save-account').disabled = true;
			document.getElementById('account-modal').classList.add('active');
		}

		function onAccountInfoChange() {
			document.getElementById('btn-save-account').disabled = true;
			document.getElementById('test-result-alert').style.display = 'none';
			document.getElementById('perm-wa-read').innerHTML = '';
			document.getElementById('perm-wa-edit').innerHTML = '';
			document.getElementById('perm-aa-read').innerHTML = '';
		}

		function updatePermissionStatus(elementId, statusObj) {
			const el = document.getElementById(elementId);
			if (!el) return;
			if (statusObj && statusObj.success) {
				el.innerHTML = '<span style="color: #10b981; font-weight: bold; margin-left: 6px;">✅ 有效</span>';
			} else {
				const err = (statusObj && statusObj.error) ? statusObj.error : '测试失败';
				el.innerHTML = '<span style="color: #ef4444; font-weight: bold; margin-left: 6px;" title="' + err.replace(/"/g, '&quot;') + '">🔴 无效</span>';
			}
		}

		function escapeHtml(str) {
			if (!str) return '';
			return String(str)
				.replace(/&/g, '&amp;')
				.replace(/</g, '&lt;')
				.replace(/>/g, '&gt;')
				.replace(/"/g, '&quot;')
				.replace(/'/g, '&#39;');
		}

		function setAlertStyle(el, type) {
			const styles = {
				warning: { bg: 'rgba(245, 158, 11, 0.12)', color: '#f59e0b', border: 'rgba(245, 158, 11, 0.3)' },
				success: { bg: 'rgba(16, 185, 129, 0.12)', color: '#10b981', border: 'rgba(16, 185, 129, 0.3)' },
				danger:  { bg: 'rgba(239, 68, 68, 0.12)', color: '#ef4444', border: 'rgba(239, 68, 68, 0.3)' }
			};
			const s = styles[type] || styles.danger;
			el.style.backgroundColor = s.bg;
			el.style.color = s.color;
			el.style.borderColor = s.border;
		}

		const ALERT_ICONS = {
			spinner: '<svg style="width:16px;height:16px;animation:spin 1s linear infinite;flex-shrink:0;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>',
			check:    '<svg style="width:18px;height:18px;flex-shrink:0;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>',
			warning: '<svg style="width:18px;height:18px;flex-shrink:0;margin-top:1px;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>'
		};

		async function testConnection() {
			const accountId = document.getElementById('account-id').value;
			const apiToken = document.getElementById('account-token').value;
			const id = document.getElementById('account-id-edit').value;
			const alertEl = document.getElementById('test-result-alert');
			alertEl.style.display = 'block';
			setAlertStyle(alertEl, 'warning');
			alertEl.innerHTML = '<div style="display:flex;align-items:center;gap:8px;">' + ALERT_ICONS.spinner + '<span>测试中...</span></div>';

			document.getElementById('perm-wa-read').innerHTML = '';
			document.getElementById('perm-wa-edit').innerHTML = '';
			document.getElementById('perm-aa-read').innerHTML = '';
			document.getElementById('btn-save-account').disabled = true;

			try {
				const res = await apiFetch('/api/accounts/test', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ id, accountId, apiToken })
				});
				const data = await res.json();
				if (data.permissions) {
					updatePermissionStatus('perm-wa-read', data.permissions.workersAiRead);
					updatePermissionStatus('perm-wa-edit', data.permissions.workersAiEdit);
					updatePermissionStatus('perm-aa-read', data.permissions.accountAnalyticsRead);
				}
				if (data.success) {
					setAlertStyle(alertEl, 'success');
					alertEl.innerHTML = '<div style="display:flex;align-items:center;gap:8px;">' + ALERT_ICONS.check + '<span>连接成功！API 权限全部有效</span></div>';
					showToast('连接测试成功！');
					document.getElementById('btn-save-account').disabled = false;
				} else {
					setAlertStyle(alertEl, 'danger');

					// Build structured error details from permissions data
					let errorDetailHtml = '';
					if (data.permissions) {
						const permList = [
							{ key: 'workersAiRead',       label: 'Workers AI > Read' },
							{ key: 'workersAiEdit',       label: 'Workers AI > Edit' },
							{ key: 'accountAnalyticsRead', label: 'Account Analytics > Read' }
						];
						const failedItems = permList.filter(p => {
							const perm = data.permissions[p.key];
							return perm && !perm.success;
						});
						if (failedItems.length > 0) {
							errorDetailHtml = failedItems.map(p => {
								const perm = data.permissions[p.key];
								const errMsg = escapeHtml(perm.error || '未知错误');
								return '<div style="padding:3px 0;word-break:break-all;overflow-wrap:break-word;"><span style="opacity:0.6;">●</span> <strong>' + p.label + '</strong>: ' + errMsg + '</div>';
							}).join('');
						}
					}
					if (!errorDetailHtml) {
						errorDetailHtml = '<div style="padding:3px 0;word-break:break-all;overflow-wrap:break-word;">' + escapeHtml(data.error || '部分权限验证未通过') + '</div>';
					}

					alertEl.innerHTML = '<div style="display:flex;align-items:flex-start;gap:8px;">' +
						ALERT_ICONS.warning +
						'<div style="flex:1;min-width:0;">' +
						'<div style="font-weight:700;margin-bottom:4px;">连接失败 — 以下权限验证未通过：</div>' +
						'<div style="font-size:12px;opacity:0.85;line-height:1.7;">' + errorDetailHtml + '</div>' +
						'</div>' +
						'</div>';

					showToast('测试连接失败，请检查 Token 权限', 'error');
					document.getElementById('btn-save-account').disabled = true;
				}
			} catch (e) {
				setAlertStyle(alertEl, 'danger');
				alertEl.innerHTML = '<div style="display:flex;align-items:center;gap:8px;">' + ALERT_ICONS.warning + '<span>连接超时或异常，请重试</span></div>';
				showToast('连接异常，请重试', 'error');
				document.getElementById('btn-save-account').disabled = true;
			}
		}

		async function saveAccount() {
			const id = document.getElementById('account-id-edit').value;
			const name = document.getElementById('account-name').value;
			const accountId = document.getElementById('account-id').value;
			const apiToken = document.getElementById('account-token').value;
			if (!accountId || !apiToken) {
				showToast('Account ID 和 API Token 均为必填项！', 'warning');
				return;
			}
			const res = await apiFetch('/api/accounts', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id, name, accountId, apiToken })
			});
			if (res.ok) {
				closeAccountModal();
				loadAccounts();
				showToast('账号保存成功！');
			} else {
				showToast('保存失败！', 'error');
			}
		}

		// 通用确认小卡片：uiConfirm(message, {title, okText, danger}) → Promise<boolean>
		let _confirmResolve = null;
		function uiConfirm(message, opts) {
			opts = opts || {};
			document.getElementById('confirm-title').textContent = opts.title || '确认操作';
			document.getElementById('confirm-text').textContent = message;
			document.getElementById('confirm-icon').textContent = opts.danger ? '🗑️' : '⚠️';
			var okBtn = document.getElementById('confirm-ok-btn');
			okBtn.textContent = opts.okText || '确定';
			okBtn.style.background = opts.danger ? 'var(--danger-color)' : '';
			okBtn.style.borderColor = opts.danger ? 'var(--danger-color)' : '';
			document.getElementById('confirm-modal').classList.add('active');
			return new Promise(function (resolve) { _confirmResolve = resolve; });
		}
		(function initConfirmModal() {
			var modal = document.getElementById('confirm-modal');
			if (!modal) return;
			function finish(v) {
				if (!_confirmResolve) return;
				modal.classList.remove('active');
				var r = _confirmResolve;
				_confirmResolve = null;
				r(v);
			}
			document.getElementById('confirm-ok-btn').addEventListener('click', function () { finish(true); });
			document.getElementById('confirm-cancel-btn').addEventListener('click', function () { finish(false); });
			modal.addEventListener('click', function (e) { if (e.target === modal) finish(false); });
		})();

		async function deleteAccount(id) {
			if (!(await uiConfirm('确定要删除这个 Cloudflare 账号吗？', { danger: true, okText: '删除' }))) return;
			const res = await apiFetch('/api/accounts', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id })
			});
			if (res.ok) {
				loadAccounts();
				showToast('账号已成功删除');
			} else {
				showToast('删除失败', 'error');
			}
		}

		async function loadKeys() {
			try {
				const res = await apiFetch('/api/keys');
				const keys = await res.json();
				const list = document.getElementById('keys-list');
				list.innerHTML = '';
				const userKeys = keys.filter(k => !k.system);
				if (userKeys.length === 0) {
					document.getElementById('no-key-warning').classList.remove('hidden');
				} else {
					document.getElementById('no-key-warning').classList.add('hidden');
				}
				const now = Date.now();
				keys.forEach(k => {
					const isSystem = !!k.system;
					const dateStr = k.createdAt ? new Date(k.createdAt).toLocaleString() : '—';
					// 摘要（行内短标签）与明细（展开后完整说明）分开，行内不塞 br
					let statusShort, statusDetail;
					if (isSystem) {
						statusShort = k.rotationEnabled ? '自动轮换' : '永久';
						statusDetail = k.rotationEnabled
							? ('自动轮换，下次 ' + (k.nextRotationAt ? new Date(k.nextRotationAt).toLocaleDateString() : '—'))
							: '永久（手动管理）';
					} else if (k.expiresAt) {
						statusShort = k.expiresAt > now ? '有效' : '已过期';
						statusDetail = k.expiresAt > now
							? ('有效至 ' + new Date(k.expiresAt).toLocaleDateString())
							: ('已于 ' + new Date(k.expiresAt).toLocaleDateString() + ' 过期');
					} else {
						statusShort = '永久';
						statusDetail = '永久';
					}
					const masked = k.key.length > 6 ? k.key.substring(0, 5) + '...' + k.key.substring(k.key.length - 1) : k.key.substring(0, Math.min(3, k.key.length)) + '...';
					const card = document.createElement('div');
					card.className = 'list-card';
					card.innerHTML = '<div class="list-head" onclick="toggleListCard(this)">'
						+ '<span class="list-arrow">▸</span>'
						+ '<span class="list-title">' + sen(k.name) + '</span>'
						+ (isSystem ? '<span class="badge badge-info">系统默认 · 不可删</span>' : '')
						+ '<span class="list-spacer"></span>'
						+ '<span class="list-meta">' + sen(statusShort) + '</span>'
						+ '</div>'
						+ '<div class="list-body">'
						+ '<div class="list-row"><span class="list-label">创建时间</span><span style="font-size:12px;">' + sen(dateStr) + '</span></div>'
						+ '<div class="list-row"><span class="list-label">有效期</span><span style="font-size:12px;">' + sen(statusDetail) + '</span></div>'
						+ '<div class="list-row"><span class="list-label">密钥</span><span class="key-cell" title="点击复制完整密钥" data-copy-key>' + sen(masked) + '</span></div>'
						+ '<div class="list-actions">'
						+ (isSystem
							? '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-regen>重新生成</button>'
							: '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-del>删除</button>')
						+ '</div>'
						+ '</div>';
					if (isSystem) card.querySelector('[data-regen]').onclick = regenerateSystemKey;
					else card.querySelector('[data-del]').onclick = () => deleteKey(k.id);
					// 点击密钥框直接复制完整密钥（不再单放「复制」按钮）；闭包捕获，不拼内联事件
					const keyCell = card.querySelector('[data-copy-key]');
					if (keyCell) keyCell.onclick = () => copyKeyText(k.key);
					list.appendChild(card);
				});
				loadRotationState();
			} catch (e) {
				console.error(e);
			}
		}

		async function loadRotationState() {
			try {
				const res = await apiFetch('/api/system-key-rotation');
				const data = await res.json();
				const toggle = document.getElementById('rotation-toggle');
				const next = document.getElementById('rotation-next');
				if (!toggle) return;
				toggle.checked = !!data.enabled;
				toggle.onchange = () => setRotationEnabled(toggle.checked);
				if (next) {
					next.textContent = data.enabled && data.nextRotationAt
						? '下次轮换：' + new Date(data.nextRotationAt).toLocaleDateString()
						: '（默认关闭）';
				}
			} catch (e) { }
		}

		async function setRotationEnabled(enabled) {
			try {
				const res = await apiFetch('/api/system-key-rotation', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ enabled })
				});
				if (res.ok) {
					showToast(enabled ? '已开启系统默认密钥每 7 天自动轮换' : '已关闭自动轮换');
					loadRotationState();
				} else {
					showToast('保存失败', 'error');
				}
			} catch (e) {
				showToast('保存失败', 'error');
			}
		}

		function sen(str) {
			return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
		}

		function copyKeyText(val) {
			const input = document.createElement('input');
			input.value = val;
			document.body.appendChild(input);
			input.select();
			document.execCommand('copy');
			document.body.removeChild(input);
			showToast('API Key 复制成功！');
		}

		function openAddKeyModal() {
			document.getElementById('key-name').value = '';
			document.getElementById('key-val').value = '';
			document.getElementById('key-modal-title').innerText = '生成新 API 密钥';
			document.getElementById('key-modal-form').classList.remove('hidden');
			document.getElementById('key-modal-success').classList.add('hidden');
			document.getElementById('key-modal').classList.add('active');
		}

		function closeKeyModal() {
			document.getElementById('key-modal').classList.remove('active');
		}

		async function saveKey() {
			const name = document.getElementById('key-name').value;
			const key = document.getElementById('key-val').value;
			const expiresInDays = parseInt(document.getElementById('key-expires').value, 10) || 0;
			if (!name) {
				showToast('请输入描述名称！', 'warning');
				return;
			}
			const res = await apiFetch('/api/keys', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ name, key, expiresInDays })
			});
			if (res.ok) {
				const data = await res.json();
				loadKeys();
				document.getElementById('key-modal-title').innerText = '密钥已生成';
				document.getElementById('key-modal-form').classList.add('hidden');
				document.getElementById('key-modal-success').classList.remove('hidden');
				document.getElementById('generated-key-val').value = data.key;
			} else {
				showToast('保存密钥失败！', 'error');
			}
		}

		function copyGeneratedKey() {
			const el = document.getElementById('generated-key-val');
			el.select();
			document.execCommand('copy');
			showToast('API Key 复制成功！');
		}

		async function deleteKey(id) {
			if (id === '__system__') {
				showToast('系统默认密钥不可删除', 'warning');
				return;
			}
			if (!(await uiConfirm('确定要删除这个 API 密钥吗？', { danger: true, okText: '删除' }))) return;
			const res = await apiFetch('/api/keys', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id })
			});
			if (res.ok) {
				loadKeys();
				showToast('密钥已成功删除');
			} else {
				showToast('删除密钥失败', 'error');
			}
		}

		async function regenerateSystemKey() {
			if (!(await uiConfirm('旧的默认密钥会立即失效，所有用它调用的客户端需要换成新密钥。', { title: '重新生成系统默认密钥？', okText: '重新生成', danger: true }))) return;
			const res = await apiFetch('/api/keys', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ system: true })
			});
			if (res.ok) {
				showToast('系统默认密钥已重新生成');
				loadKeys();
				loadAccessKey();
			} else {
				showToast('重新生成失败', 'error');
			}
		}

		function copyModelId(val) {
			const input = document.createElement('input');
			input.value = val;
			document.body.appendChild(input);
			input.select();
			document.execCommand('copy');
			document.body.removeChild(input);
			showToast(\`已复制模型: \${val}\`);
		}

		function loadAccessSample() {
			const el = document.getElementById('access-curl-sample');
			if (!el) return;
			const origin = window.location.origin;
			const q = "'";
			// 占位符刻意避开尖括号：<...> 在 shell 里会被当成重定向符，示例照抄会跑不起来
			const openaiBody = JSON.stringify({ model: 'MODEL_NAME', messages: [{ role: 'user', content: 'hi' }] });
			const anthropicBody = JSON.stringify({ model: 'MODEL_NAME', max_tokens: 64, messages: [{ role: 'user', content: 'hi' }] });
			const lines = [
				'# OpenAI 兼容格式',
				'curl ' + origin + '/v1/chat/completions',
				'  -H "Content-Type: application/json"',
				'  -H "Authorization: Bearer YOUR_KEY"',
				'  -d ' + q + openaiBody + q,
				'',
				'# Anthropic 兼容格式',
				'curl ' + origin + '/v1/messages',
				'  -H "Content-Type: application/json"',
				'  -H "x-api-key: YOUR_KEY"',
				'  -d ' + q + anthropicBody + q,
				'',
				'# 把 MODEL_NAME 换成你要用的模型名，YOUR_KEY 换成上面复制的密钥'
			];
			el.textContent = lines.join(String.fromCharCode(10));
			// 同一个 Tab：刷新「当前密钥」和下面的密钥管理表格
			loadAccessKey();
			loadKeys();
		}

		// 接入页的「当前密钥」：默认脱敏展示 + 一键复制（复制拿完整值、显示保持脱敏，
		// 免得这页被截图时把密钥带出去）。多把密钥只展示第一把，其余去「API 密钥」页管理。
		function maskKey(k) {
			const s = String(k || '');
			if (!s) return '';
			return s.length <= 10 ? s.substring(0, 2) + '...' : s.substring(0, 6) + '...' + s.substring(s.length - 4);
		}

		async function loadAccessKey() {
			const codeEl = document.getElementById('access-key-value');
			if (!codeEl) return;
			const btn = document.getElementById('access-key-copy');
			const moreEl = document.getElementById('access-key-more');
			try {
				const keys = await (await apiFetch('/api/keys')).json();
				if (!keys || !keys.length) {
					codeEl.textContent = '未配置';
					delete codeEl.dataset.full;
					codeEl.style.cursor = 'default';
					if (btn) btn.style.display = 'none';
					if (moreEl) moreEl.textContent = '';
					return;
				}
				const userKeys = keys.filter(k => !k.system);
			const primary = userKeys.length ? userKeys[0] : keys[0];
				codeEl.textContent = maskKey(primary.key);
				codeEl.dataset.full = primary.key;
				codeEl.title = '点击复制完整密钥';
				codeEl.style.cursor = 'pointer';
				codeEl.onclick = () => copyAccessKey();
				if (btn) btn.style.display = '';
				if (moreEl) moreEl.textContent = keys.length > 1 ? '含系统默认密钥，共 ' + keys.length + ' 把' : '';
			} catch (e) {
				codeEl.textContent = '载入失败';
				if (btn) btn.style.display = 'none';
			}
		}

		// 只复制完整值，界面上始终只显示脱敏串
		function copyAccessKey() {
			const codeEl = document.getElementById('access-key-value');
			const full = codeEl && codeEl.dataset.full;
			if (full) copyKeyText(full);
		}

		async function loadRuntimeState() {
			try {
				const res = await apiFetch('/api/runtime');
				applyRuntimeState(await res.json());
			} catch (e) {
				console.error(e);
			}
		}

		// 把运行模式落到界面上：body 上的 cf-off 类控制「概览」分组显隐，侧边栏状态条同步文案
		function applyRuntimeState(rt) {
			runtimeState = rt;
			document.body.classList.toggle('cf-off', !rt.cfPoolEnabled);

			const el = document.getElementById('runtime-status-text');
			if (!el) return;

			// 侧边栏顶部只放一行摘要；账号数 / 默认渠道这些细节挂到 title 上，
			// 完整信息在点击打开的弹窗里（那儿本来就有）
			const parts = [rt.cfPoolEnabled ? '账号池 已启用（' + rt.accounts + ' 个账号）' : '账号池 已关闭'];
			if (rt.defaultProviderId) {
				const p = providersCache.find(x => x.id === rt.defaultProviderId);
				parts.push('默认渠道 ' + (p ? p.name : '已失效'));
			} else {
				parts.push('未设默认渠道');
			}
			el.textContent = '运行模式 · ' + parts[0];
			const box = el.parentElement;
			if (box) box.title = parts.join(' · ') + '（点击修改）';

			// 状态点：账号池启用为绿色，关闭为灰色
			const dot = document.getElementById('runtime-status-dot');
			if (dot) dot.style.backgroundColor = rt.cfPoolEnabled ? 'var(--success-color)' : 'var(--text-muted)';
		}

		function buildProviderOptions(selectedId) {
			const select = document.getElementById('default-provider-select');
			select.innerHTML = '<option value="">不设置</option>';
			providersCache.forEach(p => {
				const opt = document.createElement('option');
				opt.value = p.id;
				opt.textContent = p.name + (p.status === 'disabled' ? '（已停用）' : '');
				select.appendChild(opt);
			});
			select.value = selectedId || '';
		}

		async function openRuntimeModal() {
			// 渠道列表可能还没加载过，这里补一次，否则默认渠道下拉是空的
			if (!providersCache.length) {
				try {
					const res = await apiFetch('/api/providers');
					providersCache = await res.json();
				} catch (e) {
					console.error(e);
				}
			}
			buildProviderOptions(runtimeState.defaultProviderId);
			document.getElementById('cf-pool-toggle').checked = !!runtimeState.cfPoolEnabled;

			const msgs = [];
			msgs.push(runtimeState.cfPoolEnabled
				? '账号池启用中，绑定了 ' + runtimeState.accounts + ' 个账号。'
				: '账号池已关闭，当前是纯第三方反代模式。');
			if (!runtimeState.defaultProviderId) {
				msgs.push('未设置默认渠道：没命中任何映射的模型名会直接返回错误。');
			} else {
				const p = providersCache.find(x => x.id === runtimeState.defaultProviderId);
				msgs.push('未命中映射的模型名会原样转发到「' + (p ? p.name : '已失效') + '」。');
			}
			document.getElementById('runtime-hint').textContent = msgs.join(' ');

			document.getElementById('runtime-modal').classList.add('active');
		}

		function closeRuntimeModal() {
			document.getElementById('runtime-modal').classList.remove('active');
		}

		async function saveRuntimeMode() {
			const cfPoolEnabled = document.getElementById('cf-pool-toggle').checked;
			const defaultProviderId = document.getElementById('default-provider-select').value;
			const res = await apiFetch('/api/runtime', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ cfPoolEnabled, defaultProviderId })
			});
			if (!res.ok) {
				showToast('保存运行模式失败', 'error');
				return;
			}

			// 不整页刷新：直接切 body 上的 class，界面即时生效
			applyRuntimeState({ cfPoolEnabled, defaultProviderId, accounts: runtimeState.accounts });
			closeRuntimeModal();

			// 当前 Tab 被新模式隐藏了就换到可见的那个（access 在任何模式下都可见）
			const currentMenu = document.getElementById('menu-' + currentTab);
			if (!cfPoolEnabled && currentMenu && currentMenu.classList.contains('cf-only')) {
				currentTab = '';
				switchTab('access');
			}
			showToast('运行模式已保存');
		}

		// 渠道列表：紧凑卡片 + 点开展开（方案B，2026-10-09 用户拍板）。
		// 模型是「备忘录」，展开后以 chips 横排；超 8 个先收成「+N 展开」，点一下铺开。
		const PROVIDER_CHIP_PREVIEW = 8;

		function providerChipHtml(m, h) {
			const dotColor = !h ? 'var(--text-muted)' : (h.ok ? 'var(--success-color)' : 'var(--danger-color)');
			const dotTitle = !h ? '尚未测试' : (h.ok ? '上次测试正常' : '上次测试失败');
			return '<span class="chip" title="' + sen(dotTitle + ' · ' + m) + '"><span style="color:' + dotColor + ';">●</span> ' + sen(m) + '</span>';
		}

		async function loadProviders() {
			try {
				// 顺带拉今日调用统计：转发次数和探测（测试）次数分开显示
				const [provRes, statsRes] = await Promise.all([
					apiFetch('/api/providers'),
					apiFetch('/api/provider-stats?range=today').catch(() => null)
				]);
				providersCache = await provRes.json();
				const statsById = {};
				try {
					const st = statsRes ? await statsRes.json() : null;
					((st && st.providers) || []).forEach(x => { statsById[x.id] = x; });
				} catch (e) { }
				applyRuntimeState(runtimeState);
				const list = document.getElementById('providers-list');
				list.innerHTML = '';
				if (!providersCache.length) {
					list.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 30px;">暂无第三方渠道</div>';
					return;
				}
				providersCache.forEach(p => {
					const enabled = p.status !== 'disabled';
					const healthMap = p.modelHealth || {};
					const allChips = (p.models || []).map(m => providerChipHtml(m, healthMap[m]));
					let chipsHtml;
					if (!allChips.length) {
						chipsHtml = '<span style="color: var(--text-muted); font-size:12px;">未填写</span>';
					} else if (allChips.length <= PROVIDER_CHIP_PREVIEW) {
						chipsHtml = allChips.join('');
					} else {
						const rest = allChips.length - PROVIDER_CHIP_PREVIEW;
						chipsHtml = allChips.slice(0, PROVIDER_CHIP_PREVIEW).join('')
							+ '<span class="chip chip-more" data-rest="' + rest + '" onclick="expandProviderChips(this)">+' + rest + ' 展开</span>';
					}
					// 今日用量：转发与测试分开显示。测试同样消耗上游额度，且会计入配额判断
					const st = statsById[p.id];
					const usageText = st ? ('今日转发 ' + st.req + ' · 测试 ' + (st.probeReq || 0) + ' 次') : '今日无调用';
					const card = document.createElement('div');
					card.className = 'list-card';
					card.dataset.pid = p.id;
					card.innerHTML = '<div class="list-head" onclick="toggleListCard(this)">'
						+ '<span class="list-arrow">▸</span>'
						+ '<span class="list-title">' + sen(p.name) + '</span>'
						+ (enabled ? '<span class="badge badge-success">启用</span>' : '<span class="badge badge-danger">已停用</span>')
						+ '<span class="list-meta">' + (p.models || []).length + ' 个模型</span>'
						+ '<span class="list-meta">' + sen(usageText) + '</span>'
						+ '<span class="list-spacer"></span>'
						+ '<label class="switch" title="' + (enabled ? '点击停用' : '点击启用') + '" onclick="event.stopPropagation()">'
						+ '<input type="checkbox" ' + (enabled ? 'checked' : '') + ' onchange="toggleProviderStatus(this)">'
						+ '<span class="switch-track"></span>'
						+ '</label>'
						+ '</div>'
						+ '<div class="list-body">'
						+ '<div class="list-row"><span class="list-label">Base URL</span>'
						+ '<span class="list-url" title="点击复制">' + sen(p.baseUrl || '') + '</span></div>'
						+ '<div class="list-row"><span class="list-label">模型</span><span class="chips">' + chipsHtml + '</span></div>'
						+ '<div class="list-actions">'
						+ '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-pp-health>测模型</button>'
						+ '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-pp-spec>模型规格</button>'
						+ '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-pp-edit>编辑</button>'
						+ '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-pp-del>删除</button>'
						+ '</div>'
						+ '</div>';
					card.querySelector('[data-pp-health]').onclick = () => openProviderHealthModal(p.id);
					card.querySelector('[data-pp-spec]').onclick = () => openModelSpecModal(p.id);
					card.querySelector('[data-pp-edit]').onclick = () => openProviderModal(p.id);
					card.querySelector('[data-pp-del]').onclick = () => deleteProvider(p.id);
					// Base URL 点击复制：闭包捕获，不拼进内联事件
					const urlEl = card.querySelector('.list-url');
					if (urlEl) urlEl.onclick = () => copyKeyText(p.baseUrl || '');
					list.appendChild(card);
				});
			} catch (e) {
				console.error(e);
			}
		}

		function openProviderModal(id) {
			const p = id ? providersCache.find(x => x.id === id) : null;
			document.getElementById('provider-id-edit').value = p ? p.id : '';
			document.getElementById('provider-name').value = p ? p.name : '';
			document.getElementById('provider-base-url').value = p ? (p.baseUrl || '') : '';
			const keyInput = document.getElementById('provider-api-key');
			keyInput.value = '';
			keyInput.placeholder = p ? '留空表示不修改当前密钥' : 'sk-...';
			document.getElementById('provider-models').value = (p && p.models) ? p.models.join('\\n') : '';
			document.getElementById('provider-status').value = p ? (p.status || 'active') : 'active';
			document.getElementById('provider-gemini-native').value =
				(p && p.geminiNative === true) ? 'on' : ((p && p.geminiNative === false) ? 'off' : 'auto');
			document.getElementById('provider-fastpass').checked = !!(p && p.streamFastPass === true);
			// 回显用 != null：0 是合法的「免费渠道」值，不能当 falsy 落空（否则编辑时看不见已填的 0）。
			document.getElementById('provider-price-input').value =
				(p && p.pricing && p.pricing.inputPer1M != null) ? p.pricing.inputPer1M : '';
			document.getElementById('provider-price-output').value =
				(p && p.pricing && p.pricing.outputPer1M != null) ? p.pricing.outputPer1M : '';
			document.getElementById('provider-modal-title').innerText = p ? '编辑第三方渠道' : '添加第三方渠道';
			initProviderPresets();
			document.getElementById('provider-preset').value = '';
			const box = document.getElementById('provider-test-result');
			box.style.display = 'none';
			box.innerText = '';
			document.getElementById('provider-modal').classList.add('active');
		}

		function closeProviderModal() {
			document.getElementById('provider-modal').classList.remove('active');
		}

		function collectProviderForm() {
			// 「自动」= 不传这个字段（后端按 baseUrl 判断）；on/off 才显式下发
			const gnSel = document.getElementById('provider-gemini-native').value;
			const gn = gnSel === 'on' ? true : (gnSel === 'off' ? false : undefined);
			const payload = {
				id: document.getElementById('provider-id-edit').value || undefined,
				name: document.getElementById('provider-name').value.trim(),
				baseUrl: document.getElementById('provider-base-url').value.trim(),
				apiKey: document.getElementById('provider-api-key').value.trim(),
				models: document.getElementById('provider-models').value,
				status: document.getElementById('provider-status').value
			};
			if (gn !== undefined) payload.geminiNative = gn;
			payload.streamFastPass = document.getElementById('provider-fastpass').checked === true;
			// 计费单价：两个都空 → 显式清空（null）；否则下发对象
			const rawIn = document.getElementById('provider-price-input').value.trim();
			const rawOut = document.getElementById('provider-price-output').value.trim();
			const priceIn = Number(rawIn);
			const priceOut = Number(rawOut);
			// 留空 = 未填（清空）；显式填数（含 0）= 已填。0/0 ⇒ 免费渠道（成本按 0 算）。负数钳 0。
			const inFilled = rawIn !== '';
			const outFilled = rawOut !== '';
			const inN = inFilled ? (isFinite(priceIn) && priceIn >= 0 ? priceIn : 0) : 0;
			const outN = outFilled ? (isFinite(priceOut) && priceOut >= 0 ? priceOut : 0) : 0;
			payload.pricing = (inFilled || outFilled) ? { inputPer1M: inN, outputPer1M: outN } : null;
			return payload;
		}

		function pickFirstModel(raw) {
			return String(raw || '').split(/[\\n,]/).map(s => s.trim()).filter(Boolean)[0] || '';
		}

		function showProviderTestResult(ok, text, raw) {
			const wrap = document.getElementById('provider-test-result');
			const box = document.getElementById('provider-test-summary');
			const rawWrap = document.getElementById('provider-test-raw-wrap');
			const rawBox = document.getElementById('provider-test-raw');

			wrap.style.display = 'block';
			box.style.borderColor = ok ? 'rgba(16, 185, 129, 0.4)' : 'rgba(239, 68, 68, 0.4)';
			box.style.backgroundColor = ok ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)';
			box.style.color = ok ? 'var(--success-color)' : 'var(--danger-color)';
			box.innerText = text;

			if (raw) {
				rawWrap.style.display = 'block';
				rawBox.innerText = raw;
			} else {
				rawWrap.style.display = 'none';
				rawWrap.open = false;
				rawBox.innerText = '';
			}
			// 结果可能在滚动区下方，滚进来让用户直接看到
			try { box.scrollIntoView({ block: 'nearest' }); } catch (_) { }
		}

		// 把失败结果整理成人能读的几行（失败原因 → 上游原话 → 建议 → 实际地址 → 原始响应）
		function describeProbeFailure(r) {
			const nl = String.fromCharCode(10);
			const lines = [];
			lines.push('测试失败：' + (r.status ? 'HTTP ' + r.status : '请求未完成'));
			if (r.upstreamMessage) {
				lines.push('');
				lines.push('上游说：' + r.upstreamMessage);
			}
			if (r.hint) {
				lines.push('');
				lines.push('建议：' + r.hint);
			}
			lines.push('');
			lines.push('实际请求地址：' + (r.endpoint || ''));
			return lines.join(nl);
		}

		// ---- 模型可用性检查 ----
		// 注意：这段代码在 Worker 的模板字符串里，不能写带转义斜杠的正则（\/ 会被折叠成 /），
		// 所以去掉末尾斜杠用循环而不是 replace(/\/+$/, '')
		function trimTrailingSlashes(u) {
			let s = String(u || '');
			while (s.endsWith('/')) s = s.slice(0, -1);
			return s;
		}

		function openProviderHealthModal(id) {
			const p = providersCache.find(x => x.id === id);
			if (!p) return;
			healthProviderId = id;
			healthResults = {};
			document.getElementById('health-modal-title').innerText = '模型可用性 · ' + p.name;
			document.getElementById('health-endpoint').textContent =
				'实际请求地址：' + trimTrailingSlashes(p.baseUrl) + '/chat/completions';
			renderHealthTable();
			document.getElementById('provider-health-modal').classList.add('active');
		}

		function closeProviderHealthModal() {
			document.getElementById('provider-health-modal').classList.remove('active');
		}

		// ---- 模型规格（上游元信息：上下文 / 最大输出 / 价格 / 能力） ----
		let specProviderId = '';
		let specProviderModels = [];

		function fmtTokenCount(n) {
			if (n === null || n === undefined) return '';
			n = Number(n);
			if (!isFinite(n) || n <= 0) return '';
			if (n >= 1000000) return (n / 1000000).toFixed(n % 1000000 ? 2 : 0) + 'M';
			if (n >= 1000) return (n / 1000).toFixed(n % 1000 ? 1 : 0) + 'K';
			return String(n);
		}

		// OpenRouter 的 pricing 是「美元 / token」的字符串，换算成「/百万 tokens」更好读
		function fmtModelPrice(pricing) {
			if (!pricing || typeof pricing !== 'object') return '';
			const out = [];
			const pairs = [['prompt', '入'], ['completion', '出']];
			for (const pair of pairs) {
				const v = Number(pricing[pair[0]]);
				if (isFinite(v) && v > 0) {
					const perM = v * 1000000;
					out.push(pair[1] + ' $' + (perM < 1 ? perM.toFixed(3) : perM.toFixed(2)));
				}
			}
			return out.length ? out.join(' / ') + ' /M' : '';
		}

		// 短标签：最多显示 max 个，其余折叠成 +N，鼠标悬停 title 看全 —— 避免长逗号串把列撑爆
		function specChips(list, max) {
			if (!Array.isArray(list) || !list.length) return '';
			// 标签：超长条目（如超长参数名）允许在标签内折行、且不超出列宽（否则 nowrap 会溢出单元格）
			const chip = 'display:inline-block; max-width:100%; padding:1px 6px; margin:1px 3px 1px 0; border-radius:6px; background: var(--section-item-bg, rgba(148,163,184,0.15)); font-size:11px; overflow-wrap:anywhere;';
			let html = list.slice(0, max).map(function (s) { return '<span style="' + chip + '">' + sen(String(s)) + '</span>'; }).join('');
			if (list.length > max) html += '<span style="font-size:11px; color: var(--text-muted);">+' + (list.length - max) + '</span>';
			return '<span title="' + sen(list.join(', ')) + '">' + html + '</span>';
		}

		function fmtModelCaps(d) {
			const lines = [];
			if (Array.isArray(d.inputModalities) && d.inputModalities.length) lines.push('入：' + specChips(d.inputModalities, 4));
			if (Array.isArray(d.outputModalities) && d.outputModalities.length) lines.push('出：' + specChips(d.outputModalities, 4));
			if (Array.isArray(d.supportedParameters) && d.supportedParameters.length) lines.push('参数：' + specChips(d.supportedParameters, 4));
			if (!lines.length && Array.isArray(d.supportedMethods) && d.supportedMethods.length) lines.push('方法：' + specChips(d.supportedMethods, 2));
			return lines.join('<br>');
		}

		// 只列「本渠道已添加的模型」——上游（尤其 OpenRouter）动辄返回几百个，
		// 全列出来既没用又难找。想看全部就去「编辑」里拉取模型列表。
		function renderModelSpecs(data) {
			const box = document.getElementById('model-spec-content');
			const details = (data && data.details) || [];
			const byId = new Map();
			for (const d of details) byId.set(d.id, d);
			const saved = specProviderModels || [];
			if (!saved.length) {
				box.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 24px;">这个渠道还没配置模型，先点「编辑」加上</div>';
				return;
			}
			const dash = '<span style="color: var(--text-muted);">—</span>';
			// 数字列右对齐 + 等宽数字，方便竖向对比
			const numStyle = 'text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums;';
			let rows = '';
			let missing = 0;
			// ⚠️ 这个 <code> 必须 inline-block：全局 code 样式是 display:inline + padding 4px/8px + border，
			// 内联元素的上下 padding/border **不撑高行盒**、只会画到行盒外 —— 于是模型 id 的底色和边框
			// 会压住下一行的显示名（只有带 displayName 的官方模型才会多出那一行，所以看着像「名称重叠」）。
			// 再加 margin-bottom 与显示名的 margin-top 留出间距。
			const nameLineStyle = 'font-size:11px; color: var(--text-muted); margin-top: 4px; line-height: 1.4;';
			for (const id of saved) {
				const d = byId.get(id);
				if (!d) missing++;
				const ctx = d ? fmtTokenCount(d.contextLength) : '';
				const maxOut = d ? fmtTokenCount(d.maxOutput) : '';
				const price = d ? fmtModelPrice(d.pricing) : '';
				const caps = d ? fmtModelCaps(d) : '';
				rows += '<tr>'
					+ '<td style="overflow-wrap: anywhere;" title="' + sen(id) + '">'
					+ '<code style="font-size:12.5px; display:inline-block; max-width:100%; margin-bottom:2px;">' + sen(id) + '</code>'
					+ (d && d.name && d.name !== d.id ? '<div style="' + nameLineStyle + '">' + sen(d.name) + '</div>' : '')
					+ (!d ? '<div style="font-size:11px; color: var(--warning-color); margin-top: 4px; line-height: 1.4;">上游未返回该模型（可能已下线或名字不对）</div>' : '')
					+ '</td>'
					+ '<td style="' + numStyle + '">' + (ctx || dash) + '</td>'
					+ '<td style="' + numStyle + '">' + (maxOut || dash) + '</td>'
					+ '<td style="font-size: 12px; line-height: 1.5;">' + (price ? sen(price) : dash) + '</td>'
					+ '<td style="font-size: 12px; line-height: 1.9;">' + (caps || dash) + '</td>'
					+ '</tr>';
			}
			// table-layout:fixed + 各列显式宽度 → 不再出现横向滚动条，长内容改为优雅换行
			// 表头一律 nowrap + 留够宽度（否则「上下文/最大输出」会被单元格内边距挤成两行）
			box.innerHTML = '<table style="width: 100%; table-layout: fixed;"><thead><tr>'
				+ '<th style="width: auto; white-space: nowrap;">模型</th>'
				+ '<th style="width: 96px; text-align: right; white-space: nowrap;">上下文</th>'
				+ '<th style="width: 104px; text-align: right; white-space: nowrap;">最大输出</th>'
				+ '<th style="width: 136px; white-space: nowrap;">价格</th>'
				+ '<th style="width: 30%; white-space: nowrap;">能力 / 参数</th>'
				+ '</tr></thead><tbody>' + rows + '</tbody></table>'
				+ '<div class="section-note" style="margin-top: 8px;">只列本渠道已添加的 ' + saved.length + ' 个模型'
				+ (missing ? '（其中 ' + missing + ' 个上游未返回）' : '')
				+ '；本次上游共返回 ' + details.length + ' 个。要看全部请到「编辑」里拉取模型列表。</div>';
		}

		function loadModelSpecs(force) {
			if (!specProviderId) return;
			const box = document.getElementById('model-spec-content');
			box.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 24px;">正在从上游拉取…</div>';
			document.getElementById('model-spec-endpoint').textContent = '';
			apiFetch('/api/providers/models?id=' + encodeURIComponent(specProviderId) + (force ? '&refresh=1' : ''))
				.then(r => r.json())
				.then(data => {
					if (!data.success) {
						box.innerHTML = '<div style="color: var(--danger-color); padding: 16px; font-size: 13px;">拉取失败：' + sen(data.error || '') + '</div>';
						return;
					}
					document.getElementById('model-spec-endpoint').textContent =
						'实际请求地址：' + (data.endpoint || '') + (data.cached ? '（来自缓存）' : '');
					renderModelSpecs(data);
				})
				.catch(e => {
					box.innerHTML = '<div style="color: var(--danger-color); padding: 16px; font-size: 13px;">请求异常：' + sen(e.message) + '</div>';
				});
		}

		function openModelSpecModal(id) {
			const p = providersCache.find(x => x.id === id);
			if (!p) return;
			specProviderId = id;
			specProviderModels = (p.models || []).slice();
			document.getElementById('model-spec-title').textContent = '模型规格 · ' + p.name;
			document.getElementById('model-spec-modal').classList.add('active');
			loadModelSpecs(false);
		}

		function closeModelSpecModal() {
			document.getElementById('model-spec-modal').classList.remove('active');
		}

		function reloadModelSpecs() {
			loadModelSpecs(true);
		}

		function renderHealthTable() {
			const tbody = document.getElementById('health-table-body');
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p) { tbody.innerHTML = ''; return; }

			const models = p.models || [];
			if (!models.length) {
				tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-muted); padding: 24px;">这个渠道还没配置模型，先点「编辑」加上</td></tr>';
				return;
			}

			const saved = p.modelHealth || {};
			tbody.innerHTML = '';
			models.forEach((m, idx) => {
				const live = healthResults[m];
				let badge = '<span class="badge">未测试</span>';
				let h = saved[m];
				if (live && live.pending) {
					badge = '<span class="badge badge-info">测试中</span>';
					h = null;
				} else if (live) {
					badge = live.ok ? '<span class="badge badge-success">正常</span>' : '<span class="badge badge-danger">失败</span>';
					h = live;
				} else if (h) {
					badge = h.ok ? '<span class="badge badge-success">正常</span>' : '<span class="badge badge-danger">失败</span>';
				}

				const elapsed = (h && h.elapsed) ? h.elapsed + 'ms' : '—';
				let detail = '—';
				let detailTitle = '';
				if (h && !h.ok && (h.upstreamMessage || h.error)) {
					detail = sen(h.upstreamMessage || h.error);
					detailTitle = ' title="' + sen(h.error || '') + '"';
				} else if (h && h.ok && h.reply) {
					detail = '回复: ' + sen(h.reply);
				} else if (h && h.at) {
					detail = '测试于 ' + new Date(h.at).toLocaleString();
				}
				const hint = (h && !h.ok && h.hint)
					? '<div style="color: var(--warning-color); margin-top: 4px;">建议：' + sen(h.hint) + '</div>'
					: '';
				const sug = (h && !h.ok && h.suggestedModel) ? h.suggestedModel : '';
				const sugBlock = sug
					? '<div style="margin-top: 6px;">上游建议改用 <code>' + sen(sug) + '</code> '
						+ '<button class="btn btn-secondary" style="padding: 2px 10px; font-size: 11px; border-radius: 6px;" onclick="useSuggestedModelAt(' + idx + ')">换用它</button></div>'
					: '';

				const tr = document.createElement('tr');
				tr.innerHTML =
					'<td style="font-size:12px; word-break:break-all;">' + sen(m) + '</td>' +
					'<td>' + badge + '</td>' +
					'<td style="font-size:12px;">' + elapsed + '</td>' +
					'<td style="font-size:12px; line-height:1.6; word-break:break-word;"' + detailTitle + '>' + detail + hint + sugBlock + '</td>' +
					'<td><div style="display:flex; gap:6px; align-items:center; white-space:nowrap;">'
						+ '<button class="btn btn-secondary" style="padding:4px 8px; font-size:11px; border-radius:6px;" onclick="testOneModelAt(' + idx + ')">重测</button>'
						+ '<button class="btn btn-danger" style="padding:4px 8px; font-size:11px; border-radius:6px;" title="从该渠道的模型列表里删掉它，不用退出重进" onclick="removeModelAt(' + idx + ')">删除</button>'
						+ '</div></td>';
				tbody.appendChild(tr);
			});
		}

		function testOneModelAt(idx) {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p || !p.models || !p.models[idx]) return;
			healthResults[p.models[idx]] = { pending: true };
			renderHealthTable();
			runModelProbe([p.models[idx]]);
		}

		// 用一份新的模型列表回写渠道（编辑弹窗之外的快捷改动：一键换名 / 健康面板里删模型）。
		// ⚠️ 必须显式带上 geminiNative：POST 里未传 = undefined，JSON 序列化会把这个键丢掉 →
		//    等于把「原生 / 兼容」开关悄悄重置成「自动」。status 同理不能漏。
		async function saveProviderModels(p, nextModels) {
			return apiFetch('/api/providers', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					id: p.id,
					name: p.name,
					baseUrl: p.baseUrl,
					models: nextModels.join(String.fromCharCode(10)),
					status: p.status,
					geminiNative: p.geminiNative,
					streamFastPass: p.streamFastPass === true
				})
			});
		}

		// 上游建议了替代模型名（如 Gemini 提示 2.5-flash 已下线、改用 3.8-flash），一键替换
		async function useSuggestedModelAt(idx) {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p || !p.models || !p.models[idx]) return;
			const from = p.models[idx];
			const h = healthResults[from] || (p.modelHealth || {})[from];
			const to = h && h.suggestedModel;
			if (!to) return;
			if (!(await uiConfirm('把模型「' + from + '」替换为「' + to + '」？', { okText: '替换' }))) return;

			const res = await saveProviderModels(p, p.models.map(m => m === from ? to : m));
			if (!res.ok) {
				showToast('替换失败', 'error');
				return;
			}
			delete healthResults[from];
			showToast('已把 ' + from + ' 换成 ' + to);
			await loadProviders();
			renderHealthTable();
		}

		// 健康面板里直接删掉这个模型 —— 坏模型不用退出去编辑渠道，免得回来忘了到底是哪个有问题
		async function removeModelAt(idx) {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p || !p.models || !p.models[idx]) return;
			const m = p.models[idx];
			if (!(await uiConfirm('从渠道「' + p.name + '」里删除模型「' + m + '」？', { okText: '删除' }))) return;

			const res = await saveProviderModels(p, p.models.filter(x => x !== m));
			if (!res.ok) {
				showToast('删除失败', 'error');
				return;
			}
			delete healthResults[m];
			showToast('已删除模型 ' + m);
			await loadProviders();
			renderHealthTable();
		}

		// 从上游 /models 拉一份模型列表，避免手抄到已过期的模型名
		async function fetchUpstreamModels() {
			const id = document.getElementById('provider-id-edit').value;
			if (!id) {
				showToast('请先保存这个渠道，再从上游拉取模型列表', 'warning');
				return;
			}
			const btn = document.getElementById('btn-fetch-models');
			const oldText = btn.textContent;
			btn.disabled = true;
			btn.textContent = '拉取中...';
			const nl = String.fromCharCode(10);
			try {
				const res = await apiFetch('/api/providers/models?id=' + encodeURIComponent(id));
				const data = await res.json();
				if (!data.success) {
					showProviderTestResult(false,
						'拉取模型列表失败'
						+ (data.upstreamMessage ? nl + nl + '上游说：' + data.upstreamMessage : '')
						+ (data.hint ? nl + nl + '提示：' + data.hint : '')
						+ nl + nl + '实际请求地址：' + (data.endpoint || ''),
						data.error || '');
					showToast('拉取失败，详情见下方红框', 'error');
					return;
				}
				if (!data.models.length) {
					showToast('上游没有返回任何模型', 'warning');
					return;
				}
				document.getElementById('provider-models').value = data.models.join(nl);
				showToast('已拉取 ' + data.models.length + ' 个模型，请删掉不用的再保存');
			} catch (e) {
				showToast('拉取请求异常：' + e.message, 'error');
			} finally {
				btn.disabled = false;
				btn.textContent = oldText;
			}
		}

		function testAllProviderModels() {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p || !(p.models || []).length) {
				showToast('这个渠道还没配置模型', 'warning');
				return;
			}
			const models = p.models.slice();
			healthResults = {};
			models.forEach(m => { healthResults[m] = { pending: true }; });
			renderHealthTable();
			runModelProbe(models);
		}

		async function runModelProbe(models) {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p) return;
			const btnAll = document.getElementById('btn-test-all-models');
			if (btnAll) btnAll.disabled = true;
			try {
				const res = await apiFetch('/api/providers/test', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ id: p.id, models })
				});
				const data = await res.json();
				if (!res.ok) {
					showToast(data.error || '测试失败', 'error');
					models.forEach(m => { delete healthResults[m]; });
					renderHealthTable();
					return;
				}
				if (data.endpoint) {
					document.getElementById('health-endpoint').textContent = '实际请求地址：' + data.endpoint;
				}
				(data.results || []).forEach(r => { healthResults[r.model] = r; });
				renderHealthTable();
				await loadProviders();  // 拉回带 modelHealth 的数据，刷新渠道列表里的状态点
				if (data.truncated) showToast('单次最多测 12 个模型，其余已跳过', 'warning');
				const total = (data.results || []).length;
				const okCount = (data.results || []).filter(r => r.ok).length;
				showToast(okCount + ' / ' + total + ' 个模型正常', okCount === total ? 'success' : 'error');
			} catch (e) {
				showToast('测试请求异常：' + e.message, 'error');
			} finally {
				if (btnAll) btnAll.disabled = false;
			}
		}

		async function testProvider() {
			const payload = collectProviderForm();
			if (!payload.baseUrl) {
				showToast('请先填写 Base URL', 'warning');
				return;
			}
			const firstModel = pickFirstModel(payload.models);
			if (!firstModel) {
				showToast('请先填写至少一个模型名', 'warning');
				return;
			}
			const btn = document.getElementById('btn-test-provider');
			btn.disabled = true;
			showProviderTestResult(true, '正在测试连通性...');
			const nl = String.fromCharCode(10);
			try {
				const res = await apiFetch('/api/providers/test', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ id: payload.id, baseUrl: payload.baseUrl, apiKey: payload.apiKey, model: firstModel })
				});
				const data = await res.json();
				const r = (data.results || [])[0];
				if (!r) {
					showProviderTestResult(false, '测试未返回结果：' + (data.error || '未知错误'));
					return;
				}
				if (r.ok) {
					showProviderTestResult(true,
						'连通正常（' + r.elapsed + 'ms）· 模型 ' + r.model
						+ (r.reply ? ' · 回复: ' + r.reply : '')
						+ nl + '实际请求地址：' + r.endpoint);
				} else {
					showProviderTestResult(false, describeProbeFailure(r), r.error);
				}
			} catch (e) {
				showProviderTestResult(false, '测试请求异常：' + e.message);
			} finally {
				btn.disabled = false;
			}
		}

		async function saveProvider() {
			const payload = collectProviderForm();
			if (!payload.name || !payload.baseUrl) {
				showToast('渠道名和 Base URL 不能为空', 'warning');
				return;
			}
			const res = await apiFetch('/api/providers', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(payload)
			});
			if (res.ok) {
				closeProviderModal();
				loadProviders();
				showToast('第三方渠道已保存');
			} else {
				const err = await res.json().catch(() => ({}));
				showToast(err.error || '保存渠道失败', 'error');
			}
		}

		async function deleteProvider(id) {
			const p = providersCache.find(x => x.id === id);
			if (!(await uiConfirm('指向该渠道的模型映射也会一并清理。', { title: '删除渠道「' + (p ? p.name : '') + '」？', danger: true, okText: '删除' }))) return;
			const res = await apiFetch('/api/providers', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id })
			});
			if (res.ok) {
				const data = await res.json().catch(() => ({}));
				loadProviders();
				showToast(data.removedMappings ? '渠道已删除，同时清理了 ' + data.removedMappings + ' 条映射' : '渠道已删除');
			} else {
				showToast('删除渠道失败', 'error');
			}
		}

		let quotaGroupsCache = [];

		async function loadQuotaGroups() {
			const list = document.getElementById('quota-groups-list');
			if (!list) return;
			list.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 24px;">载入中...</div>';
			try {
				// 顺便刷渠道缓存 —— 列表里的渠道名和弹窗里的渠道下拉都要用
				providersCache = await (await apiFetch('/api/providers')).json();
				const data = await (await apiFetch('/api/quota-groups')).json();
				quotaGroupsCache = data.groups || [];
				const warn = document.getElementById('quota-d1-warning');
				if (warn) warn.classList.toggle('hidden', !!data.d1);
				// 调度总开关 + 冷却摘要（跟着组列表一起刷新，省一次请求）
				renderQuotaSchedBar(data);
				renderQuotaGroups(data);
			} catch (e) {
				list.innerHTML = '<div style="color: var(--danger-color); padding: 20px;">加载失败：' + sen(e.message) + '</div>';
			}
		}

		// 单个成员的展示单元：进度条 / 用量文字 / 状态徽标。
		// 已用次数拿不到时（未绑 D1）必须显式区分，不能当成 0 —— 否则会误判成"还有很多额度"。
		function quotaUsedCell(m, period) {
			// 注意：这里的 periodLabel 是「时间段」（本月/今日），与「计量单位」是两回事 —— 别混用。
			const periodLabel = period === 'month' ? '本月' : '今日';
			// 计量单位：成员设了 token 就显示 token，否则「次」—— 必须与调度实际口径一致，
			// 否则设成 token 的成员在卡片上仍写着「次」，用户会以为配额没生效（2026-10-08 实际反馈）。
			const meter = m.unit === 'token' ? 'token' : '次';
			const limit = Number(m.limit) || 0;
			// 本小时用量 = 负载均衡的依据，顺手展示（统计没启用时不显示）
			const hourTxt = (m.hourUsed === null || m.hourUsed === undefined)
				? ''
				: '<span style="color: var(--text-muted); margin-left: 6px;">本小时 ' + Number(m.hourUsed) + ' ' + meter + '</span>';
			// 冷却中优先展示：这是「上游报错 → 临时踢出调度」的状态，必须看得见
			if (Number(m.cooldownLeft) > 0) {
				const secs = Number(m.cooldownLeft);
				const t = secs >= 60 ? Math.ceil(secs / 60) + ' 分钟' : secs + ' 秒';
				return {
					bar: '',
					right: '<span style="color: var(--warning-color);">冷却中，还剩 ' + t + '</span>'
						+ '<span style="color: var(--text-muted); margin-left: 6px;">' + sen(m.cooldownReason || '上游报错') + '</span>',
					badge: '<span class="badge badge-warning">冷却中</span>'
				};
			}
			if (m.providerMissing) return { bar: '', right: '<span style="color: var(--danger-color);">渠道已删除，该成员会被跳过</span>', badge: '<span class="badge badge-danger">失效</span>' };
			if (m.providerDisabled) return { bar: '', right: '<span style="color: var(--text-muted);">所在渠道已停用</span>' + hourTxt, badge: '<span class="badge badge-warning">跳过</span>' };
			if (m.status === 'disabled') return { bar: '', right: '<span style="color: var(--text-muted);">已手动停用</span>' + hourTxt, badge: '<span class="badge badge-warning">停用</span>' };
			if (limit <= 0) {
				const t = m.used === null ? '不限（统计未启用）' : '不限 · ' + periodLabel + '已用 ' + m.used + ' ' + meter;
				return { bar: '', right: '<span style="color: var(--text-muted);">' + t + '</span>' + hourTxt, badge: '<span class="badge badge-info">不限</span>' };
			}
			if (m.used === null) {
				return { bar: '', right: '<span style="color: var(--warning-color);">无法统计（需绑定 D1）</span>', badge: '<span class="badge badge-warning">未知</span>' };
			}
			const used = Number(m.used) || 0;
			const pct = Math.min(100, Math.round(used / limit * 100));
			const color = used >= limit ? 'var(--danger-color)' : (pct >= 80 ? 'var(--warning-color)' : 'var(--success-color)');
			const bar = '<div style="height:6px; border-radius:3px; background: var(--border-color); overflow:hidden;"><div style="height:100%; width:' + pct + '%; background:' + color + ';"></div></div>';
			const right = '<span style="font-weight:500;">' + used + ' / ' + limit + '</span>'
				+ '<span style="color: var(--text-muted); margin-left: 6px;">剩 ' + Math.max(0, limit - used) + ' ' + meter + '</span>'
				+ hourTxt;
			const badge = used >= limit
				? '<span class="badge badge-danger">已用满</span>'
				: '<span class="badge badge-success">可用</span>';
			return { bar: bar, right: right, badge: badge };
		}

		// 调度总开关那一行：勾选状态 + 当前有几个成员在冷却（+ 一键解除）
		function renderQuotaSchedBar(data) {
			const toggle = document.getElementById('quota-sched-toggle');
			if (!toggle) return;
			const cdEl = document.getElementById('quota-sched-cooldowns');
			const btn = document.getElementById('quota-sched-clear');
			toggle.checked = data.scheduling !== false;
			let cooling = 0;
			for (const g of (data.groups || [])) {
				for (const m of (g.members || [])) if (Number(m.cooldownLeft) > 0) cooling++;
			}
			if (cdEl) {
				cdEl.textContent = cooling
					? ('当前有 ' + cooling + ' 个成员在冷却中（上游报错，被临时踢出调度）')
					: (data.scheduling === false ? '已关闭：按组内顺序取第一个未满，失败不切换' : '');
				cdEl.style.color = cooling ? 'var(--warning-color)' : 'var(--text-muted)';
			}
			if (btn) btn.style.display = cooling ? 'inline-flex' : 'none';
		}

		async function toggleQuotaScheduling(enabled) {
			const res = await apiFetch('/api/quota-scheduling', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ enabled: !!enabled })
			});
			if (!res.ok) { showToast('保存失败', 'error'); loadQuotaGroups(); return; }
			showToast(enabled ? '已开启负载均衡调度' : '已关闭：回到「按顺序取第一个未满」');
			loadQuotaGroups();
		}

		async function clearQuotaCooldowns() {
			const res = await apiFetch('/api/quota-scheduling', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ clearCooldowns: true })
			});
			if (!res.ok) { showToast('解除失败', 'error'); return; }
			showToast('已解除全部冷却');
			loadQuotaGroups();
		}

		function renderQuotaGroups(data) {
			const list = document.getElementById('quota-groups-list');
			const groups = data.groups || [];
			if (!groups.length) {
				list.innerHTML = '<div style="text-align:center; color: var(--text-muted); padding: 24px;">还没有配额组。点右上角「新建配额组」开始。</div>';
				return;
			}
			list.innerHTML = groups.map(g => {
				const period = g.period === 'month' ? 'month' : 'day';
				// ★ 卡片也按「优先级 → 配置顺序」稳定排序（2026-10-10）：与弹窗口径一致，
				// 让卡片上「第 N 行」就是真正会优先被调用的次序（此前按配置顺序渲染，纯误导）。
				const orderedMembers = (g.members || []).slice().sort((a, b) => {
					const pa = (Number(a.priority) > 0 ? Math.floor(Number(a.priority)) : 1);
					const pb = (Number(b.priority) > 0 ? Math.floor(Number(b.priority)) : 1);
					return pa - pb;   // 稳定排序 → 同优先级保持原配置顺序
				});
				const rows = orderedMembers.map((m, i) => {
					const c = quotaUsedCell(m, period);
					return '<div style="display:grid; grid-template-columns: 20px 110px minmax(0,1fr) 110px 170px 76px; gap:10px; align-items:center; padding:8px 0; border-top:1px solid var(--border-color);">'
						+ '<span style="font-size:12px; color: var(--text-muted);">' + (i + 1) + '</span>'
						+ '<span style="font-size:12.5px; color: var(--text-muted); overflow-wrap:anywhere;">' + sen(m.providerName || '（已删除）')
						+ (Number(m.priority) > 1 ? ' <span class="badge badge-info" title="组内优先级：数字越小越先用；同层内仍按负载均衡分摊">优先 ' + Math.floor(Number(m.priority)) + '</span>' : '')
						+ '</span>'
						+ '<code style="font-size:12.5px; word-break:break-all;">' + sen(m.model) + '</code>'
						+ '<div>' + c.bar + '</div>'
						+ '<div style="font-size:12.5px;">' + c.right + '</div>'
						+ '<div>' + c.badge + '</div>'
						+ '</div>';
				}).join('');
				const opened = !!quotaOpenGroups[g.id];
				const memberCount = (g.members || []).length;
				return '<div class="quota-group-card' + (opened ? ' open' : '') + '" data-group-id="' + sen(g.id) + '">'
					+ '<div class="quota-group-head" onclick="toggleQuotaGroup(this)">'
					+ '<span class="quota-group-arrow">' + (opened ? '▾' : '▸') + '</span>'
					+ '<strong style="font-size:14px;">' + sen(g.name) + '</strong>'
					+ '<span class="badge badge-info">' + (period === 'month' ? '每月' : '每天') + '</span>'
					+ (g.status === 'disabled' ? '<span class="badge badge-warning">已停用</span>' : '')
					+ '<span style="font-size:12px; color:var(--text-muted);">' + sen(g.resetLabel || '') + '</span>'
					+ '<span style="font-size:12px; color:var(--text-muted);">' + memberCount + ' 个成员</span>'
					// 组级上游冷却非默认时才显示，免得默认组挂一串噪音
					+ (Number(g.cooldownSec) > 0 ? '<span style="font-size:12px; color:var(--text-muted);" title="组级上游冷却：覆盖「模型池挤满」与「429 限流」两类">冷却 ' + Math.floor(Number(g.cooldownSec)) + 's</span>' : '')
					+ (g.streamFastPass === true ? '<span style="font-size:12px; color:var(--text-muted);" title="长流直通：流式不逐 chunk 读取，防超长输出被 CF CPU 限制掐断；token 统计记 0">直通</span>' : '')
					+ '<span style="margin-left:auto; display:flex; gap:10px; align-items:center;">'
					+ '<label class="switch" title="' + (g.status === 'disabled' ? '点击启用' : '点击停用') + '" onclick="event.stopPropagation()">'
					+ '<input type="checkbox" data-gid="' + sen(g.id) + '"' + (g.status === 'disabled' ? '' : ' checked') + ' onchange="toggleQuotaGroupStatus(this)">'
					+ '<span class="switch-track"></span>'
					+ '</label>'
					+ '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px;" data-id="' + sen(g.id) + '" onclick="event.stopPropagation(); openQuotaModal(this.dataset.id)">编辑</button>'
					+ '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px;" data-id="' + sen(g.id) + '" onclick="event.stopPropagation(); deleteQuotaGroup(this.dataset.id)">删除</button>'
					+ '</span></div>'
					+ '<div class="quota-group-body">'
					+ '<div style="font-size:12px; color: var(--text-muted); margin-top:6px;">客户端直接填 <code style="cursor:pointer;" title="点击复制" onclick="copyQuotaCallText(this)">TT:' + sen(g.name) + '</code></div>'
					+ '<div style="font-size:12px; color: var(--text-muted); margin-top:4px;">下次重置 <span style="font-weight:500; color:var(--text-main);">' + sen(g.nextResetText || '') + '</span>（北京时间）'
					+ (g.nextResetLocalText ? '<span style="margin-left:6px;">＝ 当地 ' + sen(g.nextResetLocalText || '') + '</span>' : '')
					+ '</div>'
					+ '<div style="margin-top:8px;">' + (rows || '<div style="padding:14px 0; color:var(--text-muted); font-size:12.5px;">还没有成员</div>') + '</div>'
					+ '</div></div>';
			}).join('');
		}

		function quotaEditRowHtml(m) {
			const p = m || {};
			const inputStyle = 'background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 10px 12px; border-radius: 8px; outline: none; font-size: 13px; font-family: inherit; width: 100%;';
			const opts = ['<option value="">选择渠道…</option>'].concat(
				providersCache.map(x => '<option value="' + sen(x.id) + '"' + (x.id === p.providerId ? ' selected' : '') + '>' + sen(x.name) + '</option>')
			).join('');
			// 计量单位：'token' = 按 token 总量，其它（缺省）= 按请求条数
			const unit = p.unit === 'token' ? 'token' : 'count';
			return '<div class="quota-member-row" style="display: grid; grid-template-columns: 22px 52px 1.2fr 1.5fr 78px 72px 72px 34px; gap: 8px; align-items: center;">'
				// 行首拖拽手柄（2026-10-10 方案 C）：拖动改「视觉顺序」，落定时按新顺序回写 priority；
				// 与右侧数字框双向联动（改数字自动重排、拖完自动改数字）
				+ '<span class="quota-drag-handle" draggable="true" title="拖动调整顺序（越靠上优先级越高）">⠿</span>'
				// 组内优先级：数字越小越先用；同优先级的人之间仍然按负载均衡分摊（两者正交）
				+ '<input type="number" class="quota-m-prio" min="1" step="1" placeholder="1" title="数字越小越先用；同一层内仍然按负载均衡分摊" value="' + (Number(p.priority) > 0 ? Math.floor(Number(p.priority)) : 1) + '" style="' + inputStyle + '">'
				+ '<select class="quota-m-provider" onchange="syncQuotaRowModels(this)" style="' + inputStyle + '">' + opts + '</select>'
				// 模型候选用自建下拉（2026-10-09 弃用原生 datalist，同映射页）：
				// 包一层 .map-combo 作定位容器，面板绝对定位向下展开、盖在右侧几列之上
				+ '<div class="map-combo" style="min-width: 0;">'
				+ '<input type="text" class="quota-m-model" placeholder="下拉选，或手输" value="' + sen(p.model || '') + '" style="' + inputStyle + '">'
				+ '<div class="map-combo-menu"></div>'
				+ '</div>'
				+ '<input type="number" class="quota-m-limit" min="0" placeholder="0" value="' + (Number(p.limit) || 0) + '" style="' + inputStyle + '">'
				+ '<select class="quota-m-unit" style="' + inputStyle + '">'
				+ '<option value="count"' + (unit === 'count' ? ' selected' : '') + '>次数</option>'
				+ '<option value="token"' + (unit === 'token' ? ' selected' : '') + '>token</option>'
				+ '</select>'
				+ '<select class="quota-m-status" style="' + inputStyle + '">'
				+ '<option value="active"' + (p.status !== 'disabled' ? ' selected' : '') + '>启用</option>'
				+ '<option value="disabled"' + (p.status === 'disabled' ? ' selected' : '') + '>停用</option>'
				+ '</select>'
				+ '<button class="btn btn-secondary" onclick="removeQuotaMemberRow(this)" style="padding:8px 4px; font-size:14px; color: var(--danger-color);">×</button>'
				+ '</div>';
		}

		// 换了渠道 → 该行的模型候选跟着换（只列这个渠道的模型，避免选到别家的）。
		// 只重填内容、不弹开面板 —— 开合由 focus/input 事件管（2026-10-09 弃用原生 datalist）
		function syncQuotaRowModels(sel) {
			const row = sel.closest('.quota-member-row');
			if (!row) return;
			const menu = row.querySelector('.map-combo-menu');
			if (!menu) return;
			const p = providersCache.find(x => x.id === sel.value);
			fillComboMenu(menu, (p && p.models) || [], '', false);
		}

		// 打开弹窗后把每行的模型候选初始化一遍（编辑已有成员时要还原）
		function syncAllQuotaRowModels() {
			[].slice.call(document.querySelectorAll('#quota-members .quota-m-provider'))
				.forEach(sel => syncQuotaRowModels(sel));
		}

		function addQuotaMemberRow() {
			const box = document.getElementById('quota-members');
			if (box) box.insertAdjacentHTML('beforeend', quotaEditRowHtml(null));
			// 新行默认优先级 1；若组里已有其它优先级，让它跟着重排到「同层末尾」，所见即所得
			if (box) sortQuotaRowsByPriority(box);
		}

		function removeQuotaMemberRow(btn) {
			const row = btn.closest('.quota-member-row');
			if (row) row.remove();
		}

		// ---- 成员排序（2026-10-10，方案 C）----
		// 读一行的优先级（留空/非法一律 1，与服务端 collect 的口径一致）
		function quotaRowPrio(row) {
			const v = (row.querySelector('.quota-m-prio') || {}).value;
			const n = Math.floor(Number(v));
			return Number.isFinite(n) && n > 0 ? n : 1;
		}

		// 按「优先级 → 当前 DOM 顺序」稳定重排成员行。
		// 只在界面上重排，不改数字；拖拽落定时另走 applyDragTier() 并进目标行那一层。
		function sortQuotaRowsByPriority(box) {
			if (!box) return;
			const rows = [].slice.call(box.querySelectorAll('.quota-member-row'));
			if (rows.length < 2) return;
			// ★ 用「行当前下标」当稳定排序的次序键：sort 在各浏览器已稳定，
			//   但显式带上 idx 可以保证「同优先级保持相对顺序」，不依赖引擎实现。
			const keyed = rows.map((r, i) => ({ r, i, p: quotaRowPrio(r) }));
			keyed.sort((a, b) => (a.p - b.p) || (a.i - b.i));
			// 若顺序没变就不动 DOM（避免打断正在输入的数字框焦点）
			const changed = keyed.some((k, i) => k.r !== rows[i]);
			if (!changed) return;
			const active = document.activeElement;
			keyed.forEach(k => box.appendChild(k.r));
			// 重排后把焦点还回原来的输入框（拖拽/改数字时最容易丢焦点）
			if (active && box.contains(active)) active.focus();
		}

		// 拖拽落定：被拖的行「并到它落到的那一行所在的层」（2026-10-10，方案 C）。
		// ★ 刻意**不做**「按视觉顺序整表重编号成 1,2,3…」：那会把用户原本手配的相同优先级
		//   （如 3,3 表示「这两个同层、内部负载均衡」）全部拆散，等于静默废掉层内负载均衡
		//   ——而那个语义是 2026-10-09 拍板的，且 MIN(floor(p),1) 的初始默认值就是全员 1 层。
		//   拖拽只负责「排到第几位 / 并进哪一层」；要新建层或脱离某层，仍然手动改数字。
		function applyDragTier(movedRow, overRow) {
			if (!movedRow || !overRow || movedRow === overRow) return;
			const inp = movedRow.querySelector('.quota-m-prio');
			if (inp) inp.value = String(quotaRowPrio(overRow));
		}

		// 挂一次拖拽事件（事件委托到 #quota-members）
		function initQuotaDrag() {
			const box = document.getElementById('quota-members');
			if (!box || box.dataset.dragWired === '1') return;
			box.dataset.dragWired = '1';
			let dragRow = null;

			const clearMarks = () => {
				[].slice.call(box.querySelectorAll('.quota-member-row')).forEach(r => {
					r.classList.remove('dragging', 'drop-above', 'drop-below');
				});
			};

			box.addEventListener('dragstart', (e) => {
				const handle = e.target.closest ? e.target.closest('.quota-drag-handle') : null;
				if (!handle) return;
				dragRow = handle.closest('.quota-member-row');
				if (!dragRow) return;
				dragRow.classList.add('dragging');
				if (e.dataTransfer) {
					e.dataTransfer.effectAllowed = 'move';
					// Firefox 必须有 setData 才会触发后续 drag 事件
					try { e.dataTransfer.setData('text/plain', ''); } catch (_) { }
				}
			});

			box.addEventListener('dragover', (e) => {
				if (!dragRow) return;
				e.preventDefault();               // 不 preventDefault 不会触发 drop
				if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
				const over = e.target.closest ? e.target.closest('.quota-member-row') : null;
				[].slice.call(box.querySelectorAll('.quota-member-row')).forEach(r => r.classList.remove('drop-above', 'drop-below'));
				if (!over || over === dragRow) return;
				// 以行的垂直中线判断插到上面还是下面
				const rect = over.getBoundingClientRect();
				const after = e.clientY > rect.top + rect.height / 2;
				over.classList.add(after ? 'drop-below' : 'drop-above');
			});

			box.addEventListener('drop', (e) => {
				if (!dragRow) return;
				e.preventDefault();
				const over = e.target.closest ? e.target.closest('.quota-member-row') : null;
				if (over && over !== dragRow) {
					const rect = over.getBoundingClientRect();
					const after = e.clientY > rect.top + rect.height / 2;
					if (after) over.insertAdjacentElement('afterend', dragRow);
					else over.insertAdjacentElement('beforebegin', dragRow);
					// 落到谁旁边就并进谁那一层（同级者之间仍按负载均衡分摊）
					applyDragTier(dragRow, over);
					sortQuotaRowsByPriority(box);
				}
				clearMarks();
				dragRow = null;
			});

			box.addEventListener('dragend', () => { clearMarks(); dragRow = null; });
		}

		// ---- 配额组的「预设下拉」----
		// 刻意**只向下展开**：向上弹会盖住相邻字段（2026-10-08 用户实测否掉）。
		// 也刻意不用原生 <datalist>：它的面板宽度不受 CSS 控制，做不出与输入框对齐。
		function quotaComboEls(inputId) {
			return {
				combo: document.getElementById(inputId + '-combo'),
				input: document.getElementById(inputId),
				menu: document.getElementById(inputId + '-menu')
			};
		}

		function quotaComboClose(inputId) {
			const combo = document.getElementById(inputId + '-combo');
			if (combo) combo.classList.remove('open');
		}

		function quotaComboCloseAll() {
			['quota-period', 'quota-reset-tz'].forEach(quotaComboClose);
		}

		function quotaComboOpen(inputId) {
			const { combo, menu, input } = quotaComboEls(inputId);
			if (!combo || !menu) return;
			quotaComboCloseAll();
			combo.classList.add('open');
			// 当前值对应的那项高亮 —— 免得"点开发现第一项就是现在这个"让人困惑。
			// 输入框为空时按**默认值**高亮（周期默认「每天」、时区默认「UTC」），与实际生效的一致。
			const fallback = inputId === 'quota-period' ? '每天' : 'UTC';
			const cur = String((input && input.value) || '').trim() || fallback;
			[].slice.call(menu.querySelectorAll('.quota-combo-item')).forEach(function (it) {
				it.classList.toggle('on', it.dataset.value === cur);
			});
			// 展开后贴住可见区（弹窗内容区可滚动）—— 「只向下弹」的配套：
			// 被 .modal-body 裁到就往上拉，再按剩余空间收高度（共用 map-combo 那套，2026-10-09）
			fitComboMenu(menu);
		}

		function quotaComboToggle(inputId) {
			const combo = document.getElementById(inputId + '-combo');
			if (combo && combo.classList.contains('open')) quotaComboClose(inputId);
			else quotaComboOpen(inputId);
		}

		function quotaComboPick(inputId, value) {
			const input = document.getElementById(inputId);
			if (!input) return;
			input.value = value;
			quotaComboClose(inputId);
			updateQuotaResetPreview();
		}

		function quotaComboKey(ev, inputId) {
			if (ev.key === 'Escape') quotaComboClose(inputId);
			else if (ev.key === 'ArrowDown') { ev.preventDefault(); quotaComboOpen(inputId); }
		}

		// 点预设项 → 填入；点别处 → 收起（事件委托，不给每项挂监听）
		document.addEventListener('click', function (ev) {
			const t = ev.target;
			if (!t || !t.closest) return;
			const item = t.closest('.quota-combo-item');
			if (item) {
				const menu = item.closest('.quota-combo-menu');
				if (menu && menu.id) quotaComboPick(menu.id.replace(/-menu$/, ''), item.dataset.value);
				return;
			}
			if (!t.closest('.quota-combo')) quotaComboCloseAll();
		});

		// ---- 重置基准：把「本地时刻 + 时区」实时换算成 UTC 几点，省得用户自己算 ----
		// 偏移范围统一夹在 ±840 分钟（与服务端一致）
		function clampOffsetMinutes(v) {
			return Math.max(-840, Math.min(840, Math.round(Number(v) || 0)));
		}

		// 从前端自己的「建议列表」里查一项：先按可读名（value）精确匹配，再按预设键（data-key）兜底。
		// ⚠️⚠️ 前端**绝不能**引用 Worker 顶层的时区预设常量 —— 那是服务端作用域的东西，浏览器里
		//    根本没有，一引用就是 ReferenceError：openQuotaModal 直接抛错，
		//    「新建 / 编辑配额组」按钮全部无反应（2026-10-08 实际踩过）。预设信息只能从 DOM 读。
		function quotaTzOptionOf(text) {
			const menu = document.getElementById('quota-reset-tz-menu');
			if (!menu) return null;
			const t = String(text == null ? '' : text).trim();
			if (!t) return null;
			const opts = [].slice.call(menu.querySelectorAll('.quota-combo-item'));
			const exact = opts.find(function (o) { return o.dataset.value === t; })
				|| opts.find(function (o) { return o.dataset.key === t; });
			if (exact) return exact;
			// 名字打不全时的宽容处理：**仅在该前缀唯一命中时**才认。
			// 「北京时间」→ 唯一命中「北京时间 UTC+8」✓
			// 「美国太平洋」→ 同时命中夏令时/冬令时两项 → 不认，让用户补全
			//（宁可多打几个字，也不要静默猜错一个时区 —— 那会让配额在错误的时间重置）。
			const byPrefix = opts.filter(function (o) { return String(o.dataset.value || '').indexOf(t) === 0; });
			return byPrefix.length === 1 ? byPrefix[0] : null;
		}

		// 分钟偏移 → 'UTC+08:00' / 'UTC-07:30'：把自定义偏移显示成一眼可读的形式，
		// 免得用户把「8」（8 分钟）误当成 UTC+8。
		function fmtOffsetMinutes(min) {
			const n = Math.round(Number(min) || 0);
			const abs = Math.abs(n);
			return 'UTC' + (n < 0 ? '-' : '+')
				+ String(Math.floor(abs / 60)).padStart(2, '0') + ':' + String(abs % 60).padStart(2, '0');
		}

		// 基准时区输入 → { resetTz, resetTzOffset, offset, ok }
		// 认三种写法：① 建议列表里的预设名或预设键  ② UTC+8 / GMT-7:30  ③ 纯数字分钟（480 = UTC+8）
		// ⚠️ 正则一律用 [0-9] / [ ] 这类写法，别用反斜杠转义简写：内联脚本整体位于一个
		//    模板字符串里，反斜杠会被吃掉（简写退化成普通字母），正则会静默失效且不报错（硬约定 #4）。
		function parseQuotaTzInput(text) {
			const t = String(text == null ? '' : text).trim();
			// 留空 = 用默认 UTC（同理：新建时是空的）
			if (!t) return { resetTz: 'utc', resetTzOffset: null, offset: 0, ok: true };
			const um = t.match(/^(?:utc|gmt)[ ]*([+-])[ ]*([0-9]{1,2})(?::([0-9]{2}))?$/i);
			if (um) {
				const sign = um[1] === '-' ? -1 : 1;
				const min = clampOffsetMinutes(sign * (Number(um[2]) * 60 + Number(um[3] || 0)));
				return { resetTz: 'custom', resetTzOffset: min, offset: min, ok: true };
			}
			if (/^[+-]?[0-9]+$/.test(t)) {
				const min = clampOffsetMinutes(Number(t));
				return { resetTz: 'custom', resetTzOffset: min, offset: min, ok: true };
			}
			const opt = quotaTzOptionOf(t);
			if (opt && opt.dataset.key) {
				return { resetTz: opt.dataset.key, resetTzOffset: null, offset: Number(opt.dataset.offset) || 0, ok: true };
			}
			return { resetTz: 'utc', resetTzOffset: null, offset: 0, ok: false };
		}

		// 组配置 → 输入框该显示的文本（预设显示可读名；自定义偏移显示分钟数）
		function quotaTzDisplay(group) {
			const key = (group && group.resetTz) ? String(group.resetTz) : 'utc';
			if (key !== 'custom') {
				const opt = quotaTzOptionOf(key);
				if (opt) return opt.dataset.value;
				// 未知键：服务端按偏移 0 处理（等同 UTC），这里也照实显示 UTC，别显示成 "0"
				return 'UTC';
			}
			return String(Number(group && group.resetTzOffset) || 0);
		}

		// 重置周期输入 → { period, resetDay, ok }
		// 「每天」→ day；含 1-31 的数字 → 每月第 N 日；只说「每月」→ 每月 1 日
		function parseQuotaPeriodInput(text) {
			const t = String(text == null ? '' : text).trim();
			// 留空 = 用默认「每天」。新建时输入框是空的，不能因此拦住保存
			if (!t) return { period: 'day', resetDay: 1, ok: true };
			if (/^(每天|每日|day|日)$/i.test(t)) return { period: 'day', resetDay: 1, ok: true };
			const m = t.match(/([0-9]{1,2})/);
			if (m) {
				const d = Number(m[1]);
				if (d >= 1 && d <= 31) return { period: 'month', resetDay: d, ok: true };
				return { period: 'month', resetDay: 1, ok: false };
			}
			if (/^(每月|月|month|monthly)$/i.test(t)) return { period: 'month', resetDay: 1, ok: true };
			return { period: 'day', resetDay: 1, ok: false };
		}

		function quotaResetOffsetMinutes() {
			return Number(parseQuotaTzInput(document.getElementById('quota-reset-tz').value).offset) || 0;
		}

		function hhmm(min) {
			const m = ((Math.round(Number(min) || 0) % 1440) + 1440) % 1440;
			return String(Math.floor(m / 60)).padStart(2, '0') + ':' + String(m % 60).padStart(2, '0');
		}

		function updateQuotaNamePreview() {
			const code = document.getElementById('quota-name-call-code');
			if (!code) return;
			// 边打字边把完整调用名拼出来 —— 用户不用记前缀，照着复制就行
			const name = (document.getElementById('quota-name').value || '').trim();
			code.textContent = 'TT:' + (name || '<组名>');
		}

		function copyQuotaCallText(el) {
			const val = ((el && el.textContent) || '').trim();
			if (!val || val === 'TT:<组名>') {
				showToast('请先填写组名', 'warning');
				return;
			}
			const input = document.createElement('input');
			input.value = val;
			document.body.appendChild(input);
			input.select();
			document.execCommand('copy');
			document.body.removeChild(input);
			showToast('调用名已复制：' + val);
		}

		function copyQuotaCallName() {
			copyQuotaCallText(document.getElementById('quota-name-call-code'));
		}

		function updateQuotaResetPreview() {
			const el = document.getElementById('quota-reset-preview');
			if (!el) return;
			const parts = (document.getElementById('quota-reset-time').value || '00:00').split(':');
			const localMin = (Number(parts[0]) || 0) * 60 + (Number(parts[1]) || 0);
			const off = quotaResetOffsetMinutes();
			const utcMin = ((localMin - off) % 1440 + 1440) % 1440;
			// 时区已是自由输入：认得出来就显示原文，自定义偏移额外标出规范化写法
			//（输入「8」其实是 8 分钟 → 标成 UTC+00:08，一眼就能看出不是 UTC+8）；
			// 认不出来就明确标注会回落 UTC，不静默吞掉。
			const tzRaw = String(document.getElementById('quota-reset-tz').value || '').trim();
			const tzParsed = parseQuotaTzInput(tzRaw);
			let tzLabel;
			if (!tzRaw) tzLabel = 'UTC';
			else if (!tzParsed.ok) tzLabel = tzRaw + '（无法识别 → 按 UTC）';
			else if (tzParsed.resetTz === 'custom') tzLabel = tzRaw + '（' + fmtOffsetMinutes(tzParsed.offset) + '）';
			else tzLabel = tzRaw;
			const per = parseQuotaPeriodInput(document.getElementById('quota-period').value);
			const period = !per.ok ? '（重置周期无法识别）'
				: (per.period === 'month' ? ('每月 ' + per.resetDay + ' 日') : '每天');
			// 这三行说的是同一个瞬间 —— 并排写出来，免得被当成三个不同的时间
			el.textContent = '同一个时刻的三种写法：当地 ' + hhmm(localMin) + '（' + tzLabel + '）'
				+ ' ＝ UTC ' + hhmm(utcMin)
				+ ' ＝ 北京时间 ' + hhmm(utcMin + 480)
				+ '。' + period + '按这个时刻切分周期。'
				+ (utcMin % 60 === 0 ? '' : '（非整点会向外取整到整点，宁可提前切换，也不超发上游额度）');
		}

		function openQuotaModal(id) {
			const g = id ? quotaGroupsCache.find(x => x.id === id) : null;
			document.getElementById('quota-id-edit').value = g ? g.id : '';
			document.getElementById('quota-name').value = g ? g.name : '';
			// 周期/时区：**新建时留空**，只在编辑时回显当前值。
			// 之前新建会预填「每天」/「UTC」，用户每次都得先删掉才能填别的 —— 那个体验很烦
			// （2026-10-08 用户明确反馈）。留空时的语义 = 用默认（每天 / UTC），见解析函数。
			const resetDay = (g && Number(g.resetDay) > 0) ? Math.floor(Number(g.resetDay)) : 1;
			document.getElementById('quota-period').value = g
				? ((g.period === 'month') ? ('每月 ' + resetDay + ' 日') : '每天')
				: '';
			// 时区：预设显示它的可读名；自定义偏移直接显示分钟数
			document.getElementById('quota-reset-tz').value = g ? quotaTzDisplay(g) : '';
			document.getElementById('quota-reset-time').value = (g && g.resetLocalTime) ? g.resetLocalTime : '00:00';
			// 先设好值再算预览，否则算的是上一次的状态
			updateQuotaResetPreview();
			updateQuotaNamePreview();
			document.getElementById('quota-status-active').checked = g ? g.status !== 'disabled' : true;
			document.getElementById('quota-sticky').checked = !(g && g.sticky === false);
			document.getElementById('quota-fastpass').checked = !!(g && g.streamFastPass === true);
			// 上游冷却：0/缺省 → 输入框留空（= 跟随全局默认），别回显成 "0" 让人以为设了值
			document.getElementById('quota-cooldown').value = (g && Number(g.cooldownSec) > 0) ? Math.floor(Number(g.cooldownSec)) : '';
			document.getElementById('quota-modal-title').innerText = g ? '编辑配额组' : '新建配额组';

			const members = (g && g.members) || [];
			// ★ 按「优先级 → 配置顺序」稳定排序后展示（2026-10-10）：此前直接按配置顺序渲染，
			// 卡片上看到的顺序和真正选号用的顺序脱节，用户根本看不出实际调用次序。
			const orderedMembers = members.slice().sort((a, b) => {
				const pa = (Number(a.priority) > 0 ? Math.floor(Number(a.priority)) : 1);
				const pb = (Number(b.priority) > 0 ? Math.floor(Number(b.priority)) : 1);
				return pa - pb;   // Array.sort 稳定 → 同优先级保持原配置顺序
			});
			document.getElementById('quota-members').innerHTML = orderedMembers.length
				? orderedMembers.map(m => quotaEditRowHtml(m)).join('')
				: quotaEditRowHtml(null);
			// 行插进 DOM 之后才能按各自选中的渠道填充模型候选
			syncAllQuotaRowModels();
			initQuotaDrag();

			document.getElementById('quota-modal-hint').textContent = providersCache.length
				? '已用量从调用统计里实时读取，不在这个弹窗里设置。上限填 0 表示不限；单位按你填的数字口径选「次数」或「token」。'
				: '还没有第三方渠道 —— 请先到「第三方渠道」添加一个，再回来配置配额组。';

			document.getElementById('quota-modal').classList.add('active');
		}

		function closeQuotaModal() {
			document.getElementById('quota-modal').classList.remove('active');
		}

		function collectQuotaMembers() {
			const rows = [].slice.call(document.querySelectorAll('#quota-members .quota-member-row'));
			return rows.map(r => ({
				providerId: r.querySelector('.quota-m-provider').value,
				model: r.querySelector('.quota-m-model').value.trim(),
				limit: Math.max(0, Math.floor(Number(r.querySelector('.quota-m-limit').value) || 0)),
				// 组内优先级：留空/非法一律回落 1（最高优先级）
				priority: Math.max(1, Math.floor(Number((r.querySelector('.quota-m-prio') || {}).value) || 1)),
				// 单位：只认 'token'，其余一律 'count'（与服务端白名单一致）
				unit: (r.querySelector('.quota-m-unit') || {}).value === 'token' ? 'token' : 'count',
				status: r.querySelector('.quota-m-status').value
			})).filter(m => m.providerId && m.model);
		}

		async function saveQuotaGroup() {
			// 周期 / 时区现在是自由输入 → 先解析；解析不出来就明确拦下，不静默兜底成别的语义
			const per = parseQuotaPeriodInput(document.getElementById('quota-period').value);
			const tz = parseQuotaTzInput(document.getElementById('quota-reset-tz').value);
			if (!per.ok) { showToast('重置周期请填「每天」或「每月 N 日」（N 为 1-31）', 'warning'); return; }
			if (!tz.ok) { showToast('基准时区请从建议列表选择，或直接填偏移分钟数（如 480 / -420）', 'warning'); return; }
			// 上游冷却（秒）：留空 = 跟随全局默认；填了就必须是 0~600 —— 先给即时提示，
			// 服务端还有一道同样的校验（防绕过界面直接打接口）
			const coolRaw = document.getElementById('quota-cooldown').value.trim();
			let cooldownSec = 0;
			if (coolRaw !== '') {
				const cn = Number(coolRaw);
				if (!Number.isFinite(cn) || cn < 0 || cn > 600) {
					showToast('上游冷却请填 0~600 的秒数（留空或 0 = 跟随全局默认）', 'warning');
					return;
				}
				cooldownSec = Math.floor(cn);
			}
			const payload = {
				name: document.getElementById('quota-name').value.trim(),
				period: per.period,
				resetDay: per.resetDay,
				status: document.getElementById('quota-status-active').checked ? 'active' : 'disabled',
				resetTz: tz.resetTz,
				resetLocalTime: document.getElementById('quota-reset-time').value || '00:00',
				resetTzOffset: tz.resetTz === 'custom' ? (Number(tz.resetTzOffset) || 0) : 0,
				sticky: document.getElementById('quota-sticky').checked === true,
				streamFastPass: document.getElementById('quota-fastpass').checked === true,
				cooldownSec,
				members: collectQuotaMembers()
			};
			const editId = document.getElementById('quota-id-edit').value;
			if (editId) payload.id = editId;

			if (!payload.name) { showToast('组名不能为空', 'warning'); return; }
			if (payload.name.indexOf('/') !== -1) { showToast('组名不能包含 "/"', 'warning'); return; }
			if (!payload.members.length) { showToast('至少需要一个有效成员（渠道 + 模型名）', 'warning'); return; }

			const res = await apiFetch('/api/quota-groups', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(payload)
			});
			if (!res.ok) {
				const data = await res.json().catch(() => ({}));
				showToast(data.error || '保存失败', 'error');
				return;
			}
			closeQuotaModal();
			showToast('配额组已保存');
			loadQuotaGroups();
		}

		async function deleteQuotaGroup(id) {
			const g = quotaGroupsCache.find(x => x.id === id);
			if (!(await uiConfirm('指向它的模型映射会失效。', { title: '删除配额组「' + (g ? g.name : '') + '」？', danger: true, okText: '删除' }))) return;
			const res = await apiFetch('/api/quota-groups', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id })
			});
			if (res.ok) { showToast('配额组已删除'); loadQuotaGroups(); }
			else showToast('删除失败', 'error');
		}

		// 「目标模型」的下拉候选：所有启用渠道的模型 + 内置 CF 预设的值。
		// 只是加速输入 —— 输入框本身仍可手输任意值（渠道里没填的模型也能临时用）。
		// 2026-10-09：原生 datalist 的弹出面板不受 CSS 控制（字体小、宽度窄、选项截断），
		// 改自建下拉渲染候选；教训同配额「重置基准」：只向下展开、绝不向上翻。
		let mapTargetCandidates = [];
		function refreshMappingTargetOptions() {
			const menu = document.getElementById('map-target-menu');
			if (!menu) return;
			const seen = {};
			const opts = [];
			const push = (v) => {
				if (!v || seen[v]) return;
				seen[v] = true;
				opts.push(v);
			};
			providersCache.forEach(p => {
				if (p.status === 'disabled') return;
				(p.models || []).forEach(m => push('provider:' + p.name + '/' + m));
			});
			Object.keys(defaultMappings).forEach(k => push(defaultMappings[k]));
			mapTargetCandidates = opts;
			// ★ 只填内容、不弹开（withOpen=false）：切到映射页（loadSettings）时刷新候选，
			//   旧实现走 renderMapTargetMenu → withOpen=true → 面板没点就自动弹开、还盖住
			//   下方的映射表格（2026-10-09 用户截图报障）。开合只归 focus/input 事件管。
			fillComboMenu(menu, mapTargetCandidates, '', false);
		}

		// 面板「贴住可见区」——2026-10-09 修（用户报「模型多了，滑动带动整个窗口」）。
		// 面板是 .map-combo 下的绝对定位子元素，会被**最近的滚动容器**（弹窗的 .modal-body）裁掉：
		// 无头 Chrome + 真实滚轮实测 —— 弹窗里靠底部的成员行，面板整块落到 .modal-body 下沿之外，
		// 滚轮命中的是 .modal-body 而不是面板 → 滚的是整个弹窗，列表纹丝不动。
		// 这里先「尽量」把它拉进可见区，再把高度收到可见区剩余空间以内：面板自带滚动、不再被裁。
		function fitComboMenu(menu) {
			if (!menu) return;
			menu.style.maxHeight = '';   // 先按原始高度量（上一次留下的限制会干扰取数）
			const cssMax = parseFloat(getComputedStyle(menu).maxHeight) || 320;
			const box = menu.closest('.modal-body');   // 弹窗内的面板被它裁；映射页（非弹窗）没有这层
			const room = () => {
				const bottom = box ? box.getBoundingClientRect().bottom : window.innerHeight;
				return Math.min(window.innerHeight, bottom) - menu.getBoundingClientRect().top - 6;
			};
			const need = Math.min(cssMax, menu.getBoundingClientRect().height) + 4;   // 面板「想要」多高
			// 还被裁就往上拉（最多 3 次：拉不动说明内容已经到底，别在这死循环）
			for (let i = 0; box && i < 3 && room() < need; i++) box.scrollTop += (need - room());
			// 兜底 88px：空间实在不够时宁可留一点裁切，也别把面板压成一条缝、点不动
			menu.style.maxHeight = Math.max(88, Math.min(cssMax, room())) + 'px';
		}

		// 渲染候选内容的公共件（映射页 + 配额成员行共用）：
		// 按关键字小写包含过滤；withOpen=true 时同时按「有无结果」开合面板
		function fillComboMenu(menu, candidates, kwRaw, withOpen) {
			const kw = (kwRaw || '').toLowerCase();
			const items = (candidates || []).filter(v => !kw || String(v).toLowerCase().indexOf(kw) !== -1);
			menu.innerHTML = items.map(v =>
				'<button type="button" class="map-combo-item" data-value="' + sen(v) + '" title="' + sen(v) + '">' + sen(v) + '</button>'
			).join('');
			if (withOpen) {
				const combo = menu.closest('.map-combo');
				if (combo) {
					combo.classList.toggle('open', items.length > 0);
					if (items.length) fitComboMenu(menu);   // 展开后贴住可见区（否则被 .modal-body 裁掉）
				}
			}
		}

		// 按关键字渲染映射页候选面板；关键字为空 = 全量。开合挂在 .map-combo 的 open 类上
		function renderMapTargetMenu(kwRaw) {
			const menu = document.getElementById('map-target-menu');
			if (!menu) return;
			fillComboMenu(menu, mapTargetCandidates, kwRaw, true);
		}

		// 配额弹窗成员行的候选：跟着该行选中的渠道走（现查 providersCache，不落变量）
		function fillQuotaRowMenu(row, kwRaw) {
			if (!row) return;
			const menu = row.querySelector('.map-combo-menu');
			if (!menu) return;
			const sel = row.querySelector('.quota-m-provider');
			const p = sel ? providersCache.find(x => x.id === sel.value) : null;
			fillComboMenu(menu, (p && p.models) || [], kwRaw, true);
		}

		// 成员行是动态生成的 → 在 #quota-members 上事件委托只挂一次
		// （focus 给全量列表、input 按关键字过滤、Escape 收起、mousedown 选中）
		function initQuotaModelCombo() {
			const box = document.getElementById('quota-members');
			if (!box) return;
			box.addEventListener('focusin', (e) => {
				if (!e.target.closest('.quota-m-model')) return;
				// 只保留当前这一个：别处还开着的面板全部收掉
				document.querySelectorAll('.map-combo.open').forEach(el => el.classList.remove('open'));
				fillQuotaRowMenu(e.target.closest('.quota-member-row'), '');
			});
			box.addEventListener('input', (e) => {
				if (!e.target.closest('.quota-m-model')) return;
				fillQuotaRowMenu(e.target.closest('.quota-member-row'), e.target.value.trim());
			});
			// 改「优先」数字 → 输入停顿后自动把该行插到对应位置（所见即所得）。
			// 用定时器防抖：连按上下箭头改数字时不要每一下都重排（会把焦点/手感打乱）。
			let prioTimer = null;
			box.addEventListener('input', (e) => {
				if (!e.target.closest('.quota-m-prio')) return;
				if (prioTimer) clearTimeout(prioTimer);
				prioTimer = setTimeout(() => sortQuotaRowsByPriority(box), 500);
			});
			// 数字框按下回车也立即生效（不必等 500ms）
			box.addEventListener('keydown', (e) => {
				if (e.key !== 'Enter' || !e.target.closest('.quota-m-prio')) return;
				e.preventDefault();
				if (prioTimer) clearTimeout(prioTimer);
				sortQuotaRowsByPriority(box);
			});
			box.addEventListener('keydown', (e) => {
				if (e.key !== 'Escape') return;
				const combo = e.target.closest('.map-combo');
				if (combo) combo.classList.remove('open');
			});
			box.addEventListener('mousedown', (e) => {
				const item = e.target.closest('.map-combo-item');
				if (!item) return;
				e.preventDefault(); // mousedown 先于 blur：拦下默认行为，免得面板先被收掉
				const row = item.closest('.quota-member-row');
				const input = row ? row.querySelector('.quota-m-model') : null;
				if (input) input.value = item.dataset.value;
				item.closest('.map-combo').classList.remove('open');
			});
		}

		// 交互接线（只挂一次）：聚焦给全量列表（改值时不被现有值过滤住），输入时按关键字过滤
		function initMapTargetCombo() {
			const input = document.getElementById('map-target');
			const menu = document.getElementById('map-target-menu');
			if (!input || !menu) return;
			const combo = menu.closest('.map-combo');
			input.addEventListener('focus', () => {
				// 只保留当前这一个：别处还开着的面板收掉（含配额弹窗的行内面板）
				document.querySelectorAll('.map-combo.open').forEach(el => el.classList.remove('open'));
				renderMapTargetMenu('');
			});
			input.addEventListener('input', () => renderMapTargetMenu(input.value.trim()));
			menu.addEventListener('mousedown', (e) => {
				const item = e.target.closest('.map-combo-item');
				if (!item) return;
				e.preventDefault(); // mousedown 先于 blur：拦下默认行为，免得面板先被收掉
				input.value = item.dataset.value;
				combo.classList.remove('open');
			});
			// 点到页面上其他地方：除目标所在的那个外，已打开的下拉全部收起
			document.addEventListener('click', (e) => {
				const keep = e.target.closest ? e.target.closest('.map-combo') : null;
				document.querySelectorAll('.map-combo.open').forEach(el => {
					if (el !== keep) el.classList.remove('open');
				});
			});
			input.addEventListener('keydown', (e) => {
				if (e.key === 'Escape') combo.classList.remove('open');
			});
		}

		async function loadSettings() {
			try {
				// 顺带刷渠道缓存 —— 目标模型的下拉候选要用到各渠道的模型列表
				const [settingsRes, providersRes] = await Promise.all([
					apiFetch('/api/settings'),
					apiFetch('/api/providers')
				]);
				providersCache = await providersRes.json();
				const data = await settingsRes.json();
				customMappings = data.customModelMap || {};
				mappingInvalid = data.invalid || {};
				mappingCfEnabled = data.cfPoolEnabled !== false;
				disabledMappings = new Set(Array.isArray(data.disabledMappings) ? data.disabledMappings : []);
				fastPassMappings = new Set(Array.isArray(data.fastPassMappings) ? data.fastPassMappings : []);
				refreshMappingTargetOptions();
				renderMappings();
			} catch (e) {
				console.error(e);
			}
		}

		// Cloudflare 模型路径（@cf/...）单独归一组，其余（provider:... 或裸渠道写法）归第三方渠道组
		function isCfMapping(target) {
			return typeof target === 'string' && target.trim().startsWith('@cf/');
		}

		function mappingGroupHeader(key, label, count, effective) {
			const collapsed = !!mappingGroupCollapsed[key];
			const state = effective
				? '<span class="badge badge-success">当前生效</span>'
				: '<span class="badge badge-danger">当前不生效</span>';
			return '<tr class="mapping-group" data-group="' + key + '" onclick="toggleMappingGroup(this.dataset.group)">'
				+ '<td colspan="4"><span class="group-arrow">' + (collapsed ? '▸' : '▾') + '</span>'
				+ sen(label) + '（' + count + ' 条）' + state + '</td></tr>';
		}


		// 额度消耗档位徽标：只标在能查到档位的 CF 模型上（第三方渠道的模型不标）
		function costTierBadge(target) {
			const tier = mappingCostTier[typeof target === 'string' ? target.trim() : ''];
			if (tier === 'low') return ' <span class="badge badge-success" title="每百万 token 约 1k~50k Neurons">省额度</span>';
			if (tier === 'mid') return ' <span class="badge badge-warning" title="每百万 token 约 50k~150k Neurons">中等</span>';
			if (tier === 'high') return ' <span class="badge badge-danger" title="每百万 token 约 450k Neurons；官方文档标注需付费计费方式">较贵</span>';
			return '';
		}

		function mappingRowHtml(source, target) {
			const problem = mappingInvalid[source];
			// 逐条开关：被停用的行整行变淡 + 标红「已停用」，开关关掉
			const off = disabledMappings.has(source);
			// 长流直通：勾了 = 该源名的流式请求不逐 chunk 读（防免费档 CPU 掐超长输出）
			const fp = fastPassMappings.has(source);
			const isPreset = Object.prototype.hasOwnProperty.call(defaultMappings, source) && defaultMappings[source] === target;
			const typeText = (isPreset ? '<span class="badge badge-success">预设映射</span>' : '<span class="badge badge-warning">自定义</span>')
				+ (off ? ' <span class="badge badge-danger">已停用</span>' : '')
				+ (fp ? ' <span class="badge badge-success" title="长流直通：流式响应不逐 chunk 读取，防超长输出被 CF CPU 限制掐断；流内 model 不回写、token 统计记 0">直通</span>' : '')
				+ (problem ? ' <span class="badge badge-danger">未生效</span>' : '')
				+ (problem ? '<div style="color: var(--danger-color); font-size:11.5px; margin-top:5px; max-width:220px; white-space:normal; line-height:1.5;">' + sen(problem) + '</div>' : '');
			return '<tr' + (off ? ' style="opacity:.55;"' : '') + '>'
				+ '<td><code style="cursor:pointer;" title="点击复制" data-copy="' + sen(source) + '" onclick="copyModelId(this.dataset.copy)">' + sen(source) + '</code></td>'
				+ '<td><code style="cursor:pointer; word-break:break-all;" title="点击复制" data-copy="' + sen(target) + '" onclick="copyModelId(this.dataset.copy)">' + sen(target) + '</code>' + costTierBadge(target) + '</td>'
				+ '<td>' + typeText + '</td>'
				+ '<td style="white-space:nowrap;">'
				+ '<label class="switch" title="' + (fp ? '点击关闭长流直通' : '点击开启长流直通（防超长输出被 CF 掐断）') + '" style="vertical-align:middle; margin-right:10px;">'
				+ '<input type="checkbox" data-fpsrc="' + sen(source) + '"' + (fp ? ' checked' : '') + ' onchange="toggleMappingFastPass(this)">'
				+ '<span class="switch-track"></span>'
				+ '</label>'
				+ '<label class="switch" title="' + (off ? '点击启用这条映射' : '点击停用这条映射') + '" style="vertical-align:middle;">'
				+ '<input type="checkbox" data-src="' + sen(source) + '"' + (off ? '' : ' checked') + ' onchange="toggleMappingStatus(this)">'
				+ '<span class="switch-track"></span>'
				+ '</label>'
				+ '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px; margin-left:10px;" data-del="' + sen(source) + '" onclick="deleteMapping(this.dataset.del)">删除</button>'
				+ '</td>'
				+ '</tr>';
		}

		function renderMappings() {
			const tbody = document.getElementById('mappings-table-body');
			if (!tbody) return;
			const sources = Object.keys(customMappings);
			if (!sources.length) {
				tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-muted); padding:24px;">还没有映射</td></tr>';
				return;
			}
			const cfSources = sources.filter(s => isCfMapping(customMappings[s]));
			const providerSources = sources.filter(s => !isCfMapping(customMappings[s]));
			let html = '';
			if (cfSources.length) {
				html += mappingGroupHeader('cf', '去向 · Cloudflare 账号池', cfSources.length, mappingCfEnabled);
				if (!mappingGroupCollapsed.cf) {
					html += cfSources.map(s => mappingRowHtml(s, customMappings[s])).join('');
				}
			}
			if (providerSources.length) {
				html += mappingGroupHeader('provider', '去向 · 第三方渠道', providerSources.length, true);
				if (!mappingGroupCollapsed.provider) {
					html += providerSources.map(s => mappingRowHtml(s, customMappings[s])).join('');
				}
			}
			tbody.innerHTML = html;
		}

		// 说明卡折叠：默认状态由 HTML 上的 collapsed class 决定
		// （与映射分组一致，不落存储 —— 刷新即回到默认，行为可预测）
		function toggleHint(id) {
			const card = document.getElementById(id);
			if (!card) return;
			const collapsed = card.classList.toggle('collapsed');
			const arrow = card.querySelector('.hint-arrow');
			if (arrow) arrow.textContent = collapsed ? '▸' : '▾';
		}

		// 配额组卡片折叠：默认全部收起（组多了好找），点整行标题切换。
		// 展开态记在这张内存表里，重渲染（改/删后刷新）时保留；不落存储。
		let quotaOpenGroups = {};

		function toggleQuotaGroup(head) {
			const card = head.closest('.quota-group-card');
			if (!card) return;
			const open = card.classList.toggle('open');
			const arrow = card.querySelector('.quota-group-arrow');
			if (arrow) arrow.textContent = open ? '▾' : '▸';
			const id = card.dataset.groupId;
			if (id) { if (open) quotaOpenGroups[id] = true; else delete quotaOpenGroups[id]; }
		}

		// 配额组外置启用/停用开关：整体启用或停用某个组（2026-10-09 用户要求）。
		// 停用后 TT:<组名> 会显式报「组未启用」，不参与选号；不用进编辑弹窗。
		async function toggleQuotaGroupStatus(cb) {
			const gid = cb.dataset.gid;
			if (!gid) return;
			const status = cb.checked ? 'active' : 'disabled';
			const res = await apiFetch('/api/quota-groups/status', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id: gid, status })
			});
			if (!res.ok) { showToast('切换失败', 'error'); loadQuotaGroups(); return; }
			showToast(status === 'active' ? '已启用该配额组' : '已停用该配额组');
			loadQuotaGroups();
		}

		// 通用：紧凑列表卡片（渠道 / 账号 / 密钥）「点标题行展开」
		function toggleListCard(head) {
			const card = head.closest('.list-card');
			if (!card) return;
			const open = card.classList.toggle('open');
			const arrow = card.querySelector('.list-arrow');
			if (arrow) arrow.textContent = open ? '▾' : '▸';
		}

		// 渠道模型 chips：点「+N 展开」把该渠道全部模型铺开
		function expandProviderChips(el) {
			const card = el.closest('.list-card');
			const p = card && providersCache.find(x => x.id === card.dataset.pid);
			if (!p) return;
			const healthMap = p.modelHealth || {};
			const chips = el.parentElement;
			if (chips) chips.innerHTML = (p.models || []).map(m => providerChipHtml(m, healthMap[m])).join('');
		}

		// 渠道外置启用/停用开关：直接切状态，不用进编辑框（2026-10-09 用户要求）
		async function toggleProviderStatus(cb) {
			const card = cb.closest('.list-card');
			const pid = card && card.dataset.pid;
			if (!pid) return;
			const status = cb.checked ? 'active' : 'disabled';
			const res = await apiFetch('/api/providers/status', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id: pid, status })
			});
			if (!res.ok) { showToast('切换失败', 'error'); loadProviders(); return; }
			showToast(status === 'active' ? '已启用该渠道' : '已停用该渠道');
			loadProviders();
		}

		function toggleMappingGroup(key) {
			mappingGroupCollapsed[key] = !mappingGroupCollapsed[key];
			renderMappings();
		}

		// 单条映射的启用/停用（映射表每行的小开关）：只改 config.disabledMappings，不删映射。
		async function toggleMappingStatus(cb) {
			const source = cb.dataset.src;
			if (!source) return;
			const res = await apiFetch('/api/mappings/status', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ source, enabled: cb.checked })
			});
			if (!res.ok) { showToast('切换失败', 'error'); loadSettings(); return; }
			showToast(cb.checked ? '已启用该映射' : '已停用该映射');
			loadSettings();
		}

		// 单条映射的「长流直通」开关（2026-10-10）：只改 config.fastPassMappings，不删映射。
		async function toggleMappingFastPass(cb) {
			const source = cb.dataset.fpsrc;
			if (!source) return;
			const res = await apiFetch('/api/mappings/fastpass', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ source, enabled: cb.checked })
			});
			if (!res.ok) { showToast('切换失败', 'error'); loadSettings(); return; }
			showToast(cb.checked ? '已开启该映射的长流直通' : '已关闭该映射的长流直通');
			loadSettings();
		}

		async function addMapping() {
			const source = document.getElementById('map-source').value.trim();
			const target = document.getElementById('map-target').value.trim();
			if (!source || !target) {
				showToast('请求模型名称和目标模型路径不能为空！', 'warning');
				return;
			}
			customMappings[source] = target;
			const res = await apiFetch('/api/settings', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ customModelMap: customMappings })
			});
			if (!res.ok) {
				showToast('添加映射失败！', 'error');
				return;
			}

			// 重新添加/修改这条映射 = 用户希望它生效 → 顺手清掉该源的「已停用」标记
			if (disabledMappings.has(source)) {
				await apiFetch('/api/mappings/status', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ source, enabled: true })
				});
			}

			const data = await res.json().catch(() => ({}));
			document.getElementById('map-source').value = '';
			document.getElementById('map-target').value = '';
			loadSettings();

			// 这条没生效 / 被自动补全了，都要立刻说清楚，别让它悄悄躺着
			const problem = (data.invalid || {})[source];
			const fixed = (data.autoFixed || {})[source];
			if (problem) {
				showToast('已保存，但这条映射未生效：' + problem, 'warning');
			} else if (fixed) {
				showToast('已自动补全为 ' + fixed);
			} else {
				showToast('映射配置成功！');
			}
		}

		async function restorePresetMappings() {
			// 顺手清掉老版内置表留下的裸短名（已被 cf/<短名> 取代，留着就是重复别名）
			const cleaned = {};
			for (const k of Object.keys(customMappings)) {
				if (defaultMappings['cf/' + k] === customMappings[k]) continue;
				cleaned[k] = customMappings[k];
			}
			const mergedMappings = { ...cleaned, ...defaultMappings };
			const hasChanges = Object.keys(defaultMappings).some(source => customMappings[source] !== defaultMappings[source]);

			if (!hasChanges && Object.keys(defaultMappings).every(source => customMappings[source] === defaultMappings[source])) {
				showToast('预设映射已存在，无需重复添加');
				return;
			}

			const res = await apiFetch('/api/settings', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ customModelMap: mergedMappings })
			});
			if (res.ok) {
				customMappings = mergedMappings;
				loadSettings();
				showToast('已恢复预设映射');
			} else {
				showToast('恢复预设映射失败！', 'error');
			}
		}

		async function deleteMapping(source) {
			if (!(await uiConfirm('确定要删除此映射吗？', { danger: true, okText: '删除' }))) return;
			delete customMappings[source];
			const res = await apiFetch('/api/settings', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ customModelMap: customMappings })
			});
			if (res.ok) {
				loadSettings();
				showToast('已删除映射');
			} else {
				showToast('删除映射失败！', 'error');
			}
		}
	</script>
</body>
</html>`;

	return new Response(html, { headers: htmlCacheHeaders(pageEtag) });
}

// 3. KV 未绑定时的报错页面
function handleKVError(request) {
	const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>KV 绑定异常 - Workers API Hub</title>
	${FAVICON_LINK}
	<link rel="preconnect" href="https://fonts.googleapis.com">
	<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
	<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
	<style>
		:root {
			--bg-color: #0b0f19;
			--card-bg: rgba(30, 41, 59, 0.45);
			--border-color: rgba(239, 68, 68, 0.2);
			--text-main: #f8fafc;
			--text-muted: #94a3b8;
			--primary-gradient: linear-gradient(135deg, #ef4444 0%, #ec4899 100%);
			--accent-color: #ec4899;
			--glass-blur: 20px;
			--card-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
			--orb-1-color: rgba(239, 68, 68, 0.08);
			--orb-2-color: rgba(236, 72, 153, 0.06);
		}

		${COMMON_CSS_RESET}

		body {
			font-family: 'Inter', sans-serif;
			background-color: var(--bg-color);
			color: var(--text-main);
			min-height: 100vh;
			display: flex;
			align-items: center;
			justify-content: center;
			padding: 20px;
			position: relative;
			overflow: hidden;
		}

		/* Dynamic Background Orbs */
		.bg-orbs-container {
			position: fixed;
			top: 0;
			left: 0;
			width: 100%;
			height: 100%;
			z-index: -1;
			overflow: hidden;
			pointer-events: none;
		}

		.bg-orb {
			position: absolute;
			border-radius: 50%;
			filter: blur(100px);
			animation: float 25s infinite alternate ease-in-out;
		}

		.bg-orb-1 {
			top: -10%;
			left: -10%;
			width: 50vw;
			height: 50vw;
			background: var(--orb-1-color);
		}

		.bg-orb-2 {
			bottom: -10%;
			right: -10%;
			width: 60vw;
			height: 60vw;
			background: var(--orb-2-color);
		}

		@keyframes float {
			0% { transform: translate(0, 0) scale(1); }
			100% { transform: translate(5%, 5%) scale(1.05); }
		}

		.error-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 20px;
			padding: 40px;
			max-width: 500px;
			width: 100%;
			text-align: center;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			z-index: 10;
		}

		h1 {
			font-family: 'Outfit', sans-serif;
			font-size: 24px;
			color: #ef4444;
			margin-bottom: 16px;
			font-weight: 600;
		}

		p {
			color: var(--text-muted);
			font-size: 15px;
			line-height: 1.6;
			margin-bottom: 24px;
		}

		.code-block {
			background-color: rgba(0, 0, 0, 0.25);
			padding: 20px;
			border-radius: 12px;
			font-family: monospace;
			font-size: 13px;
			color: #e9d5ff;
			text-align: left;
			margin-bottom: 26px;
			border: 1px solid rgba(255, 255, 255, 0.05);
			line-height: 1.8;
		}

		.btn {
			display: inline-block;
			background: var(--primary-gradient);
			color: white;
			text-decoration: none;
			padding: 12px 28px;
			border-radius: 10px;
			font-weight: 600;
			font-size: 14px;
			transition: all 0.3s;
			box-shadow: 0 4px 14px rgba(239, 68, 68, 0.2);
		}

		.btn:hover {
			transform: translateY(-2px);
			box-shadow: 0 6px 20px rgba(239, 68, 68, 0.35);
			opacity: 0.95;
		}
	</style>
</head>
<body>
	<div class="bg-orbs-container">
		<div class="bg-orb bg-orb-1"></div>
		<div class="bg-orb bg-orb-2"></div>
	</div>
	<div class="error-card">
		<div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
		<h1>KV 命名空间未绑定</h1>
		<p>系统检测到您未在 Cloudflare 平台中为该项目绑定 KV 命名空间，或者绑定的变量名称不为 <strong>KV</strong>。这会导致数据无法保存，系统无法正常运行。</p>
		
		<div class="code-block">
			<strong>解决方案：</strong><br>
			1. 进入您的 Cloudflare Workers/Pages 仪表盘。<br>
			2. 导航至 Settings -> Functions (或 Settings -> Variables) -> KV namespace bindings。<br>
			3. 添加绑定，将【变量名称 (Variable name)】设置为: <strong>KV</strong><br>
			4. 保存并重新部署项目即可。
		</div>
		
		<a href="https://developers.cloudflare.com/kv/learning/kv-bindings/" target="_blank" class="btn">查看官方绑定教程</a>
	</div>
</body>
</html>`;

	const url = new URL(request.url);
	if (url.pathname.startsWith('/v1/') || url.pathname.startsWith('/api/')) {
		return new Response(JSON.stringify({
			error: {
				message: "Cloudflare KV namespace binding 'KV' is missing. Please bind a KV namespace to 'KV' in your Worker/Pages settings.",
				type: "server_error"
			}
		}), { status: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
	}

	return new Response(html, {
		headers: { 'Content-Type': 'text/html; charset=utf-8' }
	});
}

// 4. Password Error UI Page
function handlePasswordError(request) {
	const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>管理员密码未配置 - Workers API Hub</title>
	${FAVICON_LINK}
	<link rel="preconnect" href="https://fonts.googleapis.com">
	<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
	<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
	<style>
		:root {
			--bg-color: #0b0f19;
			--card-bg: rgba(30, 41, 59, 0.45);
			--border-color: rgba(239, 68, 68, 0.2);
			--text-main: #f8fafc;
			--text-muted: #94a3b8;
			--primary-gradient: linear-gradient(135deg, #ef4444 0%, #ec4899 100%);
			--accent-color: #ec4899;
			--glass-blur: 20px;
			--card-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
			--orb-1-color: rgba(239, 68, 68, 0.08);
			--orb-2-color: rgba(236, 72, 153, 0.06);
		}

		${COMMON_CSS_RESET}

		body {
			font-family: 'Inter', sans-serif;
			background-color: var(--bg-color);
			color: var(--text-main);
			min-height: 100vh;
			display: flex;
			align-items: center;
			justify-content: center;
			padding: 20px;
			position: relative;
			overflow: hidden;
		}

		/* Dynamic Background Orbs */
		.bg-orbs-container {
			position: fixed;
			top: 0;
			left: 0;
			width: 100%;
			height: 100%;
			z-index: -1;
			overflow: hidden;
			pointer-events: none;
		}

		.bg-orb {
			position: absolute;
			border-radius: 50%;
			filter: blur(100px);
			animation: float 25s infinite alternate ease-in-out;
		}

		.bg-orb-1 {
			top: -10%;
			left: -10%;
			width: 50vw;
			height: 50vw;
			background: var(--orb-1-color);
		}

		.bg-orb-2 {
			bottom: -10%;
			right: -10%;
			width: 60vw;
			height: 60vw;
			background: var(--orb-2-color);
		}

		@keyframes float {
			0% { transform: translate(0, 0) scale(1); }
			100% { transform: translate(5%, 5%) scale(1.05); }
		}

		.error-card {
			background-color: var(--card-bg);
			border: 1px solid var(--border-color);
			border-radius: 20px;
			padding: 40px;
			max-width: 500px;
			width: 100%;
			text-align: center;
			box-shadow: var(--card-shadow);
			backdrop-filter: blur(var(--glass-blur));
			-webkit-backdrop-filter: blur(var(--glass-blur));
			z-index: 10;
		}

		h1 {
			font-family: 'Outfit', sans-serif;
			font-size: 24px;
			color: #ef4444;
			margin-bottom: 16px;
			font-weight: 600;
		}

		p {
			color: var(--text-muted);
			font-size: 15px;
			line-height: 1.6;
			margin-bottom: 24px;
		}

		.code-block {
			background-color: rgba(0, 0, 0, 0.25);
			padding: 20px;
			border-radius: 12px;
			font-family: monospace;
			font-size: 13px;
			color: #e9d5ff;
			text-align: left;
			margin-bottom: 26px;
			border: 1px solid rgba(255, 255, 255, 0.05);
			line-height: 1.8;
		}
	</style>
</head>
<body>
	<div class="bg-orbs-container">
		<div class="bg-orb bg-orb-1"></div>
		<div class="bg-orb bg-orb-2"></div>
	</div>
	<div class="error-card">
		<div style="font-size: 48px; margin-bottom: 16px;">🔑</div>
		<h1>管理员密码未配置</h1>
		<p>系统检测到您未在 Cloudflare 平台中为该项目配置 <strong>ADMIN_PASSWORD</strong> 环境变量。为了您的接口 and 管理后台安全，系统已拦截所有访问，直到密码配置完成。</p>
		
		<div class="code-block">
			<strong>解决方案：</strong><br>
			1. 进入您的 Cloudflare Workers/Pages 仪表盘。<br>
			2. 导航至 Settings -> Variables (或 Settings -> Environment Variables)。<br>
			3. 点击【Add variable】，将【Variable name】设置为: <strong>ADMIN_PASSWORD</strong><br>
			4. 输入您的管理员登录密码作为其值，保存并部署即可。
		</div>
	</div>
</body>
</html>`;

	const url = new URL(request.url);
	if (url.pathname.startsWith('/v1/') || url.pathname.startsWith('/api/')) {
		return new Response(JSON.stringify({
			error: {
				message: "ADMIN_PASSWORD environment variable is missing. Please add the ADMIN_PASSWORD variable to your Worker/Pages settings.",
				type: "server_error"
			}
		}), { status: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-api-key' } });
	}

	return new Response(html, {
		headers: { 'Content-Type': 'text/html; charset=utf-8' }
	});
}
