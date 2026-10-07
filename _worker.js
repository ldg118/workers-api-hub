/**
 * Workers API Hub
 * Cloudflare Workers 上的多上游 API 网关：把第三方 OpenAI 兼容渠道（OpenRouter、Gemini 等）
 * 与 Cloudflare Workers AI 账号池统一转换成 OpenAI / Anthropic 双协议接口；
 * 支持负载均衡、调用配额调度、故障冷却与自动切换，并自带可视化管理面板。
 */

// 构建标识：部署后 curl /version（或看落地页页脚）核对线上版本。
// Direct Upload 没有版本概念，防「部署了没生效」的白跑排查。
// ★★ 硬约定：**每次改动 _worker.js 都要把版本号 +1**（日期变了就用新日期、序号归 1）。
//    格式固定 `YYYY-MM-DD.N`。验证脚本会拦下格式不对的值，但「有没有 +1」只能靠自觉 ——
//    曾经因为版本号没变，本地/线上分不清哪个是哪版，白排查了一整轮。
const BUILD_ID = '2026-10-06.68';

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
		const response = await fetch(url, { ...init, signal: timeoutCtl.signal });
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
	systemApiKeyPrev: null
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
				// 未显式设置过时：有 CF 账号就沿用「开启」（不静默改变现有部署的行为），
				// 一个账号都没有则视为纯第三方模式，默认关闭账号池
				cfPoolEnabled: typeof data.cfPoolEnabled === 'boolean' ? data.cfPoolEnabled : accounts.length > 0,
				defaultProviderId: typeof data.defaultProviderId === 'string' ? data.defaultProviderId : '',
				// 配额组：一组「同一用途的候选模型 + 各自的次数上限」，按顺序自动切换到未满的那个
				quotaGroups: Array.isArray(data.quotaGroups) ? data.quotaGroups : [],
				// 系统默认密钥：自动轮换相关字段（老配置缺省时按安全默认值补齐）
				systemKeyRotationEnabled: typeof data.systemKeyRotationEnabled === 'boolean' ? data.systemKeyRotationEnabled : false,
				systemKeyRotatedAt: data.systemKeyRotatedAt || null,
				systemApiKeyPrev: data.systemApiKeyPrev || null,
				systemApiKey: data.systemApiKey || null,
				systemApiKeyCreatedAt: data.systemApiKeyCreatedAt || null
			};
		} catch (e) { }
	} else {
		// 仅在 KV 首次初始化时写入默认模型映射，避免覆盖已有配置。
		parsed.customModelMap = { ...DEFAULT_MODEL_MAP };
		await env.KV.put('config', JSON.stringify(parsed));
	}

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

// 当前配额的周期窗口，返回统计桶的半开区间 [startKey, endKey)
function quotaWindow(group, nowMs) {
	const offsetMin = resolveResetOffset(group);
	const resetMin = parseResetTime(group && group.resetLocalTime);
	const isMonth = !!(group && group.period === 'month');
	// 先把时间平移到该时区的「墙上时间」，之后一律用 UTC getter 读 —— 等价于读本地时间，
	// 但不依赖运行时的本机时区设置（Workers 恒为 UTC）。
	const localMs = Number(nowMs) + offsetMin * 60000;
	const d = new Date(localMs);
	let startLocal;
	if (isMonth) {
		// 本月 1 日的基准时刻；还没到就把窗口退回上个月
		startLocal = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) + resetMin * 60000;
		if (startLocal > localMs) startLocal = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() - 1, 1) + resetMin * 60000;
	} else {
		// 该时区当天的基准时刻；还没到就退回昨天
		startLocal = Math.floor(localMs / 86400000) * 86400000 + resetMin * 60000;
		if (startLocal > localMs) startLocal -= 86400000;
	}
	const sd = new Date(startLocal);
	const endLocal = isMonth
		? Date.UTC(sd.getUTCFullYear(), sd.getUTCMonth() + 1, 1) + resetMin * 60000
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
	const period = group && group.period === 'month' ? '每月 1 日' : '每天';
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

// 查一组配额成员的「本周期已用次数」。周期边界由组自己的重置时区/时刻决定。
// 返回 Map<JSON[providerId, model], used>；返回 null 表示算不出来（未绑 D1 / 查询失败）——
// 调用方必须区分 null 和 0。
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
	const sql = `SELECT provider_id, model, COALESCE(SUM(req + probe_req), 0) AS used FROM stats
	   WHERE day >= ? AND day < ? AND provider_id IN (${placeholders})
	   GROUP BY provider_id, model`;
	try {
		const { results } = await env.DB.prepare(sql).bind(win.startKey, win.endKey, ...ids).all();
		const out = new Map();
		for (const r of results || []) {
			out.set(JSON.stringify([String(r.provider_id), String(r.model)]), Number(r.used) || 0);
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

const QUOTA_COOLDOWN_429_MS = 60000; // 429 限流：冷却 60s（上游给了 retry_after 就取更长的）
const QUOTA_COOLDOWN_ERR_MS = 30000; // 5xx / 超时 / 连不上：冷却 30s
const QUOTA_COOLDOWN_MAX_MS = 600000; // 冷却上限 10 分钟（防被上游的超长 retry_after 卡死）
const QUOTA_MAX_SWITCH = 3; // 一次请求内最多尝试几个成员（含第一个）
// 瞬时基础设施错误（5xx / 超时 / 连不上）的「原地重试」次数。
// 现实依据（2026-10-06 直连 Google 实测）：gemini-3.1-flash-lite 会间歇性返回
// 503「This model is currently experiencing high demand. Spikes in demand are usually temporary.」
// —— 5 次里挂 1 次；重试一次基本就过。
// 只在「没有别的成员可换」时才用（非配额组请求，或第一个成员且无备选），避免放大故障时的请求量。
const PROVIDER_TRANSIENT_RETRY = 1;
const PROVIDER_TRANSIENT_RETRY_DELAY_MS = 500; // 重试前小睡一下，给上游喘息
const QUOTA_HOUR_WEIGHT = 3; // 选号打分里「本小时已用」的权重（越大越避免短时扎堆）
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
	// 顺手清掉已过期的，免得这个键无限膨胀
	for (const k of Object.keys(map)) {
		if (!(Number(map[k] && map[k].until) > now)) delete map[k];
	}
	cooldownCache = { at: now, map };
	return map;
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
	if (env.KV) {
		try { await env.KV.put(COOLDOWN_KV_KEY, JSON.stringify(map)); } catch (e) { /* 冷却落盘失败不影响主流程 */ }
	}
	return map[key];
}

async function clearCooldown(env, providerId, model) {
	const key = cooldownKeyOf(providerId, model);
	const map = await getCooldowns(env);
	if (!map[key]) return false;
	delete map[key];
	cooldownCache = { at: Date.now(), map };
	if (env.KV) {
		try { await env.KV.put(COOLDOWN_KV_KEY, JSON.stringify(map)); } catch (e) { /* 忽略 */ }
	}
	return true;
}

// 把上游失败分类：值不值得「换个成员重试」+ 冷却多久。
// transient = 瞬时基础设施错误（5xx / 超时 / 连不上）→ 值得**原地再试一次**；
//             429 / 401 / 403 不算（原地重试没用，得换成员或等冷却）。
function classifyUpstreamFailure(result) {
	const s = Number((result && (result.upstreamStatus || result.status)) || 0);
	const msg = String((result && result.error) || '');
	let out;
	if (s === 429) out = { retryable: true, transient: false, coolMs: QUOTA_COOLDOWN_429_MS, reason: '上游 429 限流' };
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
	const sql = `SELECT provider_id, model, COALESCE(SUM(req + probe_req), 0) AS used FROM stats
	   WHERE day = ? AND provider_id IN (${placeholders})
	   GROUP BY provider_id, model`;
	try {
		const { results } = await env.DB.prepare(sql).bind(hourKey, ...ids).all();
		const out = new Map();
		for (const r of results || []) {
			out.set(JSON.stringify([String(r.provider_id), String(r.model)]), Number(r.used) || 0);
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
				error: `配额组「${group.name}」的成员设了次数上限，但统计不到已用次数 —— `
					+ '请在 Worker 上绑定 D1 数据库（变量名 DB）。未绑定时无法安全地按配额分流。'
			};
		}
	}
	// 本小时用量：把请求在一小时内摊开（拿不到就退化成只看本周期，不报错）。调度关掉时不查。
	const hourUsage = sched ? await queryQuotaHourUsage(env, usable) : null;

	const rows = [];
	for (const m of usable) {
		const k = JSON.stringify([String(m.providerId), String(m.model)]);
		const limit = Number(m.limit) || 0;
		const used = usage ? (usage.get(k) || 0) : 0;
		if (limit > 0 && used >= limit) continue; // 本周期已满 → 硬性剔除
		const hour = hourUsage ? (hourUsage.get(k) || 0) : 0;
		const busy = inflight ? (inflight.get(m._key) || 0) : 0;
		rows.push({ m, limit, used, hour, busy, rnd: Math.random() });
	}

	if (!rows.length) {
		const detail = usable.map(m => {
			const limit = Number(m.limit) || 0;
			if (limit <= 0) return `${m.model} 不限`;
			const used = usage ? (usage.get(JSON.stringify([String(m.providerId), String(m.model)])) || 0) : 0;
			return `${m.providerName}/${m.model} ${used}/${limit}`;
		}).join('，');
		const win = quotaWindow(group, Date.now());
		return {
			ok: false,
			error: `配额组「${group.name}」${period === 'month' ? '本月' : '今日'}配额已用完（${detail}）。`
				+ `下次重置：${fmtBeijing(win.endMs)}（北京时间；基准 ${describeReset(group)}）。`
		};
	}

	// 调度总开关关闭 → 回到旧行为：按组内顺序取第一个未满（rows 保持组内顺序，未排序）
	if (!sched) {
		const first = rows[0];
		return { ok: true, providerId: first.m.providerId, model: first.m.model, used: first.used, limit: first.limit };
	}

	// 会话粘性（组级开关打开时）：同一会话尽量落到同一个成员 —— 多轮对话 / 工具调用更稳。
	// 用「组内配置顺序 + 会话散列」确定性计算，不需要存状态；该成员不可用时自动回落均衡。
	if (opts.sessionKey && group.sticky === true) {
		const want = usable[hashString(opts.sessionKey) % usable.length];
		const hit = rows.find(r => r.m._key === want._key);
		if (hit) return { ok: true, providerId: hit.m.providerId, model: hit.m.model, used: hit.used, limit: hit.limit, sticky: true };
	}

	// 负载均衡排序：本小时已用(带权重) + 在途 → 本周期已用 → 随机
	rows.sort((a, b) =>
		((a.hour + a.busy) * QUOTA_HOUR_WEIGHT) - ((b.hour + b.busy) * QUOTA_HOUR_WEIGHT)
		|| a.used - b.used
		|| a.rnd - b.rnd);
	const pick = rows[0];
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
async function checkAdminAuth(request, env) {
	// 1. 先从 Cookie 里取登录令牌（浏览器访问时走这里）
	const cookies = request.headers.get('Cookie') || '';
	const cookieMatch = cookies.match(/admin_token=([^;]+)/);
	let token = cookieMatch ? cookieMatch[1] : null;

	// 2. Cookie 里没有的话，再从 Authorization 请求头里取（API 工具调用时走这里）
	if (!token) {
		const authHeader = request.headers.get('Authorization');
		if (authHeader && authHeader.startsWith('Bearer ')) {
			token = authHeader.substring(7);
		}
	}

	if (!token) return false;

	const expectedPassword = env.ADMIN_PASSWORD ? env.ADMIN_PASSWORD.trim() : '';

	if (!expectedPassword) return false; // 还没配置管理员密码

	const expectedHash = await sha256(expectedPassword);
	return token === expectedHash;
}

// 校验管理员的登录 Cookie（用于页面访问的权限判断）
async function verifyAdminCookie(request, env) {
	const cookies = request.headers.get('Cookie') || '';
	const cookieMatch = cookies.match(/admin_token=([^;]+)/);
	if (!cookieMatch) return false;

	const token = cookieMatch[1];

	const expectedPassword = env.ADMIN_PASSWORD ? env.ADMIN_PASSWORD.trim() : '';
	if (!expectedPassword) return false;

	const expectedHash = await sha256(expectedPassword);
	return token === expectedHash;
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
				if (typeof customMap[key] === 'string' && !customMap[key].startsWith('@cf/')) {
					combinedMap[key] = customMap[key];
				}
			}
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
		const modelsData = Object.keys(combinedMap)
			.filter(id => !isLegacyPresetAlias(id, combinedMap[id]))
			.map(id => ({
				id,
				object: 'model',
				created: 1686935000,
				owned_by: ownedByOf(combinedMap[id])
			}));

		// 第三方渠道的模型也一并暴露，id 用「渠道名/模型名」形式，可直接拿来调用
		const knownIds = new Set(Object.keys(combinedMap));
		for (const provider of providers) {
			for (const m of provider.models || []) {
				const id = `${provider.name}/${m}`;
				if (!m || knownIds.has(id)) continue;
				knownIds.add(id);
				modelsData.push({
					id,
					object: 'model',
					created: 1686935000,
					owned_by: provider.name
				});
			}
		}

		// 配额组名同样列出来，客户端可直接调用（实际走组内第一个未满配额的成员）
		for (const g of config.quotaGroups || []) {
			if (g.status === 'disabled' || !g.name || knownIds.has('TT:' + g.name)) continue;
			knownIds.add('TT:' + g.name);
		modelsData.push({
			id: 'TT:' + g.name,
			object: 'model',
				created: 1686935000,
				owned_by: 'quota-group'
			});
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
				lastError = `CF API returned ${cfResponse.status}: ${errorText}`;
			}
		} catch (e) {
			lastError = (e && e.isTimeout)
				? `CF API did not respond within ${Math.round(PROVIDER_TIMEOUT_MS / 1000)}s (timeout)`
				: `Connection error: ${e.message}`;
		}
	}

	return { success: false, error: `All Cloudflare accounts failed. Last error: ${lastError}` };
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
		// 老表补列：测试/探测调用单独计数（语句失败说明列已存在，忽略）。
		// last_ms/last_at = 最近一次调用的延迟与时刻，每次都覆盖写（看板「最近」列的数据源）。
		// 只在这里跑一次，statsTableReady 之后就跳过了。
		for (const [col, def] of [
			['probe_req', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_ok', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_fail', 'INTEGER NOT NULL DEFAULT 0'],
			['probe_ms_total', 'INTEGER NOT NULL DEFAULT 0'],
			['tokens', 'INTEGER NOT NULL DEFAULT 0'],
			['reasoning_tokens', 'INTEGER NOT NULL DEFAULT 0'],
			['last_ms', 'INTEGER NOT NULL DEFAULT 0'],
			['last_at', 'TEXT']
		]) {
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
function recordProviderCall(env, ctx, provider, model, ok, ms, isProbe, tokens, reasoningTokens) {
	if (!env.DB) return;
	// 桶是「小时」而不是「天」：配额组的重置时刻可以不在 UTC 零点（如 Gemini 是太平洋午夜
	// = UTC 07:00），只有小时级的桶才能把周期边界切准。列名仍叫 day（SQLite 改主键成本高），
	// 值形如 2026-10-04T07 —— 老数据是纯日期串，字符串比较下仍能被看板的 >= 范围查询覆盖。
	const day = new Date().toISOString().slice(0, 13);
	const probe = isProbe ? 1 : 0;
	const msRounded = Math.max(0, Math.round(Number(ms) || 0));
	const tokensRounded = Math.max(0, Math.round(Number(tokens) || 0));
	const reasoningRounded = Math.max(0, Math.round(Number(reasoningTokens) || 0));
	const task = (async () => {
		await ensureStatsTable(env);
		await env.DB.prepare(
			`INSERT INTO stats (day, provider_id, provider_name, model,
			                    req, ok, fail, ms_total,
			                    probe_req, probe_ok, probe_fail, probe_ms_total, tokens, reasoning_tokens,
			                    last_ms, last_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
	if (!u) return { tokens: 0, reasoningTokens: 0 };
	const tokens = Number(u.total_tokens || u.totalTokens || 0) || 0;
	let reasoning = 0;
	if (u.reasoning_tokens) reasoning = Number(u.reasoning_tokens) || 0;
	else if (u.completion_tokens_details && u.completion_tokens_details.reasoning_tokens) reasoning = Number(u.completion_tokens_details.reasoning_tokens) || 0;
	else if (u.output_tokens_details && u.output_tokens_details.reasoning_tokens) reasoning = Number(u.output_tokens_details.reasoning_tokens) || 0;
	return { tokens, reasoningTokens: reasoning };
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
								const m = /"total_tokens"\s*:\s*(\d+)/.exec(s);
								if (m) maxTotal = Math.max(maxTotal, Number(m[1]) || 0);
								const mr = /"reasoning_tokens"\s*:\s*(\d+)/.exec(s);
								if (mr) maxReasoning = Math.max(maxReasoning, Number(mr[1]) || 0);
							}
							if (buf.length > 200000) buf = buf.slice(-4096); // 防异常上游把缓冲撑爆
						} catch (e) { /* 探测失败不影响转发 */ }
						controller.enqueue(value);
					}
					try { controller.close(); } catch (e) { }
				} catch (e) {
					try { controller.error(e); } catch (_) { }
				} finally {
					try { onUsage({ tokens: maxTotal, reasoningTokens: maxReasoning }); } catch (e) { /* 统计失败绝不影响请求 */ }
				}
			})();
		},
		cancel(reason) {
			try { reader.cancel(reason); } catch (e) { }
			// 回调必须与 start 尾部的形状一致（对象），否则调用方读 uo.tokens 恒为 undefined → 中断的流 token 记 0
			try { onUsage({ tokens: maxTotal, reasoningTokens: maxReasoning }); } catch (e) { }
		},
	});
}

// 落地页公开汇总接口（/api/public/provider-stats）的 isolate 内缓存，30s TTL，避免公开端点被刷穿 D1
let publicProviderSummaryCache = null;

// 第三方渠道统计：总览 + 按渠道（含各模型明细）
// 数据全部来自本代理埋点，与 CF 官方账单是两条独立链路
async function queryProviderStats(env, range) {
	if (!env.DB) return { enabled: false, reason: 'no-db' };
	await ensureStatsTable(env);
	// 第三方渠道「估算成本」单价（美元 / 千 token），纯示意估算，非真实账单。
	// 想更准就按渠道/模型配价；这里只做 Token 级粗估，可用环境变量 THIRD_PARTY_EST_COST_PER_1K 覆盖。
	const estRate = Number(env && env.THIRD_PARTY_EST_COST_PER_1K) || 0.011;

	const today = new Date().toISOString().slice(0, 10);
	let sinceDay = null;
	if (range === 'today') sinceDay = today;
	else if (range === '7d') sinceDay = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);

	const where = sinceDay ? 'WHERE day >= ?' : '';
	const args = sinceDay ? [sinceDay] : [];
	const run = (stmt) => (sinceDay ? stmt.bind(...args) : stmt).all();

	const totalsStmt = env.DB.prepare(
		'SELECT COALESCE(SUM(req),0) AS req, COALESCE(SUM(ok),0) AS ok, COALESCE(SUM(fail),0) AS fail, COALESCE(SUM(ms_total),0) AS msTotal, COALESCE(SUM(probe_req),0) AS probeReq, COALESCE(SUM(probe_ok),0) AS probeOk, COALESCE(SUM(probe_fail),0) AS probeFail, COALESCE(SUM(probe_ms_total),0) AS probeMsTotal, COALESCE(SUM(tokens),0) AS tokens, COALESCE(SUM(reasoning_tokens),0) AS reasoningTokens FROM stats ' + where
	);
	const totalsRow = (await run(totalsStmt)).results?.[0] || {};

	const rowsStmt = env.DB.prepare(
		// MAX(last_at) + 裸列 last_ms：SQLite 规定裸列取自 MAX 命中的那一行 —— 即拿到「最近一次」的延迟
		'SELECT provider_id, provider_name, model, SUM(req) AS req, SUM(ok) AS ok, SUM(fail) AS fail, SUM(ms_total) AS msTotal, SUM(probe_req) AS probeReq, SUM(probe_ok) AS probeOk, SUM(probe_fail) AS probeFail, SUM(probe_ms_total) AS probeMsTotal, SUM(tokens) AS tokens, SUM(reasoning_tokens) AS reasoningTokens, MAX(last_at) AS lastAt, last_ms AS lastMs FROM stats ' + where + ' GROUP BY provider_id, model ORDER BY req DESC'
	);
	const rows = (await run(rowsStmt)).results || [];

	// 图表数据①：近 7 日逐日逐模型 token（趋势折线，固定 7 天窗口，不受 range 切换影响）
	// tokens 列只在真实转发时写入（探测不记 token），所以这里天然不含探测流量
	const weekStart = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);
	const dayList = [];
	for (let i = 6; i >= 0; i--) dayList.push(new Date(Date.now() - i * 86400000).toISOString().slice(0, 10));
	const trendRows = (await env.DB.prepare(
		'SELECT substr(day,1,10) AS d, model, SUM(tokens) AS tokens FROM stats WHERE day >= ? GROUP BY d, model ORDER BY d'
	).bind(weekStart).all()).results || [];
	const trendByModel = new Map();
	for (const r of trendRows) {
		if (!trendByModel.has(r.model)) trendByModel.set(r.model, {});
		trendByModel.get(r.model)[r.d] = (trendByModel.get(r.model)[r.d] || 0) + (r.tokens || 0);
	}

	// 图表数据②：今日逐模型 token + 请求数（占比环形，请求数含探测 probe_req，与配额面板口径一致）
	const todayRows = (await env.DB.prepare(
		'SELECT model, SUM(tokens) AS tokens, SUM(req) AS req, SUM(probe_req) AS probeReq FROM stats WHERE day >= ? GROUP BY model ORDER BY tokens DESC'
	).bind(today).all()).results || [];

	// 把「渠道 + 模型」的扁平行聚成两层结构
	const byProvider = new Map();
	for (const r of rows) {
		const key = String(r.provider_id);
		if (!byProvider.has(key)) {
			byProvider.set(key, { id: key, name: r.provider_name, req: 0, ok: 0, fail: 0, msTotal: 0, probeReq: 0, probeOk: 0, probeFail: 0, probeMsTotal: 0, tokens: 0, reasoningTokens: 0, models: [] });
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
		// 渠道级「最近一次」= 各模型里时刻最新的那个
		if (r.lastAt && (!p.lastAt || r.lastAt > p.lastAt)) { p.lastAt = r.lastAt; p.lastMs = r.lastMs || 0; }
		if (r.provider_name) p.name = r.provider_name;
		p.models.push({
			model: r.model,
			// 与 shape 同口径：请求数/成功失败/平均延迟并入探测（.47）
			req: (r.req || 0) + (r.probeReq || 0),
			ok: (r.ok || 0) + (r.probeOk || 0),
			fail: (r.fail || 0) + (r.probeFail || 0),
			avgMs: ((r.req || 0) + (r.probeReq || 0)) ? Math.round(((r.msTotal || 0) + (r.probeMsTotal || 0)) / ((r.req || 0) + (r.probeReq || 0))) : 0,
			lastMs: r.lastMs || 0,
			lastAt: r.lastAt || null,
			tokens: r.tokens || 0,
			reasoningTokens: r.reasoningTokens || 0,
			costEst: Math.round((r.tokens || 0) / 1000 * estRate * 100) / 100,
			probeReq: r.probeReq || 0,
			probeOk: r.probeOk || 0,
			probeFail: r.probeFail || 0,
			probeAvgMs: r.probeReq ? Math.round((r.probeMsTotal || 0) / r.probeReq) : 0
		});
	}

	const shape = (o) => ({
		// req/ok/fail/avgMs 已并入探测调用（probe_*），与配额面板、占比环形图口径一致（.47）
		req: (o.req || 0) + (o.probeReq || 0),
		ok: (o.ok || 0) + (o.probeOk || 0),
		fail: (o.fail || 0) + (o.probeFail || 0),
		okRate: ((o.req || 0) + (o.probeReq || 0)) ? Math.round(((o.ok || 0) + (o.probeOk || 0)) / ((o.req || 0) + (o.probeReq || 0)) * 1000) / 10 : 0,
		avgMs: ((o.req || 0) + (o.probeReq || 0)) ? Math.round(((o.msTotal || 0) + (o.probeMsTotal || 0)) / ((o.req || 0) + (o.probeReq || 0))) : 0,
		tokens: o.tokens || 0,
		reasoningTokens: o.reasoningTokens || 0,
		costEst: Math.round((o.tokens || 0) / 1000 * estRate * 100) / 100,
		probeReq: o.probeReq || 0,
		probeOk: o.probeOk || 0,
		probeFail: o.probeFail || 0,
		probeAvgMs: o.probeReq ? Math.round((o.probeMsTotal || 0) / o.probeReq) : 0
	});

	return {
		enabled: true,
		range,
		sinceDay,
		today,
		summary: shape(totalsRow),
		providers: [...byProvider.values()].map(p => ({ id: p.id, name: p.name, ...shape(p), lastMs: p.lastMs || 0, lastAt: p.lastAt || null, models: p.models })),
		trend: {
			days: dayList,
			series: [...trendByModel.entries()].map(([model, byDay]) => ({ model, data: dayList.map(d => byDay[d] || 0) }))
		},
		todayByModel: todayRows.map(r => ({ model: r.model, tokens: r.tokens || 0, req: (r.req || 0) + (r.probeReq || 0) }))
	};
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
	if (!schema || typeof schema !== 'object') return { type: 'object', properties: {} };
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
					if (!args || typeof args !== 'object') args = {};
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
			let resp = null;
			try { resp = JSON.parse(m.content); } catch (_) { resp = null; }
			if (!resp || typeof resp !== 'object') resp = { result: openaiContentToText(m.content) };
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
	if (Object.keys(gc).length) out.generationConfig = gc;
	return out;
}

// Gemini generateContent 响应 → OpenAI chat.completion
function geminiResponseToOpenAI(gj, modelName) {
	const cand = (gj && Array.isArray(gj.candidates) && gj.candidates[0]) || {};
	const parts = (cand.content && Array.isArray(cand.content.parts)) ? cand.content.parts : [];
	let text = '';
	const toolCalls = [];
	for (const p of parts) {
		// 跳过内部思考 part（Gemini 3 会以 thought:true 回吐），别混进正文
		if (p && p.thought === true) continue;
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
								if (p && p.thought === true) continue; // 内部思考不下发
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

async function callProvider(provider, payload, stream) {
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
		// 统一延迟口径：拿到响应头即测 TTFB（首字节），流式/非流式都记这个值
		const sendPayload = stream
			? { ...payload, stream_options: { include_usage: true } }
			: payload;
		const { response: upstream, ttfb } = await fetchWithTtfb(`${baseUrl}/chat/completions`, {
			method: 'POST',
			headers,
			body: JSON.stringify(sendPayload),
		});

		if (!upstream.ok) {
			const errorText = await upstream.text();
			// upstreamStatus 供「失败分类」用（429/5xx 可换成员重试，400 不可）；
			// status 是给客户端的：4xx 原样透出（CF 只替换 5xx 响应体），其余压成 502。
			return {
				success: false,
				upstreamStatus: upstream.status,
				status: (upstream.status >= 400 && upstream.status < 500) ? upstream.status : 502,
				error: `Provider "${provider.name}" returned ${upstream.status}: ${errorText}`
			};
		}

		if (stream) {
			return { success: true, stream: upstream.body, ttfb };
		}
		return { success: true, data: await upstream.json(), ttfb };
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
	const tried = new Set();
	const switchBudget = (route.quotaGroupId && sched) ? QUOTA_MAX_SWITCH : 1;
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
				const groups = await getQuotaGroups(env);
				const g = (groups || []).find(x => x.id === route.quotaGroupId);
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
		const r = await callProvider(curProvider, preparePayload(curProvider, curModel), stream);
		inflightDec(memberKey);
		const elapsed = Date.now() - startedAt;

		if (r.success) {
			// 成功 → 顺手解除该成员的冷却（可能只是偶发抖动）
			if (route.quotaGroupId && sched) await clearCooldown(env, curProvider.id, curModel);
			// 延迟统一记 TTFB（首字节），流式/非流式口径一致（见 callProvider/callGeminiNative/callAccountPool）
			const okMs = (r.ttfb != null) ? r.ttfb : elapsed;
			if (stream && r.stream) {
				// 流式：token 要等流走完才知道，交给探针在结束时落库
				r.stream = withUsageTap(r.stream, (uo) => {
					recordProviderCall(env, ctx, curProvider, curModel, true, okMs, false, uo.tokens, uo.reasoningTokens);
				});
			} else {
				const uo = usageOf(r);
				recordProviderCall(env, ctx, curProvider, curModel, true, okMs, false, uo.tokens, uo.reasoningTokens);
			}
			result = r;
			break;
		}

		// 失败：先落统计，再判断该不该冷却 / 换下一个 / 原地重试
		recordProviderCall(env, ctx, curProvider, curModel, false, elapsed, false, 0);
		lastCls = classifyUpstreamFailure(r);
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

	// 兼容老写法：内置表已统一叫 cf/<短名>，客户端填的裸短名（glm-4.7-flash）继续认
	if (!mapped && !requested.includes('/')) {
		mapped = combinedMap['cf/' + requested];
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
		const transformedStream = withSseHeartbeat(passthroughStream(result.stream, model));
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

				const assistantMsg = { role: 'assistant', content: textContent || null };
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
	if (firstNonSystemMsg && firstNonSystemMsg.role === 'assistant') {
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
function convertOpenAIErrorToAnthropic(openaiError) {
	return {
		type: 'error',
		error: {
			type: 'api_error',
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
			errorDetail || { message: result.error }
		);
		return new Response(JSON.stringify(anthropicError), {
			status,
			headers: { 'Content-Type': 'application/json' }
		});
	}

	if (stream) {
		// 流式：转换流
		const transformedStream = withSseHeartbeat(anthropicStreamTransform(result.stream, model, anthropicBody.messages));
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
function anthropicStreamTransform(upstreamBody, modelName, originalMessages) {
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
	let inputTokens = 0;
	let outputTokens = 0;

	return new ReadableStream({
		// ⚠️ 主动排空（eager drain），理由同 passthroughStream：
		// Workers 上 pull 驱动可能永远等不到下游需求 → 流不产出 → 被判 hang → 502。
		start(controller) {
			(async () => {
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) {
							if (buffer.trim()) {
								buffer = processLines(buffer, controller);
							}
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
					const choice = chunk.choices?.[0];
					if (!choice) continue;

					const delta = choice.delta || {};

					// 更新 usage
					if (chunk.usage) {
						inputTokens = chunk.usage.prompt_tokens || 0;
						outputTokens = chunk.usage.completion_tokens || 0;
					}

					// 处理 tool_calls delta
					if (delta.tool_calls && Array.isArray(delta.tool_calls)) {
						// 首次发送任何数据前先发送 message_start（Bug #1）
						if (!streamStarted) {
							sendMessageStart(controller);
							streamStarted = true;
						}

						for (const tc of delta.tool_calls) {
							if (tc.id) {
								// 新的 tool_call 开始
								if (currentToolCallId) {
									// 先结束上一个
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
					} else if (delta.content) {
						// 文本内容 delta
						if (!streamStarted) {
							sendMessageStart(controller);
							contentBlockIndex++;
							sendContentBlockStart(controller, 'text');
							streamStarted = true;
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
						if (currentToolCallId && currentToolArgs) {
							// 发送最终的 tool_use input
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
		// 如果 finish_reason 触发时已发送过 content_block_stop，跳过重复发送（Bug #2）
		if (!blockStopSent) {
			sendContentBlockStop(controller);
		}

		let stopReason = 'end_turn';
		if (currentToolCallId) {
			stopReason = 'tool_use';
		}

		const event = {
			type: 'message_delta',
			delta: {
				stop_reason: stopReason,
				stop_sequence: null
			},
			usage: { output_tokens: outputTokens || 0 }
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

function passthroughStream(upstreamBody, modelName) {
	const reader = upstreamBody.getReader();
	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	let buffer = '';

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
			(async () => {
				try {
					while (true) {
						const { value, done } = await reader.read();
						if (done) {
							// 把缓冲区里剩下的内容输出掉
							if (buffer.trim()) {
								buffer = processLines(buffer, controller);
							}
							controller.enqueue(encoder.encode('data: [DONE]\n\n'));
							controller.close();
							break;
						}

						buffer += decoder.decode(value, { stream: true });
						buffer = processLines(buffer, controller);
					}
				} catch (e) {
					// 上游断流：发一条错误事件再正常收尾 —— 让客户端看到真实原因，
					// 而不是让边缘拿「不完整响应」去回 502（2026-10-05）。错误后不补 [DONE]，避免被当成正常结束
					try {
						controller.enqueue(encoder.encode(`data: {"error":{"message":${JSON.stringify(String(e && e.message || e))},"type":"server_error","code":"upstream_stream_error"}}\n\n`));
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
		const remaining = lines.pop(); // 把最后可能不完整的一行留在缓冲区里

		for (const line of lines) {
			const trimmed = line.trim();
			if (!trimmed) continue;

			if (trimmed.startsWith('data: ')) {
				const dataStr = trimmed.slice(6);
				if (dataStr === '[DONE]') continue;

				try {
					const chunk = JSON.parse(dataStr);
					// 只改模型名，其他字段全部原样透传
					// 这样 tool_calls、finish_reason、usage、reasoning_content 都能保留下来
					if (chunk.model !== undefined) chunk.model = modelName;
					controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
				} catch (_) {
					// 解析不了的行，按原样转发
					controller.enqueue(encoder.encode(`${line}\n`));
				}
			} else {
				// 非 data 开头的 SSE 行（注释、事件等），原样转发
				controller.enqueue(encoder.encode(`${line}\n`));
			}
		}
		return remaining;
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
		return new Response(JSON.stringify({
			isSetup: true
		}), { headers: { 'Content-Type': 'application/json' } });
	}

	// 2. 设置首个管理员密码（已停用，改由环境变量 ADMIN_PASSWORD 配置）
	if (url.pathname === '/api/auth/setup' && method === 'POST') {
		return new Response(JSON.stringify({ error: 'Setup is handled via environment variable ADMIN_PASSWORD' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
	}

	// 3. 登录（直接比对密码，不读写 KV，既快又省钱）
	if (url.pathname === '/api/auth/login' && method === 'POST') {
		const { password } = await request.json();
		const expectedPassword = env.ADMIN_PASSWORD ? env.ADMIN_PASSWORD.trim() : '';
		if (password === expectedPassword) {
			const token = await sha256(password);
			return new Response(JSON.stringify({ success: true }), {
				headers: {
					'Content-Type': 'application/json',
					'Set-Cookie': `admin_token=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400`
				}
			});
		} else {
			return new Response(JSON.stringify({ error: 'Incorrect password' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
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
			// 开启时若还没有起始时间，记录为现在，避免一开启就因 rotatedAt 过旧而立即轮换
			if (enabled && !config.systemKeyRotatedAt) config.systemKeyRotatedAt = Date.now();
			await saveAppConfig(env, config);
			return new Response(JSON.stringify({ success: true, enabled }), { headers: { 'Content-Type': 'application/json' } });
		}
	}

	if (url.pathname === '/api/provider-stats' && method === 'GET') {
		// 渠道/模型级明细仅管理员可见（2026-10-06 补鉴权：此前任何知道 URL 的人都能查）
		if (!(await checkAdminAuth(request, env))) {
			return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } });
		}
		const raw = url.searchParams.get('range') || '7d';
		const range = ['today', '7d', 'all'].includes(raw) ? raw : '7d';
		return new Response(JSON.stringify(await queryProviderStats(env, range)), { headers: { 'Content-Type': 'application/json' } });
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
				cfPoolEnabled: cfEnabled
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

			// 保存本身照常成功（避免用户卡在一条错映射上连删都删不掉），
			// 但把问题显式回传，界面会立刻提示
			const problems = findInvalidMappings(normalized, providers, cfEnabled);
			const warnings = Object.entries(problems).map(([s, p]) => '「' + s + '」：' + p);
			return new Response(JSON.stringify({ success: true, autoFixed, warnings, invalid: problems }), { headers: { 'Content-Type': 'application/json' } });
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
			const { id, name, baseUrl, apiKey, models, status, geminiNative } = await request.json();
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
			if (id) {
				let found = false;
				providers = providers.map(p => {
					if (p.id !== id) return p;
					found = true;
					// 编辑时密钥留空或仍是掩码，则保留原值
					const nextKey = (!apiKey || apiKey.includes('...') || apiKey === '********') ? p.apiKey : apiKey;
					return {
						...p,
						name,
						baseUrl: String(baseUrl).replace(/\/+$/, ''),
						apiKey: nextKey,
						models: modelList,
					status: status || p.status || 'active',
					geminiNative: gnFlag
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
					createdAt: new Date().toISOString()
				});
			}

			await saveProviders(env, providers);
			return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
		}

		if (method === 'DELETE') {
			const { id } = await request.json();
			const config = await getAppConfig(env);
			let providers = config.providers || [];
			const target = providers.find(p => p.id === id);
			providers = providers.filter(p => p.id !== id);
			await saveProviders(env, providers);

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

	// 12. 配额组：一组候选模型 + 各自的次数上限，按顺序自动切换到未满的那个
	if (url.pathname === '/api/quota-groups') {
		if (method === 'GET') {
			const config = await getAppConfig(env);
			const groups = config.quotaGroups || [];
			const providers = config.providers || [];
			const nowMs = Date.now();
			// 调度状态也一并回给界面：冷却中的成员要「看得见」（谁在冷却 / 还剩多久 / 为什么）
			const cooldowns = await getCooldowns(env);

			// 每个组可以有自己的重置时区/时刻，窗口不同 —— 所以逐组查用量，不能合并成两次查询
			const groupsOut = [];
			for (const g of groups) {
				const map = await queryQuotaUsage(env, g, g.members || []);
				// 本小时用量 = 负载均衡的依据，也展示给用户看
				const hourMap = await queryQuotaHourUsage(env, g.members || []);
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
					sticky: g.sticky === true,
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
							// null 表示统计不可用（未绑 D1），界面要区分「0 次」和「算不出来」
							used: map ? (map.get(k) || 0) : null,
							hourUsed: hourMap ? (hourMap.get(k) || 0) : null,
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

			const members = (Array.isArray(body.members) ? body.members : []).map(m => ({
				providerId: String(m.providerId || '').trim(),
				model: String(m.model || '').trim(),
				limit: Math.max(0, Math.floor(Number(m.limit) || 0)),
				status: m.status === 'disabled' ? 'disabled' : 'active'
			})).filter(m => m.providerId && m.model);

			if (!members.length) {
				return new Response(JSON.stringify({ error: '至少需要一个有效成员（渠道 + 模型名）' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
			}

			let groups = await getQuotaGroups(env);
			if (groups.some(g => g.name === name && g.id !== body.id)) {
				return new Response(JSON.stringify({ error: `已有同名的配额组：${name}` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
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
				// 会话粘性：同一会话尽量固定同一个成员（多轮/工具调用更稳）；默认关
				sticky: body.sticky === true,
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

		const baseUrl = String(provider.baseUrl || '').replace(/\/+$/, '');
		const headers = { 'Content-Type': 'application/json' };
		if (provider.apiKey) headers['Authorization'] = `Bearer ${provider.apiKey}`;
		const endpoint = baseUrl + '/models';

		try {
			const res = await fetch(endpoint, { method: 'GET', headers, signal: AbortSignal.timeout(20000) });
			if (!res.ok) {
				const errText = await res.text();
				return new Response(JSON.stringify({
					success: false,
					endpoint,
					error: `HTTP ${res.status}: ${errText.slice(0, 300)}`,
					upstreamMessage: extractUpstreamMessage(errText),
					hint: '并非所有上游都提供 /models 接口，遇到这种就只能手动填模型名。'
				}), { headers: { 'Content-Type': 'application/json' } });
			}

			const data = await res.json().catch(() => null);
			let models = [];
			if (Array.isArray(data?.data)) models = data.data.map(m => m && (m.id || m.name));
			else if (Array.isArray(data?.models)) models = data.models.map(m => m && (m.name || m.id));
			else if (Array.isArray(data)) models = data.map(m => (typeof m === 'string' ? m : (m && (m.id || m.name))));

			// 去掉 models/ 前缀，保证是可以直接用于 chat/completions 的裸 ID
			models = models.map(m => String(m || '').replace(/^models\//, '').trim()).filter(Boolean);

			return new Response(JSON.stringify({
				success: true,
				endpoint,
				count: models.length,
				models
			}), { headers: { 'Content-Type': 'application/json' } });
		} catch (e) {
			return new Response(JSON.stringify({
				success: false,
				endpoint,
				error: `连接失败: ${e.message}`,
				hint: '并非所有上游都提供 /models 接口，遇到这种就只能手动填模型名。'
			}), { headers: { 'Content-Type': 'application/json' } });
		}
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
async function handleLandingPage(request, env, ctx) {
	// ⚠️ 模板里（悬浮按钮 / 登录弹窗）引用了 isLoggedIn，删了这里就会 Error 1101（2026-10-05 踩过）
	const isLoggedIn = await verifyAdminCookie(request, env);
	// 账号池关闭时首页不显示 CF 用量看板（纯第三方反代模式下那些数字恒为 0）
	const cfEnabled = await getCfPoolEnabled(env);

	const html = `<!DOCTYPE html>
<head>
	<meta charset="UTF-8">
	<meta name="robots" content="noindex, nofollow">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Workers API Hub</title>
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
				<div class="form-group" style="margin-top: 10px;">
					<label for="login-password">管理员密码</label>
					<input type="password" id="login-password" placeholder="请输入管理员密码" onkeydown="if(event.key==='Enter')submitLogin()">
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
			const pwdInput = document.getElementById('login-password');
			if (pwdInput) {
				pwdInput.value = '';
				setTimeout(() => pwdInput.focus(), 100);
			}
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

				// 如果后端认为需要更新，则继续发送 POST 请求触发静默更新并获取最新数据
				if (data.needUpdate) {
					const updateRes = await fetch('/api/usage/summary', { method: 'POST' });
					if (updateRes.ok) {
						const freshData = await updateRes.json();
						renderPublicSummary(freshData);
					}
				}
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
			const password = document.getElementById('login-password').value;
			const res = await fetch('/api/auth/login', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ password })
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

	return new Response(html, {
		headers: { 'Content-Type': 'text/html; charset=utf-8' }
	});
}

// 2. 后台管理控制台页面
async function handleAdminPage(request, env, ctx) {
	// 运行模式决定渲染哪些界面：账号池关闭时（纯第三方反代）走「接入信息」作为落地页
	const cfEnabled = await getCfPoolEnabled(env);
	const defaultTab = cfEnabled ? 'overview' : 'access';

	// 配额组「重置基准」的时区下拉。offset 挂在 data-offset 上，前端的实时换算直接读它，
	// 不用再往页面里注一份数据。
	const resetTzOptions = Object.keys(RESET_TZ_PRESETS).map(function (k) {
		const p = RESET_TZ_PRESETS[k];
		return '<option value="' + k + '" data-offset="' + (p.offset === null ? '' : p.offset) + '">' + p.label + '</option>';
	}).join('');

	const html = `<!DOCTYPE html>
<head>
	<meta charset="UTF-8">
	<meta name="robots" content="noindex, nofollow">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Workers API Hub Dashboard</title>
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
			padding: 40px;
			min-width: 0;
			z-index: 10;
		}

		header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			margin-bottom: 30px;
			gap: 16px;
		}

		/* 页面标题右侧：主题切换 + 退出登录（原本在侧边栏底部） */
		.view-actions {
			display: flex;
			align-items: center;
			gap: 8px;
			flex-shrink: 0;
		}

		.view-actions .btn {
			height: 36px;
			padding: 0 13px;
			display: flex;
			align-items: center;
			gap: 6px;
			font-size: 13px;
			white-space: nowrap;
		}

		/* 窄屏：标题与操作挤在一起，按钮退化成纯图标 */
		@media (max-width: 760px) {
			header {
				gap: 12px;
			}

			header h1 {
				font-size: 20px !important;
			}

			.view-actions .btn-label {
				display: none;
			}

			.view-actions .btn {
				padding: 0 10px;
			}
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
		/* 渠道模型列：默认只露前几个，多了折起来 */
		.prov-models-toggle {
			display: block;
			background: none;
			border: none;
			padding: 2px 0;
			margin-top: 4px;
			color: var(--accent-color);
			cursor: pointer;
			font-size: 11.5px;
			font-family: inherit;
		}
		.prov-models-toggle:hover {
			text-decoration: underline;
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
				<div class="nav-item cf-only" id="menu-accounts" onclick="switchTab('accounts')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
					账号管理
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
				<div class="nav-item" id="menu-settings" onclick="switchTab('settings')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
					模型映射
				</div>
				<div class="nav-item" id="menu-quota" onclick="switchTab('quota')">
					<svg style="width: 18px; height: 18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"></path></svg>
					调用配额
				</div>
			</div>

			<div class="aside-footer">
				<div style="text-align: center; font-size: 11px; color: var(--text-muted); opacity: 0.55; padding-top: 4px;">
					<a href="https://github.com/ldg118/workers-api-hub" target="_blank" rel="noopener noreferrer" style="color: inherit; text-decoration: none; border-bottom: 1px solid currentColor;">GitHub</a> · Workers API Hub
				</div>
			</div>
		</aside>

		<!-- Main Workspace -->
		<main>
			<div id="auth-views" style="display: flex; flex-direction: column; gap: 30px; width: 100%;">
				
				<!-- Header -->
				<header>
					<div style="min-width: 0;">
						<h1 style="font-size: 26px; font-weight: 700;" id="view-title">数据看板</h1>
						<p style="color: var(--text-muted); font-size: 14px; margin-top: 4px;" id="view-subtitle"></p>
					</div>
					<div class="view-actions">
						<button class="btn btn-secondary" onclick="toggleTheme()" title="切换日间/夜间模式">
							<svg class="theme-icon-sun" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none; width: 16px; height: 16px;">
								<circle cx="12" cy="12" r="4" />
								<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
							</svg>
							<svg class="theme-icon-moon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width: 16px; height: 16px;">
								<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z" />
							</svg>
							<span class="btn-label">切换主题</span>
						</button>
						<button class="btn btn-secondary" onclick="logout()" title="退出登录">
							<svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path></svg>
							<span class="btn-label">退出登录</span>
						</button>
					</div>
				</header>

				<!-- TAB: Overview -->
				<div id="tab-overview" class="tab-content${defaultTab === 'overview' ? ' active' : ''}">
					<!-- 第三方渠道调用统计（本代理埋点，与下方 CF 官方账单是两条独立链路） -->
					<div class="section-card" style="margin-top: 24px;">
						<div class="section-header">
							<div class="section-title">第三方渠道调用</div>
							<div style="display: flex; align-items: center; gap: 8px;">
								<button class="btn btn-secondary" id="stats-range-today" onclick="setStatsRange('today')" style="padding: 6px 12px; font-size: 12px;">今日</button>
								<button class="btn btn-secondary" id="stats-range-7d" onclick="setStatsRange('7d')" style="padding: 6px 12px; font-size: 12px;">近 7 天</button>
								<button class="btn btn-secondary" id="stats-range-all" onclick="setStatsRange('all')" style="padding: 6px 12px; font-size: 12px;">全部</button>
								<button class="btn btn-secondary" id="stats-refresh" onclick="refreshProviderStats()" title="重新从 D1 读取统计" style="padding: 6px 12px; font-size: 12px;">↻ 刷新</button>
							</div>
						</div>
						<div class="section-note">数据来自本代理埋点（每次调用写一条到 D1），统计口径是「经过本代理的请求」，与上游账单不是一回事。</div>

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

						<table>
							<thead>
								<tr>
									<th>别名</th>
									<th>Account ID</th>
									<th>API Token</th>
									<th>操作</th>
								</tr>
							</thead>
							<tbody id="accounts-table-body">
								<!-- Accounts rows -->
							</tbody>
						</table>
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

						<table>
							<thead>
								<tr>
									<th>密钥描述</th>
									<th>API Key</th>
									<th>创建时间</th>
									<th>有效期 / 轮换</th>
									<th>操作</th>
								</tr>
							</thead>
							<tbody id="keys-table-body">
								<!-- Keys rows -->
							</tbody>
						</table>
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
							<div class="form-group" style="margin-bottom: 0;">
								<label>目标模型</label>
								<input type="text" id="map-target" list="map-target-options" placeholder="下拉选已有模型，或直接手输">
								<datalist id="map-target-options"></datalist>
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
									<th style="width: 100px;">操作</th>
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
						<div class="section-note">把一组「同一用途的候选模型」放进配额组，每个成员设一个次数上限（每天或每月）。默认按用量<b>自动分摊</b>，某个成员报错会<b>自动换下一个</b>并把它临时冷却。</div>
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
								<div class="hint-foot">已用次数复用调用统计里的数据，每个请求只多一次本地查询，<b>不会增加上游调用次数</b>。上限填 <code>0</code> 表示不限次数；<b>成员顺序只在用量相同时才决定优先</b>（不是固定顺位）—— 正常是「谁这一小时用得少用谁」。<br>重置时刻按组各自配置，默认 UTC 零点。用「基准时区 + 时刻」对齐上游的真实重置时间 —— 例如 Gemini 的每日配额在<b>太平洋时间午夜</b>重置，选「美国太平洋（夏令时）UTC-7」+ <code>00:00</code> 即可（换算成 UTC 07:00 / 北京时间 15:00）。</div>
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
							<strong>注意：</strong> 未绑定 D1 数据库（变量名 <code>DB</code>），配额组无法统计已用次数。设了次数上限的组会<b>直接报错</b>，不会静默放行。请到 Worker → Settings → Bindings 添加 D1。
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

						<table style="margin-top: 20px;">
							<thead>
								<tr>
									<th>渠道名</th>
									<th>Base URL</th>
									<th>模型</th>
									<th>状态</th>
									<th style="width: 190px;">操作</th>
								</tr>
							</thead>
							<tbody id="providers-table-body">
								<!-- Provider rows -->
							</tbody>
						</table>
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

			<div class="form-group" style="margin-top: 14px;">
					<label for="provider-gemini-native">Gemini 协议（只对 googleapis 渠道有意义）</label>
					<select id="provider-gemini-native" style="background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 12px 16px; border-radius: 10px; outline: none; font-size: 14px; font-family: inherit;">
						<option value="auto">自动：地址含 googleapis 就走原生</option>
						<option value="on">强制走原生（工具调用更稳）</option>
						<option value="off">强制走 OpenAI 兼容端点</option>
					</select>
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
							<th style="width: 64px;">操作</th>
						</tr>
					</thead>
					<tbody id="health-table-body">
						<!-- Health rows -->
					</tbody>
				</table>
				<div class="section-note">逐个向该渠道发一句 ping。结果会记在渠道上，渠道列表里模型名前面的圆点即为最近一次结果（灰=未测、绿=正常、红=失败）。单次最多测 12 个模型。</div>
				<div style="background-color: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 12px 14px; border-radius: 10px; font-size: 12.5px; color: var(--warning-color); line-height: 1.6; margin-top: 12px;"><strong>注意：</strong>测试会<b>真实调用上游</b> —— 既消耗上游额度，也会计入「调用配额」的已用次数。上游是按总请求数算额度的，测试同样占额度，所以别频繁点。</div>
			</div>
			<div class="modal-footer">
				<button class="btn btn-secondary" onclick="closeProviderHealthModal()">关闭</button>
				<button class="btn btn-success" id="btn-test-all-models" onclick="testAllProviderModels()" title="会消耗上游额度，并计入调用配额">测试全部模型</button>
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
				<div style="display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px;">
					<div class="form-group">
						<label for="quota-name">组名（自定义命名，不能含 / 和前缀 TT:）</label>
						<input type="text" id="quota-name" placeholder="如: gemini-free" oninput="updateQuotaNamePreview()">
						<div id="quota-name-call" style="font-size: 11px; color: var(--text-muted); margin-top: 4px; line-height: 1.5;">客户端调用时模型名填 <code id="quota-name-call-code" style="cursor: pointer;" onclick="copyQuotaCallName()" title="点击复制">TT:&lt;组名&gt;</code></div>
					</div>
					<div class="form-group">
						<label for="quota-period">重置周期</label>
						<select id="quota-period" onchange="updateQuotaResetPreview()">
							<option value="day">每天</option>
							<option value="month">每月 1 日</option>
						</select>
					</div>
					<div class="form-group">
						<label for="quota-reset-time">重置时刻（该时区的本地时间）</label>
						<input type="time" id="quota-reset-time" value="00:00" onchange="updateQuotaResetPreview()">
					</div>
					<div class="form-group">
						<label for="quota-reset-tz">基准时区</label>
						<select id="quota-reset-tz" onchange="toggleQuotaCustomOffset(); updateQuotaResetPreview();">
							${resetTzOptions}
						</select>
					</div>
				</div>

				<div class="form-group" id="quota-offset-wrap" style="display: none;">
					<label for="quota-reset-offset">自定义偏移（分钟；北京时间 = 480，太平洋夏令时 = -420）</label>
					<input type="number" id="quota-reset-offset" value="0" oninput="updateQuotaResetPreview()">
				</div>

				<div id="quota-reset-preview" style="font-size: 12px; color: var(--text-muted); line-height: 1.6;"></div>

				<div style="display: flex; align-items: center; justify-content: space-between;">
					<span style="font-size: 13px; color: var(--text-muted);">成员 —— 按负载自动分摊（谁用得少用谁），某个成员报错会自动换下一个</span>
					<button class="btn btn-secondary" onclick="addQuotaMemberRow()" style="padding: 6px 12px; font-size: 12px;">添加成员</button>
				</div>

				<div style="display: grid; grid-template-columns: 1.2fr 1.6fr 90px 76px 34px; gap: 8px; font-size: 12px; color: var(--text-muted);">
					<span>渠道</span><span>上游模型名</span><span>次数上限</span><span>状态</span><span></span>
				</div>
				<div id="quota-members" style="display: flex; flex-direction: column; gap: 10px;"></div>

				<label style="display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer;">
					<input type="checkbox" id="quota-status-active" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);">
					启用这个配额组
				</label>
				<label style="display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--text-muted); cursor: pointer; margin-top: 8px;">
					<input type="checkbox" id="quota-sticky" style="width: 16px; height: 16px; padding: 0; margin: 0; flex: none; accent-color: var(--accent-color);">
					会话粘性（同一个会话尽量固定用同一个成员 —— 多轮对话 / 工具调用更稳，代价是分摊略不均）
				</label>

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
		// 分组折叠状态：cf=null 表示首次加载还没初始化（按当前模式决定默认是否折叠）
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
				console.log('Skipping auto refresh, last fetch was ' + Math.round((now - lastFetched) / 1000) + 's ago');
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

		// ---------- 第三方渠道调用统计（本代理埋点，与 CF 官方账单是两条独立链路） ----------
		let statsRange = '7d';

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
				const res = await apiFetch('/api/provider-stats?range=' + encodeURIComponent(statsRange));
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
			setTextById('stats-tokens', (s.tokens || 0).toLocaleString());
			setTextById('stats-reasoning', '含思考 ' + (s.reasoningTokens || 0).toLocaleString());
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
				const models = (p.models || []).map(m => '<tr class="stats-model-row" data-parent="' + sen(p.id) + '" style="display:none;">'
					+ '<td style="padding-left:36px; font-size:12.5px; color: var(--text-muted);">' + sen(m.model || '(未记录)') + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.req || 0) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.ok || 0) + ' / ' + (m.fail || 0) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.req ? Math.round(m.ok / m.req * 1000) / 10 + '%' : '—') + '</td>'
					+ '<td style="font-size:12.5px;">' + statsLatencyCell(m.avgMs, m.req, m.lastMs, m.lastAt) + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.tokens || 0).toLocaleString() + '</td>'
					+ '<td style="font-size:12.5px;">' + (m.reasoningTokens || 0).toLocaleString() + '</td>'
					+ '<td style="font-size:12.5px;">$' + (m.costEst != null ? m.costEst.toFixed(2) : '0.00') + '</td>'
					+ '</tr>').join('');
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
			applyTabMeta(currentTab);
			loadRuntimeState();
			loadAccessSample();
			if (!document.body.classList.contains('cf-off')) {
				loadUsageDetails();
			}
			// 渠道统计两种模式都要加载（CF 段关闭时它仍然有内容）
			setStatsRange('7d');
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

		const tabMeta = {
			overview: ['数据看板', '第三方渠道调用统计与 Cloudflare 账号池用量总览'],
			accounts: ['账号管理', '管理 Cloudflare 账号，请求随机打散并自动故障切换'],
			access: ['接入信息', '把客户端接到这个代理上，点击地址即可复制'],
			providers: ['第三方渠道', '接入任意 OpenAI 兼容端点，按模型名分流'],
			settings: ['模型映射', '决定某个模型名最终走哪一个上游'],
			quota: ['调用配额', '给一组候选模型设次数上限，用满自动切换到下一个']
		};

		function applyTabMeta(tabName) {
			const meta = tabMeta[tabName] || ['', ''];
			document.getElementById('view-title').innerText = meta[0];
			document.getElementById('view-subtitle').innerText = meta[1];
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

			applyTabMeta(tabName);
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
				const tbody = document.getElementById('accounts-table-body');
				tbody.innerHTML = '';
				if (accounts.length === 0) {
					tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-muted); padding: 30px;">暂无配置的 Cloudflare 账号</td></tr>';
					return;
				}
				accounts.forEach(acc => {
					const maskedToken = acc.apiToken.length > 8 ? acc.apiToken.substring(0, 4) + '...' + acc.apiToken.substring(acc.apiToken.length - 4) : '********';
					const tr = document.createElement('tr');
					tr.innerHTML = \`
						<td><strong style="font-weight:600;">\${acc.name}</strong></td>
						<td><code>\${acc.accountId.length > 12 ? acc.accountId.substring(0, 6) + '...' + acc.accountId.substring(acc.accountId.length - 4) : '********'}</code></td>
						<td><code>\${maskedToken}</code></td>
						<td>
							<div style="display:flex; gap:8px;">
								<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" onclick="editAccount('\${acc.id}', '\${acc.name}', '\${acc.accountId}', '\${acc.apiToken}')">编辑</button>
								<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" onclick="deleteAccount('\${acc.id}')">删除</button>
							</div>
						</td>
					\`;
					tbody.appendChild(tr);
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

		function editAccount(id, name, accountId, apiToken) {
			document.getElementById('account-modal-title').innerText = '编辑 Cloudflare 账号';
			document.getElementById('account-id-edit').value = id;
			document.getElementById('account-name').value = name;
			document.getElementById('account-id').value = accountId;
			document.getElementById('account-token').value = apiToken;
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
			const tbody = document.getElementById('keys-table-body');
			tbody.innerHTML = '';
			const userKeys = keys.filter(k => !k.system);
			if (userKeys.length === 0) {
				document.getElementById('no-key-warning').classList.remove('hidden');
			} else {
				document.getElementById('no-key-warning').classList.add('hidden');
			}
			const now = Date.now();
			keys.forEach(k => {
				const tr = document.createElement('tr');
				const isSystem = !!k.system;
				const dateStr = k.createdAt ? new Date(k.createdAt).toLocaleString() : '—';
				// 有效期 / 轮换状态列
				let statusCell;
				if (isSystem) {
					statusCell = k.rotationEnabled
						? '自动轮换<br><span style="font-size:11px;color:var(--text-muted);">下次 ' + (k.nextRotationAt ? new Date(k.nextRotationAt).toLocaleDateString() : '—') + '</span>'
						: '永久（手动管理）';
				} else if (k.expiresAt) {
					statusCell = k.expiresAt > now
						? '<span style="color:var(--success-color);">有效</span> 至 ' + new Date(k.expiresAt).toLocaleDateString()
						: '<span style="color:var(--danger-color);">已过期</span>';
				} else {
					statusCell = '永久';
				}
				tr.innerHTML = \`
					<td><strong style="font-weight:600;">\${sen(k.name)}</strong>\${isSystem ? ' <span style="font-size:11px;color:var(--text-muted);">（系统默认 · 不可删）</span>' : ''}</td>
					<td>
						<div style="display:flex; align-items:center; gap:8px;">
							<code id="key-val-\${isSystem ? 'system' : k.id}">\${k.key.length > 6 ? k.key.substring(0, 5) + '...' + k.key.substring(k.key.length - 1) : k.key.substring(0, Math.min(3, k.key.length)) + '...'}</code>
							<button class="btn btn-secondary" style="padding:4px 8px; font-size:11px; border-radius:6px;" onclick="copyKeyText('\${k.key}')">复制</button>
						</div>
					</td>
					<td>\${dateStr}</td>
					<td style="font-size:13px;">\${statusCell}</td>
					<td>
						\${isSystem
							? '<button class="btn btn-secondary" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-regen>重新生成</button>'
							: '<button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-del="' + k.id + '">删除</button>'}
					</td>
				\`;
				if (isSystem) {
					tr.querySelector('[data-regen]').onclick = regenerateSystemKey;
				} else {
					tr.querySelector('[data-del]').onclick = () => deleteKey(k.id);
				}
				tbody.appendChild(tr);
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

		// 渠道模型列默认只露前几个，其余收起（模型名是「备忘录」，全列会把那一行撑很高）
		const PROVIDER_MODELS_PREVIEW = 3;

		async function loadProviders() {
			try {
				// 顺带拉今日调用统计：转发次数和探测（测试）次数分开显示在渠道名下面
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
				const tbody = document.getElementById('providers-table-body');
				tbody.innerHTML = '';
				if (!providersCache.length) {
					tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-muted); padding: 30px;">暂无第三方渠道</td></tr>';
					return;
				}
				providersCache.forEach(p => {
					const statusBadge = p.status === 'disabled'
						? '<span class="badge badge-danger">已停用</span>'
						: '<span class="badge badge-success">启用</span>';
					const healthMap = p.modelHealth || {};
					// 模型列表当「备忘录」用，全列出来会把整行撑很高 —— 默认只露前几个，其余折起来
					const modelLines = (p.models || []).map(m => {
						const h = healthMap[m];
						const dotColor = !h ? 'var(--text-muted)' : (h.ok ? 'var(--success-color)' : 'var(--danger-color)');
						const dotTitle = !h ? '尚未测试' : (h.ok ? '上次测试正常' : '上次测试失败');
						return '<span title="' + dotTitle + '" style="color:' + dotColor + ';">●</span> ' + sen(m);
					});
					let modelsText;
					if (!modelLines.length) {
						modelsText = '<span style="color: var(--text-muted);">未填写</span>';
					} else if (modelLines.length <= PROVIDER_MODELS_PREVIEW) {
						modelsText = modelLines.join('<br>');
					} else {
						const hiddenCount = modelLines.length - PROVIDER_MODELS_PREVIEW;
						modelsText = '<div class="prov-models">'
							+ modelLines.slice(0, PROVIDER_MODELS_PREVIEW).join('<br>')
							+ '<div class="prov-models-rest" style="display:none;">' + modelLines.slice(PROVIDER_MODELS_PREVIEW).join('<br>') + '</div>'
							+ '<button type="button" class="prov-models-toggle" data-rest="' + hiddenCount + '" onclick="toggleProviderModels(this)">…等 ' + hiddenCount + ' 个，点击展开</button>'
							+ '</div>';
					}
					// 今日用量：转发与测试分开显示。测试同样消耗上游额度，且会计入配额判断
					const st = statsById[p.id];
					const usageText = st
						? '<div style="font-size:11px; color: var(--text-muted); margin-top:4px;">今日转发 ' + st.req + ' · 测试 ' + (st.probeReq || 0) + ' 次</div>'
						: '';
				const fallbackText = usageText;
					const tr = document.createElement('tr');
					tr.innerHTML = \`
						<td><strong style="font-weight:600;">\${sen(p.name)}</strong>\${fallbackText}</td>
						<td><code style="word-break: break-all;">\${sen(p.baseUrl || '')}</code></td>
						<td style="font-size:12px; line-height:1.7;">\${modelsText}</td>
						<td>\${statusBadge}</td>
						<td style="white-space: nowrap;">
							<button class="btn btn-secondary" style="padding:6px 10px; font-size:12px; border-radius:6px; margin-right:6px;" onclick="openProviderHealthModal('\${p.id}')">测模型</button>
							<button class="btn btn-secondary" style="padding:6px 10px; font-size:12px; border-radius:6px; margin-right:6px;" onclick="openProviderModal('\${p.id}')">编辑</button>
							<button class="btn btn-danger" style="padding:6px 10px; font-size:12px; border-radius:6px;" onclick="deleteProvider('\${p.id}')">删除</button>
						</td>
					\`;
					tbody.appendChild(tr);
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
					'<td><button class="btn btn-secondary" style="padding:4px 8px; font-size:11px; border-radius:6px;" onclick="testOneModelAt(' + idx + ')">重测</button></td>';
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

		// 上游建议了替代模型名（如 Gemini 提示 2.5-flash 已下线、改用 3.8-flash），一键替换
		async function useSuggestedModelAt(idx) {
			const p = providersCache.find(x => x.id === healthProviderId);
			if (!p || !p.models || !p.models[idx]) return;
			const from = p.models[idx];
			const h = healthResults[from] || (p.modelHealth || {})[from];
			const to = h && h.suggestedModel;
			if (!to) return;
			if (!(await uiConfirm('把模型「' + from + '」替换为「' + to + '」？', { okText: '替换' }))) return;

			const nextModels = p.models.map(m => m === from ? to : m);
			const res = await apiFetch('/api/providers', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					id: p.id,
					name: p.name,
					baseUrl: p.baseUrl,
					models: nextModels.join(String.fromCharCode(10)),
					status: p.status,
					geminiNative: p.geminiNative
				})
			});
			if (!res.ok) {
				showToast('替换失败', 'error');
				return;
			}
			delete healthResults[from];
			showToast('已把 ' + from + ' 换成 ' + to);
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
			const unit = period === 'month' ? '本月' : '今日';
			const limit = Number(m.limit) || 0;
			// 本小时用量 = 负载均衡的依据，顺手展示（统计没启用时不显示）
			const hourTxt = (m.hourUsed === null || m.hourUsed === undefined)
				? ''
				: '<span style="color: var(--text-muted); margin-left: 6px;">本小时 ' + Number(m.hourUsed) + ' 次</span>';
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
				const t = m.used === null ? '不限次数（统计未启用）' : '不限次数 · ' + unit + '已用 ' + m.used + ' 次';
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
				+ '<span style="color: var(--text-muted); margin-left: 6px;">剩 ' + Math.max(0, limit - used) + ' 次</span>'
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
				const rows = (g.members || []).map((m, i) => {
					const c = quotaUsedCell(m, period);
					return '<div style="display:grid; grid-template-columns: 20px 110px minmax(0,1fr) 110px 170px 76px; gap:10px; align-items:center; padding:8px 0; border-top:1px solid var(--border-color);">'
						+ '<span style="font-size:12px; color: var(--text-muted);">' + (i + 1) + '</span>'
						+ '<span style="font-size:12.5px; color: var(--text-muted); overflow-wrap:anywhere;">' + sen(m.providerName || '（已删除）') + '</span>'
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
					+ '<span style="margin-left:auto; display:flex; gap:8px;">'
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

		let quotaRowSeq = 0;

		function quotaEditRowHtml(m) {
			const p = m || {};
			// 每行一个独立的 datalist —— 模型候选要跟着该行的渠道变，不能共用同一个
			const rid = 'qrow-' + (++quotaRowSeq);
			const inputStyle = 'background-color: var(--input-bg); border: 1px solid var(--input-border); color: var(--input-text); padding: 10px 12px; border-radius: 8px; outline: none; font-size: 13px; font-family: inherit; width: 100%;';
			const opts = ['<option value="">选择渠道…</option>'].concat(
				providersCache.map(x => '<option value="' + sen(x.id) + '"' + (x.id === p.providerId ? ' selected' : '') + '>' + sen(x.name) + '</option>')
			).join('');
			return '<div class="quota-member-row" style="display: grid; grid-template-columns: 1.2fr 1.6fr 90px 76px 34px; gap: 8px; align-items: center;">'
				+ '<select class="quota-m-provider" onchange="syncQuotaRowModels(this)" style="' + inputStyle + '">' + opts + '</select>'
				+ '<input type="text" class="quota-m-model" list="' + rid + '-models" placeholder="下拉选，或手输" value="' + sen(p.model || '') + '" style="' + inputStyle + '">'
				+ '<datalist id="' + rid + '-models"></datalist>'
				+ '<input type="number" class="quota-m-limit" min="0" placeholder="0" value="' + (Number(p.limit) || 0) + '" style="' + inputStyle + '">'
				+ '<select class="quota-m-status" style="' + inputStyle + '">'
				+ '<option value="active"' + (p.status !== 'disabled' ? ' selected' : '') + '>启用</option>'
				+ '<option value="disabled"' + (p.status === 'disabled' ? ' selected' : '') + '>停用</option>'
				+ '</select>'
				+ '<button class="btn btn-secondary" onclick="removeQuotaMemberRow(this)" style="padding:8px 4px; font-size:14px; color: var(--danger-color);">×</button>'
				+ '</div>';
		}

		// 换了渠道 → 该行的模型候选跟着换（只列这个渠道的模型，避免选到别家的）
		function syncQuotaRowModels(sel) {
			const row = sel.closest('.quota-member-row');
			if (!row) return;
			const list = row.querySelector('datalist');
			if (!list) return;
			const p = providersCache.find(x => x.id === sel.value);
			list.innerHTML = ((p && p.models) || [])
				.map(m => '<option value="' + sen(m) + '"></option>').join('');
		}

		// 打开弹窗后把每行的模型候选初始化一遍（编辑已有成员时要还原）
		function syncAllQuotaRowModels() {
			[].slice.call(document.querySelectorAll('#quota-members .quota-m-provider'))
				.forEach(sel => syncQuotaRowModels(sel));
		}

		function addQuotaMemberRow() {
			const box = document.getElementById('quota-members');
			if (box) box.insertAdjacentHTML('beforeend', quotaEditRowHtml(null));
		}

		function removeQuotaMemberRow(btn) {
			const row = btn.closest('.quota-member-row');
			if (row) row.remove();
		}

		// ---- 重置基准：把「本地时刻 + 时区」实时换算成 UTC 几点，省得用户自己算 ----
		function quotaResetOffsetMinutes() {
			const sel = document.getElementById('quota-reset-tz');
			if (!sel) return 0;
			if (sel.value === 'custom') {
				return Math.round(Number(document.getElementById('quota-reset-offset').value) || 0);
			}
			const opt = sel.options[sel.selectedIndex];
			const v = opt ? Number(opt.dataset.offset) : 0;
			return Number.isFinite(v) ? v : 0;
		}

		function toggleQuotaCustomOffset() {
			const sel = document.getElementById('quota-reset-tz');
			const wrap = document.getElementById('quota-offset-wrap');
			if (!sel || !wrap) return;
			wrap.style.display = sel.value === 'custom' ? '' : 'none';
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
			const tzSel = document.getElementById('quota-reset-tz');
			const tzLabel = (tzSel && tzSel.options[tzSel.selectedIndex])
				? String(tzSel.options[tzSel.selectedIndex].textContent || '').trim()
				: 'UTC';
			const period = document.getElementById('quota-period').value === 'month' ? '每月 1 日' : '每天';
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
			document.getElementById('quota-period').value = (g && g.period === 'month') ? 'month' : 'day';
			document.getElementById('quota-reset-tz').value = (g && g.resetTz) ? g.resetTz : 'utc';
			document.getElementById('quota-reset-time').value = (g && g.resetLocalTime) ? g.resetLocalTime : '00:00';
			document.getElementById('quota-reset-offset').value = (g && g.resetTzOffset) ? g.resetTzOffset : 0;
			// 先设好值再切显隐/算预览，否则算的是上一次的状态
			toggleQuotaCustomOffset();
			updateQuotaResetPreview();
			updateQuotaNamePreview();
			document.getElementById('quota-status-active').checked = g ? g.status !== 'disabled' : true;
			document.getElementById('quota-sticky').checked = !!(g && g.sticky === true);
			document.getElementById('quota-modal-title').innerText = g ? '编辑配额组' : '新建配额组';

			const members = (g && g.members) || [];
			document.getElementById('quota-members').innerHTML = members.length
				? members.map(m => quotaEditRowHtml(m)).join('')
				: quotaEditRowHtml(null);
			// 行插进 DOM 之后才能按各自选中的渠道填充模型候选
			syncAllQuotaRowModels();

			document.getElementById('quota-modal-hint').textContent = providersCache.length
				? '已用次数从调用统计里实时读取，不在这个弹窗里设置。上限填 0 表示不限次数。'
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
				status: r.querySelector('.quota-m-status').value
			})).filter(m => m.providerId && m.model);
		}

		async function saveQuotaGroup() {
			const payload = {
				name: document.getElementById('quota-name').value.trim(),
				period: document.getElementById('quota-period').value,
				status: document.getElementById('quota-status-active').checked ? 'active' : 'disabled',
				resetTz: document.getElementById('quota-reset-tz').value,
				resetLocalTime: document.getElementById('quota-reset-time').value || '00:00',
				resetTzOffset: Math.round(Number(document.getElementById('quota-reset-offset').value) || 0),
				sticky: document.getElementById('quota-sticky').checked === true,
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
		// 只是加速输入 —— 输入框本身仍可手输任意值（渠道里没填的模型也能临时用）
		function refreshMappingTargetOptions() {
			const list = document.getElementById('map-target-options');
			if (!list) return;
			const seen = {};
			const opts = [];
			const push = (v) => {
				if (!v || seen[v]) return;
				seen[v] = true;
				opts.push('<option value="' + sen(v) + '"></option>');
			};
			providersCache.forEach(p => {
				if (p.status === 'disabled') return;
				(p.models || []).forEach(m => push('provider:' + p.name + '/' + m));
			});
			Object.keys(defaultMappings).forEach(k => push(defaultMappings[k]));
			list.innerHTML = opts.join('');
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
				// 当前模式下不生效的那组默认折叠（只初始化一次，之后尊重用户手动展开）
				if (mappingGroupCollapsed.cf === null) mappingGroupCollapsed.cf = !mappingCfEnabled;
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
			const isPreset = Object.prototype.hasOwnProperty.call(defaultMappings, source) && defaultMappings[source] === target;
			const typeText = (isPreset ? '<span class="badge badge-success">预设映射</span>' : '<span class="badge badge-warning">自定义</span>')
				+ (problem ? ' <span class="badge badge-danger">未生效</span>' : '')
				+ (problem ? '<div style="color: var(--danger-color); font-size:11.5px; margin-top:5px; max-width:220px; white-space:normal; line-height:1.5;">' + sen(problem) + '</div>' : '');
			return '<tr>'
				+ '<td><code style="cursor:pointer;" title="点击复制" data-copy="' + sen(source) + '" onclick="copyModelId(this.dataset.copy)">' + sen(source) + '</code></td>'
				+ '<td><code style="cursor:pointer; word-break:break-all;" title="点击复制" data-copy="' + sen(target) + '" onclick="copyModelId(this.dataset.copy)">' + sen(target) + '</code>' + costTierBadge(target) + '</td>'
				+ '<td>' + typeText + '</td>'
				+ '<td><button class="btn btn-danger" style="padding:6px 12px; font-size:12px; border-radius:6px;" data-del="' + sen(source) + '" onclick="deleteMapping(this.dataset.del)">删除</button></td>'
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

		// 渠道模型列的「展开 / 收起」：默认只露前几个，点按钮切换
		function toggleProviderModels(btn) {
			const box = btn.closest('.prov-models');
			if (!box) return;
			const rest = box.querySelector('.prov-models-rest');
			if (!rest) return;
			const opening = rest.style.display === 'none';
			rest.style.display = opening ? '' : 'none';
			btn.textContent = opening ? '收起' : ('…等 ' + (btn.dataset.rest || '') + ' 个，点击展开');
		}

		function toggleMappingGroup(key) {
			mappingGroupCollapsed[key] = !mappingGroupCollapsed[key];
			renderMappings();
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

	return new Response(html, {
		headers: { 'Content-Type': 'text/html; charset=utf-8' }
	});
}

// 3. KV 未绑定时的报错页面
function handleKVError(request) {
	const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>KV 绑定异常 - Workers API Hub</title>
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
