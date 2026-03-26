import { SNAPSHOT as FALLBACK_SNAPSHOT } from "./snapshot-data";

interface Env {
  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_WEBHOOK_SECRET: string;
  BOT_NOTIFY_TOKEN: string;
  GITHUB_ACTIONS_TOKEN: string;
  SUBSCRIBERS: KVNamespace;
}

type AppSnapshot = typeof FALLBACK_SNAPSHOT;
type StrategyKey = keyof AppSnapshot["strategies"];
type RebalanceKey = keyof AppSnapshot["rebalance"];
type SignalSnapshot = AppSnapshot["rebalance"][RebalanceKey];
type PriceRef = {
  symbol?: string;
  weightPct?: number;
  entryDay?: string;
  entryDayOpen?: number | null;
  latestMarketDay?: string;
  latestClose?: number | null;
};

type TelegramResponse = {
  ok: boolean;
  result?: unknown;
  description?: string;
  error_code?: number;
};

type InlineKeyboard = {
  inline_keyboard: Array<Array<{ text: string; callback_data: string }>>;
};

type TelegramChat = {
  id: number;
  type?: string;
};

type TelegramMessage = {
  message_id: number;
  chat?: TelegramChat;
  text?: string;
};

type TelegramCallbackQuery = {
  id: string;
  data?: string;
  message?: TelegramMessage;
};

type TelegramUpdate = {
  update_id?: number;
  message?: TelegramMessage;
  callback_query?: TelegramCallbackQuery;
};

const MENU_KEYBOARD: InlineKeyboard = {
  inline_keyboard: [
    [
      { text: "V2 Summary", callback_data: "strategy:v2" },
      { text: "V14 Summary", callback_data: "strategy:v14" },
    ],
    [{ text: "Current Signal", callback_data: "rebalance" }],
    [{ text: "Refresh Now", callback_data: "refresh" }],
    [
      { text: "Alerts On", callback_data: "subscribe" },
      { text: "Alerts Off", callback_data: "unsubscribe" },
    ],
    [{ text: "Menu", callback_data: "menu" }],
  ],
};

const SUBSCRIBERS_KEY = "telegram_subscribers";
const SNAPSHOT_KEY = "telegram_live_snapshot_v1";

function jsonResponse(payload: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
}

function formatPct(value: number, digits = 2): string {
  return `${value.toFixed(digits)}%`;
}

function formatDeltaPct(value: number, digits = 2): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}%p`;
}

function formatWeightRow(position: { symbol?: string; weight_pct?: number }): string {
  const symbol = String(position.symbol ?? "-");
  const weight = Number(position.weight_pct ?? 0);
  return `- ${symbol}: ${weight.toFixed(1)}%`;
}

function formatCompactPositions(signal: SignalSnapshot): string {
  if (!Array.isArray(signal.positions) || signal.positions.length === 0) {
    return "no positions";
  }
  return signal.positions
    .map((position) => `${String(position.symbol ?? "-")} ${Number(position.weight_pct ?? 0).toFixed(0)}%`)
    .join(", ");
}

function getPriceRefs(signal: SignalSnapshot): PriceRef[] {
  const refs = (signal as SignalSnapshot & { positionPriceRefs?: PriceRef[] }).positionPriceRefs;
  return Array.isArray(refs) ? refs : [];
}

function renderPriceReferenceBlock(signal: SignalSnapshot): string[] {
  const refs = getPriceRefs(signal);
  if (refs.length === 0) {
    return [];
  }
  const lines = ["Execution references"];
  for (const ref of refs) {
    const symbol = String(ref.symbol ?? "-");
    const entryOpen = Number(ref.entryDayOpen ?? NaN);
    const latestClose = Number(ref.latestClose ?? NaN);
    if (Number.isFinite(entryOpen)) {
      lines.push(`- original weekly entry: ${symbol} ${entryOpen.toFixed(2)} (${String(ref.entryDay || "-")} open)`);
    }
    if (Number.isFinite(latestClose)) {
      lines.push(`- if entering now: ${symbol} ${latestClose.toFixed(2)} (${String(ref.latestMarketDay || "-")} close ref)`);
    }
  }
  return lines;
}

function isWeeklySignalDay(signal: SignalSnapshot): boolean {
  return String(signal.latestMarketDay || "") === String(signal.signalDay || "");
}

function hasFreshWeeklySignal(snapshot: AppSnapshot): boolean {
  return Object.values(snapshot.rebalance).some((signal) => isWeeklySignalDay(signal));
}

function isSnapshotLike(value: unknown): value is AppSnapshot {
  if (!value || typeof value !== "object") {
    return false;
  }
  const payload = value as Record<string, unknown>;
  return Boolean(payload.generatedAt) && typeof payload.strategies === "object" && typeof payload.rebalance === "object";
}

async function loadSnapshot(env: Env): Promise<AppSnapshot> {
  const raw = await env.SUBSCRIBERS.get(SNAPSHOT_KEY);
  if (!raw) {
    return FALLBACK_SNAPSHOT;
  }
  try {
    const parsed = JSON.parse(raw);
    return isSnapshotLike(parsed) ? parsed : FALLBACK_SNAPSHOT;
  } catch {
    return FALLBACK_SNAPSHOT;
  }
}

async function saveSnapshot(env: Env, snapshot: AppSnapshot): Promise<void> {
  await env.SUBSCRIBERS.put(SNAPSHOT_KEY, JSON.stringify(snapshot));
}

function renderMenuText(snapshot: AppSnapshot): string {
  return [
    "Autostock Telegram Bot",
    "",
    "Commands",
    "/menu - main menu",
    "/v2 - latest Strategy V2 summary",
    "/v14 - latest Strategy V14 summary",
    "/rebalance - current rebalance signal",
    "/refresh - collect latest data and recompute now",
    "/snapshot - full snapshot",
    "/subscribe - daily status + weekly rebalance alerts on",
    "/unsubscribe - alerts off",
    "",
    `Snapshot generated at: ${snapshot.generatedAt}`,
  ].join("\n");
}

function renderStrategyText(snapshot: AppSnapshot, strategyKey: StrategyKey): string {
  const strategy = snapshot.strategies[strategyKey];
  const diff = Number(strategy.cagr) - Number(strategy.benchmarkCagr);
  return [
    `${strategy.label}`,
    "",
    `CAGR: ${formatPct(Number(strategy.cagr))}`,
    `QQQ CAGR: ${formatPct(Number(strategy.benchmarkCagr))}`,
    `CAGR diff: ${formatDeltaPct(diff)}`,
    `Max drawdown: ${formatPct(Number(strategy.drawdown))}`,
    `Avg turnover: ${Number(strategy.turnover).toFixed(3)}`,
    `P(alpha>0): ${Number(strategy.pAlphaGt0).toFixed(3)}`,
    "",
    `Snapshot generated at: ${snapshot.generatedAt}`,
  ].join("\n");
}

function renderSignalText(snapshot: AppSnapshot, strategyKey: RebalanceKey): string {
  const signal = snapshot.rebalance[strategyKey];
  const positions = signal.positions.length > 0 ? signal.positions.map(formatWeightRow).join("\n") : "- no positions";
  const priceRefs = renderPriceReferenceBlock(signal);
  return [
    `${String(strategyKey).toUpperCase()} rebalance`,
    "",
    `Latest market day: ${signal.latestMarketDay}`,
    `Signal day: ${signal.signalDay}`,
    `Entry day: ${signal.entryDay}`,
    `State: ${signal.regimeState}`,
    `Reason: ${signal.regimeReason || "-"}`,
    "",
    "Target weights",
    positions,
    "",
    `QQQ close: ${Number(signal.signalQqqClose).toFixed(2)}`,
    `MA200 gap: ${formatPct(Number(signal.signalQqqMa200Gap) * 100)}`,
    `21d return: ${formatPct(Number(signal.signalQqqReturn21d) * 100)}`,
    `63d return: ${formatPct(Number(signal.signalQqqReturn63d) * 100)}`,
    `VIX: ${Number(signal.signalVixClose).toFixed(2)}`,
    ...(priceRefs.length > 0 ? ["", ...priceRefs] : []),
  ].join("\n");
}

function renderRebalanceText(snapshot: AppSnapshot): string {
  return [
    renderSignalText(snapshot, "v2"),
    "",
    "--------------------",
    "",
    renderSignalText(snapshot, "v14"),
  ].join("\n");
}

function renderDailyStatusText(snapshot: AppSnapshot): string {
  const v2 = snapshot.rebalance.v2;
  const v14 = snapshot.rebalance.v14;
  return [
    "Daily market status",
    "",
    "No new weekly rebalance signal today.",
    "Keep the current weekly posture unless your own execution rules say otherwise.",
    "",
    `Latest market day: ${v2.latestMarketDay}`,
    `QQQ close: ${Number(v2.latestQqqClose).toFixed(2)}`,
    `QQQ MA200 gap: ${formatPct(Number(v2.latestQqqMa200Gap) * 100)}`,
    `QQQ 21d return: ${formatPct(Number(v2.latestQqqReturn21d) * 100)}`,
    `QQQ 63d return: ${formatPct(Number(v2.latestQqqReturn63d) * 100)}`,
    `VIX: ${Number(v2.latestVixClose).toFixed(2)}`,
    "",
    `V2 posture: ${v2.regimeState} -> ${formatCompactPositions(v2)}`,
    `V14 posture: ${v14.regimeState} -> ${formatCompactPositions(v14)}`,
    "",
    `Last weekly signal day: ${v2.signalDay}`,
    `Snapshot generated at: ${snapshot.generatedAt}`,
  ].join("\n");
}

function renderWeeklyRebalanceAlertText(snapshot: AppSnapshot): string {
  return [
    "Weekly rebalance signal",
    "",
    renderRebalanceText(snapshot),
    "",
    `Snapshot generated at: ${snapshot.generatedAt}`,
  ].join("\n");
}

function buildAutomatedAlert(snapshot: AppSnapshot): { kind: "daily_status" | "weekly_rebalance"; text: string } {
  if (hasFreshWeeklySignal(snapshot)) {
    return {
      kind: "weekly_rebalance",
      text: renderWeeklyRebalanceAlertText(snapshot),
    };
  }
  return {
    kind: "daily_status",
    text: renderDailyStatusText(snapshot),
  };
}

function renderSnapshotText(snapshot: AppSnapshot): string {
  return [
    renderStrategyText(snapshot, "v2"),
    "",
    "====================",
    "",
    renderStrategyText(snapshot, "v14"),
    "",
    "====================",
    "",
    renderRebalanceText(snapshot),
  ].join("\n");
}

async function telegramApi(env: Env, method: string, payload: Record<string, unknown>): Promise<TelegramResponse> {
  const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
    body: JSON.stringify(payload),
  });
  return (await response.json()) as TelegramResponse;
}

async function sendMessage(env: Env, chatId: number, text: string): Promise<TelegramResponse> {
  return telegramApi(env, "sendMessage", {
    chat_id: chatId,
    text,
    reply_markup: MENU_KEYBOARD,
    disable_web_page_preview: true,
  });
}

async function editMessage(env: Env, chatId: number, messageId: number, text: string): Promise<TelegramResponse> {
  return telegramApi(env, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    reply_markup: MENU_KEYBOARD,
    disable_web_page_preview: true,
  });
}

async function answerCallback(env: Env, callbackQueryId: string, text?: string): Promise<TelegramResponse> {
  return telegramApi(env, "answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
  });
}

async function loadSubscribers(env: Env): Promise<number[]> {
  const raw = await env.SUBSCRIBERS.get(SUBSCRIBERS_KEY);
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
  } catch {
    return [];
  }
}

async function saveSubscribers(env: Env, chatIds: number[]): Promise<void> {
  const unique = Array.from(new Set(chatIds.map((value) => Number(value)).filter((value) => Number.isFinite(value))));
  await env.SUBSCRIBERS.put(SUBSCRIBERS_KEY, JSON.stringify(unique));
}

async function ensureSubscribed(env: Env, chatId: number): Promise<boolean> {
  const subscribers = await loadSubscribers(env);
  if (subscribers.includes(chatId)) {
    return false;
  }
  subscribers.push(chatId);
  await saveSubscribers(env, subscribers);
  return true;
}

async function ensureUnsubscribed(env: Env, chatId: number): Promise<boolean> {
  const subscribers = await loadSubscribers(env);
  const next = subscribers.filter((value) => value !== chatId);
  if (next.length === subscribers.length) {
    return false;
  }
  await saveSubscribers(env, next);
  return true;
}

function parseCommand(text: string): string {
  return text.trim().split(/\s+/)[0].split("@")[0].toLowerCase();
}

function resolveTextForCommand(snapshot: AppSnapshot, command: string): string {
  switch (command) {
    case "/start":
    case "/menu":
    case "/help":
      return renderMenuText(snapshot);
    case "/v2":
      return renderStrategyText(snapshot, "v2");
    case "/v14":
      return renderStrategyText(snapshot, "v14");
    case "/rebalance":
      return renderRebalanceText(snapshot);
    case "/snapshot":
      return renderSnapshotText(snapshot);
    case "/refresh":
      return [
        "Refresh started.",
        "",
        "The bot is collecting current market data and recomputing the rebalance from scratch.",
        "Check /rebalance again in about a minute.",
      ].join("\n");
    case "/subscribe":
      return [
        "Alerts are now ON.",
        "",
        "You will receive daily market status updates and a weekly rebalance alert when the fresh signal day arrives.",
      ].join("\n");
    case "/unsubscribe":
      return [
        "Alerts are now OFF.",
        "",
        "You can turn them back on anytime with /subscribe.",
      ].join("\n");
    default:
      return [
        "Unknown command.",
        "",
        renderMenuText(snapshot),
      ].join("\n");
  }
}

function resolveTextForCallback(snapshot: AppSnapshot, data: string | undefined): string {
  switch (data) {
    case "menu":
      return renderMenuText(snapshot);
    case "rebalance":
      return renderRebalanceText(snapshot);
    case "strategy:v2":
      return renderStrategyText(snapshot, "v2");
    case "strategy:v14":
      return renderStrategyText(snapshot, "v14");
    case "refresh":
      return [
        renderMenuText(snapshot),
        "",
        "Refresh started.",
        "Check /rebalance again in about a minute.",
      ].join("\n");
    case "subscribe":
      return [
        renderMenuText(snapshot),
        "",
        "Alerts are now ON.",
      ].join("\n");
    case "unsubscribe":
      return [
        renderMenuText(snapshot),
        "",
        "Alerts are now OFF.",
      ].join("\n");
    default:
      return renderMenuText(snapshot);
  }
}

function isSubscriptionCommand(command: string): boolean {
  return command === "/start" || command === "/subscribe";
}

function isUnsubscribeCommand(command: string): boolean {
  return command === "/unsubscribe";
}

function verifyWebhook(request: Request, env: Env): boolean {
  const secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token");
  return Boolean(secret) && secret === env.TELEGRAM_WEBHOOK_SECRET;
}

function verifyNotifyRequest(request: Request, env: Env): boolean {
  const auth = request.headers.get("Authorization") || "";
  return auth === `Bearer ${env.BOT_NOTIFY_TOKEN}`;
}

async function triggerRefreshWorkflow(env: Env): Promise<boolean> {
  const response = await fetch("https://api.github.com/repos/needitem/autostock/actions/workflows/update-cloudflare-telegram-bot.yml/dispatches", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_ACTIONS_TOKEN}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "autostock-telegram-bot",
    },
    body: JSON.stringify({
      ref: "master",
      inputs: {
        notify_broadcast: "false",
      },
    }),
  });
  return response.ok;
}

function shouldDropSubscriber(result: TelegramResponse): boolean {
  const description = String(result.description || "").toLowerCase();
  return description.includes("chat not found") || description.includes("bot was blocked");
}

async function syncSnapshot(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as unknown;
  if (!isSnapshotLike(payload)) {
    return jsonResponse({ ok: false, error: "invalid_snapshot" }, { status: 400 });
  }
  await saveSnapshot(env, payload);
  return jsonResponse({
    ok: true,
    generatedAt: payload.generatedAt,
  });
}

async function broadcastAutomatedAlert(env: Env): Promise<Response> {
  const subscribers = await loadSubscribers(env);
  const snapshot = await loadSnapshot(env);
  const alert = buildAutomatedAlert(snapshot);
  let delivered = 0;
  let removed = 0;

  for (const chatId of subscribers) {
    const result = await sendMessage(env, chatId, alert.text);
    if (result.ok) {
      delivered += 1;
      continue;
    }
    if (shouldDropSubscriber(result)) {
      await ensureUnsubscribed(env, chatId);
      removed += 1;
    }
  }

  return jsonResponse({
    ok: true,
    kind: alert.kind,
    delivered,
    removed,
    subscribers: subscribers.length,
    generatedAt: snapshot.generatedAt,
  });
}

async function handleTelegramUpdate(update: TelegramUpdate, env: Env): Promise<Response> {
  const snapshot = await loadSnapshot(env);

  if (update.message?.chat?.id && typeof update.message.text === "string") {
    const command = parseCommand(update.message.text);
    if (command === "/refresh") {
      const started = await triggerRefreshWorkflow(env);
      const text = started
        ? resolveTextForCommand(snapshot, command)
        : "Refresh request failed. Try again in a moment.";
      await sendMessage(env, update.message.chat.id, text);
      return jsonResponse({ ok: started });
    }
    if (isSubscriptionCommand(command)) {
      await ensureSubscribed(env, update.message.chat.id);
    } else if (isUnsubscribeCommand(command)) {
      await ensureUnsubscribed(env, update.message.chat.id);
    }
    const text = resolveTextForCommand(snapshot, command);
    await sendMessage(env, update.message.chat.id, text);
    return jsonResponse({ ok: true });
  }

  if (update.callback_query?.message?.chat?.id && update.callback_query.message.message_id) {
    const chatId = update.callback_query.message.chat.id;
    if (update.callback_query.data === "refresh") {
      const started = await triggerRefreshWorkflow(env);
      const text = started
        ? resolveTextForCallback(snapshot, update.callback_query.data)
        : [renderMenuText(snapshot), "", "Refresh request failed. Try again in a moment."].join("\n");
      await editMessage(env, chatId, update.callback_query.message.message_id, text);
      await answerCallback(env, update.callback_query.id, started ? "Refresh started" : "Refresh failed");
      return jsonResponse({ ok: started });
    }
    if (update.callback_query.data === "subscribe") {
      await ensureSubscribed(env, chatId);
    } else if (update.callback_query.data === "unsubscribe") {
      await ensureUnsubscribed(env, chatId);
    }
    const text = resolveTextForCallback(snapshot, update.callback_query.data);
    await editMessage(env, chatId, update.callback_query.message.message_id, text);
    await answerCallback(env, update.callback_query.id);
    return jsonResponse({ ok: true });
  }

  return jsonResponse({ ok: true, ignored: true });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      const subscribers = await loadSubscribers(env);
      const snapshot = await loadSnapshot(env);
      return jsonResponse({
        ok: true,
        service: "autostock-telegram-bot",
        generatedAt: snapshot.generatedAt,
        commands: ["/menu", "/v2", "/v14", "/rebalance", "/refresh", "/snapshot", "/subscribe", "/unsubscribe"],
        subscribers: subscribers.length,
        automatedAlertKind: buildAutomatedAlert(snapshot).kind,
      });
    }

    if (request.method === "GET" && url.pathname === "/api/snapshot") {
      const snapshot = await loadSnapshot(env);
      return jsonResponse(snapshot);
    }

    if (request.method === "POST" && url.pathname === "/api/snapshot-sync") {
      if (!verifyNotifyRequest(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, { status: 401 });
      }
      return syncSnapshot(request, env);
    }

    if (request.method === "POST" && url.pathname === "/api/notify") {
      if (!verifyNotifyRequest(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, { status: 401 });
      }
      return broadcastAutomatedAlert(env);
    }

    if (request.method === "POST" && url.pathname === "/telegram") {
      if (!verifyWebhook(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, { status: 401 });
      }
      const update = (await request.json()) as TelegramUpdate;
      return handleTelegramUpdate(update, env);
    }

    return jsonResponse({ ok: false, error: "not_found" }, { status: 404 });
  },
};
