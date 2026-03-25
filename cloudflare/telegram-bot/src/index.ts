import { SNAPSHOT } from "./snapshot-data";

interface Env {
  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_WEBHOOK_SECRET: string;
  BOT_NOTIFY_TOKEN: string;
  SUBSCRIBERS: KVNamespace;
}

type StrategyKey = keyof typeof SNAPSHOT.strategies;
type RebalanceKey = keyof typeof SNAPSHOT.rebalance;

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
    [{ text: "Daily Rebalance", callback_data: "rebalance" }],
    [
      { text: "Alerts On", callback_data: "subscribe" },
      { text: "Alerts Off", callback_data: "unsubscribe" },
    ],
    [{ text: "Menu", callback_data: "menu" }],
  ],
};

const SUBSCRIBERS_KEY = "telegram_subscribers";

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

function renderMenuText(): string {
  return [
    "Autostock Telegram Bot",
    "",
    "Commands",
    "/menu - main menu",
    "/v2 - latest Strategy V2 summary",
    "/v14 - latest Strategy V14 summary",
    "/rebalance - current rebalance signal",
    "/snapshot - full snapshot",
    "/subscribe - daily rebalance alerts on",
    "/unsubscribe - daily rebalance alerts off",
    "",
    `Snapshot generated at: ${SNAPSHOT.generatedAt}`,
  ].join("\n");
}

function renderStrategyText(strategyKey: StrategyKey): string {
  const strategy = SNAPSHOT.strategies[strategyKey];
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
    `Snapshot generated at: ${SNAPSHOT.generatedAt}`,
  ].join("\n");
}

function renderSignalText(strategyKey: RebalanceKey): string {
  const signal = SNAPSHOT.rebalance[strategyKey];
  const positions = signal.positions.length > 0 ? signal.positions.map(formatWeightRow).join("\n") : "- no positions";
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
    `QQQ close: ${Number(signal.qqqClose).toFixed(2)}`,
    `MA200 gap: ${formatPct(Number(signal.qqqMa200Gap) * 100)}`,
    `21d return: ${formatPct(Number(signal.qqqReturn21d) * 100)}`,
    `63d return: ${formatPct(Number(signal.qqqReturn63d) * 100)}`,
    `VIX: ${Number(signal.vixClose).toFixed(2)}`,
  ].join("\n");
}

function renderRebalanceText(): string {
  return [
    renderSignalText("v2"),
    "",
    "--------------------",
    "",
    renderSignalText("v14"),
  ].join("\n");
}

function renderDailyAlertText(): string {
  return [
    "Daily rebalance update",
    "",
    renderRebalanceText(),
    "",
    `Snapshot generated at: ${SNAPSHOT.generatedAt}`,
  ].join("\n");
}

function renderSnapshotText(): string {
  return [
    renderStrategyText("v2"),
    "",
    "====================",
    "",
    renderStrategyText("v14"),
    "",
    "====================",
    "",
    renderRebalanceText(),
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

function resolveTextForCommand(command: string): string {
  switch (command) {
    case "/start":
    case "/menu":
    case "/help":
      return renderMenuText();
    case "/v2":
      return renderStrategyText("v2");
    case "/v14":
      return renderStrategyText("v14");
    case "/rebalance":
      return renderRebalanceText();
    case "/snapshot":
      return renderSnapshotText();
    case "/subscribe":
      return [
        "Daily rebalance alerts are now ON.",
        "",
        "You will receive the current rebalance update every day after the snapshot refresh runs.",
      ].join("\n");
    case "/unsubscribe":
      return [
        "Daily rebalance alerts are now OFF.",
        "",
        "You can turn them back on anytime with /subscribe.",
      ].join("\n");
    default:
      return [
        "Unknown command.",
        "",
        renderMenuText(),
      ].join("\n");
  }
}

function resolveTextForCallback(data: string | undefined): string {
  switch (data) {
    case "menu":
      return renderMenuText();
    case "rebalance":
      return renderRebalanceText();
    case "strategy:v2":
      return renderStrategyText("v2");
    case "strategy:v14":
      return renderStrategyText("v14");
    case "subscribe":
      return [
        renderMenuText(),
        "",
        "Daily rebalance alerts are now ON.",
      ].join("\n");
    case "unsubscribe":
      return [
        renderMenuText(),
        "",
        "Daily rebalance alerts are now OFF.",
      ].join("\n");
    default:
      return renderMenuText();
  }
}

function isSubscriptionCommand(command: string): boolean {
  return command !== "/unsubscribe";
}

function verifyWebhook(request: Request, env: Env): boolean {
  const secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token");
  return Boolean(secret) && secret === env.TELEGRAM_WEBHOOK_SECRET;
}

function verifyNotifyRequest(request: Request, env: Env): boolean {
  const auth = request.headers.get("Authorization") || "";
  return auth === `Bearer ${env.BOT_NOTIFY_TOKEN}`;
}

function shouldDropSubscriber(result: TelegramResponse): boolean {
  const description = String(result.description || "").toLowerCase();
  return description.includes("chat not found") || description.includes("bot was blocked");
}

async function broadcastDailyAlert(env: Env): Promise<Response> {
  const subscribers = await loadSubscribers(env);
  const text = renderDailyAlertText();
  let delivered = 0;
  let removed = 0;

  for (const chatId of subscribers) {
    const result = await sendMessage(env, chatId, text);
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
    delivered,
    removed,
    subscribers: subscribers.length,
    generatedAt: SNAPSHOT.generatedAt,
  });
}

async function handleTelegramUpdate(update: TelegramUpdate, env: Env): Promise<Response> {
  if (update.message?.chat?.id && typeof update.message.text === "string") {
    const command = parseCommand(update.message.text);
    if (isSubscriptionCommand(command)) {
      await ensureSubscribed(env, update.message.chat.id);
    } else {
      await ensureUnsubscribed(env, update.message.chat.id);
    }
    const text = resolveTextForCommand(command);
    await sendMessage(env, update.message.chat.id, text);
    return jsonResponse({ ok: true });
  }

  if (update.callback_query?.message?.chat?.id && update.callback_query.message.message_id) {
    const chatId = update.callback_query.message.chat.id;
    if (update.callback_query.data === "unsubscribe") {
      await ensureUnsubscribed(env, chatId);
    } else {
      await ensureSubscribed(env, chatId);
    }
    const text = resolveTextForCallback(update.callback_query.data);
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
      return jsonResponse({
        ok: true,
        service: "autostock-telegram-bot",
        generatedAt: SNAPSHOT.generatedAt,
        commands: ["/menu", "/v2", "/v14", "/rebalance", "/snapshot", "/subscribe", "/unsubscribe"],
        subscribers: subscribers.length,
      });
    }

    if (request.method === "GET" && url.pathname === "/api/snapshot") {
      return jsonResponse(SNAPSHOT);
    }

    if (request.method === "POST" && url.pathname === "/api/notify") {
      if (!verifyNotifyRequest(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, { status: 401 });
      }
      return broadcastDailyAlert(env);
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
