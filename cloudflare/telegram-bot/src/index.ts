import { SNAPSHOT } from "./snapshot-data";

interface Env {
  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_WEBHOOK_SECRET: string;
}

type StrategyKey = keyof typeof SNAPSHOT.strategies;
type RebalanceKey = keyof typeof SNAPSHOT.rebalance;

type TelegramResponse = {
  ok: boolean;
  result?: unknown;
  description?: string;
};

type InlineKeyboard = {
  inline_keyboard: Array<Array<{ text: string; callback_data: string }>>;
};

type TelegramMessage = {
  message_id: number;
  chat?: { id: number };
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
      { text: "V2 성과", callback_data: "strategy:v2" },
      { text: "V14 성과", callback_data: "strategy:v14" },
    ],
    [{ text: "이번주 리밸런싱", callback_data: "rebalance" }],
    [{ text: "메뉴 새로고침", callback_data: "menu" }],
  ],
};

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
    "사용 가능한 명령",
    "/menu - 메뉴",
    "/v2 - Strategy V2 최신 요약",
    "/v14 - Strategy V14 최신 요약",
    "/rebalance - 이번주 리밸런싱",
    "/snapshot - 전체 스냅샷",
    "",
    `스냅샷 생성 시각: ${SNAPSHOT.generatedAt}`,
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
    `스냅샷 생성 시각: ${SNAPSHOT.generatedAt}`,
  ].join("\n");
}

function renderSignalText(strategyKey: RebalanceKey): string {
  const signal = SNAPSHOT.rebalance[strategyKey];
  const positions = signal.positions.length > 0 ? signal.positions.map(formatWeightRow).join("\n") : "- 포지션 없음";
  return [
    `${String(strategyKey).toUpperCase()} 이번주 리밸런싱`,
    "",
    `최신 시장일: ${signal.latestMarketDay}`,
    `신호일: ${signal.signalDay}`,
    `진입일: ${signal.entryDay}`,
    `상태: ${signal.regimeState}`,
    `사유: ${signal.regimeReason || "-"}`,
    "",
    "목표 비중",
    positions,
    "",
    `QQQ 종가: ${Number(signal.qqqClose).toFixed(2)}`,
    `MA200 gap: ${formatPct(Number(signal.qqqMa200Gap) * 100)}`,
    `21일 수익률: ${formatPct(Number(signal.qqqReturn21d) * 100)}`,
    `63일 수익률: ${formatPct(Number(signal.qqqReturn63d) * 100)}`,
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

async function sendMessage(env: Env, chatId: number, text: string): Promise<void> {
  await telegramApi(env, "sendMessage", {
    chat_id: chatId,
    text,
    reply_markup: MENU_KEYBOARD,
    disable_web_page_preview: true,
  });
}

async function editMessage(env: Env, chatId: number, messageId: number, text: string): Promise<void> {
  await telegramApi(env, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    reply_markup: MENU_KEYBOARD,
    disable_web_page_preview: true,
  });
}

async function answerCallback(env: Env, callbackQueryId: string, text?: string): Promise<void> {
  await telegramApi(env, "answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
  });
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
    default:
      return [
        "지원하지 않는 명령입니다.",
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
    default:
      return renderMenuText();
  }
}

async function handleTelegramUpdate(update: TelegramUpdate, env: Env): Promise<Response> {
  if (update.message?.chat?.id && typeof update.message.text === "string") {
    const text = resolveTextForCommand(parseCommand(update.message.text));
    await sendMessage(env, update.message.chat.id, text);
    return jsonResponse({ ok: true });
  }

  if (update.callback_query?.message?.chat?.id && update.callback_query.message.message_id) {
    const text = resolveTextForCallback(update.callback_query.data);
    await editMessage(
      env,
      update.callback_query.message.chat.id,
      update.callback_query.message.message_id,
      text,
    );
    await answerCallback(env, update.callback_query.id);
    return jsonResponse({ ok: true });
  }

  return jsonResponse({ ok: true, ignored: true });
}

function verifyWebhook(request: Request, env: Env): boolean {
  const secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token");
  return Boolean(secret) && secret === env.TELEGRAM_WEBHOOK_SECRET;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return jsonResponse({
        ok: true,
        service: "autostock-telegram-bot",
        generatedAt: SNAPSHOT.generatedAt,
        commands: ["/menu", "/v2", "/v14", "/rebalance", "/snapshot"],
      });
    }

    if (request.method === "GET" && url.pathname === "/api/snapshot") {
      return jsonResponse(SNAPSHOT);
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
