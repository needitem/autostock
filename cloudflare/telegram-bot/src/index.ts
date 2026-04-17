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
type EventRuntimeKey = keyof AppSnapshot["eventRuntime"];
type EventRuntimeSnapshot = AppSnapshot["eventRuntime"][EventRuntimeKey];
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
    [{ text: "Overview", callback_data: "overview:tsla" }],
    [{ text: "Next Event", callback_data: "event_next:tsla" }],
    [{ text: "Why", callback_data: "event_why:tsla" }],
    [{ text: "Refresh", callback_data: "refresh" }],
    [
      { text: "Alerts On", callback_data: "subscribe" },
      { text: "Alerts Off", callback_data: "unsubscribe" },
    ],
    [{ text: "Menu", callback_data: "menu" }],
  ],
};

const SUBSCRIBERS_KEY = "telegram_subscribers";
const SNAPSHOT_KEY = "telegram_live_snapshot_v1";
const BUDGET_STATE_PREFIX = "telegram_budget_state:";
const MARKET_SESSION_STATE_PREFIX = "telegram_market_session_alert:";
const USDKRW_RATE_URL = "https://api.frankfurter.dev/v2/rate/USD/KRW";

const BUDGET_MODE_KEYBOARD: InlineKeyboard = {
  inline_keyboard: [
    [
      { text: "KRW 금액 입력", callback_data: "budget_mode:krw" },
      { text: "USD 금액 입력", callback_data: "budget_mode:usd" },
    ],
    [
      { text: "₩100만", callback_data: "budget_preset:krw:1000000" },
      { text: "₩500만", callback_data: "budget_preset:krw:5000000" },
    ],
    [
      { text: "USD 5k", callback_data: "budget_preset:usd:5000" },
      { text: "USD 10k", callback_data: "budget_preset:usd:10000" },
    ],
    [{ text: "Open Menu", callback_data: "menu" }],
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

function parseOptionalNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function formatMaybePct(value: unknown, digits = 2): string {
  const num = parseOptionalNumber(value);
  return num === null ? "-" : formatPct(num, digits);
}

function formatMaybePrice(value: unknown, digits = 2): string {
  const num = parseOptionalNumber(value);
  return num === null ? "-" : num.toFixed(digits);
}

function formatUsd(value: number, digits = 2): string {
  return `$${value.toFixed(digits)}`;
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

function getLivePriceRefs(signal: SignalSnapshot): PriceRef[] {
  const refs = (signal as SignalSnapshot & { livePositionPriceRefs?: PriceRef[] }).livePositionPriceRefs;
  return Array.isArray(refs) ? refs : [];
}

function renderWeeklyPriceReferenceBlock(signal: SignalSnapshot): string[] {
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

function renderLivePriceReferenceBlock(signal: SignalSnapshot): string[] {
  const refs = getLivePriceRefs(signal);
  if (refs.length === 0) {
    return [];
  }
  const lines = ["Now references"];
  for (const ref of refs) {
    const symbol = String(ref.symbol ?? "-");
    const latestClose = Number(ref.latestClose ?? NaN);
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

function snapshotTimestamp(snapshot: AppSnapshot): number {
  const raw = String(snapshot.generatedAt || "").trim();
  const normalized = raw.includes("T") ? raw : raw.replace(" ", "T");
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : 0;
}

function hasUsableRebalanceSnapshot(snapshot: AppSnapshot): boolean {
  const signal = snapshot.rebalance.v4;
  return Boolean(
    String(signal.latestMarketDay || signal.signalDay || signal.liveSignalDay || "").trim()
  );
}

async function loadSnapshot(env: Env): Promise<AppSnapshot> {
  const raw = await env.SUBSCRIBERS.get(SNAPSHOT_KEY);
  if (!raw) {
    return FALLBACK_SNAPSHOT;
  }
  try {
    const parsed = JSON.parse(raw);
    if (!isSnapshotLike(parsed)) {
      return FALLBACK_SNAPSHOT;
    }
    return snapshotTimestamp(parsed) >= snapshotTimestamp(FALLBACK_SNAPSHOT) ? parsed : FALLBACK_SNAPSHOT;
  } catch {
    return FALLBACK_SNAPSHOT;
  }
}

async function saveSnapshot(env: Env, snapshot: AppSnapshot): Promise<void> {
  await env.SUBSCRIBERS.put(SNAPSHOT_KEY, JSON.stringify(snapshot));
}

type BudgetState = {
  currency: "USD" | "KRW";
};

type FxQuote = {
  rate: number;
  date?: string;
  source: "api" | "manual";
};

type MarketSessionKind = "open" | "close";

function budgetStateKey(chatId: number): string {
  return `${BUDGET_STATE_PREFIX}${chatId}`;
}

async function loadBudgetState(env: Env, chatId: number): Promise<BudgetState | null> {
  const raw = await env.SUBSCRIBERS.get(budgetStateKey(chatId));
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as BudgetState;
    if (parsed && (parsed.currency === "USD" || parsed.currency === "KRW")) {
      return parsed;
    }
  } catch {
    return null;
  }
  return null;
}

async function saveBudgetState(env: Env, chatId: number, state: BudgetState): Promise<void> {
  await env.SUBSCRIBERS.put(budgetStateKey(chatId), JSON.stringify(state));
}

async function clearBudgetState(env: Env, chatId: number): Promise<void> {
  await env.SUBSCRIBERS.delete(budgetStateKey(chatId));
}

function marketSessionStateKey(kind: MarketSessionKind, dateKey: string): string {
  return `${MARKET_SESSION_STATE_PREFIX}${kind}:${dateKey}`;
}

function currentNewYorkParts(now = new Date()): {
  dateKey: string;
  weekday: number;
  hour: number;
  minute: number;
  year: number;
  month: number;
  day: number;
} {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(fmt.formatToParts(now).map((part) => [part.type, part.value]));
  const weekdayMap: Record<string, number> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
  };
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  return {
    dateKey: `${parts.year}-${parts.month}-${parts.day}`,
    weekday: weekdayMap[String(parts.weekday)] ?? 0,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    year,
    month,
    day,
  };
}

function nthWeekdayOfMonth(year: number, month: number, weekday: number, nth: number): number {
  const first = new Date(Date.UTC(year, month - 1, 1));
  const firstWeekday = first.getUTCDay();
  const offset = (weekday - firstWeekday + 7) % 7;
  return 1 + offset + (nth - 1) * 7;
}

function lastWeekdayOfMonth(year: number, month: number, weekday: number): number {
  const last = new Date(Date.UTC(year, month, 0));
  const lastWeekday = last.getUTCDay();
  const offset = (lastWeekday - weekday + 7) % 7;
  return last.getUTCDate() - offset;
}

function observedFixedHoliday(year: number, month: number, day: number): string {
  const dt = new Date(Date.UTC(year, month - 1, day));
  const weekday = dt.getUTCDay();
  if (weekday === 6) {
    dt.setUTCDate(dt.getUTCDate() - 1);
  } else if (weekday === 0) {
    dt.setUTCDate(dt.getUTCDate() + 1);
  }
  return dt.toISOString().slice(0, 10);
}

function easterSunday(year: number): { month: number; day: number } {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return { month, day };
}

function goodFridayDateKey(year: number): string {
  const easter = easterSunday(year);
  const dt = new Date(Date.UTC(year, easter.month - 1, easter.day));
  dt.setUTCDate(dt.getUTCDate() - 2);
  return dt.toISOString().slice(0, 10);
}

function isUsMarketHoliday(year: number, month: number, day: number): boolean {
  const dateKey = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  const holidays = new Set<string>([
    observedFixedHoliday(year, 1, 1),
    `${year}-01-${String(nthWeekdayOfMonth(year, 1, 1, 3)).padStart(2, "0")}`,
    `${year}-02-${String(nthWeekdayOfMonth(year, 2, 1, 3)).padStart(2, "0")}`,
    goodFridayDateKey(year),
    `${year}-05-${String(lastWeekdayOfMonth(year, 5, 1)).padStart(2, "0")}`,
    observedFixedHoliday(year, 6, 19),
    observedFixedHoliday(year, 7, 4),
    `${year}-09-${String(nthWeekdayOfMonth(year, 9, 1, 1)).padStart(2, "0")}`,
    `${year}-11-${String(nthWeekdayOfMonth(year, 11, 4, 4)).padStart(2, "0")}`,
    observedFixedHoliday(year, 12, 25),
  ]);
  return holidays.has(dateKey);
}

function currentMarketSession(now = new Date()): { kind: MarketSessionKind; dateKey: string } | null {
  const ny = currentNewYorkParts(now);
  if (ny.weekday === 0 || ny.weekday === 6) {
    return null;
  }
  if (isUsMarketHoliday(ny.year, ny.month, ny.day)) {
    return null;
  }
  if (ny.hour === 9 && ny.minute >= 30 && ny.minute < 45) {
    return { kind: "open", dateKey: ny.dateKey };
  }
  if (ny.hour === 16 && ny.minute >= 0 && ny.minute < 15) {
    return { kind: "close", dateKey: ny.dateKey };
  }
  return null;
}

function renderMenuText(snapshot: AppSnapshot): string {
  return [
    "Autostock Event Runtime",
    "",
    "Commands",
    "/menu - main menu",
    "/event - current event signal",
    "/next - next important event",
    "/why - current event rationale",
    "/refresh - recompute current runtime now",
    "/snapshot - full event snapshot",
    "/subscribe - alerts on",
    "/unsubscribe - alerts off",
    "",
    `Snapshot generated at: ${snapshot.generatedAt}`,
  ].join("\n");
}

function eventRuntimeSnapshot(snapshot: AppSnapshot, key: string = "tsla"): EventRuntimeSnapshot | null {
  const runtime = (snapshot as AppSnapshot & { eventRuntime?: Record<string, EventRuntimeSnapshot> }).eventRuntime;
  if (!runtime || typeof runtime !== "object") {
    return null;
  }
  const selected = runtime[key];
  return selected && typeof selected === "object" ? selected : null;
}

function renderOverviewText(snapshot: AppSnapshot, key = "tsla"): string {
  const runtime = eventRuntimeSnapshot(snapshot, key);
  if (!runtime) {
    return "Event runtime is not available yet.";
  }
  const nextRow = Array.isArray(runtime.nextKnownEvents) && runtime.nextKnownEvents.length > 0
    ? runtime.nextKnownEvents[0]
    : null;
  const lastEvent = Array.isArray(runtime.rawEvents) && runtime.rawEvents.length > 0
    ? runtime.rawEvents[0]
    : null;
  return [
    `${String(runtime.symbol || key).toUpperCase()} overview`,
    "",
    `Action: ${String(runtime.action || "-")}`,
    `Confidence: ${Number(runtime.confidence ?? 0).toFixed(2)}`,
    `Macro: ${String(runtime.macroMode || "-")} / ${String(runtime.macroReason || "-")}`,
    `Chart: ${String(runtime.chartState || "-")} / volume ${formatMaybePrice(runtime.volumeRatio)}`,
    `Price: ${formatMaybePrice(runtime.price)}`,
    "",
    `Next event: ${nextRow ? `${String(nextRow.headline || "-")} (${String(nextRow.expected_date || "-")})` : "-"}`,
    `Latest important event: ${lastEvent ? String(lastEvent.headline || "-") : "-"}`,
    "",
    `Last runtime update: ${String((runtime.state || {}).lastRunAt || runtime.generatedAt || "-")}`,
  ].join("\n");
}

function renderBudgetModeText(snapshot: AppSnapshot): string {
  const signal = snapshot.rebalance.v4;
  const liveCash = Number((signal as SignalSnapshot & { liveCashPct?: number }).liveCashPct ?? NaN);
  return [
    "Buy quantity calculator",
    "",
    "Choose a currency mode with the buttons below.",
    "After choosing, send only the amount.",
    "You can also tap a quick amount button.",
    "",
    `Current target invested: ${(100 - (Number.isFinite(liveCash) ? liveCash : 0)).toFixed(2)}%`,
    `Current target cash: ${Number.isFinite(liveCash) ? liveCash.toFixed(2) : "-"}%`,
    "KRW mode uses live USD/KRW from Frankfurter.",
  ].join("\n");
}

function renderBudgetInputPrompt(currency: "USD" | "KRW"): string {
  if (currency === "KRW") {
    return [
      "KRW input mode",
      "",
      "Now send the total portfolio amount in KRW.",
      "Examples:",
      "- 10000000",
      "- 1000만원",
      "",
      "Live USD/KRW will be fetched automatically.",
    ].join("\n");
  }
  return [
    "USD input mode",
    "",
    "Now send the total portfolio amount in USD.",
    "Examples:",
    "- 10000",
    "- 25000",
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

function renderEventRuntimeText(snapshot: AppSnapshot, key = "tsla"): string {
  const runtime = eventRuntimeSnapshot(snapshot, key);
  if (!runtime) {
    return "Event runtime is not available yet.";
  }
  return [
    `${String(runtime.symbol || key).toUpperCase()} event runtime`,
    "",
    `Action: ${String(runtime.action || "-")}`,
    `Confidence: ${Number(runtime.confidence ?? 0).toFixed(2)}`,
    `Event: ${String(runtime.eventSignal || "-")} / ${String(runtime.eventStrength || "-")}`,
    `Macro mode: ${String(runtime.macroMode || "-")}`,
    `Macro reason: ${String(runtime.macroReason || "-")}`,
    `Price: ${formatMaybePrice(runtime.price)}`,
    `Chart: ${String(runtime.chartState || "-")}`,
    `Volume ratio: ${formatMaybePrice(runtime.volumeRatio)}`,
    "",
    `Last runtime update: ${String((runtime.state || {}).lastRunAt || runtime.generatedAt || "-")}`,
    `Seen events: ${String((runtime.state || {}).seenEventCount ?? "-")}`,
  ].join("\n");
}

function renderEventNextText(snapshot: AppSnapshot, key = "tsla"): string {
  const runtime = eventRuntimeSnapshot(snapshot, key);
  if (!runtime) {
    return "Next important events are not available yet.";
  }
  const rows = Array.isArray(runtime.nextKnownEvents) ? runtime.nextKnownEvents : [];
  return [
    `${String(runtime.symbol || key).toUpperCase()} next important events`,
    "",
    ...(rows.length > 0
      ? rows.slice(0, 5).map((row) => `- ${String(row.headline || "-")} (${String(row.expected_date || "-")}, D-${String(row.days_until ?? "-")})`)
      : ["- none"]),
  ].join("\n");
}

function renderEventWhyText(snapshot: AppSnapshot, key = "tsla"): string {
  const runtime = eventRuntimeSnapshot(snapshot, key);
  if (!runtime) {
    return "Event rationale is not available yet.";
  }
  const reasons = Array.isArray(runtime.reasons) ? runtime.reasons : [];
  const events = Array.isArray(runtime.rawEvents) ? runtime.rawEvents : [];
  return [
    `${String(runtime.symbol || key).toUpperCase()} why now`,
    "",
    `Current action: ${String(runtime.action || "-")}`,
    `Macro: ${String(runtime.macroMode || "-")} / ${String(runtime.macroReason || "-")}`,
    `Chart: ${String(runtime.chartState || "-")} / volume ${formatMaybePrice(runtime.volumeRatio)}`,
    "",
    "Reasons",
    ...(reasons.length > 0 ? reasons.map((row) => `- ${String(row)}`) : ["- none"]),
    "",
    "Recent important events",
    ...(events.length > 0
      ? events.slice(0, 5).map((row) => `- ${String(row.headline || "-")} [${String(row.source || "-")}/${String(row.category || "-")}]`)
      : ["- none"]),
  ].join("\n");
}

function renderSignalText(snapshot: AppSnapshot, strategyKey: RebalanceKey): string {
  const signal = snapshot.rebalance[strategyKey];
  const signalCash = Number((signal as SignalSnapshot & { cashPct?: number }).cashPct ?? NaN);
  const liveCash = Number((signal as SignalSnapshot & { liveCashPct?: number }).liveCashPct ?? NaN);
  const positions = signal.positions.length > 0 ? signal.positions.map(formatWeightRow).join("\n") : "- no positions";
  const livePositions = Array.isArray(signal.livePositions) && signal.livePositions.length > 0
    ? signal.livePositions.map(formatWeightRow).join("\n")
    : "- no positions";
  const weeklyPriceRefs = renderWeeklyPriceReferenceBlock(signal);
  const livePriceRefs = renderLivePriceReferenceBlock(signal);
  return [
    `${String(strategyKey).toUpperCase()} current rebalance`,
    "",
    "Latest actionable rebalance",
    `- latest market day: ${signal.latestMarketDay}`,
    `- signal day: ${signal.signalDay}`,
    `- entry day: ${signal.entryDay}`,
    `- state: ${signal.regimeState}`,
    `- reason: ${signal.regimeReason || "-"}`,
    "",
    "Target weights",
    positions,
    `- CASH: ${Number.isFinite(signalCash) ? signalCash.toFixed(2) : "-"}%`,
    "",
    "Signal-day benchmark context",
    `- QQQ close: ${formatMaybePrice(signal.signalQqqClose)}`,
    `- MA200 gap: ${formatMaybePct(signal.signalQqqMa200Gap)}`,
    `- 21d return: ${formatMaybePct(signal.signalQqqReturn21d)}`,
    `- 63d return: ${formatMaybePct(signal.signalQqqReturn63d)}`,
    `- VIX: ${formatMaybePrice(signal.signalVixClose)}`,
    ...(weeklyPriceRefs.length > 0 ? ["", ...weeklyPriceRefs] : []),
    "",
    "Latest portfolio state",
    `- as of: ${signal.liveSignalDay || signal.latestMarketDay}`,
    `- state: ${signal.liveRegimeState || "-"}`,
    `- reason: ${signal.liveRegimeReason || "-"}`,
    "",
    "Now target weights",
    livePositions,
    `- CASH: ${Number.isFinite(liveCash) ? liveCash.toFixed(2) : "-"}%`,
    "",
    "Latest benchmark context",
    `- QQQ close: ${formatMaybePrice(signal.latestQqqClose)}`,
    `- MA200 gap: ${formatMaybePct(signal.latestQqqMa200Gap)}`,
    `- 21d return: ${formatMaybePct(signal.latestQqqReturn21d)}`,
    `- 63d return: ${formatMaybePct(signal.latestQqqReturn63d)}`,
    `- VIX: ${formatMaybePrice(signal.latestVixClose)}`,
    ...(livePriceRefs.length > 0 ? ["", ...livePriceRefs] : []),
  ].join("\n");
}

function renderRebalanceText(snapshot: AppSnapshot): string {
  return renderSignalText(snapshot, "v4");
}

function renderDailyStatusText(snapshot: AppSnapshot): string {
  const v4 = snapshot.rebalance.v4;
  return [
    "Daily market status",
    "",
    "No new weekly rebalance signal today.",
    "Keep the current weekly posture unless your own execution rules say otherwise.",
    "",
    `Latest market day: ${v4.latestMarketDay}`,
    `QQQ close: ${formatMaybePrice(v4.latestQqqClose)}`,
    `QQQ MA200 gap: ${formatMaybePct(v4.latestQqqMa200Gap)}`,
    `QQQ 21d return: ${formatMaybePct(v4.latestQqqReturn21d)}`,
    `QQQ 63d return: ${formatMaybePct(v4.latestQqqReturn63d)}`,
    `VIX: ${formatMaybePrice(v4.latestVixClose)}`,
    "",
    `V4 posture now: ${String(v4.liveRegimeState || v4.regimeState)} -> ${formatCompactPositions({ ...v4, positions: v4.livePositions || v4.positions })}`,
    "",
    `Last weekly signal day: ${v4.signalDay}`,
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

function renderMarketSessionAlertText(snapshot: AppSnapshot, kind: MarketSessionKind): string {
  const signal = snapshot.rebalance.v4;
  const liveCash = Number((signal as SignalSnapshot & { liveCashPct?: number }).liveCashPct ?? NaN);
  return [
    kind === "open" ? "US market opened" : "US market closed",
    "",
    `Session: ${kind === "open" ? "regular open (09:30 ET)" : "regular close (16:00 ET)"}`,
    `V4 posture: ${String(signal.liveRegimeState || signal.regimeState || "-")}`,
    `Target weights: ${formatCompactPositions({ ...signal, positions: signal.livePositions || signal.positions })}`,
    `Target cash: ${Number.isFinite(liveCash) ? liveCash.toFixed(2) : "-"}%`,
    "",
    "Use /rebalance for the full current allocation.",
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
    renderOverviewText(snapshot, "tsla"),
    "",
    "====================",
    "",
    renderEventWhyText(snapshot, "tsla"),
  ].join("\n");
}

type BudgetRequest = {
  currency: "USD" | "KRW";
  rawAmount: number;
  totalUsd?: number;
  fxRate?: number;
  fxDate?: string;
};

function parseKrwAmount(text: string): number | null {
  const compact = text.replace(/,/g, "").replace(/\s+/g, "").toLowerCase();
  const eokMatch = compact.match(/([0-9]+(?:\.[0-9]+)?)억/);
  const manMatch = compact.match(/([0-9]+(?:\.[0-9]+)?)만/);
  if (eokMatch || manMatch) {
    const eok = eokMatch ? Number(eokMatch[1]) : 0;
    const man = manMatch ? Number(manMatch[1]) : 0;
    const total = eok * 100_000_000 + man * 10_000;
    return Number.isFinite(total) && total > 0 ? total : null;
  }
  const numeric = compact.match(/([0-9]+(?:\.[0-9]+)?)/);
  if (!numeric) {
    return null;
  }
  const value = Number(numeric[1]);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function parseUsdAmount(text: string): number | null {
  const compact = text.replace(/,/g, "").replace(/\s+/g, "").toLowerCase();
  const suffixMatch = compact.match(/([0-9]+(?:\.[0-9]+)?)(k|m)\b/);
  if (suffixMatch) {
    const base = Number(suffixMatch[1]);
    const mult = suffixMatch[2] === "m" ? 1_000_000 : 1_000;
    const total = base * mult;
    return Number.isFinite(total) && total > 0 ? total : null;
  }
  const numeric = compact.match(/([0-9]+(?:\.[0-9]+)?)/);
  if (!numeric) {
    return null;
  }
  const value = Number(numeric[1]);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function parseBudgetRequest(text: string): BudgetRequest | null {
  const cleaned = text.replace(/,/g, " ").trim();
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return null;
  }

  const first = parts[0].toLowerCase();
  const tokens = first === "/budget" ? parts.slice(1) : parts;
  if (tokens.length === 0) {
    return null;
  }

  const amount = Number(tokens[0]);
  if (!Number.isFinite(amount) || amount <= 0) {
    return null;
  }

  if (tokens.length >= 3 && tokens[1].toLowerCase() === "krw") {
    const fxRate = Number(tokens[2]);
    if (!Number.isFinite(fxRate) || fxRate <= 0) {
      return null;
    }
    return {
      currency: "KRW",
      rawAmount: amount,
      totalUsd: amount / fxRate,
      fxRate,
    };
  }

  if (tokens.length >= 2 && tokens[1].toLowerCase() === "usd") {
    return {
      currency: "USD",
      rawAmount: amount,
      totalUsd: amount,
    };
  }

  if (first === "/budget" || /^\d+(\.\d+)?$/.test(tokens[0])) {
    return {
      currency: "USD",
      rawAmount: amount,
      totalUsd: amount,
    };
  }

  return null;
}

function parseBudgetRequestForState(text: string, state: BudgetState): BudgetRequest | null {
  if (state.currency === "KRW") {
    const rawAmount = parseKrwAmount(text);
    if (rawAmount === null) {
      return null;
    }
    return {
      currency: "KRW",
      rawAmount,
    };
  }
  const rawAmount = parseUsdAmount(text);
  if (rawAmount === null) {
    return null;
  }
  return {
    currency: "USD",
    rawAmount,
    totalUsd: rawAmount,
  };
}

async function fetchUsdKrwRate(): Promise<FxQuote | null> {
  try {
    const response = await fetch(USDKRW_RATE_URL, {
      headers: {
        accept: "application/json",
      },
    });
    if (!response.ok) {
      return null;
    }
    const payload = (await response.json()) as Record<string, unknown>;
    const rate = Number(payload.rate);
    if (!Number.isFinite(rate) || rate <= 0) {
      return null;
    }
    return {
      rate,
      date: typeof payload.date === "string" ? payload.date : undefined,
      source: "api",
    };
  } catch {
    return null;
  }
}

async function normalizeBudgetRequest(budget: BudgetRequest): Promise<{ budget: BudgetRequest; fxQuote?: FxQuote } | null> {
  if (budget.currency === "USD") {
    return {
      budget: {
        ...budget,
        totalUsd: budget.totalUsd ?? budget.rawAmount,
      },
    };
  }

  if (Number.isFinite(Number(budget.fxRate)) && Number(budget.fxRate) > 0) {
    const rate = Number(budget.fxRate);
    return {
      budget: {
        ...budget,
        totalUsd: budget.rawAmount / rate,
        fxRate: rate,
      },
      fxQuote: {
        rate,
        date: budget.fxDate,
        source: "manual",
      },
    };
  }

  const fxQuote = await fetchUsdKrwRate();
  if (!fxQuote) {
    return null;
  }
  return {
    budget: {
      ...budget,
      totalUsd: budget.rawAmount / fxQuote.rate,
      fxRate: fxQuote.rate,
      fxDate: fxQuote.date,
    },
    fxQuote,
  };
}

function renderBudgetHelpText(snapshot: AppSnapshot): string {
  const signal = snapshot.rebalance.v4;
  const liveCash = Number((signal as SignalSnapshot & { liveCashPct?: number }).liveCashPct ?? NaN);
  return [
    "Buy quantity calculator",
    "",
    "Send one of these:",
    "/budget 10000",
    "/budget 10000000 krw 1370",
    "",
    "Meaning:",
    "- first form: total portfolio in USD",
    "- second form: total portfolio in KRW and USD/KRW FX rate",
    "",
    `Current target invested: ${(100 - (Number.isFinite(liveCash) ? liveCash : 0)).toFixed(2)}%`,
    `Current target cash: ${Number.isFinite(liveCash) ? liveCash.toFixed(2) : "-"}%`,
  ].join("\n");
}

function renderBudgetPlan(snapshot: AppSnapshot, budget: BudgetRequest, fxQuote?: FxQuote): string {
  const signal = snapshot.rebalance.v4;
  const positions = Array.isArray(signal.livePositions) ? signal.livePositions : [];
  const refs = getLivePriceRefs(signal);
  const priceBySymbol = new Map<string, number>();
  for (const ref of refs) {
    const symbol = String(ref.symbol ?? "").toUpperCase();
    const price = Number(ref.latestClose ?? NaN);
    if (symbol && Number.isFinite(price) && price > 0) {
      priceBySymbol.set(symbol, price);
    }
  }

  const lines = ["Buy quantity plan", ""];
  if (budget.currency === "KRW") {
    lines.push(`Portfolio: ₩${budget.rawAmount.toFixed(0)} @ ${budget.fxRate?.toFixed(2)} = ${formatUsd(Number(budget.totalUsd ?? 0))}`);
  } else {
    lines.push(`Portfolio: ${formatUsd(Number(budget.totalUsd ?? 0))}`);
  }
  if (fxQuote) {
    lines.push(
      fxQuote.source === "api"
        ? `FX: USD/KRW ${fxQuote.rate.toFixed(2)}${fxQuote.date ? ` (${fxQuote.date})` : ""} via Frankfurter`
        : `FX: USD/KRW ${fxQuote.rate.toFixed(2)} (manual)`
    );
  }

  const liveCash = Number((signal as SignalSnapshot & { liveCashPct?: number }).liveCashPct ?? NaN);
  const totalUsd = Number(budget.totalUsd ?? 0);
  const targetCashUsd = Number.isFinite(liveCash) ? totalUsd * (liveCash / 100) : 0;
  lines.push(`Target cash reserve: ${formatUsd(targetCashUsd)}`);
  lines.push("");

  let estimatedBuyUsd = 0;
  const skipped: string[] = [];
  for (const position of positions) {
    const symbol = String(position.symbol ?? "").toUpperCase();
    const weightPct = Number(position.weight_pct ?? 0);
    const price = priceBySymbol.get(symbol);
    if (!symbol || !Number.isFinite(weightPct) || weightPct <= 0 || price === undefined || price <= 0) {
      continue;
    }
    const targetUsd = totalUsd * (weightPct / 100);
    const shares = Math.floor(targetUsd / price);
    const estCost = shares * price;
    estimatedBuyUsd += estCost;
    if (shares <= 0) {
      skipped.push(`${symbol} (${formatUsd(targetUsd)} target < 1 share)`);
      continue;
    }
    lines.push(`- ${symbol}: ${shares} shares @ ${formatUsd(price)} = ${formatUsd(estCost)} (target ${weightPct.toFixed(2)}%)`);
  }

  if (skipped.length > 0) {
    lines.push("");
    lines.push("Skipped");
    for (const item of skipped) {
      lines.push(`- ${item}`);
    }
  }

  lines.push("");
  lines.push(`Estimated stock buys: ${formatUsd(estimatedBuyUsd)}`);
  lines.push(`Residual cash after rounding: ${formatUsd(totalUsd - estimatedBuyUsd)}`);
  lines.push("Uses latest close references from the latest rebalance snapshot.");
  return lines.join("\n");
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

async function sendMessageWithKeyboard(
  env: Env,
  chatId: number,
  text: string,
  keyboard: InlineKeyboard,
): Promise<TelegramResponse> {
  return telegramApi(env, "sendMessage", {
    chat_id: chatId,
    text,
    reply_markup: keyboard,
    disable_web_page_preview: true,
  });
}

async function editMessage(
  env: Env,
  chatId: number,
  messageId: number,
  text: string,
  keyboard: InlineKeyboard = MENU_KEYBOARD,
): Promise<TelegramResponse> {
  return telegramApi(env, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    reply_markup: keyboard,
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
    case "/event":
      return renderOverviewText(snapshot, "tsla");
    case "/next":
      return renderEventNextText(snapshot, "tsla");
    case "/why":
      return renderEventWhyText(snapshot, "tsla");
    case "/snapshot":
      return renderSnapshotText(snapshot);
    case "/refresh":
      return [
        "Refresh started.",
        "",
        "The bot is collecting current event data and recomputing the runtime state.",
        "Check /event again in about a minute.",
      ].join("\n");
    case "/subscribe":
      return [
        "Alerts are now ON.",
        "",
        "You will receive runtime alerts when the event state changes.",
      ].join("\n");
    case "/unsubscribe":
      return [
        "Alerts are now OFF.",
        "",
        "You can turn them back on anytime with /subscribe.",
      ].join("\n");
    case "/v4":
    case "/v2":
    case "/v14":
      return renderStrategyText(snapshot, "v4");
    case "/budget":
      return renderBudgetHelpText(snapshot);
    case "/rebalance":
      return renderRebalanceText(snapshot);
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
    case "overview:tsla":
    case "event_runtime:tsla":
      return renderOverviewText(snapshot, "tsla");
    case "event_next:tsla":
      return renderEventNextText(snapshot, "tsla");
    case "event_why:tsla":
      return renderEventWhyText(snapshot, "tsla");
    case "refresh":
      return [
        renderMenuText(snapshot),
        "",
        "Refresh started.",
        "Check /event again in about a minute.",
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
    case "strategy:v4":
    case "strategy:v2":
    case "strategy:v14":
      return renderStrategyText(snapshot, "v4");
    case "rebalance":
      return renderRebalanceText(snapshot);
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
  if (!hasUsableRebalanceSnapshot(payload)) {
    return jsonResponse({ ok: false, error: "empty_rebalance_snapshot" }, { status: 400 });
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

async function broadcastMarketSessionAlert(env: Env): Promise<Response> {
  const session = currentMarketSession();
  if (!session) {
    return jsonResponse({ ok: true, sent: false, reason: "outside_window" });
  }

  const dedupeKey = marketSessionStateKey(session.kind, session.dateKey);
  const alreadySent = await env.SUBSCRIBERS.get(dedupeKey);
  if (alreadySent) {
    return jsonResponse({ ok: true, sent: false, reason: "already_sent", kind: session.kind, dateKey: session.dateKey });
  }

  const subscribers = await loadSubscribers(env);
  const snapshot = await loadSnapshot(env);
  const text = renderMarketSessionAlertText(snapshot, session.kind);
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

  await env.SUBSCRIBERS.put(dedupeKey, JSON.stringify({ sentAt: new Date().toISOString() }));
  return jsonResponse({
    ok: true,
    sent: true,
    kind: session.kind,
    dateKey: session.dateKey,
    delivered,
    removed,
    subscribers: subscribers.length,
  });
}

async function handleTelegramUpdate(update: TelegramUpdate, env: Env): Promise<Response> {
  const snapshot = await loadSnapshot(env);

  if (update.message?.chat?.id && typeof update.message.text === "string") {
    const pendingBudget = await loadBudgetState(env, update.message.chat.id);
    if (pendingBudget) {
      const budgetFromState = parseBudgetRequestForState(update.message.text, pendingBudget);
      if (budgetFromState) {
        await clearBudgetState(env, update.message.chat.id);
        const normalized = await normalizeBudgetRequest(budgetFromState);
        if (!normalized) {
          await sendMessageWithKeyboard(
            env,
            update.message.chat.id,
            "Live USD/KRW fetch failed. Please try again shortly.",
            BUDGET_MODE_KEYBOARD,
          );
          return jsonResponse({ ok: false, mode: "budget_fx_failed" }, { status: 502 });
        }
        const text = renderBudgetPlan(snapshot, normalized.budget, normalized.fxQuote);
        await sendMessage(env, update.message.chat.id, text);
        return jsonResponse({ ok: true, mode: "budget_state" });
      }
      await sendMessageWithKeyboard(
        env,
        update.message.chat.id,
        `Could not parse a ${pendingBudget.currency} amount. Please send the amount again.`,
        BUDGET_MODE_KEYBOARD,
      );
      return jsonResponse({ ok: false, mode: "budget_state_parse_failed" }, { status: 400 });
    }

    const budget = parseBudgetRequest(update.message.text);
    if (budget) {
      const normalized = await normalizeBudgetRequest(budget);
      if (!normalized) {
        await sendMessageWithKeyboard(
          env,
          update.message.chat.id,
          "Live USD/KRW fetch failed. Please try again shortly, or include a manual FX rate.",
          BUDGET_MODE_KEYBOARD,
        );
        return jsonResponse({ ok: false, mode: "budget_fx_failed" }, { status: 502 });
      }
      const text = renderBudgetPlan(snapshot, normalized.budget, normalized.fxQuote);
      await sendMessage(env, update.message.chat.id, text);
      return jsonResponse({ ok: true, mode: "budget" });
    }

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
    const data = update.callback_query.data;
    if (data === "budget_help") {
      await clearBudgetState(env, chatId);
      await editMessage(
        env,
        chatId,
        update.callback_query.message.message_id,
        renderBudgetModeText(snapshot),
        BUDGET_MODE_KEYBOARD,
      );
      await answerCallback(env, update.callback_query.id, "Choose KRW or USD");
      return jsonResponse({ ok: true, mode: "budget_menu" });
    }
    if (data === "budget_mode:krw" || data === "budget_mode:usd") {
      const currency = data.endsWith(":krw") ? "KRW" : "USD";
      await saveBudgetState(env, chatId, { currency });
      await editMessage(
        env,
        chatId,
        update.callback_query.message.message_id,
        renderBudgetInputPrompt(currency),
        BUDGET_MODE_KEYBOARD,
      );
      await answerCallback(env, update.callback_query.id, `${currency} input enabled`);
      return jsonResponse({ ok: true, mode: "budget_mode", currency });
    }
    if (data?.startsWith("budget_preset:")) {
      const [, currencyRaw, amountRaw] = data.split(":");
      const amount = Number(amountRaw);
      const currency = currencyRaw?.toLowerCase() === "krw" ? "KRW" : "USD";
      const budget = currency === "KRW"
        ? { currency, rawAmount: amount }
        : { currency, rawAmount: amount, totalUsd: amount };
      const normalized = await normalizeBudgetRequest(budget);
      if (!normalized) {
        await editMessage(
          env,
          chatId,
          update.callback_query.message.message_id,
          "Live USD/KRW fetch failed. Please try again shortly.",
          BUDGET_MODE_KEYBOARD,
        );
        await answerCallback(env, update.callback_query.id, "FX fetch failed");
        return jsonResponse({ ok: false, mode: "budget_preset_fx_failed" }, { status: 502 });
      }
      await clearBudgetState(env, chatId);
      const text = renderBudgetPlan(snapshot, normalized.budget, normalized.fxQuote);
      await editMessage(env, chatId, update.callback_query.message.message_id, text);
      await answerCallback(env, update.callback_query.id, "Calculated");
      return jsonResponse({ ok: true, mode: "budget_preset", currency });
    }
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
    } else if (update.callback_query.data === "menu") {
      await clearBudgetState(env, chatId);
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
        commands: ["/menu", "/event", "/next", "/why", "/refresh", "/snapshot", "/subscribe", "/unsubscribe"],
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

    if (request.method === "POST" && url.pathname === "/api/market-session-notify") {
      if (!verifyNotifyRequest(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, { status: 401 });
      }
      return broadcastMarketSessionAlert(env);
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
