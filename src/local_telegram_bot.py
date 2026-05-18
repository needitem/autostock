from __future__ import annotations

import json
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import RLock
from typing import Any

import requests

from local_telegram_journal import evaluate_shadow_journal, record_recommendation_run, render_journal_html
from local_telegram_trade import (
    analyze_rebalance_universe,
    full_news_analysis_limit,
    render_trade_view_html,
)


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "outputs" / "telegram" / "bot_state.json"
MENU_BOOTSTRAP_VERSION = "2026-05-15-no-chart-route-v1"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _env_bool(key: str, default: bool) -> bool:
    raw = _s(os.getenv(key, "1" if default else "0")).lower()
    return raw in {"1", "true", "yes", "on", "y"}


class LocalTelegramBot:
    def __init__(self) -> None:
        token = _s(os.getenv("TELEGRAM_BOT_TOKEN"))
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session = requests.Session()
        self.api_lock = RLock()
        self.state_lock = RLock()
        self.analysis_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="telegram-analysis")
        self.analysis_future: Future[Any] | None = None
        self.state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        if not STATE_PATH.exists():
            return {"offset": None}
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {"offset": None}

    def _save_state(self) -> None:
        with self.state_lock:
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            STATE_PATH.write_text(json.dumps(self.state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _track_chat(self, chat_id: int) -> None:
        with self.state_lock:
            self.state["last_chat_id"] = int(chat_id)
            self._save_state()

    def _api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self.api_lock:
            response = self.session.post(
                f"{self.base_url}/{method}",
                json=payload,
                timeout=60,
            )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            try:
                payload_json = response.json()
            except Exception:
                raise
            description = _s(payload_json.get("description")).lower()
            if response.status_code == 400 and "message is not modified" in description:
                return {"ok": True, "result": None}
            raise
        return response.json()

    def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str = "HTML",
    ) -> None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": parse_mode,
        }
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        self._api("sendMessage", payload)

    def edit_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str = "HTML",
    ) -> None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": parse_mode,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        self._api("editMessageText", payload)

    def answer_callback(self, callback_query_id: str, text: str = "") -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._api("answerCallbackQuery", payload)

    def get_updates(self) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": 30,
            "allowed_updates": ["message", "callback_query"],
        }
        if self.state.get("offset") is not None:
            payload["offset"] = int(self.state["offset"])
        response = self._api("getUpdates", payload)
        rows = response.get("result")
        return rows if isinstance(rows, list) else []

    def _main_reply_keyboard(self) -> dict[str, Any]:
        return {
            "keyboard": [
                ["뉴스+차트 분석", "전체 정밀 분석"],
                ["기록 평가", "새로고침"],
                ["도움말"],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
            "one_time_keyboard": False,
        }

    def _help_text(self) -> str:
        return "\n".join(
            [
                "<b>Autostock Telegram</b>",
                "",
                "하단 고정 키보드로 바로 분석할 수 있습니다.",
                "",
                "<b>명령</b>",
                "/trade  빠른 요약",
                "/tradefull  all_us 전체 뉴스/Codex 풀분석",
                "/journal  추천 체결/성과 기록 평가",
                "/refresh  캐시 무시하고 다시 계산",
                "/menu  메인 메뉴",
                "",
                "자유 질문 예시",
                "지금 들어갈만한 종목 알려줘",
            ]
        )

    def _menu_text(self) -> str:
        return "\n".join(
            [
                "<b>Autostock Trade Menu</b>",
                "",
                "뉴스+차트 분석: 후보를 차트+뉴스/Codex 기준으로 빠르게 분석",
                "전체 정밀 분석: all_us 전체 뉴스/Codex 풀분석",
                "기록 평가: 추천 이후 실제 체결 가능성/TP1/손절 추적",
                "",
                "아래 하단 고정 키보드나 Telegram 메뉴 명령을 눌러 시작하세요.",
            ]
        )

    def _is_force_refresh(self, text: str) -> bool:
        lowered = text.lower()
        return "/refresh" in lowered or "새로고침" in text or "다시" in text or "refresh" in lowered or "최신으로" in text

    def _is_full_request(self, text: str) -> bool:
        lowered = text.lower()
        compact = "".join(text.split())
        return (
            "/tradefull" in lowered
            or "전체정밀" in compact
            or "정밀분석" in compact
            or "전체분석" in compact
            or "전체종목" in compact
            or "all_us" in lowered
            or "전종목" in text
        )

    def _is_news_chart_request(self, text: str) -> bool:
        lowered = text.lower()
        compact = "".join(text.split())
        is_quick_trade_command = "/trade" in lowered and "/tradefull" not in lowered
        return (
            is_quick_trade_command
            or "뉴스+차트" in compact
            or "뉴스차트" in compact
            or "차트+뉴스" in compact
            or "차트뉴스" in compact
            or "빠른 분석" in text
        )

    def _is_removed_chart_request(self, text: str) -> bool:
        compact = "".join(text.split())
        return not self._is_news_chart_request(text) and ("차트만" in compact or ("차트" in text and "분석" in text))

    def _is_journal_request(self, text: str) -> bool:
        lowered = text.lower()
        return "/journal" in lowered or "기록 평가" in text or "성과 평가" in text or "체결 평가" in text

    def _is_help_request(self, text: str) -> bool:
        lowered = text.lower()
        return text.startswith("/help") or "도움말" in text or lowered == "help"

    def _should_run_trade_analysis(self, text: str) -> bool:
        if (
            self._is_force_refresh(text)
            or self._is_full_request(text)
            or self._is_news_chart_request(text)
        ):
            return True
        lowered = text.lower()
        trigger_words = [
            "/journal",
            "/trade",
            "/tradefull",
            "/refresh",
            "들어갈만",
            "진입",
            "매수",
            "포트폴리오",
            "뉴스",
            "종목",
            "분석",
            "매도가",
            "손절",
        ]
        return any(word in lowered if word.startswith("/") else word in text for word in trigger_words)

    def _analysis_mode(self, text: str) -> str:
        if self._is_full_request(text):
            return "f"
        if self._is_news_chart_request(text):
            return "q"
        if "새로고침" in text:
            return "f" if _s(self.state.get("last_mode")) == "f" else "q"
        return "q"

    def _run_analysis_payload(self, mode: str, force_refresh: bool) -> dict[str, Any]:
        return analyze_rebalance_universe(
            force_refresh=force_refresh,
            news_limit=full_news_analysis_limit() if mode == "f" else None,
        )

    def _render_payload(self, mode: str, payload: dict[str, Any], view: str = "summary") -> str:
        return render_trade_view_html(payload, view=view)

    def _analysis_busy(self) -> bool:
        return self.analysis_future is not None and not self.analysis_future.done()

    def _mark_last_mode(self, mode: str) -> None:
        with self.state_lock:
            self.state["last_mode"] = mode
            self._save_state()

    def _busy_text(self) -> str:
        return "\n".join(
            [
                "<b>분석 작업이 이미 실행 중입니다.</b>",
                "",
                "전체 뉴스/차트 분석은 오래 걸릴 수 있어서 동시에 1개만 돌립니다.",
                "완료되면 이 채팅방에 결과가 자동으로 올라옵니다.",
            ]
        )

    def _submit_analysis(
        self,
        chat_id: int,
        message_id: int,
        mode: str,
        force_refresh: bool,
        *,
        trigger: str,
        edit_message_id: int | None = None,
    ) -> None:
        if self._analysis_busy():
            if edit_message_id is not None:
                self.edit_message(chat_id, edit_message_id, self._busy_text())
            else:
                self.send_message(
                    chat_id,
                    self._busy_text(),
                    reply_to_message_id=message_id,
                    reply_markup=self._main_reply_keyboard(),
                )
            return

        def _job() -> None:
            job_started = time.perf_counter()
            print(f"telegram analysis job started: mode={mode} force_refresh={force_refresh} trigger={trigger}")
            try:
                payload = self._run_analysis_payload(mode, force_refresh)
                self._record_payload(mode, payload, trigger=trigger)
                self._mark_last_mode(mode)
                rendered = self._render_payload(mode, payload, view="summary")
                if edit_message_id is not None:
                    self.edit_message(chat_id, edit_message_id, rendered)
                else:
                    self.send_message(
                        chat_id,
                        rendered,
                        reply_to_message_id=message_id,
                        reply_markup=self._main_reply_keyboard(),
                    )
                print(
                    "telegram analysis job finished: "
                    f"mode={mode} available={bool(payload.get('available'))} "
                    f"elapsed={time.perf_counter() - job_started:.2f}s"
                )
            except Exception as exc:
                error_text = f"분석 실패: {type(exc).__name__}: {exc}"
                print(f"telegram analysis job error: {error_text}")
                try:
                    if edit_message_id is not None:
                        self.edit_message(chat_id, edit_message_id, error_text)
                    else:
                        self.send_message(
                            chat_id,
                            error_text,
                            reply_to_message_id=message_id,
                            reply_markup=self._main_reply_keyboard(),
                        )
                except Exception as send_exc:
                    print(f"telegram analysis error send failed: {type(send_exc).__name__}: {send_exc}")

        print(f"telegram analysis queued: mode={mode} force_refresh={force_refresh} trigger={trigger}")
        self.analysis_future = self.analysis_executor.submit(_job)

    def _record_payload(self, mode: str, payload: dict[str, Any], trigger: str) -> None:
        try:
            record_recommendation_run(mode, payload, trigger=trigger)
        except Exception as exc:
            print(f"shadow journal record error: {type(exc).__name__}: {exc}")

    def _reply_with_journal(self, chat_id: int, message_id: int | None = None) -> None:
        payload = evaluate_shadow_journal()
        self.send_message(
            chat_id,
            render_journal_html(payload),
            reply_to_message_id=message_id,
            reply_markup=self._main_reply_keyboard(),
        )

    def _wait_text(self, mode: str, force_refresh: bool) -> str:
        prefix = "다시 계산 중입니다." if force_refresh else "분석 중입니다."
        suffix = (
            "all_us 전체를 차트 스캔 후 뉴스/Codex까지 풀분석 중입니다. 시간이 조금 걸릴 수 있습니다."
            if mode == "f"
            else "전체 후보를 차트+뉴스 기준으로 다시 분석 중입니다. 잠시만 기다려주세요."
        )
        return f"{prefix}\n\n{suffix}"

    def _reply_with_analysis(self, chat_id: int, message_id: int, text: str) -> None:
        mode = self._analysis_mode(text)
        force_refresh = self._is_force_refresh(text)
        if self._analysis_busy():
            self._submit_analysis(chat_id, message_id, mode, force_refresh, trigger="message")
            return
        self.send_message(
            chat_id,
            self._wait_text(mode, force_refresh),
            reply_to_message_id=message_id,
            reply_markup=self._main_reply_keyboard(),
        )
        self._submit_analysis(chat_id, message_id, mode, force_refresh, trigger="message")

    def _configure_bot_ui(self) -> None:
        commands = [
            {"command": "menu", "description": "메인 메뉴"},
            {"command": "trade", "description": "차트+뉴스 빠른 분석"},
            {"command": "tradefull", "description": "전체 뉴스/Codex 정밀 분석"},
            {"command": "journal", "description": "추천 체결/성과 평가"},
            {"command": "help", "description": "도움말"},
        ]
        try:
            self._api("deleteWebhook", {"drop_pending_updates": False})
            self._api("setMyCommands", {"commands": commands})
            self._api("setChatMenuButton", {"menu_button": {"type": "commands"}})
            print("Telegram commands/menu configured.")
        except Exception as exc:
            print(f"telegram menu setup error: {type(exc).__name__}: {exc}")

    def _send_startup_menu_if_possible(self) -> None:
        if not _env_bool("TELEGRAM_STARTUP_MENU_PUSH_ENABLED", True):
            return
        chat_id = self.state.get("last_chat_id")
        if not chat_id or self.state.get("last_menu_bootstrap_version") == MENU_BOOTSTRAP_VERSION:
            return
        try:
            self.send_message(int(chat_id), self._menu_text(), reply_markup=self._main_reply_keyboard())
            self.state["last_menu_bootstrap_version"] = MENU_BOOTSTRAP_VERSION
            self._save_state()
            print(f"startup menu pushed: chat_id={chat_id}")
        except Exception as exc:
            print(f"startup menu push error: {type(exc).__name__}: {exc}")

    def _handle_callback(self, callback_query: dict[str, Any]) -> None:
        callback_id = _s(callback_query.get("id"))
        message = callback_query.get("message") if isinstance(callback_query.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        if not callback_id or not chat_id:
            return
        self.answer_callback(callback_id, "하단 메뉴를 사용해주세요.")
        self._track_chat(int(chat_id))
        self.send_message(
            int(chat_id),
            "이전 메시지 버튼은 더 이상 사용하지 않습니다. 하단 고정 키보드로 실행해주세요.",
            reply_markup=self._main_reply_keyboard(),
        )

    def handle_update(self, update: dict[str, Any]) -> None:
        callback_query = update.get("callback_query")
        if isinstance(callback_query, dict):
            self._handle_callback(callback_query)
            return

        message = update.get("message") if isinstance(update.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        text = _s(message.get("text"))
        message_id = int(message.get("message_id") or 0)
        if not chat_id or not text:
            return
        self._track_chat(int(chat_id))

        if text.startswith("/start") or text.startswith("/menu"):
            self.send_message(int(chat_id), self._menu_text(), reply_to_message_id=message_id, reply_markup=self._main_reply_keyboard())
            return

        if self._is_help_request(text):
            self.send_message(int(chat_id), self._help_text(), reply_to_message_id=message_id, reply_markup=self._main_reply_keyboard())
            return

        if self._is_journal_request(text):
            self._reply_with_journal(int(chat_id), message_id)
            return

        if self._is_removed_chart_request(text):
            self.send_message(
                int(chat_id),
                "차트 단독 분석 경로는 제거했습니다. `뉴스+차트 분석` 또는 /trade를 사용해주세요.",
                reply_to_message_id=message_id,
                reply_markup=self._main_reply_keyboard(),
            )
            return

        if self._should_run_trade_analysis(text):
            self._reply_with_analysis(int(chat_id), message_id, text)
            return

        self.send_message(
            int(chat_id),
            "하단 키보드나 Telegram 메뉴 명령을 누르면 바로 분석합니다.",
            reply_to_message_id=message_id,
            reply_markup=self._main_reply_keyboard(),
        )

    def run(self) -> None:
        self._configure_bot_ui()
        self._send_startup_menu_if_possible()
        print("Local Telegram bot polling started.")
        while True:
            try:
                updates = self.get_updates()
                for update in updates:
                    update_id = int(update.get("update_id") or 0)
                    if update_id:
                        self.state["offset"] = update_id + 1
                        self._save_state()
                    self.handle_update(update)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"telegram bot error: {type(exc).__name__}: {exc}")
                time.sleep(5)


def run_local_telegram_bot() -> None:
    LocalTelegramBot().run()


__all__ = ["run_local_telegram_bot"]
