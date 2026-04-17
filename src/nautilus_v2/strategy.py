from dataclasses import dataclass
from decimal import Decimal
from typing import Any


NAUTILUS_AVAILABLE = False

try:  # pragma: no cover - real Nautilus path exercised in venv smoke
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.core.data import Data
    from nautilus_trader.model.custom import customdataclass
    from nautilus_trader.model.data import Bar
    from nautilus_trader.model.data import BarType
    from nautilus_trader.model.data import DataType
    from nautilus_trader.model.identifiers import ClientId
    from nautilus_trader.model.enums import OrderSide
    from nautilus_trader.model.enums import TimeInForce
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.model.instruments import Instrument
    from nautilus_trader.trading.strategy import Strategy

    NAUTILUS_AVAILABLE = True
except Exception:  # pragma: no cover - fallback path used in main repo env
    class Data:  # type: ignore[override]
        @classmethod
        def fully_qualified_name(cls) -> str:
            return f"{cls.__module__}:{cls.__name__}"

    def customdataclass(cls=None, **kwargs):  # type: ignore[override]
        def _wrap(target):
            return dataclass(target)

        return _wrap if cls is None else _wrap(cls)

    class DataType:  # type: ignore[override]
        def __init__(self, type_: type, metadata: dict[str, Any] | None = None) -> None:
            self.type = type_
            self.metadata = metadata or {}

    class InstrumentId(str):  # type: ignore[override]
        @classmethod
        def from_str(cls, value: str) -> "InstrumentId":
            return cls(value)

    class ClientId(str):  # type: ignore[override]
        pass

    class StrategyConfig:  # type: ignore[override]
        def __init_subclass__(cls, **kwargs):
            return None

    class BarType(str):  # type: ignore[override]
        @classmethod
        def from_str(cls, value: str) -> "BarType":
            return cls(value)

    class Bar:  # type: ignore[override]
        instrument_id: InstrumentId
        volume: Any

    class Instrument:  # type: ignore[override]
        id: InstrumentId

        def make_qty(self, value: Decimal):
            return value

    class Strategy:  # type: ignore[override]
        def __init__(self, config: Any | None = None) -> None:
            self.config = config

    class OrderSide:  # type: ignore[override]
        BUY = "BUY"

    class TimeInForce:  # type: ignore[override]
        IOC = "IOC"


def _as_double(value: Any) -> float:
    try:
        return float(value.as_double())
    except Exception:
        try:
            return float(value)
        except Exception:
            return 0.0


@customdataclass
class TslaNewsEvent(Data):
    instrument_id: InstrumentId = InstrumentId.from_str("TSLA.XNAS")
    headline: str = ""
    category: str = ""
    sentiment: str = "neutral"
    source: str = ""
    magnitude: float = 0.0


@customdataclass
class TslaMacroEvent(Data):
    instrument_id: InstrumentId = InstrumentId.from_str("TSLA.XNAS")
    macro_mode: str = "neutral"
    macro_reason: str = ""
    position_scale: float = 1.0
    allow_new_longs: bool = True
    fear_greed_score: int = 50


class TslaEventStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("10")
    custom_data_client_id: str = "CUSTOM"
    news_buy_threshold: float = 0.85
    news_sell_threshold: float = -0.85
    min_volume_ratio_for_entry: float = 1.5
    allow_entries_in_risk_off: bool = False
    signal_name: str = "tsla_event_action"


class TslaEventStrategy(Strategy):
    """
    TSLA-only event strategy for the separate Nautilus lane.

    Logic:
    - bullish news event + sufficient bar-volume confirmation + macro allows longs -> buy if flat
    - bearish news event or macro lockout -> close existing long
    - every state transition also emits a signal for inspection
    """

    def __init__(self, config: TslaEventStrategyConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        self._latest_news_score = 0.0
        self._latest_macro_mode = "neutral"
        self._allow_new_longs = True
        self._last_action = "HOLD"
        self._volume_window: list[float] = []
        self._latest_volume_ratio = 1.0

    def on_start(self) -> None:  # pragma: no cover - exercised in Nautilus runtime smoke
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        self.subscribe_bars(self.config.bar_type)
        client_id = ClientId(self.config.custom_data_client_id)
        self.subscribe_data(DataType(TslaNewsEvent), client_id=client_id)
        self.subscribe_data(DataType(TslaMacroEvent), client_id=client_id)

    def on_data(self, data: Data) -> None:  # pragma: no cover - exercised in Nautilus runtime smoke
        if isinstance(data, TslaNewsEvent):
            self._consume_news(data)
        elif isinstance(data, TslaMacroEvent):
            self._consume_macro(data)
        self._act(ts_event=getattr(data, "ts_event", 0))

    def on_bar(self, bar: Bar) -> None:  # pragma: no cover - exercised in Nautilus runtime smoke
        if getattr(bar, "bar_type", None) != self.config.bar_type:
            return
        volume = _as_double(getattr(bar, "volume", 0.0))
        trailing_avg = sum(self._volume_window[-20:]) / len(self._volume_window[-20:]) if self._volume_window[-20:] else volume or 1.0
        self._latest_volume_ratio = volume / max(1.0, trailing_avg)
        self._volume_window.append(volume)
        self._act(ts_event=getattr(bar, "ts_event", 0))

    def _consume_news(self, event: TslaNewsEvent) -> None:
        sign = 0.0
        sentiment = str(event.sentiment or "").lower()
        if sentiment == "bullish":
            sign = 1.0
        elif sentiment == "bearish":
            sign = -1.0
        self._latest_news_score = sign * float(getattr(event, "magnitude", 0.0))

    def _consume_macro(self, event: TslaMacroEvent) -> None:
        self._latest_macro_mode = str(getattr(event, "macro_mode", "neutral") or "neutral")
        self._allow_new_longs = bool(getattr(event, "allow_new_longs", True))

    def _decide_action(self) -> str:
        if self._latest_news_score <= float(self.config.news_sell_threshold):
            return "SELL"
        if self._latest_news_score >= float(self.config.news_buy_threshold):
            if self._latest_macro_mode == "risk_off" and not bool(self.config.allow_entries_in_risk_off):
                return "WATCH"
            if not self._allow_new_longs:
                return "WATCH"
            if self._latest_volume_ratio < float(self.config.min_volume_ratio_for_entry):
                return "WATCH"
            return "BUY"
        if self._latest_macro_mode == "risk_off" and not self._allow_new_longs:
            return "SELL"
        return "HOLD"

    def _act(self, ts_event: int) -> None:
        action = self._decide_action()
        if action != self._last_action:
            self.publish_signal(self.config.signal_name, action, ts_event=ts_event)
            self._last_action = action

        if self.instrument is None:
            return
        if action == "BUY" and self.portfolio.is_flat(self.config.instrument_id):
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self.instrument.make_qty(self.config.trade_size),
                time_in_force=TimeInForce.IOC,
            )
            self.submit_order(order)
        elif action == "SELL" and self.portfolio.is_net_long(self.config.instrument_id):
            self.close_all_positions(self.config.instrument_id)


__all__ = [
    "NAUTILUS_AVAILABLE",
    "TslaEventStrategyConfig",
    "TslaEventStrategy",
    "TslaNewsEvent",
    "TslaMacroEvent",
]
