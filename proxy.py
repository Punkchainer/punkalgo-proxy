#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
proxy.py — Puente Webhook -> BITUNIX (Futuros USDT-M)

Escucha señales de trading por webhook (formato de alerta tipo Telegram/TradingView),
abre la posición en Bitunix tal como la pide la señal (lado, precio de entrada, SL y
todos los objetivos), fuerza margen AISLADO, apalancamiento configurable (default x20)
y usa un % configurable del equity de la cuenta (default 5%).

Cuando el precio llega a Objetivo 1 (TP1) mueve el Stop Loss a break-even
(precio de entrada +/- un offset para cubrir comisiones).

Señal que entiende:

    #CHZ/USDT - Short 🔴

    Entrada: 0.00883
    Stop Loss: 0.0090066

    🎯 Objetivo 1: 0.00873336
    🎯 Objetivo 2: 0.00871886
    🎯 Objetivo 3: 0.00870219
    🎯 Objetivo 4: 0.00868302
    🎯 Objetivo 5: 0.00866097

    Apalancamiento: x20

Uso:
    python3 proxy.py serve              # levanta el servidor de webhooks
    python3 proxy.py selftest           # valida parsing/reparto sin tocar la API
    python3 proxy.py parse señal.txt    # parsea una señal desde archivo o stdin
    python3 proxy.py status             # pide /status al servidor local
    python3 proxy.py config             # muestra la configuración activa

Configuración: variables de entorno o archivo .env junto al script (ver .env.example).
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

# --------------------------------------------------------------------------------------
# Utilidades de entorno / configuración
# --------------------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = Path(os.environ.get("PROXY_ENV_FILE", SCRIPT_DIR / ".env"))


def load_dotenv(path: Path) -> None:
    """Carga un .env sencillo (KEY=VALUE) sin pisar variables ya definidas."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().replace("export ", "")
        os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def s(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()


def f(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def i(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def b(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on", "si", "sí"}


load_dotenv(ENV_FILE)


@dataclass
class Config:
    """Configuración del bot. Todo se puede sobreescribir con variables de entorno."""

    # --- API Bitunix -------------------------------------------------------------------
    api_key: str = ""
    api_secret: str = ""
    base_url: str = "https://fapi.bitunix.com"
    margin_coin: str = "USDT"

    # --- Servidor webhook --------------------------------------------------------------
    host: str = "0.0.0.0"
    port: int = 8080
    webhook_path: str = "/webhook"
    webhook_token: str = ""              # header X-Webhook-Token, ?token=, Bearer o /webhook/<token>
    webhook_keyword: str = ""            # frase opcional que debe aparecer en el mensaje
    max_body_kb: int = 64

    # --- Riesgo / tamaño ---------------------------------------------------------------
    position_size_pct: float = 10.0      # % del equity por operación (margin o notional según sizing_mode)
    sizing_mode: str = "margin"          # margin | notional
    max_total_margin_pct: float = 50.0   # tope de margen sumado entre todas las operaciones activas
    default_leverage: int = 20           # apalancamiento por defecto (x20)
    use_signal_leverage: bool = True     # usa "Apalancamiento: xNN" de la señal
    max_leverage: int = 20               # tope de seguridad
    margin_mode: str = "ISOLATION"       # aislado (requisito, no configurable)
    max_open_positions: int = 5
    allow_reverse: bool = False          # permitir abrir en contra de una posición abierta
    max_price_deviation_pct: float = 5.0  # rechazo si la entrada está muy lejos del mercado (0 = off)

    # --- Orden de entrada --------------------------------------------------------------
    entry_order_type: str = "LIMIT"      # LIMIT | MARKET
    entry_time_in_force: str = "GTC"     # GTC | IOC | FOK | POST_ONLY
    limit_order_ttl_sec: int = 900       # cancela la limit si no llena (0 = no cancelar)

    # --- Take profits / Stop loss ------------------------------------------------------
    tp_mode: str = "partials"            # partials | single
    single_tp_target: str = "first"      # first | last | 1..N (solo tp_mode=single)
    tp_split: str = "equal"              # equal | "20,20,20,20,20"
    tp_stop_type: str = "LAST_PRICE"     # LAST_PRICE | MARK_PRICE
    sl_stop_type: str = "LAST_PRICE"
    tp_order_type: str = "MARKET"        # MARKET | LIMIT
    sl_order_type: str = "MARKET"

    # --- Break-even --------------------------------------------------------------------
    be_enabled: bool = True
    be_trigger: str = "both"             # fill | price | both
    be_after_tp: int = 1                 # BE al alcanzar el objetivo N (TP1)
    be_offset_pct: float = 0.1           # offset sobre la entrada para cubrir fees
    be_cancel_remaining_tp: bool = False  # cancelar los TP restantes al hacer BE

    # --- Operativa ---------------------------------------------------------------------
    poll_interval_sec: float = 5.0
    fast_arm_sec: float = 12.0          # sg esperando el fill dentro del webhook
    dedupe_window_sec: int = 180
    request_timeout_sec: float = 15.0
    retries: int = 3
    state_file: str = "proxy_state.json"
    log_file: str = "proxy.log"
    log_level: str = "INFO"
    dry_run: bool = False
    dry_run_equity: float = 1000.0       # equity simulado para pruebas en dry-run
    dry_run_sim: bool = True             # en dry-run, simula fill/TP/BE/cierre sin llamar al exchange
    dry_run_phase_sec: float = 18.0      # segundos de cada fase simulada (entrada, TP1, TP2…)


CFG = Config(
    api_key=s("BITUNIX_API_KEY", ""),
    api_secret=s("BITUNIX_API_SECRET", ""),
    base_url=s("BITUNIX_BASE_URL", "https://fapi.bitunix.com").rstrip("/"),
    margin_coin=s("MARGIN_COIN", "USDT"),
    host=s("PROXY_HOST", "0.0.0.0"),
    port=i("PROXY_PORT", 8080),
    webhook_path=s("WEBHOOK_PATH", "/webhook"),
    webhook_token=s("WEBHOOK_TOKEN", ""),
    webhook_keyword=s("WEBHOOK_KEYWORD", ""),
    max_body_kb=i("MAX_BODY_KB", 64),
    position_size_pct=f("POSITION_SIZE_PCT", 10.0),
    sizing_mode=s("SIZING_MODE", "margin").lower(),
    max_total_margin_pct=f("MAX_TOTAL_MARGIN_PCT", 50.0),
    default_leverage=i("DEFAULT_LEVERAGE", 20),
    use_signal_leverage=b("USE_SIGNAL_LEVERAGE", True),
    max_leverage=i("MAX_LEVERAGE", 20),
    margin_mode="ISOLATION",
    max_open_positions=i("MAX_OPEN_POSITIONS", 5),
    allow_reverse=b("ALLOW_REVERSE", False),
    max_price_deviation_pct=f("MAX_PRICE_DEVIATION_PCT", 5.0),
    entry_order_type=s("ENTRY_ORDER_TYPE", "LIMIT").upper(),
    entry_time_in_force=s("ENTRY_TIME_IN_FORCE", "GTC").upper(),
    limit_order_ttl_sec=i("LIMIT_ORDER_TTL_SEC", 900),
    tp_mode=s("TP_MODE", "partials").lower(),
    single_tp_target=s("SINGLE_TP_TARGET", "first").lower(),
    tp_split=s("TP_SPLIT", "equal"),
    tp_stop_type=s("TP_STOP_TYPE", "LAST_PRICE").upper(),
    sl_stop_type=s("SL_STOP_TYPE", "LAST_PRICE").upper(),
    tp_order_type=s("TP_ORDER_TYPE", "MARKET").upper(),
    sl_order_type=s("SL_ORDER_TYPE", "MARKET").upper(),
    be_enabled=b("BE_ENABLED", True),
    be_trigger=s("BE_TRIGGER", "both").lower(),
    be_after_tp=i("BE_AFTER_TP", 1),
    be_offset_pct=f("BE_OFFSET_PCT", 0.1),
    be_cancel_remaining_tp=b("BE_CANCEL_REMAINING_TP", False),
    poll_interval_sec=f("POLL_INTERVAL_SEC", 5.0),
    fast_arm_sec=f("FAST_ARM_SEC", 12.0),
    dedupe_window_sec=i("DEDUPE_WINDOW_SEC", 180),
    request_timeout_sec=f("REQUEST_TIMEOUT_SEC", 15.0),
    retries=i("RETRIES", 3),
    state_file=s("STATE_FILE", "proxy_state.json"),
    log_file=s("LOG_FILE", "proxy.log"),
    log_level=s("LOG_LEVEL", "INFO").upper(),
    dry_run=b("DRY_RUN", False),
    dry_run_equity=f("DRY_RUN_EQUITY", 1000.0),
    dry_run_sim=b("DRY_RUN_SIM", True),
    dry_run_phase_sec=f("DRY_RUN_PHASE_SEC", 18.0),
)

if CFG.sizing_mode not in ("margin", "notional"):
    CFG.sizing_mode = "margin"
if CFG.tp_mode not in ("partials", "single"):
    CFG.tp_mode = "partials"
if CFG.be_trigger not in ("fill", "price", "both"):
    CFG.be_trigger = "both"

# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
LOG_PATH = Path(CFG.log_file)
if not LOG_PATH.is_absolute():
    LOG_PATH = SCRIPT_DIR / LOG_PATH
_LOG_LOCK = threading.Lock()

# Anillo de eventos en memoria: cola de logs que sirve GET /events.
EVENTS: deque[dict] = deque(maxlen=i("EVENT_BUFFER", 400))
_EVENT_IDS = count(1)


def log(level: str, msg: str) -> None:
    if LOG_LEVELS.get(level, 20) < LOG_LEVELS.get(CFG.log_level, 20):
        return
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{stamp} {level:<7} {msg}"
    with _LOG_LOCK:
        EVENTS.append({"id": next(_EVENT_IDS), "ts": time.time(), "utc": stamp,
                       "level": level, "msg": msg})
        print(line, flush=True)
        try:
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


# --------------------------------------------------------------------------------------
# Números / formato de precios y cantidades
# --------------------------------------------------------------------------------------


def parse_number(raw: str) -> float:
    """'0.00883', '1,234.56' o '0,00883' -> float."""
    txt = raw.strip().replace(" ", "").replace("$", "")
    if "," in txt and "." in txt:
        txt = txt.replace(",", "")
    elif "," in txt:
        txt = txt.replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        raise SignalError(f"número inválido: {raw!r}")


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def quantize(value: Any, places: int, rounding=ROUND_HALF_UP) -> Decimal:
    return dec(value).quantize(Decimal(1).scaleb(-places), rounding=rounding)


def fmt(value: Decimal) -> str:
    """'0.0200' -> '0.02', sin notación científica."""
    text = format(value.normalize(), "f")
    return "0" if text in ("-0", "-0.0") else text


def fmt_price(value: Any, places: int, rounding=ROUND_HALF_UP) -> str:
    return fmt(quantize(value, places, rounding))


def fmt_qty(value: Any, places: int) -> str:
    return fmt(quantize(value, places, ROUND_FLOOR))


# --------------------------------------------------------------------------------------
# Parsing de señales
# --------------------------------------------------------------------------------------


class SignalError(Exception):
    """Error de parsing o validación de la señal."""


RE_PAIR = re.compile(r"([A-Za-z0-9]{2,12})\s*/\s*([A-Za-z]{2,6})")
RE_BARE = re.compile(r"\b([A-Z]{2,10}USDT)(?:\.P)?\b")
RE_ENTRY = re.compile(r"(?:entrada|entry|precio\s*de\s*entrada|\bprice\b)\s*[:=]?\s*\$?([-+]?[\d.,]+)", re.I)
RE_SL = re.compile(r"(?:stop\s*[- ]?loss|stoploss|\bsl\b)\s*[:=]?\s*\$?([-+]?[\d.,]+)", re.I)
RE_TP = re.compile(
    r"(?:🎯\s*)?(?:objetivo|目标|target|take\s*profit|(?<![a-z])tp(?![a-z]))\s*#?\s*(\d{1,2})\s*[:=]?\s*\$?([-+]?[\d.,]+)",
    re.I,
)
RE_LEV = re.compile(r"(?:apalancamiento|leverage)\s*[:=]?\s*x?\s*(\d{1,3})\s*x?", re.I)
RE_SIDE = (
    ("SHORT", re.compile(r"\bshort\b|🔴|venta|bajista", re.I)),
    ("LONG", re.compile(r"\blong\b|🟢|🟩|compra|alcista", re.I)),
)


@dataclass
class Signal:
    symbol: str
    side: str                                  # LONG | SHORT
    entry: float
    sl: float
    tps: list[float] = field(default_factory=list)  # ordenados: tps[0] es el primero en ejecutarse
    leverage: int | None = None
    raw: str = ""

    def brief(self) -> str:
        lev = self.leverage or CFG.default_leverage
        return (f"{self.symbol} {self.side} entrada={self.entry} sl={self.sl} "
                f"tps={self.tps} lev=x{lev}")


def extract_text(payload: Any) -> str:
    """Devuelve el texto de la señal venga como texto plano o dentro de JSON."""
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", "replace")
    if isinstance(payload, str):
        txt = payload.strip()
        if txt.startswith("{") or txt.startswith("["):
            try:
                return extract_text(json.loads(txt))
            except json.JSONDecodeError:
                pass
        return txt
    if isinstance(payload, dict):
        lowered = {str(k).lower(): v for k, v in payload.items()}
        if (lowered.get("symbol") or lowered.get("par")) and (lowered.get("entry") or lowered.get("entrada")):
            return json.dumps(payload, ensure_ascii=False)
        for key in ("text", "alert", "message", "msg", "body", "content", "signal", "data", "payload"):
            value = lowered.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, (dict, list)):
                nested = extract_text(value)
                if nested.strip():
                    return nested
        return json.dumps(payload, ensure_ascii=False)
    if isinstance(payload, list):
        for item in payload:
            nested = extract_text(item)
            if nested.strip():
                return nested
        return ""
    return str(payload)


def parse_signal(payload: Any) -> Signal:
    """Convierte el body del webhook en una Signal validada."""
    text = extract_text(payload)
    if not text.strip():
        raise SignalError("webhook vacío")

    # JSON estructurado: {"symbol":"CHZUSDT","side":"short","entry":0.00883,...}
    try:
        maybe = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        maybe = None
    if isinstance(maybe, dict):
        lowered = {str(k).lower(): v for k, v in maybe.items()}
        if (lowered.get("symbol") or lowered.get("par")) and (lowered.get("entry") or lowered.get("entrada")):
            symbol = str(lowered.get("symbol") or lowered.get("par")).upper().replace("/", "").replace(" ", "")
            side_raw = str(lowered.get("side") or lowered.get("lado") or lowered.get("direccion") or "").lower()
            side = "SHORT" if any(k in side_raw for k in ("short", "sell", "corto")) else "LONG"
            entry = parse_number(str(lowered.get("entry") or lowered.get("entrada")))
            sl_raw = lowered.get("sl") or lowered.get("stop_loss") or lowered.get("stop")
            if sl_raw is None:
                raise SignalError("JSON sin stop loss")
            raw_tps = lowered.get("tps") or lowered.get("objetivos") or lowered.get("targets") or []
            if isinstance(raw_tps, (str, int, float)):
                raw_tps = [raw_tps]
            lev = lowered.get("leverage") or lowered.get("apalancamiento")
            return validate_signal(Signal(
                symbol=symbol,
                side=side,
                entry=entry,
                sl=parse_number(str(sl_raw)),
                tps=[parse_number(str(t)) for t in raw_tps],
                leverage=int(parse_number(str(lev))) if lev not in (None, "") else None,
                raw=text,
            ))

    match = RE_PAIR.search(text)
    if match:
        symbol = (match.group(1) + match.group(2)).upper()
    else:
        match = RE_BARE.search(text.upper())
        if not match:
            raise SignalError("no encontré el par en la señal")
        symbol = match.group(1)

    side, best_pos = "", math.inf
    for name, rx in RE_SIDE:
        hit = rx.search(text)
        if hit and hit.start() < best_pos:
            best_pos, side = hit.start(), name
    if not side:
        raise SignalError("no encontré el lado (Long/Short)")

    match = RE_ENTRY.search(text)
    if not match:
        raise SignalError("no encontré la entrada")
    entry = parse_number(match.group(1))

    match = RE_SL.search(text)
    if not match:
        raise SignalError("no encontré el stop loss")
    sl = parse_number(match.group(1))

    targets: dict[int, float] = {}
    for hit in RE_TP.finditer(text):
        idx = int(hit.group(1))
        targets.setdefault(idx, parse_number(hit.group(2)))
    if not targets:
        raise SignalError("no encontré ningún objetivo (TP)")
    tps = [targets[k] for k in sorted(targets)]

    match = RE_LEV.search(text)
    leverage = int(match.group(1)) if match else None

    return validate_signal(Signal(symbol=symbol, side=side, entry=entry, sl=sl,
                                  tps=tps, leverage=leverage, raw=text))


def validate_signal(sig: Signal) -> Signal:
    if not re.fullmatch(r"[A-Z0-9]{5,20}", sig.symbol):
        raise SignalError(f"par inválido: {sig.symbol!r}")
    if sig.entry <= 0 or sig.sl <= 0:
        raise SignalError("entrada o stop loss inválidos")
    if not sig.tps or any(t <= 0 for t in sig.tps):
        raise SignalError("objetivos inválidos")

    if sig.side == "SHORT":
        if sig.sl <= sig.entry:
            raise SignalError("SHORT: el SL debe estar por encima de la entrada")
        bad = [t for t in sig.tps if t >= sig.entry]
        sig.tps = sorted(sig.tps, reverse=True)
    else:
        if sig.sl >= sig.entry:
            raise SignalError("LONG: el SL debe estar por debajo de la entrada")
        bad = [t for t in sig.tps if t <= sig.entry]
        sig.tps = sorted(sig.tps)
    if bad:
        raise SignalError(f"los objetivos contradicen el lado {sig.side}: {bad}")

    if sig.leverage is not None and sig.leverage < 1:
        sig.leverage = None
    return sig


# --------------------------------------------------------------------------------------
# Cliente Bitunix (API v3, firma SHA256 doble)
# --------------------------------------------------------------------------------------


class BitunixError(Exception):
    def __init__(self, code: Any, msg: str, path: str = ""):
        super().__init__(f"[{code}] {msg} ({path})")
        self.code = code
        self.msg = msg
        self.path = path


class Bitunix:
    def __init__(self, cfg: Config, session: requests.Session | None = None):
        self.cfg = cfg
        self.http = session or requests.Session()
        self._pairs: dict[str, dict] = {}
        self._pairs_ts = 0.0
        self._setup_cache: set[str] = set()
        self.position_mode: str | None = None

    # ---- firma ----------------------------------------------------------------------
    @staticmethod
    def _sha(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _query_str(params: dict | None) -> str:
        if not params:
            return ""
        return "".join(f"{k}{'' if v is None else v}" for k, v in sorted(params.items()))

    def _headers(self, query: str, body: str) -> dict[str, str]:
        nonce = secrets.token_hex(16)
        ts = str(int(time.time() * 1000))
        digest = self._sha(nonce + ts + self.cfg.api_key + query + body)
        return {
            "api-key": self.cfg.api_key,
            "nonce": nonce,
            "timestamp": ts,
            "sign": self._sha(digest + self.cfg.api_secret),
            "language": "en-US",
            "Content-Type": "application/json",
            "User-Agent": "proxy-bitunix/1.0",
        }

    # ---- HTTP -----------------------------------------------------------------------
    def _call(self, method: str, path: str, params: dict | None = None, body: dict | None = None,
              auth: bool = True, mutating: bool = False) -> Any:
        query = self._query_str(params)
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""
        url = f"{self.cfg.base_url}{path}"

        if mutating and self.cfg.dry_run:
            log("DRY", f"{method} {path} {body_str or query}")
            return {"orderId": "dry-" + secrets.token_hex(4)}

        last_err: Exception | None = None
        for attempt in range(self.cfg.retries + 1):
            try:
                headers = self._headers(query, body_str) if auth else {"User-Agent": "proxy-bitunix/1.0"}
                resp = self.http.request(
                    method, url,
                    params=sorted((k, "" if v is None else str(v)) for k, v in params.items()) if params else None,
                    data=body_str or None,
                    headers=headers,
                    timeout=self.cfg.request_timeout_sec,
                )
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise BitunixError(f"http-{resp.status_code}", resp.text[:200], path)
                try:
                    payload = resp.json()
                except ValueError:
                    raise BitunixError(f"http-{resp.status_code}", resp.text[:200], path)
                code = payload.get("code")
                if code not in (0, "0", None):
                    raise BitunixError(code, str(payload.get("msg") or "sin mensaje"), path)
                return payload.get("data")
            except BitunixError as exc:
                last_err = exc
                retryable = str(exc.code).startswith("http-5") or str(exc.code) == "http-429"
                if retryable and attempt < self.cfg.retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise
            except requests.RequestException as exc:
                last_err = exc
                if attempt < self.cfg.retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise BitunixError("network", str(exc), path)
        raise BitunixError("unknown", str(last_err), path)

    # ---- market (público) -----------------------------------------------------------
    def trading_pairs(self, force: bool = False) -> dict[str, dict]:
        if force or time.time() - self._pairs_ts > 600:
            data = self._call("GET", "/api/v1/futures/market/trading_pairs", auth=False) or []
            self._pairs = {row["symbol"]: row for row in data if row.get("symbol")}
            self._pairs_ts = time.time()
        return self._pairs

    def pair(self, symbol: str) -> dict:
        info = self.trading_pairs().get(symbol)
        if not info:
            rows = self._call("GET", "/api/v1/futures/market/trading_pairs",
                              params={"symbols": symbol}, auth=False) or []
            info = next((row for row in rows if row.get("symbol") == symbol), None)
        if not info:
            raise BitunixError("no_pair", f"{symbol} no existe en Bitunix futuros", "trading_pairs")
        return info

    def last_price(self, symbol: str) -> float | None:
        data = self._call("GET", "/api/v1/futures/market/tickers", params={"symbols": symbol}, auth=False)
        if isinstance(data, dict):
            data = [data]
        for row in data or []:
            if row.get("symbol") != symbol:
                continue
            for key in ("lastPrice", "last", "markPrice"):
                if row.get(key):
                    try:
                        return float(row[key])
                    except (TypeError, ValueError):
                        continue
        return None

    # ---- cuenta / posiciones --------------------------------------------------------
    def account(self) -> dict:
        data = self._call("GET", "/api/v1/futures/account", params={"marginCoin": self.cfg.margin_coin})
        if isinstance(data, list):
            data = data[0] if data else {}
        self.position_mode = data.get("positionMode") or self.position_mode
        return data

    def equity(self) -> tuple[float, float]:
        """(equity total, disponible) en la moneda de margen."""
        acc = self.account()
        get = lambda k: float(acc.get(k) or 0)
        available, frozen, margin = get("available"), get("frozen"), get("margin")
        unrealized = get("crossUnrealizedPNL") + get("isolationUnrealizedPNL") + get("bonus")
        return available + frozen + margin + unrealized, available

    def pending_positions(self, symbol: str | None = None) -> list[dict]:
        data = self._call("GET", "/api/v1/futures/position/get_pending_positions",
                          params={"symbol": symbol} if symbol else None)
        if isinstance(data, dict):
            data = data.get("list") or data.get("data") or []
        return [row for row in data or [] if dec(row.get("qty") or 0) != 0]

    def position(self, position_id: str) -> dict | None:
        for row in self.pending_positions():
            if str(row.get("positionId")) == str(position_id):
                return row
        return None

    def order_detail(self, order_id: str | None = None, client_id: str | None = None) -> dict | None:
        if order_id:
            params: dict[str, str] | None = {"orderId": str(order_id)}
        elif client_id:
            params = {"clientId": client_id}
        else:
            return None
        data = self._call("GET", "/api/v1/futures/trade/get_order_detail", params=params)
        return data if isinstance(data, dict) else None

    def pending_tp_sl(self, symbol: str | None = None, position_id: str | None = None) -> list[dict]:
        params: dict[str, Any] = {"limit": 100}
        if symbol:
            params["symbol"] = symbol
        if position_id:
            params["positionId"] = position_id
        data = self._call("GET", "/api/v1/futures/tpsl/get_pending_orders", params=params)
        if isinstance(data, dict):
            return data.get("list") or []
        return data or []

    # ---- configuración del símbolo --------------------------------------------------
    def ensure_symbol_setup(self, symbol: str, leverage: int) -> None:
        """Deja el símbolo en margen AISLADO y con el apalancamiento pedido."""
        key = f"{symbol}:{leverage}"
        if key in self._setup_cache:
            return
        info = self._call("GET", "/api/v1/futures/account/get_leverage_margin_mode",
                          params={"symbol": symbol, "marginCoin": self.cfg.margin_coin}) or {}
        current_lev = int(info.get("leverage") or 0)
        current_mode = str(info.get("marginMode") or "").upper()

        if current_mode != self.cfg.margin_mode:
            try:
                self._call("POST", "/api/v1/futures/account/change_margin_mode",
                           body={"symbol": symbol, "marginMode": self.cfg.margin_mode,
                                 "marginCoin": self.cfg.margin_coin}, mutating=True)
                log("INFO", f"{symbol}: margen cambiado a {self.cfg.margin_mode}")
            except BitunixError as exc:
                raise BitunixError(exc.code,
                                   f"no se pudo dejar el margen en ISOLATION: {exc.msg}", "margin_mode")

        if current_lev != leverage:
            self._call("POST", "/api/v1/futures/account/change_leverage",
                       body={"symbol": symbol, "leverage": leverage,
                             "marginCoin": self.cfg.margin_coin}, mutating=True)
            log("INFO", f"{symbol}: apalancamiento cambiado a x{leverage}")
        self._setup_cache.add(key)

    # ---- órdenes -------------------------------------------------------------------
    def place_order(self, symbol: str, side: str, qty: str, price: str | None, order_type: str,
                    effect: str, client_id: str, trade_side: str | None = None) -> dict:
        body: dict[str, Any] = {
            "symbol": symbol, "qty": qty, "side": side,
            "orderType": order_type, "effect": effect, "clientId": client_id,
        }
        if order_type == "LIMIT" and price:
            body["price"] = price
        if trade_side:
            body["tradeSide"] = trade_side
        return self._call("POST", "/api/v1/futures/trade/place_order", body=body, mutating=True) or {}

    def cancel_order(self, symbol: str, order_id: str | None = None, client_id: str | None = None) -> Any:
        # La API no tiene /trade/cancel_order (404): se cancela con /trade/cancel_orders y una
        # lista de órdenes; los rechazos por orden vienen en failureList, no en el code general.
        item = {"orderId": str(order_id)} if order_id else {"clientId": client_id}
        path = "/api/v1/futures/trade/cancel_orders"
        data = self._call("POST", path, body={"symbol": symbol, "orderList": [item]}, mutating=True)
        failures = data.get("failureList") if isinstance(data, dict) else None
        if failures:
            first = failures[0]
            raise BitunixError(first.get("errorCode"), str(first.get("errorMsg") or "cancelación rechazada"), path)
        return data

    def place_position_tp_sl(self, symbol: str, position_id: str, sl_price: str,
                             tp_price: str | None = None) -> dict:
        body: dict[str, Any] = {
            "symbol": symbol, "positionId": str(position_id),
            "slPrice": sl_price, "slStopType": self.cfg.sl_stop_type,
        }
        if tp_price:
            body["tpPrice"] = tp_price
            body["tpStopType"] = self.cfg.tp_stop_type
        return self._call("POST", "/api/v1/futures/tpsl/position/place_order",
                          body=body, mutating=True) or {}

    def modify_position_tp_sl(self, symbol: str, position_id: str, sl_price: str) -> dict:
        return self._call("POST", "/api/v1/futures/tpsl/position/modify_order", mutating=True,
                          body={"symbol": symbol, "positionId": str(position_id),
                                "slPrice": sl_price, "slStopType": self.cfg.sl_stop_type}) or {}

    def place_partial_tp(self, symbol: str, position_id: str, tp_price: str, qty: str) -> dict:
        body: dict[str, Any] = {
            "symbol": symbol, "positionId": str(position_id),
            "tpPrice": tp_price, "tpStopType": self.cfg.tp_stop_type,
            "tpQty": qty, "tpOrderType": self.cfg.tp_order_type,
        }
        if self.cfg.tp_order_type == "LIMIT":
            body["tpOrderPrice"] = tp_price
        return self._call("POST", "/api/v1/futures/tpsl/place_order", body=body, mutating=True) or {}

    def cancel_tp_sl(self, symbol: str, order_id: str) -> Any:
        return self._call("POST", "/api/v1/futures/tpsl/cancel_order", mutating=True,
                          body={"symbol": symbol, "orderId": str(order_id)})


# --------------------------------------------------------------------------------------
# Estado persistente (sobrevive reinicios)
# --------------------------------------------------------------------------------------


class State:
    """Estado persistido: posiciones, dedupe, series de equity y contadores diarios."""

    METRICS_KEEP = 1440          # ~12 h a un muestreo por poll
    DAILY_KEEP = 400             # días que conserva el mapa de actividad

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.positions: list[dict] = []
        self.seen: dict[str, float] = {}
        self.metrics: list[dict] = []          # series {ts, equity, available, margin, active}
        self.daily: dict[str, dict] = {}       # {"2026-11-22": {"signals": 3, ...}}
        self.counters: dict[str, float] = {}   # totales históricos
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        self.positions = raw.get("positions") or []
        self.seen = {k: float(v) for k, v in (raw.get("seen") or {}).items()}
        self.metrics = [m for m in (raw.get("metrics") or []) if isinstance(m, dict)]
        self.daily = {k: v for k, v in (raw.get("daily") or {}).items() if isinstance(v, dict)}
        self.counters = {k: float(v) for k, v in (raw.get("counters") or {}).items()}
        log("INFO", f"estado recuperado: {len(self.active())} operación(es) activa(s) a re-monitorear, "
                    f"{len(self.metrics)} muestras de equity")

    def save(self) -> None:
        with self.lock:
            payload = json.dumps({"positions": self.positions, "seen": self.seen,
                                  "metrics": self.metrics, "daily": self.daily,
                                  "counters": self.counters},
                                 ensure_ascii=False, indent=2)
        try:
            # temporal único: webhook y monitor pueden guardar a la vez
            tmp = self.path.with_name(f"{self.path.name}.{os.getpid()}-{threading.get_ident()}.tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:
            log("ERROR", f"no se pudo guardar el estado: {exc}")

    def counts(self) -> dict:
        """Totales por hito, sin los marcas de tiempo internas."""
        return {k: v for k, v in self.counters.items() if not k.startswith("last_")}

    def lasts(self) -> dict:
        """Último hito de cada tipo, para /status."""
        return {k[5:]: v for k, v in self.counters.items() if k.startswith("last_")}

    def seen_recently(self, digest: str, window: int) -> bool:
        now = time.time()
        with self.lock:
            self.seen = {k: v for k, v in self.seen.items() if now - v < window * 3}
            last = self.seen.get(digest)
            if last is not None and now - last < window:
                return True
            self.seen[digest] = now
        self.save()
        return False

    def add(self, record: dict) -> None:
        with self.lock:
            self.positions.append(record)
        self.save()

    def update(self, record_id: str, **fields: Any) -> None:
        with self.lock:
            for row in self.positions:
                if str(row.get("id")) == str(record_id):
                    row.update(fields)
                    row["updated"] = time.time()
                    break
        self.save()

    def active(self) -> list[dict]:
        with self.lock:
            return [dict(p) for p in self.positions if p.get("status") in ("WAITING_FILL", "OPEN")]

    def simulating(self) -> list[dict]:
        """Registros del ciclo simulado: los nacidos en dry-run y los abiertos marcados como simulados."""
        live = ("DRY_RUN", "OPEN", "WAITING_FILL")
        with self.lock:
            return [dict(p) for p in self.positions
                    if p.get("status") in live and (p.get("sim") or p.get("status") == "DRY_RUN")]

    def prune(self, keep: int = 60) -> None:
        with self.lock:
            closed = [p for p in self.positions if p.get("status") not in ("WAITING_FILL", "OPEN")]
            if len(closed) > keep:
                drop = {p["id"] for p in closed[:-keep]}
                self.positions = [p for p in self.positions if p.get("id") not in drop]
        self.save()

    # ---- series y contadores (historial en /status y /events) -----------------------
    def bump(self, kind: str) -> None:
        """Cuenta un hito (signals/fills/be/closes/errors) en el día UTC de hoy."""
        ts = time.time()
        day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            row = self.daily.setdefault(day, {})
            row[kind] = int(row.get(kind, 0)) + 1
            if len(self.daily) > self.DAILY_KEEP:
                for old in sorted(self.daily)[: len(self.daily) - self.DAILY_KEEP]:
                    self.daily.pop(old, None)
            self.counters[kind] = int(self.counters.get(kind, 0)) + 1
            self.counters["last_" + kind] = ts

    def sample(self, ts: float, equity: float | None, available: float | None,
               margin: float, active: int) -> None:
        """Una muestra de cuenta; si va en el mismo segundo que la última, la sustituye."""
        row = {"ts": int(ts),
               "equity": None if equity is None else round(float(equity), 4),
               "available": None if available is None else round(float(available), 4),
               "margin": round(float(margin), 4), "active": int(active)}
        with self.lock:
            if self.metrics and ts - self.metrics[-1]["ts"] < 5:
                self.metrics[-1] = row
            else:
                self.metrics.append(row)
                if len(self.metrics) > self.METRICS_KEEP:
                    del self.metrics[:-self.METRICS_KEEP]
        self.save()

    def series(self, hours: float = 24.0, max_points: int = 140) -> list[dict]:
        """Últimas `hours` reducidas a como mucho `max_points` puntos, media por cubo."""
        cut = time.time() - hours * 3600
        rows = [m for m in self.metrics if m["ts"] >= cut] or self.metrics[-max_points:]
        if len(rows) <= max_points:
            return rows
        step = math.ceil(len(rows) / max_points)
        out = []
        for i in range(0, len(rows), step):
            chunk = rows[i:i + step]
            avg = lambda k: (sum(r[k] for r in chunk if r.get(k) is not None) /
                             max(1, sum(1 for r in chunk if r.get(k) is not None)))
            out.append({"ts": chunk[-1]["ts"], "equity": avg("equity"),
                        "available": avg("available"), "margin": avg("margin"),
                        "active": int(round(avg("active")))})
        return out

    def daily_days(self, days: int = 120) -> dict[str, dict]:
        keys = sorted(self.daily)[-days:]
        return {k: self.daily[k] for k in keys}


STATE = State(Path(CFG.state_file) if Path(CFG.state_file).is_absolute() else SCRIPT_DIR / CFG.state_file)
EX = Bitunix(CFG)
START_TS = time.time()


# --------------------------------------------------------------------------------------
# Plan de la operación (tamaño, precios, apalancamiento)
# --------------------------------------------------------------------------------------


def decide_leverage(sig: Signal) -> int:
    lev = int(sig.leverage) if (CFG.use_signal_leverage and sig.leverage) else CFG.default_leverage
    lev = max(1, min(lev, CFG.max_leverage))
    if sig.leverage and lev != sig.leverage:
        log("WARNING", f"apalancamiento x{sig.leverage} ajustado a x{lev} por MAX_LEVERAGE")
    return lev


def as_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def plan_trade(sig: Signal) -> dict:
    info = EX.pair(sig.symbol)
    if not info.get("isApiSupported", True):
        raise SignalError(f"{sig.symbol}: trading por API deshabilitado")
    if str(info.get("symbolStatus") or "OPEN") != "OPEN":
        raise SignalError(f"{sig.symbol}: estado {info.get('symbolStatus')}")

    price_places = as_int(info.get("quotePrecision"), 4)
    qty_places = as_int(info.get("basePrecision"), 0)
    min_qty = float(info.get("minTradeVolume") or 0) or float(Decimal(1).scaleb(-qty_places))

    leverage = decide_leverage(sig)
    max_lev = int(info.get("maxLeverage") or leverage)
    if leverage > max_lev:
        log("WARNING", f"{sig.symbol}: x{leverage} supera el máximo del par, uso x{max_lev}")
        leverage = max_lev

    entry = float(fmt_price(sig.entry, price_places))
    market = EX.last_price(sig.symbol)
    if market and CFG.max_price_deviation_pct > 0 and CFG.entry_order_type == "LIMIT":
        deviation = abs(market - entry) / market * 100
        if deviation > CFG.max_price_deviation_pct:
            raise SignalError(
                f"entrada {entry} a {deviation:.2f}% del mercado ({market}); "
                f"sube MAX_PRICE_DEVIATION_PCT si es intencional")

    if CFG.dry_run and not (CFG.api_key and CFG.api_secret):
        equity = available = _dry_equity()            # sin claves: ni lo intentamos
    else:
        try:
            equity, available = EX.equity()
        except BitunixError as exc:
            if not CFG.dry_run:
                raise
            equity = available = _dry_equity()
            log("WARNING", f"dry-run: sin equity real ({exc.msg}); uso {equity:.2f} simulado")
    pct = CFG.position_size_pct / 100.0
    if CFG.sizing_mode == "notional":
        notional = equity * pct
    else:
        notional = equity * pct * leverage
    cap = available * 0.98 * leverage           # margen disponible real, con colchón para fees
    if notional > cap:
        log("WARNING", f"notional {notional:.2f} recortado a {cap:.2f} (disponible {available:.2f})")
        notional = cap

    qty = fmt_qty(notional / entry, qty_places)
    if dec(qty) < dec(min_qty):
        raise SignalError(
            f"cantidad {qty} < mínimo del par ({min_qty}); equity={equity:.2f} "
            f"size={CFG.position_size_pct}% lev=x{leverage}")
    max_vol = float(info.get("maxLimitOrderVolume") or 0)
    if max_vol and dec(qty) > dec(max_vol):
        raise SignalError(f"cantidad {qty} supera el máximo del par ({max_vol})")

    tps = [float(fmt_price(t, price_places)) for t in sig.tps]
    sl = float(fmt_price(sig.sl, price_places))
    if sig.side == "SHORT":
        if sl <= entry:
            raise SignalError("SL redondeado no queda por encima de la entrada")
        tps = [t for t in tps if t < entry]
    else:
        if sl >= entry:
            raise SignalError("SL redondeado no queda por debajo de la entrada")
        tps = [t for t in tps if t > entry]
    if not tps:
        raise SignalError("ningún objetivo sobrevive al redondeo del tick")
    unique_tps = list(dict.fromkeys(tps))
    if len(unique_tps) != len(tps):
        log("WARNING", f"{sig.symbol}: objetivos repetidos tras redondear -> {unique_tps}")
        tps = unique_tps

    return {
        "price_places": price_places, "qty_places": qty_places, "min_qty": min_qty,
        "leverage": leverage, "entry": entry, "sl": sl, "tps": tps, "qty": qty,
        "notional": notional, "margin": notional / leverage,
        "equity": equity, "available": available, "market": market,
    }


def split_qty(total: str, count: int, places: int, min_qty: Any,
              weights: list[float] | None = None) -> list[str]:
    """Reparte la cantidad entre los N objetivos sin romper la cantidad mínima."""
    total_d = dec(total)
    minimum = dec(min_qty)
    if count <= 1:
        return [fmt(total_d)]
    parts = ([w / sum(weights) for w in weights]
             if weights and len(weights) == count and sum(weights) > 0
             else [1.0 / count] * count)

    chunks: list[Decimal] = []
    remaining = total_d
    for idx, part in enumerate(parts):
        if idx == count - 1:
            chunks.append(remaining)
            break
        raw = quantize(total_d * dec(part), places, ROUND_FLOOR)
        if raw <= 0 or remaining - raw < minimum:
            raw = Decimal(0)
        chunks.append(raw)
        remaining -= raw

    out = [fmt(c) for c in chunks if dec(c) >= minimum]
    if not out or sum(dec(x) for x in out) < total_d:
        return [fmt(total_d)]          # mejor un solo TP con todo, que migas inválidas
    return out


def parse_weights(raw: str) -> list[float] | None:
    try:
        parts = [float(x) for x in re.split(r"[,;/]", raw) if x.strip()]
    except ValueError:
        return None
    return parts or None


def be_price(side: str, entry: float, places: int) -> str:
    offset = CFG.be_offset_pct / 100.0
    raw = entry * (1 - offset) if side == "SHORT" else entry * (1 + offset)
    return fmt_price(raw, places)


# --------------------------------------------------------------------------------------
# Ejecución de señales
# --------------------------------------------------------------------------------------


def committed_margin() -> float:
    """Margen ya comprometido por las operaciones que estamos gestionando."""
    total = 0.0
    for rec in STATE.active():
        try:
            total += float(rec.get("notional") or 0) / max(1, int(rec.get("leverage") or 1))
        except (TypeError, ValueError):
            continue
    return total


def handle_signal(payload: Any, origin: str = "webhook") -> dict:
    """Parsea, valida y ejecuta una señal. Devuelve la respuesta para el webhook."""
    try:
        sig = parse_signal(payload)
    except (SignalError, BitunixError) as exc:
        log("ERROR", f"señal rechazada ({origin}): {exc} :: {extract_text(payload)[:200]!r}")
        return {"ok": False, "error": str(exc)}

    digest = hashlib.sha256(
        f"{sig.symbol}|{sig.side}|{sig.entry}|{sig.sl}|{sorted(sig.tps)}".encode()).hexdigest()[:32]
    if STATE.seen_recently(digest, CFG.dedupe_window_sec):
        log("WARNING", f"señal duplicada ignorada: {sig.brief()}")
        return {"ok": False, "error": "señal duplicada", "signal": sig.brief()}

    if sig.symbol in {p["symbol"] for p in STATE.active()}:
        log("WARNING", f"{sig.symbol}: ya hay una operación gestionada; señal ignorada")
        return {"ok": False, "error": f"{sig.symbol} ya está gestionado", "signal": sig.brief()}

    if CFG.max_open_positions > 0 and len(STATE.active()) >= CFG.max_open_positions:
        return {"ok": False, "error": f"límite de {CFG.max_open_positions} operaciones activas"}

    if not CFG.allow_reverse and CFG.api_key and CFG.api_secret:
        try:
            for pos in EX.pending_positions(sig.symbol):
                if norm_ex_side(pos.get("side")) != sig.side:
                    raise SignalError(f"{sig.symbol}: posición {norm_ex_side(pos.get('side'))} abierta "
                                      f"en contra (ALLOW_REVERSE=false)")
        except BitunixError as exc:
            log("WARNING", f"no pude consultar posiciones abiertas: {exc}")

    try:
        plan = plan_trade(sig)
    except (SignalError, BitunixError) as exc:
        log("ERROR", f"plan inválido para {sig.symbol}: {exc}")
        return {"ok": False, "error": f"plan inválido: {exc}", "signal": sig.brief()}

    used = committed_margin()
    if CFG.max_total_margin_pct > 0 and plan["equity"] > 0:
        cap = plan["equity"] * CFG.max_total_margin_pct / 100.0
        if used + plan["margin"] > cap:
            msg = (f"margen sumado {used + plan['margin']:.2f}USDT pasaría del tope de "
                   f"{CFG.max_total_margin_pct:g}% ({cap:.2f}USDT)")
            log("WARNING", f"{sig.symbol}: {msg}; señal descartada")
            return {"ok": False, "error": msg, "signal": sig.brief()}
        log("INFO", f"{sig.symbol}: margen {used + plan['margin']:.2f}/{cap:.2f}USDT "
                    f"({CFG.max_total_margin_pct:g}% del equity)")

    client_id = f"px{int(time.time() * 1000)}{secrets.token_hex(3)}"
    order_side = "BUY" if sig.side == "LONG" else "SELL"
    price = fmt_price(plan["entry"], plan["price_places"]) if CFG.entry_order_type == "LIMIT" else None
    trade_side = "OPEN" if (EX.position_mode or "").upper() == "HEDGE" else None

    record = {
        "id": client_id,
        "symbol": sig.symbol,
        "side": sig.side,
        "status": "WAITING_FILL",
        "entry_price": plan["entry"],
        "sl": plan["sl"],
        "tps": plan["tps"],
        "qty": plan["qty"],
        "filled": "0",
        "leverage": plan["leverage"],
        "notional": round(plan["notional"], 4),
        "price_places": plan["price_places"],
        "qty_places": plan["qty_places"],
        "min_qty": plan["min_qty"],
        "order_id": None,
        "position_id": None,
        "sl_order_id": None,
        "tp_orders": {},
        "tp_plan": {},
        "be_done": False,
        "be_price": None,
        "note": "",
        "created": time.time(),
        "updated": time.time(),
        "raw": sig.raw[:800],
    }

    summary = {"qty": plan["qty"], "notional": round(plan["notional"], 4),
               "margin": round(plan["margin"], 4), "leverage": plan["leverage"],
               "entry": plan["entry"], "sl": plan["sl"], "tps": plan["tps"],
               "be_price": float(be_price(sig.side, plan["entry"], plan["price_places"]))
                           if CFG.be_enabled else None}

    if CFG.dry_run:
        log("DRY", f"orden: {sig.symbol} {order_side} qty={plan['qty']} @ {price or 'MERCADO'} "
                   f"lev=x{plan['leverage']} margen={plan['margin']:.2f}USDT "
                   f"notional={plan['notional']:.2f}USDT equity={plan['equity']:.2f}")
        log("DRY", f"protección: SL en {plan['sl']} y objetivos {plan['tps']} "
                   f"(modo {CFG.tp_mode}, split {CFG.tp_split}); BE en "
                   f"{summary['be_price'] if CFG.be_enabled else 'desactivado'}")
        record["status"] = "DRY_RUN"
        record["note"] = "dry-run: no se envió nada al exchange"
        STATE.add(record)
        STATE.bump("signals")
        return {"ok": True, "dry_run": True, "signal": sig.brief(), "plan": summary}

    # Margin mode aislado + apalancamiento antes de enviar la orden.
    EX.ensure_symbol_setup(sig.symbol, plan["leverage"])

    try:
        placed = EX.place_order(sig.symbol, order_side, plan["qty"], price,
                                CFG.entry_order_type, CFG.entry_time_in_force, client_id, trade_side)
    except BitunixError as exc:
        log("ERROR", f"orden rechazada por Bitunix en {sig.symbol}: {exc}")
        return {"ok": False, "error": f"orden rechazada: {exc.msg}", "signal": sig.brief()}

    record["order_id"] = str(placed.get("orderId") or "")
    STATE.add(record)
    STATE.bump("signals")
    log("INFO", f"ORDEN {sig.symbol} {sig.side} qty={plan['qty']} @ {price or 'MERCADO'} "
                f"lev=x{plan['leverage']} notional={plan['notional']:.2f}USDT orderId={record['order_id']}")

    # SL/TP se arman aquí mismo, en cuanto se llena la entrada; el monitor es la copia de seguridad.
    protection = fast_protect(record, max_sec=CFG.fast_arm_sec)
    log("INFO", f"{sig.symbol}: protección {protection}")
    return {"ok": True, "signal": sig.brief(), "order_id": record["order_id"],
            "protection": protection, **summary}


# --------------------------------------------------------------------------------------
# Monitor: llenado, protección de la posición y break-even
# --------------------------------------------------------------------------------------


def norm_ex_side(v: object) -> str:
    """En HEDGE el exchange reporta el lado como BUY/SELL; lo normalizamos a LONG/SHORT."""
    s = str(v or "").upper()
    return {"BUY": "LONG", "SELL": "SHORT"}.get(s, s)


def find_position(rec: dict, allow_older: bool = False) -> dict | None:
    """Posición generada por la orden de entrada (mismo símbolo y lado).

    Con allow_older=True también acepta una posición que ya existiera (ONE_WAY sumando a
    una posición previa), para no dejar una orden llena sin protección.
    """
    since = int(rec["created"] * 1000) - 15_000
    newest, older = None, None
    for pos in EX.pending_positions(rec["symbol"]):
        if norm_ex_side(pos.get("side")) != rec["side"]:
            continue
        created = int(pos.get("ctime") or 0)
        bucket = older if (created and created < since) else newest
        if bucket is None or dec(pos.get("qty") or 0) > dec(bucket.get("qty") or 0):
            if created and created < since:
                older = pos
            else:
                newest = pos
    return newest if newest is not None else (older if allow_older else None)


def single_tp_price(rec: dict) -> str | None:
    """Precio del TP único en modo single; None en modo partials."""
    if CFG.tp_mode != "single":
        return None
    tps, places = rec["tps"], int(rec["price_places"])
    target = CFG.single_tp_target
    if target == "last":
        tp = tps[-1]
    elif target.isdigit():
        tp = tps[min(int(target), len(tps)) - 1]
    else:
        tp = tps[0]
    return fmt_price(tp, places)


def ensure_position_sl(rec: dict) -> None:
    """Coloca (o reintenta) la orden de posición con el SL de la señal."""
    if rec.get("sl_order_id"):
        return
    tp_price = single_tp_price(rec)
    sl_price = fmt_price(rec["sl"], int(rec["price_places"]))
    try:
        res = EX.place_position_tp_sl(rec["symbol"], str(rec["position_id"]), sl_price, tp_price)
    except BitunixError as exc:
        log("ERROR", f"{rec['symbol']}: !! el SL de posición NO se colocó ({exc.msg}); "
                     f"reintento en el próximo tick")
        return
    order_id = str(res.get("orderId") or "")
    STATE.update(rec["id"], sl_order_id=order_id)
    rec["sl_order_id"] = order_id
    log("INFO", f"{rec['symbol']}: SL de posición en {sl_price}"
                + (f" / TP {tp_price}" if tp_price else ""))


def plan_partial_tps(rec: dict) -> dict:
    """Cantidad por objetivo, calculada una sola vez y guardada en el registro."""
    if rec.get("tp_plan"):
        return rec["tp_plan"]
    tps = list(rec["tps"])
    filled = rec.get("filled") or "0"
    size = filled if dec(filled) > 0 else rec["qty"]
    chunks = split_qty(size, len(tps), int(rec["qty_places"]), rec["min_qty"],
                       parse_weights(CFG.tp_split))
    if len(chunks) != len(tps):
        log("WARNING", f"{rec['symbol']}: objetivos agrupados en {len(chunks)} orden(es)")
        tps = tps[:len(chunks)]
    plan = {fmt_price(price, int(rec["price_places"])): qty for price, qty in zip(tps, chunks)}
    STATE.update(rec["id"], tp_plan=plan)
    rec["tp_plan"] = plan
    return plan


def ensure_partial_tps(rec: dict) -> None:
    """Coloca los TP parciales que falten (uno por objetivo, reduce la posición)."""
    if CFG.tp_mode != "partials":
        return
    plan = plan_partial_tps(rec)
    orders = dict(rec.get("tp_orders") or {})
    for price, qty in plan.items():
        if price in orders:
            continue
        try:
            out = EX.place_partial_tp(rec["symbol"], str(rec["position_id"]), price, qty)
        except BitunixError as exc:
            log("ERROR", f"{rec['symbol']}: no se pudo colocar el TP {price} ({exc.msg})")
            continue
        orders[price] = {"order_id": str(out.get("orderId") or ""), "qty": qty, "filled": False}
        STATE.update(rec["id"], tp_orders=orders)
        log("INFO", f"{rec['symbol']}: TP {price} por {qty}")
    rec["tp_orders"] = orders


def arm_protection(rec: dict, position_id: str, adopted: bool = False) -> None:
    """Pasa la operación a OPEN y arma SL + objetivos.

    Se marca OPEN antes de colocar nada: si algo falla a mitad, el siguiente tick reintenta
    sólo lo que falte (ensure_position_sl / ensure_partial_tps) en vez de duplicar órdenes.
    """
    STATE.update(rec["id"], position_id=str(position_id), status="OPEN",
                 note="posición previa adoptada" if adopted else "")
    STATE.bump("fills")
    rec["position_id"], rec["status"] = str(position_id), "OPEN"
    if adopted:
        log("WARNING", f"{rec['symbol']}: la orden llenó sobre una posición ya abierta; "
                       f"se gestiona sólo el tamaño de la señal ({rec['qty']})")
    ensure_position_sl(rec)
    ensure_partial_tps(rec)
    log("INFO", f"{rec['symbol']}: posición {position_id} protegada (break-even "
               f"{'activado' if CFG.be_enabled else 'desactivado'})")


def do_break_even(rec: dict, reason: str) -> None:
    """Mueve el SL de la posición al precio de entrada + offset de comisiones."""
    if rec.get("be_done") or not CFG.be_enabled:
        return
    price = be_price(rec["side"], float(rec["entry_price"]), int(rec["price_places"]))
    try:
        EX.modify_position_tp_sl(rec["symbol"], str(rec["position_id"]), price)
    except BitunixError as exc:
        log("ERROR", f"{rec['symbol']}: fallo al mover el SL a break-even: {exc}")
        return
    STATE.update(rec["id"], be_done=True, be_price=float(price), note=f"BE por {reason}")
    STATE.bump("be")
    log("INFO", f"{rec['symbol']}: BREAK-EVEN -> SL en {price} (motivo: {reason})")
    if CFG.be_cancel_remaining_tp:
        cancel_leftover_tp(rec)


def cancel_leftover_tp(rec: dict) -> None:
    """Cancela las órdenes TP que quedaron vivas (posición ya cerrada o BE agresivo)."""
    orders = rec.get("tp_orders") or {}
    for price, meta in list(orders.items()):
        order_id = str(meta.get("order_id") or "")
        if not order_id or meta.get("filled"):
            continue
        try:
            EX.cancel_tp_sl(rec["symbol"], order_id)
            log("INFO", f"{rec['symbol']}: TP {price} cancelado")
            orders[price] = {**meta, "cancelled": True}
            STATE.update(rec["id"], tp_orders=orders)
        except BitunixError as exc:
            log("DEBUG", f"{rec['symbol']}: no se pudo cancelar el TP {price}: {exc}")


def try_protect(rec: dict) -> str:
    """Con fill confirmado: enlaza la posición y arma SL+TP en el acto.

    Devuelve 'armed' si quedó armada, 'waiting' si la posición aún no se ve y 'gone'
    si tras el fill la posición desapareció (cerrada a mano casi de inmediato).
    """
    pos = find_position(rec)
    adopted = False
    if pos is None:
        pos = find_position(rec, allow_older=True)
        adopted = pos is not None
    if pos is None:
        if time.time() - rec["updated"] > 120:
            mark_closed(rec, "entrada llena sin posición visible (¿cerrada manualmente "
                             "justo tras el fill?)", "WARNING")
            return "gone"
        return "waiting"
    arm_protection(rec, str(pos["positionId"]), adopted=adopted)
    return "armed"


def fast_protect(rec: dict, max_sec: float = 12.0, tick: float = 1.0) -> str:
    """Arma SL/TP dentro del propio webhook: vigilas la entrada cada segundo y al
    primer fill armas la protección, sin esperar al tick del monitor (que sigue de
    respaldo si esto no lo logra o si el proceso se reinicia a mitad)."""
    deadline = time.time() + max(2.0, max_sec)
    while True:
        try:
            order = EX.order_detail(order_id=rec.get("order_id"), client_id=rec["id"])
        except BitunixError as exc:
            log("WARNING", f"{rec['symbol']}: no pude leer la orden de entrada: {exc}")
            return "unknown"
        if order:
            filled = str(order.get("tradeQty") or "0")
            status = str(order.get("status") or "").upper()
            if filled != rec.get("filled"):
                STATE.update(rec["id"], filled=filled)
                rec["filled"] = filled
            if status == "REJECTED" or (status == "CANCELED" and dec(filled) == 0):
                mark_closed(rec, "orden de entrada rechazada o cancelada sin fill", "ERROR")
                return "rejected"
            if status in ("FILLED", "PART_FILLED", "CANCELED") and dec(filled) > 0:
                state = try_protect(rec)
                if state != "waiting":
                    return state
        if time.time() >= deadline:
            return "waiting"
        time.sleep(tick)


def mark_closed(rec: dict, note: str, level: str = "INFO") -> None:
    STATE.update(rec["id"], status="CLOSED", note=note)
    STATE.bump("closes")
    log(level, f"{rec['symbol']}: operación cerrada ({note})")


def check_waiting_fill(rec: dict) -> None:
    """Espera el fill de la orden de entrada y arma la protección."""
    try:
        order = EX.order_detail(order_id=rec.get("order_id"), client_id=rec["id"])
    except BitunixError as exc:
        log("WARNING", f"{rec['symbol']}: no pude leer la orden de entrada: {exc}")
        return
    if not order:
        stale_after = max(120.0, CFG.limit_order_ttl_sec * 2 if CFG.limit_order_ttl_sec else 600.0)
        if time.time() - rec["created"] > stale_after:
            mark_closed(rec, "orden de entrada no encontrada", "WARNING")
        return

    filled = str(order.get("tradeQty") or "0")
    status = str(order.get("status") or "").upper()
    if filled != rec.get("filled"):
        STATE.update(rec["id"], filled=filled)
        rec["filled"] = filled

    if status in ("INIT", "NEW"):
        ttl = CFG.limit_order_ttl_sec
        if CFG.entry_order_type == "LIMIT" and ttl and time.time() - rec["created"] > ttl:
            try:
                EX.cancel_order(rec["symbol"], order_id=rec.get("order_id"), client_id=rec["id"])
                mark_closed(rec, f"entrada sin fill, cancelada por TTL de {ttl}s")
            except BitunixError as exc:
                log("WARNING", f"{rec['symbol']}: no se pudo cancelar la entrada: {exc}")
        return

    if status in ("FILLED", "PART_FILLED", "CANCELED") and dec(filled) > 0:
        if try_protect(rec) == "waiting":
            log("WARNING", f"{rec['symbol']}: orden con fill {filled} pero no veo la posición; "
                           f"reintento en el próximo tick")
        return

    if status == "CANCELED":
        mark_closed(rec, "entrada cancelada sin fill")


def check_open_position(rec: dict) -> None:
    """Vigila la posición abierta: SL/TP pendientes, break-even y cierre."""
    position_id = rec.get("position_id")
    if not position_id:
        mark_closed(rec, "sin positionId", "WARNING")
        return

    try:
        pos = EX.position(str(position_id))
    except BitunixError as exc:
        log("WARNING", f"{rec['symbol']}: error consultando la posición: {exc}")
        return

    if pos is None:
        cancel_leftover_tp(rec)
        mark_closed(rec, "posición cerrada en el exchange")
        return

    ensure_position_sl(rec)
    ensure_partial_tps(rec)

    if not CFG.be_enabled or rec.get("be_done"):
        return

    idx = max(0, min(CFG.be_after_tp, len(rec["tps"])) - 1)
    trigger = float(rec["tps"][idx])
    trigger_str = fmt_price(trigger, int(rec["price_places"]))
    places = int(rec["price_places"])
    reason = None

    if CFG.be_trigger in ("fill", "both") and CFG.tp_mode == "partials":
        try:
            pending = EX.pending_tp_sl(rec["symbol"], str(position_id))
        except BitunixError as exc:
            log("WARNING", f"{rec['symbol']}: no pude leer los TP pendientes: {exc}")
            pending = None
        # Lista vacía = datos fiables a medias; mejor esperar al siguiente tick.
        if pending is not None and (pending or any(m.get("filled") for m in
                                                  (rec.get("tp_orders") or {}).values())):
            live = {fmt_price(row.get("tpPrice") or 0, places) for row in pending}
            if trigger_str not in live:
                reason = f"objetivo 1 ({trigger_str}) ejecutado"
                orders = {k: ({**v, "filled": True} if k == trigger_str else v)
                          for k, v in (rec.get("tp_orders") or {}).items()}
                STATE.update(rec["id"], tp_orders=orders)
                rec["tp_orders"] = orders

    if reason is None and CFG.be_trigger in ("price", "both"):
        last = EX.last_price(rec["symbol"])
        if last:
            crossed = last <= trigger if rec["side"] == "SHORT" else last >= trigger
            if crossed:
                reason = f"precio {last} en el objetivo 1 ({trigger_str})"

    if reason:
        do_break_even(rec, reason)


def simulate_lifecycle(rec: dict) -> None:
    """En seco recorre el ciclo entero en local: fill, SL, TP parciales, break-even y cierre.

    Sirve para probar dimensionado y avisos sin claves y sin dinero real; nada de esto
    toca el exchange. Cada fase dura DRY_RUN_PHASE_SEC segundos.
    """
    step = max(1.0, CFG.dry_run_phase_sec)
    t0 = float(rec.get("sim_start") or rec["created"])
    phase = int((time.time() - t0) // step)

    if rec["status"] in ("DRY_RUN", "WAITING_FILL"):     # fase 1: la entrada llena
        if phase < 1:
            return
        pid = "SIM" + str(rec["id"]).replace("-", "")[:10]
        STATE.update(rec["id"], status="OPEN", sim=True, sim_start=t0, position_id=pid,
                     filled=rec["qty"], note="dry-run: simulacro, nada llegó al exchange")
        rec.update(status="OPEN", sim=True, position_id=pid, filled=rec["qty"])
        STATE.bump("fills")
        log("DRY", f"{rec['symbol']}: entrada simulada ejecutada @ {rec['entry_price']} "
                   f"(qty {rec['qty']}, posición simulada {pid})")
        return

    orders = dict(rec.get("tp_orders") or {})
    if not orders:
        plan = plan_partial_tps(rec)
        orders = {price: {"order_id": f"SIMTP{idx}", "qty": qty, "filled": False}
                  for idx, (price, qty) in enumerate(plan.items(), 1)}
        STATE.update(rec["id"], tp_orders=orders, sl_order_id="SIMSL" + str(rec["id"])[:8])
        rec["tp_orders"] = orders
        log("DRY", f"{rec['symbol']}: SL en {rec['sl']} y {len(orders)} objetivos parciales "
                   f"colocados (simulado)")
        return

    keys = list(orders.keys())                      # en el orden de la señal, como en vivo
    done = sum(1 for m in orders.values() if m.get("filled"))
    if done >= len(keys):
        mark_closed(rec, "dry-run: todos los objetivos ejecutados")
        return
    if phase < 2 + done:
        return

    key = keys[done]
    orders[key] = {**orders[key], "filled": True}
    STATE.update(rec["id"], tp_orders=orders)
    rec["tp_orders"] = orders
    STATE.bump("fills")
    dirn = -1.0 if rec["side"] == "SHORT" else 1.0
    _dry_pnl(float(orders[key]["qty"]) * (float(key) - float(rec["entry_price"])) * dirn)
    log("DRY", f"{rec['symbol']}: objetivo {done + 1} ({key}) ejecutado (simulado)")
    if CFG.be_enabled and not rec.get("be_done") and done + 1 >= max(1, min(CFG.be_after_tp, len(keys))):
        price = be_price(rec["side"], float(rec["entry_price"]), int(rec["price_places"]))
        STATE.update(rec["id"], be_done=True, be_price=float(price),
                     note=f"dry-run: BE por objetivo {done + 1}")
        STATE.bump("be")
        log("DRY", f"{rec['symbol']}: BREAK-EVEN (simulado) -> SL en {price}")


_SIM_NOTE = {"ts": 0.0}


def monitor_once() -> None:
    if CFG.dry_run and CFG.dry_run_sim:
        for rec in STATE.simulating():
            try:
                simulate_lifecycle(rec)
            except Exception as exc:                        # la simulación nunca tumba el monitor
                STATE.bump("errors")
                log("ERROR", f"simulación dry-run: {exc!r}")

    live = [rec for rec in STATE.active() if not rec.get("sim")]
    watch = [] if CFG.dry_run else live           # en seco no se toca el exchange ni para vigilar
    if CFG.dry_run and live and time.time() - _SIM_NOTE["ts"] > 300:
        _SIM_NOTE["ts"] = time.time()
        log("WARNING", f"{len(live)} operación(es) abiertas en modo dry-run: sin I/O contra el "
                       f"exchange no se pueden vigilar (manda una señal nueva y se simula el ciclo)")

    for rec in watch:
        try:
            if rec["status"] == "WAITING_FILL":
                check_waiting_fill(rec)
            elif rec["status"] == "OPEN":
                check_open_position(rec)
        except BitunixError as exc:
            STATE.bump("errors")
            log("ERROR", f"monitor {rec.get('symbol')}: {exc}")
        except Exception as exc:                        # nunca tumbar el hilo del monitor
            STATE.bump("errors")
            log("ERROR", f"monitor inesperado: {exc!r}")
    STATE.prune()


def sample_metrics(force: bool = False) -> None:
    """Guarda una muestra de cuenta para los gráficos; el equity real, como mucho, por minuto."""
    global _SAMPLE
    ts = time.time()
    if not force and ts - _SAMPLE["ts"] < max(5.0, CFG.poll_interval_sec):
        return
    if force or ts - _SAMPLE["eq_ts"] >= 60:
        equity, available, error = account_snapshot()
        if error is None:
            _SAMPLE.update(equity=equity, available=available, eq_ts=ts)
    _SAMPLE["ts"] = ts
    STATE.sample(ts, _SAMPLE["equity"], _SAMPLE["available"], committed_margin(), len(STATE.active()))


_SAMPLE = {"ts": 0.0, "eq_ts": 0.0, "equity": None, "available": None}


def monitor_loop(stop: threading.Event) -> None:
    log("INFO", "monitor iniciado")
    while not stop.is_set():
        monitor_once()
        try:
            sample_metrics()
        except Exception as exc:
            log("DEBUG", f"muestreo de métricas falló: {exc!r}")
        stop.wait(max(1.0, CFG.poll_interval_sec))
    log("INFO", "monitor detenido")


# --------------------------------------------------------------------------------------
# Servidor webhook
# --------------------------------------------------------------------------------------

try:
    from flask import Flask, Response, request
except ImportError:                                        # pragma: no cover
    Flask = None                                           # type: ignore[assignment]

def authorize(token: str | None, headers: dict[str, str], query: dict[str, str], body: str) -> bool:
    """Token en la ruta, headers, query o Authorization: Bearer, más frase opcional."""
    if CFG.webhook_keyword and CFG.webhook_keyword not in body:
        return False
    if not CFG.webhook_token:
        return True
    lowered = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    bearer = lowered.get("authorization", "").replace("Bearer ", "").strip()
    provided = [token, lowered.get("x-webhook-token"), lowered.get("x-token"),
                (query or {}).get("token"), bearer]
    return any(v and hmac.compare_digest(str(v), CFG.webhook_token) for v in provided)


def handle_webhook(body: str, remote: str = "?", token: str | None = None,
                   headers: dict | None = None, query: dict | None = None) -> tuple[int, dict]:
    """Núcleo común para el servidor Flask y el de la librería estándar."""
    if len(body.encode("utf-8", "replace")) > CFG.max_body_kb * 1024:
        return 413, {"ok": False, "error": "body demasiado grande"}
    if not authorize(token, headers or {}, query or {}, body):
        return 401, {"ok": False, "error": "no autorizado"}
    log("INFO", f"webhook {len(body)}B desde {remote}")
    try:
        result = handle_signal(body, "webhook")
        if result.get("ok"):
            try:
                sample_metrics(force=True)          # que el gráfico refleje la operación ya
            except Exception:
                pass
    except BitunixError as exc:
        STATE.bump("errors")
        log("ERROR", f"error de Bitunix procesando la señal: {exc}")
        return 502, {"ok": False, "error": f"exchange: {exc.msg}"}
    except Exception as exc:                                   # noqa: BLE001
        STATE.bump("errors")
        log("ERROR", f"error inesperado procesando la señal: {exc!r}")
        return 500, {"ok": False, "error": "error interno"}
    return (200 if result.get("ok") else 400), result


def health_payload() -> dict:
    return {"ok": True, "active": len(STATE.active()), "records": len(STATE.positions),
            "events": len(EVENTS), "dry_run": CFG.dry_run, "time": time.time()}


def account_snapshot() -> tuple[float | None, float | None, str | None]:
    """(equity, disponible, error): en dry-run cae en el equity simulado en vez de fallar."""
    if CFG.dry_run and not (CFG.api_key and CFG.api_secret):
        base = _dry_equity()
        return base, base, None
    try:
        equity, available = EX.equity()
        return equity, available, None
    except BitunixError as exc:
        if not CFG.dry_run:
            return None, None, str(exc)
        base = _dry_equity()
        return base, base, None


def _dry_equity() -> float:
    """Equity del simulacro: arranca en la última muestra guardada y va moviéndose con el PnL simulado."""
    global _DRY_EQUITY
    if _DRY_EQUITY is None:
        with STATE.lock:
            last = STATE.metrics[-1].get("equity") if STATE.metrics else None
        _DRY_EQUITY = float(last if last is not None else CFG.dry_run_equity)
    return _DRY_EQUITY


def _dry_pnl(delta: float) -> None:
    """Apunta una ganancia/pérdida simulada en el equity del simulacro."""
    global _DRY_EQUITY
    _DRY_EQUITY = _dry_equity() + float(delta)


_DRY_EQUITY: float | None = None


def status_payload() -> dict:
    equity, available, error = account_snapshot()
    return {
        "ok": True, "equity": equity, "available": available, "error": error,
        "position_mode": EX.position_mode,
        "config": {
            "position_size_pct": CFG.position_size_pct, "sizing_mode": CFG.sizing_mode,
            "max_total_margin_pct": CFG.max_total_margin_pct,
            "default_leverage": CFG.default_leverage, "max_leverage": CFG.max_leverage,
            "margin_mode": CFG.margin_mode, "entry_order_type": CFG.entry_order_type,
            "entry_time_in_force": CFG.entry_time_in_force,
            "limit_order_ttl_sec": CFG.limit_order_ttl_sec, "tp_mode": CFG.tp_mode,
            "tp_split": CFG.tp_split, "be_enabled": CFG.be_enabled,
            "be_after_tp": CFG.be_after_tp, "be_offset_pct": CFG.be_offset_pct,
            "max_open_positions": CFG.max_open_positions,
            "dedupe_window_sec": CFG.dedupe_window_sec,
            "dry_run": CFG.dry_run,
        },
        "margin_in_use": round(committed_margin(), 4),
        "counters": STATE.counts(), "lasts": STATE.lasts(), "samples": len(STATE.metrics),
        "uptime_sec": round(time.time() - START_TS),
        "managed": STATE.active(), "recent": STATE.positions[-15:],
    }


def events_payload(after: int = 0, limit: int = 120) -> dict:
    with _LOG_LOCK:
        rows = [e for e in EVENTS if e["id"] > after][-max(1, min(limit, 400)):]
    return {"ok": True, "events": rows,
            "cursor": rows[-1]["id"] if rows else after,
            "server_time": time.time()}


def cancel_action(payload: dict) -> tuple[int, dict]:
    """Cancela la orden de entrada de una operación gestionada (id o symbol)."""
    wanted = str(payload.get("id") or payload.get("symbol") or "").strip()
    if not wanted:
        return 400, {"ok": False, "error": "falta id o symbol"}
    for rec in STATE.active():
        if wanted not in (str(rec.get("id")), str(rec.get("symbol"))):
            continue
        if rec["status"] != "WAITING_FILL":
            return 409, {"ok": False, "error": f"{rec['symbol']}: la posición ya está abierta, "
                                               f"ciérrala desde el exchange"}
        try:
            EX.cancel_order(rec["symbol"], order_id=rec.get("order_id"), client_id=rec["id"])
        except BitunixError as exc:
            return 502, {"ok": False, "error": f"exchange: {exc.msg}"}
        STATE.update(rec["id"], status="CANCELLED", note="cancelada via /api/cancel")
        log("INFO", f"{rec['symbol']}: entrada cancelada via /api/cancel")
        return 200, {"ok": True, "cancelled": rec["id"]}
    return 404, {"ok": False, "error": f"{wanted} no está entre las operaciones activas"}


def http_dispatch(method: str, path: str, headers: dict, query: dict, body: str,
                  remote: str = "?", token: str | None = None) -> tuple[int, Any, str]:
    """Una sola tabla de rutas para Flask y para el servidor de la librería estándar."""
    base = CFG.webhook_path.rstrip("/")
    if method == "POST":
        if path == base or path.startswith(base + "/"):
            status, payload = handle_webhook(body, remote=remote, token=token,
                                            headers=headers, query=query)
            return status, payload, "application/json"
        if path == "/api/cancel":
            if not authorize(None, headers, query, body):
                return 401, {"ok": False, "error": "no autorizado"}, "application/json"
            try:
                payload = json.loads(body or "{}")
            except json.JSONDecodeError:
                return 400, {"ok": False, "error": "body json inválido"}, "application/json"
            status, reply = cancel_action(payload if isinstance(payload, dict) else {})
            return status, reply, "application/json"
        return 404, {"ok": False, "error": f"ruta {path} inexistente"}, "application/json"

    if path == "/health":
        return 200, health_payload(), "application/json"
        return 200, health_payload(), "application/json"
    if path in ("/status", "/events"):
        if not authorize(None, headers, query, ""):
            return 401, {"ok": False, "error": "no autorizado"}, "application/json"
        if path == "/status":
            return 200, status_payload(), "application/json"
        try:
            after = int(query.get("after") or 0)
        except (TypeError, ValueError):
            after = 0
        return 200, events_payload(after), "application/json"
    return 404, {"ok": False, "error": f"ruta {path} inexistente"}, "application/json"


def build_app():
    app = Flask("proxy")
    base = CFG.webhook_path.rstrip("/")

    def endpoint(token: str | None = None):
        status, payload, ctype = http_dispatch(
            request.method, (request.path or "/").rstrip("/") or "/",
            dict(request.headers.items()), request.args.to_dict(),
            request.get_data(cache=True, as_text=True) or "",
            remote=str(request.remote_addr), token=token)
        if ctype.startswith("application/json"):
            payload = json.dumps(payload, ensure_ascii=False)
        return Response(payload, status=status, headers={"Content-Type": ctype})

    @app.route(base, methods=["POST"])
    def webhook():
        return endpoint()

    @app.route(f"{base}/<token>", methods=["POST"])
    def webhook_with_token(token: str):
        return endpoint(token)

    @app.route("/<path:rest>", methods=["GET", "POST"])
    @app.route("/", defaults={"rest": ""}, methods=["GET", "POST"])
    def everything(rest: str):
        return endpoint()

    return app


class StdHTTPServer(ThreadingHTTPServer):
    """Servidor de respaldo para correr el proxy sin Flask (solo stdlib + requests)."""

    daemon_threads = True

    def serve_forever_logging(self) -> None:
        log("INFO", f"escuchando en http://{self.server_address[0]}:{self.server_address[1]} "
                    f"(stdlib, sin flask) — webhook en {CFG.webhook_path}")
        super().serve_forever()


class StdHTTPHandler(BaseHTTPRequestHandler):
    """Adapter entre http.server y el núcleo del proxy."""

    server_version = "proxy/1.0"
    protocol_version = "HTTP/1.1"

    def _reply(self, status: int, payload, ctype: str) -> None:
        if isinstance(payload, str):
            data = payload.encode("utf-8")
        elif isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        else:
            data, ctype = json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json"
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        path = (parsed.path or "/").rstrip("/") or "/"
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length > 0 else ""
        base = CFG.webhook_path.rstrip("/")
        token = path[len(base) + 1:].strip("/") if path.startswith(base + "/") else None
        status, payload, ctype = http_dispatch(
            method, path, dict(self.headers.items()), query, body,
            remote=self.client_address[0], token=token)
        self._reply(status, payload, ctype)

    def do_POST(self) -> None:
        self._handle("POST")

    def do_GET(self) -> None:
        self._handle("GET")

    def log_message(self, fmt: str, *args) -> None:
        log("DEBUG", f"http {self.address_string()} {fmt % args}")


def run_server() -> None:
    if not CFG.api_key or not CFG.api_secret:
        if CFG.dry_run:
            log("WARNING", "sin API key (modo dry-run)")
        else:
            raise SystemExit("Define BITUNIX_API_KEY y BITUNIX_API_SECRET en .env (o usa DRY_RUN=true)")

    tiene_claves = bool(CFG.api_key and CFG.api_secret)
    if not tiene_claves:
        log("INFO", "dry-run sin claves: no llamo a ningún endpoint privado")
    else:
        try:
            EX.account()
            log("INFO", f"conectado a Bitunix ({CFG.base_url}) moneda={CFG.margin_coin} "
                        f"modo de posición={EX.position_mode}")
        except BitunixError as exc:
            log("ERROR", f"no pude validar credenciales: {exc}")
            if not CFG.dry_run:
                raise SystemExit(1)

    threading.Thread(target=monitor_loop, args=(threading.Event(),), daemon=True).start()
    try:
        sample_metrics(force=True)
    except Exception as exc:
        log("DEBUG", f"primera muestra de métricas falló: {exc!r}")
    log("INFO", f"config: size={CFG.position_size_pct}% ({CFG.sizing_mode}) "
                f"lev=x{CFG.default_leverage} tope margen total={CFG.max_total_margin_pct}% "
                f"margen={CFG.margin_mode} entrada={CFG.entry_order_type} TP={CFG.tp_mode} "
                f"BE={'sí' if CFG.be_enabled else 'no'} offset={CFG.be_offset_pct}% "
                f"dry_run={CFG.dry_run}")
    if Flask is not None:
        app = build_app()
        try:
            from waitress import serve
            log("INFO", f"escuchando en http://{CFG.host}:{CFG.port} (waitress) — webhook en {CFG.webhook_path}")
            serve(app, host=CFG.host, port=CFG.port, threads=8)
            return
        except ImportError:
            log("WARNING", "waitress no instalado; uso el servidor de desarrollo de Flask")
            app.run(host=CFG.host, port=CFG.port, threaded=True)
            return

    log("WARNING", "Flask no instalado; uso el servidor HTTP de la librería estándar")
    StdHTTPServer((CFG.host, CFG.port), StdHTTPHandler).serve_forever_logging()


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------

SAMPLE = """#CHZ/USDT - Short 🔴

Entrada: 0.00883
Stop Loss: 0.0090066

🎯 Objetivo 1: 0.00873336
🎯 Objetivo 2: 0.00871886
🎯 Objetivo 3: 0.00870219
🎯 Objetivo 4: 0.00868302
🎯 Objetivo 5: 0.00866097

Apalancamiento: x20
"""


def selftest() -> int:
    checks: list[tuple[bool, str]] = []
    sig = parse_signal(SAMPLE)
    print("== señal de ejemplo ==")
    print(json.dumps(asdict(sig), ensure_ascii=False, indent=2))

    checks += [
        (sig.symbol == "CHZUSDT", "par CHZUSDT"),
        (sig.side == "SHORT", "lado SHORT"),
        (sig.entry == 0.00883, "entrada 0.00883"),
        (sig.sl == 0.0090066, "SL 0.0090066"),
        (len(sig.tps) == 5, "5 objetivos"),
        (sig.tps[0] == 0.00873336, "TP1 = 0.00873336 (el primero en ejecutarse)"),
        (sig.tps[-1] == 0.00866097, "TP5 = 0.00866097"),
        (sig.leverage == 20, "apalancamiento x20"),
    ]

    lng = parse_signal("BTC/USDT Long 🟢\nEntrada: 100000\nSL: 98000\n"
                       "Objetivo 1: 101000\nObjetivo 2: 102000\nApalancamiento: x10")
    checks += [(lng.side == "LONG", "detecta LONG"),
               (lng.tps == [101000.0, 102000.0], "TPs ordenados ascendente")]

    pegado = parse_signal("AVAXUSDT LONG\nEntrada 24.42\nStop Loss: 23.68\n"
                          "TP1: 25.10\nTP 2: 25.62\n🎯 TP#3 = 26.35\nx20")
    checks += [(pegado.tps == [25.10, 25.62, 26.35], "TP1 / TP 2 / TP#3 sin espacio también valen"),
               (pegado.sl == 23.68, "SL con `Stop Loss:` sin dos puntos después del número")]

    checks += [(parse_signal(json.dumps({"text": SAMPLE})).symbol == "CHZUSDT", "JSON campo text"),
               (parse_signal(json.dumps({"alert": SAMPLE})).symbol == "CHZUSDT", "JSON campo alert"),
               (parse_signal(json.dumps({"symbol": "CHZUSDT", "side": "short", "entry": 0.00883,
                                         "sl": 0.0090066, "tps": [0.00873336]})).side == "SHORT",
                "JSON estructurado")]

    for bad, label in [("XUSDT Long\nEntrada: 10\nSL: 11\nTP 1: 12", "rechaza LONG con SL arriba"),
                       ("XUSDT Short\nEntrada: 10\nSL: 9\nTP 1: 8", "rechaza SHORT con SL abajo"),
                       ("hola mundo", "rechaza señal sin par"),
                       ("#AAA/USDT Long\nSL: 9\nTP 1: 12", "rechaza señal sin entrada")]:
        try:
            parse_signal(bad)
            checks += [(False, label)]
        except SignalError:
            checks += [(True, label)]

    print("\n== reparto de cantidades ==")
    a = split_qty("12.5", 5, 1, "0.1")
    bparts = split_qty("0.45", 5, 1, "0.1")
    cparts = split_qty("1000", 5, 0, "1", [20, 20, 20, 20, 20])
    print(" 12.5/5 ->", a)
    print(" 0.45/5 (migas) ->", bparts)
    print(" 1000/5 pesos ->", cparts)
    checks += [
        (sum(dec(x) for x in a) == dec("12.5") and len(a) == 5, "reparto igual conserva el total"),
        (bparts == ["0.45"], "migas inválidas -> un TP con todo"),
        (sum(dec(x) for x in cparts) == dec("1000"), "reparto por pesos conserva el total"),
    ]

    print("\n== break-even (offset %.3f%%) ==" % CFG.be_offset_pct)
    print(" SHORT 100 ->", be_price("SHORT", 100.0, 2))
    print(" LONG  100 ->", be_price("LONG", 100.0, 2))
    checks += [(be_price("SHORT", 100.0, 2) == "99.9", "BE SHORT por debajo de la entrada"),
               (be_price("LONG", 100.0, 2) == "100.1", "BE LONG por encima de la entrada")]

    print("\n== formato de precios ==")
    checks += [(fmt_price(0.00883, 5) == "0.00883", "precio 5 decimales"),
               (fmt_qty(11.864, 1) == "11.8", "cantidad truncada hacia abajo")]
    print(" ", fmt_price(0.00873336, 5), fmt_qty(11.864, 1))

    print("\n== cancelar una orden ==")
    stub, calls = Bitunix(CFG), []
    stub._call = lambda method, path, **k: calls.append((path, k.get("body"))) or {"successList": [], "failureList": []}
    stub.cancel_order("HYPEUSDT", order_id="2124945915042959360")
    print(" ", calls)
    checks += [(calls and calls[0] == ("/api/v1/futures/trade/cancel_orders",
                                       {"symbol": "HYPEUSDT", "orderList": [{"orderId": "2124945915042959360"}]}),
                "cancelar usa POST /trade/cancel_orders con orderList (/trade/cancel_order da 404)")]
    stub._call = lambda *a, **k: {"successList": [], "failureList": [{"orderId": "1", "errorCode": 10013,
                                                                      "errorMsg": "Order status error"}]}
    try:
        stub.cancel_order("HYPEUSDT", order_id="1")
        rechazo = False
    except BitunixError:
        rechazo = True
    checks += [(rechazo, "cancelación rechazada en failureList -> BitunixError")]

    print()
    for passed, label in checks:
        print(("  OK    " if passed else "  FALLA ") + label)
    ok = all(passed for passed, _ in checks)
    print("\n" + ("selftest OK" if ok else "selftest FALLÓ"))
    return 0 if ok else 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="proxy: webhook de señales -> Bitunix futures")
    parser.add_argument("command", nargs="?", default="serve",
                        choices=["serve", "selftest", "parse", "status", "config"])
    parser.add_argument("arg", nargs="?", help="archivo con la señal (para 'parse')")
    parser.add_argument("--dry-run", action="store_true", help="no envía órdenes reales")
    parser.add_argument("--port", type=int, help="puerto del servidor")
    args = parser.parse_args(argv)

    CFG.dry_run = CFG.dry_run or args.dry_run
    if args.port:
        CFG.port = args.port

    if args.command == "selftest":
        return selftest()
    if args.command == "parse":
        raw = Path(args.arg).read_text(encoding="utf-8") if args.arg else sys.stdin.read()
        print(json.dumps(asdict(parse_signal(raw)), ensure_ascii=False, indent=2))
        return 0
    if args.command == "config":
        print(json.dumps({**asdict(CFG),
                          "api_key": "***" if CFG.api_key else "",
                          "api_secret": "***" if CFG.api_secret else ""},
                         ensure_ascii=False, indent=2))
        return 0
    if args.command == "status":
        print(json.dumps(requests.get(f"http://127.0.0.1:{CFG.port}/status", timeout=15).json(),
                         ensure_ascii=False, indent=2))
        return 0
    run_server()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
