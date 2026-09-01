import os
import json
import time
import hmac
import hashlib
import requests
import threading
import websocket
from datetime import datetime, timezone, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request as flask_request, jsonify, send_file

try:
    from hyperliquid.exchange import Exchange as HLExchange
    from hyperliquid.info    import Info    as HLInfo
    from hyperliquid.utils.signing import OrderType as HLOrderType, TriggerOrderType as HLTriggerOrderType
    from eth_account         import Account as EthAccount
    HL_SDK_AVAILABLE = True
except ImportError:
    HL_SDK_AVAILABLE = False
    print("[HL] SDK not installed — mirror disabled")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None
    print("[DB] psycopg2 not installed — trigger tracking disabled")

# ─────────────────────────────────────────────
# CONFIG — set these as environment variables
# ─────────────────────────────────────────────
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

SYMBOL                  = os.environ.get("SYMBOL", "BTCUSDT")
EARLY_THRESHOLD         = int(os.environ.get("EARLY_THRESHOLD", "3"))
WARN_THRESHOLD          = int(os.environ.get("WARN_THRESHOLD", "5"))
STOP_THRESHOLD_DEFAULT  = int(os.environ.get("STOP_THRESHOLD", "20"))
FIRST_LOSS_STOP_THRESHOLD = int(os.environ.get("FIRST_LOSS_STOP_THRESHOLD", "20"))  # tightened cap if trade #1 is a loss
LOSS_STREAK_LIMIT       = int(os.environ.get("LOSS_STREAK_LIMIT", "2"))
BALANCE_ALERT_PCT       = float(os.environ.get("BALANCE_ALERT_PCT", "30"))
FEE_ALERT_THRESHOLD     = float(os.environ.get("FEE_ALERT_THRESHOLD", "70"))   # USDT
REVENGE_WINDOW_MINS     = int(os.environ.get("REVENGE_WINDOW_MINS", "20"))      # minutes after a loss
OVERTRADE_GAP_MINS      = int(os.environ.get("OVERTRADE_GAP_MINS", "120"))      # warn if avg gap between trades drops below this
PEAK_DRAWDOWN_PCT       = float(os.environ.get("PEAK_DRAWDOWN_PCT", "25"))       # % drop from intraday peak
SAME_SIDE_LIMIT         = int(os.environ.get("SAME_SIDE_LIMIT", "2"))            # consecutive same-side losses
WEBHOOK_SECRET          = os.environ.get("WEBHOOK_SECRET", "")                   # optional auth token
WEBHOOK_PORT            = int(os.environ.get("WEBHOOK_PORT", "5000"))
READ_API_SECRET         = os.environ.get("READ_API_SECRET", "")                  # auth token for read-only GET endpoints (e.g. compounding history)
DAILY_LOSS_LIMIT        = float(os.environ.get("DAILY_LOSS_LIMIT", "400"))       # max daily loss in $
DAILY_LOSS_STREAK_LIMIT = int(os.environ.get("DAILY_LOSS_STREAK_LIMIT", "20"))    # losses today before hard block.
DAILY_LOSS_WARN_PCT     = float(os.environ.get("DAILY_LOSS_WARN_PCT", "80"))     # warn at this % of limit
DAILY_PNL_TARGET        = float(os.environ.get("DAILY_PNL_TARGET", "100"))       # daily net P&L target in USDT
PROFIT_LOCK_ENABLED     = os.environ.get("PROFIT_LOCK_ENABLED", "false").lower() == "true"
WEEKLY_PNL_TARGET       = float(os.environ.get("WEEKLY_PNL_TARGET", "500"))      # weekly net P&L target in USDT
ENTRY2_EXPIRY_HOURS     = float(os.environ.get("ENTRY2_EXPIRY_HOURS", "2"))      # auto-cancel Entry2 after N hours
POLL_INTERVAL           = int(os.environ.get("POLL_INTERVAL", "45"))             # seconds — backup loop only; real SL/TP orders on Binance are the primary protection
SLOW_REFRESH_INTERVAL   = int(os.environ.get("SLOW_REFRESH_INTERVAL", "60"))      # seconds — trade history/balance, changes rarely
ORDER_FILL_POLL_INTERVAL = int(os.environ.get("ORDER_FILL_POLL_INTERVAL", "25"))  # seconds — checks tracked order fills (Entry2/TP1/TP2)

# ── DAYSCORE integration — auto-ticks the "$60 profit day" criterion when DAILY_PNL_TARGET is hit ──
DAYSCORE_PB_URL            = os.environ.get("DAYSCORE_PB_URL", "https://pocketbase-production-2a23.up.railway.app")
DAYSCORE_PB_ADMIN_EMAIL    = os.environ.get("PB_ADMIN_EMAIL", "")
DAYSCORE_PB_ADMIN_PASSWORD = os.environ.get("PB_ADMIN_PASSWORD", "")
DAYSCORE_OWNER               = "nathan"
DAYSCORE_PROFIT_CRITERIA_ID  = 10   # must match the "$60 profit day" id in perfect_day_logger.html's state.criteria

# ── Trade execution ──────────────────────────────────────────────────────────
EXECUTION_ENABLED       = os.environ.get("EXECUTION_ENABLED", "false").lower() == "true"
TRADE_EXECUTION_PACING_SEC = float(os.environ.get("TRADE_EXECUTION_PACING_SEC", "1.5"))
# A single /long or /short can chain 10+ Binance calls (guards, position
# checks, order placement) in the space of a second when fired directly.
# Signal-prompt trades naturally avoid this because there's a human pause
# for the stress-check question between the light guard check and the
# actual heavy execution — manual commands have no equivalent pause, which
# is what caused repeated rate-limit bans. This adds a small deliberate
# gap between each Binance-touching step during execution, spreading the
# same total call count over several seconds instead of firing it all in
# one burst. Temporary measure until the websocket migration removes the
# need for this kind of pacing entirely.
def _pace_execution():
    time.sleep(TRADE_EXECUTION_PACING_SEC)
# Paused for now to test reliability of the composite/HTF guards on their own.
# Set to "true" in Railway env vars to re-enable the 7am bias question + bias suppression filter.
BIAS_FILTER_ENABLED     = os.environ.get("BIAS_FILTER_ENABLED", "false").lower() == "true"
TRADE_SIZE_PCT          = float(os.environ.get("TRADE_SIZE_PCT", "2"))           # % of balance per trade
STOP_PCT                = float(os.environ.get("STOP_PCT", "0.5"))               # SL distance from entry %
TP_PCT                  = float(os.environ.get("TP_PCT", "0.40"))                # TP1 distance from entry %
TP2_PCT                 = float(os.environ.get("TP2_PCT", "0.60"))               # TP2 distance from entry %
TP1_CLOSE_PCT           = float(os.environ.get("TP1_CLOSE_PCT", "50"))           # % of position to close at TP1
TP1_SL_MOVE_PCT         = float(os.environ.get("TP1_SL_MOVE_PCT", "0.15"))       # move SL to this % above entry after TP1
ENTRY2_OFFSET_PCT       = float(os.environ.get("ENTRY2_OFFSET_PCT", "0.12"))     # 2nd entry distance % below/above entry
ENTRY2_SIZE_PCT         = float(os.environ.get("ENTRY2_SIZE_PCT", "30"))         # 2nd entry size as % of total trade value
ENTRY2_SL_PCT           = float(os.environ.get("ENTRY2_SL_PCT", "0.3"))          # SL distance % from avg entry after Entry2 fills
LEVERAGE                = int(os.environ.get("LEVERAGE", "60"))                  # futures leverage
MAKER_ENTRY             = os.environ.get("MAKER_ENTRY", "true").lower() == "true"
MAKER_OFFSET_PCT        = float(os.environ.get("MAKER_OFFSET_PCT", "0.01"))
MAKER_TIMEOUT_SEC       = int(os.environ.get("MAKER_TIMEOUT_SEC", "120"))

# ── Hyperliquid Mirror — generalized multi-client registry ────────────────
# Client #3 onward: this was rebuilt from the earlier hl_/hl2_ duplicated
# pattern into a proper looped/config-list structure, since a real client #3
# arrived. New clients are added purely via CLIENT{n}_* env vars — no code
# changes needed. Client #1 and #2 keep working off their existing HL_/HL2_
# Railway vars (no need to rename anything already deployed); CLIENT1_*/
# CLIENT2_* env vars, if set, simply take precedence.
HL_BASE_URL             = "https://api.hyperliquid.xyz"
HL_COIN                 = "BTC"


class ClientConfig:
    def __init__(self, cid: str, private_key: str, address: str, leverage: int,
                 size_pct: float, tg_token: str, tg_chat_id: str):
        self.id          = cid            # e.g. "1", "2", "3" — used in logs/labels
        self.private_key = private_key
        self.address     = address
        self.leverage    = leverage
        self.size_pct    = size_pct
        self.tg_token    = tg_token
        self.tg_chat_id  = tg_chat_id
        # per-client runtime state (was a separate set of globals per client before)
        self.trades_today        = 0
        self.wins_today          = 0
        self.losses_today        = 0
        self.summary_sent_today  = False
        self.tg_last_update_id   = 0

    def is_configured(self) -> bool:
        return HL_SDK_AVAILABLE and bool(self.private_key) and bool(self.address)

    def label(self) -> str:
        return f"Client #{self.id}"


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _build_clients() -> list:
    clients = []

    # Client #1 — falls back to legacy HL_* vars already deployed on Railway
    c1_key  = _env("CLIENT1_PRIVATE_KEY")     or _env("HL_PRIVATE_KEY")
    c1_addr = _env("CLIENT1_ACCOUNT_ADDRESS") or _env("HL_ACCOUNT_ADDRESS")
    c1_lev  = int(_env("CLIENT1_LEVERAGE")    or _env("HL_LEVERAGE", "60"))
    c1_pct  = float(_env("CLIENT1_SIZE_PCT")  or _env("HL_SIZE_PCT", "70"))
    c1_tok  = _env("CLIENT1_TG_TOKEN")        or _env("HL_CLIENT_TG_TOKEN")
    c1_chat = _env("CLIENT1_TG_CHAT_ID")      or _env("HL_CLIENT_CHAT_ID")
    clients.append(ClientConfig("1", c1_key, c1_addr, c1_lev, c1_pct, c1_tok, c1_chat))

    # Client #2 — falls back to legacy HL2_* vars already deployed on Railway
    c2_key  = _env("CLIENT2_PRIVATE_KEY")     or _env("HL2_PRIVATE_KEY")
    c2_addr = _env("CLIENT2_ACCOUNT_ADDRESS") or _env("HL2_ACCOUNT_ADDRESS")
    c2_lev  = int(_env("CLIENT2_LEVERAGE")    or _env("HL2_LEVERAGE", "60"))
    c2_pct  = float(_env("CLIENT2_SIZE_PCT")  or _env("HL2_SIZE_PCT", "70"))
    c2_tok  = _env("CLIENT2_TG_TOKEN")        or _env("HL2_CLIENT_TG_TOKEN")
    c2_chat = _env("CLIENT2_TG_CHAT_ID")      or _env("HL2_CLIENT_CHAT_ID")
    clients.append(ClientConfig("2", c2_key, c2_addr, c2_lev, c2_pct, c2_tok, c2_chat))

    # Client #3 onward — pure CLIENT{n}_* vars, just keep adding without touching code
    n = 3
    while True:
        key  = _env(f"CLIENT{n}_PRIVATE_KEY")
        addr = _env(f"CLIENT{n}_ACCOUNT_ADDRESS")
        tok  = _env(f"CLIENT{n}_TG_TOKEN")
        chat = _env(f"CLIENT{n}_TG_CHAT_ID")
        # Stop scanning only once NONE of a client's fields are set — this lets
        # the Telegram side be wired up and tested before the wallet exists
        # (e.g. testing /mystats before agent wallet keys are ready), same as
        # clients #1/#2 which are always instantiated regardless of config state.
        if not key and not addr and not tok and not chat:
            break
        lev  = int(_env(f"CLIENT{n}_LEVERAGE", "60"))
        pct  = float(_env(f"CLIENT{n}_SIZE_PCT", "70"))
        clients.append(ClientConfig(str(n), key, addr, lev, pct, tok, chat))
        n += 1

    return clients


CLIENTS = _build_clients()  # populated at import time; HL_SDK_AVAILABLE must already be defined above this point
AUTO_TRADE_SIGNALS      = os.environ.get("AUTO_TRADE_SIGNALS", "").split(",")

# Cooldown window (UK time — auto-adjusts for GMT/BST)
COOLDOWN_START  = dt_time(14, 0)
COOLDOWN_END    = dt_time(15, 15)

def _parse_time(val: str, default: dt_time) -> dt_time:
    try:
        h, m = val.strip().split(":")
        return dt_time(int(h), int(m))
    except Exception:
        return default

_raw_cooldown_start = os.environ.get("COOLDOWN_START", "")
_raw_cooldown_end   = os.environ.get("COOLDOWN_END", "")
_raw_night_block    = os.environ.get("NIGHT_BLOCK_START", "")

if _raw_cooldown_start: COOLDOWN_START = _parse_time(_raw_cooldown_start, COOLDOWN_START)
if _raw_cooldown_end:   COOLDOWN_END   = _parse_time(_raw_cooldown_end,   COOLDOWN_END)
NIGHT_BLOCK_START = _parse_time(_raw_night_block, dt_time(22, 15))
UK_TZ           = ZoneInfo("Europe/London")

FUTURES_BASE    = "https://fapi.binance.com"
MAIN_BASE       = "https://api.binance.com"   # used for Universal Transfer (futures <-> spot)
DATABASE_URL    = os.environ.get("DATABASE_URL", "")
STATS_FILE      = "weekly_stats.json"

# ─────────────────────────────────────────────
# DAILY STATE
# ─────────────────────────────────────────────
early_warned_today          = False
warned_today                = False
stopped_today               = False
STOP_THRESHOLD              = STOP_THRESHOLD_DEFAULT   # can tighten to FIRST_LOSS_STOP_THRESHOLD if trade #1 is a loss
first_loss_tightened_today  = False
loss_streak_alerted         = False
balance_up_alerted          = False
balance_down_alerted        = False
fee_alerted_today           = False
eod_summary_sent_today      = False
morning_recap_sent_today    = False
last_cooldown_warned_count  = 0
last_reset_day              = None
snapshot_balance            = None
yesterday_stats             = None
peak_balance_today          = None
peak_drawdown_alerted       = False
same_side_alerted           = False
same_side_block: dict       = {}
daily_losses_block          = False   # set True when 3 losses hit — resets midnight
daily_losses_block_reset_at: float = 0.0  # timestamp of last midnight reset
last_loss_close_time: float = 0.0     # timestamp of last losing close — for revenge block
last_tp2_close_time: float = 0.0      # timestamp of last TP2 close — for post-win cooldown
maker_entry_cancel_requested = False  # set True via /cancel_entry to abort pending maker

current_zone                = "unknown"
current_composite           = None
current_htf_composite       = None
htf_color_last              = None   # tracks last known HTF color to detect changes
htf_bias                    = None   # "bullish" or "bearish" — set by 2H structure break alerts, gates A/B+/B- grading
htf_bias_updated_at         = None   # timestamp of last update, for staleness checks
EMA_GUARD_ENABLED           = os.environ.get("EMA_GUARD_ENABLED", "false").lower() == "true"
SIMPLE_EMA_GUARD_ENABLED    = os.environ.get("SIMPLE_EMA_GUARD_ENABLED", "false").lower() == "true"
WPR_GUARD_ENABLED           = os.environ.get("WPR_GUARD_ENABLED", "false").lower() == "true"
WPR_LONG_MAX                = float(os.environ.get("WPR_LONG_MAX", "-35"))   # standalone WPR guard: block longs if WPR above this
WPR_SHORT_MIN               = float(os.environ.get("WPR_SHORT_MIN", "-65"))  # standalone WPR guard: block shorts if WPR below this
WPR_GRADE_A_LONG_MAX        = float(os.environ.get("WPR_GRADE_A_LONG_MAX", "-65"))   # setup grading: Grade A threshold for longs — WPR must be at or below this
WPR_GRADE_A_SHORT_MIN       = float(os.environ.get("WPR_GRADE_A_SHORT_MIN", "-65"))   # setup grading: Grade A threshold for shorts — WPR must be at or above this
WPR_LONG_A_TO_NOMANS_LAND   = float(os.environ.get("WPR_LONG_A_TO_NOMANS_LAND", "-35"))  # below this (toward -65) is no-man's-land; above this (toward -18) is B- band
WPR_SHORT_A_TO_NOMANS_LAND  = float(os.environ.get("WPR_SHORT_A_TO_NOMANS_LAND", "-35"))  # mirrored
WPR_B_MINUS_LONG_MAX        = float(os.environ.get("WPR_B_MINUS_LONG_MAX", "-18"))   # B- Setup hard ceiling for longs — above this is still a full block, no exception
WPR_B_MINUS_SHORT_MIN       = float(os.environ.get("WPR_B_MINUS_SHORT_MIN", "-82"))  # B- Setup hard floor for shorts — below this is still a full block, no exception
HTF_WPR_SHORT_EXHAUSTION_MAX = float(os.environ.get("HTF_WPR_SHORT_EXHAUSTION_MAX", "-86"))  # if 2H WPR is more negative than this, HTF is too exhausted to short — overrides every short grade
HTF_WPR_LONG_EXHAUSTION_MIN  = float(os.environ.get("HTF_WPR_LONG_EXHAUSTION_MIN", "-14"))   # if 2H WPR is above this (closer to 0), HTF is too exhausted to long — mirrors the short-side gate
STALE_COMPOSITE_MAX_AGE_SEC = int(os.environ.get("STALE_COMPOSITE_MAX_AGE_SEC", "1200"))  # 20 min — B- Setup composite check fails safe if older than this
zone_last_updated           = None
composite_last_updated      = None
signal_approved             = {"long": None, "short": None}
last_trigger                = None
last_trigger_time           = None
last_trigger_direction      = None
last_short_trigger_time     = None
last_long_trigger_time      = None
pending_trigger_id          = None
TRIGGER_ATTRIBUTION_SECS    = 3600
revenge_trade_ids_alerted   = set()
overtrade_alerted           = False
pending_manual_trade         = None   # holds direction/grade while awaiting /long /short confirmation
pending_bias_reset           = None   # holds signal_key awaiting a bias-reset confirmation
elevated_silence_until       = 0.0    # timestamp until which prompts are silenced after an elevated-stress response

daily_target_alerted        = False
weekly_target_alerted       = False
daily_loss_warn_alerted     = False
daily_loss_stop_alerted     = False
dayscore_ticked_today       = False  # resets daily — guards against re-ticking after a manual close/reopen
profit_locked_today: float  = 0.0   # cumulative amount already moved to Spot today
profit_lock_last_attempt_at: float = 0.0   # timestamp of last transfer attempt (success OR failure) — prevents
                                            # retrying/re-alerting every single poll cycle when a transfer keeps
                                            # failing (e.g. missing API permission), which spammed the same
                                            # failure message every ~15s until manually caught
PROFIT_LOCK_RETRY_COOLDOWN_SEC = 1800  # 30 min between retry attempts after a failure
overnight_alerted_1015      = False
overnight_alerted_1045      = False
overnight_auto_breakeven_done = False

entry2_placed_at: float     = 0.0
position_mins_profit: float = 0.0
position_mins_under: float  = 0.0
underwater_ratio_alerted    = False  # one-shot per position, resets when position closes
UNDERWATER_RATIO_THRESHOLD  = 1.5    # alert when underwater time >= this multiple of profit time
trade_mfe: float            = 0.0   # max favorable excursion (furthest in-profit price move)
trade_mae: float            = 0.0   # max adverse excursion (furthest against-you price move)
trade_entry_time: float     = 0.0   # timestamp when trade opened (for time_to_tp1 calc)
retrace_protect_triggered   = False # whether auto-breakeven retracement protection has fired this trade

tracked_orders: dict = {}
current_trade_entry: dict = {}
indicator_trade_ids: set  = set()

win_streak_alerted_at       = 0
daily_streaks_3plus         = 0

rules_broken_today          = {}


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    if not DATABASE_URL:
        print("[DB] No DATABASE_URL set — trigger tracking disabled.")
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trigger_performance (
                id              SERIAL PRIMARY KEY,
                trade_date      DATE,
                trigger         TEXT,
                direction       TEXT,
                outcome         TEXT,
                pnl             FLOAT,
                entry_price     FLOAT,
                sl_price        FLOAT,
                tp1_price       FLOAT,
                tp2_price       FLOAT,
                mae             FLOAT,
                mfe             FLOAT,
                time_to_tp1_mins FLOAT,
                entry_time      TIMESTAMPTZ DEFAULT NOW(),
                close_time      TIMESTAMPTZ,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trigger_fires (
                id          SERIAL PRIMARY KEY,
                trigger     TEXT,
                fired_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stress_log (
                id              SERIAL PRIMARY KEY,
                logged_at       TIMESTAMPTZ DEFAULT NOW(),
                direction       TEXT,
                signal          TEXT,
                state           TEXT,
                traded          BOOLEAN,
                trigger_log_id  INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS invalidation_log (
                id          SERIAL PRIMARY KEY,
                logged_at   TIMESTAMPTZ DEFAULT NOW(),
                trigger     TEXT,
                direction   TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS warnings_log (
                id          SERIAL PRIMARY KEY,
                logged_at   TIMESTAMPTZ DEFAULT NOW(),
                category    TEXT,
                message     TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS compounding_targets (
                id                  SERIAL PRIMARY KEY,
                period_type         TEXT,          -- 'week' or 'cycle' (4-week)
                period_start        DATE,
                starting_balance    FLOAT,
                created_at          TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS persistent_state (
                key         TEXT PRIMARY KEY,
                value       TEXT,
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_balance_log (
                id          SERIAL PRIMARY KEY,
                log_date    DATE UNIQUE,
                balance     FLOAT,
                logged_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS locked_targets (
                id                  SERIAL PRIMARY KEY,
                week_start          DATE UNIQUE,
                weekly_multiplier   FLOAT,
                daily_multiplier    FLOAT,
                locked_at           TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS virtual_trigger_trades (
                id              SERIAL PRIMARY KEY,
                trigger         TEXT,
                direction       TEXT,
                entry_price     FLOAT,
                target_price    FLOAT,
                stop_price      FLOAT,
                outcome         TEXT,          -- 'win', 'loss', or NULL while still open
                exit_price      FLOAT,
                opened_at       TIMESTAMPTZ DEFAULT NOW(),
                closed_at       TIMESTAMPTZ
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS htf_bias_changes (
                id              SERIAL PRIMARY KEY,
                logged_at       TIMESTAMPTZ DEFAULT NOW(),
                new_bias        TEXT,
                triggered_by    TEXT,
                action          TEXT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Tables ready.")

        # ── Migration: add entry quality columns if not yet present ──────
        try:
            conn = get_db()
            cur  = conn.cursor()
            for col, coltype in [
                ("entry_price",      "FLOAT"),
                ("sl_price",         "FLOAT"),
                ("tp1_price",        "FLOAT"),
                ("tp2_price",        "FLOAT"),
                ("mae",              "FLOAT"),
                ("mfe",              "FLOAT"),
                ("time_to_tp1_mins", "FLOAT"),
                ("time_to_tp2_mins", "FLOAT"),
                ("time_to_sl_mins",  "FLOAT"),
            ]:
                cur.execute(f"""
                    ALTER TABLE trigger_performance
                    ADD COLUMN IF NOT EXISTS {col} {coltype}
                """)
            conn.commit()
            cur.close()
            conn.close()
            print("[DB] Entry quality columns ready.")
        except Exception as e:
            print(f"[DB] Migration warning: {e}")

        # ── Migration: add trigger_log_id to stress_log if not yet present ──
        try:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute("""
                ALTER TABLE stress_log
                ADD COLUMN IF NOT EXISTS trigger_log_id INTEGER
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("[DB] stress_log.trigger_log_id ready.")
        except Exception as e:
            print(f"[DB] Migration warning: {e}")

    except Exception as e:
        print(f"[DB] Init error: {e}")


def save_persistent_state(key: str, value):
    """
    Saves a value to the database so it survives a restart — used for
    things like current_composite, which previously only lived in memory
    and got wiped back to None every time the bot restarted (which has
    happened repeatedly from rate-limit bans), even though the last real
    TradingView alert value was still perfectly valid.
    """
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO persistent_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (key, str(value)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Persistent State] Save error for '{key}': {e}")


def load_persistent_state(key: str, max_age_sec: int = None):
    """
    Loads a previously saved value. If max_age_sec is given and the stored
    value is older than that, returns None instead — for values where a
    very stale restore would be worse than starting fresh (composite,
    for instance, shouldn't be trusted if it's a day old).
    Returns None if not found, too old, or on any error.
    """
    if not DATABASE_URL:
        return None
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT value, updated_at FROM persistent_state WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        if max_age_sec is not None:
            age = (datetime.now(timezone.utc) - row["updated_at"]).total_seconds()
            if age > max_age_sec:
                return None
        return row["value"]
    except Exception as e:
        print(f"[Persistent State] Load error for '{key}': {e}")
        return None


def get_hl_balance() -> float:
    """Fetch current HL account value for client #1. Not currently called anywhere,
    kept for a future TWRR/compounding dashboard feature."""
    try:
        if not CLIENTS or not CLIENTS[0].address:
            return None
        info  = hl_get_info()
        state = info.user_state(CLIENTS[0].address)
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as e:
        print(f"[Client TWRR] Balance fetch error: {e}")
        return None





def db_log_htf_bias_change(new_bias: str, triggered_by: str, action: str):
    """Log when HTF bias changes are detected and handled."""
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO htf_bias_changes (logged_at, new_bias, triggered_by, action)
            VALUES (NOW(), %s, %s, %s)
        """, (new_bias, triggered_by, action))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[HTF] Bias change logged: {new_bias} via {triggered_by} ({action})")
    except Exception as e:
        print(f"[HTF] Log error: {e}")


def db_log_stress(direction: str, signal: str, state: str, traded: bool):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO stress_log (direction, signal, state, traded)
            VALUES (%s, %s, %s, %s)
        """, (direction, signal, state, traded))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Stress log error: {e}")


def db_link_stress_to_trigger(direction: str, signal: str, trigger_log_id: int):
    """Link the most recent matching, unlinked stress_log entry to the trade it produced."""
    if not DATABASE_URL or trigger_log_id is None:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE stress_log
            SET trigger_log_id = %s
            WHERE id = (
                SELECT id FROM stress_log
                WHERE direction = %s AND signal = %s AND traded = TRUE AND trigger_log_id IS NULL
                ORDER BY logged_at DESC LIMIT 1
            )
        """, (trigger_log_id, direction, signal))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Stress link error: {e}")


def db_log_trigger_fire(trigger: str):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("INSERT INTO trigger_fires (trigger) VALUES (%s)", (trigger,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Log trigger fire error: {e}")


def db_log_invalidation(trigger: str, direction: str):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("INSERT INTO invalidation_log (trigger, direction) VALUES (%s, %s)", (trigger, direction))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Log invalidation error: {e}")


def db_log_warning(category: str, message: str):
    """Log a warning-type alert for later review via /warnings."""
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("INSERT INTO warnings_log (category, message) VALUES (%s, %s)", (category, message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Log warning error: {e}")


def db_was_warning_logged_today(category: str) -> bool:
    """
    Checks the DATABASE (not an in-memory flag) for whether a warning of this
    category has already been logged today (UK calendar day). Used so
    once-per-day alerts (like the daily loss block) don't re-fire after a
    Railway restart wipes in-memory flags — the flag disappears on restart,
    but the DB record of "I already told you this today" survives.
    """
    if not DATABASE_URL:
        return False
    try:
        today_start_uk = datetime.now(UK_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_uk.astimezone(timezone.utc)
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            "SELECT 1 FROM warnings_log WHERE category = %s AND logged_at >= %s LIMIT 1",
            (category, today_start_utc)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row is not None
    except Exception as e:
        print(f"[DB] Warning-today check error: {e}")
        return False  # fail open — if the DB check fails, don't block a real alert from firing


def db_was_message_logged_today(category: str, must_contain: str) -> bool:
    """
    Like db_was_warning_logged_today, but for cases where the same category
    can legitimately fire multiple times per day (e.g. revenge_trade) and we
    need to dedup on a specific identifier (e.g. a Binance order ID) rather
    than "any alert of this category today".
    """
    if not DATABASE_URL:
        return False
    try:
        today_start_uk = datetime.now(UK_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_uk.astimezone(timezone.utc)
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            "SELECT 1 FROM warnings_log WHERE category = %s AND message LIKE %s AND logged_at >= %s LIMIT 1",
            (category, f"%{must_contain}%", today_start_utc)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row is not None
    except Exception as e:
        print(f"[DB] Message-today check error: {e}")
        return False


def db_get_invalidation_stats() -> dict:
    """Returns {trigger: {invalidations, fires, rate_pct}} for every trigger that has fired or been invalidated."""
    if not DATABASE_URL:
        return {}
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT trigger, COUNT(*) as cnt FROM invalidation_log GROUP BY trigger")
        inv_counts = {r["trigger"]: r["cnt"] for r in cur.fetchall()}
        cur.execute("SELECT trigger, COUNT(*) as cnt FROM trigger_fires GROUP BY trigger")
        fire_counts = {r["trigger"]: r["cnt"] for r in cur.fetchall()}
        cur.close()
        conn.close()

        stats = {}
        all_triggers = set(inv_counts.keys()) | set(fire_counts.keys())
        for t in all_triggers:
            fires = fire_counts.get(t, 0)
            invs  = inv_counts.get(t, 0)
            rate  = (invs / fires * 100) if fires > 0 else None
            stats[t] = {"invalidations": invs, "fires": fires, "rate_pct": rate}
        return stats
    except Exception as e:
        print(f"[DB] Invalidation stats error: {e}")
        return {}


def db_get_avg_trigger_gaps() -> dict:
    if not DATABASE_URL:
        return {}
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT trigger, fired_at
            FROM trigger_fires
            ORDER BY trigger, fired_at
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        from collections import defaultdict
        fires = defaultdict(list)
        for r in rows:
            fires[r["trigger"]].append(r["fired_at"])

        avg_gaps = {}
        for trigger, times in fires.items():
            if len(times) < 2:
                avg_gaps[trigger] = None
            else:
                gaps = [(times[i] - times[i-1]).total_seconds() / 60 for i in range(1, len(times))]
                avg_gaps[trigger] = sum(gaps) / len(gaps)
        return avg_gaps
    except Exception as e:
        print(f"[DB] Avg trigger gaps error: {e}")
        return {}


def db_save_trigger(trigger: str, direction: str,
                    entry_price: float = None, sl_price: float = None,
                    tp1_price: float = None, tp2_price: float = None) -> int | None:
    if not DATABASE_URL:
        return None
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO trigger_performance
                (trade_date, trigger, direction, outcome, pnl,
                 entry_price, sl_price, tp1_price, tp2_price)
            VALUES (%s, %s, %s, 'open', 0, %s, %s, %s, %s)
            RETURNING id
        """, (datetime.now(UK_TZ).date(), trigger, direction,
              entry_price, sl_price, tp1_price, tp2_price))
        row_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return row_id
    except Exception as e:
        print(f"[DB] Save trigger error: {e}")
        return None


def db_update_outcome(row_id: int, outcome: str, pnl: float,
                      mae: float = None, mfe: float = None,
                      time_to_tp1_mins: float = None,
                      time_to_tp2_mins: float = None,
                      time_to_sl_mins: float = None):
    if not DATABASE_URL or not row_id:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE trigger_performance
            SET outcome = %s, pnl = %s, close_time = NOW(),
                mae = %s, mfe = %s,
                time_to_tp1_mins = %s,
                time_to_tp2_mins = %s,
                time_to_sl_mins  = %s
            WHERE id = %s
        """, (outcome, pnl, mae, mfe,
              time_to_tp1_mins, time_to_tp2_mins, time_to_sl_mins,
              row_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Update outcome error: {e}")


def db_get_trigger_performance() -> list:
    if not DATABASE_URL:
        return []
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT trigger, direction, outcome, pnl
            FROM trigger_performance
            WHERE outcome IN ('win', 'loss')
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[DB] Get trigger performance error: {e}")
        return []


def is_rate_limit_error(exc: Exception) -> bool:
    """Detects Binance 418 (IP auto-banned) or 429 (rate limit warning) responses."""
    return isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None and exc.response.status_code in (418, 429)


def sign_request(params: dict) -> dict:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    signature = hmac.new(
        BINANCE_API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()
    params["signature"] = signature
    return params


def get_futures_trades(start_ms: int, end_ms: int = None) -> list:
    now_ms  = int(time.time() * 1000)
    params  = {
        "symbol":    SYMBOL,
        "startTime": start_ms,
        "endTime":   end_ms or now_ms,
        "limit":     1000,
        "timestamp": now_ms,
    }
    params = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.get(f"{FUTURES_BASE}/fapi/v1/userTrades",
                        params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_futures_trades_today() -> list:
    # Use UK local midnight as the day boundary — matches check_and_reset()
    # and every other "today" calculation in the bot. Using UTC midnight here
    # created a ~1hr window each night (UTC midnight to UK midnight, or the
    # reverse depending on BST/GMT) where this function and the reset logic
    # disagreed on which day it was, causing yesterday's trades to still
    # count as "today" right after the flags had already reset.
    now_uk = datetime.now(UK_TZ)
    today_start_uk = now_uk.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start_uk.astimezone(timezone.utc)
    return get_futures_trades(int(today_start_utc.timestamp() * 1000))


_slow_refresh_cache = {"trades": None, "balance": None, "fetched_at": 0}
_last_slow_checks_run = 0.0  # gates WPR/HTF flip/aggressive volume/virtual trigger checks to SLOW_REFRESH_INTERVAL

def get_slow_refresh_data() -> tuple:
    """
    Cached wrapper around get_futures_trades_today() + get_usdt_balance(),
    used ONLY by the main polling loop. Trade history and balance genuinely
    don't change meaningfully within a SLOW_REFRESH_INTERVAL window — real
    fills only land occasionally, so hitting Binance for this every single
    fast-loop tick was pure waste. On-demand commands (/stats, /experiment
    etc.) still call the underlying functions directly for fresh data —
    this cache only affects the background loop's own bookkeeping.
    Returns (trades, balance).
    """
    now = time.time()
    if _slow_refresh_cache["trades"] is not None and (now - _slow_refresh_cache["fetched_at"]) < SLOW_REFRESH_INTERVAL:
        return _slow_refresh_cache["trades"], _slow_refresh_cache["balance"]
    trades = get_futures_trades_today()
    try:
        balance = get_usdt_balance()
    except Exception as e:
        print(f"[Slow Refresh] Balance fetch error: {e}")
        balance = _slow_refresh_cache["balance"]  # reuse last known balance rather than crash
    _slow_refresh_cache["trades"]     = trades
    _slow_refresh_cache["balance"]    = balance
    _slow_refresh_cache["fetched_at"] = now
    return trades, balance


def get_usdt_balance() -> float:
    now_ms = int(time.time() * 1000)
    params = {"timestamp": now_ms}
    params = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.get(f"{FUTURES_BASE}/fapi/v2/balance",
                        params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    for asset in resp.json():
        if asset["asset"] == "USDT":
            return float(asset["balance"])
    raise ValueError("USDT balance not found.")


def get_usdt_available_balance() -> float:
    """Free USDT not locked in margin — the actual transferable amount."""
    now_ms = int(time.time() * 1000)
    params = {"timestamp": now_ms}
    params = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.get(f"{FUTURES_BASE}/fapi/v2/balance",
                        params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    for asset in resp.json():
        if asset["asset"] == "USDT":
            return float(asset["availableBalance"])
    raise ValueError("USDT balance not found.")


def binance_transfer_futures_to_spot(amount: float) -> dict:
    """Universal Transfer: USDⓈ-M Futures -> Spot. Internal transfer, no withdrawal involved."""
    now_ms = int(time.time() * 1000)
    params = {
        "type":      "UMFUTURE_MAIN",
        "asset":     "USDT",
        "amount":    f"{amount:.2f}",
        "timestamp": now_ms,
    }
    params  = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.post(f"{MAIN_BASE}/sapi/v1/asset/transfer",
                         params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────
# HELPERS — ANALYSIS
# ─────────────────────────────────────────────

def group_by_order(trades: list) -> list:
    orders = {}
    for t in trades:
        oid = t.get("orderId", t.get("id"))
        if oid not in orders:
            orders[oid] = {
                "orderId":     oid,
                "side":        t.get("side", ""),
                "time":        int(t["time"]),
                "realizedPnl": 0.0,
                "commission":  0.0,
                "notional":    0.0,  # sum of price*qty across fills, for ROI calc
                "qty":         0.0,
            }
        price = float(t.get("price", 0))
        qty   = float(t.get("qty", 0))
        orders[oid]["realizedPnl"] += float(t.get("realizedPnl", 0))
        orders[oid]["commission"]  += float(t.get("commission", 0))
        orders[oid]["notional"]    += price * qty
        orders[oid]["qty"]         += qty
        orders[oid]["time"]         = min(orders[oid]["time"], int(t["time"]))

    return sorted(orders.values(), key=lambda o: o["time"])


def group_by_position(trades: list) -> list:
    orders = group_by_order(trades)
    positions = []
    current = None

    for o in orders:
        is_entry = o["realizedPnl"] == 0

        if is_entry:
            if current and current["realizedPnl"] != 0:
                positions.append(current)
            if current is None or current["realizedPnl"] != 0:
                current = {
                    "time":        o["time"],
                    "close_time":  o["time"],
                    "side":        o["side"],
                    "realizedPnl": 0.0,
                    "commission":  o["commission"],
                    "order_ids":   [o["orderId"]],
                    "entry_notional": o["notional"],  # $ value of entry fills, for ROI calc
                }
            else:
                current["commission"]  += o["commission"]
                current["order_ids"].append(o["orderId"])
                current["entry_notional"] += o["notional"]  # scaling in (Entry2 etc.) adds to entry basis
        else:
            if current is None:
                current = {
                    "time":        o["time"],
                    "close_time":  o["time"],
                    "side":        o["side"],
                    "realizedPnl": 0.0,
                    "commission":  0.0,
                    "order_ids":   [],
                    "entry_notional": 0.0,
                }
            current["realizedPnl"] += o["realizedPnl"]
            current["commission"]  += o["commission"]
            current["close_time"]   = o["time"]
            current["order_ids"].append(o["orderId"])

    if current and current["realizedPnl"] != 0:
        positions.append(current)

    return positions


TRADING_WINDOW_START_HOUR = int(os.environ.get("TRADING_WINDOW_START_HOUR", "8"))   # 8am
TRADING_WINDOW_END_HOUR   = int(os.environ.get("TRADING_WINDOW_END_HOUR", "23"))    # 11pm
TIME_IN_TRADE_TARGET_PCT  = float(os.environ.get("TIME_IN_TRADE_TARGET_PCT", "20")) # target: stay under this %
TIME_IN_TRADE_WARN_PCT    = TIME_IN_TRADE_TARGET_PCT * 0.70  # early warning at 70% of the way to the target (14% by default)

def calculate_time_in_trade(trades: list, for_date=None) -> dict:
    """
    Sums actual time spent in positions (entry to close, per position) that
    falls within a FIXED daily window (8am-11pm UK by default, 15 hours) —
    not the old "first entry to last close" span, which gave a misleading
    100% on any day with just one trade. This is a consistent, comparable
    denominator every day regardless of how many trades were taken or when.
    Each position's time is CLIPPED to the window — e.g. a trade that opened
    at 7:30am only counts from 8:00am onward; one still open past 11pm only
    counts up to 11:00pm.
    for_date: a date object for which day's window to use (defaults to today,
    UK time) — matters because the window boundaries are anchored to a
    specific calendar day, not just "the last 15 hours".
    Returns None values if there are no closed positions overlapping the window.
    """
    positions = group_by_position(trades)
    if not positions:
        return {"in_trade_mins": None, "window_mins": None, "in_trade_pct": None}

    uk_tz = ZoneInfo("Europe/London")
    if for_date is None:
        for_date = datetime.now(uk_tz).date()

    window_start = datetime(for_date.year, for_date.month, for_date.day,
                            TRADING_WINDOW_START_HOUR, 0, 0, tzinfo=uk_tz)
    window_end   = datetime(for_date.year, for_date.month, for_date.day,
                            TRADING_WINDOW_END_HOUR, 0, 0, tzinfo=uk_tz)
    window_start_ms = int(window_start.timestamp() * 1000)
    window_end_ms   = int(window_end.timestamp() * 1000)
    window_mins = (window_end_ms - window_start_ms) / 60000

    in_trade_ms = 0
    for p in positions:
        entry_ms = p["time"]
        close_ms = p["close_time"]
        # Clip this position's span to the window — ignore anything outside it.
        clipped_start = max(entry_ms, window_start_ms)
        clipped_end   = min(close_ms, window_end_ms)
        if clipped_end > clipped_start:
            in_trade_ms += (clipped_end - clipped_start)

    return {
        "in_trade_mins": in_trade_ms / 60000,
        "window_mins":   window_mins,
        "in_trade_pct":  (in_trade_ms / (window_end_ms - window_start_ms) * 100) if window_end_ms > window_start_ms else 0.0,
    }


time_in_trade_warned_today = False

def check_time_in_trade_warning(trades=None):
    """
    Fires once per day, the first time today's in-trade % crosses
    TIME_IN_TRADE_WARN_PCT (70% of the way to TIME_IN_TRADE_TARGET_PCT) —
    an early heads-up before the actual daily target gets breached.
    Accepts an optional pre-fetched `trades` list (reuses the main loop's
    already-cached data via get_slow_refresh_data) so this doesn't fire a
    second, redundant Binance call every cycle on top of the one the loop
    already makes.
    """
    global time_in_trade_warned_today
    if time_in_trade_warned_today:
        return
    try:
        todays_trades = trades if trades is not None else get_futures_trades_today()
        tit = calculate_time_in_trade(todays_trades)
        if tit["window_mins"] is None:
            return
        if tit["in_trade_pct"] >= TIME_IN_TRADE_WARN_PCT:
            time_in_trade_warned_today = True
            db_log_warning("time_in_trade_warning", f"In-trade time at {tit['in_trade_pct']:.1f}% — crossed {TIME_IN_TRADE_WARN_PCT:.0f}% warning threshold (target is under {TIME_IN_TRADE_TARGET_PCT:.0f}%)")
            send_telegram(
                f"⏱️ <b>Time in trade — approaching target</b>\n\n"
                f"You're at {tit['in_trade_pct']:.0f}% of today's {TRADING_WINDOW_START_HOUR:02d}:00–{TRADING_WINDOW_END_HOUR:02d}:00 window "
                f"spent in a trade — that's 70% of the way to your {TIME_IN_TRADE_TARGET_PCT:.0f}% target.\n\n"
                f"Worth being mindful before it tips over."
            )
    except Exception as e:
        print(f"[Time In Trade Warning] Error: {e}")


def build_stats(trades: list) -> dict:
    orders    = group_by_order(trades)
    positions = group_by_position(trades)
    wins      = [p for p in positions if p["realizedPnl"] > 0]
    losses    = [p for p in positions if p["realizedPnl"] < 0]
    total_pnl  = sum(p["realizedPnl"] for p in positions)
    total_fees = sum(o["commission"]  for o in orders)
    win_rate   = (len(wins) / len(positions) * 100) if positions else 0
    entries    = [o for o in orders if o["realizedPnl"] == 0]

    # ── Average ROI % — realizedPnl as a % of margin used (entry_notional / leverage) ──
    # This is what actually tells you whether wins or losses need work: a
    # 60% win rate with tiny average win % and huge average loss % is a
    # very different problem than a 40% win rate with the reverse.
    def position_roi_pct(p):
        margin = (p.get("entry_notional", 0) / LEVERAGE) if LEVERAGE > 0 else 0
        return (p["realizedPnl"] / margin * 100) if margin > 0 else None

    win_rois  = [r for r in (position_roi_pct(p) for p in wins)  if r is not None]
    loss_rois = [r for r in (position_roi_pct(p) for p in losses) if r is not None]
    avg_win_roi_pct  = (sum(win_rois) / len(win_rois))   if win_rois  else 0
    avg_loss_roi_pct = (sum(loss_rois) / len(loss_rois)) if loss_rois else 0

    return {
        "total_trades":     len(entries),
        "closed_positions": len(positions),
        "wins":             len(wins),
        "losses":           len(losses),
        "win_rate":         win_rate,
        "total_pnl":        total_pnl,
        "total_fees":       total_fees,
        "net_pnl":          total_pnl - total_fees,
        "avg_win_roi_pct":  avg_win_roi_pct,
        "avg_loss_roi_pct": avg_loss_roi_pct,
    }


def get_consecutive_losses(trades: list) -> int:
    positions = group_by_position(trades)
    streak    = 0
    for p in reversed(positions):
        if p["realizedPnl"] < 0:
            streak += 1
        else:
            break
    return streak


# ─────────────────────────────────────────────
# TRADE EXECUTION
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# LIVE WEBSOCKET DATA — replaces repeated REST polling
# ─────────────────────────────────────────────
# The real, structural fix for tonight's repeated Binance 418/1003 rate-limit
# bans. Every REST-based fix so far (caching, pacing, wider windows) only
# ever reduced call VOLUME — it never eliminated the underlying problem,
# which is that this bot was fundamentally polling REST endpoints on a
# timer for data that Binance can just push to us continuously for free.
#
# This opens ONE persistent websocket connection to Binance's combined
# stream endpoint, subscribed to mark price + the kline intervals the bot
# actually uses (15m, 2H, 4H). Every incoming message updates an in-memory
# store — zero REST weight cost per update, no matter how often price
# moves or how many guards check it. get_mark_price(), get_live_ema240(),
# and get_live_ema()/get_live_williams_r()'s underlying kline fetch all now
# read from this store FIRST, only falling back to a real REST call if the
# websocket hasn't populated that specific piece of data yet (e.g. in the
# first few seconds after the bot starts, before the first message arrives).

_ws_mark_price = {"value": None, "updated_at": 0}
_ws_klines = {}  # keyed by interval string (e.g. "15m", "2h", "4h") -> list of [open_time, open, high, low, close, ...] oldest-first, CLOSED candles only
_ws_lock = threading.Lock()
_ws_connected = False
_ws_last_message_at = 0

WS_KLINE_INTERVALS = ["15m", "2h", "4h"]  # every interval currently used anywhere in the bot
WS_KLINE_HISTORY_LIMIT = 850  # candles kept in memory per interval — EMA(240) needs ~800 candles
                               # to converge properly, this comfortably covers that plus headroom
WS_STALE_THRESHOLD_SEC = 30  # if no message received in this long, treat the websocket as down and fall back to REST

def _ws_seed_kline_history(interval: str):
    """
    One-time REST call per interval at startup to seed historical closed
    candles, since the websocket only pushes NEW updates from the moment it
    connects — it has no memory of candles before that. This is the only
    REST kline call this system makes under normal operation; everything
    after this point comes from the stream.
    """
    try:
        resp = requests.get(
            f"{FUTURES_BASE}/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": interval, "limit": WS_KLINE_HISTORY_LIMIT},
            timeout=15
        )
        resp.raise_for_status()
        klines = resp.json()
        # Drop the last one — it's still-forming, the websocket will replace
        # it with real-time updates and eventually a genuine close event.
        closed = klines[:-1]
        with _ws_lock:
            _ws_klines[interval] = closed
        print(f"[WS] Seeded {len(closed)} closed candles for {interval}")
    except Exception as e:
        print(f"[WS] Seed error for {interval}: {e}")


def _ws_on_message(ws, message):
    global _ws_last_message_at
    _ws_last_message_at = time.time()
    try:
        data = json.loads(message)
        payload = data.get("data", data)  # combined stream wraps in {"stream":..., "data":...}
        event_type = payload.get("e")

        if event_type == "markPriceUpdate":
            with _ws_lock:
                _ws_mark_price["value"] = float(payload["p"])
                _ws_mark_price["updated_at"] = time.time()

        elif event_type == "kline":
            k = payload["k"]
            interval = k["i"]
            is_closed = k["x"]
            candle = [k["t"], k["o"], k["h"], k["l"], k["c"], k["v"]]
            with _ws_lock:
                existing = _ws_klines.get(interval, [])
                if existing and existing[-1][0] == k["t"]:
                    # Same still-forming candle, in-place update if it's now closed
                    if is_closed:
                        existing[-1] = candle
                elif is_closed:
                    # A genuinely new closed candle
                    existing.append(candle)
                    if len(existing) > WS_KLINE_HISTORY_LIMIT:
                        existing.pop(0)
                _ws_klines[interval] = existing
    except Exception as e:
        print(f"[WS] Message handling error: {e}")


def _ws_on_error(ws, error):
    print(f"[WS] Error: {error}")


def _ws_on_close(ws, close_status_code, close_msg):
    global _ws_connected
    _ws_connected = False
    print(f"[WS] Connection closed ({close_status_code}: {close_msg}) — will auto-reconnect")


def _ws_on_open(ws):
    global _ws_connected
    _ws_connected = True
    print("[WS] Connected")


def _ws_run_forever():
    """
    Runs the websocket connection with automatic reconnection. Called once
    in its own daemon thread at bot startup. If the connection drops for
    any reason (network blip, Binance restart, etc.), it reconnects after
    a short delay rather than leaving the bot silently degraded back to
    pure REST polling forever.
    """
    streams = [f"{SYMBOL.lower()}@markPrice@1s"] + [f"{SYMBOL.lower()}@kline_{iv}" for iv in WS_KLINE_INTERVALS]
    stream_path = "/".join(streams)
    url = f"wss://fstream.binance.com/stream?streams={stream_path}"

    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=_ws_on_open,
                on_message=_ws_on_message,
                on_error=_ws_on_error,
                on_close=_ws_on_close,
            )
            ws.run_forever(ping_interval=180, ping_timeout=10)  # Binance sends its own ping every 3 min
        except Exception as e:
            print(f"[WS] run_forever crashed: {e}")
        print("[WS] Reconnecting in 5s...")
        time.sleep(5)


def start_websocket_feed():
    """Call once at bot startup — seeds kline history via REST, then opens the persistent websocket in a background thread."""
    for interval in WS_KLINE_INTERVALS:
        _ws_seed_kline_history(interval)
    ws_thread = threading.Thread(target=_ws_run_forever, daemon=True)
    ws_thread.start()
    print("[WS] Feed thread started")


def _ws_is_healthy() -> bool:
    """True if the websocket has delivered a message recently — used to decide whether to trust its data or fall back to REST."""
    return _ws_connected and (time.time() - _ws_last_message_at) < WS_STALE_THRESHOLD_SEC


def ws_get_mark_price():
    """Returns the live websocket mark price, or None if unavailable/stale — caller should fall back to REST."""
    if not _ws_is_healthy():
        return None
    with _ws_lock:
        if _ws_mark_price["value"] is not None and (time.time() - _ws_mark_price["updated_at"]) < WS_STALE_THRESHOLD_SEC:
            return _ws_mark_price["value"]
    return None


def ws_get_closed_klines(interval: str, min_count: int = 1):
    """Returns the in-memory closed-candle list for this interval, or None if unavailable/insufficient — caller should fall back to REST."""
    if not _ws_is_healthy():
        return None
    with _ws_lock:
        candles = _ws_klines.get(interval)
        if candles and len(candles) >= min_count:
            return list(candles)  # copy, so caller can't mutate the shared store
    return None


_mark_price_cache = {"value": None, "fetched_at": 0}
MARK_PRICE_CACHE_SECONDS = 15  # only used as the REST fallback cache now that the websocket is primary —
                                # this fires only when the websocket is down/stale, so it's a genuine
                                # safety net rather than the main data path.

def get_mark_price() -> float:
    # ── Primary: live websocket data, zero REST cost ──
    ws_price = ws_get_mark_price()
    if ws_price is not None:
        return ws_price

    # ── Fallback: REST, only reached if the websocket is down or hasn't
    # delivered a price yet (e.g. the first second after bot startup) ──
    now = time.time()
    if _mark_price_cache["value"] is not None and (now - _mark_price_cache["fetched_at"]) < MARK_PRICE_CACHE_SECONDS:
        return _mark_price_cache["value"]
    resp = requests.get(
        f"{FUTURES_BASE}/fapi/v1/premiumIndex",
        params={"symbol": SYMBOL}, timeout=10
    )
    resp.raise_for_status()
    price = float(resp.json()["markPrice"])
    _mark_price_cache["value"]      = price
    _mark_price_cache["fetched_at"] = now
    return price


def composite_color(value: float) -> str:
    """Maps a composite score to its zone color name, per Nathan's confirmed table."""
    if value is None:
        return "Unknown"
    if value > 80:
        return "Red"
    elif value >= 65:
        return "Orange"
    elif value >= 55:
        return "Yellow"
    elif value >= 45:
        return "Grey"
    elif value >= 35:
        return "Blue"
    elif value >= 20:
        return "Green"
    else:
        return "Cyan"


def calculate_ema(closes: list, period: int) -> float:
    """Standard EMA calculation. closes must be oldest-first."""
    if len(closes) < period:
        raise ValueError(f"Need at least {period} candles, got {len(closes)}")
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period  # seed with SMA of first `period` closes
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema


def calculate_williams_r(highs: list, lows: list, closes: list, length: int) -> float:
    """
    Modified Williams %R over the most recent `length` candles — uses the
    TYPICAL PRICE (High + Low + Close) / 3 in place of the raw close, per
    Nathan's spec. Standard Williams %R uses close only; this version
    smooths that out slightly by incorporating the candle's full range.
    %R = (Highest High - Typical Price) / (Highest High - Lowest Low) * -100
    Range: -100 (at recent low) to 0 (at recent high).
    highs/lows/closes must be oldest-first, same length, aligned by index.
    """
    if len(closes) < length:
        raise ValueError(f"Need at least {length} candles, got {len(closes)}")
    recent_highs = highs[-length:]
    recent_lows  = lows[-length:]
    highest_high = max(recent_highs)
    lowest_low   = min(recent_lows)
    typical_price = (highs[-1] + lows[-1] + closes[-1]) / 3
    if highest_high == lowest_low:
        return -50.0  # flat range — neutral fallback, avoids divide-by-zero
    return (highest_high - typical_price) / (highest_high - lowest_low) * -100


_wpr_cache = {}  # keyed by interval string -> {"value": ..., "fetched_at": ...}
WPR_CACHE_SECONDS = 600  # REST fallback cache duration, only used when the websocket is down/stale
_wpr_last_error = None  # diagnosable reason for the most recent WPR fetch failure, if any

def get_live_williams_r(interval: str = "15m", length: int = 120) -> float:
    """
    Calculates Williams %R on the LAST CLOSED candle for the given interval.
    Primary data source is the live websocket kline stream (zero REST cost);
    falls back to a direct REST fetch only if the websocket is down/stale
    or doesn't have enough history yet for this interval. Cache is keyed
    per-interval — previously a single shared cache meant a 2H WPR request
    could incorrectly be served a cached 15m value or vice versa, a real
    bug introduced when the 2H HTF WPR gate was added.
    """
    global _wpr_last_error

    # ── Primary: websocket kline data, zero REST cost ──
    ws_klines = ws_get_closed_klines(interval, min_count=length)
    if ws_klines is not None:
        try:
            recent = ws_klines[-length:]
            highs  = [float(k[2]) for k in recent]
            lows   = [float(k[3]) for k in recent]
            closes = [float(k[4]) for k in recent]
            wpr = calculate_williams_r(highs, lows, closes, length)
            _wpr_last_error = None
            return wpr
        except Exception as e:
            print(f"[WPR] Websocket calc error, falling back to REST: {e}")

    # ── Fallback: REST, cached per-interval ──
    now = time.time()
    cached = _wpr_cache.get(interval)
    if cached and (now - cached["fetched_at"]) < WPR_CACHE_SECONDS:
        return cached["value"]
    try:
        resp = requests.get(
            f"{FUTURES_BASE}/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": interval, "limit": length + 5},
            timeout=10
        )
        resp.raise_for_status()
        klines = resp.json()
        closed_klines = klines[:-1]  # drop the still-forming current candle
        if len(closed_klines) < length:
            _wpr_last_error = f"only {len(closed_klines)} closed candles returned, need {length}"
            return None
        highs  = [float(k[2]) for k in closed_klines]
        lows   = [float(k[3]) for k in closed_klines]
        closes = [float(k[4]) for k in closed_klines]
        wpr = calculate_williams_r(highs, lows, closes, length)
        _wpr_cache[interval] = {"value": wpr, "fetched_at": now}
        _wpr_last_error = None
        return wpr
    except requests.exceptions.HTTPError as e:
        _wpr_last_error = f"Binance HTTP error: {e}"
        print(f"[WPR] Live calculation error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        _wpr_last_error = f"network/timeout error: {e}"
        print(f"[WPR] Live calculation error: {e}")
        return None
    except Exception as e:
        _wpr_last_error = f"unexpected error: {e}"
        print(f"[WPR] Live calculation error: {e}")
        return None


# ── WPR reversal/exhaustion signals ────────────────────────────────────────
# WPR crossing DOWN through -18 = price just rolled over off its recent
# high — a SHORT entry (the extreme high has exhausted). WPR crossing UP
# through -82 = price just bounced off its recent low — a LONG/BUY entry
# (the extreme low has exhausted). Uses closed-candle WPR only, same
# discipline as the HTF flip signals, to avoid live-candle whipsaw.
WPR_SHORT_CROSS_ENABLED = os.environ.get("WPR_SHORT_CROSS_ENABLED", "false").lower() == "true"
WPR_LONG_CROSS_ENABLED  = os.environ.get("WPR_LONG_CROSS_ENABLED", "false").lower() == "true"
_wpr_last_below_18 = None  # tracks previous closed-candle state for cross detection
_wpr_last_above_82 = None

def check_wpr_short_cross():
    global _wpr_last_below_18
    if not WPR_SHORT_CROSS_ENABLED:
        return
    wpr = get_live_williams_r(interval="15m", length=120)
    if wpr is None:
        return
    currently_below_18 = wpr < -18
    just_crossed_down = (_wpr_last_below_18 is False and currently_below_18 is True)
    _wpr_last_below_18 = currently_below_18
    if just_crossed_down:
        threading.Thread(
            target=process_webhook_signal,
            args=({"signal": "wpr_crosses_down_18"},),
            daemon=True
        ).start()


def check_wpr_long_cross():
    global _wpr_last_above_82
    if not WPR_LONG_CROSS_ENABLED:
        return
    wpr = get_live_williams_r(interval="15m", length=120)
    if wpr is None:
        return
    currently_above_82 = wpr > -82
    just_crossed_up = (_wpr_last_above_82 is False and currently_above_82 is True)
    _wpr_last_above_82 = currently_above_82
    if just_crossed_up:
        threading.Thread(
            target=process_webhook_signal,
            args=({"signal": "wpr_crosses_up_82"},),
            daemon=True
        ).start()






_ema240_cache = {"value": None, "fetched_at": 0}
EMA240_CACHE_SECONDS = 1200  # a 15m EMA(240) moves negligibly within a single 15m candle, and
                              # even less between candles than WPR does (it's a 240-period smoothed
                              # average, not recalculated fresh each candle like WPR) — 20 minutes
                              # is safe here even for the manual /long /short guard check, unlike
                              # WPR where a shorter window was kept deliberately.

_ema120_guard_cache = {"value": None, "fetched_at": 0}
EMA120_GUARD_CACHE_SECONDS = 1200

def get_live_ema120_guard(interval: str = "15m") -> float:
    """
    Calculates the EMA(120) on the given interval — used by the standalone
    EMA_GUARD_ENABLED/SIMPLE_EMA_GUARD_ENABLED guard. Separate from the
    120/240 pair in get_setup_grade()'s A/B+/B- logic, which needs its own
    240 EMA (get_live_ema240) untouched.
    """
    ws_klines = ws_get_closed_klines(interval, min_count=120)
    if ws_klines is not None:
        try:
            closes = [float(k[4]) for k in ws_klines]
            return calculate_ema(closes, 120)
        except Exception as e:
            print(f"[EMA] Websocket calc error, falling back to REST: {e}")

    now = time.time()
    if _ema120_guard_cache["value"] is not None and (now - _ema120_guard_cache["fetched_at"]) < EMA120_GUARD_CACHE_SECONDS:
        return _ema120_guard_cache["value"]
    try:
        resp = requests.get(
            f"{FUTURES_BASE}/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": interval, "limit": 300},
            timeout=10
        )
        resp.raise_for_status()
        klines = resp.json()
        closes = [float(k[4]) for k in klines]
        ema = calculate_ema(closes, 120)
        _ema120_guard_cache["value"]      = ema
        _ema120_guard_cache["fetched_at"] = now
        return ema
    except Exception as e:
        print(f"[EMA] Live calculation error: {e}")
        return None


def get_live_ema240(interval: str = "15m") -> float:
    """
    Calculates the EMA(240) on the given interval. Primary data source is
    the live websocket kline stream (zero REST cost); falls back to a
    direct REST fetch only if the websocket is down/stale or doesn't have
    enough history yet for this interval.
    Returns the EMA value, or None on failure.
    """
    # ── Primary: websocket kline data, zero REST cost ──
    ws_klines = ws_get_closed_klines(interval, min_count=240)
    if ws_klines is not None:
        try:
            closes = [float(k[4]) for k in ws_klines]
            return calculate_ema(closes, 240)
        except Exception as e:
            print(f"[EMA] Websocket calc error, falling back to REST: {e}")

    # ── Fallback: REST ──
    now = time.time()
    if _ema240_cache["value"] is not None and (now - _ema240_cache["fetched_at"]) < EMA240_CACHE_SECONDS:
        return _ema240_cache["value"]
    try:
        resp = requests.get(
            f"{FUTURES_BASE}/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": interval, "limit": 800},
            timeout=10
        )
        resp.raise_for_status()
        klines = resp.json()
        closes = [float(k[4]) for k in klines]  # index 4 = close price, oldest-first
        ema = calculate_ema(closes, 240)
        _ema240_cache["value"]      = ema
        _ema240_cache["fetched_at"] = now
        return ema
    except Exception as e:
        print(f"[EMA] Live calculation error: {e}")
        return None


_generic_ema_cache = {}  # keyed by (interval, period) -> {"value": ..., "fetched_at": ...}
GENERIC_EMA_CACHE_SECONDS = 1200  # only ever called with 15m intervals currently — same
                                   # reasoning as EMA240 above, safe to extend

def get_live_ema(interval: str, period: int) -> float:
    """
    Generic version of get_live_ema240 for arbitrary interval/period combos —
    used by signals that need EMAs get_live_ema240 doesn't cover (e.g. the
    120/240 EMA pair used in the A/B+/B- setup grading).
    Primary data source is the live websocket kline stream (zero REST cost);
    falls back to a direct REST fetch, cached per (interval, period), only
    if the websocket is down/stale or doesn't have enough history yet.
    """
    # ── Primary: websocket kline data, zero REST cost ──
    ws_klines = ws_get_closed_klines(interval, min_count=period)
    if ws_klines is not None:
        try:
            closes = [float(k[4]) for k in ws_klines]
            return calculate_ema(closes, period)
        except Exception as e:
            print(f"[EMA] Websocket calc error ({interval}, period {period}), falling back to REST: {e}")

    # ── Fallback: REST ──
    cache_key = (interval, period)
    now = time.time()
    cached = _generic_ema_cache.get(cache_key)
    if cached and (now - cached["fetched_at"]) < GENERIC_EMA_CACHE_SECONDS:
        return cached["value"]
    try:
        resp = requests.get(
            f"{FUTURES_BASE}/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": interval, "limit": max(300, period * 3)},
            timeout=10
        )
        resp.raise_for_status()
        klines = resp.json()
        closes = [float(k[4]) for k in klines]
        ema = calculate_ema(closes, period)
        _generic_ema_cache[cache_key] = {"value": ema, "fetched_at": now}
        return ema
    except Exception as e:
        print(f"[EMA] Live calculation error ({interval}, period {period}): {e}")
        return None


_symbol_info_cache = {"value": None, "fetched_at": 0}
SYMBOL_INFO_CACHE_SECONDS = 3600  # tick size / qty step for BTCUSDT essentially never change —
                                   # this endpoint (/fapi/v1/exchangeInfo) returns rules for EVERY
                                   # symbol on the exchange, one of Binance's heaviest calls, and
                                   # was being hit fresh on every single trade attempt (13 call
                                   # sites, zero caching) — a real contributor to burst rate-limit
                                   # trips when opening a trade with all its follow-up orders.

def get_symbol_info() -> dict:
    now = time.time()
    if _symbol_info_cache["value"] is not None and (now - _symbol_info_cache["fetched_at"]) < SYMBOL_INFO_CACHE_SECONDS:
        return _symbol_info_cache["value"]
    resp = requests.get(f"{FUTURES_BASE}/fapi/v1/exchangeInfo", timeout=15)
    resp.raise_for_status()
    for s in resp.json()["symbols"]:
        if s["symbol"] == SYMBOL:
            info = {"qty_step": 0.001, "price_tick": 0.1}
            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    info["qty_step"] = float(f["stepSize"])
                elif f["filterType"] == "PRICE_FILTER":
                    info["price_tick"] = float(f["tickSize"])
            _symbol_info_cache["value"]      = info
            _symbol_info_cache["fetched_at"] = now
            return info
    fallback = {"qty_step": 0.001, "price_tick": 0.1}
    _symbol_info_cache["value"]      = fallback
    _symbol_info_cache["fetched_at"] = now
    return fallback


def round_step(value: float, step: float) -> float:
    import math
    precision = max(0, -int(math.floor(math.log10(step))))
    return round(math.floor(value / step) * step, precision)


def set_leverage_for_symbol():
    now_ms = int(time.time() * 1000)
    params = {"symbol": SYMBOL, "leverage": LEVERAGE, "timestamp": now_ms}
    params = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.post(f"{FUTURES_BASE}/fapi/v1/leverage",
                         params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


_open_position_cache = {"value": "unset", "fetched_at": 0}
OPEN_POSITION_CACHE_SECONDS = 10  # emergency-widened from 5s, same reasoning as mark price above

def invalidate_position_cache():
    """Call this immediately after any action that closes/changes the position, so the
    next get_open_position() call fetches fresh data instead of returning a stale cached result."""
    _open_position_cache["value"]      = "unset"
    _open_position_cache["fetched_at"] = 0


def get_open_position() -> dict | None:
    now = time.time()
    if _open_position_cache["value"] != "unset" and (now - _open_position_cache["fetched_at"]) < OPEN_POSITION_CACHE_SECONDS:
        return _open_position_cache["value"]
    now_ms = int(time.time() * 1000)
    params = {"symbol": SYMBOL, "timestamp": now_ms}
    params = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.get(f"{FUTURES_BASE}/fapi/v2/positionRisk",
                        params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    result = None
    for pos in resp.json():
        if pos["symbol"] == SYMBOL and float(pos["positionAmt"]) != 0:
            result = pos
            break
    _open_position_cache["value"]      = result
    _open_position_cache["fetched_at"] = now
    return result


def cancel_open_orders():
    now_ms  = int(time.time() * 1000)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    params = {"symbol": SYMBOL, "timestamp": now_ms}
    params = sign_request(params)
    try:
        requests.delete(f"{FUTURES_BASE}/fapi/v1/allOpenOrders",
                        params=params, headers=headers, timeout=10)
    except Exception:
        pass

    # Regular orders and algo (conditional) orders live in genuinely separate
    # order books since Binance's 2025-12-09 mandatory migration — the old
    # /fapi/v1/allOpenOrders/algo path was never a real endpoint (confirmed
    # against official docs), meaning this never actually cleared any real
    # STOP_MARKET/TAKE_PROFIT_MARKET orders. The correct endpoint is
    # /fapi/v1/algoOpenOrders.
    now_ms = int(time.time() * 1000)
    params = {"symbol": SYMBOL, "timestamp": now_ms}
    params = sign_request(params)
    try:
        requests.delete(f"{FUTURES_BASE}/fapi/v1/algoOpenOrders",
                        params=params, headers=headers, timeout=10)
    except Exception:
        pass


def place_algo_order(side: str, order_type: str, stop_price: float,
                     quantity: float = None, close_position: bool = False,
                     reduce_only: bool = False) -> dict:
    """
    Places a conditional (algo) order — STOP_MARKET, TAKE_PROFIT_MARKET, etc.
    Binance made this endpoint MANDATORY for these order types effective
    2025-12-09 — the old POST /fapi/v1/order endpoint now rejects them
    outright with error -4120 ("Order type not supported for this endpoint.
    Please use the Algo Order API endpoints instead."). This was discovered
    after the earlier "real SL order" fix silently failed in production —
    every stop-loss placed via the old endpoint since that fix was added
    never actually existed on the exchange, despite the bot reporting
    success in some paths. This function uses the correct, current endpoint.
    Returns the parsed JSON response, with "algoId" in place of "orderId".
    """
    now_ms = int(time.time() * 1000)
    params: dict = {
        "algoType":    "CONDITIONAL",
        "symbol":      SYMBOL,
        "side":        side,
        "type":        order_type,
        "triggerPrice": stop_price,
        "workingType": "MARK_PRICE",
        "timestamp":   now_ms,
    }
    if close_position:
        params["closePosition"] = "true"
    else:
        if quantity is not None:
            params["quantity"] = quantity
        if reduce_only:
            params["reduceOnly"] = "true"
    params  = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.post(f"{FUTURES_BASE}/fapi/v1/algoOrder",
                         params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def cancel_algo_order(algo_id) -> dict:
    """Cancels a single algo (conditional) order by its algoId."""
    now_ms  = int(time.time() * 1000)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    params  = sign_request({"symbol": SYMBOL, "algoId": algo_id, "timestamp": now_ms})
    resp = requests.delete(f"{FUTURES_BASE}/fapi/v1/algoOrder",
                           params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_open_algo_orders() -> list:
    """Returns all open algo (conditional) orders for this symbol — the new equivalent of /fapi/v1/openOrders for STOP_MARKET/TAKE_PROFIT_MARKET etc."""
    now_ms  = int(time.time() * 1000)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    params  = sign_request({"symbol": SYMBOL, "timestamp": now_ms})
    resp = requests.get(f"{FUTURES_BASE}/fapi/v1/openAlgoOrders",
                        params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("orders", data) if isinstance(data, dict) else data


def cancel_all_algo_orders(keep_latest: bool = False):
    """
    Cancels open algo (conditional) orders for this symbol.
    keep_latest=True cancels every algo order EXCEPT the most recently
    placed one — used by adjust_sl() so placing the new SL first, then
    cleaning up, doesn't also cancel the SL we just placed.
    """
    if not keep_latest:
        now_ms  = int(time.time() * 1000)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        params  = sign_request({"symbol": SYMBOL, "timestamp": now_ms})
        try:
            requests.delete(f"{FUTURES_BASE}/fapi/v1/algoOpenOrders",
                            params=params, headers=headers, timeout=10)
        except Exception as e:
            print(f"[Cancel Algo Orders] Error: {e}")
        return

    try:
        orders = get_open_algo_orders()
        if not orders:
            return
        # Keep the order with the highest algoId (most recently placed), cancel the rest
        latest = max(orders, key=lambda o: o.get("algoId", 0))
        for o in orders:
            if o.get("algoId") != latest.get("algoId"):
                try:
                    cancel_algo_order(o["algoId"])
                except Exception as e:
                    print(f"[Cancel Algo Orders] Error cancelling {o.get('algoId')}: {e}")
    except Exception as e:
        print(f"[Cancel Algo Orders] keep_latest error: {e}")


def place_order(side: str, order_type: str, quantity: float,
                stop_price: float = None, reduce_only: bool = False,
                limit_price: float = None, close_position: bool = False) -> dict:
    now_ms = int(time.time() * 1000)
    params: dict = {
        "symbol":    SYMBOL,
        "side":      side,
        "type":      order_type,
        "timestamp": now_ms,
    }
    if close_position:
        params["closePosition"] = "true"
        params["stopPrice"]     = stop_price
    else:
        params["quantity"] = quantity
        if order_type == "LIMIT":
            params["price"]       = limit_price
            params["timeInForce"] = "GTC"
        if stop_price is not None:
            params["stopPrice"] = stop_price
        if reduce_only:
            params["reduceOnly"] = "true"
    params  = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.post(f"{FUTURES_BASE}/fapi/v1/order",
                         params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def place_maker_entry(side: str, quantity: float, direction: str, price_tick: float):
    global maker_entry_cancel_requested
    maker_entry_cancel_requested = False  # reset at start of each attempt
    mark = get_mark_price()
    if direction == "long":
        limit_price = round_step(mark * (1 - MAKER_OFFSET_PCT / 100), price_tick)
    else:
        limit_price = round_step(mark * (1 + MAKER_OFFSET_PCT / 100), price_tick)

    send_telegram(f"Placing maker limit entry at ${limit_price:,.2f} (mark: ${mark:,.2f})")
    resp = place_order(side, "LIMIT", quantity, limit_price=limit_price)
    order_id = resp.get("orderId")

    deadline = time.time() + MAKER_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(2)

        # Check for manual cancel request
        if maker_entry_cancel_requested:
            maker_entry_cancel_requested = False
            try:
                now_ms = int(time.time() * 1000)
                params = sign_request({"symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms})
                headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                params=params, headers=headers, timeout=10)
            except Exception:
                pass
            send_telegram("✅ <b>Maker entry cancelled</b> — order removed, no position opened.")
            return None
        now_ms = int(time.time() * 1000)
        params = sign_request({"symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms})
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        check = requests.get(f"{FUTURES_BASE}/fapi/v1/order",
                             params=params, headers=headers, timeout=10)
        check.raise_for_status()
        status = check.json().get("status")
        if status == "FILLED":
            send_telegram(f"Maker entry filled at ${limit_price:,.2f}")
            return resp
        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            break

    try:
        now_ms = int(time.time() * 1000)
        params = sign_request({"symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms})
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                        params=params, headers=headers, timeout=10)
    except Exception:
        pass

    send_telegram(f"Maker entry not filled in {MAKER_TIMEOUT_SEC}s — falling back to market.")
    return place_order(side, "MARKET", quantity)

_execute_trade_lock = threading.Lock()

def execute_trade(direction: str, size_pct: float = None, size_usdt: float = None,
                  stop_pct: float = None, tp_pct: float = None,
                  triggered_by: str = None, zone: str = None,
                  z_score: float = None, composite: float = None) -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled. Set EXECUTION_ENABLED=true to enable.")
        return

    # ── Prevent overlapping executions ──────────────────────────────────
    # A single pass through this function makes up to ~10 Binance calls
    # (guards, position checks, symbol info). If a second /long, /short, or
    # signal fires while one is still running, both threads' full guard
    # chains overlap and can stack enough requests to trip Binance's rate
    # limit — this is what actually caused repeated bans tonight, not any
    # single uncached call. non-blocking acquire: if already running, this
    # call is rejected immediately rather than queuing (queuing would just
    # delay the same problem, not fix it).
    if not _execute_trade_lock.acquire(blocking=False):
        send_telegram(
            f"⏳ <b>Already processing a trade request — try again in a moment.</b>\n\n"
            f"Another /long, /short, or signal is still being evaluated. "
            f"Rapid repeated commands were the actual cause of tonight's rate-limit bans."
        )
        return
    try:
        _execute_trade_inner(direction, size_pct, size_usdt, stop_pct, tp_pct,
                              triggered_by, zone, z_score, composite)
    finally:
        _execute_trade_lock.release()


def _execute_trade_inner(direction: str, size_pct: float = None, size_usdt: float = None,
                          stop_pct: float = None, tp_pct: float = None,
                          triggered_by: str = None, zone: str = None,
                          z_score: float = None, composite: float = None) -> None:
    # ── Time-based blocks ─────────────────────────────────────────────
    uk_tz   = ZoneInfo("Europe/London")
    now_uk_full = datetime.now(uk_tz)
    now_uk  = now_uk_full.time()

    if now_uk_full.weekday() == 4 and now_uk >= dt_time(19, 0):
        send_telegram(
            f"<b>Trade blocked — Friday cutoff (19:00)</b>\n\n"
            f"No new positions after 19:00 on Fridays. Week's done — see you Monday."
        )
        return

    if now_uk >= NIGHT_BLOCK_START:
        send_telegram(
            f"<b>Trade blocked — after 22:00</b>\n\n"
            f"No new positions after 22:00. Come back tomorrow."
        )
        return

    if COOLDOWN_START <= now_uk < COOLDOWN_END:
        send_telegram(
            f"<b>Trade blocked — US open cooldown</b>\n\n"
            f"No new positions between {COOLDOWN_START.strftime('%H:%M')} and "
            f"{COOLDOWN_END.strftime('%H:%M')} UK time.\n"
            f"Wait for volatility to settle."
        )
        return

    # ── Same-side loss block ──────────────────────────────────────────
    block_until = same_side_block.get(direction, 0)
    if time.time() < block_until:
        remaining_mins = int((block_until - time.time()) / 60)
        send_telegram(
            f"<b>{direction.upper()} blocked — consecutive loss cooldown</b>\n\n"
            f"You've been blocked from {direction}s after consecutive losses.\n"
            f"Unblocks in {remaining_mins}m."
        )
        return

    # ── Daily losses block — hard stop after DAILY_LOSS_STREAK_LIMIT losses ─
    # Checks real trade data directly, not just the in-memory flag — a
    # restart resets the flag to False, but if you've genuinely had
    # DAILY_LOSS_STREAK_LIMIT losses today, this must still block instantly,
    # not wait for the next poll cycle to re-derive and re-set the flag.
    try:
        todays_trades_for_block_check = get_futures_trades_today()
        todays_stats_for_block_check  = build_stats(todays_trades_for_block_check)
        real_losses_today = todays_stats_for_block_check["losses"]
    except Exception as e:
        print(f"[Daily Loss Block] Live check error: {e}")
        real_losses_today = 0  # fail open — don't block on a data-fetch error

    if daily_losses_block or real_losses_today >= DAILY_LOSS_STREAK_LIMIT:
        send_telegram(
            f"🚫 <b>Blocked — {DAILY_LOSS_STREAK_LIMIT} losses today</b>\n\n"
            f"You've hit {DAILY_LOSS_STREAK_LIMIT} losing trades today. Trading is blocked for the rest of the session.\n"
            f"Resets at midnight. Walk away and come back tomorrow."
        )
        return

    # ── Daily hard stop (trade count or loss limit) ────────────────────
    if stopped_today:
        send_telegram(
            f"🚫 <b>Trade blocked — daily trading halted</b>\n\n"
            f"Either your trade count cap or daily loss limit was hit today.\n"
            f"Check /warnings for the exact reason. Resets at midnight."
        )
        return

    # ── Revenge trade block ───────────────────────────────────────────
    if last_loss_close_time > 0 and (time.time() - last_loss_close_time) < REVENGE_WINDOW_MINS * 60:
        remaining_secs = int(REVENGE_WINDOW_MINS * 60 - (time.time() - last_loss_close_time))
        remaining_mins = remaining_secs // 60
        remaining_s    = remaining_secs % 60
        send_telegram(
            f"🚫 <b>Revenge trade blocked — you just tried it</b>\n\n"
            f"Your last trade was a loss {int(time.time() - last_loss_close_time) // 60}m ago.\n"
            f"You are not allowed to trade for {REVENGE_WINDOW_MINS} minutes after a loss.\n\n"
            f"Unblocks in {remaining_mins}m {remaining_s}s. Step away."
        )
        return

    # ── TP2 close blocker (20 mins after TP2 hit) ────────────────────────
    if last_tp2_close_time > 0 and (time.time() - last_tp2_close_time) < REVENGE_WINDOW_MINS * 60:
        remaining_secs = int(REVENGE_WINDOW_MINS * 60 - (time.time() - last_tp2_close_time))
        remaining_mins = remaining_secs // 60
        remaining_s    = remaining_secs % 60
        send_telegram(
            f"✋ <b>TP2 cooldown — step away</b>\n\n"
            f"You just banked a clean win {int(time.time() - last_tp2_close_time) // 60}m ago.\n"
            f"Let it settle. No trading for {REVENGE_WINDOW_MINS} minutes after TP2.\n\n"
            f"Unblocks in {remaining_mins}m {remaining_s}s."
        )
        return

    # ── Daily bias check ──────────────────────────────────────────────
    if BIAS_FILTER_ENABLED and daily_bias is None:
        send_telegram(
            f"🚫 <b>{direction.upper()} blocked — no daily bias set</b>\n\n"
            f"Reply /bullish or /bearish to set your HTF bias."
        )
        return

    # ── Composite guards ──────────────────────────────────────────────
    # Block 1: No longs at extreme premium (don't buy the top)
    if direction == "long" and current_composite is not None and current_composite > COMPOSITE_LONG_MAX:
        send_telegram(
            f"🚫 <b>Long blocked — composite too high ({current_composite:.1f})</b>\n\n"
            f"Composite is above {COMPOSITE_LONG_MAX:.0f} — price is at extreme premium.\n"
            f"No longs at the top. Wait for composite to pull back."
        )
        return

    # Block 2: No shorts at extreme discount (don't sell the bottom)
    if direction == "short" and current_composite is not None and current_composite < COMPOSITE_SHORT_MIN:
        send_telegram(
            f"🚫 <b>Short blocked — composite too low ({current_composite:.1f})</b>\n\n"
            f"Composite is below {COMPOSITE_SHORT_MIN:.0f} — price is at extreme discount.\n"
            f"No shorts at the bottom. Wait for composite to recover."
        )
        return

    # Block 3: No shorts when hyperextended premium (chasing too late)
    if direction == "short" and current_composite is not None and current_composite > COMPOSITE_SHORT_MAX:
        send_telegram(
            f"🚫 <b>Short blocked — composite hyperextended ({current_composite:.1f})</b>\n\n"
            f"Composite is above {COMPOSITE_SHORT_MAX:.0f} — move is already overextended.\n"
            f"Shorts are blocked to prevent chasing extended moves."
        )
        return

    # Block 4: No longs when hyperextended discount (chasing too late)
    if direction == "long" and current_composite is not None and current_composite < COMPOSITE_LONG_MIN:
        send_telegram(
            f"🚫 <b>Long blocked — composite hyperextended ({current_composite:.1f})</b>\n\n"
            f"Composite is below {COMPOSITE_LONG_MIN:.0f} — move is already overextended.\n"
            f"Longs are blocked to prevent chasing extended moves."
        )
        return

    # ── HTF composite guards (2H) ─────────────────────────────────────
    if direction == "long" and current_htf_composite is not None and current_htf_composite < 12:
        send_telegram(
            f"🚫 <b>Long blocked — HTF composite too low ({current_htf_composite:.1f})</b>\n\n"
            f"2H composite is below 12 — HTF bearish bias.\n"
            f"Wait for HTF to recover above 11."
        )
        return

    if direction == "long" and current_htf_composite is not None and current_htf_composite > 88:
        send_telegram(
            f"🚫 <b>Long blocked — HTF composite hyperextended ({current_htf_composite:.1f})</b>\n\n"
            f"2H composite is above 88 — HTF extremely overbought.\n"
            f"Wait for pullback below 89."
        )
        return

    if direction == "short" and current_htf_composite is not None and current_htf_composite < 12:
        send_telegram(
            f"🚫 <b>Short blocked — HTF composite too low ({current_htf_composite:.1f})</b>\n\n"
            f"2H composite is below 12 — HTF already deeply oversold.\n"
            f"Don't sell the bottom on the higher timeframe. Wait for HTF to recover above 11."
        )
        return

    if direction == "short" and current_htf_composite is not None and current_htf_composite > 88:
        send_telegram(
            f"🚫 <b>Short blocked — HTF composite hyperextended ({current_htf_composite:.1f})</b>\n\n"
            f"2H composite is above 88 — HTF still deeply bullish.\n"
            f"LTF shorts here are fighting the higher timeframe trend — likely to bounce hard. "
            f"Wait for pullback below 89."
        )
        return

    # ── EMA premium/discount guard — don't chase, only enter on pullback ──
    # Block longs when price is above the 240 EMA (buying a premium, chasing strength).
    # Block shorts when price is below the 240 EMA (selling a discount, chasing weakness).
    # EMA calculated live from Binance klines — no dependency on any TradingView
    # indicator or alert field.
    if EMA_GUARD_ENABLED:
        live_ema120 = get_live_ema120_guard()
        if live_ema120 is not None:
            try:
                ema_check_price = get_mark_price()
            except Exception:
                ema_check_price = None
            if ema_check_price is not None:
                if direction == "long" and ema_check_price > live_ema120:
                    db_log_warning("ema_guard_block", f"Long blocked — price ${ema_check_price:,.2f} above 120 EMA (${live_ema120:,.2f})")
                    send_telegram(
                        f"🚫 <b>Long blocked — price above 120 EMA (premium)</b>\n\n"
                        f"Price: ${ema_check_price:,.2f}\n"
                        f"120 EMA: ${live_ema120:,.2f}\n\n"
                        f"This is chasing strength, not buying a pullback. Wait for price to "
                        f"come back to or below the 120 EMA before longing."
                    )
                    return
                if direction == "short" and ema_check_price < live_ema120:
                    db_log_warning("ema_guard_block", f"Short blocked — price ${ema_check_price:,.2f} below 120 EMA (${live_ema120:,.2f})")
                    send_telegram(
                        f"🚫 <b>Short blocked — price below 120 EMA (discount)</b>\n\n"
                        f"Price: ${ema_check_price:,.2f}\n"
                        f"120 EMA: ${live_ema120:,.2f}\n\n"
                        f"This is chasing weakness, not selling a bounce. Wait for price to "
                        f"come back to or above the 120 EMA before shorting."
                    )
                    return

    # ── Simple 120 EMA trade-flexibility guard ────────────────────────
    # Block SHORT if price is below the 15m 120 EMA — designed to reduce
    # how often Nathan needs to check Binance manually, while still
    # allowing some trade flexibility rather than a full lockout.
    if SIMPLE_EMA_GUARD_ENABLED:
        if direction == "short":
            ema_120 = get_live_ema120_guard()  # reuses the same cached 120 EMA as the guard above — avoids a second, duplicate kline fetch for the identical value
            if ema_120 is not None:
                try:
                    check_price = get_mark_price()
                except Exception:
                    check_price = None
                if check_price is not None and check_price < ema_120:
                    db_log_warning("simple_ema_guard_block", f"Short blocked — price ${check_price:,.2f} below 15m 120 EMA (${ema_120:,.2f})")
                    send_telegram(
                        f"🚫 <b>Short blocked — price below 15m 120 EMA</b>\n\n"
                        f"Price: ${check_price:,.2f}\n"
                        f"120 EMA: ${ema_120:,.2f}"
                    )
                    return

    # ── Williams %R guard (30m, length 25) ─────────────────────────────
    # Block LONG if WPR is above -35 (price already near recent high —
    # chasing strength). Block SHORT if WPR is below -65 (price already
    # near recent low — chasing weakness). Same "don't chase" philosophy
    # as the EMA guards, just measured differently.
    if WPR_GUARD_ENABLED:
        wpr = get_live_williams_r(interval="30m", length=25)
        if wpr is not None:
            if direction == "long" and wpr > WPR_LONG_MAX:
                db_log_warning("wpr_guard_block", f"Long blocked — WPR {wpr:.1f} above {WPR_LONG_MAX}")
                send_telegram(
                    f"🚫 <b>Long blocked — WPR too high ({wpr:.1f})</b>\n\n"
                    f"Williams %R is above {WPR_LONG_MAX} — price is already near its recent high.\n"
                    f"This is chasing strength. Wait for WPR to pull back below {WPR_LONG_MAX}."
                )
                return
            if direction == "short" and wpr < WPR_SHORT_MIN:
                db_log_warning("wpr_guard_block", f"Short blocked — WPR {wpr:.1f} below {WPR_SHORT_MIN}")
                send_telegram(
                    f"🚫 <b>Short blocked — WPR too low ({wpr:.1f})</b>\n\n"
                    f"Williams %R is below {WPR_SHORT_MIN} — price is already near its recent low.\n"
                    f"This is chasing weakness. Wait for WPR to recover above {WPR_SHORT_MIN}."
                )
                return

    global entry2_placed_at

    # ── Attribute trade to trigger if within 1 hour ───────────────────
    global pending_trigger_id
    if last_trigger and last_trigger_time and (time.time() - last_trigger_time) <= TRIGGER_ATTRIBUTION_SECS:
        pending_trigger_id = db_save_trigger(last_trigger, direction)
        if pending_trigger_id and triggered_by:
            db_link_stress_to_trigger(direction, triggered_by, pending_trigger_id)
    else:
        pending_trigger_id = None

    stop_pct = stop_pct or STOP_PCT
    tp_pct   = tp_pct   or TP_PCT

    side       = "BUY"  if direction == "long"  else "SELL"
    close_side = "SELL" if direction == "long"  else "BUY"

    try:
        existing = get_open_position()
        if existing:
            amt      = float(existing["positionAmt"])
            side_str = "LONG" if amt > 0 else "SHORT"
            send_telegram(
                f"⚠️ <b>Already in a {side_str} position ({amt:+.4f} BTC).</b>\n"
                f"Use /close to exit first."
            )
            return

        set_leverage_for_symbol()

        price      = get_mark_price()
        sym_info   = get_symbol_info()
        qty_step   = sym_info["qty_step"]
        price_tick = sym_info["price_tick"]
        balance    = get_usdt_balance()

        if size_usdt is not None:
            notional = size_usdt * LEVERAGE
            size_label = f"${size_usdt:,.2f}"
        else:
            pct      = size_pct if size_pct is not None else TRADE_SIZE_PCT
            notional = balance * (pct / 100) * LEVERAGE
            size_label = f"{pct}% of balance"

        raw_qty  = notional / price
        quantity = round_step(raw_qty, qty_step)

        if quantity <= 0:
            send_telegram("❌ Position size too small — check your amount or balance.")
            return

        tp2_pct = TP2_PCT
        if direction == "long":
            sl_price  = round_step(price * (1 - stop_pct / 100), price_tick)
            tp1_price = round_step(price * (1 + tp_pct   / 100), price_tick)
            tp2_price = round_step(price * (1 + tp2_pct  / 100), price_tick)
        else:
            sl_price  = round_step(price * (1 + stop_pct / 100), price_tick)
            tp1_price = round_step(price * (1 - tp_pct   / 100), price_tick)
            tp2_price = round_step(price * (1 - tp2_pct  / 100), price_tick)

        qty_tp1 = round_step(quantity * (TP1_CLOSE_PCT / 100), qty_step)
        qty_tp2 = round_step(quantity - qty_tp1, qty_step)
        if qty_tp2 <= 0:
            qty_tp1 = quantity
            qty_tp2 = None

        cancel_open_orders()
        if MAKER_ENTRY:
            entry_resp = place_maker_entry(side, quantity, direction, price_tick)
            if entry_resp is None:
                return  # cancelled by user
        else:
            entry_resp = place_order(side, "MARKET", quantity)
        invalidate_position_cache()  # a new position just opened — don't let a stale "no position" cache linger
        current_trade_entry["price"]     = price
        current_trade_entry["direction"] = direction
        current_trade_entry["sl_price"]  = sl_price
        current_trade_entry["tp1_price"] = tp1_price
        current_trade_entry["tp2_price"] = tp2_price
        current_trade_entry["qty_tp1"]   = qty_tp1
        current_trade_entry["qty_tp2"]   = qty_tp2 or 0
        if triggered_by and entry_resp.get("orderId"):
            indicator_trade_ids.add(str(entry_resp["orderId"]))

        # ── Place REAL exchange-backed SL order on Binance ──────────────
        # Previously the SL only lived in current_trade_entry (a Python
        # dict in memory) and was "enforced" purely by the polling loop
        # comparing mark price to that stored value every cycle. If the bot
        # restarted, crashed, or got rate-limited for even a few minutes —
        # all of which have happened — that in-memory value vanished and
        # NOTHING was protecting the position, even though the Telegram
        # message said "SL updated." TP1/TP2 already have real LIMIT orders
        # placed below (reduce_only) — SL was the one genuine gap. This
        # places a real STOP_MARKET order via Binance's algo order endpoint
        # (mandatory since 2025-12-09 for conditional order types — the old
        # /fapi/v1/order endpoint rejects STOP_MARKET outright), which fires
        # on Binance's own servers regardless of whether this bot is even
        # running. The software check in the polling loop is now a
        # secondary/backup layer, not the only protection.
        close_side = "SELL" if direction == "long" else "BUY"
        try:
            place_algo_order(close_side, "STOP_MARKET", sl_price, close_position=True)
        except Exception as e:
            send_telegram(f"⚠️ <b>Real SL order failed to place</b>\n\n{e}\n\nSoftware SL is still active as backup, but please verify manually.")
            print(f"[Real SL] Placement error: {e}")

        # ── Update trigger_performance row with entry price data ──────
        global trade_mfe, trade_mae, trade_entry_time, retrace_protect_triggered
        trade_mfe        = 0.0
        trade_mae        = 0.0
        trade_entry_time = time.time()
        retrace_protect_triggered = False
        if pending_trigger_id:
            try:
                conn = get_db()
                cur  = conn.cursor()
                cur.execute("""
                    UPDATE trigger_performance
                    SET entry_price = %s, sl_price = %s, tp1_price = %s, tp2_price = %s
                    WHERE id = %s
                """, (price, sl_price, tp1_price, tp2_price, pending_trigger_id))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"[DB] Entry price update error: {e}")

        mirror_open_all(direction, sl_price, tp1_price, tp2_price)

        entry2_qty = round_step(quantity * (ENTRY2_SIZE_PCT / 100), qty_step)
        if entry2_qty > 0:
            if direction == "long":
                entry2_price = round_step(price * (1 - ENTRY2_OFFSET_PCT / 100), price_tick)
            else:
                entry2_price = round_step(price * (1 + ENTRY2_OFFSET_PCT / 100), price_tick)
            e2_resp = place_order(side, "LIMIT", entry2_qty, limit_price=entry2_price)
            register_order(e2_resp, "Entry2")
            entry2_placed_at = time.time()
        else:
            entry2_qty   = 0
            entry2_price = None

        tp1_resp = place_order(close_side, "LIMIT", qty_tp1,
                               limit_price=tp1_price, reduce_only=True)
        register_order(tp1_resp, "TP1")

        if qty_tp2:
            tp2_resp = place_order(close_side, "LIMIT", qty_tp2,
                                   limit_price=tp2_price, reduce_only=True)
            register_order(tp2_resp, "TP2")

        signal_line = f"📡 Signal:   {triggered_by}\n" if triggered_by else ""
        zone_line   = f"📍 Zone:     {zone}\n"          if zone        else ""
        zscore_line = f"📊 Z-Score:  {z_score}\n"       if z_score is not None else ""
        pos_size_usd = quantity * price / LEVERAGE
        entry2_size_usd = (entry2_qty * entry2_price / LEVERAGE) if entry2_qty > 0 and entry2_price else 0
        entry2_line = (
            f"📥 Entry2:  ${entry2_price:,.2f}  (-{ENTRY2_OFFSET_PCT}%) — ${entry2_size_usd:,.2f}\n"
            if entry2_qty > 0 else ""
        )
        tp2_line = f"🎯 TP2:     ${tp2_price:,.2f}  ({tp2_pct}%)\n" if qty_tp2 else ""

        # Max loss/win on initial position size (before Entry2 fills)
        max_loss_usd = quantity * abs(price - sl_price)
        max_win_usd  = qty_tp1 * abs(tp1_price - price)
        if qty_tp2:
            max_win_usd += qty_tp2 * abs(tp2_price - price)

        send_telegram(
            f"{'🟢' if direction == 'long' else '🔴'} <b>{'LONG' if direction == 'long' else 'SHORT'} entered</b>\n\n"
            f"Symbol:     {SYMBOL}\n"
            f"Entry:      ${price:,.2f}\n"
            f"Size:       ${pos_size_usd:,.2f} (×{LEVERAGE} lev)\n"
            f"{signal_line}"
            f"{zone_line}"
            f"{zscore_line}"
            f"{entry2_line}"
            f"\n🛑 SL:      ${sl_price:,.2f}  ({stop_pct}%) — max loss {format_pnl(-max_loss_usd)}\n"
            f"🎯 TP1:     ${tp1_price:,.2f}  ({tp_pct}%) — {TP1_CLOSE_PCT:.0f}% of position\n"
            f"{tp2_line}"
            f"💰 Max win: {format_pnl(max_win_usd)} (TP1+TP2 full)\n"
            f"\nBalance:    ${balance:,.2f} USDT"
        )
        print(f"[Execution] {direction.upper()} — qty {quantity}, SL {sl_price}, TP1 {tp1_price}, TP2 {tp2_price}")
        start_prompt_silence("Position open — stay focused on the trade.")

    except requests.exceptions.HTTPError as e:
        try:
            err_json = e.response.json()
            err_msg  = f"Code {err_json.get('code')}: {err_json.get('msg')}"
        except Exception:
            err_msg  = str(e)
        send_telegram(f"❌ <b>Order failed</b>\n{err_msg}")
        print(f"[Execution ERROR] {err_msg}")
    except Exception as e:
        send_telegram(f"❌ <b>Execution error</b>\n{e}")
        print(f"[Execution ERROR] {e}")


_close_position_lock = threading.Lock()

def close_position_now(reason: str = None) -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled. Set EXECUTION_ENABLED=true to enable.")
        return
    # Non-blocking: if a close is already in progress, reject the duplicate
    # rather than let two /close presses (or a manual /close overlapping a
    # guard-triggered force-close) both hammer Binance for the same position.
    if not _close_position_lock.acquire(blocking=False):
        send_telegram("⏳ A close is already being processed — please wait a moment.")
        return
    try:
        _close_position_inner(reason)
    finally:
        _close_position_lock.release()


def _close_position_inner(reason: str = None) -> None:
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("ℹ️ No open position to close.")
            return

        amt       = float(pos["positionAmt"])
        side      = "SELL" if amt > 0 else "BUY"
        quantity  = abs(amt)
        direction = "LONG" if amt > 0 else "SHORT"

        cancel_open_orders()
        place_order(side, "MARKET", quantity, reduce_only=True)
        invalidate_position_cache()  # position is now closed — don't let a stale cached "still open" linger
        unreal_pnl  = float(pos.get("unRealizedProfit", 0))
        db_update_outcome(pending_trigger_id, "win" if unreal_pnl >= 0 else "loss", unreal_pnl,
                          mae=trade_mae, mfe=trade_mfe)
        current_trade_entry.clear()

        entry_price = float(pos.get("entryPrice", 0))
        unreal_pnl  = float(pos.get("unRealizedProfit", 0))
        mark_price  = get_mark_price()
        header = "🛡️ <b>FORCE CLOSED — {direction}</b>" if reason else "🏳️ <b>{direction} position closed</b>"
        reason_line = f"\nReason:     {reason}" if reason else ""
        send_telegram(
            f"{header.format(direction=direction)}\n\n"
            f"Symbol:     {SYMBOL}\n"
            f"Size:       ${quantity * mark_price / LEVERAGE:,.2f}\n"
            f"Entry:      ${entry_price:,.2f}\n"
            f"Exit:       ${mark_price:,.2f}\n"
            f"Unrealised: {format_pnl(unreal_pnl)}"
            f"{reason_line}"
        )
        mirror_close_all()
        print(f"[Execution] Position closed — {direction} {quantity}" + (f" — {reason}" if reason else ""))

    except requests.exceptions.HTTPError as e:
        err = e.response.json() if e.response else str(e)
        send_telegram(f"❌ <b>Close failed</b>\n{err}")
    except Exception as e:
        send_telegram(f"❌ <b>Close error</b>\n{e}")


def status_message() -> None:
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("📊 No open position.")
            return
        amt         = float(pos["positionAmt"])
        direction   = "LONG 🟢" if amt > 0 else "SHORT 🔴"
        entry_price = float(pos.get("entryPrice", 0))
        mark_price  = get_mark_price()
        unreal_pnl  = float(pos.get("unRealizedProfit", 0))
        liq_price   = float(pos.get("liquidationPrice", 0))

        margin     = abs(amt) * entry_price / LEVERAGE if entry_price > 0 else 0
        unreal_pct = (unreal_pnl / margin * 100) if margin > 0 else 0
        unreal_sign = "+" if unreal_pct >= 0 else ""

        sl_price = current_trade_entry.get("sl_price")
        if sl_price:
            sl_loss    = abs(sl_price - entry_price) * abs(amt) if entry_price > 0 else 0
            sl_pct     = abs(sl_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            sl_line    = f"🛑 SL:         ${sl_price:,.2f}  ({sl_pct:.2f}% — loss: {format_pnl(-sl_loss)})\n"
        else:
            sl_line = "🛑 SL:         Not set (use /sl to set)\n"

        tp1_price = None
        tp2_price = None
        tp1_qty   = None
        tp2_qty   = None
        for oid, meta in tracked_orders.items():
            if meta["label"] == "TP1":
                tp1_price = meta["price"]
                tp1_qty   = meta["qty"]
            elif meta["label"] == "TP2":
                tp2_price = meta["price"]
                tp2_qty   = meta["qty"]

        if tp1_price is None:
            try:
                now_ms  = int(time.time() * 1000)
                params  = {"symbol": SYMBOL, "timestamp": now_ms}
                params  = sign_request(params)
                headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                resp    = requests.get(f"{FUTURES_BASE}/fapi/v1/openOrders",
                                       params=params, headers=headers, timeout=10)
                live_orders = resp.json() if resp.status_code == 200 else []
                close_side  = "SELL" if amt > 0 else "BUY"
                limit_closes = sorted(
                    [o for o in live_orders
                     if o.get("type") == "LIMIT"
                     and o.get("side") == close_side
                     and o.get("reduceOnly")],
                    key=lambda o: float(o["price"]),
                    reverse=(amt < 0)
                )
                valid = [o for o in limit_closes
                         if entry_price > 0
                         and abs(float(o["price"]) - entry_price) / entry_price * 100 <= 1]
                if len(valid) >= 1:
                    tp1_price = float(valid[0]["price"])
                    tp1_qty   = float(valid[0].get("origQty", 0))
                if len(valid) >= 2:
                    tp2_price = float(valid[1]["price"])
                    tp2_qty   = float(valid[1].get("origQty", 0))
            except Exception:
                pass

        if tp1_price and entry_price > 0:
            tp1_profit = abs(tp1_price - entry_price) * (tp1_qty or abs(amt))
            tp1_pct    = abs(tp1_price - entry_price) / entry_price * 100
            tp1_line   = f"🎯 TP1:        ${tp1_price:,.2f}  ({tp1_pct:.2f}% — profit: {format_pnl(tp1_profit)})\n"
        else:
            tp1_line = "🎯 TP1:        Not set\n"

        if tp2_price and entry_price > 0:
            tp2_profit = abs(tp2_price - entry_price) * (tp2_qty or abs(amt))
            tp2_pct    = abs(tp2_price - entry_price) / entry_price * 100
            tp2_line   = f"🎯 TP2:        ${tp2_price:,.2f}  ({tp2_pct:.2f}% — profit: {format_pnl(tp2_profit)})\n"
        else:
            tp2_line = "🎯 TP2:        Not set\n"

        def fmt_position_mins(mins: float) -> str:
            if mins >= 60:
                return f"{int(mins // 60)}h {int(mins % 60)}m"
            return f"{int(mins)}m"

        time_line = (
            f"⏱ In profit: {fmt_position_mins(position_mins_profit)}  |  "
            f"Underwater: {fmt_position_mins(position_mins_under)}\n"
        )

        send_telegram(
            f"📊 <b>Open position: {direction}</b>\n\n"
            f"Symbol:       {SYMBOL}\n"
            f"Size:         ${abs(amt) * mark_price / LEVERAGE:,.2f}  (×{LEVERAGE} lev)\n"
            f"Entry:        ${entry_price:,.2f}\n"
            f"Mark:         ${mark_price:,.2f}\n"
            f"Unrealised:   {format_pnl(unreal_pnl)}  ({unreal_sign}{unreal_pct:.2f}%)\n"
            f"{time_line}"
            f"{sl_line}"
            f"{tp1_line}"
            f"{tp2_line}"
            f"Liquidation:  ${liq_price:,.2f}"
        )
    except Exception as e:
        send_telegram(f"❌ Status error: {e}")


def adjust_sl(new_sl_price: float) -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("ℹ️ No open position to adjust SL on.")
            return

        amt       = float(pos["positionAmt"])
        direction = "LONG" if amt > 0 else "SHORT"
        entry     = float(pos.get("entryPrice", 0))
        quantity  = abs(amt)
        close_side = "SELL" if amt > 0 else "BUY"

        # ── Replace the REAL exchange-backed stop order safely ──
        # Place the NEW SL first, confirm it succeeded, THEN cancel the old
        # one — never the other way round. Cancelling first meant a failed
        # new-SL placement left the position completely unprotected.
        try:
            place_algo_order(close_side, "STOP_MARKET", new_sl_price, close_position=True)
        except Exception as e:
            send_telegram(f"❌ <b>Failed to place new SL order — old SL left in place</b>\n\n{e}\n\nPosition is still protected by the previous stop.")
            print(f"[Adjust SL] Real order placement error: {e}")
            return
        cancel_all_algo_orders(keep_latest=True)

        current_trade_entry["sl_price"] = new_sl_price

        sl_pct   = abs(new_sl_price - entry) / entry * 100 if entry > 0 else 0
        max_loss = abs(new_sl_price - entry) * quantity if entry > 0 else 0
        send_telegram(
            f"✏️ <b>SL updated — {direction}</b>\n\n"
            f"New SL:   ${new_sl_price:,.2f}  ({sl_pct:.2f}% from entry)\n"
            f"Entry:    ${entry:,.2f}\n"
            f"Max loss: {format_pnl(-max_loss)}\n\n"
            f"✅ Real order placed on exchange."
        )
    except requests.exceptions.HTTPError as e:
        try:
            err = e.response.json()
            send_telegram(f"❌ SL adjust failed\nCode {err.get('code')}: {err.get('msg')}")
        except Exception:
            send_telegram(f"❌ SL adjust failed: {e}")
    except Exception as e:
        send_telegram(f"❌ SL adjust error: {e}")


def adjust_tp(new_tp_price: float, label: str = "TP1") -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("ℹ️ No open position to adjust TP on.")
            return

        amt        = float(pos["positionAmt"])
        direction  = "LONG" if amt > 0 else "SHORT"
        close_side = "SELL" if amt > 0 else "BUY"
        entry      = float(pos.get("entryPrice", 0))
        sym_info   = get_symbol_info()
        price_tick = sym_info["price_tick"]
        qty_step   = sym_info["qty_step"]

        tp_order_id = None
        tp_qty      = None
        for oid, meta in list(tracked_orders.items()):
            if meta["label"] == label:
                tp_order_id = int(oid)
                tp_qty      = meta["qty"]
                break

        if tp_order_id:
            now_ms  = int(time.time() * 1000)
            params  = {"symbol": SYMBOL, "orderId": tp_order_id, "timestamp": now_ms}
            params  = sign_request(params)
            headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
            try:
                requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                params=params, headers=headers, timeout=10)
            except Exception:
                pass
            tracked_orders.pop(str(tp_order_id), None)
        else:
            try:
                now_ms      = int(time.time() * 1000)
                params      = {"symbol": SYMBOL, "timestamp": now_ms}
                params      = sign_request(params)
                headers     = {"X-MBX-APIKEY": BINANCE_API_KEY}
                resp        = requests.get(f"{FUTURES_BASE}/fapi/v1/openOrders",
                                           params=params, headers=headers, timeout=10)
                live_orders = resp.json() if resp.status_code == 200 else []
                for o in live_orders:
                    if o.get("type") == "LIMIT" and o.get("side") == close_side and o.get("reduceOnly"):
                        cancel_params = {"symbol": SYMBOL, "orderId": o["orderId"], "timestamp": int(time.time()*1000)}
                        cancel_params = sign_request(cancel_params)
                        requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                        params=cancel_params, headers={"X-MBX-APIKEY": BINANCE_API_KEY}, timeout=10)
                        if tp_qty is None:
                            tp_qty = float(o.get("origQty", 0))
                        break
            except Exception:
                pass

        quantity = round_step(tp_qty or abs(amt), qty_step)
        resp = place_order(close_side, "LIMIT", quantity,
                           limit_price=new_tp_price, reduce_only=True)
        register_order(resp, label)

        # ── Auto-adjust TP2 if new TP1 would conflict ────────────────
        if label == "TP1":
            tp2_meta = next(((oid, m) for oid, m in tracked_orders.items() if m["label"] == "TP2"), None)
            if tp2_meta:
                tp2_oid, tp2_m = tp2_meta
                tp2_price = tp2_m["price"]
                tp2_qty   = tp2_m["qty"]
                conflict  = (amt > 0 and new_tp_price >= tp2_price) or \
                            (amt < 0 and new_tp_price <= tp2_price)
                if conflict:
                    try:
                        # Cancel old TP2
                        now_ms2  = int(time.time() * 1000)
                        params2  = {"symbol": SYMBOL, "orderId": int(tp2_oid), "timestamp": now_ms2}
                        params2  = sign_request(params2)
                        requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                        params=params2, headers={"X-MBX-APIKEY": BINANCE_API_KEY}, timeout=10)
                        tracked_orders.pop(str(tp2_oid), None)
                        # Place new TP2 same distance from new TP1 as it was from old TP1
                        old_tp1_price = current_trade_entry.get("tp1_price", entry)
                        tp2_offset    = abs(tp2_price - old_tp1_price)
                        if amt > 0:  # long
                            new_tp2_price = round_step(new_tp_price + tp2_offset, price_tick)
                        else:        # short
                            new_tp2_price = round_step(new_tp_price - tp2_offset, price_tick)
                        tp2_qty_rounded = round_step(tp2_qty or abs(amt) * (1 - TP1_CLOSE_PCT / 100), qty_step)
                        resp2 = place_order(close_side, "LIMIT", tp2_qty_rounded,
                                            limit_price=new_tp2_price, reduce_only=True)
                        register_order(resp2, "TP2")
                        current_trade_entry["tp2_price"] = new_tp2_price
                        send_telegram(
                            f"🔄 <b>TP2 auto-adjusted to avoid conflict</b>\n\n"
                            f"New TP2: ${new_tp2_price:,.2f}  (same offset from new TP1)"
                        )
                    except Exception as e:
                        send_telegram(f"⚠️ TP2 auto-adjust failed: {e}\nUse /tp2 to reset it manually.")

        max_profit = abs(new_tp_price - entry) * quantity if entry > 0 else 0
        tp_pct     = abs(new_tp_price - entry) / entry * 100 if entry > 0 else 0

        send_telegram(
            f"✏️ <b>{label} updated — {direction}</b>\n\n"
            f"Entry:       ${entry:,.2f}\n"
            f"New {label}:   ${new_tp_price:,.2f}  ({tp_pct:.2f}% from entry)\n"
            f"Max profit:  {format_pnl(max_profit)}"
        )
    except requests.exceptions.HTTPError as e:
        try:
            err = e.response.json()
            send_telegram(f"❌ {label} adjust failed\nCode {err.get('code')}: {err.get('msg')}")
        except Exception:
            send_telegram(f"❌ {label} adjust failed: {e}")
    except Exception as e:
        send_telegram(f"❌ {label} adjust error: {e}")


def move_to_breakeven() -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("ℹ️ No open position.")
            return

        entry     = float(pos.get("entryPrice", 0))
        unreal    = float(pos.get("unRealizedProfit", 0))
        amt       = float(pos["positionAmt"])
        direction = "LONG" if amt > 0 else "SHORT"

        if unreal <= 0:
            send_telegram(
                f"⚠️ <b>Not in profit yet.</b>\n"
                f"Unrealised P&L is {format_pnl(unreal)} — "
                f"moving SL to breakeven would lock in a loss."
            )
            return

        sym_info   = get_symbol_info()
        price_tick = sym_info["price_tick"]
        be_price   = round_step(entry, price_tick)
        mark       = get_mark_price()

        adjust_sl(be_price)
        current_trade_entry["sl_price"] = be_price
        send_telegram(
            f"⚖️ <b>SL moved to breakeven — {direction}</b>\n\n"
            f"Entry price:     ${entry:,.2f}\n"
            f"New SL:          ${be_price:,.2f}  (exact entry — zero risk from here)\n"
            f"Current mark:    ${mark:,.2f}\n"
            f"Unrealised P&L:  {format_pnl(unreal)} (not locked in — still floating)\n\n"
            f"Worst case from here is a scratch trade. Upside still fully open."
        )
    except Exception as e:
        send_telegram(f"❌ Breakeven error: {e}")


def partial_close(pct: float) -> None:
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        pos = get_open_position()
        if not pos:
            send_telegram("ℹ️ No open position to partially close.")
            return

        amt        = float(pos["positionAmt"])
        direction  = "LONG" if amt > 0 else "SHORT"
        close_side = "SELL" if amt > 0 else "BUY"
        sym_info   = get_symbol_info()
        qty_step   = sym_info["qty_step"]

        close_qty = round_step(abs(amt) * (pct / 100), qty_step)
        if close_qty <= 0:
            send_telegram("❌ Quantity too small to close.")
            return

        place_order(close_side, "MARKET", close_qty, reduce_only=True)
        invalidate_position_cache()  # position size just changed — don't let a stale cache linger

        remaining_usd = (abs(amt) - close_qty) * get_mark_price()
        send_telegram(
            f"✂️ <b>Partial close {pct:.0f}% — {direction}</b>\n\n"
            f"Closed:     ${close_qty * get_mark_price():,.2f}\n"
            f"Remaining:  ${remaining_usd:,.2f}"
        )
    except requests.exceptions.HTTPError as e:
        try:
            err = e.response.json()
            send_telegram(f"❌ Partial close failed\nCode {err.get('code')}: {err.get('msg')}")
        except Exception:
            send_telegram(f"❌ Partial close failed: {e}")
    except Exception as e:
        send_telegram(f"❌ Partial close error: {e}")


# ─────────────────────────────────────────────
# PROFIT ALERT STATE
# ─────────────────────────────────────────────
BREAKEVEN_SUGGEST_PCT   = float(os.environ.get("BREAKEVEN_SUGGEST_PCT", "0.20"))
BREAKEVEN_TIME_MINS     = float(os.environ.get("BREAKEVEN_TIME_MINS", "40"))   # suggest if in profit this long without TP1
RETRACE_PROTECT_PCT     = float(os.environ.get("RETRACE_PROTECT_PCT", "0.35")) # auto-protect if pulled back from this much profit
RETRACE_LOCK_IN_PCT     = float(os.environ.get("RETRACE_LOCK_IN_PCT", "0.15")) # % profit actually locked in (SL moved here, not to breakeven)
breakeven_suggested     = False
breakeven_last_suggested_at: float = 0.0
BREAKEVEN_REPEAT_MINS   = 30

def check_profit_alert(pos: dict) -> None:
    global breakeven_suggested, breakeven_last_suggested_at
    if not pos:
        return
    if breakeven_suggested and (time.time() - breakeven_last_suggested_at) < BREAKEVEN_REPEAT_MINS * 60:
        return

    entry     = float(pos.get("entryPrice", 0))
    mark      = get_mark_price()
    amt       = float(pos["positionAmt"])
    direction = "LONG" if amt > 0 else "SHORT"

    if entry <= 0:
        return

    pct_move = ((mark - entry) / entry * 100) if amt > 0 else ((entry - mark) / entry * 100)

    triggered_by_pct  = pct_move >= BREAKEVEN_SUGGEST_PCT
    triggered_by_time = position_mins_profit >= BREAKEVEN_TIME_MINS

    if triggered_by_pct or triggered_by_time:
        if triggered_by_time and not triggered_by_pct:
            reason_line = f"In profit for {position_mins_profit:.0f}m without hitting TP1"
        else:
            reason_line = f"Position up {pct_move:.2f}%"
        send_telegram(
            f"💡 <b>{reason_line} — consider breakeven</b>\n\n"
            f"Entry:  ${entry:,.2f}\n"
            f"Mark:   ${mark:,.2f}\n\n"
            f"Send /breakeven to move SL to entry and protect the trade."
        )
        breakeven_suggested = True
        breakeven_last_suggested_at = time.time()


# ─────────────────────────────────────────────
# 8AM MORNING BRIEF
# ─────────────────────────────────────────────
morning_brief_sent_today = False
weekly_digest_sent_today = False
weekly_okr_sent_today    = False
monthly_review_sent_today = False
daily_bias               = None
bias_question_sent_today = False
pending_signal           = None
SIGNAL_EXPIRY_SECS       = 5400  # 90 minutes
pending_stress_check     = None   # {direction, label, message_id, timestamp}
elevated_silence_until: float = 0.0  # suppress all prompts until this timestamp
pending_bias_reset: dict     = None   # {direction, structure_break}

# Today's trigger activity — reset at midnight
trigger_counts_today: dict      = {}   # {sig_key: count}
long_trigger_times_today: list  = []   # timestamps of long triggers today
short_trigger_times_today: list = []   # timestamps of short triggers today


def get_signal_track_record(sig_key: str) -> str:
    """Returns a short stat line for this signal's historical win rate and invalidation rate, or '' if not enough data."""
    if not DATABASE_URL or not sig_key:
        return ""
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                COUNT(*) FILTER (WHERE outcome = 'win') as wins
            FROM trigger_performance
            WHERE trigger = %s
        """, (sig_key,))
        r = cur.fetchone()

        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM trigger_fires WHERE trigger = %s) as fires,
                (SELECT COUNT(*) FROM invalidation_log WHERE trigger = %s) as invalidations
        """, (sig_key, sig_key))
        r2 = cur.fetchone()
        cur.close()
        conn.close()

        parts = []
        total = r["total"] if r else 0
        if total and total >= 3:
            wr = r["wins"] / total * 100
            parts.append(f"{wr:.0f}% WR ({total} trades)")
        fires = r2["fires"] if r2 else 0
        invs  = r2["invalidations"] if r2 else 0
        if fires and fires >= 3:
            inv_rate = invs / fires * 100
            parts.append(f"{inv_rate:.0f}% invalidation rate")
        if not parts:
            return ""
        return "📊 " + "  |  ".join(parts)
    except Exception as e:
        print(f"[Signal Track Record] Error: {e}")
        return ""


def send_signal_prompt(direction: str, label: str, sig_key: str = None):
    global pending_signal
    if elevated_silence_until > time.time():
        print(f"[Signal] Suppressed {label} — elevated silence active")
        return
    if stopped_today:
        print(f"[Signal] Suppressed {label} — daily trade limit hit")
        return
    emoji = "🟢" if direction == "long" else "🔴"
    track_record = get_signal_track_record(sig_key)
    track_line = f"\n{track_record}\n" if track_record else ""
    text = (
        f"⚡ <b>{label}</b>\n\n"
        f"Direction: {emoji} {direction.upper()}\n"
        f"{track_line}"
        f"\nDo you want to take this trade?\n"
        f"⏱ Expires in 90 minutes."
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "✅ Take Trade", "callback_data": f"take_{direction}"},
                {"text": "❌ Skip",       "callback_data": f"skip_{direction}"}
            ]]
        }
    }
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
        msg_id = resp.json().get("result", {}).get("message_id")
        pending_signal = {
            "direction": direction,
            "label":     label,
            "timestamp": time.time(),
            "message_id": msg_id
        }
    except Exception as e:
        print(f"[Signal Prompt] Error: {e}")


def answer_callback(callback_id: str, text: str = ""):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=5
        )
    except Exception as e:
        print(f"[Callback] Error: {e}")


def edit_prompt_message(message_id: int, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "message_id": message_id,
                "text":       text,
                "parse_mode": "HTML"
            },
            timeout=5
        )
    except Exception as e:
        print(f"[Edit Message] Error: {e}")


def send_bias_invalidation_alert(label: str, direction: str, signal_key: str):
    global pending_bias_reset
    emoji     = "🟢" if direction == "long" else "🔴"
    new_bias  = "bullish" if direction == "long" else "bearish"
    text = (
        f"⚠️ <b>HTF regime change detected</b>\n\n"
        f"Structure break: {label}\n"
        f"Direction: {emoji} {direction.upper()}\n\n"
        f"Your bias is currently {daily_bias.upper()}.\n"
        f"This structure break suggests {new_bias.upper()} may be taking over.\n\n"
        f"Review and reset your bias?"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "🔄 Reset to Bullish",  "callback_data": f"reset_bias_bullish_{signal_key}"},
                {"text": "🔄 Reset to Bearish",  "callback_data": f"reset_bias_bearish_{signal_key}"},
                {"text": "❌ Ignore",            "callback_data": f"ignore_reset_{signal_key}"}
            ]]
        }
    }
    try:
        resp   = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
        msg_id = resp.json().get("result", {}).get("message_id")
        pending_bias_reset = {
            "signal_key": signal_key,
            "message_id": msg_id,
            "timestamp": time.time()
        }
    except Exception as e:
        print(f"[HTF Alert] Error: {e}")


def send_stress_check(direction: str, label: str):
    global pending_stress_check
    emoji = "🟢" if direction == "long" else "🔴"
    text = (
        f"🧠 <b>Pre-trade check — {label}</b>\n\n"
        f"Direction: {emoji} {direction.upper()}\n\n"
        f"What is your current stress state?"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "😌 Relaxed",  "callback_data": f"stress_relaxed_{direction}"},
                {"text": "🎯 Focused",  "callback_data": f"stress_focused_{direction}"},
                {"text": "😤 Elevated", "callback_data": f"stress_elevated_{direction}"},
                {"text": "🌀 Distracted", "callback_data": f"stress_distracted_{direction}"}
            ]]
        }
    }
    try:
        resp   = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
        msg_id = resp.json().get("result", {}).get("message_id")
        pending_stress_check = {
            "direction":  direction,
            "label":      label,
            "message_id": msg_id,
            "timestamp":  time.time()
        }
    except Exception as e:
        print(f"[Stress Check] Error: {e}")


def start_prompt_silence(reason: str = ""):
    """Silence all signal prompts for 10 minutes and notify when lifted."""
    global elevated_silence_until
    elevated_silence_until = time.time() + 600
    def _lift():
        time.sleep(600)
        send_telegram(f"🔔 <b>Prompts active again.</b>{(' ' + reason) if reason else ''}")
    threading.Thread(target=_lift, daemon=True).start()


def notify_client(c, text: str):
    if not c.tg_token or not c.tg_chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{c.tg_token}/sendMessage",
            json={"chat_id": c.tg_chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"[{c.label()} TG] Error: {e}")


def hl_get_info():
    return HLInfo(HL_BASE_URL, skip_ws=True)


def hl_get_mark_price() -> float:
    info = hl_get_info()
    all_mids = info.all_mids()
    price = float(all_mids.get(HL_COIN, 0))
    print(f"[HL Mirror] Mark price for {HL_COIN}: {price}")
    return price


def client_get_exchange(c):
    account = EthAccount.from_key(c.private_key)
    return HLExchange(account, HL_BASE_URL, account_address=c.address)


def client_get_balance(c) -> float:
    info = hl_get_info()
    state = info.user_state(c.address)
    print(f"[{c.label()}] Raw user state: {state.get('marginSummary', {})}")
    return float(state["marginSummary"]["accountValue"])


def client_calculate_size(c) -> float:
    try:
        balance    = client_get_balance(c)
        mark_price = hl_get_mark_price()
        print(f"[{c.label()}] Balance: {balance}, Mark: {mark_price}")
        if mark_price <= 0 or balance <= 0:
            print(f"[{c.label()}] Size calc failed — balance={balance} mark={mark_price}")
            return 0.0
        margin   = balance * (c.size_pct / 100)
        notional = margin * c.leverage
        size_btc = notional / mark_price
        print(f"[{c.label()}] Size: {size_btc} BTC (margin={margin}, notional={notional})")
        return round(size_btc, 4)
    except Exception as e:
        print(f"[{c.label()}] Size calc error: {e}")
        send_telegram(f"⚠️ <b>{c.label()} size calc error</b>\n\n{e}")
        return 0.0


def client_mirror_open(c, direction: str, sl_price: float = None, tp1_price: float = None, tp2_price: float = None):
    if not c.is_configured():
        print(f"[{c.label()}] Not configured — skipping")
        return
    try:
        exchange = client_get_exchange(c)
        size     = client_calculate_size(c)

        if size <= 0:
            send_telegram(f"⚠️ {c.label()}: could not calculate size — skipping")
            return

        is_buy = direction == "long"

        exchange.update_leverage(c.leverage, HL_COIN, is_cross=True)

        result = exchange.market_open(HL_COIN, is_buy, size, None, 0.05)

        if result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            avg_px   = float(statuses[0].get("filled", {}).get("avgPx", 0)) if statuses else 0
            emoji    = "🟢" if is_buy else "🔴"
            send_telegram(
                f"✅ <b>{c.label()} Mirror opened</b>\n\n"
                f"Direction: {emoji} {direction.upper()}\n"
                f"Size:      {size} BTC\n"
                f"Entry:     ${avg_px:,.2f}"
            )

            # ── Place REAL trigger orders on Hyperliquid for SL/TP1/TP2 ──
            # Every client gets real resting orders, not just a Telegram
            # message — otherwise the position is naked between polling
            # cycles, protected by nothing but this bot noticing and
            # manually closing it.
            close_is_buy = not is_buy  # closing side is opposite of entry
            real_sl_line, real_tp1_line, real_tp2_line = "", "", ""
            if sl_price:
                try:
                    sl_order_type = HLOrderType(trigger=HLTriggerOrderType(
                        triggerPx=round(sl_price), isMarket=True, tpsl="sl"
                    ))
                    sl_result = exchange.order(HL_COIN, close_is_buy, size, round(sl_price), sl_order_type, reduce_only=True)
                    if sl_result.get("status") == "ok":
                        real_sl_line = f"🛑 SL:  ${sl_price:,.2f} (real order placed)\n"
                    else:
                        real_sl_line = f"🛑 SL:  ${sl_price:,.2f} ⚠️ FAILED TO PLACE — verify manually\n"
                        print(f"[{c.label()}] SL order failed: {sl_result}")
                except Exception as e:
                    real_sl_line = f"🛑 SL:  ${sl_price:,.2f} ⚠️ FAILED TO PLACE — verify manually\n"
                    print(f"[{c.label()}] SL order error: {e}")

            if tp1_price:
                try:
                    tp1_order_type = HLOrderType(trigger=HLTriggerOrderType(
                        triggerPx=round(tp1_price), isMarket=True, tpsl="tp"
                    ))
                    tp1_result = exchange.order(HL_COIN, close_is_buy, size, round(tp1_price), tp1_order_type, reduce_only=True)
                    if tp1_result.get("status") == "ok":
                        real_tp1_line = f"🎯 TP1: ${tp1_price:,.2f} (real order placed)\n"
                    else:
                        real_tp1_line = f"🎯 TP1: ${tp1_price:,.2f} ⚠️ FAILED TO PLACE\n"
                        print(f"[{c.label()}] TP1 order failed: {tp1_result}")
                except Exception as e:
                    real_tp1_line = f"🎯 TP1: ${tp1_price:,.2f} ⚠️ FAILED TO PLACE\n"
                    print(f"[{c.label()}] TP1 order error: {e}")

            if tp2_price:
                try:
                    tp2_order_type = HLOrderType(trigger=HLTriggerOrderType(
                        triggerPx=round(tp2_price), isMarket=True, tpsl="tp"
                    ))
                    tp2_result = exchange.order(HL_COIN, close_is_buy, size, round(tp2_price), tp2_order_type, reduce_only=True)
                    if tp2_result.get("status") == "ok":
                        real_tp2_line = f"🎯 TP2: ${tp2_price:,.2f} (real order placed)\n"
                    else:
                        real_tp2_line = f"🎯 TP2: ${tp2_price:,.2f} ⚠️ FAILED TO PLACE\n"
                        print(f"[{c.label()}] TP2 order failed: {tp2_result}")
                except Exception as e:
                    real_tp2_line = f"🎯 TP2: ${tp2_price:,.2f} ⚠️ FAILED TO PLACE\n"
                    print(f"[{c.label()}] TP2 order error: {e}")

            notify_client(c,
                f"{emoji} <b>Trade Opened</b>\n\n"
                f"Direction: {direction.upper()}\n"
                f"Entry:     ${avg_px:,.2f}\n"
                f"{real_sl_line}"
                f"{real_tp1_line}"
                f"{real_tp2_line}"
            )
        else:
            send_telegram(f"⚠️ <b>{c.label()} Mirror failed to open</b>\n\n{result}")
            print(f"[{c.label()}] Open failed: {result}")

    except Exception as e:
        send_telegram(f"⚠️ <b>{c.label()} Mirror error (open)</b>\n\n{e}")
        print(f"[{c.label()}] Open error: {e}")


def client_mirror_close(c, max_retries: int = 4, retry_delay: int = 15):
    if not c.is_configured():
        return
    try:
        info  = hl_get_info()
        state = info.user_state(c.address)

        # Find the open BTC position
        position  = None
        positions = state.get("assetPositions", [])
        for p in positions:
            pos_data = p.get("position", {})
            if pos_data.get("coin") == HL_COIN:
                szi = float(pos_data.get("szi", 0))
                if szi != 0:
                    position = pos_data
                    break

        if not position:
            send_telegram(f"⚠️ <b>{c.label()} Mirror close</b> — no open position found on HL.")
            return

        close_qty = abs(float(position["szi"]))
        szi       = float(position["szi"])
        is_buy    = szi < 0   # short position → close with buy, long → close with sell
        entry_px  = float(position.get("entryPx", 0))
        print(f"[{c.label()}] Closing {close_qty} BTC — {'BUY' if is_buy else 'SELL'} — entry was ${entry_px:,.2f}")

        exchange   = client_get_exchange(c)

        # ── Cancel any lingering SL/TP trigger orders first ─────────────
        # Otherwise a stale reduce-only trigger order from this position
        # could unexpectedly interact with whatever opens next.
        try:
            open_orders = info.open_orders(c.address)
            for o in open_orders:
                if o.get("coin") == HL_COIN:
                    exchange.cancel(HL_COIN, o.get("oid"))
        except Exception as e:
            print(f"[{c.label()}] Order cancel error (non-fatal): {e}")

        avg_px     = 0
        closed_pnl = 0
        still_open = True

        for attempt in range(1, max_retries + 1):
            # Use plain market_open in opposite direction — HL one-way netting closes the position
            # reduce_only removed as it may be rejected in agent setups
            result = exchange.market_open(HL_COIN, is_buy, close_qty, None, 0.05)
            print(f"[{c.label()}] Close attempt {attempt}/{max_retries} result: {result}")

            if result.get("status") == "ok":
                statuses   = result.get("response", {}).get("data", {}).get("statuses", [])
                avg_px     = float(statuses[0].get("filled", {}).get("avgPx", 0)) if statuses else 0
                closed_pnl = float(statuses[0].get("filled", {}).get("closedPnl", 0)) if statuses else 0

            # Verify with a fresh info call
            time.sleep(3)
            info2      = hl_get_info()
            state2     = info2.user_state(c.address)
            positions2 = state2.get("assetPositions", [])
            still_open = any(
                p.get("position", {}).get("coin") == HL_COIN
                and float(p.get("position", {}).get("szi", 0)) != 0
                for p in positions2
            )

            if not still_open:
                # Manual PnL fallback — HL's closedPnl field is unreliable/often missing
                if closed_pnl == 0 and avg_px > 0 and entry_px > 0:
                    if is_buy:    # was short, profit if exit < entry
                        closed_pnl = (entry_px - avg_px) * close_qty
                    else:         # was long, profit if exit > entry
                        closed_pnl = (avg_px - entry_px) * close_qty
                    print(f"[{c.label()}] closedPnl was 0/missing — calculated manually: ${closed_pnl:+.2f}")
                break  # confirmed closed — stop retrying

            if attempt < max_retries:
                send_telegram(
                    f"⚠️ <b>{c.label()} Mirror close attempt {attempt} failed — retrying in {retry_delay}s</b>\n\n"
                    f"{c.label()} position still open. Auto-retry {attempt}/{max_retries}."
                )
                # Recalculate qty in case it partially filled
                close_qty = abs(float(position_data.get("szi", close_qty))) if (position_data := next(
                    (p.get("position", {}) for p in positions2 if p.get("position", {}).get("coin") == HL_COIN), None
                )) else close_qty
                time.sleep(retry_delay)

        if still_open:
            send_telegram(
                f"🚨 <b>{c.label()} Mirror close FAILED after {max_retries} attempts — UNPROTECTED POSITION</b>\n\n"
                f"{c.label()}'s HL position is still open with no SL/TP watching it.\n"
                f"Close it manually NOW at app.hyperliquid.xyz\n\n"
                f"{c.label()} address: <code>{c.address}</code>"
            )
            notify_client(c,
                f"🚨 <b>Action needed — your position is still open</b>\n\n"
                f"Please check your account at app.hyperliquid.xyz immediately."
            )
            return

        emoji       = "✅" if closed_pnl >= 0 else "❌"
        close_label = "Target Hit" if closed_pnl > 0 else "Stop Hit" if closed_pnl < 0 else "Trade Closed"
        c.trades_today += 1
        if closed_pnl > 0:
            c.wins_today += 1
        elif closed_pnl < 0:
            c.losses_today += 1
        send_telegram(
            f"{emoji} <b>{c.label()} Mirror closed</b>\n\n"
            f"Exit:  ${avg_px:,.2f}\n"
            f"PnL:   ${closed_pnl:+.2f}"
        )
        notify_client(c,
            f"{emoji} <b>{close_label}</b>\n\n"
            f"Exit:  ${avg_px:,.2f}\n"
            f"PnL:   ${closed_pnl:+.2f}"
        )

    except Exception as e:
        send_telegram(f"⚠️ <b>{c.label()} Mirror error (close)</b>\n\n{e}")
        print(f"[{c.label()}] Close error: {e}")


def mirror_open_all(direction: str, sl_price: float = None, tp1_price: float = None, tp2_price: float = None):
    """Mirror a fresh entry to every configured client, each in its own thread."""
    for c in CLIENTS:
        if c.is_configured():
            threading.Thread(target=client_mirror_open, args=(c, direction, sl_price, tp1_price, tp2_price), daemon=True).start()


def mirror_close_all(max_retries: int = 4, retry_delay: int = 15):
    """Close the mirrored position on every configured client, each in its own thread."""
    for c in CLIENTS:
        if c.is_configured():
            threading.Thread(target=client_mirror_close, args=(c, max_retries, retry_delay), daemon=True).start()


def execute_signal_trade(direction: str, label: str):
    """
    Execute a trade confirmed via Take Trade button.
    Checks time blocks, then goes to the stress-check step.
    """
    now_uk_full = datetime.now(UK_TZ)
    uk_now = now_uk_full.time()

    # ── Time blocks ───────────────────────────────────────────────────
    if now_uk_full.weekday() == 4 and uk_now >= dt_time(19, 0):
        send_telegram(
            f"🚫 <b>{direction.upper()} blocked — Friday cutoff (19:00)</b>\n\n"
            f"No new positions after 19:00 on Fridays. Week's done — see you Monday."
        )
        return
    if COOLDOWN_START <= uk_now <= COOLDOWN_END:
        send_telegram(
            f"🚫 <b>{direction.upper()} blocked — US open cooldown</b>\n\n"
            f"No trades between {COOLDOWN_START.strftime('%H:%M')} and {COOLDOWN_END.strftime('%H:%M')} UK time."
        )
        return
    if uk_now >= NIGHT_BLOCK_START:
        send_telegram(
            f"🚫 <b>{direction.upper()} blocked — night time</b>\n\n"
            f"No trades after 10:15pm UK time."
        )
        return

    # ── All clear — stress check before executing ─────────────────────
    send_stress_check(direction, label)


def send_bias_question():
    send_telegram(
        "🧭 <b>Good morning. What is the HTF 4H trend today?</b>\n\n"
        "Reply with:\n"
        "/bullish — bullish bias\n"
        "/bearish — bearish bias\n\n"
        "⚠️ Trades are blocked until you set your bias."
    )


def send_morning_brief():
    weekly = load_last_7_days_stats()
    if not weekly:
        send_telegram(
            "☀️ <b>Good morning.</b>\n\n"
            "No stats from yesterday yet. Get to work. 💪"
        )
        return

    yesterday = weekly[-1]
    week_pnl  = sum(d["net_pnl"] for d in weekly)
    gap       = WEEKLY_PNL_TARGET - week_pnl

    day_emoji = "✅" if yesterday["net_pnl"] > 0 else "❌"
    yest_line = (
        f"{day_emoji} Yesterday:  {format_pnl(yesterday['net_pnl'])} "
        f"({yesterday['wins']}W / {yesterday['losses']}L, "
        f"{yesterday['win_rate']:.0f}% WR)"
    )

    # ── 7-day rolling win rate vs 60% target ────────────────────────────
    winrate_line = ""
    if DATABASE_URL:
        try:
            conn = get_db()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                    COUNT(*) FILTER (WHERE outcome = 'loss') as losses
                FROM trigger_performance
                WHERE entry_time >= NOW() - INTERVAL '7 days'
                  AND outcome IN ('win', 'loss')
            """)
            r = cur.fetchone()
            cur.close()
            conn.close()
            total_7d = (r["wins"] or 0) + (r["losses"] or 0) if r else 0
            if total_7d >= 3:
                wr_7d = r["wins"] / total_7d * 100
                wr_emoji = "✅" if wr_7d >= 60 else "⚠️"
                winrate_line = f"{wr_emoji} 7d win rate: {wr_7d:.0f}% (target 60%+)"
        except Exception as e:
            print(f"[Morning Brief] Win rate calc error: {e}")

    if gap <= 0:
        week_line = f"🏅 Weekly target: HIT (+{format_pnl(abs(gap))} over)"
    else:
        week_line = f"📊 Weekly target: {format_pnl(gap)} to go ({format_pnl(week_pnl)} so far)"

    try:
        balance = get_usdt_balance()
        balance_yesterday = balance - yesterday["net_pnl"]
        bal_change = balance - balance_yesterday
        bal_emoji  = "🟢" if bal_change >= 0 else "🔴"
        bal_line = (
            f"\n💰 <b>BALANCE: ${balance:,.2f} USDT</b>\n"
            f"{bal_emoji} {format_pnl(bal_change)} vs yesterday (${balance_yesterday:,.2f})"
        )
    except Exception:
        bal_line = ""

    # ── Daily game plan — week-so-far stats, with fallback to 30d if thin ──
    plan_line = ""
    if DATABASE_URL:
        try:
            uk_now = datetime.now(UK_TZ)
            week_start_uk = uk_now - timedelta(days=uk_now.weekday())  # Monday this week
            week_start_uk = week_start_uk.replace(hour=0, minute=0, second=0, microsecond=0)

            conn = get_db()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # ── Try week-so-far first ──
            cur.execute("""
                SELECT
                    AVG(pnl) FILTER (WHERE outcome = 'win')  as avg_win,
                    AVG(pnl) FILTER (WHERE outcome = 'loss') as avg_loss,
                    COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                    COUNT(*) FILTER (WHERE outcome = 'loss') as losses
                FROM trigger_performance
                WHERE entry_time >= %s
                  AND outcome IN ('win', 'loss')
            """, (week_start_uk.astimezone(timezone.utc),))
            r = cur.fetchone()
            total = (r["wins"] or 0) + (r["losses"] or 0) if r else 0
            data_source = "this week so far"
            confidence_note = ""

            # ── Fall back to last 30 days if week-so-far is too thin ──
            if total < 5:
                cur.execute("""
                    SELECT
                        AVG(pnl) FILTER (WHERE outcome = 'win')  as avg_win,
                        AVG(pnl) FILTER (WHERE outcome = 'loss') as avg_loss,
                        COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                        COUNT(*) FILTER (WHERE outcome = 'loss') as losses
                    FROM trigger_performance
                    WHERE entry_time >= NOW() - INTERVAL '30 days'
                      AND outcome IN ('win', 'loss')
                """)
                r = cur.fetchone()
                total = (r["wins"] or 0) + (r["losses"] or 0) if r else 0
                data_source = "last 30d (week-so-far too thin)"
                confidence_note = "\n  ⚠️ Early in the week — using 30d average as a fallback, not week-specific yet."

            # ── Avg fee per trade, from TODAY's live Binance data ──────
            # (fees aren't stored in trigger_performance historically, only
            # available live per-day from Binance directly)
            avg_fee_today = 0.0
            try:
                todays_trades_for_fee = get_futures_trades_today()
                todays_stats_for_fee  = build_stats(todays_trades_for_fee)
                trades_today_count = todays_stats_for_fee.get("closed_positions", 0)
                if trades_today_count > 0:
                    avg_fee_today = todays_stats_for_fee.get("total_fees", 0) / trades_today_count
            except Exception as fee_e:
                print(f"[Morning Brief] Fee calc error: {fee_e}")

            # ── Worst 2-hour window, week-so-far, fallback to 30d ──
            cur.execute("""
                SELECT
                    (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2 as chunk_start,
                    COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                    COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                    COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
                FROM trigger_performance
                WHERE close_time IS NOT NULL
                  AND close_time >= %s
                  AND outcome IN ('win', 'loss')
                GROUP BY (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2
                ORDER BY net_pnl ASC
            """, (week_start_uk.astimezone(timezone.utc),))
            window_rows = cur.fetchall()
            window_source = "this week"
            if not window_rows or sum((wr["wins"] or 0) + (wr["losses"] or 0) for wr in window_rows) < 5:
                cur.execute("""
                    SELECT
                        (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2 as chunk_start,
                        COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                        COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                        COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
                    FROM trigger_performance
                    WHERE close_time IS NOT NULL
                      AND close_time >= NOW() - INTERVAL '30 days'
                      AND outcome IN ('win', 'loss')
                    GROUP BY (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2
                    ORDER BY net_pnl ASC
                """)
                window_rows = cur.fetchall()
                window_source = "last 30d"

            cur.close()
            conn.close()

            plan_parts = []

            if r and total >= 2 and r["avg_win"]:
                win_rate  = r["wins"] / total
                loss_rate = r["losses"] / total
                avg_win   = float(r["avg_win"])
                avg_loss  = abs(float(r["avg_loss"])) if r["avg_loss"] else 0
                avg_fee   = avg_fee_today
                expected_per_trade = (avg_win * win_rate) - (avg_loss * loss_rate)

                if expected_per_trade > 0:
                    trades_needed = DAILY_PNL_TARGET / expected_per_trade
                    plan_parts.append(
                        f"🎯 <b>To hit today's ${DAILY_PNL_TARGET:.0f} target:</b>\n"
                        f"  ~{trades_needed:.1f} trades expected ({data_source}: "
                        f"{win_rate*100:.0f}% WR, avg win {format_pnl(avg_win)}, avg loss {format_pnl(-avg_loss)})"
                        f"{confidence_note}"
                    )
                    if avg_fee > 0:
                        max_trades_before_fee_limit = int(FEE_ALERT_THRESHOLD / avg_fee)
                        if max_trades_before_fee_limit < trades_needed * 1.5:
                            plan_parts.append(
                                f"\n💸 <b>Fee ceiling:</b> at your average fee "
                                f"({format_pnl(-avg_fee)}/trade), roughly {max_trades_before_fee_limit} "
                                f"trades before hitting the ${FEE_ALERT_THRESHOLD:.0f} fee limit — "
                                f"stay disciplined, don't force extra trades."
                            )
                else:
                    plan_parts.append(
                        f"⚠️ <b>{data_source.capitalize()} expectancy is negative</b> "
                        f"(avg win {format_pnl(avg_win)}, avg loss {format_pnl(-avg_loss)}, {win_rate*100:.0f}% WR).\n"
                        f"  More trades won't fix this — the edge needs reviewing first."
                        f"{confidence_note}"
                    )

            if window_rows:
                worst_w = window_rows[0]
                worst_total = (worst_w["wins"] or 0) + (worst_w["losses"] or 0)
                if worst_total >= 2 and worst_w["net_pnl"] < 0:
                    worst_start = int(worst_w["chunk_start"])
                    sample_note = " (small sample — take as a lean, not a rule)" if worst_total < 5 else ""
                    plan_parts.append(
                        f"\n⏰ <b>Time to be careful:</b> {worst_start:02d}:00–{worst_start+2:02d}:00 "
                        f"has been your worst window ({window_source}) — "
                        f"{format_pnl(worst_w['net_pnl'])} ({worst_w['wins']}W/{worst_w['losses']}L){sample_note}."
                    )

            if plan_parts:
                plan_line = "\n" + "\n".join(plan_parts)
        except Exception as e:
            print(f"[Morning Brief] Daily game plan error: {e}")

    compound_line = ""
    try:
        progress = get_compounding_progress()
        if progress and "week_multiplier" in progress:
            week_mult = progress["week_multiplier"]
            week_emoji = "✅" if week_mult >= WEEKLY_COMPOUND_TARGET else "📈"
            compound_line = (
                f"{week_emoji} Week compounding: {week_mult:.4f}x of {WEEKLY_COMPOUND_TARGET:.4f}x target "
                f"({progress['week_pct_of_target']:.0f}% there)"
            )
    except Exception as e:
        print(f"[Morning Brief] Compounding progress error: {e}")

    pace_line = ""
    try:
        pace = get_daily_compounding_pace()
        if pace:
            pace_emoji = "✅" if pace["on_pace"] else "⚠️"
            pace_line = (
                f"\n{pace_emoji} <b>To stay on pace for {WEEKLY_COMPOUND_TARGET:.4f}x by Fri 8pm:</b>\n"
                f"  Need +{pace['required_pct_today']:.2f}% today (+{format_pnl(pace['required_usd_today'])})\n"
                f"  {pace['days_remaining']} trading day(s) left this week\n"
                f"  {'On pace so far.' if pace['on_pace'] else 'Behind pace — today matters more.'}"
            )
    except Exception as e:
        print(f"[Morning Brief] Daily pace error: {e}")

    lines = ["☀️ <b>Good morning. Here's where you stand:</b>\n", yest_line]
    if winrate_line:
        lines.append(winrate_line)
    if compound_line:
        lines.append(compound_line)
    if pace_line:
        lines.append(pace_line)
    lines.append(week_line)
    if bal_line:
        lines.append(bal_line)
    if plan_line:
        lines.append(plan_line)
    lines.append("\nStick to the plan. Trust your edge. 🎯")

    send_telegram("\n".join(lines))


def get_discipline_comparison(period_days: int) -> dict:
    """
    Compares the last `period_days` against the equivalent period before
    that (e.g. this week vs last week, this month vs last month) across
    the 4 core discipline metrics: fee threshold breaches, loss streak
    warnings, revenge trades, and loss-limit block days. Returns counts
    for both periods plus the delta, so callers can render an up/down
    arrow — the actual OKR the user asked to track over time.
    """
    result = {}
    if not DATABASE_URL:
        return result
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        metrics = [
            ("fee_threshold",  "fee_breaches"),
            ("loss_streak",    "loss_streak_warnings"),
            ("revenge_trade",  "revenge_trades"),
        ]
        for category, key in metrics:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE logged_at >= NOW() - INTERVAL '%s days') as current_count,
                    COUNT(*) FILTER (WHERE logged_at >= NOW() - INTERVAL '%s days'
                                        AND logged_at <  NOW() - INTERVAL '%s days') as previous_count
                FROM warnings_log
                WHERE category = %s
            """, (period_days, period_days * 2, period_days, category))
            row = cur.fetchone()
            result[key] = {
                "current":  row["current_count"] or 0,
                "previous": row["previous_count"] or 0,
            }

        # Loss-limit block DAYS (distinct days, combining both categories) —
        # matches the exact definition already used in /analytics.
        cur.execute("""
            SELECT
                COUNT(DISTINCT logged_at::date) FILTER (WHERE logged_at >= NOW() - INTERVAL '%s days') as current_count,
                COUNT(DISTINCT logged_at::date) FILTER (WHERE logged_at >= NOW() - INTERVAL '%s days'
                                                            AND logged_at <  NOW() - INTERVAL '%s days') as previous_count
            FROM warnings_log
            WHERE category IN ('daily_loss_block', 'daily_loss_limit')
        """, (period_days, period_days * 2, period_days))
        row = cur.fetchone()
        result["loss_limit_days"] = {
            "current":  row["current_count"] or 0,
            "previous": row["previous_count"] or 0,
        }

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Discipline Comparison] Error: {e}")
    return result


def format_discipline_comparison_lines(comparison: dict) -> list:
    """
    Turns get_discipline_comparison()'s output into display lines with
    up/down arrows. Down (fewer incidents) is good — shown as ✅🔽.
    Up (more incidents) is bad — shown as ⚠️🔼. Flat is ➡️.
    """
    labels = [
        ("fee_breaches",         "💸 Fee threshold breaches"),
        ("loss_streak_warnings", "🔴 Loss streak warnings"),
        ("revenge_trades",       "😤 Revenge trades"),
        ("loss_limit_days",      "🚫 Loss-limit block days"),
    ]
    lines = []
    for key, label in labels:
        data = comparison.get(key)
        if not data:
            continue
        current, previous = data["current"], data["previous"]
        if current < previous:
            arrow = f"✅🔽 down from {previous}"
        elif current > previous:
            arrow = f"⚠️🔼 up from {previous}"
        else:
            arrow = f"➡️ flat vs {previous}"
        lines.append(f"{label}: {current}  ({arrow})")
    return lines


def send_monthly_discipline_review():
    """
    1st of the month, 8:30am — compares the last 30 days against the 30
    days before that across the 4 core discipline metrics. Same idea as
    the weekly comparison, just zoomed out to catch slower trends the
    week-to-week view might miss.
    """
    try:
        comparison = get_discipline_comparison(30)
        comp_lines = format_discipline_comparison_lines(comparison)
        if not comp_lines:
            return
        msg = (
            "📆 <b>Monthly Discipline Review</b>\n\n"
            "<i>This month vs last month. Reduce these 4 numbers and you retain "
            "real money — independent of any single trade or signal.</i>\n\n"
            + "\n".join(comp_lines)
        )
        send_telegram(msg)
    except Exception as e:
        print(f"[Monthly Discipline Review] Error: {e}")


def send_weekly_okr():
    """
    Monday morning — reflects on the week just finished (last 7 days) and
    gives concrete, data-backed things to do more of / less of to hit the
    daily (1.1x) and weekly (1.4x) compounding targets. Every number here
    is calculated from real trade data, not generic advice — if the data
    doesn't support a claim, that section is skipped rather than filled
    with something vague.
    """
    if not DATABASE_URL:
        return
    total = 0
    win_rate = None
    wr_row = {"wins": 0, "losses": 0}
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Win rate over last 7 days ──
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                   COUNT(*) FILTER (WHERE outcome = 'loss') as losses
            FROM trigger_performance
            WHERE entry_time >= NOW() - INTERVAL '7 days'
              AND outcome IN ('win', 'loss')
        """)
        wr_row = cur.fetchone()
        total = (wr_row["wins"] or 0) + (wr_row["losses"] or 0) if wr_row else 0
        win_rate = (wr_row["wins"] / total * 100) if total > 0 else None

        # ── Worst trigger by win rate (virtual win-rate data), with its $ cost ──
        cur.execute("""
            SELECT trigger,
                   COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                   COUNT(*) FILTER (WHERE outcome = 'win') as wins
            FROM virtual_trigger_trades
            WHERE opened_at >= NOW() - INTERVAL '7 days'
            GROUP BY trigger
            HAVING COUNT(*) FILTER (WHERE outcome IN ('win','loss')) >= 3
            ORDER BY (COUNT(*) FILTER (WHERE outcome = 'win')::float /
                      NULLIF(COUNT(*) FILTER (WHERE outcome IN ('win','loss')), 0)) ASC
            LIMIT 1
        """)
        worst_trigger_row = cur.fetchone()

        # ── Real $ cost of that worst trigger, from actual trades taken on it ──
        worst_trigger_real_cost = None
        if worst_trigger_row:
            cur.execute("""
                SELECT COALESCE(SUM(pnl), 0) as total_pnl, COUNT(*) as n
                FROM trigger_performance
                WHERE trigger = %s
                  AND entry_time >= NOW() - INTERVAL '7 days'
                  AND outcome IN ('win', 'loss')
            """, (worst_trigger_row["trigger"],))
            wt_cost_row = cur.fetchone()
            if wt_cost_row and wt_cost_row["n"] > 0:
                worst_trigger_real_cost = float(wt_cost_row["total_pnl"])

        # ── Worst 2-hour window, with its $ cost ──
        cur.execute("""
            SELECT
                (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2 as chunk_start,
                COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl,
                COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total
            FROM trigger_performance
            WHERE close_time IS NOT NULL
              AND close_time >= NOW() - INTERVAL '7 days'
              AND outcome IN ('win', 'loss')
            GROUP BY (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2
            ORDER BY net_pnl ASC
        """)
        window_rows = cur.fetchall()
        worst_window = window_rows[0] if window_rows and window_rows[0]["total"] >= 3 and window_rows[0]["net_pnl"] < 0 else None

        # ── Avg win/loss $ for the ratio math ──
        cur.execute("""
            SELECT
                AVG(pnl) FILTER (WHERE outcome = 'win')  as avg_win,
                AVG(pnl) FILTER (WHERE outcome = 'loss') as avg_loss,
                COUNT(*) FILTER (WHERE outcome = 'loss') as loss_count
            FROM trigger_performance
            WHERE entry_time >= NOW() - INTERVAL '7 days'
              AND outcome IN ('win', 'loss')
        """)
        roi_row = cur.fetchone()

        # ── Revenge trades and their real cost, last 7 days ──
        cur.execute("""
            SELECT COUNT(*) as n FROM warnings_log
            WHERE category = 'revenge_trade' AND logged_at >= NOW() - INTERVAL '7 days'
        """)
        revenge_row = cur.fetchone()

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Weekly OKR] Data fetch error: {e}")
        return

    try:
        current_balance = get_usdt_balance()
    except Exception:
        current_balance = None

    sections = ["📋 <b>Weekly OKR — Monday Reset</b>\n"]
    sections.append(
        "<i>Targets: 1.1x daily, 1.4x weekly. This isn't about trying harder — "
        "it's about doing 2 things more and 2 things less, based on what actually "
        "cost or made you money last week.</i>\n"
    )

    if total > 0:
        wr_emoji = "✅" if win_rate >= 60 else "⚠️"
        sections.append(f"{wr_emoji} Last week: {win_rate:.0f}% win rate ({wr_row['wins']}W/{wr_row['losses']}L)")
    else:
        sections.append("— No closed trades last week to review.")

    # ── What a 1.1x day / 1.4x week actually requires, in plain $ terms ──
    if current_balance and current_balance > 0:
        daily_gain_needed  = current_balance * 0.10
        weekly_gain_needed = current_balance * 0.40
        sections.append(
            f"\n📐 <b>What the targets actually mean right now:</b>\n"
            f"  1.1x a day = +{format_pnl(daily_gain_needed)} on today's balance\n"
            f"  1.4x a week = +{format_pnl(weekly_gain_needed)} by Friday"
        )

    do_more = []
    do_less = []

    avg_win  = float(roi_row["avg_win"])  if roi_row and roi_row["avg_win"]  else 0
    avg_loss = abs(float(roi_row["avg_loss"])) if roi_row and roi_row["avg_loss"] else 0
    loss_count = roi_row["loss_count"] if roi_row else 0

    if avg_win > 0 and avg_loss > 0:
        ratio = avg_loss / avg_win
        if ratio >= 1.3:
            # If losses were even 30% smaller, here's what that would have saved
            potential_save = avg_loss * 0.30 * loss_count
            do_less.append(
                f"<b>Cut losses faster.</b> Avg loss ({format_pnl(-avg_loss)}) is {ratio:.1f}x "
                f"avg win ({format_pnl(avg_win)}). Cutting losses just 30% smaller would have "
                f"retained ~{format_pnl(potential_save)} last week alone."
            )
        elif ratio <= 0.6:
            do_more.append(
                f"<b>Let winners run further.</b> Avg win ({format_pnl(avg_win)}) is small "
                f"relative to avg loss ({format_pnl(-avg_loss)}) — give winning trades more "
                f"room before taking profit."
            )

    if worst_trigger_row and worst_trigger_real_cost is not None:
        wt_total = worst_trigger_row["total"]
        wt_wr = (worst_trigger_row["wins"] / wt_total * 100) if wt_total > 0 else 0
        if wt_wr < 40:
            trigger_label = worst_trigger_row["trigger"].replace("_", " ").title()
            cost_line = (
                f" — this cost you {format_pnl(worst_trigger_real_cost)} in real trades."
                if worst_trigger_real_cost < 0 else ""
            )
            do_less.append(
                f"<b>Stop taking {trigger_label}.</b> {wt_wr:.0f}% win rate over {wt_total} "
                f"virtual trades this week{cost_line} Avoiding this signal alone would have "
                f"directly improved last week's result."
            )

    if worst_window:
        w_start = int(worst_window["chunk_start"])
        do_less.append(
            f"<b>Avoid trading {w_start:02d}:00–{w_start+2:02d}:00.</b> This window cost you "
            f"{format_pnl(worst_window['net_pnl'])} over {worst_window['total']} trades — "
            f"skipping it would have kept that {format_pnl(abs(worst_window['net_pnl']))} in your account."
        )

    if revenge_row and revenge_row["n"] and revenge_row["n"] >= 3:
        do_less.append(
            f"<b>Cut the revenge-trade reflex.</b> {revenge_row['n']} revenge trades flagged "
            f"last week — each one is a trade taken from emotion, not the setup."
        )

    if win_rate is not None and win_rate >= 60 and total >= 5:
        do_more.append(
            f"<b>Keep doing what you're doing.</b> {win_rate:.0f}% win rate over {total} trades "
            f"is above the 60% target — don't change the process, just do more of it."
        )

    sections.append("")
    if do_more:
        sections.append("📈 <b>Do more of:</b>\n" + "\n".join(f"  • {s}" for s in do_more[:2]))
    if do_less:
        sections.append("📉 <b>Do less of:</b>\n" + "\n".join(f"  • {s}" for s in do_less[:2]))
    if not do_more and not do_less:
        sections.append("No strong signal in the data this week — steady as you go.")

    # ── Week-over-week discipline comparison — the real OKR ────────────
    # Reducing these 4 numbers, independent of any signal or setup working,
    # directly retains money. This is compared against the equivalent
    # 7-day period before last week, so it's a genuine trend, not a
    # one-off snapshot.
    try:
        comparison = get_discipline_comparison(7)
        comp_lines = format_discipline_comparison_lines(comparison)
        if comp_lines:
            sections.append(
                "\n📊 <b>This week vs last week — the real OKR:</b>\n"
                "<i>Reduce these 4 numbers and you keep money, regardless of what else happens.</i>\n"
                + "\n".join(comp_lines)
            )
    except Exception as e:
        print(f"[Weekly OKR] Discipline comparison error: {e}")

    send_telegram("\n".join(sections))


def send_weekly_digest():
    """Sunday evening rollup pulling highlights from /patterns, /quality, /invalidations, /warnings, /analytics."""
    weekly = load_last_7_days_stats()
    week_pnl = sum(d["net_pnl"] for d in weekly) if weekly else 0
    week_wins   = sum(d["wins"] for d in weekly) if weekly else 0
    week_losses = sum(d["losses"] for d in weekly) if weekly else 0
    week_total  = week_wins + week_losses
    week_wr     = (week_wins / week_total * 100) if week_total > 0 else 0
    pnl_emoji   = "✅" if week_pnl >= 0 else "❌"

    sections = [
        f"📈 <b>Weekly Digest</b>\n",
        f"{pnl_emoji} <b>P&L:</b> {format_pnl(week_pnl)}  ({week_wins}W / {week_losses}L, {week_wr:.0f}% WR)",
    ]

    if not DATABASE_URL:
        sections.append("\n⚠️ No database connected — deeper stats unavailable.")
        send_telegram("\n".join(sections))
        return

    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Most active / most reliable trigger this week ──
        cur.execute("""
            SELECT trigger,
                   COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                   COUNT(*) FILTER (WHERE outcome = 'win') as wins,
                   COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
            FROM trigger_performance
            WHERE entry_time >= NOW() - INTERVAL '7 days'
              AND trigger NOT IN ('long_setup_10m', 'short_setup_10m', 'fork', '3_bulls', 'weakness', '3_bar_buy')
            GROUP BY trigger
            HAVING COUNT(*) FILTER (WHERE outcome IN ('win','loss')) >= 1
            ORDER BY net_pnl DESC
            LIMIT 1
        """)
        best_trigger = cur.fetchone()
        cur.execute("""
            SELECT trigger,
                   COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                   COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
            FROM trigger_performance
            WHERE entry_time >= NOW() - INTERVAL '7 days'
              AND trigger NOT IN ('long_setup_10m', 'short_setup_10m', 'fork', '3_bulls', 'weakness', '3_bar_buy')
            GROUP BY trigger
            HAVING COUNT(*) FILTER (WHERE outcome IN ('win','loss')) >= 1
            ORDER BY net_pnl ASC
            LIMIT 1
        """)
        worst_trigger = cur.fetchone()

        TRIGGER_LABELS = {
            "long": "📈 Long", "3_bulls": "🐂 3 Bulls",
            "strength": "💪 Strength", "3_bar_buy": "📈 3 Bar Buy",
            "3_bears": "🐻 3 Bears", "weakness": "📉 Weakness",
            "bearish": "🔴 Bearish", "short": "📉 Short", "30m_composite_crosses_up_30": "📈 30m Composite Crosses UP 30", "30m_composite_crosses_down_70": "📉 30m Composite Crosses DOWN 70", "30m_composite_crosses_down_85": "📉 30m Composite Crosses DOWN 85", "30m_composite_crosses_up_15": "📈 30m Composite Crosses UP 15", "wpr_crosses_down_18": "📉 WPR Crosses Down -18", "wpr_crosses_up_82": "📈 WPR Crosses Up -82", "buy": "📈 Buy",
            "green_structure_break": "🟢 Green Structure Break",
            "orange_structure_break": "🟠 Orange Structure Break",
        }

        if best_trigger and worst_trigger and best_trigger["trigger"] != worst_trigger["trigger"]:
            best_label  = TRIGGER_LABELS.get(best_trigger["trigger"], best_trigger["trigger"])
            worst_label = TRIGGER_LABELS.get(worst_trigger["trigger"], worst_trigger["trigger"])
            sections.append(
                f"\n🏆 <b>Best signal:</b> {best_label} ({format_pnl(best_trigger['net_pnl'])})\n"
                f"💔 <b>Worst signal:</b> {worst_label} ({format_pnl(worst_trigger['net_pnl'])})"
            )
        elif best_trigger:
            label = TRIGGER_LABELS.get(best_trigger["trigger"], best_trigger["trigger"])
            sections.append(f"\n🏆 <b>Only signal fired:</b> {label} ({format_pnl(best_trigger['net_pnl'])})")

        # ── Warning counts by category this week ──
        cur.execute("""
            SELECT category, COUNT(*) as cnt
            FROM warnings_log
            WHERE logged_at >= NOW() - INTERVAL '7 days'
            GROUP BY category
            ORDER BY cnt DESC
        """)
        warning_rows = cur.fetchall()
        if warning_rows:
            WARNING_LABELS = {
                "counter_signal":     "Counter signals",
                "loss_streak":        "Loss streaks",
                "daily_loss_block":   "3-loss blocks",
                "daily_loss_limit":   "Daily loss limit hits",
                "revenge_trade":      "Revenge trade flags",
                "retrace_protect":    "Retracement saves",
                "fee_threshold":      "Fee threshold breaches",
                "first_loss_tighten": "First-trade-loss tightenings",
                "signal_skipped":     "Signals skipped",
            }
            top3 = warning_rows[:3]
            warn_lines = [f"  {WARNING_LABELS.get(r['category'], r['category'])}: {r['cnt']}x" for r in top3]
            sections.append(f"\n⚠️ <b>Top warnings this week:</b>\n" + "\n".join(warn_lines))
        else:
            sections.append("\n✅ <b>No warnings fired this week.</b> Clean discipline.")

        # ── Most-skipped signal this week ──
        cur.execute("""
            SELECT message, COUNT(*) as cnt
            FROM warnings_log
            WHERE category = 'signal_skipped'
              AND logged_at >= NOW() - INTERVAL '7 days'
            GROUP BY message
            ORDER BY cnt DESC
            LIMIT 1
        """)
        top_skip = cur.fetchone()
        if top_skip:
            sections.append(
                f"\n🙅 <b>Most skipped:</b> {top_skip['message'].replace('Skipped: ', '')} ({top_skip['cnt']}x)"
            )

        # ── Invalidation rate check — worst offender this week ──
        cur.execute("""
            SELECT trigger, COUNT(*) as cnt
            FROM invalidation_log
            WHERE logged_at >= NOW() - INTERVAL '7 days'
            GROUP BY trigger
            ORDER BY cnt DESC
            LIMIT 1
        """)
        top_inv = cur.fetchone()
        if top_inv:
            inv_label = TRIGGER_LABELS.get(top_inv["trigger"], top_inv["trigger"])
            sections.append(f"\n🔻 <b>Most invalidated:</b> {inv_label} ({top_inv['cnt']}x this week)")

        # ── Brute-force closes this week ──
        cur.execute("""
            SELECT COUNT(*) as cnt
            FROM warnings_log
            WHERE category IN ('brute_force_close', 'hard_structure_invalidate', 'counter_signal_close')
              AND logged_at >= NOW() - INTERVAL '7 days'
        """)
        r_brute = cur.fetchone()
        if r_brute and r_brute["cnt"]:
            sections.append(f"\n🛡️ <b>Brute-force closes:</b> {r_brute['cnt']}x this week — check /brute")

        cur.close()
        conn.close()
    except Exception as e:
        sections.append(f"\n⚠️ Digest query error: {e}")
        print(f"[Weekly Digest] Error: {e}")

    sections.append("\n\nFull detail: /patterns  /quality  /invalidations  /warnings  /analytics")
    send_telegram("\n".join(sections))


def poll_order_fills():
    global entry2_placed_at
    while True:
        try:
            if tracked_orders:
                filled_ids = []

                # ── Fetch all open orders ONCE per cycle, not once per tracked ──
                # order. Previously this called /fapi/v1/order individually for
                # EVERY tracked order (Entry2 + TP1 + TP2 = 3 separate Binance
                # calls) every single cycle, even though most cycles nothing has
                # changed. Now: one bulk call to see which order IDs are still
                # genuinely open — only orders that have DISAPPEARED from that
                # list (meaning they filled/cancelled/expired) get the more
                # detailed individual lookup, since that's the only time we
                # actually need the fill price/qty details.
                still_open_ids = set()
                try:
                    now_ms_bulk = int(time.time() * 1000)
                    bulk_params = sign_request({"symbol": SYMBOL, "timestamp": now_ms_bulk})
                    bulk_resp = requests.get(f"{FUTURES_BASE}/fapi/v1/openOrders",
                                             params=bulk_params, headers={"X-MBX-APIKEY": BINANCE_API_KEY},
                                             timeout=10)
                    if bulk_resp.status_code == 200:
                        still_open_ids = {o["orderId"] for o in bulk_resp.json()}
                    else:
                        # If the bulk call itself fails, fall back to checking
                        # every tracked order individually this cycle rather
                        # than silently skipping fill detection entirely.
                        still_open_ids = set(tracked_orders.keys())
                except Exception as e:
                    print(f"[Fill Poll] Bulk open orders check failed, falling back: {e}")
                    still_open_ids = set(tracked_orders.keys())

                for order_id, meta in list(tracked_orders.items()):
                    if order_id in still_open_ids:
                        continue  # still genuinely open — no need to check details this cycle

                    now_ms = int(time.time() * 1000)
                    params = {"symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms}
                    params = sign_request(params)
                    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                    resp = requests.get(f"{FUTURES_BASE}/fapi/v1/order",
                                        params=params, headers=headers, timeout=10)
                    if resp.status_code != 200:
                        continue
                    order  = resp.json()
                    status = order.get("status", "")

                    if status == "FILLED":
                        filled_ids.append(order_id)
                        avg_price = float(order.get("avgPrice") or meta.get("price", 0))
                        qty       = float(order.get("executedQty") or meta.get("qty", 0))
                        label     = meta["label"]

                        if label == "Entry2":
                            entry2_placed_at = 0.0
                            sl_line = ""
                            tp_grow_line = ""
                            max_lines = ""
                            try:
                                pos       = get_open_position()
                                total_usd = abs(float(pos["positionAmt"])) * avg_price / LEVERAGE if pos else qty * avg_price / LEVERAGE
                                if pos and current_trade_entry:
                                    avg_entry  = float(pos.get("entryPrice", 0))
                                    direction  = current_trade_entry.get("direction")
                                    sym_info   = get_symbol_info()
                                    tick       = sym_info["price_tick"]
                                    step       = sym_info["qty_step"]
                                    stop_pct   = STOP_PCT   # use original SL % from avg entry, not the tighter Entry2 %
                                    if avg_entry > 0 and direction:
                                        if direction == "long":
                                            new_sl = round_step(avg_entry * (1 - stop_pct / 100), tick)
                                        else:
                                            new_sl = round_step(avg_entry * (1 + stop_pct / 100), tick)
                                        current_trade_entry["sl_price"] = new_sl
                                        current_trade_entry["price"]    = avg_entry
                                        sl_line = f"\nSL recalculated: ${new_sl:,.2f} (avg entry ${avg_entry:,.2f})"

                                        # ── Grow TP2 (or TP1) to absorb Entry2 quantity ──
                                        tp2_price_existing = current_trade_entry.get("tp2_price")
                                        tp1_price_existing = current_trade_entry.get("tp1_price")
                                        close_side = "SELL" if direction == "long" else "BUY"
                                        grown = False
                                        for oid, meta in list(tracked_orders.items()):
                                            if meta["label"] == "TP2" and tp2_price_existing:
                                                try:
                                                    now_ms  = int(time.time() * 1000)
                                                    params  = {"symbol": SYMBOL, "orderId": int(oid), "timestamp": now_ms}
                                                    params  = sign_request(params)
                                                    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                                                    requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                                    params=params, headers=headers, timeout=10)
                                                    tracked_orders.pop(oid, None)
                                                    new_tp2_qty = round_step(current_trade_entry["qty_tp2"] + qty, step)
                                                    new_resp = place_order(close_side, "LIMIT", new_tp2_qty,
                                                                            limit_price=tp2_price_existing, reduce_only=True)
                                                    register_order(new_resp, "TP2")
                                                    current_trade_entry["qty_tp2"] = new_tp2_qty
                                                    tp_grow_line = f"\nTP2 grown to cover Entry2: {new_tp2_qty} BTC @ ${tp2_price_existing:,.2f}"
                                                    grown = True
                                                except Exception as e3:
                                                    tp_grow_line = f"\n⚠️ Could not grow TP2: {e3}"
                                                break
                                        if not grown and tp1_price_existing:
                                            for oid, meta in list(tracked_orders.items()):
                                                if meta["label"] == "TP1":
                                                    try:
                                                        now_ms  = int(time.time() * 1000)
                                                        params  = {"symbol": SYMBOL, "orderId": int(oid), "timestamp": now_ms}
                                                        params  = sign_request(params)
                                                        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                                                        requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                                        params=params, headers=headers, timeout=10)
                                                        tracked_orders.pop(oid, None)
                                                        new_tp1_qty = round_step(current_trade_entry["qty_tp1"] + qty, step)
                                                        new_resp = place_order(close_side, "LIMIT", new_tp1_qty,
                                                                                limit_price=tp1_price_existing, reduce_only=True)
                                                        register_order(new_resp, "TP1")
                                                        current_trade_entry["qty_tp1"] = new_tp1_qty
                                                        tp_grow_line = f"\nTP1 grown to cover Entry2: {new_tp1_qty} BTC @ ${tp1_price_existing:,.2f}"
                                                    except Exception as e3:
                                                        tp_grow_line = f"\n⚠️ Could not grow TP1: {e3}"
                                                    break

                                        # ── Recalculate max win/loss on full position ──
                                        total_qty = abs(float(pos["positionAmt"]))
                                        max_loss_usd = total_qty * abs(avg_entry - new_sl)
                                        max_win_usd  = current_trade_entry["qty_tp1"] * abs(tp1_price_existing - avg_entry) if tp1_price_existing else 0
                                        if current_trade_entry.get("qty_tp2"):
                                            max_win_usd += current_trade_entry["qty_tp2"] * abs(tp2_price_existing - avg_entry)
                                        max_lines = (
                                            f"\n\n🛑 New max loss: {format_pnl(-max_loss_usd)}"
                                            f"\n💰 New max win:  {format_pnl(max_win_usd)}"
                                        )
                                    else:
                                        sl_line = ""
                            except Exception as e:
                                total_usd = qty * avg_price / LEVERAGE
                            send_telegram(
                                f"📥 <b>Entry2 filled</b>\n\n"
                                f"Price:          ${avg_price:,.2f}\n"
                                f"Total margin:   ${total_usd:,.2f} ✅"
                                f"{sl_line}"
                                f"{tp_grow_line}"
                                f"{max_lines}"
                            )
                        elif label == "TP1":
                            time_to_tp1 = (time.time() - trade_entry_time) / 60 if trade_entry_time > 0 else None
                            if pending_trigger_id and time_to_tp1 is not None:
                                try:
                                    conn = get_db()
                                    cur  = conn.cursor()
                                    cur.execute("""
                                        UPDATE trigger_performance
                                        SET time_to_tp1_mins = %s, mfe = %s, mae = %s
                                        WHERE id = %s
                                    """, (time_to_tp1, trade_mfe, trade_mae, pending_trigger_id))
                                    conn.commit()
                                    cur.close()
                                    conn.close()
                                except Exception as e:
                                    print(f"[DB] time_to_tp1 update error: {e}")
                            send_telegram(
                                f"🎯 <b>TP1 hit — partial close filled</b>\n\n"
                                f"Price:   ${avg_price:,.2f}\n"
                                f"Closed:  ${qty * avg_price / LEVERAGE:,.2f}\n"
                                f"Moving SL to +{TP1_SL_MOVE_PCT}% from entry... 🔒\n\n"
                                f"{after_trade_summary()}"
                            )
                            for oid, meta in list(tracked_orders.items()):
                                if meta["label"] == "Entry2":
                                    try:
                                        now_ms  = int(time.time() * 1000)
                                        params  = {"symbol": SYMBOL, "orderId": int(oid), "timestamp": now_ms}
                                        params  = sign_request(params)
                                        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                                        requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                        params=params, headers=headers, timeout=10)
                                        tracked_orders.pop(oid, None)
                                        entry2_placed_at = 0.0
                                        send_telegram("🚫 Entry2 auto-cancelled — TP1 hit.")
                                    except Exception as e2:
                                        send_telegram(f"⚠️ Could not auto-cancel Entry2: {e2}")
                                    break
                            try:
                                entry_info = current_trade_entry
                                if entry_info:
                                    ep        = entry_info["price"]
                                    direction = entry_info["direction"]
                                    sym_info  = get_symbol_info()
                                    tick      = sym_info["price_tick"]
                                    new_sl    = round_step(ep * (1 + TP1_SL_MOVE_PCT / 100), tick) if direction == "long" \
                                                else round_step(ep * (1 - TP1_SL_MOVE_PCT / 100), tick)
                                    current_trade_entry["sl_price"] = new_sl
                                    try:
                                        mark     = get_mark_price()
                                        pos      = get_open_position()
                                        pos_amt  = abs(float(pos["positionAmt"])) if pos else 0
                                        sl_dist  = abs(mark - new_sl) * pos_amt
                                    except Exception:
                                        sl_dist  = 0
                                    sl_dist_line = f"\nRisk if hit: ${sl_dist:,.2f}" if sl_dist > 0 else ""
                                    send_telegram(f"🔒 SL moved to ${new_sl:,.2f} (+{TP1_SL_MOVE_PCT}% from entry){sl_dist_line}")
                                else:
                                    send_telegram("⚠️ Could not move SL — entry price unknown. Use /sl manually.")
                            except Exception as sl_e:
                                send_telegram(f"⚠️ TP1 hit but SL move failed: {sl_e}")
                        elif label == "TP2":
                            global last_tp2_close_time
                            current_trade_entry.clear()
                            last_tp2_close_time = time.time()
                            time_to_tp2 = (time.time() - trade_entry_time) / 60 if trade_entry_time > 0 else None
                            db_update_outcome(pending_trigger_id, "win", qty * avg_price / LEVERAGE,
                                              mae=trade_mae, mfe=trade_mfe,
                                              time_to_tp2_mins=time_to_tp2)
                            send_telegram(
                                f"🏆 <b>TP2 hit — position fully closed</b>\n\n"
                                f"Price:   ${avg_price:,.2f}\n"
                                f"Closed:  ${qty * avg_price / LEVERAGE:,.2f}\n"
                                f"Clean exit. ✅\n\n"
                                f"{after_trade_summary()}"
                            )
                            mirror_close_all()
                            start_prompt_silence("TP2 hit — let the cortisol settle before the next one.")
                        elif label == "SL":
                            current_trade_entry.clear()
                            last_loss_close_time = time.time()
                            time_to_sl = (time.time() - trade_entry_time) / 60 if trade_entry_time > 0 else None
                            db_update_outcome(pending_trigger_id, "loss", -(qty * avg_price / LEVERAGE),
                                              mae=trade_mae, mfe=trade_mfe,
                                              time_to_sl_mins=time_to_sl)
                            send_telegram(
                                f"🛑 <b>Stop loss hit</b>\n\n"
                                f"Price:   ${avg_price:,.2f}\n"
                                f"Closed:  ${qty * avg_price / LEVERAGE:,.2f}\n"
                                f"Loss taken. Assess before the next trade.\n\n"
                                f"{after_trade_summary()}"
                            )
                            mirror_close_all()

                    elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                        filled_ids.append(order_id)

                for oid in filled_ids:
                    tracked_orders.pop(oid, None)

        except Exception as e:
            print(f"[Fill Poll ERROR] {e}")

        time.sleep(ORDER_FILL_POLL_INTERVAL)


def register_order(order_resp: dict, label: str) -> None:
    order_id = order_resp.get("orderId")
    if order_id:
        tracked_orders[order_id] = {
            "label": label,
            "price": float(order_resp.get("price") or order_resp.get("stopPrice") or 0),
            "qty":   float(order_resp.get("origQty", 0)),
        }


def pnl_summary() -> None:
    try:
        trades    = get_futures_trades_today()
        stats     = build_stats(trades)
        positions = group_by_position(trades)

        last_line = ""
        if positions:
            last      = positions[-1]
            result    = "✅ Win" if last["realizedPnl"] > 0 else "❌ Loss"
            last_line = f"Last trade:  {result}  {format_pnl(last['realizedPnl'])}\n"

        streak_line = ""
        win_streak  = get_consecutive_wins(trades)
        loss_streak = get_consecutive_losses(trades)
        if win_streak >= 2:
            streak_line = f"Streak:      🔥 {win_streak} wins in a row\n"
        elif loss_streak >= 2:
            streak_line = f"Streak:      🔴 {loss_streak} losses in a row\n"
        elif win_streak == 1:
            streak_line = f"Streak:      ✅ Last trade was a win\n"
        elif loss_streak == 1:
            streak_line = f"Streak:      ❌ Last trade was a loss\n"

        freq_lines = ""
        if len(positions) >= 2:
            def fmt_dur(mins):
                if mins >= 60:
                    return f"{int(mins//60)}h {int(mins%60)}m"
                return f"{int(mins)}m"

            gaps = []
            for i in range(1, len(positions)):
                prev_close = positions[i-1].get("close_time")
                curr_open  = positions[i].get("open_time")
                if prev_close and curr_open:
                    gaps.append((curr_open - prev_close) / 60)

            hold_times = []
            for p in positions:
                o = p.get("open_time")
                c = p.get("close_time")
                if o and c and c > o:
                    hold_times.append((c - o) / 60)

            if gaps:
                avg_gap = sum(gaps) / len(gaps)
                freq_lines += f"\nAvg gap:     {fmt_dur(avg_gap)}"
                freq_lines += f"  (shortest: {fmt_dur(min(gaps))}, longest: {fmt_dur(max(gaps))})"
            if hold_times:
                avg_hold = sum(hold_times) / len(hold_times)
                freq_lines += f"\nAvg hold:    {fmt_dur(avg_hold)}"

        wins_pnl   = [p["realizedPnl"] for p in positions if p["realizedPnl"] > 0]
        losses_pnl = [p["realizedPnl"] for p in positions if p["realizedPnl"] < 0]
        avg_win    = sum(wins_pnl)   / len(wins_pnl)   if wins_pnl   else None
        avg_loss   = sum(losses_pnl) / len(losses_pnl) if losses_pnl else None
        rr_line = ""
        if avg_win and avg_loss:
            rr = abs(avg_win / avg_loss)
            rr_line = f"Avg win:     {format_pnl(avg_win)}\nAvg loss:    {format_pnl(avg_loss)}\nR:R ratio:   {rr:.2f}\n"
        elif avg_win:
            rr_line = f"Avg win:     {format_pnl(avg_win)}\n"
        elif avg_loss:
            rr_line = f"Avg loss:    {format_pnl(avg_loss)}\n"

        target     = DAILY_PNL_TARGET
        net        = stats["net_pnl"]
        pct_done   = (net / target * 100) if target else 0
        target_line = f"\nTarget:      {format_pnl(net)} / ${target:.0f}  ({pct_done:.1f}%)"

        try:
            balance  = get_usdt_balance()
            bal_line = f"\nBalance:     ${balance:,.2f} USDT"
        except Exception:
            bal_line = ""

        send_telegram(
            f"📊 <b>Today's stats</b>\n\n"
            f"Positions:   {stats['closed_positions']}  "
            f"({stats['wins']}W / {stats['losses']}L  —  {stats['win_rate']:.1f}% WR)\n"
            f"{last_line}"
            f"{streak_line}"
            f"\n{rr_line}"
            f"Gross P&L:   {format_pnl(stats['total_pnl'])}\n"
            f"Fees:        -${stats['total_fees']:.2f}\n"
            f"Net P&L:     {format_pnl(stats['net_pnl'])}"
            f"{target_line}"
            f"{bal_line}"
            f"{freq_lines}"
        )
    except Exception as e:
        if is_rate_limit_error(e):
            send_telegram(
                f"🚫 <b>Binance rate-limit hit — not a bug, back off needed.</b>\n\n"
                f"Your IP has been temporarily restricted by Binance ({e.response.status_code}). "
                f"This usually clears within minutes to a couple hours on its own.\n\n"
                f"Avoid rapid repeated commands until it clears."
            )
        else:
            send_telegram(f"❌ Stats error: {e}")


def compare_summary() -> None:
    try:
        trades    = get_futures_trades_today()
        positions = group_by_position(trades)

        if not positions:
            send_telegram("📊 No closed positions today yet.")
            return

        ind_trades = [p for p in positions if any(str(oid) in indicator_trade_ids for oid in p["order_ids"])]
        man_trades = [p for p in positions if not any(str(oid) in indicator_trade_ids for oid in p["order_ids"])]

        def fmt_block(label, emoji, t_list):
            if not t_list:
                return f"{emoji} <b>{label}</b>\nNo trades yet."
            wins     = [p for p in t_list if p["realizedPnl"] > 0]
            losses   = [p for p in t_list if p["realizedPnl"] < 0]
            wr       = len(wins) / len(t_list)
            avg_win  = sum(p["realizedPnl"] for p in wins)   / len(wins)   if wins   else 0
            avg_loss = sum(p["realizedPnl"] for p in losses) / len(losses) if losses else 0
            pnl      = sum(p["realizedPnl"] for p in t_list)
            expectancy = (wr * avg_win) + ((1 - wr) * avg_loss)

            def _roi_pct(p):
                margin = (p.get("entry_notional", 0) / LEVERAGE) if LEVERAGE > 0 else 0
                return (p["realizedPnl"] / margin * 100) if margin > 0 else None
            win_rois_l  = [r for r in (_roi_pct(p) for p in wins)  if r is not None]
            loss_rois_l = [r for r in (_roi_pct(p) for p in losses) if r is not None]
            avg_win_roi  = (sum(win_rois_l) / len(win_rois_l))   if win_rois_l  else 0
            avg_loss_roi = (sum(loss_rois_l) / len(loss_rois_l)) if loss_rois_l else 0

            return (
                f"{emoji} <b>{label}</b>\n"
                f"Positions:   {len(t_list)}  ({len(wins)}W / {len(losses)}L)\n"
                f"Win rate:    {wr*100:.1f}%\n"
                f"Avg win:     {format_pnl(avg_win)}  (+{avg_win_roi:.2f}% ROI)\n"
                f"Avg loss:    {format_pnl(avg_loss)}  ({avg_loss_roi:.2f}% ROI)\n"
                f"Expectancy:  {format_pnl(expectancy)} per trade\n"
                f"P&L:         {format_pnl(pnl)}"
            )

        ind_block = fmt_block("Indicator", "📡", ind_trades)
        man_block = fmt_block("Manual",    "🖐",  man_trades)

        verdict = ""
        if ind_trades and man_trades:
            ind_wr  = len([p for p in ind_trades if p["realizedPnl"] > 0]) / len(ind_trades) * 100
            man_wr  = len([p for p in man_trades if p["realizedPnl"] > 0]) / len(man_trades) * 100
            ind_wins_list  = [p["realizedPnl"] for p in ind_trades if p["realizedPnl"] > 0]
            ind_loss_list  = [p["realizedPnl"] for p in ind_trades if p["realizedPnl"] < 0]
            man_wins_list  = [p["realizedPnl"] for p in man_trades if p["realizedPnl"] > 0]
            man_loss_list  = [p["realizedPnl"] for p in man_trades if p["realizedPnl"] < 0]
            ind_exp = (ind_wr/100 * (sum(ind_wins_list)  / max(len(ind_wins_list),  1))) + \
                      ((1 - ind_wr/100) * (sum(ind_loss_list) / max(len(ind_loss_list), 1)))
            man_exp = (man_wr/100 * (sum(man_wins_list)  / max(len(man_wins_list),  1))) + \
                      ((1 - man_wr/100) * (sum(man_loss_list) / max(len(man_loss_list), 1)))
            wr_winner  = "📡 Indicator" if ind_wr  > man_wr  else ("🖐 Manual" if man_wr  > ind_wr  else "🤝 Even")
            exp_winner = "📡 Indicator" if ind_exp > man_exp else ("🖐 Manual" if man_exp > ind_exp else "🤝 Even")
            verdict = (
                f"\n🏆 WR:         {wr_winner} ({ind_wr:.1f}% vs {man_wr:.1f}%)\n"
                f"🏆 Expectancy: {exp_winner} ({format_pnl(ind_exp)} vs {format_pnl(man_exp)})"
            )

        send_telegram(
            f"⚖️ <b>Manual vs Indicator — today</b>\n\n"
            f"{ind_block}\n\n"
            f"{man_block}"
            f"{verdict}"
        )
    except Exception as e:
        send_telegram(f"❌ Compare error: {e}")


def fill_entry2_at_market() -> None:
    global entry2_placed_at
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        entry2_id  = None
        entry2_qty = None
        for oid, meta in list(tracked_orders.items()):
            if meta["label"] == "Entry2":
                entry2_id  = int(oid)
                entry2_qty = meta["qty"]
                break

        if not entry2_id:
            send_telegram("ℹ️ No Entry2 limit order found to fill.")
            return

        now_ms = int(time.time() * 1000)
        params = {"symbol": SYMBOL, "orderId": entry2_id, "timestamp": now_ms}
        params = sign_request(params)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        resp = requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                               params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        tracked_orders.pop(str(entry2_id), None)
        entry2_placed_at = 0.0

        pos = get_open_position()
        if not pos:
            send_telegram("⚠️ Entry2 cancelled but no open position found — not filling at market.")
            return

        amt       = float(pos["positionAmt"])
        side      = "BUY" if amt > 0 else "SELL"
        sym_info  = get_symbol_info()
        qty_step  = sym_info["qty_step"]
        fill_qty  = round_step(entry2_qty, qty_step)
        mark      = get_mark_price()

        place_order(side, "MARKET", fill_qty)
        time.sleep(2)  # let position update

        sl_line      = ""
        tp_grow_line = ""
        max_lines    = ""
        try:
            pos2 = get_open_position()
            if pos2 and current_trade_entry:
                avg_entry  = float(pos2.get("entryPrice", 0))
                direction  = current_trade_entry.get("direction")
                tick       = sym_info["price_tick"]
                step       = qty_step
                stop_pct   = STOP_PCT   # use original SL % from avg entry, not the tighter Entry2 %
                if avg_entry > 0 and direction:
                    if direction == "long":
                        new_sl = round_step(avg_entry * (1 - stop_pct / 100), tick)
                    else:
                        new_sl = round_step(avg_entry * (1 + stop_pct / 100), tick)
                    current_trade_entry["sl_price"] = new_sl
                    current_trade_entry["price"]    = avg_entry
                    sl_line = f"\nSL recalculated: ${new_sl:,.2f} (avg entry ${avg_entry:,.2f})"

                    tp2_price_existing = current_trade_entry.get("tp2_price")
                    tp1_price_existing = current_trade_entry.get("tp1_price")
                    close_side = "SELL" if direction == "long" else "BUY"
                    grown = False
                    for oid, meta in list(tracked_orders.items()):
                        if meta["label"] == "TP2" and tp2_price_existing:
                            try:
                                now_ms2  = int(time.time() * 1000)
                                params2  = {"symbol": SYMBOL, "orderId": int(oid), "timestamp": now_ms2}
                                params2  = sign_request(params2)
                                requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                params=params2, headers=headers, timeout=10)
                                tracked_orders.pop(oid, None)
                                new_tp2_qty = round_step(current_trade_entry["qty_tp2"] + fill_qty, step)
                                new_resp = place_order(close_side, "LIMIT", new_tp2_qty,
                                                        limit_price=tp2_price_existing, reduce_only=True)
                                register_order(new_resp, "TP2")
                                current_trade_entry["qty_tp2"] = new_tp2_qty
                                tp_grow_line = f"\nTP2 grown to cover Entry2: {new_tp2_qty} BTC @ ${tp2_price_existing:,.2f}"
                                grown = True
                            except Exception as e3:
                                tp_grow_line = f"\n⚠️ Could not grow TP2: {e3}"
                            break
                    if not grown and tp1_price_existing:
                        for oid, meta in list(tracked_orders.items()):
                            if meta["label"] == "TP1":
                                try:
                                    now_ms2  = int(time.time() * 1000)
                                    params2  = {"symbol": SYMBOL, "orderId": int(oid), "timestamp": now_ms2}
                                    params2  = sign_request(params2)
                                    requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                    params=params2, headers=headers, timeout=10)
                                    tracked_orders.pop(oid, None)
                                    new_tp1_qty = round_step(current_trade_entry["qty_tp1"] + fill_qty, step)
                                    new_resp = place_order(close_side, "LIMIT", new_tp1_qty,
                                                            limit_price=tp1_price_existing, reduce_only=True)
                                    register_order(new_resp, "TP1")
                                    current_trade_entry["qty_tp1"] = new_tp1_qty
                                    tp_grow_line = f"\nTP1 grown to cover Entry2: {new_tp1_qty} BTC @ ${tp1_price_existing:,.2f}"
                                except Exception as e3:
                                    tp_grow_line = f"\n⚠️ Could not grow TP1: {e3}"
                                break

                    total_qty_final = abs(float(pos2["positionAmt"]))
                    max_loss_usd = total_qty_final * abs(avg_entry - new_sl)
                    max_win_usd  = current_trade_entry["qty_tp1"] * abs(tp1_price_existing - avg_entry) if tp1_price_existing else 0
                    if current_trade_entry.get("qty_tp2"):
                        max_win_usd += current_trade_entry["qty_tp2"] * abs(tp2_price_existing - avg_entry)
                    max_lines = (
                        f"\n\n🛑 New max loss: {format_pnl(-max_loss_usd)}"
                        f"\n💰 New max win:  {format_pnl(max_win_usd)}"
                    )
        except Exception as e:
            print(f"[Fill2] SL recalc error: {e}")

        total_usd = (abs(amt) + fill_qty) * mark / LEVERAGE
        send_telegram(
            f"✅ <b>Entry2 filled at market</b>\n\n"
            f"Price:          ~${mark:,.2f}\n"
            f"Total margin:   ${total_usd:,.2f}"
            f"{sl_line}"
            f"{tp_grow_line}"
            f"{max_lines}"
        )
    except requests.exceptions.HTTPError as e:
        try:
            err = e.response.json()
            send_telegram(f"❌ Fill2 failed\nCode {err.get('code')}: {err.get('msg')}")
        except Exception:
            send_telegram(f"❌ Fill2 failed: {e}")
    except Exception as e:
        send_telegram(f"❌ Fill2 error: {e}")


def cancel_entry2() -> None:
    global entry2_placed_at
    if not EXECUTION_ENABLED:
        send_telegram("⚠️ Execution is disabled.")
        return
    try:
        entry2_id = None
        for oid, meta in list(tracked_orders.items()):
            if meta["label"] == "Entry2":
                entry2_id = int(oid)
                break

        if not entry2_id:
            send_telegram("ℹ️ No Entry2 limit order found.")
            return

        now_ms  = int(time.time() * 1000)
        params  = {"symbol": SYMBOL, "orderId": entry2_id, "timestamp": now_ms}
        params  = sign_request(params)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        resp    = requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                  params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        tracked_orders.pop(str(entry2_id), None)
        entry2_placed_at = 0.0
        send_telegram("✅ Entry2 limit order cancelled.")
    except requests.exceptions.HTTPError as e:
        try:
            err = e.response.json()
            send_telegram(f"❌ Cancel2 failed\nCode {err.get('code')}: {err.get('msg')}")
        except Exception:
            send_telegram(f"❌ Cancel2 failed: {e}")
    except Exception as e:
        send_telegram(f"❌ Cancel2 error: {e}")


tg_last_update_id = 0


def poll_telegram_commands():
    global tg_last_update_id, daily_bias, pending_trigger_id, pending_signal, pending_manual_trade, maker_entry_cancel_requested, pending_stress_check, pending_bias_reset, elevated_silence_until
    print("[Telegram] Command polling started.")

    while True:
        try:
            url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"timeout": 30, "offset": tg_last_update_id + 1, "allowed_updates": ["message", "callback_query"]}
            resp   = requests.get(url, params=params, timeout=40)
            resp.raise_for_status()
            updates = resp.json().get("result", [])

            for update in updates:
                tg_last_update_id = update["update_id"]

                # ── Signal prompt expiry check ─────────────────────────
                if pending_signal and time.time() - pending_signal["timestamp"] > SIGNAL_EXPIRY_SECS:
                    msg_id = pending_signal.get("message_id")
                    label  = pending_signal["label"]
                    if msg_id:
                        edit_prompt_message(msg_id, f"⏱ <b>{label}</b>\n\nSignal expired — no trade taken.")
                    pending_signal = None

                # ── Handle inline button callbacks ─────────────────────
                callback = update.get("callback_query", {})
                if callback:
                    cb_id   = callback["id"]
                    cb_data = callback.get("data", "")
                    cb_chat = str(callback.get("message", {}).get("chat", {}).get("id", ""))
                    msg_id  = callback.get("message", {}).get("message_id")

                    if cb_chat != str(TELEGRAM_CHAT_ID):
                        answer_callback(cb_id)
                        continue

                    if cb_data.startswith("take_") and pending_signal:
                        direction = pending_signal["direction"]
                        label     = pending_signal["label"]
                        answer_callback(cb_id, "✅ Taking trade...")
                        edit_prompt_message(msg_id, f"✅ <b>{label}</b>\n\nTrade confirmed — checking stress state...")
                        pending_signal = None
                        threading.Thread(
                            target=execute_signal_trade,
                            args=(direction, label),
                            daemon=True
                        ).start()
                    elif cb_data.startswith("skip_") and pending_signal:
                        label = pending_signal["label"]
                        db_log_warning("signal_skipped", f"Skipped: {label}")
                        answer_callback(cb_id, "❌ Skipped.")
                        edit_prompt_message(msg_id, f"❌ <b>{label}</b>\n\nSkipped — no trade taken.")
                        pending_signal = None

                    elif cb_data.startswith("pinkcandle_yes_") and pending_manual_trade and pending_manual_trade.get("awaiting_pink_candle"):
                        direction   = pending_manual_trade["direction"]
                        grade       = pending_manual_trade["grade"]
                        probability = pending_manual_trade["probability"]
                        reason      = pending_manual_trade["reason"]
                        answer_callback(cb_id, "🩷 Confirmed pink")
                        edit_prompt_message(msg_id, f"🩷 <b>{direction.upper()} — Grade {grade}</b>\n\nPink candle confirmed.")
                        pending_manual_trade = None
                        _send_final_grade_confirm(direction, grade, probability, reason)
                    elif cb_data.startswith("pinkcandle_no_") and pending_manual_trade and pending_manual_trade.get("awaiting_pink_candle"):
                        direction = pending_manual_trade["direction"]
                        grade     = pending_manual_trade["grade"]
                        db_log_warning("wpr_guard_block", f"{direction.capitalize()} Grade {grade} blocked — candle not confirmed pink")
                        answer_callback(cb_id, "❌ Not pink — blocked")
                        edit_prompt_message(msg_id, f"🚫 <b>{direction.upper()} — Grade {grade} blocked</b>\n\nCandle not confirmed pink — condition not met, trade blocked.")
                        pending_manual_trade = None

                    elif cb_data.startswith("manual_confirm_") and pending_manual_trade:
                        direction = pending_manual_trade["direction"]
                        grade     = pending_manual_trade["grade"]
                        db_log_warning("manual_trade_confirmed", f"{direction.capitalize()} Grade {grade} confirmed")
                        answer_callback(cb_id, f"✅ Confirmed — Grade {grade}")
                        edit_prompt_message(msg_id, f"✅ <b>{direction.upper()} confirmed — Grade {grade}</b>\n\nExecuting...")
                        pending_manual_trade = None
                        threading.Thread(
                            target=execute_trade,
                            kwargs=dict(direction=direction, triggered_by=f"manual_{direction}_grade_{grade}"),
                            daemon=True
                        ).start()
                    elif cb_data.startswith("manual_cancel_") and pending_manual_trade:
                        direction = pending_manual_trade["direction"]
                        grade     = pending_manual_trade["grade"]
                        db_log_warning("manual_trade_cancelled", f"{direction.capitalize()} Grade {grade} cancelled at confirmation")
                        answer_callback(cb_id, "❌ Cancelled.")
                        edit_prompt_message(msg_id, f"❌ <b>{direction.upper()} — Grade {grade}</b>\n\nCancelled — no trade taken.")
                        pending_manual_trade = None

                    elif cb_data.startswith("reset_bias_") and pending_bias_reset:
                        parts       = cb_data.split("_")
                        new_bias    = parts[2]   # bullish or bearish
                        signal_key  = "_".join(parts[3:])
                        msg_id      = pending_bias_reset["message_id"]
                        pending_bias_reset = None
                        
                        daily_bias = new_bias
                        answer_callback(cb_id, f"✅ Bias reset to {new_bias.upper()}")
                        edit_prompt_message(msg_id,
                            f"✅ <b>Bias reset to {new_bias.upper()}</b>\n\n"
                            f"HTF change confirmed. Trading with new bias.\n\n"
                            f"Triggered by: {signal_key}"
                        )
                        send_telegram(f"📊 Daily bias reset to <b>{new_bias.upper()}</b>\n\n"
                                      f"Triggered by structure break: {signal_key}")
                        db_log_htf_bias_change(new_bias, signal_key, "manual_reset")

                    elif cb_data.startswith("ignore_reset_") and pending_bias_reset:
                        signal_key  = cb_data.split("ignore_reset_")[1]
                        msg_id      = pending_bias_reset["message_id"]
                        pending_bias_reset = None
                        
                        answer_callback(cb_id, "Ignored.")
                        edit_prompt_message(msg_id,
                            f"❌ Bias change ignored.\n\n"
                            f"Staying with current bias: {daily_bias.upper()}"
                        )
                        db_log_htf_bias_change(daily_bias, signal_key, "ignored")

                    elif cb_data.startswith("stress_") and pending_stress_check:
                        parts     = cb_data.split("_")
                        state     = parts[1]   # relaxed, focused, elevated
                        direction = parts[2]   # long, short
                        label     = pending_stress_check["label"]
                        sc_msg_id = pending_stress_check["message_id"]
                        pending_stress_check = None

                        if state == "elevated":
                            answer_callback(cb_id, "🚫 Trade blocked.")
                            edit_prompt_message(sc_msg_id,
                                f"🚫 <b>Trade blocked — elevated stress state</b>\n\n"
                                f"You flagged yourself as elevated before this {direction.upper()}.\n"
                                f"All signal prompts silenced for 10 minutes.\n\n"
                                f"Step away, reset, and come back with a clear head."
                            )
                            db_log_stress(direction, label, "elevated", False)
                            start_prompt_silence("Take a breath. Come back when you're ready.")
                        elif state == "distracted":
                            answer_callback(cb_id, "🌀 Trade blocked — not now.")
                            edit_prompt_message(sc_msg_id,
                                f"🌀 <b>Trade blocked — you're distracted</b>\n\n"
                                f"This {direction.upper()} is skipped.\n"
                                f"Come back when you have focus."
                            )
                            db_log_stress(direction, label, "distracted", False)
                        else:
                            state_emoji = "😌" if state == "relaxed" else "🎯"
                            state_label = state.title()
                            answer_callback(cb_id, f"{state_emoji} {state_label} — executing...")
                            edit_prompt_message(sc_msg_id,
                                f"{state_emoji} <b>{state_label} — placing {direction.upper()}...</b>\n\n"
                                f"Signal: {label}"
                            )
                            db_log_stress(direction, label, state, True)
                            threading.Thread(
                                target=execute_trade,
                                kwargs=dict(direction=direction, triggered_by=label),
                                daemon=True
                            ).start()

                    else:
                        answer_callback(cb_id, "No active signal.")
                    continue

                msg = update.get("message", {})

                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                text = msg.get("text", "").strip()

                parts = text.lower().split()
                if not parts or not parts[0].startswith("/"):
                    continue

                cmd   = parts[0].lstrip("/").split("@")[0]

                size_usdt = None
                size_pct  = None
                stop_pct  = None
                tp_pct    = None

                raw_args = parts[1:]
                if raw_args:
                    size_raw = raw_args[0]
                    if size_raw.endswith("%"):
                        try: size_pct = float(size_raw[:-1])
                        except ValueError: pass
                    else:
                        try: size_usdt = float(size_raw)
                        except ValueError: pass
                    if len(raw_args) >= 3:
                        try: stop_pct = float(raw_args[1])
                        except ValueError: pass
                        try: tp_pct   = float(raw_args[2])
                        except ValueError: pass

                print(f"[Telegram CMD] /{cmd} size_usdt={size_usdt} size_pct={size_pct} sl={stop_pct} tp={tp_pct}")

                if cmd == "close":
                    if raw_args and raw_args[0].replace(".", "").isdigit():
                        partial_close(float(raw_args[0]))
                    else:
                        close_position_now()
                elif cmd == "long":
                    # Manual /long should execute the same way it historically did.
                    # The newer grade/pink-candle flow was intercepting the command
                    # before execute_trade(), so /long could appear to do nothing
                    # (or wait for a confirmation flow that was never completed).
                    send_telegram("📈 <b>/LONG received</b> — executing trade checks...")
                    threading.Thread(
                        target=execute_trade,
                        kwargs=dict(
                            direction="long",
                            size_pct=size_pct,
                            size_usdt=size_usdt,
                            stop_pct=stop_pct,
                            tp_pct=tp_pct,
                            triggered_by="manual_long",
                        ),
                        daemon=True,
                    ).start()
                elif cmd == "short":
                    # Same restoration for /short. All existing execution guards
                    # inside execute_trade() remain active.
                    send_telegram("📉 <b>/SHORT received</b> — executing trade checks...")
                    threading.Thread(
                        target=execute_trade,
                        kwargs=dict(
                            direction="short",
                            size_pct=size_pct,
                            size_usdt=size_usdt,
                            stop_pct=stop_pct,
                            tp_pct=tp_pct,
                            triggered_by="manual_short",
                        ),
                        daemon=True,
                    ).start()
                elif cmd == "cut1":
                    partial_close(25)
                elif cmd == "cut2":
                    partial_close(50)
                elif cmd == "sl":
                    if raw_args:
                        try:
                            arg = raw_args[0].replace("%", "")
                            pct_val = float(arg)
                            if pct_val >= 1:
                                pct_val /= 100
                            pos = get_open_position()
                            if not pos:
                                send_telegram("❌ No open position.")
                            else:
                                entry  = float(pos.get("entryPrice", 0))
                                amt    = float(pos["positionAmt"])
                                tick   = get_symbol_info()["price_tick"]
                                new_sl = round_step(entry * (1 - pct_val / 100), tick) if amt > 0 \
                                         else round_step(entry * (1 + pct_val / 100), tick)
                                adjust_sl(new_sl)
                        except ValueError:
                            send_telegram("❌ Usage: /sl 0.36  (percentage from entry)")
                    else:
                        send_telegram("❌ Usage: /sl 0.36  (percentage from entry)")
                elif cmd == "tp":
                    if raw_args:
                        try:
                            pct_val = float(raw_args[0].replace("%", ""))
                            if pct_val >= 1:
                                pct_val /= 100
                            pos = get_open_position()
                            if not pos:
                                send_telegram("❌ No open position.")
                            else:
                                entry  = float(pos.get("entryPrice", 0))
                                amt    = float(pos["positionAmt"])
                                tick   = get_symbol_info()["price_tick"]
                                new_tp = round_step(entry * (1 + pct_val / 100), tick) if amt > 0 \
                                         else round_step(entry * (1 - pct_val / 100), tick)
                                adjust_tp(new_tp, label="TP1")
                        except ValueError:
                            send_telegram("❌ Usage: /tp 0.36  (percentage from entry)")
                    else:
                        send_telegram("❌ Usage: /tp 0.36  (percentage from entry)")
                elif cmd == "tp2":
                    if raw_args:
                        try:
                            pct_val = float(raw_args[0].replace("%", ""))
                            if pct_val >= 1:
                                pct_val /= 100
                            pos = get_open_position()
                            if not pos:
                                send_telegram("❌ No open position.")
                            else:
                                entry   = float(pos.get("entryPrice", 0))
                                amt     = float(pos["positionAmt"])
                                tick    = get_symbol_info()["price_tick"]
                                new_tp2 = round_step(entry * (1 + pct_val / 100), tick) if amt > 0 \
                                          else round_step(entry * (1 - pct_val / 100), tick)
                                adjust_tp(new_tp2, label="TP2")
                        except ValueError:
                            send_telegram("❌ Usage: /tp2 0.50  (percentage from entry)")
                    else:
                        send_telegram("❌ Usage: /tp2 0.50  (percentage from entry)")
                elif cmd == "breakeven":
                    move_to_breakeven()
                elif cmd in ("hl_debug", "hl2_debug", "clientdebug"):
                    # hl_debug / hl2_debug kept as muscle-memory aliases for
                    # clients #1/#2; clientdebug <id> works for any client,
                    # e.g. /clientdebug 3
                    if cmd == "hl_debug":
                        debug_id = "1"
                    elif cmd == "hl2_debug":
                        debug_id = "2"
                    else:
                        debug_id = parts[1] if len(parts) > 1 else None
                    target = next((c for c in CLIENTS if c.id == debug_id), None) if debug_id else None
                    if not target:
                        ids = ", ".join(c.id for c in CLIENTS) or "none configured"
                        send_telegram(f"Usage: /clientdebug <id>\nConfigured client ids: {ids}")
                    elif not target.is_configured():
                        send_telegram(f"⚠️ {target.label()} not configured — private key/address not set.")
                    else:
                        try:
                            info    = hl_get_info()
                            state   = info.user_state(target.address)
                            balance = client_get_balance(target)
                            mark    = hl_get_mark_price()
                            size    = client_calculate_size(target)
                            send_telegram(
                                f"<b>{target.label()} Debug</b>\n\n"
                                f"Address: {target.address[:10]}...\n"
                                f"Balance: ${balance:.4f}\n"
                                f"Mark: ${mark:,.2f}\n"
                                f"Calc size: {size} BTC\n\n"
                                f"Raw state: {state.get('marginSummary', {})}"
                            )
                        except Exception as e:
                            send_telegram(f"{target.label()} Debug error: {e}")
                elif cmd == "stats":
                    pnl_summary()
                elif cmd == "patterns":
                    rows     = db_get_trigger_performance()
                    avg_gaps = db_get_avg_trigger_gaps()
                    if not rows and not avg_gaps:
                        send_telegram("📊 No trigger data in the database yet.")
                    else:
                        from collections import defaultdict
                        TRIGGER_LABELS = {
                            "long": "📈 Long", "3_bulls": "🐂 3 Bulls", "strength": "💪 Strength",
                            "3_bears": "🐻 3 Bears", "weakness": "📉 Weakness",
                            "bearish": "🔴 Bearish", "short": "📉 Short", "30m_composite_crosses_up_30": "📈 30m Composite Crosses UP 30", "30m_composite_crosses_down_70": "📉 30m Composite Crosses DOWN 70", "30m_composite_crosses_down_85": "📉 30m Composite Crosses DOWN 85", "30m_composite_crosses_up_15": "📈 30m Composite Crosses UP 15", "wpr_crosses_down_18": "📉 WPR Crosses Down -18", "wpr_crosses_up_82": "📈 WPR Crosses Up -82", "buy": "📈 Buy",
                        }
                        groups = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
                        for r in rows:
                            key = r["trigger"]
                            if r["outcome"] == "win":
                                groups[key]["wins"]   += 1
                            else:
                                groups[key]["losses"] += 1
                            groups[key]["pnl"] += r["pnl"] or 0

                        def fmt_mins(m):
                            if m is None: return "—"
                            if m >= 60: return f"{int(m//60)}h {int(m%60)}m"
                            return f"{int(m)}m"

                        RETIRED_SIGNALS = {
                            "long_setup_10m", "short_setup_10m", "short_setup_5m", "fork",
                            "3_bulls", "weakness", "3_bar_buy",
                            "composite_higher_low", "composite_lower_high",
                            # Composite threshold signals — informational only, never generate trades
                            "composite_crossed_above_15", "composite_crossed_below_15",
                            "composite_crossed_above_82", "composite_crossed_below_82",
                            "composite_crossed_above_18", "composite_crossed_below_18",
                            "composite_crossed_above_85", "composite_crossed_below_85",
                            "htf_crossed_above_12", "htf_crossed_below_12",
                            "htf_crossed_above_88", "htf_crossed_below_88",
                            # 2H structure breaks — informational only
                            "30m_bullish_structure_break", "30m_bearish_structure_break",
                            # Old cross signal names — replaced by 30m/4H versions
                            "30m_crosses_down_2h_composite", "30m_crosses_up_2h_composite",
                            "15m_crosses_down_2h_composite", "15m_crosses_up_2h_composite",
                            # Old-style MTF combo naming — replaced by long/short naming
                            "green_ltf_red_htf_bullish", "green_ltf_green_htf_bullish",
                            "green_ltf_cyan_htf_bullish", "green_ltf_yellow_htf_bullish",
                            "orange_ltf_red_htf_bearish", "orange_ltf_orange_htf_bearish",
                            "orange_ltf_green_htf_bearish",
                        }
                        all_keys = {k for k in (set(groups.keys()) | set(avg_gaps.keys())) - RETIRED_SIGNALS if not k.startswith("exp_")}
                        lines = ["📊 <b>Trigger performance</b>\n<i>Bot-executed trades only — doesn't include manual Binance app trades</i>\n"]
                        sorted_keys = sorted(all_keys, key=lambda k: groups[k]["wins"] / max(groups[k]["wins"] + groups[k]["losses"], 1) if k in groups else 0, reverse=True)
                        for key in sorted_keys:
                            label   = TRIGGER_LABELS.get(key, key.replace("_", " ").title())
                            g       = groups.get(key, {"wins": 0, "losses": 0, "pnl": 0.0})
                            total   = g["wins"] + g["losses"]
                            gap_str = fmt_mins(avg_gaps.get(key))
                            if total > 0:
                                wr      = g["wins"] / total * 100
                                pnl_str = f"+${g['pnl']:.2f}" if g["pnl"] >= 0 else f"-${abs(g['pnl']):.2f}"
                                lines.append(f"<b>{label}</b>  {g['wins']}W / {g['losses']}L — {wr:.0f}% WR — {pnl_str}\nAvg gap between signals: {gap_str}")
                            else:
                                lines.append(f"<b>{label}</b>  No trades yet\nAvg gap between signals: {gap_str}")
                        send_telegram("\n\n".join(lines))
                elif cmd == "unstick":
                    global stopped_today, daily_losses_block, first_loss_tightened_today, STOP_THRESHOLD
                    was_stopped = stopped_today
                    was_blocked = daily_losses_block
                    stopped_today = False
                    daily_losses_block = False
                    first_loss_tightened_today = False
                    STOP_THRESHOLD = STOP_THRESHOLD_DEFAULT
                    db_log_warning("manual_unstick", f"Manually cleared stopped_today (was {was_stopped}), daily_losses_block (was {was_blocked}), reset STOP_THRESHOLD to {STOP_THRESHOLD_DEFAULT}")
                    send_telegram(
                        f"🔓 <b>Manually unstuck</b>\n\n"
                        f"stopped_today: {was_stopped} → False\n"
                        f"daily_losses_block: {was_blocked} → False\n"
                        f"STOP_THRESHOLD reset to {STOP_THRESHOLD_DEFAULT}\n\n"
                        f"Trading should work again immediately — no restart needed."
                    )
                elif cmd == "wsstatus":
                    healthy = _ws_is_healthy()
                    seconds_since_msg = time.time() - _ws_last_message_at if _ws_last_message_at else None
                    lines = [
                        f"🔌 <b>Websocket Status</b>\n",
                        f"Connected: {'✅ Yes' if _ws_connected else '❌ No'}",
                        f"Healthy: {'✅ Yes' if healthy else '❌ No (stale or disconnected)'}",
                    ]
                    if seconds_since_msg is not None:
                        lines.append(f"Last message: {seconds_since_msg:.0f}s ago (stale after {WS_STALE_THRESHOLD_SEC}s)")
                    else:
                        lines.append("Last message: never received")
                    lines.append("")
                    lines.append("<b>Kline history per interval:</b>")
                    for interval in WS_KLINE_INTERVALS:
                        count = len(_ws_klines.get(interval, []))
                        lines.append(f"  {interval}: {count} candles")
                    mark_age = time.time() - _ws_mark_price["updated_at"] if _ws_mark_price["value"] is not None else None
                    if mark_age is not None:
                        lines.append(f"\nMark price: ${_ws_mark_price['value']:,.2f} ({mark_age:.0f}s ago)")
                    else:
                        lines.append("\nMark price: not yet received")
                    if not healthy:
                        lines.append("\n⚠️ Falling back to REST for all live data right now — same rate-limit risk as before the websocket build.")
                    send_telegram("\n".join(lines))
                elif cmd == "live":
                    now_uk_live = datetime.now(UK_TZ)
                    reasons = []

                    if now_uk_live.weekday() >= 5:
                        reasons.append(f"📅 Weekend ({now_uk_live.strftime('%A')}) — all signals suppressed until Monday.")

                    if now_uk_live.weekday() == 4 and now_uk_live.time() >= dt_time(19, 0):
                        reasons.append("🕖 Friday cutoff (19:00+) — no new trades until Monday.")

                    if elevated_silence_until > time.time():
                        remaining_s = int(elevated_silence_until - time.time())
                        reasons.append(f"😤 Elevated stress cooldown — {remaining_s // 60}m {remaining_s % 60}s remaining.")

                    if daily_losses_block:
                        reasons.append(f"🚫 {DAILY_LOSS_STREAK_LIMIT} losses today — blocked until midnight.")

                    if stopped_today:
                        reasons.append("🚫 Daily trade cap or $ loss limit hit — blocked until midnight.")

                    if reasons:
                        send_telegram(
                            "🔇 <b>Prompts are currently OFF.</b>\n\n" + "\n".join(reasons)
                        )
                    else:
                        send_telegram(
                            "🟢 <b>Prompts are live.</b>\n\n"
                            "No active blocks — signals will prompt you normally right now."
                        )
                elif cmd == "winrate":
                    WIN_RATE_TARGET = 60.0
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            windows = [("Today", "1 day"), ("7d", "7 days"), ("30d", "30 days")]
                            lines = [f"🎯 <b>Win Rate — target {WIN_RATE_TARGET:.0f}%+</b>\n<i>Bot-executed trades only — doesn't include manual Binance app trades</i>\n"]
                            for label, interval in windows:
                                cur.execute("""
                                    SELECT
                                        COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                                        COUNT(*) FILTER (WHERE outcome = 'loss') as losses
                                    FROM trigger_performance
                                    WHERE entry_time >= NOW() - INTERVAL %s
                                      AND outcome IN ('win', 'loss')
                                """, (interval,))
                                r = cur.fetchone()
                                total = (r["wins"] or 0) + (r["losses"] or 0)
                                if total > 0:
                                    wr = r["wins"] / total * 100
                                    emoji = "✅" if wr >= WIN_RATE_TARGET else "⚠️"
                                    gap = wr - WIN_RATE_TARGET
                                    gap_line = f"(+{gap:.0f}pts above target)" if gap >= 0 else f"({gap:.0f}pts below target)"
                                    lines.append(f"{emoji} <b>{label}:</b> {wr:.0f}% WR  ({r['wins']}W/{r['losses']}L)  {gap_line}")
                                else:
                                    lines.append(f"— <b>{label}:</b> no closed trades yet")
                            cur.close()
                            conn.close()
                            send_telegram("\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Win rate report error: {e}")
                elif cmd == "compound":
                    progress = get_compounding_progress()
                    if not progress or "week_multiplier" not in progress:
                        send_telegram("📊 Could not calculate compounding progress — check database connection.")
                    else:
                        week_mult   = progress["week_multiplier"]
                        week_target = WEEKLY_COMPOUND_TARGET
                        week_emoji  = "✅" if week_mult >= week_target else "📈"
                        week_pct    = progress["week_pct_of_target"]

                        lines = [
                            f"📊 <b>Compounding Progress</b>\n",
                            f"{week_emoji} <b>This week:</b> {week_mult:.4f}x  (target {week_target:.4f}x)\n"
                            f"  Start: ${progress['week_start_balance']:,.2f}  →  Now: ${progress['current_balance']:,.2f}\n"
                            f"  Target balance: ${progress['week_target_balance']:,.2f}\n"
                            f"  Progress to target: {week_pct:.1f}%"
                        ]

                        if "cycle_multiplier" in progress:
                            cycle_mult   = progress["cycle_multiplier"]
                            cycle_target = CYCLE_COMPOUND_TARGET
                            cycle_emoji  = "✅" if cycle_mult >= cycle_target else "📈"
                            cycle_pct    = progress["cycle_pct_of_target"]
                            lines.append(
                                f"\n{cycle_emoji} <b>This 4-week cycle:</b> {cycle_mult:.4f}x  (target {cycle_target:.4f}x)\n"
                                f"  Cycle start: ${progress['cycle_start_balance']:,.2f}  ({progress['cycle_start_date']})\n"
                                f"  Target balance: ${progress['cycle_target_balance']:,.2f}\n"
                                f"  Progress to target: {cycle_pct:.1f}%"
                            )

                        send_telegram("\n".join(lines))
                elif cmd == "vwinrate":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            days = 30
                            if len(parts) > 1:
                                try:
                                    days = int(parts[1])
                                except (ValueError, IndexError):
                                    pass
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("""
                                SELECT
                                    trigger,
                                    COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                                    COUNT(*) FILTER (WHERE outcome = 'win') as wins,
                                    COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                                    COUNT(*) FILTER (WHERE outcome IS NULL) as still_open
                                FROM virtual_trigger_trades
                                WHERE opened_at >= NOW() - INTERVAL '%s days'
                                GROUP BY trigger
                                ORDER BY (COUNT(*) FILTER (WHERE outcome = 'win')::float /
                                          NULLIF(COUNT(*) FILTER (WHERE outcome IN ('win','loss')), 0)) DESC NULLS LAST
                            """, (days,))
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()

                            if not rows:
                                send_telegram(f"📊 No virtual trigger trades in the last {days}d yet.")
                            else:
                                TRIGGER_LABELS = {
                                    "long": "📈 Long", "3_bulls": "🐂 3 Bulls",
                                    "strength": "💪 Strength", "3_bar_buy": "📈 3 Bar Buy",
                                    "3_bears": "🐻 3 Bears", "weakness": "📉 Weakness",
                                    "bearish": "🔴 Bearish", "short": "📉 Short",
                                    "green_structure_break": "🟢 Green Structure Break",
                                    "orange_structure_break": "🟠 Orange Structure Break",
                                    
                                    
                                    "30m_composite_crosses_up_30": "📈 30m Composite Crosses UP 30",
                                    "30m_composite_crosses_down_70": "📉 30m Composite Crosses DOWN 70", "30m_composite_crosses_down_85": "📉 30m Composite Crosses DOWN 85", "30m_composite_crosses_up_15": "📈 30m Composite Crosses UP 15", "wpr_crosses_down_18": "📉 WPR Crosses Down -18", "wpr_crosses_up_82": "📈 WPR Crosses Up -82", "buy": "📈 Buy",
                                }
                                lines = [
                                    f"🎯 <b>Virtual trigger win rates — last {days}d</b>\n"
                                    f"<i>Simulated, not real trades — fixed {VIRTUAL_TP_PCT}% TP / {VIRTUAL_SL_PCT}% SL "
                                    f"applied to every signal fire, regardless of whether it was actually traded, "
                                    f"bot or manual</i>\n"
                                ]
                                for r in rows:
                                    label = TRIGGER_LABELS.get(r["trigger"], r["trigger"].replace("_", " ").title())
                                    total = r["total"] or 0
                                    if total > 0:
                                        wr = r["wins"] / total * 100
                                        open_note = f"  ({r['still_open']} still open)" if r["still_open"] else ""
                                        lines.append(
                                            f"<b>{label}</b>: {wr:.0f}% WR  ({r['wins']}W/{r['losses']}L, {total} resolved){open_note}"
                                        )
                                    elif r["still_open"]:
                                        lines.append(f"<b>{label}</b>: {r['still_open']} pending, no resolved trades yet")
                                send_telegram("\n\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Virtual win rate report error: {e}")
                elif cmd == "timewindow":
                    try:
                        todays_trades_tw = get_futures_trades_today()
                        tit = calculate_time_in_trade(todays_trades_tw)
                        if tit["window_mins"] is None:
                            send_telegram("📅 No closed positions yet today.")
                        else:
                            def _fmt_hm(mins):
                                h, m = divmod(int(round(mins)), 60)
                                return f"{h}h {m}m" if h else f"{m}m"
                            under_target = tit["in_trade_pct"] <= TIME_IN_TRADE_TARGET_PCT
                            emoji = "✅" if under_target else "⚠️"
                            send_telegram(
                                f"📅 <b>Time in trade — {TRADING_WINDOW_START_HOUR:02d}:00–{TRADING_WINDOW_END_HOUR:02d}:00 window</b>\n\n"
                                f"In a trade: {_fmt_hm(tit['in_trade_mins'])}  ({tit['in_trade_pct']:.0f}%)\n"
                                f"Window:     {_fmt_hm(tit['window_mins'])}  (fixed {TRADING_WINDOW_END_HOUR - TRADING_WINDOW_START_HOUR}h window)\n\n"
                                f"{emoji} Target: under {TIME_IN_TRADE_TARGET_PCT:.0f}%"
                            )
                    except Exception as e:
                        send_telegram(f"❌ Time window calc error: {e}")
                elif cmd == "discipline":
                    week_comp  = get_discipline_comparison(7)
                    month_comp = get_discipline_comparison(30)
                    week_lines  = format_discipline_comparison_lines(week_comp)
                    month_lines = format_discipline_comparison_lines(month_comp)
                    lines = ["📊 <b>Discipline Trend</b>\n"]
                    if week_lines:
                        lines.append("<b>This week vs last week:</b>\n" + "\n".join(week_lines))
                    if month_lines:
                        lines.append("\n<b>This month vs last month:</b>\n" + "\n".join(month_lines))
                    if not week_lines and not month_lines:
                        lines.append("Not enough data yet.")
                    send_telegram("\n".join(lines))
                elif cmd == "wpr":
                    status_line = "🟢 Enabled" if WPR_GUARD_ENABLED else "🔴 Disabled (set WPR_GUARD_ENABLED=true)"
                    wpr = get_live_williams_r(interval="15m", length=120)
                    if wpr is not None:
                        long_status  = "🚫 Blocked" if wpr > WPR_LONG_MAX else "✅ Allowed"
                        short_status = "🚫 Blocked" if wpr < WPR_SHORT_MIN else "✅ Allowed"
                        send_telegram(
                            f"📐 <b>Williams %R Guard (15m, length 30)</b>\n\n"
                            f"Status: {status_line}\n\n"
                            f"Current WPR: {wpr:.1f}\n"
                            f"Long max:  {WPR_LONG_MAX:.0f}  ({long_status})\n"
                            f"Short min: {WPR_SHORT_MIN:.0f}  ({short_status})"
                        )
                    else:
                        send_telegram(f"📐 <b>Williams %R Guard</b>\n\nStatus: {status_line}\n\nCould not fetch live WPR data.")
                elif cmd == "unmatched":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            hours = 168  # default 7 days — this is a "did I miss wiring something" check
                            if len(parts) > 1:
                                try:
                                    hours = int(parts[1])
                                except (ValueError, IndexError):
                                    pass
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("""
                                SELECT message, logged_at
                                FROM warnings_log
                                WHERE category = 'unrecognized_signal'
                                  AND logged_at >= NOW() - INTERVAL '%s hours'
                                ORDER BY logged_at DESC
                                LIMIT 20
                            """, (hours,))
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()
                            if not rows:
                                send_telegram(f"✅ No unrecognized signals in the last {hours}h — every alert that's fired matched a known handler.")
                            else:
                                lines = [f"❓ <b>Unrecognized signals — last {hours}h</b> ({len(rows)})\n"]
                                for r in rows:
                                    ts = r["logged_at"].strftime("%d %b %H:%M")
                                    lines.append(f"<b>{ts}</b> — {r['message']}")
                                lines.append("\n👉 These fired from TradingView but matched nothing in bot.py. Worth checking if they need wiring in.")
                                send_telegram("\n\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Unmatched signals report error: {e}")
                elif cmd == "warnings":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            hours = 24
                            if len(parts) > 1:
                                try:
                                    hours = int(parts[1])
                                except (ValueError, IndexError):
                                    pass
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("""
                                SELECT category, message, logged_at
                                FROM warnings_log
                                WHERE logged_at >= NOW() - INTERVAL '%s hours'
                                ORDER BY logged_at DESC
                                LIMIT 30
                            """, (hours,))
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()
                            if not rows:
                                send_telegram(f"✅ No warnings in the last {hours}h. Clean session.")
                            else:
                                CATEGORY_EMOJI = {
                                    "counter_signal":    "⚠️",
                                    "loss_streak":       "🔴",
                                    "daily_loss_block":  "🚫",
                                    "revenge_trade":     "😤",
                                    "retrace_protect":   "🛡️",
                                    "fee_threshold":      "💸",
                                    "first_loss_tighten": "🎯",
                                    "daily_loss_limit":   "🚫",
                                    "brute_force_close":  "🛡️",
                                    "hard_structure_invalidate": "🚨",
                                    "counter_signal_close": "🔄",
                                    "ema_guard_block": "📐",
                                    "simple_ema_guard_block": "📏",
                                    "overnight_auto_breakeven": "😴",
                                    "aggressive_volume": "📊",
                                    "underwater_ratio": "⏱️",
                                    "wpr_guard_block": "📐",
                                    "time_in_trade_warning": "⏱️",
                                    "htf_extreme_block": "🌡️",
                                    "unrecognized_signal": "❓",
                                }
                                lines = [f"📋 <b>Warnings — last {hours}h</b> ({len(rows)})\n"]
                                for r in rows:
                                    emoji = CATEGORY_EMOJI.get(r["category"], "•")
                                    ts    = r["logged_at"].strftime("%H:%M")
                                    lines.append(f"{emoji} <b>{ts}</b> — {r['message']}")
                                send_telegram("\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Warnings report error: {e}")
                elif cmd == "ema":
                    status_line = "🟢 Enabled" if EMA_GUARD_ENABLED else "🔴 Disabled (set EMA_GUARD_ENABLED=true)"
                    live_ema120 = get_live_ema120_guard()
                    if live_ema120 is not None:
                        try:
                            mark = get_mark_price()
                            if mark > live_ema120:
                                position_line = "🚫 Price ABOVE 120 EMA — longs blocked (premium)"
                            elif mark < live_ema120:
                                position_line = "🚫 Price BELOW 120 EMA — shorts blocked (discount)"
                            else:
                                position_line = "✅ Price at 120 EMA — both directions valid"
                            price_line = (
                                f"Price: ${mark:,.2f}\n"
                                f"120 EMA: ${live_ema120:,.2f}\n\n"
                                f"{position_line}\n\n"
                                f"Calculated live from Binance 15m candles — no TradingView dependency."
                            )
                        except Exception:
                            price_line = "Could not fetch current mark price."
                    else:
                        price_line = "Could not calculate EMAs — Binance kline fetch failed."
                    send_telegram(
                        f"📐 <b>EMA Premium/Discount Guard</b>\n\n"
                        f"Status: {status_line}\n\n"
                        f"{price_line}"
                    )
                elif cmd == "brute":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            days = 30
                            if len(parts) > 1:
                                try:
                                    days = int(parts[1])
                                except (ValueError, IndexError):
                                    pass
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("""
                                SELECT category, message, logged_at
                                FROM warnings_log
                                WHERE category = 'hard_structure_invalidate'
                                  AND logged_at >= NOW() - INTERVAL '%s days'
                                ORDER BY logged_at DESC
                            """, (days,))
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()
                            if not rows:
                                send_telegram(f"🚨 No structure-invalidation closes in the last {days}d.")
                            else:
                                lines = [f"🚨 <b>Structure-invalidation closes — last {days}d</b> ({len(rows)})\n"]
                                for r in rows:
                                    ts = r["logged_at"].strftime("%d %b %H:%M")
                                    lines.append(f"🚨 <b>{ts}</b> — {r['message']}")
                                send_telegram("\n\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Brute-force report error: {e}")
                elif cmd == "analytics":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            days = 30
                            if len(parts) > 1:
                                try:
                                    days = int(parts[1])
                                except (ValueError, IndexError):
                                    pass
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            sections = []

                            # ── Section 1: Fee threshold breaches vs NY Open trading ──
                            cur.execute("""
                                WITH fee_days AS (
                                    SELECT DISTINCT logged_at::date as day
                                    FROM warnings_log
                                    WHERE category = 'fee_threshold'
                                      AND logged_at >= NOW() - INTERVAL '%s days'
                                ),
                                ny_open_trades AS (
                                    SELECT DISTINCT entry_time::date as day
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL
                                      AND entry_time::time >= %s AND entry_time::time <= %s
                                      AND entry_time >= NOW() - INTERVAL '%s days'
                                )
                                SELECT
                                    (SELECT COUNT(*) FROM fee_days) as fee_day_count,
                                    (SELECT COUNT(*) FROM fee_days WHERE day IN (SELECT day FROM ny_open_trades)) as fee_and_ny_count
                            """, (days, COOLDOWN_START, COOLDOWN_END, days))
                            r1 = cur.fetchone()
                            if r1 and r1["fee_day_count"] and r1["fee_day_count"] > 0:
                                pct = (r1["fee_and_ny_count"] / r1["fee_day_count"] * 100)
                                sections.append(
                                    f"💸 <b>Fee threshold breaches</b>\n"
                                    f"  {r1['fee_day_count']} day(s) hit fee threshold.\n"
                                    f"  {r1['fee_and_ny_count']}/{r1['fee_day_count']} ({pct:.0f}%) also traded in the "
                                    f"{COOLDOWN_START.strftime('%H:%M')}–{COOLDOWN_END.strftime('%H:%M')} block window."
                                )
                            else:
                                sections.append("💸 <b>Fee threshold breaches</b>\n  No fee threshold breaches recorded.")

                            # ── Section 2: First trade of day = loss — what time does it happen? ──
                            cur.execute("""
                                SELECT entry_time
                                FROM trigger_performance
                                WHERE id IN (
                                    SELECT DISTINCT ON (entry_time::date) id
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL AND outcome IN ('win','loss')
                                      AND entry_time >= NOW() - INTERVAL '%s days'
                                    ORDER BY entry_time::date, entry_time ASC
                                )
                                AND outcome = 'loss'
                            """, (days,))
                            first_loss_rows = cur.fetchall()
                            if first_loss_rows:
                                hours_list = [r["entry_time"].hour for r in first_loss_rows]
                                avg_hour   = sum(hours_list) / len(hours_list)
                                from collections import Counter
                                hour_counts = Counter(hours_list)
                                top_hour, top_count = hour_counts.most_common(1)[0]
                                sections.append(
                                    f"🎯 <b>First trade of day = loss</b>\n"
                                    f"  Happened {len(first_loss_rows)} time(s) in the last {days}d.\n"
                                    f"  Avg time: {int(avg_hour):02d}:xx UK.\n"
                                    f"  Most common hour: {top_hour:02d}:xx ({top_count}x)."
                                )
                            else:
                                sections.append("🎯 <b>First trade of day = loss</b>\n  Hasn't happened in this window. Strong start rate.")

                            # ── Section 3: Loss streak warnings — time of day clustering ──
                            cur.execute("""
                                SELECT logged_at
                                FROM warnings_log
                                WHERE category = 'loss_streak'
                                  AND logged_at >= NOW() - INTERVAL '%s days'
                                ORDER BY logged_at
                            """, (days,))
                            streak_rows = cur.fetchall()
                            if streak_rows:
                                from collections import Counter
                                streak_hours = Counter(r["logged_at"].hour for r in streak_rows)
                                top_streak_hour, top_streak_count = streak_hours.most_common(1)[0]
                                sections.append(
                                    f"🔴 <b>Loss streak warnings</b>\n"
                                    f"  Fired {len(streak_rows)} time(s) in the last {days}d.\n"
                                    f"  Most common hour: {top_streak_hour:02d}:xx ({top_streak_count}x)."
                                )
                            else:
                                sections.append("🔴 <b>Loss streak warnings</b>\n  None recorded in this window.")

                            # ── Section 4: Revenge trades — clustering after loss streaks ──
                            cur.execute("""
                                SELECT
                                    (SELECT COUNT(*) FROM warnings_log WHERE category = 'revenge_trade' AND logged_at >= NOW() - INTERVAL '%s days') as revenge_count,
                                    (SELECT COUNT(*) FROM warnings_log w1
                                     WHERE w1.category = 'revenge_trade'
                                       AND w1.logged_at >= NOW() - INTERVAL '%s days'
                                       AND EXISTS (
                                           SELECT 1 FROM warnings_log w2
                                           WHERE w2.category = 'loss_streak'
                                             AND w2.logged_at < w1.logged_at
                                             AND w2.logged_at >= w1.logged_at - INTERVAL '2 hours'
                                       )
                                    ) as revenge_after_streak
                            """, (days, days))
                            r4 = cur.fetchone()
                            if r4 and r4["revenge_count"] and r4["revenge_count"] > 0:
                                pct4 = (r4["revenge_after_streak"] / r4["revenge_count"] * 100)
                                sections.append(
                                    f"😤 <b>Revenge trades</b>\n"
                                    f"  {r4['revenge_count']} flagged in the last {days}d.\n"
                                    f"  {r4['revenge_after_streak']}/{r4['revenge_count']} ({pct4:.0f}%) came within 2h of a loss streak warning."
                                )
                            else:
                                sections.append("😤 <b>Revenge trades</b>\n  None recorded in this window.")

                            # ── Section 5: Daily loss limit / block days — NY Open overlap ──
                            cur.execute("""
                                WITH block_days AS (
                                    SELECT DISTINCT logged_at::date as day
                                    FROM warnings_log
                                    WHERE category IN ('daily_loss_block', 'daily_loss_limit')
                                      AND logged_at >= NOW() - INTERVAL '%s days'
                                ),
                                ny_open_trades AS (
                                    SELECT DISTINCT entry_time::date as day
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL
                                      AND entry_time::time >= %s AND entry_time::time <= %s
                                      AND entry_time >= NOW() - INTERVAL '%s days'
                                )
                                SELECT
                                    (SELECT COUNT(*) FROM block_days) as block_day_count,
                                    (SELECT COUNT(*) FROM block_days WHERE day IN (SELECT day FROM ny_open_trades)) as block_and_ny_count
                            """, (days, COOLDOWN_START, COOLDOWN_END, days))
                            r5 = cur.fetchone()
                            if r5 and r5["block_day_count"] and r5["block_day_count"] > 0:
                                pct5 = (r5["block_and_ny_count"] / r5["block_day_count"] * 100)
                                sections.append(
                                    f"🚫 <b>Loss limit / block days</b>\n"
                                    f"  {r5['block_day_count']} day(s) hit a loss block in the last {days}d.\n"
                                    f"  {r5['block_and_ny_count']}/{r5['block_day_count']} ({pct5:.0f}%) also traded in the "
                                    f"{COOLDOWN_START.strftime('%H:%M')}–{COOLDOWN_END.strftime('%H:%M')} block window."
                                )
                            else:
                                sections.append("🚫 <b>Loss limit / block days</b>\n  No loss-limit days recorded in this window.")

                            # ── Section 6: Stress state performance — does elevated/distracted actually cost you? ──
                            cur.execute("""
                                SELECT s.state,
                                       COUNT(*) FILTER (WHERE t.outcome = 'win')  as wins,
                                       COUNT(*) FILTER (WHERE t.outcome = 'loss') as losses,
                                       COALESCE(SUM(t.pnl) FILTER (WHERE t.outcome IN ('win','loss')), 0) as net_pnl
                                FROM stress_log s
                                JOIN trigger_performance t ON t.id = s.trigger_log_id
                                WHERE s.logged_at >= NOW() - INTERVAL '%s days'
                                  AND s.state IN ('elevated', 'distracted')
                                GROUP BY s.state
                            """, (days,))
                            stress_rows = cur.fetchall()
                            if stress_rows:
                                stress_lines = []
                                for r in stress_rows:
                                    total = (r["wins"] or 0) + (r["losses"] or 0)
                                    if total > 0:
                                        wr = r["wins"] / total * 100
                                        stress_lines.append(
                                            f"  {r['state'].title()}: {total} trades, {wr:.0f}% WR, "
                                            f"{format_pnl(r['net_pnl'])} net"
                                        )
                                if stress_lines:
                                    sections.append("😤 <b>Trading while elevated/distracted</b>\n" + "\n".join(stress_lines))
                                else:
                                    sections.append("😤 <b>Trading while elevated/distracted</b>\n  No closed trades logged under these states.")
                            else:
                                sections.append("😤 <b>Trading while elevated/distracted</b>\n  No trades taken while flagged elevated/distracted. Good discipline.")

                            # ── Section 7: Day-of-week performance ──
                            cur.execute("""
                                SELECT EXTRACT(DOW FROM entry_time) as dow,
                                       COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                                       COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                                       COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
                                FROM trigger_performance
                                WHERE entry_time IS NOT NULL
                                  AND entry_time >= NOW() - INTERVAL '%s days'
                                GROUP BY EXTRACT(DOW FROM entry_time)
                                ORDER BY net_pnl ASC
                            """, (days,))
                            dow_rows = cur.fetchall()
                            if dow_rows:
                                DOW_NAMES = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
                                worst = dow_rows[0]
                                best  = dow_rows[-1]
                                worst_total = (worst["wins"] or 0) + (worst["losses"] or 0)
                                best_total  = (best["wins"] or 0) + (best["losses"] or 0)
                                if worst_total > 0 and best_total > 0:
                                    sections.append(
                                        f"📅 <b>Day-of-week performance</b>\n"
                                        f"  Worst: {DOW_NAMES[int(worst['dow'])]} — {format_pnl(worst['net_pnl'])} "
                                        f"({worst['wins']}W/{worst['losses']}L)\n"
                                        f"  Best: {DOW_NAMES[int(best['dow'])]} — {format_pnl(best['net_pnl'])} "
                                        f"({best['wins']}W/{best['losses']}L)"
                                    )
                            else:
                                sections.append("📅 <b>Day-of-week performance</b>\n  Not enough closed trades yet.")

                            # ── Section 7b: Hour-of-day performance (UK local time) ──
                            cur.execute("""
                                SELECT EXTRACT(HOUR FROM entry_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London') as hour,
                                       COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                                       COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                                       COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
                                FROM trigger_performance
                                WHERE entry_time IS NOT NULL
                                  AND entry_time >= NOW() - INTERVAL '%s days'
                                  AND outcome IN ('win', 'loss')
                                GROUP BY EXTRACT(HOUR FROM entry_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')
                                ORDER BY net_pnl ASC
                            """, (days,))
                            hour_rows = cur.fetchall()
                            if hour_rows:
                                worst_h = hour_rows[0]
                                best_h  = hour_rows[-1]
                                worst_h_total = (worst_h["wins"] or 0) + (worst_h["losses"] or 0)
                                best_h_total  = (best_h["wins"] or 0) + (best_h["losses"] or 0)
                                if worst_h_total > 0 and best_h_total > 0:
                                    sections.append(
                                        f"🕐 <b>Hour-of-day performance (UK time)</b>\n"
                                        f"  Worst: {int(worst_h['hour']):02d}:00 — {format_pnl(worst_h['net_pnl'])} "
                                        f"({worst_h['wins']}W/{worst_h['losses']}L)\n"
                                        f"  Best: {int(best_h['hour']):02d}:00 — {format_pnl(best_h['net_pnl'])} "
                                        f"({best_h['wins']}W/{best_h['losses']}L)"
                                    )
                            else:
                                sections.append("🕐 <b>Hour-of-day performance (UK time)</b>\n  Not enough closed trades yet.")

                            # ── Section 7c: P&L by 2-hour window, rolling 7d and 30d ──
                            for window_days, window_label in [(7, "7d"), (30, "30d")]:
                                cur.execute("""
                                    SELECT
                                        (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2 as chunk_start,
                                        COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                                        COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                                        COALESCE(SUM(pnl) FILTER (WHERE outcome IN ('win','loss')), 0) as net_pnl
                                    FROM trigger_performance
                                    WHERE close_time IS NOT NULL
                                      AND close_time >= NOW() - INTERVAL '%s days'
                                      AND outcome IN ('win', 'loss')
                                    GROUP BY (EXTRACT(HOUR FROM close_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2
                                    ORDER BY net_pnl ASC
                                """, (window_days,))
                                chunk_rows = cur.fetchall()
                                if chunk_rows:
                                    worst_c = chunk_rows[0]
                                    best_c  = chunk_rows[-1]
                                    worst_c_total = (worst_c["wins"] or 0) + (worst_c["losses"] or 0)
                                    best_c_total  = (best_c["wins"] or 0) + (best_c["losses"] or 0)
                                    if worst_c_total > 0 and best_c_total > 0:
                                        worst_start = int(worst_c["chunk_start"])
                                        best_start  = int(best_c["chunk_start"])
                                        sections.append(
                                            f"🕑 <b>P&L by 2-hour window — rolling {window_label}</b>\n"
                                            f"  Worst: {worst_start:02d}:00–{worst_start+2:02d}:00 — {format_pnl(worst_c['net_pnl'])} "
                                            f"({worst_c['wins']}W/{worst_c['losses']}L)\n"
                                            f"  Best: {best_start:02d}:00–{best_start+2:02d}:00 — {format_pnl(best_c['net_pnl'])} "
                                            f"({best_c['wins']}W/{best_c['losses']}L)"
                                        )
                                else:
                                    sections.append(f"🕑 <b>P&L by 2-hour window — rolling {window_label}</b>\n  Not enough closed trades yet.")

                            # ── Section 7d: First-trade-of-day loss, by 2-hour window, rolling 7d and 30d ──
                            for window_days, window_label in [(7, "7d"), (30, "30d")]:
                                cur.execute("""
                                    SELECT
                                        (EXTRACT(HOUR FROM logged_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2 as chunk_start,
                                        COUNT(*) as cnt
                                    FROM warnings_log
                                    WHERE category = 'first_loss_tighten'
                                      AND logged_at >= NOW() - INTERVAL '%s days'
                                    GROUP BY (EXTRACT(HOUR FROM logged_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London')::int / 2) * 2
                                    ORDER BY cnt DESC
                                """, (window_days,))
                                fl_rows = cur.fetchall()
                                if fl_rows:
                                    total_fl = sum(r["cnt"] for r in fl_rows)
                                    top_fl   = fl_rows[0]
                                    top_start = int(top_fl["chunk_start"])
                                    pct = (top_fl["cnt"] / total_fl * 100) if total_fl > 0 else 0
                                    sections.append(
                                        f"🌅 <b>First-trade-of-day loss — rolling {window_label}</b>\n"
                                        f"  Happened {total_fl} time(s).\n"
                                        f"  Most common window: {top_start:02d}:00–{top_start+2:02d}:00 "
                                        f"({top_fl['cnt']}x, {pct:.0f}% of occurrences)"
                                    )
                                else:
                                    sections.append(f"🌅 <b>First-trade-of-day loss — rolling {window_label}</b>\n  Hasn't happened in this window.")

                            # ── Section 8: Trade sequence position — does performance degrade through the day? ──
                            cur.execute("""
                                WITH numbered AS (
                                    SELECT outcome, pnl,
                                           ROW_NUMBER() OVER (PARTITION BY entry_time::date ORDER BY entry_time) as seq
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL AND outcome IN ('win','loss')
                                      AND entry_time >= NOW() - INTERVAL '%s days'
                                )
                                SELECT
                                    CASE WHEN seq = 1 THEN 'Trade #1' WHEN seq <= 3 THEN 'Trades #2-3' ELSE 'Trade #4+' END as bucket,
                                    COUNT(*) FILTER (WHERE outcome = 'win')  as wins,
                                    COUNT(*) FILTER (WHERE outcome = 'loss') as losses,
                                    COALESCE(SUM(pnl), 0) as net_pnl
                                FROM numbered
                                GROUP BY bucket
                                ORDER BY MIN(seq)
                            """, (days,))
                            seq_rows = cur.fetchall()
                            if seq_rows:
                                seq_lines = []
                                for r in seq_rows:
                                    total = (r["wins"] or 0) + (r["losses"] or 0)
                                    if total > 0:
                                        wr = r["wins"] / total * 100
                                        seq_lines.append(f"  {r['bucket']}: {wr:.0f}% WR, {format_pnl(r['net_pnl'])} net ({total} trades)")
                                if seq_lines:
                                    sections.append("🔢 <b>Performance by trade order in the day</b>\n" + "\n".join(seq_lines))
                            else:
                                sections.append("🔢 <b>Performance by trade order in the day</b>\n  Not enough closed trades yet.")

                            # ── Section 9: Retracement protection — did it save you or cost you? ──
                            cur.execute("""
                                SELECT COUNT(*) as fire_count
                                FROM warnings_log
                                WHERE category = 'retrace_protect'
                                  AND logged_at >= NOW() - INTERVAL '%s days'
                            """, (days,))
                            r9 = cur.fetchone()
                            if r9 and r9["fire_count"]:
                                sections.append(
                                    f"🛡️ <b>Retracement protection fires</b>\n"
                                    f"  Fired {r9['fire_count']} time(s) in the last {days}d.\n"
                                    f"  Check /warnings for individual outcomes — compare exit vs what SL/TP would've done."
                                )
                            else:
                                sections.append("🛡️ <b>Retracement protection fires</b>\n  Hasn't fired in this window.")

                            # ── Section 10: Consecutive losses — getting bigger or smaller? ──
                            cur.execute("""
                                WITH ordered AS (
                                    SELECT pnl, outcome, entry_time,
                                           LAG(outcome) OVER (ORDER BY entry_time) as prev_outcome,
                                           LAG(pnl) OVER (ORDER BY entry_time) as prev_pnl
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL AND outcome IN ('win','loss')
                                      AND entry_time >= NOW() - INTERVAL '%s days'
                                )
                                SELECT
                                    COUNT(*) FILTER (WHERE outcome = 'loss' AND prev_outcome = 'loss' AND ABS(pnl) > ABS(prev_pnl)) as bigger,
                                    COUNT(*) FILTER (WHERE outcome = 'loss' AND prev_outcome = 'loss' AND ABS(pnl) < ABS(prev_pnl)) as smaller,
                                    COUNT(*) FILTER (WHERE outcome = 'loss' AND prev_outcome = 'loss' AND ABS(pnl) = ABS(prev_pnl)) as same,
                                    COUNT(*) FILTER (WHERE outcome = 'loss' AND prev_outcome = 'loss') as total_pairs
                                FROM ordered
                            """, (days,))
                            r10 = cur.fetchone()
                            if r10 and r10["total_pairs"] and r10["total_pairs"] > 0:
                                bigger_pct = r10["bigger"] / r10["total_pairs"] * 100
                                sections.append(
                                    f"📉 <b>Back-to-back losses — sizing</b>\n"
                                    f"  {r10['total_pairs']} loss-after-loss pair(s) in the last {days}d.\n"
                                    f"  Bigger: {r10['bigger']} ({bigger_pct:.0f}%)  |  Smaller: {r10['smaller']}  |  Same: {r10['same']}"
                                    + ("\n  ⚠️ Losses tend to grow when they follow another loss — possible sizing-up while on tilt."
                                       if bigger_pct >= 55 else "")
                                )
                            else:
                                sections.append("📉 <b>Back-to-back losses — sizing</b>\n  Not enough consecutive losses yet.")

                            # ── Section 11: Most-skipped signals ──
                            cur.execute("""
                                SELECT message, COUNT(*) as cnt
                                FROM warnings_log
                                WHERE category = 'signal_skipped'
                                  AND logged_at >= NOW() - INTERVAL '%s days'
                                GROUP BY message
                                ORDER BY cnt DESC
                                LIMIT 5
                            """, (days,))
                            skip_rows = cur.fetchall()
                            if skip_rows:
                                skip_lines = [f"  {r['message'].replace('Skipped: ', '')}: {r['cnt']}x" for r in skip_rows]
                                sections.append("🙅 <b>Most-skipped signals</b>\n" + "\n".join(skip_lines))
                            else:
                                sections.append("🙅 <b>Most-skipped signals</b>\n  No skips recorded yet (tracking started recently).")

                            # ── Section 12: Loss streak — how fast does it happen? ──
                            cur.execute("""
                                WITH streak_days AS (
                                    SELECT DISTINCT logged_at::date as day
                                    FROM warnings_log
                                    WHERE category = 'loss_streak'
                                      AND logged_at >= NOW() - INTERVAL '%s days'
                                ),
                                day_first_trade AS (
                                    SELECT entry_time::date as day, MIN(entry_time) as first_entry
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL
                                    GROUP BY entry_time::date
                                ),
                                streak_time AS (
                                    SELECT MIN(w.logged_at) as streak_hit_at, d.first_entry
                                    FROM warnings_log w
                                    JOIN day_first_trade d ON d.day = w.logged_at::date
                                    WHERE w.category = 'loss_streak'
                                      AND w.logged_at >= NOW() - INTERVAL '%s days'
                                    GROUP BY w.logged_at::date, d.first_entry
                                )
                                SELECT AVG(EXTRACT(EPOCH FROM (streak_hit_at - first_entry)) / 60) as avg_mins
                                FROM streak_time
                                WHERE streak_hit_at > first_entry
                            """, (days, days))
                            r12 = cur.fetchone()
                            if r12 and r12["avg_mins"] is not None:
                                mins = r12["avg_mins"]
                                sections.append(
                                    f"⏱ <b>Speed to loss streak</b>\n"
                                    f"  Avg {mins:.0f} min from day's first trade to hitting a loss streak warning."
                                    + ("\n  ⚠️ Streaks are forming fast — signals of rushing early in the session."
                                       if mins < 60 else "")
                                )
                            else:
                                sections.append("⏱ <b>Speed to loss streak</b>\n  Not enough data yet.")

                            # ── Section 13: Daily trade cap — how fast is it hit? ──
                            cur.execute("""
                                WITH cap_days AS (
                                    SELECT logged_at::date as day, MIN(logged_at) as cap_hit_at
                                    FROM warnings_log
                                    WHERE category IN ('daily_loss_block', 'daily_loss_limit')
                                      AND logged_at >= NOW() - INTERVAL '%s days'
                                    GROUP BY logged_at::date
                                ),
                                day_first_trade AS (
                                    SELECT entry_time::date as day, MIN(entry_time) as first_entry
                                    FROM trigger_performance
                                    WHERE entry_time IS NOT NULL
                                    GROUP BY entry_time::date
                                )
                                SELECT AVG(EXTRACT(EPOCH FROM (c.cap_hit_at - d.first_entry)) / 60) as avg_mins,
                                       COUNT(*) as cap_hit_count
                                FROM cap_days c
                                JOIN day_first_trade d ON d.day = c.day
                                WHERE c.cap_hit_at > d.first_entry
                            """, (days,))
                            r13 = cur.fetchone()
                            if r13 and r13["avg_mins"] is not None:
                                mins13 = r13["avg_mins"]
                                hrs13  = mins13 / 60
                                sections.append(
                                    f"🚦 <b>Speed to daily loss cap</b>\n"
                                    f"  Hit {r13['cap_hit_count']} time(s) in the last {days}d.\n"
                                    f"  Avg {hrs13:.1f}h from day's first trade to hitting the cap."
                                    + ("\n  ⚠️ Cap is being hit quickly — possible overtrading or sizing issues."
                                       if hrs13 < 2 else "")
                                )
                            else:
                                sections.append("🚦 <b>Speed to daily loss cap</b>\n  Hasn't been hit in this window.")

                            cur.close()
                            conn.close()

                            header = f"🔬 <b>Behavioral analytics — last {days}d</b>\n"
                            send_telegram(header + "\n\n" + "\n\n".join(sections))
                        except Exception as e:
                            send_telegram(f"❌ Analytics report error: {e}")
                elif cmd == "invalidations":
                    stats = db_get_invalidation_stats()
                    RETIRED_SIGNALS = {
                        "long_setup_10m", "short_setup_10m", "short_setup_5m", "fork",
                        "3_bulls", "weakness", "3_bar_buy",
                        "composite_higher_low", "composite_lower_high",
                        "composite_crossed_above_15", "composite_crossed_below_15",
                        "composite_crossed_above_82", "composite_crossed_below_82",
                        "composite_crossed_above_18", "composite_crossed_below_18",
                        "composite_crossed_above_85", "composite_crossed_below_85",
                        "htf_crossed_above_12", "htf_crossed_below_12",
                        "htf_crossed_above_88", "htf_crossed_below_88",
                        "30m_bullish_structure_break", "30m_bearish_structure_break",
                        "30m_crosses_down_2h_composite", "30m_crosses_up_2h_composite",
                        "15m_crosses_down_2h_composite", "15m_crosses_up_2h_composite",
                        # 30m/4H cross signals — valid trade signals but cannot be invalidated
                        "30m_crosses_down_4h_composite", "30m_crosses_up_4h_composite",
                        # Old-style MTF combo naming — replaced by long/short naming
                        "green_ltf_red_htf_bullish", "green_ltf_green_htf_bullish",
                        "green_ltf_cyan_htf_bullish", "green_ltf_yellow_htf_bullish",
                        "orange_ltf_red_htf_bearish", "orange_ltf_orange_htf_bearish",
                        "orange_ltf_green_htf_bearish",
                    }
                    stats = {k: v for k, v in stats.items() if k not in RETIRED_SIGNALS}
                    if not stats:
                        send_telegram("📊 No invalidation data yet.")
                    else:
                        TRIGGER_LABELS = {
                            "long": "📈 Long", "3_bulls": "🐂 3 Bulls", "strength": "💪 Strength",
                            "3_bears": "🐻 3 Bears", "weakness": "📉 Weakness",
                            "bearish": "🔴 Bearish", "short": "📉 Short", "30m_composite_crosses_up_30": "📈 30m Composite Crosses UP 30", "30m_composite_crosses_down_70": "📉 30m Composite Crosses DOWN 70", "30m_composite_crosses_down_85": "📉 30m Composite Crosses DOWN 85", "30m_composite_crosses_up_15": "📈 30m Composite Crosses UP 15", "wpr_crosses_down_18": "📉 WPR Crosses Down -18", "wpr_crosses_up_82": "📈 WPR Crosses Up -82", "buy": "📈 Buy",
                            "green_structure_break": "🟢 Green Structure Break",
                            "orange_structure_break": "🟠 Orange Structure Break",
                        }
                        # Sort worst-first — highest invalidation rate at top.
                        # Triggers with zero fires (rate_pct is None) sort to the bottom.
                        # Mismatched data (invalidations > fires) is capped so it doesn't
                        # artificially dominate the top of the list with a broken percentage.
                        def _sort_val(k):
                            s = stats[k]
                            if s["rate_pct"] is None:
                                return -1
                            return min(s["rate_pct"], 100)
                        sorted_keys = sorted(stats.keys(), key=_sort_val, reverse=True)
                        lines = ["⚠️ <b>Invalidation rates — worst first</b>\n"]
                        for key in sorted_keys:
                            s     = stats[key]
                            label = TRIGGER_LABELS.get(key, key.replace("_", " ").title())
                            if s["fires"] > 0 and s["invalidations"] > s["fires"]:
                                lines.append(
                                    f"<b>{label}</b>\n"
                                    f"  ⚠️ Data mismatch: {s['invalidations']} invalidations vs only {s['fires']} fires.\n"
                                    f"  This shouldn't be possible — check the TradingView alert for this "
                                    f"signal (delivery failures or misconfiguration likely)."
                                )
                            elif s["rate_pct"] is not None:
                                lines.append(
                                    f"<b>{label}</b>\n"
                                    f"  Invalidated {s['invalidations']}/{s['fires']} times — {s['rate_pct']:.0f}% rate"
                                )
                            else:
                                lines.append(
                                    f"<b>{label}</b>\n"
                                    f"  Invalidated {s['invalidations']}x — no fire count on record"
                                )
                        send_telegram("\n\n".join(lines))
                elif cmd == "quality":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute(r"""
                                SELECT
                                    trigger,
                                    COUNT(*) FILTER (WHERE outcome IN ('win','loss')) as total,
                                    AVG(mae) FILTER (WHERE outcome IN ('win','loss'))  as avg_mae,
                                    AVG(mfe) FILTER (WHERE outcome IN ('win','loss'))  as avg_mfe,
                                    AVG(mfe) FILTER (WHERE outcome = 'win')            as avg_mfe_win,
                                    AVG(mfe) FILTER (WHERE outcome = 'loss')           as avg_mfe_loss,
                                    AVG(time_to_tp1_mins) FILTER (WHERE time_to_tp1_mins IS NOT NULL) as avg_tp1_mins,
                                    AVG(time_to_tp2_mins) FILTER (WHERE time_to_tp2_mins IS NOT NULL) as avg_tp2_mins,
                                    AVG(time_to_sl_mins)  FILTER (WHERE time_to_sl_mins  IS NOT NULL) as avg_sl_mins,
                                    COUNT(*) FILTER (WHERE mfe > 0 AND outcome = 'loss') as near_miss
                                FROM trigger_performance
                                WHERE trigger IS NOT NULL
                                  AND trigger NOT IN ('long_setup_10m', 'short_setup_10m', 'fork', '3_bulls', 'weakness', '3_bar_buy')
                                  AND trigger NOT LIKE 'exp\_%%' ESCAPE '\'
                                GROUP BY trigger
                                HAVING COUNT(*) FILTER (WHERE outcome IN ('win','loss')) >= 2
                                ORDER BY avg_mfe DESC NULLS LAST
                            """)
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()
                            if not rows:
                                send_telegram("📊 Not enough closed trades to show quality stats yet (need 2+ per signal).")
                            else:
                                TRIGGER_LABELS = {
                                    "long": "📈 Long", "3_bulls": "🐂 3 Bulls",
                                    "strength": "💪 Strength", "3_bar_buy": "📈 3 Bar Buy",
                                    "3_bears": "🐻 3 Bears", "weakness": "📉 Weakness",
                                    "bearish": "🔴 Bearish", "short": "📉 Short", "30m_composite_crosses_up_30": "📈 30m Composite Crosses UP 30", "30m_composite_crosses_down_70": "📉 30m Composite Crosses DOWN 70", "30m_composite_crosses_down_85": "📉 30m Composite Crosses DOWN 85", "30m_composite_crosses_up_15": "📈 30m Composite Crosses UP 15", "wpr_crosses_down_18": "📉 WPR Crosses Down -18", "wpr_crosses_up_82": "📈 WPR Crosses Up -82", "buy": "📈 Buy",
                                    "green_structure_break": "🟢 Green Structure Break",
                                    "orange_structure_break": "🟠 Orange Structure Break",
                                }
                                lines = ["🔬 <b>Entry quality report</b>\n<i>Bot-executed trades only — doesn't include manual Binance app trades</i>\n"]
                                for r in rows:
                                    label    = TRIGGER_LABELS.get(r["trigger"], r["trigger"].replace("_", " ").title())
                                    avg_mae  = f"{abs(r['avg_mae']):.3f}%" if r["avg_mae"] is not None else "—"
                                    avg_mfe  = f"{r['avg_mfe']:.3f}%"     if r["avg_mfe"] is not None else "—"
                                    tp1_mins = f"{r['avg_tp1_mins']:.0f}m" if r["avg_tp1_mins"] is not None else "—"
                                    tp2_mins = f"{r['avg_tp2_mins']:.0f}m" if r["avg_tp2_mins"] is not None else "—"
                                    sl_mins  = f"{r['avg_sl_mins']:.0f}m"  if r["avg_sl_mins"]  is not None else "—"
                                    near     = r["near_miss"] or 0
                                    mfe_win  = f"{r['avg_mfe_win']:.3f}%"  if r["avg_mfe_win"]  is not None else "—"
                                    mfe_loss = f"{r['avg_mfe_loss']:.3f}%" if r["avg_mfe_loss"] is not None else "—"
                                    lines.append(
                                        f"<b>{label}</b>  ({r['total']} trades)\n"
                                        f"  Avg MFE: {avg_mfe}  |  Avg MAE: {avg_mae}\n"
                                        f"  MFE wins: {mfe_win}  |  MFE losses: {mfe_loss}\n"
                                        f"  → TP1: {tp1_mins}  |  → TP2: {tp2_mins}  |  → SL: {sl_mins}\n"
                                        f"  Near-misses: {near}"
                                    )
                                send_telegram("\n\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Quality report error: {e}")
                elif cmd == "compare":
                    compare_summary()
                elif cmd == "bullish":
                    if daily_bias is not None:
                        send_telegram(
                            f"🔒 <b>Bias already set: {daily_bias.upper()}</b>\n\n"
                            f"You cannot change your bias once it's set for the day.\n"
                            f"Stay disciplined — reassess tomorrow morning."
                        )
                    else:
                        daily_bias = "bullish"
                        send_telegram("✅ <b>Bias set: BULLISH</b>")
                elif cmd == "bearish":
                    if daily_bias is not None:
                        send_telegram(
                            f"🔒 <b>Bias already set: {daily_bias.upper()}</b>\n\n"
                            f"You cannot change your bias once it's set for the day.\n"
                            f"Stay disciplined — reassess tomorrow morning."
                        )
                    else:
                        daily_bias = "bearish"
                        send_telegram("✅ <b>Bias set: BEARISH</b>")
                elif cmd == "cancel_entry":
                    if maker_entry_cancel_requested:
                        send_telegram("⏳ Cancel already requested — waiting for the order to be removed.")
                    else:
                        maker_entry_cancel_requested = True
                        send_telegram("🚫 <b>Cancel requested</b> — aborting maker entry on next poll (up to 2 seconds).")
                elif cmd == "cancel2":
                    cancel_entry2()
                elif cmd == "fill2":
                    fill_entry2_at_market()
                elif cmd == "status":
                    print(f"[Telegram CMD] /status handled by MAIN bot for chat {chat_id}")
                    status_message()
                elif cmd == "htf":
                    try:
                        conn = get_db()
                        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                        cur.execute("""
                            SELECT logged_at, new_bias, triggered_by, action
                            FROM htf_bias_changes
                            ORDER BY logged_at DESC LIMIT 10
                        """)
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()
                        
                        if not rows:
                            send_telegram("📊 <b>HTF Bias Changes</b>\n\nNo changes logged yet.")
                        else:
                            lines = ["📊 <b>HTF Bias Changes (last 10)</b>\n"]
                            for r in rows:
                                emoji = "🔼" if r["new_bias"] == "bullish" else "🔽"
                                lines.append(
                                    f"{emoji} {r['new_bias'].upper()} @ {r['logged_at'].strftime('%d %b %H:%M')}\n"
                                    f"   Triggered by: {r['triggered_by']}\n"
                                    f"   Action: {r['action']}"
                                )
                            send_telegram("\n".join(lines))
                    except Exception as e:
                        send_telegram(f"❌ HTF history error: {e}")
                        print(f"[HTF] Command error: {e}")

                elif cmd == "stress":
                    if not DATABASE_URL:
                        send_telegram("📊 No database connected.")
                    else:
                        try:
                            conn = get_db()
                            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur.execute("""
                                SELECT
                                    s.state,
                                    COUNT(*) as total,
                                    SUM(CASE WHEN s.traded THEN 1 ELSE 0 END) as traded,
                                    SUM(CASE WHEN NOT s.traded THEN 1 ELSE 0 END) as blocked,
                                    SUM(CASE WHEN t.outcome = 'win' THEN 1 ELSE 0 END) as wins,
                                    SUM(CASE WHEN t.outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                                    SUM(CASE WHEN t.outcome IS NOT NULL THEN t.pnl ELSE 0 END) as net_pnl
                                FROM stress_log s
                                LEFT JOIN trigger_performance t ON t.id = s.trigger_log_id
                                GROUP BY s.state
                                ORDER BY s.state
                            """)
                            rows = cur.fetchall()
                            cur.close()
                            conn.close()
                            if not rows:
                                send_telegram("📊 No stress data yet.")
                            else:
                                lines = ["🧠 <b>Stress state log</b>\n"]
                                for r in rows:
                                    emoji = "😌" if r["state"] == "relaxed" else "🎯" if r["state"] == "focused" else "🌀" if r["state"] == "distracted" else "😤"
                                    closed = (r["wins"] or 0) + (r["losses"] or 0)
                                    wr_line = ""
                                    if closed > 0:
                                        wr = (r["wins"] or 0) / closed * 100
                                        wr_line = f"\n  Closed: {closed} ({wr:.0f}% WR)  |  Net P&L: {format_pnl(r['net_pnl'] or 0)}"
                                    lines.append(
                                        f"{emoji} <b>{r['state'].title()}</b>\n"
                                        f"  Total: {r['total']}  |  Traded: {r['traded']}  |  Blocked: {r['blocked']}"
                                        f"{wr_line}"
                                    )
                                send_telegram("\n\n".join(lines))
                        except Exception as e:
                            send_telegram(f"❌ Stress log error: {e}")

                elif cmd == "signals":
                    uk_tz = ZoneInfo("Europe/London")
                    def fmt_ts(ts):
                        if ts is None:
                            return "never"
                        return datetime.fromtimestamp(ts, tz=uk_tz).strftime("%H:%M:%S")

                    lines = ["📡 <b>Active signals — all configured prompts</b>\n"]

                    signal_map = [
                        ("long",                       "long",  "📈 Long",                          None),
                        ("strength",                   "long",  "💪 Strength",                      None),
                        ("green_structure_break",      "long",  "🟢 Green Structure Break Through", None),
                        ("30m_crosses_up_4h_composite","long",  "🔼 30m Crosses UP 4H Composite",   None),
                        ("30m_composite_crosses_up_30", "long", "📈 30m Composite Crosses UP 30",   None),
                        ("3_bears",                    "short", "🐻 3 Bears",                       None),
                        ("bearish",                    "short", "🔴 Bearish",                       None),
                        ("short",                 "short", "📉 Short",                    None),
                        ("orange_structure_break",     "short", "🟠 Orange Structure Break Down",   None),
                        ("30m_crosses_down_4h_composite","short","🔻 30m Crosses DOWN 4H Composite", None),
                        ("30m_composite_crosses_down_70", "short", "📉 30m Composite Crosses DOWN 70", None),
                        ("30m_composite_crosses_down_85", "short", "📉 30m Composite Crosses DOWN 85", None),
                        ("30m_composite_crosses_up_15", "long", "📈 30m Composite Crosses UP 15", None),
                        ("wpr_crosses_down_18", "short", "📉 WPR Crosses Down -18", None),
                        ("wpr_crosses_up_82", "long", "📈 WPR Crosses Up -82", None),
                        ("buy", "long", "📈 Buy", None),
                        ("green_ltf_red_htf_long",     "long",  "🎯 Red HTF + Green LTF",           None),
                        ("grey_ltf_green_htf_long",    "long",  "🎯 Green HTF + Grey LTF",          None),
                        ("grey_ltf_cyan_htf_long",     "long",  "🎯 Cyan HTF + Grey LTF — 40%",     None),
                        ("green_ltf_green_htf_long",   "long",  "🎯 Green HTF + Green LTF — 80%",   None),
                        ("green_ltf_cyan_htf_long",    "long",  "🎯 Cyan HTF + Green LTF — 70%",    None),
                        ("orange_ltf_red_htf_short",   "short", "🎯 Red HTF + Orange LTF — 70%",    None),
                        ("grey_ltf_orange_htf_short",  "short", "🎯 Orange HTF + Grey LTF",         None),
                        ("grey_ltf_red_htf_short",     "short", "🎯 Red HTF + Grey LTF — 40%",      None),
                        ("orange_ltf_orange_htf_short","short", "🎯 Orange HTF + Orange LTF — 80%", None),
                        ("orange_ltf_cyan_htf_short",  "short", "🎯 Cyan HTF + Orange LTF",         None),
                    ]

                    long_lines  = []
                    short_lines = []
                    for sig_key, direction, label, last_fired in signal_map:
                        entry = f"{label}"
                        if direction == "long":
                            long_lines.append(f"  • {entry}")
                        else:
                            short_lines.append(f"  • {entry}")

                    lines.append("🟢 <b>Long signals</b>")
                    lines.extend(long_lines)
                    lines.append("")
                    lines.append("🔴 <b>Short signals</b>")
                    lines.extend(short_lines)
                    lines.append("")
                    lines.append(f"Last long trigger:   {fmt_ts(last_long_trigger_time)}")
                    lines.append(f"Last short trigger:  {fmt_ts(last_short_trigger_time)}")
                    lines.append(f"Last signal fired:   {fmt_ts(last_trigger_time)} ({last_trigger or 'none'})")

                    # Most active triggers today
                    long_keys  = {"long", "strength"}
                    short_keys = {"3_bears", "bearish", "short"}

                    long_counts  = {k: v for k, v in trigger_counts_today.items() if k in long_keys}
                    short_counts = {k: v for k, v in trigger_counts_today.items() if k in short_keys}

                    if long_counts:
                        top_long     = max(long_counts, key=long_counts.get)
                        top_long_lbl = next((s[2] for s in signal_map if s[0] == top_long), top_long.replace("_", " ").title())
                        lines.append(f"\n🔥 Most active long:   {top_long_lbl} ({long_counts[top_long]}x today)")
                    else:
                        lines.append(f"\n🔥 Most active long:   none yet today")

                    if short_counts:
                        top_short     = max(short_counts, key=short_counts.get)
                        top_short_lbl = next((s[2] for s in signal_map if s[0] == top_short), top_short.replace("_", " ").title())
                        lines.append(f"🔥 Most active short:  {top_short_lbl} ({short_counts[top_short]}x today)")
                    else:
                        lines.append(f"🔥 Most active short:  none yet today")

                    # Most active real trigger (excludes composite, setup and structure signals)
                    real_triggers = {"long", "strength", "3_bears", "bearish", "short"}
                    real_counts   = {k: v for k, v in trigger_counts_today.items() if k in real_triggers}
                    if real_counts:
                        top_overall     = max(real_counts, key=real_counts.get)
                        top_overall_lbl = next((s[2] for s in signal_map if s[0] == top_overall), top_overall.replace("_", " ").title())
                        lines.append(f"⚡ Most active trigger: {top_overall_lbl} ({real_counts[top_overall]}x today)")

                    # Avg time between triggers today
                    def avg_gap_str(times):
                        if len(times) < 2:
                            return "not enough data"
                        gaps = [(times[i] - times[i-1]) / 60 for i in range(1, len(times))]
                        avg  = sum(gaps) / len(gaps)
                        return f"{int(avg)}m" if avg < 60 else f"{int(avg//60)}h {int(avg%60)}m"

                    lines.append(f"⏱ Avg gap (longs):    {avg_gap_str(sorted(long_trigger_times_today))}")
                    lines.append(f"⏱ Avg gap (shorts):   {avg_gap_str(sorted(short_trigger_times_today))}")

                    send_telegram("\n".join(lines))

                elif cmd == "help":
                    send_telegram(
                        "ℹ️ <b>All commands</b>\n\n"
                        "<b>Enter a trade:</b>\n"
                        "Trades open via signal prompts only — wait for a signal and click Take Trade.\n\n"
                        "<b>Manage position:</b>\n"
                        "/sl 0.5 — move stop loss to 0.5% from entry\n"
                        "/tp 0.4 — move TP1 to 0.4% from entry\n"
                        "/tp2 0.6 — move TP2 to 0.6% from entry\n"
                        "/breakeven — move SL to entry price\n"
                        "/long — manually open a long\n"
                        "/short — manually open a short\n"
                        "/close — close full position\n"
                        "/close 50 — close 50% of position\n"
                        "/cut1 — cut position size by 25%\n"
                        "/cut2 — cut position size by 50%\n"
                        "/fill2 — cancel Entry2 limit and fill at market instead\n"
                        "/cancel_entry — cancel pending maker entry before it fills\n"
                        "/cancel2 — cancel Entry2 limit order\n"
                        "/bullish — set HTF bias to bullish\n"
                        "/bearish — set HTF bias to bearish\n"
                        "/status — entry, mark price, unrealised P&amp;L\n\n"
                        "<b>Info:</b>\n"
                        "/stress — stress state log\n"
                        "/htf — HTF bias change history\n"
                        "/signals — all configured signal prompts and status\n"
                        "/stats — today's full stats\n"
                        "/patterns — best performing setups\n"
                        "/live — are prompts on right now, and why not if not\n"
                        "/vwinrate — per-trigger win rate at fixed 0.35% TP / 0.25% SL\n"
                        "/compound — weekly (2.0114x) and 4-week (16.3679x) compounding progress\n"
                        "/winrate — win rate today/7d/30d vs 60% target\n"
                        "/long — grades the setup (A/B+/B-/BLOCKED) and asks to confirm before entering\n"
                        "/short — same, for shorts\n"
                        "/timewindow — % of today's window spent in a trade\n"
                        "/wsstatus — websocket connection health, kline history, mark price freshness\n"
                        "/discipline — week/month discipline metric trend (fees, loss streaks, revenge trades, block days)\n"
                        "/wpr — Williams %R guard status (15m, length 30)\n"
                        "/unmatched — TradingView alerts that fired but matched no handler\n"
                        "/warnings — recent discipline warnings (default 24h)\n"
                        "/htf — current HTF composite value and color\n"
                        "/ema — EMA premium/discount guard status\n"
                        "/brute — force-close history (default 30d)\n"
                        "/analytics — behavioral patterns (default 30d)\n"
                        "📈 Weekly digest auto-sends every Sunday at 20:00\n"
                        "/invalidations — invalidation rates per trigger\n"
                        "/quality — entry quality: MFE, MAE, time to TP1\n"
                        "/compare — manual vs indicator win rate today\n"
                        "/help — this message"
                    )
                else:
                    send_telegram("❓ Unknown command. Send /help for the full list.")

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            print(f"[Telegram CMD ERROR] {e}")
            time.sleep(5)


# ─────────────────────────────────────────────
# WEBHOOK SERVER — receives TradingView signals
# ─────────────────────────────────────────────

webhook_app = Flask(__name__)

@webhook_app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Secret"
    return response

@webhook_app.route("/webhook", methods=["POST", "OPTIONS"])
def receive_webhook():
    if flask_request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200
    if WEBHOOK_SECRET:
        token = flask_request.headers.get("X-Secret", "")
        if token != WEBHOOK_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    data        = flask_request.get_json(silent=True) or {}
    threading.Thread(target=process_webhook_signal, args=(data,), daemon=True).start()
    return jsonify({"status": "received"}), 200


def process_webhook_signal(data):
    global last_trigger, last_trigger_time, last_trigger_direction
    global last_long_trigger_time, last_short_trigger_time
    global current_composite, composite_last_updated, current_htf_composite, htf_color_last
    signal_name = data.get("signal", "Unknown Signal")
    now_ms      = int(time.time() * 1000)

    if signal_name == "zone_update":
        global current_zone, zone_last_updated
        current_zone = data.get("zone", "unknown").strip().lower()
        zone_last_updated = time.time()
        print(f"[Zone] Updated to: {current_zone}")
        return

    if signal_name == "composite_update":
        try:
            current_composite = float(data.get("composite", 0))
            new_htf_composite = float(data.get("htf_composite", 0))
            composite_last_updated = time.time()
            print(f"[Composite] Updated to: {current_composite:.1f}")

            # ── HTF color-change alert ──────────────────────────────
            new_color = composite_color(new_htf_composite)
            if htf_color_last is not None and new_color != htf_color_last:
                send_telegram(
                    f"🎨 <b>HTF composite changed color — {htf_color_last} → {new_color}</b>\n\n"
                    f"HTF composite: {new_htf_composite:.1f}"
                )
                db_log_warning("htf_color_change", f"HTF color changed {htf_color_last} → {new_color} ({new_htf_composite:.1f})")
            htf_color_last = new_color
            current_htf_composite = new_htf_composite

        except (ValueError, TypeError):
            pass
        return

    if signal_name == "2h_bullish_structure_break":
        global htf_bias, htf_bias_updated_at
        old_bias = htf_bias
        htf_bias = "bullish"
        htf_bias_updated_at = time.time()
        print(f"[HTF Bias] Updated to: bullish (2H structure break)")
        if old_bias != "bullish":
            send_telegram(
                f"🎯 <b>HTF bias — 2H Bullish Structure Break</b>\n\n"
                f"HTF bias set to BULLISH. Only long grades (A/B+/B-) can be granted now — "
                f"all shorts blocked until this flips."
            )
        return

    if signal_name == "2h_bearish_structure_break":
        old_bias = htf_bias
        htf_bias = "bearish"
        htf_bias_updated_at = time.time()
        print(f"[HTF Bias] Updated to: bearish (2H structure break)")
        if old_bias != "bearish":
            send_telegram(
                f"🎯 <b>HTF bias — 2H Bearish Structure Break</b>\n\n"
                f"HTF bias set to BEARISH. Only short grades (A/B+/B-) can be granted now — "
                f"all longs blocked until this flips."
            )
        return

    # ── Composite threshold crossing — update state, notify only, no trade prompt ──
    COMPOSITE_THRESHOLD_SIGNALS = {
        "composite_crossed_above_18": "🟢 LTF composite crossed ABOVE 18 — longs AND shorts now valid.\nPrice recovering from extreme discount. Both directions back on.",
        "composite_crossed_below_18": "🔴 LTF composite crossed BELOW 18 — longs AND shorts blocked.\nExtreme discount. Bad place to long (chasing a falling knife) or short (don't sell the bottom).",
        "composite_crossed_above_82": "🔴 LTF composite crossed ABOVE 82 — longs AND shorts blocked.\nPrice hyperextended. Too late to chase a long (caught at the top) or a short (already overextended).",
        "composite_crossed_below_82": "🟢 LTF composite crossed BELOW 82 — longs AND shorts unblocked.\nPulled back from extreme premium. Both directions valid again.",
        "composite_crossed_above_50": "📊 LTF composite crossed ABOVE 50 — midline flip to premium side.",
        "composite_crossed_below_50": "📊 LTF composite crossed BELOW 50 — midline flip to discount side. B- Setup longs now eligible if WPR is between -35 and -18.",
        "htf_crossed_above_12":       "🟢 HTF composite crossed ABOVE 12 — longs valid (HTF).\nHigher timeframe recovering. HTF no longer blocking longs.",
        "htf_crossed_below_12":       "🔴 HTF composite crossed BELOW 12 — longs blocked (HTF).\nHTF in freefall. No longs until the higher timeframe stabilises.",
        "htf_crossed_above_88":       "🔴 HTF composite crossed ABOVE 88 — longs blocked (HTF).\nHTF severely overbought. Not a place to be buying.",
        "htf_crossed_below_88":       "🟢 HTF composite crossed BELOW 88 — longs unblocked (HTF).\nHTF pulled back from extreme. Long bias on HTF restored.",
    }
    if signal_name in COMPOSITE_THRESHOLD_SIGNALS:
        comp_val = data.get("composite")
        val_str  = f" ({float(comp_val):.1f})" if comp_val is not None else ""
        try:
            current_composite = float(comp_val)
            composite_last_updated = time.time()
        except (ValueError, TypeError, AttributeError):
            pass
        send_telegram(f"📊 {COMPOSITE_THRESHOLD_SIGNALS[signal_name]}{val_str}")

        # ── Brute-force protection — auto-close if composite invalidates the open position ──
        # Above 82/85 = extreme premium (bad for shorts). Below 18 = extreme discount (bad for longs).
        FORCE_CLOSE_SHORT_SIGNALS = {"composite_crossed_above_82", "composite_crossed_above_85"}
        FORCE_CLOSE_LONG_SIGNALS  = {"composite_crossed_below_18"}
        open_direction = current_trade_entry.get("direction")
        should_force_close = (
            (open_direction == "short" and signal_name in FORCE_CLOSE_SHORT_SIGNALS) or
            (open_direction == "long"  and signal_name in FORCE_CLOSE_LONG_SIGNALS)
        )
        if should_force_close:
            reason = (
                f"Composite crossed against open {open_direction.upper()} — "
                f"{COMPOSITE_THRESHOLD_SIGNALS[signal_name].splitlines()[0]}{val_str}"
            )
            db_log_warning("brute_force_close", reason)
            close_position_now(reason=reason)
        return

    print(f"[Webhook] Signal received: {signal_name}")

    sig_key = signal_name.lower().replace(" ", "_").replace("-", "_")

    INDICATOR_SIGNALS = {
        "long":                        ("long",  "📈 Long"),
        "strength":                    ("long",  "💪 Strength"),
        "4h_bullish_structure_break":  None,
        "3_bears":                     ("short", "🐻 3 Bears"),
        "bearish":                     ("short", "🔴 Bearish"),
        "short":                  ("short", "📉 Short"),
        "4h_bearish_structure_break":  None,
        "green_structure_break":             ("long",  "🟢 Green Structure Break Through"),
        "red_HTF_green_LTF_bullish_crossing_up":         ("long",  "🎯 MTF: Red HTF + Green LTF ↑ — 80% ✅"),
        "green_HTF_green_LTF_bullish_crossing_up":       ("long",  "🎯 MTF: Green HTF + Green LTF ↑ — 80% MAX"),
        "cyan_HTF_green_LTF_bullish_crossing_up":        ("long",  "🎯 MTF: Cyan HTF + Green LTF ↑ — 40%"),
        "red_HTF_yellow_LTF_bullish_crossing_up":        ("long",  "🎯 MTF: Red HTF + Yellow LTF ↑ — 70%"),
        "yellow_HTF_green_LTF_bullish_crossing_up":      ("long",  "🎯 MTF: Yellow HTF + Green LTF ↑ — 50%"),
        "orange_structure_break":            ("short", "🟠 Orange Structure Break Down"),
        # MTF setup signals (7 total from BTC Composite Multi-TF)
        "red_HTF_orange_LTF_bearish_crossing_up":        ("short", "🎯 MTF: Red HTF + Orange LTF ↑ — 80% ✅"),
        "orange_HTF_yellow_LTF_bearish_crossing_up":     ("short", "🎯 MTF: Orange HTF + Yellow LTF ↑ — 60%"),
        "orange_HTF_orange_LTF_bearish_crossing_up":     ("short", "🎯 MTF: Orange HTF + Orange LTF ↑ — 80%"),
        "yellow_HTF_yellow_LTF_bearish_crossing_down":   ("short", "🎯 MTF: Yellow HTF + Yellow LTF ↓ — 50%"),
        "green_HTF_yellow_LTF_bearish_crossing_down":    ("short", "🎯 MTF: Green HTF + Yellow LTF ↓ — 60%"),
        "green_ltf_red_htf_long":     ("long",  "🎯 Red HTF + Green LTF"),
        "grey_ltf_green_htf_long":    ("long",  "🎯 Green HTF + Grey LTF"),
        "grey_ltf_cyan_htf_long":     ("long",  "🎯 Cyan HTF + Grey LTF — 40%"),
        "green_ltf_green_htf_long":   ("long",  "🎯 Green HTF + Green LTF — 80%"),
        "green_ltf_cyan_htf_long":    ("long",  "🎯 Cyan HTF + Green LTF — 70%"),
        "orange_ltf_red_htf_short":   ("short", "🎯 Red HTF + Orange LTF — 70%"),
        "grey_ltf_orange_htf_short":  ("short", "🎯 Orange HTF + Grey LTF"),
        "grey_ltf_red_htf_short":     ("short", "🎯 Red HTF + Grey LTF — 40%"),
        "orange_ltf_orange_htf_short":("short", "🎯 Orange HTF + Orange LTF — 80%"),
        "orange_ltf_cyan_htf_short":  ("short", "🎯 Cyan HTF + Orange LTF"),
        "composite_crossed_above_15":        None,
        "composite_crossed_below_15":        None,
        "composite_crossed_above_82":        None,
        "composite_crossed_below_82":        None,
        "composite_crossed_above_18":        None,
        "composite_crossed_below_18":        None,
        "composite_crossed_above_85":        None,
        "composite_crossed_below_85":        None,
        "htf_crossed_above_12":              ("long",  "🟢 HTF Crossed ABOVE 12 — Long Valid"),
        "htf_crossed_below_12":              ("short", "🔴 HTF Crossed BELOW 12 — Long Invalid"),
        "htf_crossed_above_88":              ("short", "🔴 HTF Crossed ABOVE 88 — Long Blocked"),
        "htf_crossed_below_88":              ("long",  "🟢 HTF Crossed BELOW 88 — Long Unblocked"),
        "30m_crosses_down_4h_composite":     ("short", "🔻 30m Crosses DOWN 4H Composite — Short Signal"),
        "30m_crosses_up_4h_composite":       ("long",  "🔼 30m Crosses UP 4H Composite — Long Signal"),
        "30m_composite_crosses_up_30":       ("long",  "📈 30m Composite Crosses UP 30 — Long Signal"),
        "30m_composite_crosses_down_70":     ("short", "📉 30m Composite Crosses DOWN 70 — Short Signal"),
        "30m_composite_crosses_down_85":     ("short", "📉 30m Composite Crosses DOWN 85 — Short Signal"),
        "30m_composite_crosses_up_15":       ("long",  "📈 30m Composite Crosses UP 15 — Long Signal"),
        "wpr_crosses_down_18":               ("short", "📉 WPR Crosses Down -18 — Short Signal"),
        "wpr_crosses_up_82":                 ("long",  "📈 WPR Crosses Up -82 — Long/Buy Signal"),
        "buy":                               ("long",  "📈 Buy"),
    }

    INVALIDATION_SIGNALS = {
        "long_invalidated":       ("long",  "📈 Long"),
        "strength_invalidated":   ("long",  "💪 Strength"),
        "3_bears_invalidated":    ("short", "🐻 3 Bears"),
        "bearish_invalidated":    ("short", "🔴 Bearish"),
        "short_invalidated":      ("short", "📉 Short"),
        "green_structure_break_invalidated":  ("long",  "🟢 Green Structure Break"),
        "orange_structure_break_invalidated": ("short", "🟠 Orange Structure Break"),
        "buy_invalidated":        ("long",  "📈 Buy"),
    }

    if sig_key == "composite_red_orange":
        send_telegram(
            "🟠 <b>Composite: Red → Orange</b>\n\n"
            "Composite has crossed from Red to Orange.\n"
            "Potential short setup building."
        )
        return

    if sig_key == "4h_bearish_structure_break":
        send_telegram(
            "🔴 <b>4H Bearish Structure Break</b>\n\n"
            "A bearish structure break has been confirmed on the 4H.\n"
            "Bias shift — look for short setups on lower timeframes."
        )
        return

    if sig_key == "ltf_bullish_flip":
        send_telegram(
            "🟢 <b>LTF Structure Flipped Bullish</b>\n\n"
            "Lower timeframe structure has shifted bullish.\n"
            "Look for long entries on pullbacks."
        )
        return

    if sig_key == "composite_cyan_green":
        send_telegram(
            "🟢 <b>Composite: Cyan → Green</b>\n\n"
            "Composite has crossed from Cyan to Green.\n"
            "Bullish momentum building — watch for long setups."
        )
        return

    if sig_key in INVALIDATION_SIGNALS:
        inv_direction, label = INVALIDATION_SIGNALS[sig_key]
        base_trigger = sig_key.replace("_invalidated", "")
        inv_tf = data.get("tf", "")
        if inv_tf:
            label = f"{label} ({inv_tf})"

        # Skip logging this invalidation if it's a weekend — thin liquidity
        # wicks would silently skew invalidation-rate stats. The force-close
        # protection below still runs regardless, since that's real risk
        # management for any position still open from before the weekend.
        now_uk_inv_check = datetime.now(UK_TZ)
        is_weekend = now_uk_inv_check.weekday() >= 5
        if not is_weekend:
            db_log_invalidation(base_trigger, inv_direction)
        else:
            print(f"[Invalidation] {sig_key} fired on weekend — skipping stats logging, force-close protection still active")

        # ── Structure break invalidations force-close if you're in the matching position ──
        # These are strong, objective invalidations (close beyond the candle's high/low),
        # not soft pattern failures — treat them as a hard signal, not just a warning.
        FORCE_CLOSE_INVALIDATIONS = {"orange_structure_break_invalidated", "green_structure_break_invalidated"}
        if sig_key in FORCE_CLOSE_INVALIDATIONS:
            open_direction = current_trade_entry.get("direction")
            if open_direction == inv_direction:
                reason = (
                    f"{label} invalidated — price closed beyond the candle's "
                    f"{'high' if inv_direction == 'short' else 'low'}, setup unambiguously dead"
                )
                db_log_warning("hard_structure_invalidate", reason)
                close_position_now(reason=reason)
                if inv_direction == "long":
                    last_long_trigger_time = None
                else:
                    last_short_trigger_time = None
                return

        if inv_direction == "long":
            last_long_trigger_time = None
            send_telegram(
                f"⚠️ <b>Long invalidated — {label}</b>\n\n"
                f"Price closed below the trigger level.\n"
                f"If you're in a long, review your exit.\n"
                f"Do not enter another long until you get a new trigger.\n"
                f"Long setup signals are now blocked until a new trigger fires."
            )
        else:
            last_short_trigger_time = None
            send_telegram(
                f"⚠️ <b>Short invalidated — {label}</b>\n\n"
                f"Price closed above the trigger level.\n"
                f"If you're in a short, review your exit.\n"
                f"Do not enter another short until you get a new trigger.\n"
                f"Short setup signals are now blocked until a new trigger fires."
            )
        return

    if sig_key in INDICATOR_SIGNALS:
        signal_data = INDICATOR_SIGNALS[sig_key]
        if signal_data is None:
            # Informational signal — already handled above or silently ignored
            print(f"[Signal] {sig_key} is informational — no prompt generated")
            return
        direction, label = signal_data
        tf = data.get("tf", "")
        if tf:
            label = f"{label} ({tf})"

        # ── Standardized virtual trigger win-rate tracking ──────────────
        # Fires for EVERY signal, independent of guards or blocks (loss
        # limit, trade cap, etc.) or whether a real trade was taken — fixed
        # 0.35% TP / 0.25% SL per trigger, so win rate reflects the trigger
        # itself, nothing else. EXCLUDED: weekends (thin liquidity skews
        # results) and the 14:00-15:30 UK window (same reasoning — a known
        # noisy/thin period not representative of normal conditions).
        now_uk_vt_check = datetime.now(UK_TZ)
        is_weekend_vt = now_uk_vt_check.weekday() >= 5
        is_blocked_window_vt = dt_time(14, 0) <= now_uk_vt_check.time() < dt_time(15, 30)
        if not is_weekend_vt and not is_blocked_window_vt:
            try:
                vt_entry_price = get_mark_price()
                open_virtual_trigger_trade(sig_key, direction, vt_entry_price)
            except Exception as e:
                print(f"[Virtual Trigger] Open hook error: {e}")

        # ── Weekend cutoff — no prompts, no triggers, nothing at all ────
        # Applies to every trade signal (manual prompts, structure breaks,
        # MTF combos, the experiment account) — thin weekend liquidity,
        # not worth trading. Informational signals (composite_update,
        # zone_update etc.) are unaffected — those are handled earlier
        # and never reach this point.
        now_uk_weekend_check = datetime.now(UK_TZ)
        if now_uk_weekend_check.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            print(f"[Signal] Suppressed {sig_key} — weekend ({now_uk_weekend_check.strftime('%A')})")
            return

        last_trigger           = sig_key
        last_trigger_time      = time.time()
        last_trigger_direction = direction
        db_log_trigger_fire(sig_key)

        # Track today's activity
        trigger_counts_today[sig_key] = trigger_counts_today.get(sig_key, 0) + 1
        if direction == "short":
            short_trigger_times_today.append(time.time())
        elif direction == "long":
            long_trigger_times_today.append(time.time())

        # Only real candle/indicator signals open the setup window — not composite or structure signals
        VALID_LONG_TRIGGERS  = {"long", "strength", "green_structure_break", "30m_composite_crosses_up_30", "30m_composite_crosses_up_15", "wpr_crosses_up_82", "buy"}
        VALID_SHORT_TRIGGERS = {"3_bears", "bearish", "short", "orange_structure_break", "30m_composite_crosses_down_70", "30m_composite_crosses_down_85", "wpr_crosses_down_18"}

        if sig_key in VALID_LONG_TRIGGERS:
            last_long_trigger_time = time.time()
        elif sig_key in VALID_SHORT_TRIGGERS:
            last_short_trigger_time = time.time()

        if BIAS_FILTER_ENABLED and daily_bias == "bullish" and direction == "short":
            # Check if this is a structure break that contradicts the bias
            structure_break_signals = {
                "4h_bullish_structure_break", "4h_bearish_structure_break",
                "orange_structure_break"
            }
            if sig_key in structure_break_signals:
                send_bias_invalidation_alert(label, direction, sig_key)
            print(f"[Signal] Suppressed {label} — bias is bullish, shorts filtered")
            return
        if BIAS_FILTER_ENABLED and daily_bias == "bearish" and direction == "long":
            # Check if this is a structure break that contradicts the bias
            structure_break_signals = {
                "4h_bullish_structure_break", "4h_bearish_structure_break",
                "green_structure_break"
            }
            if sig_key in structure_break_signals:
                send_bias_invalidation_alert(label, direction, sig_key)
            print(f"[Signal] Suppressed {label} — bias is bearish, longs filtered")
            return

        # ── Counter-position alert ────────────────────────────────────
        # If in a long and a bearish signal fires (or vice versa) — prompt to consider closing
        open_direction = current_trade_entry.get("direction")
        if open_direction and open_direction != direction:
            entry_price = current_trade_entry.get("price", 0)
            mark        = get_mark_price()
            unreal_pct  = ((mark - entry_price) / entry_price * 100) if entry_price > 0 else 0
            if open_direction == "long":
                unreal_pct = unreal_pct
            else:
                unreal_pct = -unreal_pct

            # Pull real unrealised $ P&L directly from Binance rather than
            # approximating it — more reliable than deriving it from % and
            # an assumed position size.
            unreal_usd = None
            try:
                live_pos = get_open_position()
                if live_pos:
                    unreal_usd = float(live_pos.get("unRealizedProfit", 0))
            except Exception as e:
                print(f"[Counter Signal] Could not fetch live unrealised $: {e}")

            usd_line = f" ({format_pnl(unreal_usd)})" if unreal_usd is not None else ""
            pnl_line = f"{'🟢' if unreal_pct >= 0 else '🔴'} Unrealised: {unreal_pct:+.2f}%{usd_line} from entry"
            db_log_warning("counter_signal", f"Counter signal while in a {open_direction.upper()} — {label} ({unreal_pct:+.2f}%)")
            severity_line = (
                "🚨 <b>You are currently LOSING on this trade AND the market just signaled against you.</b>\n\n"
                if unreal_pct < 0 else
                "🚨 <b>The market just signaled directly against your open position.</b>\n\n"
            )
            send_telegram(
                f"🛑🛑 <b>COUNTER SIGNAL — {open_direction.upper()} POSITION UNDER THREAT</b> 🛑🛑\n\n"
                f"{severity_line}"
                f"You're in a <b>{open_direction.upper()}</b> and a <b>{direction.upper()}</b> signal ({label}) just fired — the opposite direction.\n\n"
                f"{pnl_line}\n\n"
                f"⚠️ <b>This is not a routine notification — the setup you entered on may already be invalidated.</b>\n"
                f"Do not ignore this. Review your position now.\n\n"
                f"👉 Use /close to exit immediately if you're not confident in staying in."
            )
            return

        send_signal_prompt(direction, label, sig_key=sig_key)
        return

    if EXECUTION_ENABLED and AUTO_TRADE_SIGNALS and signal_name in AUTO_TRADE_SIGNALS:
        sig_lower = signal_name.lower()

        size_usdt = data.get("size_usdt")
        size_pct  = data.get("size_pct")
        stop_pct  = data.get("stop_pct")
        tp_pct    = data.get("tp_pct")
        zone      = data.get("zone")
        if not zone and current_zone != "unknown":
            zone = current_zone
        composite = data.get("composite")
        if composite is not None:
            try: composite = float(composite)
            except (ValueError, TypeError): composite = None

        explicit_dir = data.get("direction", "").strip().lower()
        if explicit_dir in ("long", "buy") or any(k in sig_lower for k in ("buy", "long", "bullish", "bull", "strength")):
            direction = "long"
        elif explicit_dir in ("short", "sell") or any(k in sig_lower for k in ("sell", "short", "bearish", "bear", "weakness")):
            direction = "short"
        else:
            direction = None

        if direction is None:
            print(f"[Webhook] Could not determine direction for signal: {signal_name}")
        else:
            threading.Thread(
                target=execute_trade,
                kwargs=dict(direction=direction, size_pct=size_pct, size_usdt=size_usdt,
                            stop_pct=stop_pct, tp_pct=tp_pct, triggered_by=signal_name,
                            zone=zone, composite=composite),
                daemon=True
            ).start()
        return

    # ── Catch-all — this signal matched NOTHING above: not a known trade
    # signal, not an invalidation, not a threshold crossing, not an
    # AUTO_TRADE_SIGNALS entry. Without this, an alert set up in TradingView
    # but never wired into bot.py just silently vanishes with zero trace,
    # which is exactly how a real signal can go quietly missing for weeks.
    print(f"[Webhook] UNRECOGNIZED signal — no handler matched: '{signal_name}' (full payload: {data})")
    db_log_warning("unrecognized_signal", f"'{signal_name}' fired but matched no handler in bot.py — check if it needs wiring in")
    return




@webhook_app.route("/", methods=["GET"])
def orion_dashboard():
    dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    return send_file(dashboard_path)

@webhook_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"}), 200


@webhook_app.route("/api/compounding-history", methods=["GET"])
def api_compounding_history():
    """
    Read-only endpoint for the external compounding tracker (a separate
    static frontend, no relation to trade execution). Returns:
    - weeks: every stored weekly starting balance in order (chained, this
      IS the actual weekly ledger)
    - daily_balances: every logged daily balance snapshot (for comparing
      against a locked-in daily target within the current week)
    - locked_targets: every week's locked weekly/daily multiplier targets
    - current_balance: live balance right now
    - weekly_target_multiplier: fallback default if a week has no lock
    Never touches trading state.
    """
    if READ_API_SECRET:
        provided = flask_request.headers.get("X-Secret", "") or flask_request.args.get("secret", "")
        if provided != READ_API_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    if not DATABASE_URL:
        return jsonify({"error": "Database not configured"}), 500

    try:
        # Ensures this week's starting-balance row exists before we read —
        # same auto-seed logic the /compound Telegram command already
        # relies on. Means the tracker never has to wait for a scheduled
        # check to run first; opening the page is enough to seed a fresh
        # week if one doesn't exist yet (e.g. right after a manual reset).
        get_compounding_progress()

        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT period_start, starting_balance
            FROM compounding_targets
            WHERE period_type = 'week'
            ORDER BY period_start ASC
        """)
        weeks = [
            {"period_start": r["period_start"].isoformat(), "starting_balance": float(r["starting_balance"])}
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT log_date, balance
            FROM daily_balance_log
            ORDER BY log_date ASC
        """)
        daily_balances = [
            {"date": r["log_date"].isoformat(), "balance": float(r["balance"])}
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT week_start, weekly_multiplier, daily_multiplier
            FROM locked_targets
            ORDER BY week_start ASC
        """)
        locked_targets = [
            {
                "week_start": r["week_start"].isoformat(),
                "weekly_multiplier": float(r["weekly_multiplier"]),
                "daily_multiplier": float(r["daily_multiplier"]),
            }
            for r in cur.fetchall()
        ]

        cur.close()
        conn.close()

        try:
            current_balance = get_usdt_balance()
        except Exception as e:
            print(f"[Compounding API] Balance fetch error: {e}")
            current_balance = None

        return jsonify({
            "weeks": weeks,
            "daily_balances": daily_balances,
            "locked_targets": locked_targets,
            "current_balance": current_balance,
            "weekly_target_multiplier": WEEKLY_COMPOUND_TARGET,
        }), 200
    except Exception as e:
        print(f"[Compounding API] Error: {e}")
        return jsonify({"error": "Internal error"}), 500


@webhook_app.route("/api/lock-target", methods=["POST", "OPTIONS"])
def api_lock_target():
    """
    Locks in the weekly and daily target multipliers for a given week,
    set from the tracker frontend. Upserts — re-locking the same week
    overwrites the previous values. Read/write is scoped entirely to the
    locked_targets table; never touches trading state or execution.
    """
    if flask_request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200

    if READ_API_SECRET:
        provided = flask_request.headers.get("X-Secret", "") or flask_request.args.get("secret", "")
        if provided != READ_API_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    body = flask_request.get_json(silent=True) or {}
    week_start = body.get("week_start")
    weekly_multiplier = body.get("weekly_multiplier")
    daily_multiplier = body.get("daily_multiplier")

    if not week_start or weekly_multiplier is None or daily_multiplier is None:
        return jsonify({"error": "week_start, weekly_multiplier, and daily_multiplier are all required"}), 400

    try:
        weekly_multiplier = float(weekly_multiplier)
        daily_multiplier = float(daily_multiplier)
    except (TypeError, ValueError):
        return jsonify({"error": "weekly_multiplier and daily_multiplier must be numbers"}), 400

    ok = db_lock_week_targets(week_start, weekly_multiplier, daily_multiplier)
    if not ok:
        return jsonify({"error": "Failed to save — check server logs"}), 500

    return jsonify({
        "status": "locked",
        "week_start": week_start,
        "weekly_multiplier": weekly_multiplier,
        "daily_multiplier": daily_multiplier,
    }), 200


def start_webhook_server():
    webhook_app.run(host="0.0.0.0", port=WEBHOOK_PORT, debug=False, use_reloader=False)


def get_same_side_consecutive_losses(trades: list) -> tuple:
    positions = group_by_position(trades)
    if not positions:
        return 0, None

    streak = 0
    side   = None
    for p in reversed(positions):
        if p["realizedPnl"] < 0:
            p_side = p.get("side", "")
            if side is None:
                side = p_side
            if p_side == side:
                streak += 1
            else:
                break
        else:
            break

    return streak, side


def in_cooldown() -> bool:
    now_uk = datetime.now(UK_TZ).time()
    return COOLDOWN_START <= now_uk < COOLDOWN_END


def format_pnl(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}${value:,.2f}"


def after_trade_summary() -> str:
    try:
        trades    = get_futures_trades_today()
        stats     = build_stats(trades)
        fees      = stats["total_fees"]
        remaining = FEE_ALERT_THRESHOLD - fees
        fee_pct   = (fees / FEE_ALERT_THRESHOLD * 100) if FEE_ALERT_THRESHOLD else 0
        fee_line  = (
            f"Fees: ${fees:.2f} / ${FEE_ALERT_THRESHOLD:.0f}  ({fee_pct:.0f}% — ${remaining:.2f} left)"
            if remaining > 0 else
            f"Fees: ${fees:.2f} — limit hit"
        )
        wr_line = f"{stats['closed_positions']} positions  ({stats['wins']}W / {stats['losses']}L  —  {stats['win_rate']:.0f}% WR)"

        positions = group_by_position(trades)
        gap_line  = ""
        hold_line = ""
        if len(positions) >= 2:
            sorted_pos  = sorted(positions, key=lambda p: int(p["time"]))
            entry_times = [int(p["time"]) for p in sorted_pos]
            gaps_m      = [(entry_times[i] - entry_times[i-1]) / 60000 for i in range(1, len(entry_times))]
            avg_gap     = sum(gaps_m) / len(gaps_m)
            gap_fmt     = f"{int(avg_gap // 60)}h {int(avg_gap % 60)}m" if avg_gap >= 60 else f"{avg_gap:.0f}m"
            gap_flag    = " ⚠️" if avg_gap < 120 else " ✅"
            gap_line    = f"Avg gap:   {gap_fmt} between trades{gap_flag}"
        if len(positions) >= 1:
            hold_mins = []
            for p in positions:
                h = (int(p["close_time"]) - int(p["time"])) / 60000
                if h >= 0:
                    hold_mins.append(h)
            if hold_mins:
                avg_h     = sum(hold_mins) / len(hold_mins)
                hold_fmt  = f"{int(avg_h // 60)}h {int(avg_h % 60)}m" if avg_h >= 60 else f"{avg_h:.0f}m"
                hold_flag = " ⚠️ holding too short" if avg_h < 120 else " ✅"
                hold_line = f"Avg hold:  {hold_fmt} per trade{hold_flag}"

        lines = [wr_line, fee_line]
        if gap_line:  lines.append(gap_line)
        if hold_line: lines.append(hold_line)
        return "\n".join(lines)
    except Exception:
        return ""


# ─────────────────────────────────────────────
# HELPERS — WEEKLY STATS PERSISTENCE
# ─────────────────────────────────────────────

def load_weekly_stats() -> list:
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as f:
            return json.load(f)
    return []


def save_daily_stats_to_file(date_str: str, stats: dict):
    weekly = load_weekly_stats()
    weekly = [d for d in weekly if d.get("date") != date_str]
    weekly.append({"date": date_str, **stats})
    weekly = sorted(weekly, key=lambda d: d["date"])[-7:]
    with open(STATS_FILE, "w") as f:
        json.dump(weekly, f)


WEEKLY_COMPOUND_TARGET = 1.5   # end-of-week balance multiplier target (~50% weekly gain)
CYCLE_COMPOUND_TARGET  = 5.0625   # end-of-4-week balance multiplier target (1.5^4)
CYCLE_WEEKS            = 4

def db_get_or_create_period_start(period_type: str, period_start_date) -> float:
    """
    Returns the stored starting balance for this period (week or 4-week cycle),
    creating it fresh if one doesn't exist yet. Persisted in the DB (not just
    memory) so a Railway restart mid-week doesn't lose the anchor balance —
    the whole point of this tracker breaks if the starting point silently
    resets after a crash.
    """
    if not DATABASE_URL:
        return None
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT starting_balance FROM compounding_targets
            WHERE period_type = %s AND period_start = %s
            LIMIT 1
        """, (period_type, period_start_date))
        row = cur.fetchone()
        if row:
            cur.close()
            conn.close()
            return float(row["starting_balance"])

        # No record for this period yet — create one using today's balance
        balance = get_usdt_balance()
        cur.execute("""
            INSERT INTO compounding_targets (period_type, period_start, starting_balance)
            VALUES (%s, %s, %s)
        """, (period_type, period_start_date, balance))
        conn.commit()
        cur.close()
        conn.close()
        return balance
    except Exception as e:
        print(f"[Compounding Target] DB error: {e}")
        return None


def db_log_daily_balance(log_date, balance: float):
    """
    Records one balance snapshot per calendar day (immutable — ON CONFLICT
    DO NOTHING, so a restart or re-run on the same day can't overwrite an
    already-logged value). This is the only source of daily granularity for
    the daily-target comparison view — the weekly compounding tracker alone
    only has Monday-to-Monday snapshots.
    """
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO daily_balance_log (log_date, balance)
            VALUES (%s, %s)
            ON CONFLICT (log_date) DO NOTHING
        """, (log_date, balance))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Daily Balance Log] DB error: {e}")


def db_lock_week_targets(week_start_date, weekly_multiplier: float, daily_multiplier: float):
    """
    Locks in (or updates) the weekly and daily target multipliers for a
    given week. Upsert on week_start — calling this again for the same
    week overwrites the previous lock, which is intentional (lets you
    correct a target you set before the week's actual is compared).
    """
    if not DATABASE_URL:
        return False
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO locked_targets (week_start, weekly_multiplier, daily_multiplier)
            VALUES (%s, %s, %s)
            ON CONFLICT (week_start) DO UPDATE
            SET weekly_multiplier = EXCLUDED.weekly_multiplier,
                daily_multiplier  = EXCLUDED.daily_multiplier,
                locked_at         = NOW()
        """, (week_start_date, weekly_multiplier, daily_multiplier))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[Locked Targets] DB error: {e}")
        return False


VIRTUAL_TP_PCT = 0.35   # fixed target % for standardized per-trigger win rate tracking
VIRTUAL_SL_PCT = 0.25   # fixed stop % for the same

def open_virtual_trigger_trade(trigger: str, direction: str, entry_price: float):
    """
    Opens a standardized 'virtual trade' the moment any signal fires —
    completely independent of whether a real trade was taken. Fixed
    0.35% TP / 0.25% SL every time, so the only variable across trades is
    the trigger itself, giving a clean per-trigger win rate uncontaminated
    by discretion, sizing, or guard blocks. Skips opening a new one if this
    exact trigger already has an unresolved virtual trade open.
    """
    if not DATABASE_URL or entry_price <= 0:
        return
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            SELECT 1 FROM virtual_trigger_trades
            WHERE trigger = %s AND outcome IS NULL
            LIMIT 1
        """, (trigger,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return  # already an open virtual trade for this trigger — skip

        if direction == "long":
            target_price = entry_price * (1 + VIRTUAL_TP_PCT / 100)
            stop_price   = entry_price * (1 - VIRTUAL_SL_PCT / 100)
        else:
            target_price = entry_price * (1 - VIRTUAL_TP_PCT / 100)
            stop_price   = entry_price * (1 + VIRTUAL_SL_PCT / 100)

        cur.execute("""
            INSERT INTO virtual_trigger_trades (trigger, direction, entry_price, target_price, stop_price)
            VALUES (%s, %s, %s, %s, %s)
        """, (trigger, direction, entry_price, target_price, stop_price))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Virtual Trigger] Open error: {e}")


def check_virtual_trigger_trades():
    """
    Runs every poll cycle — checks all open virtual trigger trades against
    the current mark price, closing any that have hit their fixed TP or SL.
    Also force-resolves anything still open by Friday 22:00 UK — a virtual
    trade shouldn't drift unresolved into the weekend, since weekend price
    action isn't representative of the conditions the trigger fired in.
    """
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM virtual_trigger_trades WHERE outcome IS NULL")
        open_trades = cur.fetchall()

        if not open_trades:
            cur.close()
            conn.close()
            return  # nothing open — skip the mark price fetch entirely

        mark = get_mark_price()
        if mark <= 0:
            cur.close()
            conn.close()
            return

        now_uk_vt = datetime.now(UK_TZ)
        force_resolve_friday = now_uk_vt.weekday() == 4 and now_uk_vt.time() >= dt_time(22, 0)

        for vt in open_trades:
            direction = vt["direction"]
            hit_target = (direction == "long" and mark >= vt["target_price"]) or \
                         (direction == "short" and mark <= vt["target_price"])
            hit_stop   = (direction == "long" and mark <= vt["stop_price"]) or \
                         (direction == "short" and mark >= vt["stop_price"])

            if hit_target or hit_stop:
                outcome = "win" if hit_target else "loss"
                cur.execute("""
                    UPDATE virtual_trigger_trades
                    SET outcome = %s, exit_price = %s, closed_at = NOW()
                    WHERE id = %s
                """, (outcome, mark, vt["id"]))
            elif force_resolve_friday:
                # Neither TP nor SL hit, but it's Friday 22:00+ — force a
                # resolution based on which side of entry the price landed,
                # rather than let it drift unresolved into the weekend.
                entry = vt["entry_price"]
                if direction == "long":
                    outcome = "win" if mark >= entry else "loss"
                else:
                    outcome = "win" if mark <= entry else "loss"
                cur.execute("""
                    UPDATE virtual_trigger_trades
                    SET outcome = %s, exit_price = %s, closed_at = NOW()
                    WHERE id = %s
                """, (outcome, mark, vt["id"]))
                print(f"[Virtual Trigger] Force-resolved trigger {vt['trigger']} at Friday 22:00 cutoff — {outcome}")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Virtual Trigger] Check error: {e}")


def get_daily_compounding_pace() -> dict:
    """
    Each morning: given today's actual starting balance and how many trading
    days remain this week (Mon-Fri, week ends Friday 8pm), calculates the %
    and $ gain needed TODAY to stay on pace for the 2.0114x weekly target —
    assuming equal daily compounding for the remaining days. Recalculated
    fresh each morning off the real current balance, so it re-paces itself
    daily rather than assuming a fixed schedule.
    """
    now_uk = datetime.now(UK_TZ)
    monday_this_week = (now_uk - timedelta(days=now_uk.weekday())).date()

    week_start_balance = db_get_or_create_period_start("week", monday_this_week)
    if not week_start_balance or week_start_balance <= 0:
        return {}

    try:
        current_balance = get_usdt_balance()
    except Exception as e:
        print(f"[Daily Pace] Balance fetch error: {e}")
        return {}

    # Trading days remaining INCLUDING today, Mon=0 .. Fri=4, week ends Fri 8pm
    weekday = now_uk.weekday()
    if weekday > 4:
        return {}  # weekend — no pacing needed
    days_remaining_incl_today = 5 - weekday  # Mon->5, Tue->4, Wed->3, Thu->2, Fri->1

    week_target_balance = week_start_balance * WEEKLY_COMPOUND_TARGET
    if current_balance <= 0:
        return {}

    # Required daily multiplier, compounded evenly across remaining days,
    # to go from current_balance to week_target_balance.
    required_daily_multiplier = (week_target_balance / current_balance) ** (1 / days_remaining_incl_today)
    required_pct_today = (required_daily_multiplier - 1) * 100
    required_usd_today = current_balance * (required_daily_multiplier - 1)

    week_multiplier_so_far = current_balance / week_start_balance
    on_pace = week_multiplier_so_far >= (WEEKLY_COMPOUND_TARGET ** (weekday / 5)) if weekday > 0 else True

    return {
        "current_balance": current_balance,
        "week_start_balance": week_start_balance,
        "week_target_balance": week_target_balance,
        "days_remaining": days_remaining_incl_today,
        "required_pct_today": required_pct_today,
        "required_usd_today": required_usd_today,
        "week_multiplier_so_far": week_multiplier_so_far,
        "on_pace": on_pace,
    }


def get_setup_grade(direction: str) -> dict:
    """
    Grades a potential entry as A / B+ / B- / NONE / BLOCKED. Two genuinely
    different market environments depending on where price sits relative
    to the 240 EMA:

    BELOW the 240 EMA (long) / ABOVE the 240 EMA (short) — a discount/
    downtrend-pullback environment. Graded purely on WPR:
      long:  WPR ≤ -65        → A   (deepest oversold, composite must be <50)
             WPR -35 to -18   → B-  (not optimal, not chasing — composite must be <50)
             WPR -65 to -35   → NONE (no-man's-land — deliberately ungraded,
                                       other signals cover this zone)
             WPR > -18        → BLOCKED (full chase zone)
      short: mirrored — WPR ≥ 65 → A, WPR 35 to 18 → B-, 65 to 35 → NONE, <18 → BLOCKED

    ABOVE the 240 EMA (long) / BELOW the 240 EMA (short) — a genuinely
    different environment: buying a pullback WITHIN an established trend,
    not a reversal. Only B+ or BLOCKED apply here — no A or B- in this zone.
      long:  price above 240 EMA AND below 120 EMA AND WPR ≤ -65 AND
             composite <50 → B+. Any of those failing → BLOCKED.
      short: mirrored.

    Composite <50 (long) / >50 (short) is a HARD requirement for A and B+
    both — fails closed (treated as failing the condition) if composite
    data is missing or stale, same fail-safe philosophy as the earlier
    B- Setup guard.

    NOTE: this does NOT include the pink-candle confirmation — that's a
    yes/no question asked separately in the Telegram flow (see
    _send_setup_grade_prompt / the pink-candle callback handlers), since
    it requires Nathan's own visual confirmation, not something the bot
    can check itself.

    HTF BIAS GATE: checked FIRST, before anything else. Set by the 2H
    bullish/bearish structure break alerts. Bullish HTF bias blocks ALL
    short grades; bearish HTF bias blocks ALL long grades — applies to
    A, B+, and B- equally, no exceptions. If no HTF bias has been set yet
    (no structure break alert has fired since the bot started), this gate
    is skipped entirely rather than blocking everything — fails open, not
    closed, since an unset bias isn't the same as a genuinely conflicting one.

    HTF WPR EXHAUSTION GATE (both directions): checked SECOND, before any
    LTF condition. Shorts: if the 2H WPR is below HTF_WPR_SHORT_EXHAUSTION_MAX
    (more negative than -86 by default), the higher timeframe is already at
    its deepest oversold extreme — shorting into that is fighting an
    overextended move with genuinely asymmetric risk of a violent bounce.
    Longs (mirrored): if the 2H WPR is above HTF_WPR_LONG_EXHAUSTION_MIN
    (closer to 0 than -14 by default), the HTF is already deeply overbought —
    longing into that risks the same asymmetric reversal. Either overrides
    EVERY grade (A/B+/B-) in that direction regardless of how good the LTF
    setup looks — a favourable LTF condition does NOT override this. If HTF
    WPR data is unavailable, the gate is skipped (fails open) rather than
    blocking on a data gap.
    """
    if htf_bias == "bullish" and direction == "short":
        return {"grade": "BLOCKED", "probability": None, "reason": "HTF bias is bullish (2H) — all shorts blocked until bias flips"}
    if htf_bias == "bearish" and direction == "long":
        return {"grade": "BLOCKED", "probability": None, "reason": "HTF bias is bearish (2H) — all longs blocked until bias flips"}

    if direction == "short":
        htf_wpr = get_live_williams_r(interval="2h", length=120)
        if htf_wpr is not None and htf_wpr < HTF_WPR_SHORT_EXHAUSTION_MAX:
            return {"grade": "BLOCKED", "probability": None,
                    "reason": f"HTF (2H) WPR {htf_wpr:.1f} below {HTF_WPR_SHORT_EXHAUSTION_MAX:.0f} — "
                              f"higher timeframe already deeply exhausted, too risky to short into this. "
                              f"Overrides any favourable LTF condition."}

    if direction == "long":
        htf_wpr = get_live_williams_r(interval="2h", length=120)
        if htf_wpr is not None and htf_wpr > HTF_WPR_LONG_EXHAUSTION_MIN:
            return {"grade": "BLOCKED", "probability": None,
                    "reason": f"HTF (2H) WPR {htf_wpr:.1f} above {HTF_WPR_LONG_EXHAUSTION_MIN:.0f} — "
                              f"higher timeframe already deeply overbought, too risky to long into this. "
                              f"Overrides any favourable LTF condition."}

    wpr = get_live_williams_r(interval="15m", length=120)
    if wpr is None:
        return {"grade": None, "probability": None, "reason": f"WPR data unavailable ({_wpr_last_error or 'unknown reason'})"}

    ema_120 = get_live_ema(interval="15m", period=120)
    ema_240 = get_live_ema240()
    try:
        mark = get_mark_price()
    except Exception:
        mark = None

    composite_fresh = (
        current_composite is not None
        and composite_last_updated is not None
        and (time.time() - composite_last_updated) < STALE_COMPOSITE_MAX_AGE_SEC
    )
    # ⚠️ TEMPORARILY DISABLED (Nathan's request) — composite alerts were sending
    # {"signal": "composite_crossed_below_50"} with NO composite value in the
    # payload, so current_composite could never actually be set — this was
    # silently blocking every A/B+/B- grade in both directions since the
    # feature was built, forcing Nathan back to trading manually on Binance.
    # Hardcoded to always pass until the TradingView alert JSON is fixed to
    # include the real live composite number. REMINDER: re-enable the two
    # lines below (composite_fresh and current_composite < / > 50) once
    # that's fixed — see the flagged TODO_REENABLE_COMPOSITE_GATE marker.
    composite_confirms_long  = True   # TODO_REENABLE_COMPOSITE_GATE: composite_fresh and current_composite < 50
    composite_confirms_short = True   # TODO_REENABLE_COMPOSITE_GATE: composite_fresh and current_composite > 50
    composite_reason = "composite check temporarily disabled" if not composite_fresh else f"composite {current_composite:.1f}"

    if mark is None or ema_240 is None:
        return {"grade": None, "probability": None, "reason": "price/EMA data unavailable"}

    if direction == "long":
        above_240 = mark > ema_240
        if above_240:
            # ── B+ environment: pullback within an established uptrend ──
            if ema_120 is None:
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": "120 EMA unavailable — cannot evaluate B+ band"}
            in_band = mark < ema_120
            if in_band and wpr <= WPR_GRADE_A_LONG_MAX and composite_confirms_long:
                return {"grade": "B+", "probability": 60, "wpr": wpr,
                        "reason": f"Price above 240 EMA, below 120 EMA (uptrend pullback), WPR {wpr:.1f} ≤ {WPR_GRADE_A_LONG_MAX}, {composite_reason} (<50)"}
            reasons = []
            if not in_band:
                reasons.append("price not below 120 EMA")
            if wpr > WPR_GRADE_A_LONG_MAX:
                reasons.append(f"WPR {wpr:.1f} above {WPR_GRADE_A_LONG_MAX}")
            if not composite_confirms_long:
                reasons.append(composite_reason + " (needs <50)")
            return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": "B+ conditions not met — " + ", ".join(reasons)}
        else:
            # ── A / B- environment: discount / downtrend pullback ──
            if wpr <= WPR_GRADE_A_LONG_MAX:
                if composite_confirms_long:
                    return {"grade": "A", "probability": 70, "wpr": wpr,
                            "reason": f"WPR {wpr:.1f} ≤ {WPR_GRADE_A_LONG_MAX} (deepest oversold), {composite_reason} (<50)"}
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} qualifies for A but {composite_reason} (needs <50)"}
            elif wpr <= WPR_LONG_A_TO_NOMANS_LAND:
                return {"grade": None, "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} in no-man's-land ({WPR_GRADE_A_LONG_MAX} to {WPR_LONG_A_TO_NOMANS_LAND}) — not graded, use other signals"}
            elif wpr <= WPR_B_MINUS_LONG_MAX:
                if composite_confirms_long:
                    return {"grade": "B-", "probability": 50, "wpr": wpr,
                            "reason": f"WPR {wpr:.1f} in B- band ({WPR_LONG_A_TO_NOMANS_LAND} to {WPR_B_MINUS_LONG_MAX}), {composite_reason} (<50)"}
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} qualifies for B- but {composite_reason} (needs <50)"}
            else:
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": f"WPR {wpr:.1f} past hard cutoff ({WPR_B_MINUS_LONG_MAX}) — full chase zone"}
    else:  # short
        below_240 = mark < ema_240
        if below_240:
            # ── B+ environment (mirrored): pullback within an established downtrend ──
            if ema_120 is None:
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": "120 EMA unavailable — cannot evaluate B+ band"}
            in_band = mark > ema_120
            if in_band and wpr >= WPR_GRADE_A_SHORT_MIN and composite_confirms_short:
                return {"grade": "B+", "probability": 60, "wpr": wpr,
                        "reason": f"Price below 240 EMA, above 120 EMA (downtrend pullback), WPR {wpr:.1f} ≥ {WPR_GRADE_A_SHORT_MIN}, {composite_reason} (>50)"}
            reasons = []
            if not in_band:
                reasons.append("price not above 120 EMA")
            if wpr < WPR_GRADE_A_SHORT_MIN:
                reasons.append(f"WPR {wpr:.1f} below {WPR_GRADE_A_SHORT_MIN}")
            if not composite_confirms_short:
                reasons.append(composite_reason + " (needs >50)")
            return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": "B+ conditions not met — " + ", ".join(reasons)}
        else:
            # ── A / B- environment (mirrored): premium / uptrend pullback ──
            if wpr >= WPR_GRADE_A_SHORT_MIN:
                if composite_confirms_short:
                    return {"grade": "A", "probability": 70, "wpr": wpr,
                            "reason": f"WPR {wpr:.1f} ≥ {WPR_GRADE_A_SHORT_MIN} (deepest overbought), {composite_reason} (>50)"}
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} qualifies for A but {composite_reason} (needs >50)"}
            elif wpr >= WPR_SHORT_A_TO_NOMANS_LAND:
                return {"grade": None, "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} in no-man's-land ({WPR_SHORT_A_TO_NOMANS_LAND} to {WPR_GRADE_A_SHORT_MIN}) — not graded, use other signals"}
            elif wpr >= WPR_B_MINUS_SHORT_MIN:
                if composite_confirms_short:
                    return {"grade": "B-", "probability": 50, "wpr": wpr,
                            "reason": f"WPR {wpr:.1f} in B- band ({WPR_B_MINUS_SHORT_MIN} to {WPR_SHORT_A_TO_NOMANS_LAND}), {composite_reason} (>50)"}
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr,
                        "reason": f"WPR {wpr:.1f} qualifies for B- but {composite_reason} (needs >50)"}
            else:
                return {"grade": "BLOCKED", "probability": None, "wpr": wpr, "reason": f"WPR {wpr:.1f} past hard cutoff ({WPR_B_MINUS_SHORT_MIN}) — full chase zone"}


def _send_setup_grade_prompt(direction: str, grade_result: dict):
    """
    Sends the grade-and-confirm prompt for a manual /long or /short.
    Grades A and B+ require a pink-candle confirmation from Nathan first
    (the bot can't see chart candle coloring) — B- and the rest skip
    straight to the trade confirm/cancel step.
    """
    global pending_manual_trade
    grade = grade_result.get("grade")
    reason = grade_result.get("reason", "")

    if grade is None:
        send_telegram(f"❔ <b>{direction.upper()} — no grade</b>\n\n{reason}")
        return

    if grade == "BLOCKED":
        db_log_warning("wpr_guard_block", f"{direction.capitalize()} blocked at prompt stage — {reason}")
        send_telegram(
            f"🚫 <b>{direction.upper()} blocked</b>\n\n{reason}"
        )
        return

    if grade in ("A", "B+"):
        # A and B+ both require Nathan to confirm the candle is pink —
        # the bot has no way to read chart candle coloring itself.
        pending_manual_trade = {"direction": direction, "grade": grade, "probability": grade_result.get("probability"), "reason": reason, "awaiting_pink_candle": True}
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": (
                f"❓ <b>{direction.upper()} — provisional Grade {grade}</b>\n\n"
                f"{reason}\n\n"
                f"Is the candle pink right now?"
            ),
            "parse_mode": "HTML",
            "reply_markup": {
                "inline_keyboard": [[
                    {"text": "🩷 Yes — pink", "callback_data": f"pinkcandle_yes_{direction}"},
                    {"text": "❌ No — not pink", "callback_data": f"pinkcandle_no_{direction}"}
                ]]
            }
        }
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload, timeout=10
            )
        except Exception as e:
            print(f"[Pink Candle Prompt] Send error: {e}")
        return

    _send_final_grade_confirm(direction, grade, grade_result.get("probability"), reason)


def _send_final_grade_confirm(direction: str, grade: str, probability, reason: str):
    """The actual confirm/cancel step, reached directly for B- or after pink-candle confirmation for A/B+."""
    global pending_manual_trade
    grade_emoji = {"A": "🟢", "B+": "🟡", "B-": "🟠"}.get(grade, "⚪")
    pending_manual_trade = {"direction": direction, "grade": grade}

    text = (
        f"{grade_emoji} <b>{direction.upper()} — Grade {grade}</b>\n\n"
        f"{reason}\n"
        f"Estimated probability: ~{probability}%\n"
        f"<i>(Nathan's own stated estimate per grade, not calculated from real trade data)</i>\n\n"
        f"Confirm this trade?"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": f"✅ Confirm {grade}", "callback_data": f"manual_confirm_{direction}"},
                {"text": "❌ Cancel",            "callback_data": f"manual_cancel_{direction}"}
            ]]
        }
    }
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
    except Exception as e:
        print(f"[Setup Grade Prompt] Send error: {e}")


def get_compounding_progress() -> dict:
    """
    Returns current progress toward the weekly (2.0114x) and cycle/4-week
    (16.3679x) compounding targets, anchored to the Monday-of-this-week
    starting balance and the start-of-current-4-week-cycle balance.
    """
    now_uk = datetime.now(UK_TZ)
    monday_this_week = (now_uk - timedelta(days=now_uk.weekday())).date()

    # 4-week cycle start = the most recent Monday that's a multiple of
    # CYCLE_WEEKS weeks after the very first recorded cycle start. Simplest
    # honest approach: track cycle start as "the Monday of the week
    # containing the first ever balance snapshot", then roll forward in
    # CYCLE_WEEKS-week blocks from there.
    cycle_start_date = None
    if DATABASE_URL:
        try:
            conn = get_db()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT MIN(period_start) as first_week FROM compounding_targets
                WHERE period_type = 'week'
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row and row["first_week"]:
                first_week = row["first_week"]
                weeks_elapsed = (monday_this_week - first_week).days // 7
                cycles_elapsed = weeks_elapsed // CYCLE_WEEKS
                cycle_start_date = first_week + timedelta(weeks=cycles_elapsed * CYCLE_WEEKS)
            else:
                cycle_start_date = monday_this_week  # first week ever — this week starts cycle 1
        except Exception as e:
            print(f"[Compounding Target] Cycle calc error: {e}")
            cycle_start_date = monday_this_week

    week_start_balance  = db_get_or_create_period_start("week", monday_this_week)
    cycle_start_balance = db_get_or_create_period_start("cycle", cycle_start_date) if cycle_start_date else None

    try:
        current_balance = get_usdt_balance()
    except Exception as e:
        print(f"[Compounding Target] Balance fetch error: {e}")
        return {}

    result = {"current_balance": current_balance}

    if week_start_balance and week_start_balance > 0:
        week_multiplier = current_balance / week_start_balance
        result["week_start_balance"] = week_start_balance
        result["week_multiplier"] = week_multiplier
        result["week_target_balance"] = week_start_balance * WEEKLY_COMPOUND_TARGET
        result["week_pct_of_target"] = (week_multiplier - 1) / (WEEKLY_COMPOUND_TARGET - 1) * 100 if WEEKLY_COMPOUND_TARGET > 1 else 0

    if cycle_start_balance and cycle_start_balance > 0:
        cycle_multiplier = current_balance / cycle_start_balance
        result["cycle_start_balance"] = cycle_start_balance
        result["cycle_multiplier"] = cycle_multiplier
        result["cycle_target_balance"] = cycle_start_balance * CYCLE_COMPOUND_TARGET
        result["cycle_pct_of_target"] = (cycle_multiplier - 1) / (CYCLE_COMPOUND_TARGET - 1) * 100 if CYCLE_COMPOUND_TARGET > 1 else 0
        result["cycle_start_date"] = cycle_start_date

    return result


def load_last_7_days_stats() -> list:
    return load_weekly_stats()


# ─────────────────────────────────────────────
# HELPERS — TELEGRAM
# ─────────────────────────────────────────────

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"[Telegram] {message[:80]}...")


def get_consecutive_wins(trades: list) -> int:
    positions = group_by_position(trades)
    streak    = 0
    for p in reversed(positions):
        if p["realizedPnl"] > 0:
            streak += 1
        else:
            break
    return streak


# ─────────────────────────────────────────────
# ALERTS
# ─────────────────────────────────────────────

def send_eod_summary(trades: list, balance_start: float, balance_end: float, date_str: str):
    stats = build_stats(trades)
    balance_change = balance_end - balance_start

    orders  = group_by_order(trades)
    closing = [o for o in orders if o["realizedPnl"] != 0]
    hold_times_win  = []
    hold_times_loss = []
    sorted_orders   = sorted(orders, key=lambda o: o["time"])
    for i, o in enumerate(sorted_orders):
        if o["realizedPnl"] == 0:
            continue
        for j in range(i - 1, -1, -1):
            if sorted_orders[j]["realizedPnl"] == 0:
                hold_ms = o["time"] - sorted_orders[j]["time"]
                hold_min = hold_ms / 60000
                if 0 < hold_min < 1440:
                    if o["realizedPnl"] > 0:
                        hold_times_win.append(hold_min)
                    else:
                        hold_times_loss.append(hold_min)
                break

    avg_hold_win  = sum(hold_times_win)  / len(hold_times_win)  if hold_times_win  else None
    avg_hold_loss = sum(hold_times_loss) / len(hold_times_loss) if hold_times_loss else None

    save_daily_stats_to_file(date_str, {**stats,
        "balance_start":    balance_start,
        "balance_end":      balance_end,
        "streaks_3plus":    daily_streaks_3plus,
        "avg_hold_win":     avg_hold_win,
        "avg_hold_loss":    avg_hold_loss,
        "worst_chunk_label": None,
        "worst_chunk_pnl":   None,
    })

    try:
        _positions = group_by_position(trades)
        _uk_tz     = ZoneInfo("Europe/London")
        _chunks    = {}
        for _p in _positions:
            _ts    = datetime.fromtimestamp(int(_p["close_time"]) / 1000, tz=_uk_tz)
            _start = (_ts.hour // 2) * 2
            _label = f"{_start:02d}:00–{_start+2:02d}:00"
            _chunks[_label] = _chunks.get(_label, 0) + _p["realizedPnl"]
        if _chunks:
            _worst_label = min(_chunks, key=_chunks.get)
            _worst_pnl   = _chunks[_worst_label]
            save_daily_stats_to_file(date_str, {**stats,
                "balance_start":     balance_start,
                "balance_end":       balance_end,
                "streaks_3plus":     daily_streaks_3plus,
                "avg_hold_win":      avg_hold_win,
                "avg_hold_loss":     avg_hold_loss,
                "worst_chunk_label": _worst_label if _worst_pnl < 0 else None,
                "worst_chunk_pnl":   _worst_pnl   if _worst_pnl < 0 else None,
                "rules_broken":      dict(rules_broken_today),
            })
    except Exception:
        pass

    pos_profit_mins = 0.0
    pos_under_mins  = 0.0
    try:
        closed_positions = group_by_position(trades)
        for p in closed_positions:
            entry_ms = int(p["time"])
            close_ms = int(p["close_time"])
            hold_ms  = close_ms - entry_ms
            if hold_ms > 0:
                if p["realizedPnl"] > 0:
                    pos_profit_mins += hold_ms / 60000
                else:
                    pos_under_mins  += hold_ms / 60000
    except Exception:
        pos_profit_mins = position_mins_profit
        pos_under_mins  = position_mins_under

    if pos_profit_mins + pos_under_mins > 0:
        p, u = pos_profit_mins, pos_under_mins
        pos_time_block = (
            f"In profit:   {int(p)}m\n"
            f"Underwater:  {int(u)}m\n"
            + (
                f"Ratio:       {p/u:.2f}x  Above 1.5x benchmark"
                if u > 0 and p / u >= 1.5 else
                f"Ratio:       {p/u:.2f}x  Below 1.5x benchmark (need {int(u * 1.5 - p)}m more in profit)"
                if u > 0 else
                "Ratio:       All time in profit"
            )
        )
    else:
        pos_time_block = "No closed positions tracked today."

    # ── Time in trade — fixed 8am-11pm window ───────────────────────────
    # Sums actual time spent in a position, clipped to a fixed daily
    # window (not "first entry to last close", which gave a misleading
    # 100% on any single-trade day). Consistent, comparable denominator
    # every day.
    day_window_block = "No closed positions tracked today."
    try:
        tit = calculate_time_in_trade(trades)
        if tit["window_mins"] is not None:
            def _fmt_hm(mins):
                h, m = divmod(int(round(mins)), 60)
                return f"{h}h {m}m" if h else f"{m}m"
            under_target = tit["in_trade_pct"] <= TIME_IN_TRADE_TARGET_PCT
            emoji = "✅" if under_target else "⚠️"
            day_window_block = (
                f"In a trade: {_fmt_hm(tit['in_trade_mins'])}  ({tit['in_trade_pct']:.0f}%)\n"
                f"Window:     {_fmt_hm(tit['window_mins'])}  ({TRADING_WINDOW_START_HOUR:02d}:00–{TRADING_WINDOW_END_HOUR:02d}:00)\n"
                f"{emoji} Target: under {TIME_IN_TRADE_TARGET_PCT:.0f}%"
            )
    except Exception as e:
        print(f"[EOD Summary] Time-in-trade calc error: {e}")

    # ── Weekly P&L so far, for context alongside today's numbers ──────
    week_line = ""
    try:
        weekly = load_last_7_days_stats()
        if weekly:
            week_pnl = sum(d["net_pnl"] for d in weekly)
            gap      = WEEKLY_PNL_TARGET - week_pnl
            if gap <= 0:
                week_line = f"🏅 Weekly P&L: {format_pnl(week_pnl)} — target HIT (+{format_pnl(abs(gap))} over)\n\n"
            else:
                week_line = f"📊 Weekly P&L: {format_pnl(week_pnl)} so far ({format_pnl(gap)} to go)\n\n"
    except Exception as e:
        print(f"[EOD Summary] Weekly P&L calc error: {e}")

    msg = (
        f"📋 <b>End of Day Summary — {date_str}</b>\n\n"
        f"Positions closed:   {stats['closed_positions']}  ({stats['wins']}W / {stats['losses']}L)\n"
        f"Win rate:           {stats['win_rate']:.1f}%\n"
        f"Avg win ROI:        +{stats['avg_win_roi_pct']:.2f}%\n"
        f"Avg loss ROI:       {stats['avg_loss_roi_pct']:.2f}%\n\n"
        f"Gross P&L:          {format_pnl(stats['total_pnl'])}\n"
        f"Fees paid:          -${stats['total_fees']:.2f}\n"
        f"Net P&L:            {format_pnl(stats['net_pnl'])}\n\n"
        f"Balance:            ${balance_start:,.2f} → ${balance_end:,.2f} "
        f"({format_pnl(balance_change)})\n\n"
        f"{week_line}"
        f"⏱ <b>Position time today</b>\n"
        f"{pos_time_block}\n\n"
        f"📅 <b>Day window — time in trade</b>\n"
        f"{day_window_block}"
    )
    send_telegram(msg)

    try:
        positions = group_by_position(trades)
        if len(positions) >= 2:
            uk_tz = ZoneInfo("Europe/London")
            entry_times = []
            hold_mins   = []
            for p in positions:
                entry_ts = datetime.fromtimestamp(int(p["time"]) / 1000, tz=uk_tz)
                close_ts = datetime.fromtimestamp(int(p["close_time"]) / 1000, tz=uk_tz)
                entry_times.append(entry_ts)
                hold = (close_ts - entry_ts).total_seconds() / 60
                if hold >= 0:
                    hold_mins.append(hold)

            entry_times.sort()
            gaps = []
            for i in range(1, len(entry_times)):
                gap_mins = (entry_times[i] - entry_times[i-1]).total_seconds() / 60
                gaps.append(gap_mins)

            avg_gap   = sum(gaps) / len(gaps)
            min_gap   = min(gaps)
            max_gap   = max(gaps)
            avg_hold  = sum(hold_mins) / len(hold_mins) if hold_mins else 0

            revenge_count = 0
            sorted_pos = sorted(positions, key=lambda p: int(p["time"]))
            for i in range(1, len(sorted_pos)):
                prev = sorted_pos[i - 1]
                curr = sorted_pos[i]
                if prev["realizedPnl"] < 0:
                    gap_ms = int(curr["time"]) - int(prev["close_time"])
                    if gap_ms <= REVENGE_WINDOW_MINS * 60 * 1000:
                        revenge_count += 1
            revenge_line = f"Revenge trades: {revenge_count} ⚠️" if revenge_count > 0 else "Revenge trades: 0 ✅"

            def fmt_mins(m):
                return f"{int(m // 60)}h {int(m % 60)}m" if m >= 60 else f"{m:.0f}m"

            freq_msg = (
                f"<b>Trade frequency today</b>\n\n"
                f"Avg gap:       {fmt_mins(avg_gap)} between trades\n"
                f"Shortest gap:  {fmt_mins(min_gap)}\n"
                f"Longest gap:   {fmt_mins(max_gap)}\n\n"
                f"Avg hold time: {fmt_mins(avg_hold)} per trade\n\n"
                f"{revenge_line}"
            )
            send_telegram(freq_msg)
    except Exception as e:
        print(f"[EOD Frequency] Error: {e}")

    try:
        positions = group_by_position(trades)
        if positions:
            uk_tz  = ZoneInfo("Europe/London")
            chunks = {}

            for p in positions:
                close_ts  = datetime.fromtimestamp(int(p["close_time"]) / 1000, tz=uk_tz)
                hour      = close_ts.hour
                chunk_start = (hour // 2) * 2
                chunk_end   = chunk_start + 2
                label       = f"{chunk_start:02d}:00–{chunk_end:02d}:00"
                chunks[label] = chunks.get(label, 0) + p["realizedPnl"]

            if chunks:
                worst_chunk = min(chunks, key=chunks.get)
                worst_pnl   = chunks[worst_chunk]

                chunk_lines = ""
                for label, pnl in sorted(chunks.items()):
                    marker = " ◀ worst" if label == worst_chunk and worst_pnl < 0 else ""
                    chunk_lines += f"  {label}:  {format_pnl(pnl)}{marker}\n"

                if worst_pnl < 0:
                    worst_line = f"\n⚠️ Worst 2-hour window: <b>{worst_chunk}</b> ({format_pnl(worst_pnl)})"
                else:
                    worst_line = f"\n✅ No losing 2-hour windows today."

                send_telegram(
                    f"🕐 <b>P&L by 2-hour window</b>\n\n"
                    f"{chunk_lines}"
                    f"{worst_line}"
                )
    except Exception as e:
        print(f"[EOD Chunks] Error: {e}")

    positions   = group_by_position(trades)
    ind_trades  = [p for p in positions if any(str(oid) in indicator_trade_ids for oid in p["order_ids"])]
    man_trades  = [p for p in positions if not any(str(oid) in indicator_trade_ids for oid in p["order_ids"])]

    if ind_trades and man_trades:
        ind_wins = len([p for p in ind_trades if p["realizedPnl"] > 0])
        man_wins = len([p for p in man_trades if p["realizedPnl"] > 0])
        ind_wr   = ind_wins / len(ind_trades) * 100
        man_wr   = man_wins / len(man_trades) * 100
        ind_pnl  = sum(p["realizedPnl"] for p in ind_trades)
        man_pnl  = sum(p["realizedPnl"] for p in man_trades)

        diff = ind_wr - man_wr
        if diff > 0:
            verdict = f"📡 Indicator trades outperformed by {diff:.1f}% WR — your signals are working."
        elif diff < 0:
            verdict = f"🖐 Manual trades outperformed by {abs(diff):.1f}% WR — your eye beats the signal today."
        else:
            verdict = "🤝 Indicator and manual trades matched on win rate today."

        breakdown = (
            f"📊 <b>Indicator vs Manual</b>\n\n"
            f"📡 Indicator:  {len(ind_trades)} positions  |  {ind_wins}W/{len(ind_trades)-ind_wins}L  |  {ind_wr:.1f}% WR  |  {format_pnl(ind_pnl)}\n"
            f"🖐 Manual:     {len(man_trades)} positions  |  {man_wins}W/{len(man_trades)-man_wins}L  |  {man_wr:.1f}% WR  |  {format_pnl(man_pnl)}\n\n"
            f"{verdict}"
        )
        send_telegram(breakdown)
    elif ind_trades and not man_trades:
        ind_wins = len([p for p in ind_trades if p["realizedPnl"] > 0])
        ind_wr   = ind_wins / len(ind_trades) * 100
        send_telegram(f"📡 <b>All trades today were indicator-triggered</b>\n{len(ind_trades)} positions — {ind_wr:.1f}% WR")

    weekly      = load_last_7_days_stats()
    yesterday   = weekly[-1] if weekly else None
    positives = []

    if stats["net_pnl"] > 0:
        positives.append(f"✅ Profitable day — {format_pnl(stats['net_pnl'])} net")

    if stats["win_rate"] >= 55 and stats["closed_positions"] >= 3:
        positives.append(f"🎯 Win rate {stats['win_rate']:.1f}% — above your 55% benchmark")

    if yesterday and yesterday.get("closed_positions", 0) > 0 and stats["closed_positions"] > 0:
        today_r   = stats["net_pnl"] / stats["closed_positions"] if stats["closed_positions"] else 0
        yest_r    = yesterday["net_pnl"] / yesterday["closed_positions"] if yesterday["closed_positions"] else 0
        if today_r > yest_r:
            positives.append(f"📈 R ratio improved vs yesterday ({format_pnl(today_r)} vs {format_pnl(yest_r)} per trade)")

    if yesterday:
        week_pnl_before = sum(d["net_pnl"] for d in weekly[:-1]) if len(weekly) > 1 else 0
        week_pnl_now    = sum(d["net_pnl"] for d in weekly)
        gap_before      = WEEKLY_PNL_TARGET - week_pnl_before
        gap_now         = WEEKLY_PNL_TARGET - week_pnl_now
        if gap_now < gap_before and gap_now > 0:
            positives.append(f"📊 Closer to weekly target — {format_pnl(gap_now)} to go")
        elif gap_now <= 0:
            positives.append(f"🏅 Weekly target already cleared this week")

    if positives:
        pos_msg = "🌟 <b>Today's wins</b>\n\n" + "\n".join(positives)
        send_telegram(pos_msg)

    if rules_broken_today:
        broken_list = "\n".join(
            f"• {r}  ×{c}" if c > 1 else f"• {r}"
            for r, c in rules_broken_today.items()
        )
        total_breaks = sum(rules_broken_today.values())
        send_telegram(
            f"🚨 <b>You broke your own rules today — {total_breaks} time(s).</b>\n\n"
            f"{broken_list}\n\n"
            f"This is self-sabotage. You <i>know</i> what works — you wrote the rules yourself. "
            f"Every time you cross these lines you're choosing noise over discipline. "
            f"Stop it.\n\n"
            f"Tomorrow: stick to the routine. Trust the process. You have the strategy, "
            f"you have the edge — just get out of your own way. You can do this. 💪"
        )


def send_weekly_summary():
    weekly = load_last_7_days_stats()
    if not weekly:
        return

    total_trades    = sum(d["total_trades"] for d in weekly)
    total_wins      = sum(d["wins"] for d in weekly)
    total_losses    = sum(d["losses"] for d in weekly)
    total_closed    = sum(d["closed_positions"] for d in weekly)
    total_pnl       = sum(d["total_pnl"] for d in weekly)
    total_fees      = sum(d["total_fees"] for d in weekly)
    net_pnl         = sum(d["net_pnl"] for d in weekly)
    win_rate        = (total_wins / total_closed * 100) if total_closed else 0

    best_day  = max(weekly, key=lambda d: d["net_pnl"])
    worst_day = min(weekly, key=lambda d: d["net_pnl"])

    win_holds  = [d["avg_hold_win"]  for d in weekly if d.get("avg_hold_win")  is not None]
    loss_holds = [d["avg_hold_loss"] for d in weekly if d.get("avg_hold_loss") is not None]
    hold_line  = ""
    if win_holds or loss_holds:
        parts = []
        if win_holds:
            parts.append(f"Winners {sum(win_holds)/len(win_holds):.0f}m")
        if loss_holds:
            parts.append(f"Losers {sum(loss_holds)/len(loss_holds):.0f}m")
        hold_line = f"\nAvg hold time:         ⏱ {' | '.join(parts)}"

    msg = (
        f"📊 <b>Weekly Summary ({weekly[0]['date']} – {weekly[-1]['date']})</b>\n\n"
        f"Total trades:   {total_trades}\n"
        f"Wins / Losses:  {total_wins} / {total_losses}\n"
        f"Win rate:       {win_rate:.1f}%\n\n"
        f"Gross P&L:      {format_pnl(total_pnl)}\n"
        f"Fees paid:      -${total_fees:.2f}\n"
        f"Net P&L:        {format_pnl(net_pnl)}\n\n"
        f"Best day:       {best_day['date']} ({format_pnl(best_day['net_pnl'])})\n"
        f"Worst day:      {worst_day['date']} ({format_pnl(worst_day['net_pnl'])})\n\n"
        f"Weekly target:  {format_pnl(WEEKLY_PNL_TARGET)} — "
        f"{'✅ HIT' if net_pnl >= WEEKLY_PNL_TARGET else f'❌ missed by {format_pnl(WEEKLY_PNL_TARGET - net_pnl)}'}\n\n"
        f"Trade limit breached:  "
        + (lambda days: f"{'⚠️ ' if days > 0 else '✅ '}{days}/{len(weekly)} days")(
            sum(1 for d in weekly if d.get("total_trades", 0) >= STOP_THRESHOLD)
        )
        + "\n"
        f"Win streaks (3+):      🔥 {sum(d.get('streaks_3plus', 0) for d in weekly)} times this week"
        + hold_line
    )
    send_telegram(msg)

    worst_chunk       = worst_day.get("worst_chunk_label")
    worst_chunk_pnl   = worst_day.get("worst_chunk_pnl")
    if worst_day["net_pnl"] < 0:
        chunk_detail = ""
        if worst_chunk and worst_chunk_pnl is not None:
            chunk_detail = f"\nHeaviest loss window: <b>{worst_chunk}</b> ({format_pnl(worst_chunk_pnl)})"
        send_telegram(
            f"📉 <b>Worst day breakdown — {worst_day['date']}</b>\n\n"
            f"Positions:  {worst_day['closed_positions']}  "
            f"({worst_day['wins']}W / {worst_day['losses']}L  —  {worst_day['win_rate']:.1f}% WR)\n"
            f"Net P&L:    {format_pnl(worst_day['net_pnl'])}\n"
            f"Fees:       -${worst_day['total_fees']:.2f}"
            f"{chunk_detail}"
        )

    weekly_rules = {}
    for d in weekly:
        for rule, count in d.get("rules_broken", {}).items():
            weekly_rules[rule] = weekly_rules.get(rule, 0) + count

    if weekly_rules:
        total_breaks = sum(weekly_rules.values())
        broken_list  = "\n".join(
            f"• {r}  ×{c}" if c > 1 else f"• {r}"
            for r, c in sorted(weekly_rules.items(), key=lambda x: -x[1])
        )
        send_telegram(
            f"🚨 <b>Rule breaks this week — {total_breaks} total</b>\n\n"
            f"{broken_list}"
        )
    else:
        send_telegram("✅ <b>No rules broken this week.</b> Discipline held.")


# ─────────────────────────────────────────────
# MIDNIGHT RESET
# ─────────────────────────────────────────────

def check_and_reset(trades: list):
    global early_warned_today, warned_today, stopped_today
    global STOP_THRESHOLD, first_loss_tightened_today
    global balance_up_alerted, balance_down_alerted, fee_alerted_today
    global eod_summary_sent_today
    global morning_recap_sent_today
    global last_cooldown_warned_count, last_reset_day, snapshot_balance, yesterday_stats
    global peak_balance_today, peak_drawdown_alerted, same_side_alerted
    global daily_losses_block, daily_losses_block_reset_at, last_loss_close_time, elevated_silence_until
    global daily_bias
    global trigger_counts_today, long_trigger_times_today, short_trigger_times_today
    global overtrade_alerted, daily_target_alerted, weekly_target_alerted
    global dayscore_ticked_today
    global daily_loss_warn_alerted, daily_loss_stop_alerted, profit_locked_today, profit_lock_last_attempt_at
    global overnight_alerted_1015, overnight_alerted_1045, position_mins_profit, position_mins_under, underwater_ratio_alerted
    global win_streak_alerted_at, daily_streaks_3plus, rules_broken_today

    now_uk = datetime.now(UK_TZ)
    today  = now_uk.date()

    eod_due = now_uk.hour == 23 and now_uk.minute < 10

    if eod_due and not eod_summary_sent_today:
        try:
            balance_end = get_usdt_balance()
            send_eod_summary(trades, snapshot_balance or 0, balance_end, str(today))
            db_log_daily_balance(today, balance_end)
            if today.weekday() == 4:
                send_weekly_summary()
            eod_summary_sent_today = True
        except Exception as e:
            print(f"[ERROR] EOD summary failed: {e}")

    # ── 6am recap — yesterday's full EOD summary, resent fresh ────────
    recap_due = now_uk.hour == 6 and now_uk.minute < 10
    if recap_due and not morning_recap_sent_today:
        try:
            yesterday_uk_date = today - timedelta(days=1)
            day_start_uk = datetime.combine(yesterday_uk_date, dt_time(0, 0), tzinfo=UK_TZ)
            day_end_uk   = datetime.combine(today, dt_time(0, 0), tzinfo=UK_TZ)
            start_ms = int(day_start_uk.astimezone(timezone.utc).timestamp() * 1000)
            end_ms   = int(day_end_uk.astimezone(timezone.utc).timestamp() * 1000)
            yesterday_trades = get_futures_trades(start_ms, end_ms)
            yesterday_balance_end = snapshot_balance or get_usdt_balance()
            send_telegram(f"☕ <b>6am recap — yesterday's full summary:</b>")
            send_eod_summary(yesterday_trades, yesterday_balance_end, yesterday_balance_end, str(yesterday_uk_date))
            morning_recap_sent_today = True
        except Exception as e:
            print(f"[ERROR] 6am recap failed: {e}")

    if last_reset_day != today:
        print(f"[DAILY RESET] Running — last_reset_day was {last_reset_day}, today is {today}. stopped_today was {stopped_today}, resetting to False.")
        try:
            snapshot_balance = get_usdt_balance()
        except Exception as e:
            print(f"[ERROR] Balance snapshot failed: {e}")

        early_warned_today          = False
        warned_today                = False
        stopped_today               = False
        STOP_THRESHOLD              = STOP_THRESHOLD_DEFAULT
        first_loss_tightened_today  = False
        balance_up_alerted          = False
        balance_down_alerted        = False
        fee_alerted_today           = False
        eod_summary_sent_today      = False
        morning_recap_sent_today    = False
        last_cooldown_warned_count  = 0
        peak_balance_today          = snapshot_balance
        peak_drawdown_alerted       = False
        same_side_alerted           = False
        same_side_block.clear()
        revenge_trade_ids_alerted.clear()
        daily_losses_block          = False
        daily_losses_block_reset_at = time.time()
        last_loss_close_time        = 0.0
        last_tp2_close_time         = 0.0
        elevated_silence_until      = 0.0
        overtrade_alerted           = False
        daily_target_alerted        = False
        weekly_target_alerted       = False
        dayscore_ticked_today       = False
        daily_loss_warn_alerted     = False
        daily_loss_stop_alerted     = False
        profit_locked_today         = 0.0
        profit_lock_last_attempt_at = 0.0
        overnight_alerted_1015      = False
        overnight_alerted_1045      = False
        overnight_auto_breakeven_done = False
        for _c in CLIENTS:
            _c.summary_sent_today = False
            _c.trades_today       = 0
            _c.wins_today         = 0
            _c.losses_today       = 0
        position_mins_profit        = 0.0
        position_mins_under         = 0.0
        win_streak_alerted_at       = 0
        daily_streaks_3plus         = 0
        rules_broken_today          = {}
        morning_brief_sent_today    = False
        weekly_digest_sent_today    = False
        weekly_okr_sent_today       = False
        monthly_review_sent_today   = False
        time_in_trade_warned_today  = False
        bias_question_sent_today    = False
        daily_bias                  = None
        indicator_trade_ids.clear()
        trigger_counts_today.clear()
        long_trigger_times_today.clear()
        short_trigger_times_today.clear()
        last_reset_day              = today
        print(f"[{today}] New day — all flags reset. Snapshot: ${snapshot_balance:,.2f}")


def poll_client_commands(c):
    """Poll one client's own Telegram bot for commands — fully isolated per client."""
    if not c.tg_token or not c.tg_chat_id:
        return

    # Force clear any webhook and competing connections
    try:
        requests.post(
            f"https://api.telegram.org/bot{c.tg_token}/deleteWebhook",
            json={"drop_pending_updates": True},
            timeout=10
        )
        # Short poll to grab the latest update_id and skip old updates
        resp = requests.get(
            f"https://api.telegram.org/bot{c.tg_token}/getUpdates",
            params={"timeout": 0, "limit": 1, "offset": -1},
            timeout=10
        )
        if resp.ok:
            updates = resp.json().get("result", [])
            if updates:
                c.tg_last_update_id = updates[-1]["update_id"]
    except Exception as e:
        print(f"[{c.label()} TG] Init error: {e}")

    print(f"[{c.label()} TG] Command polling started.")
    while True:
        try:
            url    = f"https://api.telegram.org/bot{c.tg_token}/getUpdates"
            params = {"timeout": 30, "offset": c.tg_last_update_id + 1, "allowed_updates": ["message"]}
            resp   = requests.get(url, params=params, timeout=40)
            resp.raise_for_status()
            updates = resp.json().get("result", [])

            for update in updates:
                c.tg_last_update_id = update["update_id"]
                msg     = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(c.tg_chat_id):
                    continue
                text = msg.get("text", "").strip().lower()
                if text == "/mystats":
                    try:
                        if not c.is_configured():
                            notify_client(c,
                                "🎉 <b>You're connected!</b>\n\n"
                                "This confirms your account is linked and the bot can reach you here — nothing to do on your end for now, it's alive.\n\n"
                                "<b>What happens next:</b> once your exchange account is set up and linked on our side, trades will start mirroring in automatically, and you'll get a message here every time one opens or closes.\n\n"
                                "Linking the exchange side is something I'll handle from here — no action needed from you until then."
                            )
                        elif c.trades_today == 0:
                            notify_client(c, "📊 <b>Today's Stats</b>\n\nNo closed positions yet today.")
                        else:
                            wr        = (c.wins_today / c.trades_today * 100) if c.trades_today else 0
                            wr_emoji  = "✅" if wr >= 50 else "📉"
                            notify_client(c,
                                f"📊 <b>Today's Stats</b>\n\n"
                                f"Trades:    {c.trades_today}\n"
                                f"Results:   {c.wins_today}W / {c.losses_today}L\n"
                                f"{wr_emoji} Win Rate:  {wr:.0f}%\n"
                            )
                    except Exception as e:
                        print(f"[{c.label()} TG] /mystats error: {e}")

                elif text == "/balance":
                    try:
                        bal = client_get_balance(c)
                        notify_client(c, f"💰 <b>Your balance</b>\n\n${bal:,.2f} USDC")
                    except Exception as e:
                        notify_client(c, f"❌ Balance error: {e}")
                        print(f"[{c.label()} TG] /balance error: {e}")

                elif text == "/status":
                    print(f"[{c.label()} TG] /status received from chat {chat_id}")
                    try:
                        info      = hl_get_info()
                        state     = info.user_state(c.address)
                        positions = state.get("assetPositions", [])
                        pos       = next((p.get("position") for p in positions
                                         if p.get("position", {}).get("coin") == HL_COIN
                                         and float(p.get("position", {}).get("szi", 0)) != 0), None)
                        if not pos:
                            notify_client(c, "📊 No open position on your account.")
                        else:
                            szi        = float(pos["szi"])
                            direction  = "LONG 🟢" if szi > 0 else "SHORT 🔴"
                            entry      = float(pos.get("entryPx", 0))
                            unreal     = float(pos.get("unrealizedPnl", 0))
                            pnl_emoji  = "✅" if unreal >= 0 else "❌"
                            notify_client(c,
                                f"📊 <b>Your position: {direction}</b>\n\n"
                                f"Entry:       ${entry:,.2f}\n"
                                f"{pnl_emoji} Unrealised: ${unreal:+.2f}"
                            )
                    except Exception as e:
                        notify_client(c, f"❌ Status error: {e}")
                        print(f"[{c.label()} TG] /status error: {e}")

                elif text == "/close":
                    try:
                        info      = hl_get_info()
                        state     = info.user_state(c.address)
                        positions = state.get("assetPositions", [])
                        has_pos   = any(
                            p.get("position", {}).get("coin") == HL_COIN
                            and float(p.get("position", {}).get("szi", 0)) != 0
                            for p in positions
                        )
                        if not has_pos:
                            notify_client(c, "ℹ️ No open position to close.")
                        else:
                            notify_client(c, "⏳ Closing your position...")
                            threading.Thread(target=client_mirror_close, args=(c,), daemon=True).start()
                    except Exception as e:
                        notify_client(c, f"❌ Close error: {e}")
                        print(f"[{c.label()} TG] /close error: {e}")

                elif text.startswith("/"):
                    notify_client(c,
                        "ℹ️ Available commands:\n\n"
                        "/status — your open position\n"
                        "/mystats — today's stats\n"
                        "/close — close your position"
                    )

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            print(f"[{c.label()} TG CMD ERROR] {e}")
            time.sleep(5)


# ─────────────────────────────────────────────

def register_client_commands(c) -> None:
    """Register commands for one client's bot and clear any webhook conflicts."""
    if not c.tg_token:
        return
    # Clear any webhook that might conflict with polling
    try:
        requests.post(
            f"https://api.telegram.org/bot{c.tg_token}/deleteWebhook",
            timeout=10
        )
    except Exception:
        pass
    commands = [
        {"command": "mystats", "description": "Today's trading stats"},
        {"command": "balance", "description": "Your account balance"},
        {"command": "close",   "description": "Close your open position"},
    ]
    try:
        url  = f"https://api.telegram.org/bot{c.tg_token}/setMyCommands"
        resp = requests.post(url, json={"commands": commands}, timeout=10)
        if resp.ok:
            print(f"[{c.label()} TG] Commands registered ✅")
        else:
            print(f"[{c.label()} TG] Command registration failed: {resp.text}")
    except Exception as e:
        print(f"[{c.label()} TG] Command registration error: {e}")


def register_telegram_commands() -> None:
    commands = [
        {"command": "long",        "description": "Manually open a long"},
        {"command": "short",       "description": "Manually open a short"},
        {"command": "close",      "description": "Close position — /close or /close 50"},
        {"command": "cut1",       "description": "Cut position size by 25%"},
        {"command": "cut2",       "description": "Cut position size by 50%"},
        {"command": "sl",         "description": "Move stop loss by % — /sl 0.5"},
        {"command": "tp",         "description": "Move TP1 by % — /tp 0.4"},
        {"command": "tp2",        "description": "Move TP2 by % — /tp2 0.6"},
        {"command": "breakeven",  "description": "Move SL to entry price"},
        {"command": "fill2",      "description": "Fill Entry2 limit at market now"},
        {"command": "bullish",    "description": "Set HTF bias to bullish"},
        {"command": "bearish",    "description": "Set HTF bias to bearish"},
        {"command": "cancel_entry", "description": "Cancel pending maker entry before it fills"},
        {"command": "cancel2",    "description": "Cancel Entry2 limit order"},
        {"command": "status",     "description": "Show open position & unrealised P&L"},
        {"command": "stats",      "description": "Today's full stats"},
        {"command": "patterns",   "description": "Show best performing setups"},
        {"command": "live",           "description": "Are prompts on right now, and why not"},
        {"command": "vwinrate",        "description": "Per-trigger win rate (fixed TP/SL)"},
        {"command": "compound",        "description": "Weekly/4-week compounding progress"},
        {"command": "winrate",         "description": "Win rate today/7d/30d vs 60% target"},
        {"command": "timewindow",      "description": "% of today's window spent in a trade"},
        {"command": "wsstatus",        "description": "Websocket connection health"},
        {"command": "discipline",      "description": "Week/month discipline trend"},
        {"command": "wpr",             "description": "Williams %R guard status"},
        {"command": "unmatched",      "description": "Alerts that fired but matched no handler"},
        {"command": "warnings",      "description": "Recent discipline warnings"},
        {"command": "htf",              "description": "HTF composite value and color"},
        {"command": "ema",             "description": "EMA premium/discount guard status"},
        {"command": "brute",         "description": "Force-close history"},
        {"command": "analytics",     "description": "Behavioral pattern analysis"},
        {"command": "invalidations", "description": "Invalidation rates per trigger"},
        {"command": "quality",       "description": "Entry quality: MFE, MAE, time to TP1"},
        {"command": "compare",    "description": "Manual vs indicator win rate today"},
        {"command": "stress",     "description": "Show stress state log"},
        {"command": "htf",        "description": "HTF bias change history"},
        {"command": "signals",    "description": "All configured signal prompts and status"},
        {"command": "help",       "description": "Full command list"},
    ]
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
        resp = requests.post(url, json={"commands": commands}, timeout=10)
        if resp.ok:
            print("[Telegram] Commands registered ✅")
        else:
            print(f"[Telegram] Command registration failed: {resp.text}")
    except Exception as e:
        print(f"[Telegram] Command registration error: {e}")


# ─────────────────────────────────────────────
# DAYSCORE — auto-tick "$60 profit day" when DAILY_PNL_TARGET is hit
# ─────────────────────────────────────────────
# Talks directly to the same PocketBase record DAYSCORE itself reads/writes
# (dayscore_state, owner="nathan"). Deliberately defensive: any failure here
# is logged and swallowed — a DAYSCORE hiccup must never affect trading.

def dayscore_today_str() -> str:
    # DAYSCORE's own JS tags every day with new Date().toISOString().split('T')[0],
    # which is a UTC date — NOT the UK-local date bot.py uses for its own daily
    # reset. Replicated here on purpose so the tick lands on the same calendar
    # day DAYSCORE itself thinks "today" is, even in the narrow window around UK
    # midnight during BST where the two dates can briefly disagree.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def dayscore_tick_profit_day() -> bool:
    """Ticks DAYSCORE's '$60 profit day' criterion for today, once. Returns True
    only if it actually made a change. Safe to call repeatedly — idempotent."""
    if not DAYSCORE_PB_ADMIN_EMAIL or not DAYSCORE_PB_ADMIN_PASSWORD:
        print("[DAYSCORE] Missing PB_ADMIN_EMAIL / PB_ADMIN_PASSWORD env vars — skipping auto-tick")
        return False
    try:
        auth_res = requests.post(
            f"{DAYSCORE_PB_URL}/api/collections/_superusers/auth-with-password",
            json={"identity": DAYSCORE_PB_ADMIN_EMAIL, "password": DAYSCORE_PB_ADMIN_PASSWORD},
            timeout=10,
        )
        auth_res.raise_for_status()
        token = auth_res.json()["token"]

        fetch_res = requests.get(
            f"{DAYSCORE_PB_URL}/api/collections/dayscore_state/records",
            params={"filter": f"(owner='{DAYSCORE_OWNER}')", "sort": "-updated", "perPage": 1},
            headers={"Authorization": token},
            timeout=10,
        )
        fetch_res.raise_for_status()
        items = fetch_res.json().get("items", [])
        if not items:
            print(f"[DAYSCORE] No dayscore_state record found for owner '{DAYSCORE_OWNER}' — skipping")
            return False

        record   = items[0]
        pb_state = record["json"]
        today_str  = dayscore_today_str()
        active_day = pb_state.get("activeDay")

        # Already closed today (manually, or a prior call already ticked and it's since
        # been closed)? Nothing to do — the day's done.
        if any(e.get("date") == today_str for e in pb_state.get("entries", [])):
            return False

        # No active day yet for today, or the stored active day is stale (from a prior
        # date that never got closed)? Leave that to the app's own recovery flow rather
        # than guessing — same principle as the 23:30 auto-close script.
        if not active_day or active_day.get("date") != today_str:
            print(f"[DAYSCORE] No active day for {today_str} yet — skipping auto-tick")
            return False

        checked = active_day.get("checkedCriteria", [])
        if DAYSCORE_PROFIT_CRITERIA_ID in checked:
            return False  # already ticked — manually, or by an earlier call today

        active_day["checkedCriteria"] = checked + [DAYSCORE_PROFIT_CRITERIA_ID]
        pb_state["activeDay"]    = active_day
        pb_state["lastModified"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        patch_res = requests.patch(
            f"{DAYSCORE_PB_URL}/api/collections/dayscore_state/records/{record['id']}",
            json={"json": pb_state},
            headers={"Authorization": token},
            timeout=10,
        )
        patch_res.raise_for_status()
        print(f"[DAYSCORE] Auto-ticked '$60 profit day' for {today_str}")
        return True
    except Exception as e:
        print(f"[DAYSCORE] Auto-tick failed (non-fatal): {e}")
        return False


def run():
    global early_warned_today, warned_today, stopped_today
    global STOP_THRESHOLD, first_loss_tightened_today
    global loss_streak_alerted, balance_up_alerted, balance_down_alerted
    global fee_alerted_today, eod_summary_sent_today
    global snapshot_balance, last_reset_day, last_cooldown_warned_count
    global peak_balance_today, peak_drawdown_alerted, same_side_alerted, same_side_block
    global daily_losses_block, daily_losses_block_reset_at, last_loss_close_time, last_tp2_close_time, elevated_silence_until
    global revenge_trade_ids_alerted, overtrade_alerted
    global daily_target_alerted, weekly_target_alerted
    global dayscore_ticked_today
    global daily_loss_warn_alerted, daily_loss_stop_alerted
    global win_streak_alerted_at, rules_broken_today
    global morning_brief_sent_today, weekly_digest_sent_today, weekly_okr_sent_today, monthly_review_sent_today, time_in_trade_warned_today, breakeven_suggested, breakeven_last_suggested_at, bias_question_sent_today, daily_bias
    global daily_streaks_3plus
    global profit_locked_today, profit_lock_last_attempt_at, overnight_alerted_1015, overnight_alerted_1045, overnight_auto_breakeven_done
    global entry2_placed_at
    global position_mins_profit, position_mins_under, underwater_ratio_alerted
    global trade_mfe, trade_mae, trade_entry_time, retrace_protect_triggered
    global _last_slow_checks_run

    last_trade_count = 0

    print("Trade Alert Bot started.")

    webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
    webhook_thread.start()
    print(f"[Webhook] Server listening on port {WEBHOOK_PORT}")

    start_websocket_feed()

    register_telegram_commands()
    for c in CLIENTS:
        if c.tg_token:
            register_client_commands(c)
    init_db()

    tg_cmd_thread = threading.Thread(target=poll_telegram_commands, daemon=True)
    tg_cmd_thread.start()
    for c in CLIENTS:
        if c.tg_token and c.tg_chat_id:
            threading.Thread(target=poll_client_commands, args=(c,), daemon=True).start()
            print(f"[{c.label()}] Telegram command thread started.")
    if EXECUTION_ENABLED:
        fill_thread = threading.Thread(target=poll_order_fills, daemon=True)
        fill_thread.start()

    try:
        snapshot_balance = get_usdt_balance()
    except Exception as e:
        print(f"[ERROR] Startup balance: {e}")

    last_reset_day = datetime.now(timezone.utc).date()

    now_uk = datetime.now(ZoneInfo("Europe/London"))
    if now_uk.hour >= 7 and now_uk.hour < 23:
        send_telegram(
            f"⚠️ <b>Bot restarted mid-session ({now_uk.strftime('%H:%M')} UK)</b>\n\n"
            f"Order fill tracking has been reset — Entry2, TP1, TP2 and SL fills "
            f"from earlier today will not fire alerts.\n\n"
            f"🚨 <b>If you have an open position:</b>\n"
            f"• Use /status to check your position\n"
            f"• Use /sl to re-set your stop loss immediately\n"
            f"• Use /tp and /tp2 to re-set your take profits\n"
            f"• You are at risk of liquidation until SL is re-set\n\n"
            f"Balance: <b>${snapshot_balance:,.2f} USDT</b>"
            if snapshot_balance else
            f"⚠️ <b>Bot restarted mid-session ({now_uk.strftime('%H:%M')} UK)</b>\n\n"
            f"🚨 <b>If you have an open position, re-set your SL and TP immediately — you are at risk of liquidation.</b>\n\n"
            f"Order fill tracking has been reset. Check positions on Binance."
        )
    else:
        send_telegram(
            f"✅ <b>Trade Alert Bot is running.</b>\n"
            f"Monitoring Binance Futures (USDⓈ-M) — {SYMBOL}\n"
            f"Starting balance: <b>${snapshot_balance:,.2f} USDT</b>"
            if snapshot_balance else
            "✅ <b>Trade Alert Bot is running.</b>\nMonitoring Binance Futures (USDⓈ-M)."
        )

    while True:
        try:
            trades, cached_balance = get_slow_refresh_data()
            orders  = group_by_order(trades)
            count   = len(group_by_position(trades))
            now_utc = datetime.now(timezone.utc)
            now_uk  = datetime.now(UK_TZ)
            now_str = now_utc.strftime("%H:%M UTC")

            print(f"[{now_str}] Orders today: {count} (from {len(trades)} fills)")

            check_and_reset(trades)

            # ── First trade of the day was a loss — tighten daily cap ──
            # Only tightens if FIRST_LOSS_STOP_THRESHOLD is genuinely LOWER
            # than the normal cap — if Nathan has deliberately raised both
            # to the same temporary value (as tonight), this correctly does
            # nothing, instead of re-tightening every restart because a
            # loss earlier today is permanently true for the rest of the day.
            if not first_loss_tightened_today:
                positions_today = group_by_position(trades)
                if positions_today and positions_today[0]["realizedPnl"] < 0:
                    first_loss_tightened_today = True
                    if FIRST_LOSS_STOP_THRESHOLD < STOP_THRESHOLD_DEFAULT:
                        old_threshold = STOP_THRESHOLD
                        STOP_THRESHOLD = FIRST_LOSS_STOP_THRESHOLD
                        db_log_warning("first_loss_tighten", f"First trade was a loss — daily cap cut to {FIRST_LOSS_STOP_THRESHOLD}")
                        send_telegram(
                            f"🎯 <b>First trade of the day was a loss</b>\n\n"
                            f"Daily trade cap tightened from {old_threshold} to {FIRST_LOSS_STOP_THRESHOLD}.\n"
                            f"Rough start — trade fewer, trade better today."
                        )

            # ── Trade count alerts ────────────────────────────────────
            if count >= STOP_THRESHOLD and not stopped_today:
                send_telegram(
                    f"🚨 <b>{count} trades hit — STOP NOW.</b>\n\n"
                    f"Daily limit of {STOP_THRESHOLD} reached.\n"
                    f"🔇 All signal prompts and manual trades are now silenced until midnight.\n\n"
                    f"You're done for today. Walk away."
                )
                stopped_today = warned_today = early_warned_today = True
                rule = f"Exceeded daily trade limit ({count} trades vs max {STOP_THRESHOLD})"
                rules_broken_today[rule] = rules_broken_today.get(rule, 0) + 1

            elif count >= WARN_THRESHOLD and not warned_today:
                send_telegram(
                    f"⚠️ <b>{count} trades taken today</b> — approaching your limit.\n"
                    f"{STOP_THRESHOLD - count} trades left before the hard stop."
                )
                warned_today = early_warned_today = True

            elif count >= EARLY_THRESHOLD and not early_warned_today:
                send_telegram(
                    f"📊 <b>{count} trades taken today.</b>\n"
                    f"Halfway to your warning threshold — stay selective."
                )
                early_warned_today = True

            # ── Loss streak alert ─────────────────────────────────────
            streak = get_consecutive_losses(trades)
            if streak >= LOSS_STREAK_LIMIT and not loss_streak_alerted:
                db_log_warning("loss_streak", f"{streak} losses in a row")
                send_telegram(
                    f"🔴 <b>{streak} losses in a row.</b>\n"
                    f"Step back, review the setup — don't chase."
                )
                loss_streak_alerted = True
            elif streak < LOSS_STREAK_LIMIT and loss_streak_alerted:
                loss_streak_alerted = False

            # ── Win streak alert ──────────────────────────────────────
            win_streak = get_consecutive_wins(trades)
            WIN_STREAK_TRIGGER = 3
            if win_streak >= WIN_STREAK_TRIGGER and win_streak > win_streak_alerted_at:
                if win_streak == 3:
                    daily_streaks_3plus += 1
                    msg = (
                        f"🔥🎉🚀 <b>{win_streak} wins in a row!</b>\n"
                        f"You're locked in. Keep trusting the process — don't get cocky, "
                        f"just keep reading it right. 💪😤"
                    )
                elif win_streak == 4:
                    msg = (
                        f"🔥🔥🎯🏆 <b>{win_streak} wins straight!</b>\n"
                        f"This is what discipline looks like. Stay sharp, stay selective. 🙌"
                    )
                elif win_streak == 5:
                    msg = (
                        f"🚨 <b>STOP. 5 wins in a row.</b>\n\n"
                        f"The odds of winning 5 trades in a row at your win rate are extremely low. "
                        f"This is a statistical anomaly — not a sign to keep going.\n\n"
                        f"The market will revert. Overconfidence after a streak is one of the biggest "
                        f"account killers in trading.\n\n"
                        f"<b>Walk away. Lock in the day. Come back tomorrow.</b>"
                    )
                else:
                    msg = (
                        f"🚨 <b>{win_streak} in a row — seriously, stop now.</b>\n\n"
                        f"You are deep in anomaly territory. The longer this goes, "
                        f"the harder the snapback. Protect what you've made today."
                    )
                send_telegram(msg)
                win_streak_alerted_at = win_streak
            elif win_streak == 0:
                win_streak_alerted_at = 0

            # ── Balance / peak drawdown tracking ─────────────────────
            if snapshot_balance and snapshot_balance > 0:
                current_balance = cached_balance if cached_balance is not None else get_usdt_balance()
                print(f"[{now_str}] Balance: ${current_balance:,.2f}")

                if peak_balance_today is None or current_balance > peak_balance_today:
                    peak_balance_today = current_balance

                if peak_balance_today and peak_balance_today > 0:
                    drawdown_pct = ((peak_balance_today - current_balance) / peak_balance_today) * 100
                    print(f"[{now_str}] Drawdown from peak: {drawdown_pct:.1f}%")

                    if drawdown_pct >= PEAK_DRAWDOWN_PCT and not peak_drawdown_alerted:
                        send_telegram(
                            f"📉 <b>Drawdown alert — down {drawdown_pct:.1f}% from today's peak.</b>\n"
                            f"Peak: ${peak_balance_today:,.2f} → Now: ${current_balance:,.2f} USDT\n"
                            f"You were up and gave it back. Stop trading and protect what's left."
                        )
                        peak_drawdown_alerted = True

            # ── Fee tracker ───────────────────────────────────────────
            stats = build_stats(trades)
            fees_today = stats["total_fees"]
            print(f"[{now_str}] Fees today: ${fees_today:.2f}")

            if fees_today >= FEE_ALERT_THRESHOLD and not fee_alerted_today:
                db_log_warning("fee_threshold", f"${fees_today:.2f} paid in fees today — crossed ${FEE_ALERT_THRESHOLD:.0f} threshold")
                send_telegram(
                    f"💸 <b>Fees alert — ${fees_today:.2f} paid in fees today.</b>\n"
                    f"You've crossed the ${FEE_ALERT_THRESHOLD:.0f} threshold. "
                    f"Every extra trade makes this worse."
                )
                fee_alerted_today = True
                rule = f"Fee threshold breached (fees vs ${FEE_ALERT_THRESHOLD:.0f} limit)"
                rules_broken_today[rule] = rules_broken_today.get(rule, 0) + 1

            # ── Daily losses block — hard stop after DAILY_LOSS_STREAK_LIMIT losses ─
            # Restart-proof: checks the DATABASE for whether this alert was
            # already sent today, not just an in-memory flag — a flag resets
            # to False on every Railway restart, but a DB record survives,
            # so this can no longer re-spam the same alert after a crash.
            if stats["losses"] >= DAILY_LOSS_STREAK_LIMIT and not daily_losses_block:
                already_alerted_today = db_was_warning_logged_today("daily_loss_block")
                daily_losses_block = True  # block trading regardless of whether we re-alert
                if not already_alerted_today:
                    db_log_warning("daily_loss_block", f"{DAILY_LOSS_STREAK_LIMIT} losses today — blocked. Net P&L: {format_pnl(stats['net_pnl'])}")
                    send_telegram(
                        f"🚫 <b>{DAILY_LOSS_STREAK_LIMIT} losses today — trading blocked for the rest of the session.</b>\n\n"
                        f"You've had {DAILY_LOSS_STREAK_LIMIT} losing trades today. That's your limit.\n"
                        f"No more trades until midnight. Walk away.\n\n"
                        f"Losses today: {stats['losses']}  |  Net P&L: {format_pnl(stats['net_pnl'])}"
                    )
                    rule = f"Hit {DAILY_LOSS_STREAK_LIMIT} daily loss limit"
                    rules_broken_today[rule] = rules_broken_today.get(rule, 0) + 1


            if count > last_trade_count:
                closing_trades = [t for t in trades if float(t.get("realizedPnl", 0)) != 0]
                if len(closing_trades) >= 1:
                    last_close = closing_trades[-1]
                    last_pnl   = float(last_close["realizedPnl"])

                    if last_pnl < 0:
                        last_close_time_ms = int(last_close["time"])
                        revenge_window_ms  = REVENGE_WINDOW_MINS * 60 * 1000
                        cutoff_ms          = last_close_time_ms + revenge_window_ms

                        revenge_trades = [
                            t for t in trades
                            if int(t["time"]) > last_close_time_ms
                            and int(t["time"]) <= cutoff_ms
                            and float(t.get("realizedPnl", 0)) == 0
                        ]

                        for rt in revenge_trades:
                            trade_id = rt.get("orderId")
                            if trade_id and trade_id not in revenge_trade_ids_alerted:
                                # Restart-proof dedup: also check the DB in case
                                # a restart wiped the in-memory set — embeds the
                                # trade_id in the message so we can match on it.
                                already_alerted = db_was_message_logged_today("revenge_trade", str(trade_id))
                                revenge_trade_ids_alerted.add(trade_id)
                                if not already_alerted:
                                    db_log_warning("revenge_trade", f"Possible revenge trade within {REVENGE_WINDOW_MINS} mins of a loss (order {trade_id})")
                                    send_telegram(
                                        f"😤 <b>Possible revenge trade detected.</b>\n"
                                        f"You opened a new position within {REVENGE_WINDOW_MINS} minutes "
                                        f"of closing a losing trade.\n"
                                        f"Are you trading the setup or chasing the loss?"
                                    )

            # ── Same side consecutive losses ───────────────────────────
            same_side_count, same_side = get_same_side_consecutive_losses(trades)

            if same_side_count == 2 and not same_side_alerted:
                direction_key = "long" if same_side == "BUY" else "short"
                same_side_block[direction_key] = time.time() + 2700  # 45 minutes
                if same_side == "BUY":
                    msg = (
                        f"🛑 <b>2 losing longs in a row — longs blocked for 45 mins.</b>\n\n"
                        f"The market is not rewarding longs right now. Before you try again:\n\n"
                        f"• Wait for your long triggers to fully form\n"
                        f"• Wait for a key level to be swept and reclaimed\n"
                        f"• Go up a timeframe and reassess the trend\n\n"
                        f"Don't force it. Let the setup come to you."
                    )
                else:
                    msg = (
                        f"🛑 <b>2 losing shorts in a row — shorts blocked for 45 mins.</b>\n\n"
                        f"The downside momentum isn't there. Before you try again:\n\n"
                        f"• Wait for momentum to be exhausted\n"
                        f"• Go up a timeframe and reassess the trend\n"
                        f"• Wait for a higher key level to be swept\n\n"
                        f"Don't fight the move. Wait for clear exhaustion."
                    )
                send_telegram(msg)
                same_side_alerted = True
            elif same_side_count == 3 and not same_side_alerted:
                direction_key = "long" if same_side == "BUY" else "short"
                same_side_block[direction_key] = time.time() + 5400  # 90 minutes
                side_label_str = "longs" if same_side == "BUY" else "shorts"
                send_telegram(
                    f"🚨 <b>3 losing {side_label_str} in a row — blocked for 90 mins.</b>\n\n"
                    f"This direction is clearly not working today. Stop. Completely.\n\n"
                    f"Go up a timeframe. Reassess your bias. Do not override this block."
                )
                same_side_alerted = True
            elif same_side_count < SAME_SIDE_LIMIT and same_side_alerted:
                same_side_alerted = False

            # ── Avg gap overtrading warning ───────────────────────────
            try:
                day_positions = group_by_position(trades)
                if len(day_positions) >= 3:
                    sorted_pos  = sorted(day_positions, key=lambda p: int(p["time"]))
                    entry_times = [int(p["time"]) for p in sorted_pos]
                    gaps_ms     = [(entry_times[i] - entry_times[i-1]) for i in range(1, len(entry_times))]
                    avg_gap_ms  = sum(gaps_ms) / len(gaps_ms)
                    avg_gap_m   = avg_gap_ms / 60000
                    if avg_gap_m < OVERTRADE_GAP_MINS and not overtrade_alerted:
                        send_telegram(
                            f"<b>Overtrading warning</b>\n\n"
                            f"Your average gap between trades today is {avg_gap_m:.0f}m.\n"
                            f"Benchmark is {OVERTRADE_GAP_MINS}m minimum.\n\n"
                            f"You may be forcing trades. Slow down."
                        )
                        overtrade_alerted = True
                    elif avg_gap_m >= OVERTRADE_GAP_MINS and overtrade_alerted:
                        overtrade_alerted = False
            except Exception:
                pass

            last_trade_count = count

            # ── Cooldown window check ─────────────────────────────────
            if in_cooldown():
                window_start = now_uk.replace(
                    hour=COOLDOWN_START.hour, minute=COOLDOWN_START.minute,
                    second=0, microsecond=0)
                window_start_ms  = int(window_start.timestamp() * 1000)
                cooldown_orders  = [o for o in orders if int(o["time"]) >= window_start_ms]
                cooldown_count   = len(cooldown_orders)

                if cooldown_count > last_cooldown_warned_count:
                    send_telegram(
                        f"🚫 <b>Cooldown violation — {cooldown_count} trade(s) placed "
                        f"between {COOLDOWN_START.strftime('%H:%M')}–{COOLDOWN_END.strftime('%H:%M')} UK time.</b>\n"
                        f"You set this window as off-limits. Close the chart and step away."
                    )
                    last_cooldown_warned_count = cooldown_count
                    rule = "Traded during cooldown window"
                    rules_broken_today[rule] = rules_broken_today.get(rule, 0) + 1

            # ── 7am bias question / 8am morning brief ─────────────────
            if BIAS_FILTER_ENABLED and now_uk.hour == 7 and now_uk.minute < 10 and not bias_question_sent_today:
                send_bias_question()
                bias_question_sent_today = True

            if now_uk.hour == 8 and now_uk.minute < 10 and not morning_brief_sent_today:
                send_morning_brief()
                morning_brief_sent_today = True

            # ── Monday 8:10am weekly OKR reflection ──────────────────────
            if now_uk.weekday() == 0 and now_uk.hour == 8 and 10 <= now_uk.minute < 20 and not weekly_okr_sent_today:
                send_weekly_okr()
                weekly_okr_sent_today = True

            # ── 1st of month, 8:30am — monthly discipline review ─────────
            if now_uk.day == 1 and now_uk.hour == 8 and 30 <= now_uk.minute < 40 and not monthly_review_sent_today:
                send_monthly_discipline_review()
                monthly_review_sent_today = True

            # ── Sunday 8pm weekly digest ────────────────────────────────
            if now_uk.weekday() == 6 and now_uk.hour == 20 and now_uk.minute < 10 and not weekly_digest_sent_today:
                send_weekly_digest()
                weekly_digest_sent_today = True

            # ── 11:30pm client daily summary — loops every configured client ──
            is_1130 = now_uk.hour == 23 and now_uk.minute == 30
            if is_1130:
                for _c in CLIENTS:
                    if not _c.tg_token or _c.summary_sent_today:
                        continue
                    _c.summary_sent_today = True  # set BEFORE sending so exceptions can't re-trigger
                    try:
                        if _c.trades_today > 0:
                            wr        = (_c.wins_today / _c.trades_today * 100) if _c.trades_today else 0
                            wr_emoji  = "✅" if wr >= 50 else "📉"
                            notify_client(_c,
                                f"📊 <b>Today's Summary</b>\n\n"
                                f"Trades:    {_c.trades_today}\n"
                                f"Results:   {_c.wins_today}W / {_c.losses_today}L\n"
                                f"{wr_emoji} Win Rate:  {wr:.0f}%\n"
                            )
                        else:
                            notify_client(_c,
                                f"📊 <b>End of Day</b>\n\n"
                                f"No trades taken today — conditions weren't right.\n"
                                f"Patience is part of the edge. 🎯"
                            )
                        # Sunday weekly summary
                        if now_uk.weekday() == 6:
                            weekly = load_last_7_days_stats()
                            if weekly:
                                wr           = (_c.wins_today / _c.trades_today * 100) if _c.trades_today else 0
                                wr_emoji     = "✅" if wr >= 50 else "📉"
                                pnl_emoji    = "✅" if net_pnl >= 0 else "❌"
                                notify_client(_c,
                                    f"📈 <b>End of Week</b>\n\n"
                                    f"Today:     {_c.trades_today} trades  |  {_c.wins_today}W / {_c.losses_today}L\n"
                                    f"{wr_emoji} Win Rate:  {wr:.0f}%\n"
                                    f"{pnl_emoji} Today PnL: ${net_pnl:+.2f}"
                                )
                    except Exception as e:
                        print(f"[{_c.label()} Summary] Error: {e}")

            # ── Overnight position alerts (10:15 and 10:45pm) ─────────
            is_1015 = now_uk.hour == 22 and 15 <= now_uk.minute < 25
            is_1045 = now_uk.hour == 22 and 45 <= now_uk.minute < 55
            if (is_1015 and not overnight_alerted_1015) or (is_1045 and not overnight_alerted_1045):
                try:
                    pos = get_open_position()
                    if pos:
                        amt       = float(pos["positionAmt"])
                        direction = "LONG" if amt > 0 else "SHORT"
                        entry     = float(pos.get("entryPrice", 0))
                        unreal    = float(pos.get("unRealizedProfit", 0))
                        send_telegram(
                            f"🌙 <b>Overnight position warning</b>\n\n"
                            f"You still have a {direction} open at 10{'▪15' if is_1015 else '▪45'}pm.\n\n"
                            f"Entry:       ${entry:,.2f}\n"
                            f"Unrealised:  {format_pnl(unreal)}\n\n"
                            f"Are you holding this intentionally? "
                            f"Use /close if not."
                        )
                except Exception:
                    pass
                if is_1015:
                    overnight_alerted_1015 = True
                else:
                    overnight_alerted_1045 = True

            # ── Overnight auto-breakeven safety net (22:45) ────────────
            # If a position is still open and in profit at 22:45, move SL to
            # entry automatically — a safety net in case Nathan has fallen
            # asleep and forgotten to manage it. Fires once, only if genuinely
            # in profit (never moves SL to a worse position than it already
            # has, and never touches a losing position).
            is_2245_breakeven = now_uk.hour == 22 and 45 <= now_uk.minute < 55
            if is_2245_breakeven and not overnight_auto_breakeven_done:
                try:
                    pos = get_open_position()
                    if pos:
                        amt    = float(pos["positionAmt"])
                        entry  = float(pos.get("entryPrice", 0))
                        unreal = float(pos.get("unRealizedProfit", 0))
                        current_sl = current_trade_entry.get("sl_price")
                        # Only move if genuinely in profit AND the new SL would
                        # actually be an improvement on the current one (never
                        # loosen an already-better SL, e.g. one already past
                        # breakeven from a manual TP1 move).
                        already_better_or_equal = (
                            current_sl is not None and entry > 0 and (
                                (amt > 0 and current_sl >= entry) or
                                (amt < 0 and current_sl <= entry)
                            )
                        )
                        if unreal > 0 and entry > 0 and not already_better_or_equal:
                            sym_info_bo = get_symbol_info()
                            be_price = round_step(entry, sym_info_bo["price_tick"])
                            adjust_sl(be_price)
                            db_log_warning("overnight_auto_breakeven", f"Auto-moved SL to breakeven (${be_price:,.2f}) — position in profit ({format_pnl(unreal)}) at 22:45, likely asleep")
                            send_telegram(
                                f"😴 <b>Overnight safety net — SL auto-moved to breakeven</b>\n\n"
                                f"Position was in profit ({format_pnl(unreal)}) at 22:45 with no "
                                f"breakeven move made — assuming you're asleep.\n"
                                f"SL moved to entry (${be_price:,.2f}) so this can't turn into a loss overnight."
                            )
                except Exception as e:
                    print(f"[Overnight Breakeven] Error: {e}")
                overnight_auto_breakeven_done = True

            # ── Entry2 auto-cancel after 2 hours ──────────────────────
            if entry2_placed_at > 0 and (time.time() - entry2_placed_at) >= ENTRY2_EXPIRY_HOURS * 3600:
                try:
                    entry2_id = None
                    for oid, meta in list(tracked_orders.items()):
                        if meta["label"] == "Entry2":
                            entry2_id = int(oid)
                            break
                    if entry2_id:
                        now_ms  = int(time.time() * 1000)
                        params  = {"symbol": SYMBOL, "orderId": entry2_id, "timestamp": now_ms}
                        params  = sign_request(params)
                        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                        resp    = requests.delete(f"{FUTURES_BASE}/fapi/v1/order",
                                                  params=params, headers=headers, timeout=10)
                        if resp.status_code == 200:
                            tracked_orders.pop(str(entry2_id), None)
                            entry2_placed_at = 0.0
                            send_telegram(
                                f"⏱ <b>Entry2 cancelled — 2 hour expiry reached.</b>\n"
                                f"Price never pulled back to the limit. "
                                f"Position continues with original size."
                            )
                except Exception as e:
                    print(f"[Entry2 Expiry ERROR] {e}")

            # ── Software stop-loss check ──────────────────────────────
            shared_open_pos = None
            if EXECUTION_ENABLED:
                try:
                    shared_open_pos = get_open_position()
                except Exception as e:
                    print(f"[Position Fetch] Error: {e}")

            if EXECUTION_ENABLED and current_trade_entry.get("sl_price"):
                try:
                    sl_pos = shared_open_pos
                    if sl_pos:
                        mark      = get_mark_price()
                        sl_price  = current_trade_entry["sl_price"]
                        direction = current_trade_entry.get("direction")
                        hit = (direction == "long"  and mark <= sl_price) or \
                              (direction == "short" and mark >= sl_price)
                        if hit:
                            amt        = float(sl_pos["positionAmt"])
                            close_side = "SELL" if amt > 0 else "BUY"
                            quantity   = abs(amt)
                            place_order(close_side, "MARKET", quantity, reduce_only=True)
                            sl_loss = abs(sl_price - mark) * quantity
                            time_to_sl = (time.time() - trade_entry_time) / 60 if trade_entry_time > 0 else None
                            db_update_outcome(pending_trigger_id, "loss", -sl_loss,
                                              mae=trade_mae, mfe=trade_mfe,
                                              time_to_sl_mins=time_to_sl)
                            last_loss_close_time = time.time()
                            cancel_open_orders()
                            current_trade_entry.clear()
                            send_telegram(
                                f"🛑 <b>Stop loss hit — {direction.upper()} closed</b>\n\n"
                                f"SL price:  ${sl_price:,.2f}\n"
                                f"Exit:      ${mark:,.2f}\n\n"
                                f"Loss taken. Assess before the next trade.\n\n"
                                f"{after_trade_summary()}"
                            )
                            mirror_close_all()
                    else:
                        current_trade_entry.clear()
                except Exception as e:
                    print(f"[Software SL] Error: {e}")

            # ── Profit / breakeven alert + time tracking ──────────────
            if EXECUTION_ENABLED:
                try:
                    open_pos = shared_open_pos
                    if open_pos:
                        check_profit_alert(open_pos)
                        unreal = float(open_pos.get("unRealizedProfit", 0))
                        poll_mins = POLL_INTERVAL / 60
                        if unreal > 0:
                            position_mins_profit += poll_mins
                        elif unreal < 0:
                            position_mins_under  += poll_mins

                        # ── Underwater vs profit time ratio warning ──────
                        # Fires once per position if time spent underwater
                        # reaches 1.5x time spent in profit — a signal the
                        # trade is dragging rather than working, even if
                        # still open and not yet at SL. Requires the trade
                        # to have been open at least 30 minutes total before
                        # this can fire at all — otherwise it triggers on
                        # trades that are just naturally early/noisy.
                        trade_age_mins = ((time.time() - trade_entry_time) / 60) if trade_entry_time > 0 else 0
                        if trade_age_mins >= 30:
                            if not underwater_ratio_alerted and position_mins_profit > 0:
                                if position_mins_under >= position_mins_profit * UNDERWATER_RATIO_THRESHOLD:
                                    underwater_ratio_alerted = True
                                    db_log_warning("underwater_ratio", f"Underwater {position_mins_under:.0f}m vs {position_mins_profit:.0f}m in profit — {UNDERWATER_RATIO_THRESHOLD}x threshold hit")
                                    send_telegram(
                                        f"⏱️ <b>Spending more time underwater than in profit.</b>\n\n"
                                        f"In profit: {position_mins_profit:.0f}m  |  Underwater: {position_mins_under:.0f}m\n\n"
                                        f"This trade has been underwater for {UNDERWATER_RATIO_THRESHOLD}x longer than "
                                        f"it's been in profit. Worth reassessing rather than waiting it out."
                                    )
                            elif not underwater_ratio_alerted and position_mins_profit == 0 and position_mins_under > 0:
                                # Never been in profit at all, and already past the
                                # 30-min minimum — worth flagging.
                                underwater_ratio_alerted = True
                                db_log_warning("underwater_ratio", f"Underwater {position_mins_under:.0f}m with zero time in profit")
                                send_telegram(
                                    f"⏱️ <b>This trade has never been in profit — {position_mins_under:.0f}m underwater.</b>\n\n"
                                    f"Worth reassessing rather than waiting it out."
                                )

                        # ── Track MFE/MAE ─────────────────────────────
                        ep   = float(open_pos.get("entryPrice", 0))
                        mark = get_mark_price()
                        amt  = float(open_pos.get("positionAmt", 0))
                        if ep > 0 and mark > 0:
                            if amt > 0:   # long
                                excursion = (mark - ep) / ep * 100
                            else:         # short
                                excursion = (ep - mark) / ep * 100
                            if excursion > trade_mfe:
                                trade_mfe = excursion
                            if excursion < trade_mae:
                                trade_mae = excursion

                            # ── Retracement protection: lock in real profit ──
                            # If trade reached RETRACE_PROTECT_PCT profit but has since
                            # pulled back down to only RETRACE_LOCK_IN_PCT still held,
                            # move SL to lock in that remaining profit — not just
                            # breakeven, an actual small win rather than a scratch.
                            if (trade_mfe >= RETRACE_PROTECT_PCT
                                    and excursion <= RETRACE_LOCK_IN_PCT
                                    and not retrace_protect_triggered
                                    and current_trade_entry.get("sl_price")):
                                sym_info_rp = get_symbol_info()
                                tick_rp     = sym_info_rp["price_tick"]
                                if amt > 0:  # long
                                    lock_price_rp = round_step(ep * (1 + RETRACE_LOCK_IN_PCT / 100), tick_rp)
                                else:        # short
                                    lock_price_rp = round_step(ep * (1 - RETRACE_LOCK_IN_PCT / 100), tick_rp)
                                try:
                                    adjust_sl(lock_price_rp)
                                    current_trade_entry["sl_price"] = lock_price_rp
                                    retrace_protect_triggered = True
                                    db_log_warning("retrace_protect", f"SL moved to lock in +{RETRACE_LOCK_IN_PCT:.2f}% — reached +{trade_mfe:.2f}%, pulled back to {excursion:+.2f}%")
                                    send_telegram(
                                        f"🛡️ <b>Retracement protection — profit locked in</b>\n\n"
                                        f"Trade reached +{trade_mfe:.2f}% profit (above your "
                                        f"{RETRACE_PROTECT_PCT:.2f}% threshold) and has pulled back "
                                        f"to {excursion:+.2f}%.\n\n"
                                        f"SL moved to ${lock_price_rp:,.2f} — locks in +{RETRACE_LOCK_IN_PCT:.2f}% "
                                        f"minimum from here, worst case is a real (small) win, not just a scratch."
                                    )
                                except Exception as e:
                                    print(f"[Retrace Protect] SL move error: {e}")
                    else:
                        breakeven_suggested       = False
                        breakeven_last_suggested_at = 0.0
                        position_mins_profit      = 0.0
                        position_mins_under       = 0.0
                        underwater_ratio_alerted  = False
                        retrace_protect_triggered = False
                except Exception:
                    pass

            if snapshot_balance:
                current_pnl = build_stats(trades)["net_pnl"]

                if current_pnl >= DAILY_PNL_TARGET and not daily_target_alerted:
                    send_telegram(
                        f"🎯 <b>Daily target hit — {format_pnl(current_pnl)} net P&L</b>\n"
                        f"You've cleared your ${DAILY_PNL_TARGET:.0f} daily target. "
                        f"Lock it in or keep pushing."
                    )
                    daily_target_alerted = True
                    if not dayscore_ticked_today:
                        if dayscore_tick_profit_day():
                            dayscore_ticked_today = True
                elif current_pnl < DAILY_PNL_TARGET and daily_target_alerted:
                    daily_target_alerted = False

                # ── Profit lock — re-arms every time another full target's
                # worth of NEW profit accumulates, not just once per day ──
                # Cooldown added: if a transfer attempt fails (e.g. Binance
                # API permission issue), don't retry/re-alert every single
                # poll cycle — wait PROFIT_LOCK_RETRY_COOLDOWN_SEC between
                # attempts, whether the last one succeeded or failed.
                unlocked_gain = current_pnl - profit_locked_today
                if (PROFIT_LOCK_ENABLED and unlocked_gain >= DAILY_PNL_TARGET
                        and (time.time() - profit_lock_last_attempt_at) >= PROFIT_LOCK_RETRY_COOLDOWN_SEC):
                    profit_lock_last_attempt_at = time.time()
                    try:
                        available = get_usdt_available_balance()
                        transfer_amount = round(min(unlocked_gain, available), 2)
                        if transfer_amount <= 0:
                            send_telegram(
                                f"🔒 <b>Profit lock — nothing to move</b>\n\n"
                                f"${unlocked_gain:.2f} unlocked profit available but Futures "
                                f"available balance is ${available:.2f}. Skipping transfer."
                            )
                        else:
                            result = binance_transfer_futures_to_spot(transfer_amount)
                            profit_locked_today += transfer_amount
                            capped_note = (
                                f"\n⚠️ Capped to available balance (${available:.2f}) — "
                                f"the rest will carry forward and lock next time it's freed up "
                                f"or profit grows further."
                                if transfer_amount < unlocked_gain else ""
                            )
                            send_telegram(
                                f"🔒 <b>Profit locked — ${transfer_amount:.2f} moved to Spot</b>\n\n"
                                f"Daily P&L: {format_pnl(current_pnl)}\n"
                                f"Locked today (total): ${profit_locked_today:.2f}"
                                f"{capped_note}\n\n"
                                f"Walk away and protect it. 💰"
                            )
                    except Exception as e:
                        send_telegram(
                            f"⚠️ <b>Profit lock transfer FAILED</b>\n\n"
                            f"${unlocked_gain:.2f} unlocked profit but the automatic "
                            f"transfer to Spot did not go through:\n{e}\n\n"
                            f"Move it manually if you want it protected."
                        )
                        print(f"[Profit Lock] Transfer error: {e}")

                loss_warn_threshold = -(DAILY_LOSS_LIMIT * DAILY_LOSS_WARN_PCT / 100)
                if current_pnl <= -DAILY_LOSS_LIMIT and not daily_loss_stop_alerted:
                    send_telegram(
                        f"🚫 <b>DAILY LOSS LIMIT HIT — ${abs(current_pnl):,.2f} down</b>\n\n"
                        f"You have lost ${DAILY_LOSS_LIMIT:.0f} today. That is your hard limit.\n\n"
                        f"<b>Stop trading. Now.</b>\n\n"
                        f"🔇 All signal prompts and manual trades are now silenced until midnight.\n\n"
                        f"This is not a suggestion — you set this rule yourself. "
                        f"Close everything, step away, and come back tomorrow. 🛑"
                    )
                    daily_loss_stop_alerted = True
                    stopped_today = True
                    db_log_warning("daily_loss_limit", f"${abs(current_pnl):.2f} lost today — hit ${DAILY_LOSS_LIMIT:.0f} hard limit, all trading blocked")
                    rule = f"Hit ${DAILY_LOSS_LIMIT:.0f} daily loss limit"
                    rules_broken_today[rule] = rules_broken_today.get(rule, 0) + 1

                elif current_pnl <= loss_warn_threshold and not daily_loss_warn_alerted:
                    remaining = DAILY_LOSS_LIMIT - abs(current_pnl)
                    send_telegram(
                        f"⚠️ <b>Approaching daily loss limit — ${abs(current_pnl):,.2f} down</b>\n\n"
                        f"You're {DAILY_LOSS_WARN_PCT:.0f}% of the way to your ${DAILY_LOSS_LIMIT:.0f} max loss.\n"
                        f"${remaining:,.2f} left before the hard stop.\n\n"
                        f"Be very selective. Every trade now carries more weight."
                    )
                    daily_loss_warn_alerted = True

                if not weekly_target_alerted:
                    weekly = load_last_7_days_stats()
                    week_pnl = sum(d["net_pnl"] for d in weekly) + current_pnl
                    if week_pnl >= WEEKLY_PNL_TARGET:
                        send_telegram(
                            f"🏅 <b>Weekly target hit — ${week_pnl:,.2f} net P&L this week</b>\n"
                            f"You've cleared your ${WEEKLY_PNL_TARGET:.0f} weekly target. "
                            f"Solid week."
                        )
                        weekly_target_alerted = True

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Request: {e}")
        except Exception as e:
            print(f"[ERROR] {e}")

        # ── Slower periodic checks (WPR, aggressive volume, virtual ──────
        # trigger resolution) — these don't need the fast SL/TP polling
        # cadence, so they run on the same SLOW_REFRESH_INTERVAL (60s
        # default) as trade history/balance, not every fast tick.
        now_for_slow_checks = time.time()
        if (now_for_slow_checks - _last_slow_checks_run) >= SLOW_REFRESH_INTERVAL:
            _last_slow_checks_run = now_for_slow_checks

            try:
                check_virtual_trigger_trades()
            except Exception as e:
                print(f"[ERROR] Virtual trigger check: {e}")

            try:
                # `trades` may not exist this cycle if the main try block above
                # failed (e.g. a Binance rate-limit ban) before it was assigned —
                # this check avoids a crash in that case, letting the check
                # just skip this cycle rather than raising UnboundLocalError.
                check_time_in_trade_warning(trades if 'trades' in locals() else None)
            except Exception as e:
                print(f"[ERROR] Time in trade warning check: {e}")

            try:
                check_wpr_short_cross()
            except Exception as e:
                print(f"[ERROR] WPR short cross check: {e}")

            try:
                check_wpr_long_cross()
            except Exception as e:
                print(f"[ERROR] WPR long cross check: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
