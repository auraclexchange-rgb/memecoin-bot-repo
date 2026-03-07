"""
persistence.py — SQLite persistence for AURACLE_XBOT
=====================================================
Stores all user data and trade logs in a local SQLite DB.
On Railway: add a Volume mounted at /data so the DB survives redeploys.
  Railway → your bot service → Volumes → Mount Path: /data

Usage in sol_trading_bot.py:
  from persistence import load_all, save_user, save_trade_log, DB_PATH
  # At bot startup (inside main, before app.run_polling):
  load_all(users, trade_log)
  # After every trade / setting change:
  save_user(uid, users[uid])
  # After a trade is closed:
  save_trade_log(uid, trade_log[uid])
"""

import sqlite3
import json
import logging
import os
from datetime import datetime, date

logger = logging.getLogger(__name__)

# ── DB location ───────────────────────────────────────────────────────────────
# /data is a Railway persistent Volume. Falls back to local dir for dev.
_DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(_DATA_DIR, "auracle_bot.db")


# ── JSON serialiser that handles datetime / date / set objects ────────────────
class _BotEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, date):
            return {"__date__": obj.isoformat()}
        if isinstance(obj, set):
            return {"__set__": list(obj)}
        return super().default(obj)


def _bot_decoder(d: dict):
    if "__datetime__" in d:
        return datetime.fromisoformat(d["__datetime__"])
    if "__date__" in d:
        return date.fromisoformat(d["__date__"])
    if "__set__" in d:
        return set(d["__set__"])
    return d


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_BotEncoder)


def _loads(s: str):
    return json.loads(s, object_hook=_bot_decoder)


# ── Schema ────────────────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    uid     INTEGER PRIMARY KEY,
    data    TEXT    NOT NULL,
    updated REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS trade_log (
    uid     INTEGER NOT NULL,
    data    TEXT    NOT NULL,
    updated REAL    NOT NULL,
    PRIMARY KEY (uid)
);
"""


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist. Call once at startup."""
    try:
        with _get_conn() as conn:
            conn.executescript(_SCHEMA)
        logger.info(f"DB ready at {DB_PATH}")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise


# ── Load all users + trade logs into memory dicts at startup ──────────────────
def load_all(users: dict, trade_log: dict):
    """
    Populate in-memory `users` and `trade_log` dicts from SQLite.
    Call once before starting the bot.
    """
    init_db()
    loaded_users = 0
    try:
        with _get_conn() as conn:
            for row in conn.execute("SELECT uid, data FROM users"):
                try:
                    users[row["uid"]] = _loads(row["data"])
                    loaded_users += 1
                except Exception as e:
                    logger.error(f"Failed to load user {row['uid']}: {e}")

            for row in conn.execute("SELECT uid, data FROM trade_log"):
                try:
                    trade_log[row["uid"]] = _loads(row["data"])
                except Exception as e:
                    logger.error(f"Failed to load trade_log {row['uid']}: {e}")

        logger.info(f"Loaded {loaded_users} users from DB")
    except Exception as e:
        logger.error(f"load_all failed: {e}")


# ── Save a single user ────────────────────────────────────────────────────────
def save_user(uid: int, ud: dict):
    """
    Persist one user's data dict to SQLite.
    Call after every balance change, trade, or settings update.
    """
    import time
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO users (uid, data, updated) VALUES (?, ?, ?)",
                (uid, _dumps(ud), time.time())
            )
    except Exception as e:
        logger.error(f"save_user({uid}) failed: {e}")


# ── Save a user's full trade log ──────────────────────────────────────────────
def save_trade_log(uid: int, log: list):
    """
    Persist one user's trade log list to SQLite.
    Call after a position is fully closed.
    """
    import time
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO trade_log (uid, data, updated) VALUES (?, ?, ?)",
                (uid, _dumps(log), time.time())
            )
    except Exception as e:
        logger.error(f"save_trade_log({uid}) failed: {e}")


# ── Convenience: save both at once ───────────────────────────────────────────
def save_all(uid: int, ud: dict, log: list):
    save_user(uid, ud)
    save_trade_log(uid, log)


# ── Periodic autosave (belt-and-suspenders) ───────────────────────────────────
async def autosave_job(context):
    """
    Register with app.job_queue.run_repeating(autosave_job, interval=120)
    Saves every user to DB every 2 minutes as a safety net.
    """
    from persistence import save_user, save_trade_log
    # These are imported at call time to avoid circular imports
    import sol_trading_bot as _bot
    saved = 0
    for uid, ud in list(_bot.users.items()):
        try:
            save_user(uid, ud)
            if uid in _bot.trade_log:
                save_trade_log(uid, _bot.trade_log[uid])
            saved += 1
        except Exception as e:
            logger.error(f"autosave failed for uid {uid}: {e}")
    logger.debug(f"Autosave: {saved} users persisted")
