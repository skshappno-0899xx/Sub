# auth_layer.py
# -*- coding: utf-8 -*-

import os
import json
import time
import asyncio
import contextvars
import httpx
from typing import Dict, Any, Optional
from aiohttp import web

import dashboard_server as ds

bot_state = ds.bot_state

# ==================== TEMPLATE LOADER ====================
_TEMPLATE_CACHE: Dict[str, str] = {}


def _load_template(name: str) -> str:
    if name in _TEMPLATE_CACHE:
        return _TEMPLATE_CACHE[name]
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, name)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        _TEMPLATE_CACHE[name] = content
        return content
    except Exception as e:
        print(f"[auth] Template load error {name}: {e}", flush=True)
        return f"<h1>Template {name} not found</h1>"


DASHBOARD_HTML = _load_template("dashboard.html")
# ==================== END TEMPLATE LOADER ====================


DATA_DIR = "data"
NOTIF_FILE = os.path.join(DATA_DIR, "notification.json")
AUTO_SETTINGS_FILE = os.path.join(DATA_DIR, "auto_settings.json")
LOCK_FILE = os.path.join(DATA_DIR, "lock_state.json")
USER_ACCOUNTS_FILE = os.path.join(DATA_DIR, "user_accounts.json")
DELETED_HISTORY_FILE = os.path.join(DATA_DIR, "deleted_history.json")

SESSION_COOKIE = "vxl_session"
SESSION_TTL = 30 * 24 * 3600

USER_WORKERS: Dict[str, Dict[str, asyncio.Task]] = {}
USER_PAUSED: Dict[str, set] = {}
USER_DELETED: Dict[str, set] = {}
_USER_ACCOUNT_TASKS: Dict[str, set] = {}
_LAST_SYNC: Dict[str, float] = {}
_CURRENT_OWNER = contextvars.ContextVar("current_owner", default=None)

BR_API_BASE = "http://127.0.0.1:9999"
BR_START_PATH = "/BattelRoyal"
BR_STOP_PATH = "/stop"
BR_STATUS_PATH = "/status"

_mode_registry: Dict[str, str] = {}
_mode_lock = asyncio.Lock()


async def _set_mode(uid: str, mode: str):
    async with _mode_lock:
        _mode_registry[str(uid)] = mode


async def _get_mode(uid: str) -> Optional[str]:
    async with _mode_lock:
        return _mode_registry.get(str(uid))


async def _clear_mode(uid: str):
    async with _mode_lock:
        _mode_registry.pop(str(uid), None)


# ==================== LOCK STATE ====================
def load_lock_state() -> Dict[str, Any]:
    if not os.path.exists(LOCK_FILE):
        return {"locked": False}
    try:
        with open(LOCK_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return {"locked": False}


def save_lock_state(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = LOCK_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, LOCK_FILE)


def is_website_locked() -> bool:
    return bool(load_lock_state().get("locked", False))


# ==================== NOTIFICATION ====================
def load_notification() -> Dict[str, Any]:
    if not os.path.exists(NOTIF_FILE):
        return {"text": "IND server is disabled — please wait patiently!"}
    try:
        with open(NOTIF_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return {"text": "IND server is disabled — please wait patiently!"}


def save_notification(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = NOTIF_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, NOTIF_FILE)


# ==================== AUTO SETTINGS ====================
# Structure:
# {
#   "auto_delete_enabled": bool,
#   "auto_delete_target_level": int,
#   "lw_limit_enabled": bool,
#   "lw_limit": int,
#   "br_limit_enabled": bool,
#   "br_limit": int,
# }
def _load_auto_settings() -> Dict[str, Any]:
    try:
        if os.path.exists(AUTO_SETTINGS_FILE):
            with open(AUTO_SETTINGS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                if isinstance(d, dict):
                    return d
    except Exception:
        pass
    return {}


def _save_auto_settings(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = AUTO_SETTINGS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, AUTO_SETTINGS_FILE)


def _get_auto_settings() -> Dict[str, Any]:
    all_s = _load_auto_settings()
    return {
        "auto_delete_enabled": bool(all_s.get("auto_delete_enabled", False)),
        "auto_delete_target_level": int(all_s.get("auto_delete_target_level", 22) or 22),
        "lw_limit_enabled": bool(all_s.get("lw_limit_enabled", False)),
        "lw_limit": int(all_s.get("lw_limit", 3) or 3),
        "br_limit_enabled": bool(all_s.get("br_limit_enabled", False)),
        "br_limit": int(all_s.get("br_limit", 3) or 3),
    }


def _set_auto_settings(cfg: Dict[str, Any]):
    _save_auto_settings(cfg)


# ==================== USER ACCOUNTS DB ====================
def _load_user_accounts() -> Dict[str, Any]:
    try:
        if os.path.exists(USER_ACCOUNTS_FILE):
            with open(USER_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                if isinstance(d, dict):
                    return d
    except Exception:
        pass
    return {}


def _save_user_accounts(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = USER_ACCOUNTS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, USER_ACCOUNTS_FILE)


def _add_user_account(uid: str, password: str, level: int = 1, name: str = "", region: str = "", mode: str = "LW"):
    data = _load_user_accounts()
    data[str(uid)] = {
        "uid": str(uid),
        "password": password,
        "level": int(level),
        "name": name,
        "region": region,
        "mode": mode,
        "added_at": time.time(),
    }
    _save_user_accounts(data)


def _remove_user_account(uid: str):
    data = _load_user_accounts()
    if str(uid) in data:
        del data[str(uid)]
        _save_user_accounts(data)


def _update_user_account(uid: str, **kwargs):
    data = _load_user_accounts()
    if str(uid) in data:
        data[str(uid)].update(kwargs)
        _save_user_accounts(data)


# ==================== DELETED HISTORY ====================
# Structure: {uid: deleted_timestamp}
# এটা দিয়ে track করা হবে কে delete হয়েছে।
# তবে save থাকবে user_accounts.json এ UID+Password.
def _load_deleted_history() -> Dict[str, Any]:
    try:
        if os.path.exists(DELETED_HISTORY_FILE):
            with open(DELETED_HISTORY_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                if isinstance(d, dict):
                    return d
    except Exception:
        pass
    return {}


def _save_deleted_history(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = DELETED_HISTORY_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DELETED_HISTORY_FILE)


def _mark_deleted(uid: str):
    data = _load_deleted_history()
    data[str(uid)] = time.time()
    _save_deleted_history(data)


def _unmark_deleted(uid: str):
    data = _load_deleted_history()
    if str(uid) in data:
        del data[str(uid)]
        _save_deleted_history(data)


def _is_deleted_history(uid: str) -> bool:
    data = _load_deleted_history()
    return str(uid) in data


# ==================== BR API ====================
async def _br_request(path: str, uid: str = "", pw: str = "", accs: str = "", timeout: float = 30.0) -> Dict[str, Any]:
    params = {}
    if accs:
        params["accs"] = accs
    else:
        params["uid"] = uid
        params["pw"] = pw
    try:
        async with httpx.AsyncClient(timeout=timeout) as c:
            r = await c.get(BR_API_BASE + path, params=params)
            try:
                return r.json()
            except Exception:
                return {"Sorry": "Invalid JSON response", "raw": r.text[:200]}
    except Exception as e:
        return {"Sorry": "API Unreachable", "error": str(e)}


async def _br_status_ping() -> Dict[str, Any]:
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(BR_API_BASE + "/")
            latency = int((time.time() - start) * 1000)
            if r.status_code == 200:
                return {"ok": True, "latency": latency}
            return {"ok": False, "latency": latency}
    except Exception:
        return {"ok": False, "latency": 0}
        

# ==================== PAGE HANDLERS ====================
async def h_root(req):
    """Root -> if locked show lock page, else redirect to dashboard."""
    if is_website_locked():
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")
    raise web.HTTPFound("/dashboard")


async def h_lock_page(req):
    """Secret lock control page."""
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def h_dash(req):
    if is_website_locked():
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def h_logout(req):
    r = web.HTTPFound("/")
    r.del_cookie(SESSION_COOKIE, path="/")
    raise r


# ==================== STATS ====================
async def h_stats(req):
    un = "user"
    now = time.time()
    paused = USER_PAUSED.get(un, set())
    deleted = USER_DELETED.get(un, set())

    out = []
    seen_keys = set()

    # Live accounts from bot_state
    for uid, a in list(ds.bot_state.accounts.items()):
        if uid in deleted:
            continue
        is_paused = uid in paused
        status = "PAUSED" if is_paused else a.get("status", "LIVE")
        mode = await _get_mode(uid) or "LW"
        out.append({
            "uid": uid,
            "nickname": a.get("nickname", "?"),
            "region": a.get("region", "IND"),
            "level": a.get("level", 1),
            "initial_exp": a.get("initial_exp", 0),
            "current_exp": a.get("current_exp", 0),
            "gained_exp": a.get("gained_exp", 0),
            "matches_played": a.get("matches_played", 0),
            "status": status,
            "active_matches": a.get("active_matches", 0),
            "created_at": a.get("created_at"),
            "last_update": a.get("last_update"),
            "mode": mode,
        })
        seen_keys.add(str(uid))
        try:
            cred = ds.bot_state.account_credentials.get(str(uid))
            if cred:
                if cred.get("account_id"):
                    seen_keys.add(str(cred.get("account_id")))
                if cred.get("auth_uid"):
                    seen_keys.add(str(cred.get("auth_uid")))
                if cred.get("auth_token"):
                    seen_keys.add(str(cred.get("auth_token"))[:20])
        except Exception:
            pass

    # Saved accounts (not live, not deleted)
    user_accs = _load_user_accounts()
    for uid_key, acc in user_accs.items():
        if uid_key in seen_keys or uid_key in deleted:
            continue
        matched_live = False
        for bid, acc_live in ds.bot_state.accounts.items():
            cred = ds.bot_state.account_credentials.get(str(bid))
            if cred and str(cred.get("auth_uid", "")) == uid_key:
                matched_live = True
                break
        if matched_live:
            continue
        is_paused = uid_key in paused
        out.append({
            "uid": uid_key,
            "nickname": acc.get("name", "Loading..."),
            "region": acc.get("region", "IND"),
            "level": acc.get("level", 1),
            "initial_exp": 0,
            "current_exp": 0,
            "gained_exp": 0,
            "matches_played": 0,
            "status": "PAUSED" if is_paused else "CONNECTING",
            "active_matches": 0,
            "created_at": acc.get("added_at"),
            "last_update": acc.get("added_at"),
            "mode": acc.get("mode", "LW"),
        })

    out.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)

    total_running = sum(
        1 for a in out
        if a.get("status") not in ("PAUSED", "OFFLINE", "CONNECTING", "ERROR")
    )
    total_exp = sum(a.get("gained_exp", 0) for a in out)
    total_accounts = len(out)

    return web.json_response({
        "username": un,
        "total_running": total_running,
        "total_exp": total_exp,
        "total_accounts": total_accounts,
        "is_locked": is_website_locked(),
        "accounts": out,
    })


# ==================== BR API HANDLERS ====================
async def h_api_br_status(req):
    st = await _br_status_ping()
    return web.json_response(st)


# ==================== ACCOUNT HANDLERS ====================
async def _purge_account_everywhere(uid_or_account_id: str):
    """
    Fully purge an account from:
      - bot_state.accounts
      - bot_state.account_credentials
      - bot_state.account_workers
      - _mode_registry
      - USER_WORKERS (cancel worker)
      - cache
    """
    un = "user"
    keys_to_kill = set()

    uid = str(uid_or_account_id).strip()
    if not uid:
        return

    keys_to_kill.add(uid)

    # Resolve all aliases
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("account_id"):
                keys_to_kill.add(str(cred.get("account_id")))
            if cred.get("auth_uid"):
                keys_to_kill.add(str(cred.get("auth_uid")))
            if cred.get("auth_token"):
                keys_to_kill.add(str(cred.get("auth_token"))[:20])
    except Exception:
        pass

    # Also check if uid is an account_id
    try:
        for k, cred in list(ds.bot_state.account_credentials.items()):
            if str(cred.get("account_id", "")) == uid:
                keys_to_kill.add(str(k))
                if cred.get("auth_uid"):
                    keys_to_kill.add(str(cred.get("auth_uid")))
                if cred.get("auth_token"):
                    keys_to_kill.add(str(cred.get("auth_token"))[:20])
    except Exception:
        pass

    # Check saved accounts
    try:
        accs = _load_user_accounts()
        for saved_uid, saved in accs.items():
            if saved_uid == uid:
                if saved.get("uid"):
                    keys_to_kill.add(str(saved.get("uid")))
    except Exception:
        pass

    print(f"[PURGE] Killing keys: {keys_to_kill}", flush=True)

    # 1. Cancel workers
    workers = USER_WORKERS.get(un, {})
    for k in list(keys_to_kill):
        t = workers.pop(k, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=3)
            except Exception:
                pass

    # 2. Cancel from bot_state.account_workers
    for key in list(ds.bot_state.account_workers.keys()):
        suffix = str(key).split("::", 1)[-1]
        if suffix in keys_to_kill:
            t = ds.bot_state.account_workers.pop(key, None)
            if t and not t.done():
                t.cancel()

    # 3. Clear bot_state.accounts
    for k in keys_to_kill:
        ds.bot_state.accounts.pop(k, None)
        ds.bot_state.account_credentials.pop(k, None)
        await _clear_mode(k)
        try:
            ds.bot_state.close_writers_for_account(k)
        except Exception:
            pass
        try:
            ds.cache_invalidate(k)
        except Exception:
            pass
        try:
            ds.cache_invalidate(f"tok_{k}")
        except Exception:
            pass

    # 4. Add to USER_DELETED
    USER_DELETED.setdefault(un, set()).update(keys_to_kill)


async def _clear_saved_and_mark_deleted(uid: str):
    """
    Keep UID+Password in user_accounts.json.
    Mark as deleted so it won't auto-restart.
    """
    un = "user"
    uid = str(uid)

    # Mark in USER_DELETED
    USER_DELETED.setdefault(un, set()).add(uid)

    # Save to deleted_history
    _mark_deleted(uid)

    # Reset saved account live data (keep uid + password)
    data = _load_user_accounts()
    if uid in data:
        entry = data[uid]
        # Clear live data
        entry["level"] = 0
        entry["name"] = ""
        entry["region"] = ""
        entry["gained_exp"] = 0
        entry["last_exp"] = 0
        entry["last_level"] = 0
        entry["last_nickname"] = ""
        entry["last_region"] = ""
        entry["matches_played"] = 0
        entry["created_at"] = None
        entry["last_update"] = None
        _save_user_accounts(data)


async def h_acc_add_job(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    if "uid" in d and "password" in d:
        uid = str(d["uid"]).strip()
        pw = str(d["password"]).strip()
        if not uid or not pw:
            return web.json_response({"status": "error", "error": "UID And Password Required"})

        # 🔥 IMPORTANT: Remove any deletion mark → allows re-add
        _unmark_deleted(uid)
        un = "user"
        USER_DELETED.setdefault(un, set()).discard(uid)
        USER_PAUSED.setdefault(un, set()).discard(uid)

        # Purge any existing session/stale data for this uid
        await _purge_account_everywhere(uid)

        # Login fresh
        cached = m.cache_get(uid)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_uid_pass(uid, pw)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid UID/Password"})

        if not acc_data:
            return web.json_response({"status": "error", "error": "Login Failed"})

        lvl = int(acc_data.get("level", 1) or 1)
        mode = "LW" if lvl >= 3 else "BR"

        _add_user_account(uid, pw, level=lvl,
                          name=acc_data.get("nickname", ""),
                          region=acc_data.get("region", ""),
                          mode=mode)

        acc_id_str = str(acc_data.get("account_id", uid))
        await _set_mode(acc_id_str, mode)
        USER_DELETED.setdefault(un, set()).discard(acc_id_str)
        USER_PAUSED.setdefault(un, set()).discard(acc_id_str)

        asyncio.create_task(start_user_workers(un))

        return web.json_response({
            "status": "ok",
            "mode": mode,
            "nickname": str(acc_data.get("nickname", "") or ""),
            "level": lvl,
            "region": str(acc_data.get("region", "") or "").upper(),
            "account_id": acc_id_str,
        })
    return web.json_response({"status": "error", "error": "Invalid Payload"})
    

async def h_acc_bulk_add(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    accounts_list = d.get("accounts", [])
    if not isinstance(accounts_list, list):
        return web.json_response({"status": "error", "error": "Invalid accounts list"})

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    added = 0
    failed = 0
    un = "user"

    for item in accounts_list:
        try:
            uid = str(item.get("uid", "")).strip()
            pw = str(item.get("password", "")).strip()
            if not uid or not pw:
                failed += 1
                continue

            # 🔥 IMPORTANT: Remove any deletion mark → allows re-add
            _unmark_deleted(uid)
            USER_DELETED.setdefault(un, set()).discard(uid)
            USER_PAUSED.setdefault(un, set()).discard(uid)

            # Purge any stale data
            await _purge_account_everywhere(uid)

            # Fresh login
            cached = m.cache_get(uid)
            if cached and cached.get("level"):
                acc_data = cached
            else:
                acc_data = await m.process_account_uid_pass(uid, pw)

            if not acc_data:
                failed += 1
                continue

            lvl = int(acc_data.get("level", 1) or 1)
            mode = "LW" if lvl >= 3 else "BR"

            _add_user_account(uid, pw, level=lvl,
                              name=acc_data.get("nickname", ""),
                              region=acc_data.get("region", ""),
                              mode=mode)

            acc_id_str = str(acc_data.get("account_id", uid))
            await _set_mode(acc_id_str, mode)
            USER_DELETED.setdefault(un, set()).discard(acc_id_str)
            USER_PAUSED.setdefault(un, set()).discard(acc_id_str)
            added += 1
        except Exception:
            failed += 1

    if added > 0:
        asyncio.create_task(start_user_workers(un))

    return web.json_response({
        "status": "ok",
        "added": added,
        "failed": failed,
    })


async def h_acc_delete(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[DELETE] user={uid}", flush=True)

    un = "user"

    # Resolve real IDs first (before purge)
    real_uid = None
    real_acc_id = None
    real_tok_pfx = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            real_uid = str(cred.get("auth_uid", "") or "")
            real_acc_id = str(cred.get("account_id", "") or "")
            real_tok_pfx = str(cred.get("auth_token", "") or "")[:20]
    except Exception:
        pass

    # Build keys_to_kill
    keys_to_kill = {uid}
    if real_uid:
        keys_to_kill.add(real_uid)
    if real_acc_id:
        keys_to_kill.add(real_acc_id)
    if real_tok_pfx:
        keys_to_kill.add(real_tok_pfx)

    # Also check if uid matches a saved account
    try:
        saved_accs = _load_user_accounts()
        for saved_uid in list(saved_accs.keys()):
            if saved_uid == uid:
                keys_to_kill.add(saved_uid)
    except Exception:
        pass

    # 1. FULL PURGE from live
    await _purge_account_everywhere(uid)
    for k in keys_to_kill:
        await _purge_account_everywhere(k)

    # 2. KEEP uid+password in user_accounts.json, but reset live fields
    try:
        data = _load_user_accounts()
        if uid in data:
            entry = data[uid]
            entry["level"] = 0
            entry["name"] = ""
            entry["region"] = ""
            entry["gained_exp"] = 0
            entry["last_exp"] = 0
            entry["last_level"] = 0
            entry["last_nickname"] = ""
            entry["last_region"] = ""
            entry["matches_played"] = 0
            entry["created_at"] = None
            entry["last_update"] = None
            data[uid] = entry
            _save_user_accounts(data)
    except Exception:
        pass

    # 3. Mark as deleted → prevents auto-restart
    _mark_deleted(uid)
    USER_DELETED.setdefault(un, set()).add(uid)
    for k in keys_to_kill:
        USER_DELETED[un].add(k)

    print(f"[DELETE] Purged {uid} (keys: {keys_to_kill})", flush=True)

    return web.json_response({"status": "ok"})


async def h_acc_pause(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    un = "user"
    USER_PAUSED.setdefault(un, set()).add(uid)

    real_uid = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            real_uid = str(cred.get("auth_uid", "") or "")
            if real_uid:
                USER_PAUSED[un].add(real_uid)
    except Exception:
        pass

    keys_to_kill = [uid]
    if real_uid:
        keys_to_kill.append(real_uid)

    workers = USER_WORKERS.get(un, {})
    for k in keys_to_kill:
        t = workers.pop(k, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=3)
            except Exception:
                pass

    for key in list(ds.bot_state.account_workers.keys()):
        suffix = str(key).split("::", 1)[-1]
        if suffix in keys_to_kill:
            t = ds.bot_state.account_workers.pop(key, None)
            if t and not t.done():
                t.cancel()

    for k in keys_to_kill:
        if k in ds.bot_state.accounts:
            ds.bot_state.accounts[k]["status"] = "PAUSED"

    return web.json_response({"status": "ok"})


async def h_acc_resume(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    un = "user"

    # 🔥 Remove deletion mark too (so resume = re-add fresh)
    _unmark_deleted(uid)
    USER_PAUSED.get(un, set()).discard(uid)
    USER_DELETED.get(un, set()).discard(uid)

    real_uid = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            real_uid = str(cred.get("auth_uid", "") or "")
            if real_uid:
                USER_PAUSED[un].discard(real_uid)
                USER_DELETED[un].discard(real_uid)
                _unmark_deleted(real_uid)
    except Exception:
        pass

    asyncio.create_task(start_user_workers(un))
    return web.json_response({"status": "ok"})


async def h_acc_restart(req):
    """
    Restart = full delete + re-add from saved uid+password
    """
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    un = "user"
    print(f"[RESTART] uid={uid}", flush=True)

    # Get saved password
    saved = _load_user_accounts().get(uid)
    saved_pw = saved.get("password", "") if saved else ""

    if not saved_pw:
        return web.json_response({"status": "error", "error": "No saved password"})

    # 1. Full purge
    real_uid = None
    real_acc_id = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            real_uid = str(cred.get("auth_uid", "") or "")
            real_acc_id = str(cred.get("account_id", "") or "")
    except Exception:
        pass

    keys_to_kill = {uid}
    if real_uid:
        keys_to_kill.add(real_uid)
    if real_acc_id:
        keys_to_kill.add(real_acc_id)

    for k in keys_to_kill:
        await _purge_account_everywhere(k)

    USER_DELETED.setdefault(un, set()).update(keys_to_kill)

    # small wait
    await asyncio.sleep(1.0)

    # 2. Re-add fresh
    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    _unmark_deleted(uid)
    USER_DELETED[un].discard(uid)
    USER_PAUSED[un].discard(uid)

    try:
        acc_data = await m.process_account_uid_pass(uid, saved_pw)
    except Exception:
        return web.json_response({"status": "error", "error": "Re-login failed"})

    if not acc_data:
        return web.json_response({"status": "error", "error": "Re-login failed"})

    lvl = int(acc_data.get("level", 1) or 1)
    mode = "LW" if lvl >= 3 else "BR"

    _add_user_account(uid, saved_pw, level=lvl,
                      name=acc_data.get("nickname", ""),
                      region=acc_data.get("region", ""),
                      mode=mode)

    acc_id_str = str(acc_data.get("account_id", uid))
    await _set_mode(acc_id_str, mode)
    USER_DELETED[un].discard(acc_id_str)

    asyncio.create_task(start_user_workers(un))

    return web.json_response({"status": "ok"})
   

# ==================== AUTO HANDLERS ====================
async def h_auto_get(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    cfg = _get_auto_settings()
    return web.json_response({
        "status": "ok",
        "auto_delete_enabled": cfg["auto_delete_enabled"],
        "auto_delete_target_level": cfg["auto_delete_target_level"],
        "lw_limit_enabled": cfg["lw_limit_enabled"],
        "lw_limit": cfg["lw_limit"],
        "br_limit_enabled": cfg["br_limit_enabled"],
        "br_limit": cfg["br_limit"],
    })


async def h_auto_toggle(req):
    """Toggle auto delete on/off"""
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    enabled = bool(d.get("enabled", False))
    cfg = _load_auto_settings()
    cfg["auto_delete_enabled"] = enabled
    _save_auto_settings(cfg)
    print(f"[AUTO] auto_delete_enabled = {enabled}", flush=True)
    return web.json_response({"status": "ok", "auto_delete_enabled": enabled})


async def h_auto_set_level(req):
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    lvl = int(d.get("target_level", 0) or 0)
    if lvl < 1 or lvl > 100:
        return web.json_response({"status": "error", "error": "Target level 1-100"})
    cfg = _load_auto_settings()
    cfg["auto_delete_target_level"] = lvl
    _save_auto_settings(cfg)
    print(f"[AUTO] auto_delete_target_level = {lvl}", flush=True)
    return web.json_response({"status": "ok", "auto_delete_target_level": lvl})


async def h_auto_toggle_lw(req):
    """Toggle LW limit on/off"""
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    enabled = bool(d.get("enabled", False))
    cfg = _load_auto_settings()
    cfg["lw_limit_enabled"] = enabled
    _save_auto_settings(cfg)
    print(f"[LIMIT] lw_limit_enabled = {enabled}", flush=True)
    return web.json_response({"status": "ok", "lw_limit_enabled": enabled})


async def h_auto_toggle_br(req):
    """Toggle BR limit on/off"""
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    enabled = bool(d.get("enabled", False))
    cfg = _load_auto_settings()
    cfg["br_limit_enabled"] = enabled
    _save_auto_settings(cfg)
    print(f"[LIMIT] br_limit_enabled = {enabled}", flush=True)
    return web.json_response({"status": "ok", "br_limit_enabled": enabled})


async def h_auto_set_limits(req):
    """Set LW/BR limits"""
    if is_website_locked():
        return web.json_response({"status": "error", "error": "Website Locked"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    cfg = _load_auto_settings()

    if "lw_limit" in d:
        lw = int(d.get("lw_limit", 3) or 3)
        if lw < 1 or lw > 100:
            return web.json_response({"status": "error", "error": "LW limit 1-100"})
        cfg["lw_limit"] = lw

    if "br_limit" in d:
        br = int(d.get("br_limit", 3) or 3)
        if br < 1 or br > 100:
            return web.json_response({"status": "error", "error": "BR limit 1-100"})
        cfg["br_limit"] = br

    _save_auto_settings(cfg)
    return web.json_response({
        "status": "ok",
        "lw_limit": int(cfg.get("lw_limit", 3) or 3),
        "br_limit": int(cfg.get("br_limit", 3) or 3),
    })


# ==================== LOCK HANDLERS ====================
async def h_lock_get(req):
    st = load_lock_state()
    return web.json_response({
        "status": "ok",
        "locked": bool(st.get("locked", False)),
    })


async def h_lock_toggle(req):
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    locked = bool(d.get("locked", False))
    save_lock_state({"locked": locked, "updated_at": time.time()})
    print(f"[LOCK] Website locked = {locked}", flush=True)
    return web.json_response({"status": "ok", "locked": locked})


async def h_lock_data(req):
    """Return all saved accounts as JSON for lock page display + download."""
    user_accs = _load_user_accounts()
    out = []
    total_exp = 0
    for uid_key, acc in user_accs.items():
        live = ds.bot_state.accounts.get(str(uid_key))
        gained = 0
        level = int(acc.get("level", 1) or 1)
        name = acc.get("name", "")
        region = acc.get("region", "")
        if live:
            gained = int(live.get("gained_exp", 0) or 0)
            level = int(live.get("level", level) or level)
            name = live.get("nickname", name) or name
            region = live.get("region", region) or region
        total_exp += gained
        out.append({
            "uid": str(uid_key),
            "password": acc.get("password", ""),
            "level": level,
            "name": name,
            "region": region,
            "gained_exp": gained,
        })

    running = 0
    for uid, a in ds.bot_state.accounts.items():
        st = a.get("status", "")
        if st not in ("PAUSED", "OFFLINE", "CONNECTING", "ERROR"):
            running += 1

    return web.json_response({
        "status": "ok",
        "total_accounts": len(out),
        "total_exp": total_exp,
        "total_running": running,
        "accounts": out,
    })
    

# ==================== WORKER MANAGEMENT ====================
async def stop_user_workers(username):
    workers = USER_WORKERS.pop(username, {})
    tasks = list(workers.values())
    for t in tasks:
        if not t.done():
            t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(username + "::"):
            t = bot_state.account_workers.pop(key, None)
            if t and not t.done():
                t.cancel()
            if t:
                try:
                    await asyncio.wait_for(t, timeout=3)
                except Exception:
                    pass

    await asyncio.sleep(0.2)


async def start_user_workers(username):
    user_accs = _load_user_accounts()

    if username not in USER_WORKERS:
        USER_WORKERS[username] = {}
    if username not in USER_PAUSED:
        USER_PAUSED[username] = set()
    if username not in USER_DELETED:
        USER_DELETED[username] = set()

    workers = USER_WORKERS[username]
    paused = USER_PAUSED[username]
    deleted = USER_DELETED[username]

    # Get limits
    cfg = _get_auto_settings()
    lw_limit_enabled = cfg["lw_limit_enabled"]
    br_limit_enabled = cfg["br_limit_enabled"]
    lw_limit = max(1, int(cfg.get("lw_limit", 3)))
    br_limit = max(1, int(cfg.get("br_limit", 3)))

    # Count currently running
    lw_running = 0
    br_running = 0

    for key, t in list(workers.items()):
        if t and not t.done():
            mode = await _get_mode(key)
            if mode == "BR":
                br_running += 1
            else:
                lw_running += 1

    for uid_key, acc in user_accs.items():
        if not uid_key:
            continue
        if uid_key in paused or uid_key in deleted:
            continue
        if _is_deleted_history(uid_key):
            continue

        old = workers.get(uid_key)
        if old and not old.done():
            continue

        # Determine mode by level
        lvl = int(acc.get("level", 1) or 1)
        mode = "LW" if lvl >= 3 else "BR"

        # Check limits
        if mode == "LW" and lw_limit_enabled:
            if lw_running >= lw_limit:
                print(f"[LIMIT] Skipping {uid_key} (LW limit reached: {lw_running}/{lw_limit})", flush=True)
                continue
            lw_running += 1
        elif mode == "BR" and br_limit_enabled:
            if br_running >= br_limit:
                print(f"[LIMIT] Skipping {uid_key} (BR limit reached: {br_running}/{br_limit})", flush=True)
                continue
            br_running += 1

        pw = acc.get("password", "")
        if not pw:
            continue

        await _set_mode(uid_key, mode)
        t = asyncio.create_task(_guest_worker(username, uid_key, pw, mode))
        workers[uid_key] = t
        bot_state.account_workers[username + "::" + uid_key] = t
        print(f"[START] Worker started uid={uid_key} mode={mode}", flush=True)


async def _guest_worker(username, uid, password, mode="LW"):
    try:
        import main as m
    except Exception as e:
        print(f"[auth] Cannot import main: {e}", flush=True)
        return
    my_task = asyncio.current_task()
    _USER_ACCOUNT_TASKS.setdefault(username, set()).add(my_task)

    try:
        while True:
            try:
                # Check delete flags
                if uid in USER_DELETED.get(username, set()):
                    print(f"[auth] Guest worker {uid} deleted -> exit", flush=True)
                    break
                if uid in USER_PAUSED.get(username, set()):
                    print(f"[auth] Guest worker {uid} paused -> exit", flush=True)
                    break
                if _is_deleted_history(uid):
                    print(f"[auth] Guest worker {uid} deleted_history -> exit", flush=True)
                    break

                cv = _CURRENT_OWNER.set(username)
                try:
                    print(f"[auth] Guest login starting uid={uid} mode={mode}", flush=True)
                    ad = await m.process_account_uid_pass(uid, password)
                    if not ad:
                        print(f"[auth] Guest login FAILED uid={uid} -> retry in 15s", flush=True)
                        await asyncio.sleep(15)
                        continue

                    acc_id = str(ad.get("account_id", ""))

                    # Check delete again after login
                    if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted after login -> exit", flush=True)
                        break
                    if uid in USER_PAUSED.get(username, set()) or acc_id in USER_PAUSED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} paused after login -> exit", flush=True)
                        break
                    if _is_deleted_history(uid) or _is_deleted_history(acc_id):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted_history after login -> exit", flush=True)
                        break

                    level = int(ad.get("level", 1) or 1)
                    mode = "LW" if level >= 3 else "BR"
                    await _set_mode(acc_id, mode)
                    _update_user_account(uid, mode=mode, level=level,
                                         name=ad.get("nickname", ""),
                                         region=ad.get("region", ""))

                    print(f"[auth] Guest login OK uid={uid} acc_id={acc_id} mode={mode}", flush=True)
                    await m.run_account_worker(ad, uid, mode=mode)

                    if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted -> exit", flush=True)
                        break
                    if _is_deleted_history(uid) or _is_deleted_history(acc_id):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted_history -> exit", flush=True)
                        break

                    print(f"[auth] Guest worker exited uid={uid} -> restart in 3s", flush=True)
                    await asyncio.sleep(3)
                finally:
                    _CURRENT_OWNER.reset(cv)
            except asyncio.CancelledError:
                print(f"[auth] Guest worker cancelled uid={uid}", flush=True)
                raise
            except Exception as e:
                print(f"[auth] Guest worker error uid={uid}: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(10)
    finally:
        try:
            _USER_ACCOUNT_TASKS.get(username, set()).discard(my_task)
        except Exception:
            pass

# ==================== AUTO DELETE WATCHER ====================
async def _auto_delete_watcher():
    """
    প্রতি 10 সেকেন্ডে check করে:
      - auto_delete_enabled কিনা
      - কোন account target_level এ পৌঁছেছে কিনা
      - পৌঁছালে delete বাটনের মতো পুরো কিছু purge করবে
    """
    while True:
        try:
            await asyncio.sleep(10)

            cfg = _get_auto_settings()
            if not cfg.get("auto_delete_enabled"):
                continue

            target_lvl = int(cfg.get("auto_delete_target_level", 0) or 0)
            if target_lvl <= 0:
                continue

            un = "user"
            to_delete = []

            for uid_key, acc in list(ds.bot_state.accounts.items()):
                lvl = int(acc.get("level", 1) or 1)
                if lvl >= target_lvl:
                    to_delete.append((str(uid_key), lvl))

            if not to_delete:
                continue

            for uid_key, lvl in to_delete:
                try:
                    print(f"[AUTO-DELETE] uid={uid_key} reached Lv{lvl} (target={target_lvl}) -> full purge", flush=True)

                    # Resolve real IDs BEFORE purge
                    real_uid = None
                    real_acc_id = None
                    try:
                        cred = ds.bot_state.account_credentials.get(str(uid_key))
                        if cred:
                            real_uid = str(cred.get("auth_uid", "") or "")
                            real_acc_id = str(cred.get("account_id", "") or "")
                    except Exception:
                        pass

                    keys_to_kill = {uid_key}
                    if real_uid:
                        keys_to_kill.add(real_uid)
                    if real_acc_id:
                        keys_to_kill.add(real_acc_id)

                    # Full purge from live state
                    for k in keys_to_kill:
                        await _purge_account_everywhere(k)

                    # Reset saved uid+password (keep uid+password, reset live fields)
                    _uid_main = uid_key
                    data = _load_user_accounts()
                    if _uid_main in data:
                        entry = data[_uid_main]
                        entry["level"] = 0
                        entry["name"] = ""
                        entry["region"] = ""
                        entry["gained_exp"] = 0
                        entry["last_exp"] = 0
                        entry["last_level"] = 0
                        entry["last_nickname"] = ""
                        entry["last_region"] = ""
                        entry["matches_played"] = 0
                        entry["created_at"] = None
                        entry["last_update"] = None
                        data[_uid_main] = entry
                        _save_user_accounts(data)

                    # Mark as deleted to prevent auto-restart
                    _mark_deleted(uid_key)
                    USER_DELETED.setdefault(un, set()).update(keys_to_kill)

                    print(f"[AUTO-DELETE] Purged {uid_key} (keys: {keys_to_kill})", flush=True)

                except Exception as _de:
                    print(f"[AUTO-DELETE] error deleting {uid_key}: {_de}", flush=True)

            if to_delete:
                # Start any pending saved accounts (if slot available)
                try:
                    await start_user_workers(un)
                except Exception:
                    pass

        except asyncio.CancelledError:
            break
        except Exception as _e:
            print(f"[AUTO-DELETE] watcher error: {type(_e).__name__}: {_e}", flush=True)


# ==================== LIMIT WATCHER ====================
async def _limit_watcher():
    """
    প্রতি 30 সেকেন্ডে check করে LW/BR limit ঠিক আছে কিনা।
    বেশি হলে extra workers বন্ধ, কম হলে নতুন start।
    """
    while True:
        try:
            await asyncio.sleep(30)
            cfg = _get_auto_settings()
            lw_limit_enabled = cfg["lw_limit_enabled"]
            br_limit_enabled = cfg["br_limit_enabled"]
            lw_limit = max(1, int(cfg.get("lw_limit", 3)))
            br_limit = max(1, int(cfg.get("br_limit", 3)))

            un = "user"
            workers = USER_WORKERS.get(un, {})
            paused = USER_PAUSED.get(un, set())
            deleted = USER_DELETED.get(un, set())

            lw_list = []
            br_list = []
            for k, t in list(workers.items()):
                if not t or t.done():
                    workers.pop(k, None)
                    continue
                mode = await _get_mode(k)
                if mode == "BR":
                    br_list.append(k)
                else:
                    lw_list.append(k)

            stop_keys = []
            if lw_limit_enabled and len(lw_list) > lw_limit:
                stop_keys.extend(lw_list[lw_limit:])
            if br_limit_enabled and len(br_list) > br_limit:
                stop_keys.extend(br_list[br_limit:])

            for k in stop_keys:
                t = workers.pop(k, None)
                if t and not t.done():
                    t.cancel()
                    try:
                        await asyncio.wait_for(t, timeout=2)
                    except Exception:
                        pass
                print(f"[LIMIT] stopped {k} (over limit)", flush=True)

            # Check if we need to start new workers
            user_accs = _load_user_accounts()
            need_start = False
            for uid_key in user_accs.keys():
                if uid_key in paused or uid_key in deleted:
                    continue
                if _is_deleted_history(uid_key):
                    continue
                t = workers.get(uid_key)
                if t and not t.done():
                    continue
                need_start = True
                break

            if need_start:
                await start_user_workers(un)

        except asyncio.CancelledError:
            break
        except Exception as _e:
            print(f"[LIMIT] watcher error: {type(_e).__name__}: {_e}", flush=True)

# ==================== ROUTES ====================
async def start_web_dashboard(host="0.0.0.0", port=20248):
    app = web.Application()

    # Pages
    app.router.add_get("/", h_root)
    app.router.add_get("/dashboard", h_dash)
    app.router.add_get("/logout", h_logout)
    app.router.add_get("/shappno_004x-lock", h_lock_page)

    # Stats & API
    app.router.add_get("/api/stats", h_stats)
    app.router.add_get("/api/br-status", h_api_br_status)
    app.router.add_get("/api/notification", h_api_br_status)

    # Account actions
    app.router.add_post("/api/account/add-job", h_acc_add_job)
    app.router.add_post("/api/account/bulk-add", h_acc_bulk_add)
    app.router.add_post("/api/account/delete", h_acc_delete)
    app.router.add_post("/api/account/pause", h_acc_pause)
    app.router.add_post("/api/account/resume", h_acc_resume)
    app.router.add_post("/api/account/restart", h_acc_restart)

    # Auto settings
    app.router.add_get("/api/auto/get", h_auto_get)
    app.router.add_post("/api/auto/toggle", h_auto_toggle)
    app.router.add_post("/api/auto/set-level", h_auto_set_level)
    app.router.add_post("/api/auto/set-limits", h_auto_set_limits)
    app.router.add_post("/api/auto/toggle-lw", h_auto_toggle_lw)
    app.router.add_post("/api/auto/toggle-br", h_auto_toggle_br)

    # Lock
    app.router.add_get("/api/lock/get", h_lock_get)
    app.router.add_post("/api/lock/toggle", h_lock_toggle)
    app.router.add_get("/api/lock/data", h_lock_data)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"[+] Web Dashboard: http://{host}:{port}", flush=True)

    # Background watchers
    asyncio.create_task(_auto_delete_watcher())
    asyncio.create_task(_limit_watcher())

    # Start initial workers
    try:
        asyncio.create_task(start_user_workers("user"))
    except Exception:
        pass


__all__ = ["bot_state", "start_web_dashboard"]