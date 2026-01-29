import requests
import json
import csv
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
import time

# ============================================================
# CONFIG
# ============================================================

# Always save files next to this script (not where you run it from)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _p(filename: str) -> str:
    return os.path.join(BASE_DIR, filename)

API_KEY = os.getenv("HYPIXEL_API_KEY", "").strip() or "bcde7d6c-cc0a-4c60-b2d3-34f2f784bc56"
BASE_URL = "https://api.hypixel.net/v2"
MOJANG_API = "https://sessionserver.mojang.com/session/minecraft/profile"

CACHE_FILE = _p("ign_cache.json")
PLAYER_CACHE_FILE = _p("player_cache.json")
PSEUDO_REQS_FILE = _p("pseudo_requirement.json")
PSEUDO_REQS_FILE_OLD = _p("pseudo_requirements.json")
WHITELIST_FILE = _p("kick_whitelist.json")
REQ_WHITELIST_FILE = _p("requirement_whitelist.json")  # ✅ new: excludes from requirement % totals



# Toggles
ENABLE_BEDWARS_WINS = os.getenv("ENABLE_BW_WINS", "1").strip() != "0"
ENABLE_REQUIREMENT_CHECKS = os.getenv("ENABLE_REQUIREMENT_CHECKS", "1").strip() != "0"
ENABLE_SKYBLOCK_LEVEL = os.getenv("ENABLE_SKYBLOCK_LEVEL", "1").strip() != "0"

# Cache TTLs
PLAYER_CACHE_TTL_HOURS = int(os.getenv("PLAYER_CACHE_TTL_HOURS", "24"))
SKYBLOCK_CACHE_TTL_HOURS = int(os.getenv("SKYBLOCK_CACHE_TTL_HOURS", "24"))

# Rate-limit safety (soft throttle, seconds)
HYPIXEL_MIN_INTERVAL_S = float(os.getenv("HYPIXEL_MIN_INTERVAL_S", "0.20"))
MOJANG_MIN_INTERVAL_S = float(os.getenv("MOJANG_MIN_INTERVAL_S", "0.05"))

# Priority penalty for meeting 0 requirements (applies when combined req count == 0)
REQ_ZERO_PENALTY = -3  

# ✅ PSEUDO REQUIREMENT PRIORITY BONUSES
PSEUDO_PRIORITY_BONUSES: Dict[str, int] = {
    "LB": 10,
}

# ============================================================
# ANSI COLORS
# ============================================================
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
ORANGE = "\033[38;5;208m"
YELLOW = "\033[38;5;226m"
GREEN = "\033[92m"
BLUE = "\033[94m"
PURPLE = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"
GRAY = "\033[90m"

DEFAULT_COLOR = CYAN
JOIN_DATE_COLOR = PURPLE
WHITELIST_HIGHLIGHT = BOLD + GREEN

# Score colors (for breakdown)
POS = GREEN
NEG = RED
NEU = GRAY

# ============================================================
# SMALL UTILS
# ============================================================
def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _now_ts() -> int:
    return int(time.time())

def _ratio(n: float, d: float) -> float:
    d = float(d)
    if d <= 0:
        return float(n) if n > 0 else 0.0
    return float(n) / d

def _json_load(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _json_save(path: str, data: Any) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)

def _normalize_uuid(u: str) -> str:
    """
    Canonical UUID key for caches/files:
      - lower
      - remove dashes
      - keep only 0-9a-f
      - max 32 chars
    """
    u = (u or "").strip().lower().replace("-", "")
    u = "".join(ch for ch in u if ch in "0123456789abcdef")
    return u[:32]

# ============================================================
# KICK WHITELIST (permanent)
# ============================================================
def load_kick_whitelist() -> Dict[str, Any]:
    data = _json_load(WHITELIST_FILE, {"uuids": []})
    if not isinstance(data, dict):
        data = {"uuids": []}
    uuids = data.get("uuids", [])
    if not isinstance(uuids, list):
        uuids = []
    uuids = [_normalize_uuid(str(u)) for u in uuids]
    uuids = [u for u in uuids if u]
    seen = set()
    uniq = []
    for u in uuids:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    data["uuids"] = uniq
    return data

def save_kick_whitelist(data: Dict[str, Any]) -> None:
    _json_save(WHITELIST_FILE, data)

KICK_WHITELIST = load_kick_whitelist()

def is_whitelisted_member(m: Dict[str, Any]) -> bool:
    uuid = _normalize_uuid(str(m.get("uuid", "")))
    if not uuid:
        return False
    return uuid in (KICK_WHITELIST.get("uuids", []) or [])

def _whitelist_add_uuid(uuid: str) -> bool:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return False
    KICK_WHITELIST.setdefault("uuids", [])
    if uuid not in KICK_WHITELIST["uuids"]:
        KICK_WHITELIST["uuids"].append(uuid)
        save_kick_whitelist(KICK_WHITELIST)
        return True
    return False

def _whitelist_remove_uuid(uuid: str) -> bool:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return False
    uuids = KICK_WHITELIST.get("uuids", []) or []
    if uuid in uuids:
        KICK_WHITELIST["uuids"] = [u for u in uuids if u != uuid]
        save_kick_whitelist(KICK_WHITELIST)
        return True
    return False

# ============================================================
# REQUIREMENT CHECK WHITELIST (permanent)
#   - members here are excluded from requirement summary % totals
# ============================================================
def load_req_whitelist() -> Dict[str, Any]:
    data = _json_load(REQ_WHITELIST_FILE, {"uuids": []})
    if not isinstance(data, dict):
        data = {"uuids": []}
    uuids = data.get("uuids", [])
    if not isinstance(uuids, list):
        uuids = []
    uuids = [_normalize_uuid(str(u)) for u in uuids]
    uuids = [u for u in uuids if u]
    seen = set()
    uniq = []
    for u in uuids:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    data["uuids"] = uniq
    return data

def save_req_whitelist(data: Dict[str, Any]) -> None:
    _json_save(REQ_WHITELIST_FILE, data)

REQ_WHITELIST = load_req_whitelist()

def is_req_whitelisted_member(m: Dict[str, Any]) -> bool:
    uuid = _normalize_uuid(str(m.get("uuid", "")))
    if not uuid:
        return False
    return uuid in (REQ_WHITELIST.get("uuids", []) or [])

def _req_whitelist_add_uuid(uuid: str) -> bool:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return False
    REQ_WHITELIST.setdefault("uuids", [])
    if uuid not in REQ_WHITELIST["uuids"]:
        REQ_WHITELIST["uuids"].append(uuid)
        save_req_whitelist(REQ_WHITELIST)
        return True
    return False

def _req_whitelist_remove_uuid(uuid: str) -> bool:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return False
    uuids = REQ_WHITELIST.get("uuids", []) or []
    if uuid in uuids:
        REQ_WHITELIST["uuids"] = [u for u in uuids if u != uuid]
        save_req_whitelist(REQ_WHITELIST)
        return True
    return False

# ============================================================
# IGN CACHE
# ============================================================
def load_ign_cache() -> Dict[str, str]:
    data = _json_load(CACHE_FILE, {})
    if not isinstance(data, dict):
        return {}
    # normalize keys to canonical uuid (no dashes)
    out: Dict[str, str] = {}
    for k, v in data.items():
        nk = _normalize_uuid(str(k))
        if nk:
            out[nk] = str(v)
    return out

def save_ign_cache(cache: Dict[str, str]) -> None:
    _json_save(CACHE_FILE, cache)

IGN_CACHE = load_ign_cache()

# ============================================================
# PLAYER CACHE (extracted stats)
# ============================================================
def load_player_cache() -> Dict[str, Any]:
    data = _json_load(PLAYER_CACHE_FILE, {})
    if not isinstance(data, dict):
        return {}
    # normalize keys to canonical uuid (no dashes)
    out: Dict[str, Any] = {}
    for k, v in data.items():
        nk = _normalize_uuid(str(k))
        if nk:
            out[nk] = v
    return out

def save_player_cache(cache: Dict[str, Any]) -> None:
    _json_save(PLAYER_CACHE_FILE, cache)

PLAYER_CACHE = load_player_cache()

# ============================================================
# PSEUDO REQUIREMENTS (manual)
# ============================================================
def load_pseudo_reqs() -> Dict[str, Any]:
    # ✅ migrate old filename -> new filename (so you don’t lose existing roles)
    if not os.path.exists(PSEUDO_REQS_FILE) and os.path.exists(PSEUDO_REQS_FILE_OLD):
        try:
            old = _json_load(PSEUDO_REQS_FILE_OLD, {"defs": {}, "members": {}})
            # normalize member UUID keys
            if isinstance(old, dict) and isinstance(old.get("members"), dict):
                norm_members = {}
                for k, v in old["members"].items():
                    nk = _normalize_uuid(str(k))
                    if nk:
                        norm_members[nk] = v
                old["members"] = norm_members
            _json_save(PSEUDO_REQS_FILE, old)
        except Exception:
            pass

    data = _json_load(PSEUDO_REQS_FILE, {"defs": {}, "members": {}})
    if not isinstance(data, dict):
        return {"defs": {}, "members": {}}
    data.setdefault("defs", {})
    data.setdefault("members", {})
    if not isinstance(data["defs"], dict):
        data["defs"] = {}
    if not isinstance(data["members"], dict):
        data["members"] = {}

    # normalize member keys
    norm_members: Dict[str, Any] = {}
    for k, v in data["members"].items():
        nk = _normalize_uuid(str(k))
        if nk:
            norm_members[nk] = v
    data["members"] = norm_members
    return data

def save_pseudo_reqs(data: Dict[str, Any]) -> None:
    # ensure normalized UUID keys
    if isinstance(data, dict) and isinstance(data.get("members"), dict):
        norm_members: Dict[str, Any] = {}
        for k, v in data["members"].items():
            nk = _normalize_uuid(str(k))
            if nk:
                norm_members[nk] = v
        data["members"] = norm_members
    _json_save(PSEUDO_REQS_FILE, data)

PSEUDO_REQS = load_pseudo_reqs()

def _normalize_code(code: str) -> str:
    return "".join(ch for ch in code.strip().upper() if ch.isalnum() or ch in ("_", "-"))[:12]

def get_member_pseudo_codes(uuid: str) -> List[str]:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return []
    codes = PSEUDO_REQS.get("members", {}).get(uuid, [])
    if not isinstance(codes, list):
        return []
    out = []
    for c in codes:
        cc = _normalize_code(str(c))
        if cc:
            out.append(cc)
    seen = set()
    uniq = []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq

def set_member_pseudo_codes(uuid: str, codes: List[str]) -> None:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return
    PSEUDO_REQS.setdefault("members", {})
    PSEUDO_REQS["members"][uuid] = codes
    save_pseudo_reqs(PSEUDO_REQS)

def add_or_update_pseudo_def(code: str, short: str, desc: str) -> str:
    code = _normalize_code(code)
    if not code:
        return ""
    PSEUDO_REQS.setdefault("defs", {})
    PSEUDO_REQS["defs"][code] = {
        "short": str(short).strip()[:24],
        "desc": str(desc).strip()[:80],
    }
    save_pseudo_reqs(PSEUDO_REQS)
    return code

def delete_pseudo_def(code: str) -> None:
    code = _normalize_code(code)
    if not code:
        return
    PSEUDO_REQS.setdefault("defs", {})
    PSEUDO_REQS["defs"].pop(code, None)
    members = PSEUDO_REQS.get("members", {})
    for uuid, codes in list(members.items()):
        if isinstance(codes, list):
            new_codes = [c for c in codes if _normalize_code(str(c)) != code]
            members[uuid] = new_codes
    save_pseudo_reqs(PSEUDO_REQS)

def _pseudo_priority_bonus_for_codes(codes: List[str]) -> int:
    """
    Sums any configured bonuses for pseudo codes.
    ✅ Guarantees LB always contributes +10 if present.
    """
    norm = [_normalize_code(str(c)) for c in (codes or [])]
    norm = [c for c in norm if c]

    total = 0
    for cc in norm:
        total += int(PSEUDO_PRIORITY_BONUSES.get(cc, 0))

    if "LB" in norm:
        configured = int(PSEUDO_PRIORITY_BONUSES.get("LB", 0))
        guaranteed = max(configured, 10)
        if configured < guaranteed:
            total += (guaranteed - configured)

    return int(total)

def _pseudo_bonus_detail(codes: List[str]) -> str:
    parts = []
    norm = [_normalize_code(str(c)) for c in (codes or [])]
    norm = [c for c in norm if c]

    for cc in norm:
        bonus = int(PSEUDO_PRIORITY_BONUSES.get(cc, 0))
        if cc == "LB":
            bonus = max(bonus, 10)
        if bonus:
            parts.append(f"{cc} +{bonus}")
    return ", ".join(parts)

# ============================================================
# HTTP SESSIONS + SOFT THROTTLE
# ============================================================
mojang_session = requests.Session()
mojang_session.verify = True
mojang_session.headers.update({"User-Agent": "Mozilla/5.0"})

hypixel_session = requests.Session()
hypixel_session.verify = True
hypixel_session.headers.update({"User-Agent": "Mozilla/5.0"})

_LAST_HYPIXEL_CALL_AT = 0.0
_LAST_MOJANG_CALL_AT = 0.0

def _throttle_hypixel(min_interval: float = HYPIXEL_MIN_INTERVAL_S) -> None:
    global _LAST_HYPIXEL_CALL_AT
    now = time.time()
    wait = (_LAST_HYPIXEL_CALL_AT + float(min_interval)) - now
    if wait > 0:
        time.sleep(wait)
    _LAST_HYPIXEL_CALL_AT = time.time()

def _throttle_mojang(min_interval: float = MOJANG_MIN_INTERVAL_S) -> None:
    global _LAST_MOJANG_CALL_AT
    now = time.time()
    wait = (_LAST_MOJANG_CALL_AT + float(min_interval)) - now
    if wait > 0:
        time.sleep(wait)
    _LAST_MOJANG_CALL_AT = time.time()

# ============================================================
# API HELPERS
# ============================================================
# ============================================================
# API HELPERS (Hypixel wrapper w/ 429 backoff)
# ============================================================

def _retry_after_seconds(resp: requests.Response) -> float:
    """
    Best-effort: respect Retry-After if present, else fall back to 1s.
    """
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(float(ra), 0.5)
        except Exception:
            pass
    return 1.0

def _hypixel_get(path: str, params: Dict[str, Any], timeout: int = 15, max_attempts: int = 6) -> Dict[str, Any]:
    """
    Centralized Hypixel GET with:
      - soft throttle between requests
      - 429 handling (Retry-After if provided)
      - exponential backoff on transient errors
    """
    url = f"{BASE_URL}{path}"
    params = dict(params or {})
    params["key"] = API_KEY

    backoff = 1.0
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            _throttle_hypixel()

            r = hypixel_session.get(url, params=params, timeout=timeout)

            # 429: Too Many Requests
            if r.status_code == 429:
                wait = _retry_after_seconds(r)
                # add gentle exponential growth so repeated 429s back off harder
                wait = max(wait, backoff)
                print(f"{YELLOW}{DIM}Hypixel 429 (rate limited). Waiting {wait:.1f}s then retrying...{RESET}")
                time.sleep(wait)
                backoff = min(backoff * 1.8, 30.0)
                continue

            # other 5xx / transient issues: backoff retry
            if 500 <= r.status_code <= 599:
                print(f"{YELLOW}{DIM}Hypixel {r.status_code}. Retrying in {backoff:.1f}s...{RESET}")
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 30.0)
                continue

            r.raise_for_status()
            return (r.json() or {})

        except requests.RequestException as e:
            last_exc = e
            # network hiccup, timeout, etc.
            print(f"{YELLOW}{DIM}Hypixel request error ({attempt}/{max_attempts}). Retrying in {backoff:.1f}s...{RESET}")
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 30.0)

    # If we got here, give a clean error
    raise RuntimeError(f"Hypixel request failed after {max_attempts} attempts. Last error: {last_exc}")

# small TTL cache to avoid hammering /guild if user flips menus quickly
_GUILD_CACHE: Dict[str, Any] = {"name": "", "fetched_at": 0, "guild": None}
GUILD_CACHE_TTL_S = float(os.getenv("GUILD_CACHE_TTL_S", "10"))

def get_guild_by_name(guild_name: str) -> Dict[str, Any]:
    guild_name = (guild_name or "").strip()
    now = time.time()

    if (
        _GUILD_CACHE.get("guild")
        and _GUILD_CACHE.get("name") == guild_name
        and (now - float(_GUILD_CACHE.get("fetched_at", 0))) < GUILD_CACHE_TTL_S
    ):
        return _GUILD_CACHE["guild"]

    data = _hypixel_get("/guild", params={"name": guild_name}, timeout=15, max_attempts=6)

    if not data.get("success") or not data.get("guild"):
        # Hypixel sometimes returns success:false with a cause
        cause = str(data.get("cause", "")).strip()
        msg = "Guild not found (or API key invalid)."
        if cause:
            msg += f" Cause: {cause}"
        raise ValueError(msg)

    _GUILD_CACHE["name"] = guild_name
    _GUILD_CACHE["fetched_at"] = now
    _GUILD_CACHE["guild"] = data["guild"]
    return data["guild"]

def uuid_to_ign(uuid: str) -> str:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return "unknown"

    if uuid in IGN_CACHE:
        return IGN_CACHE[uuid]

    ign = None
    for _ in range(3):
        try:
            _throttle_mojang()
            # Mojang sessionserver expects the UUID without dashes
            response = mojang_session.get(f"{MOJANG_API}/{uuid}", timeout=7)
            if response.status_code == 200:
                ign = (response.json() or {}).get("name")
                if ign:
                    break
        except Exception:
            time.sleep(1)

    if not ign:
        ign = uuid[:8]

    IGN_CACHE[uuid] = ign
    return ign

def _get_game_stats(player_obj: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    stats = player_obj.get("stats", {}) or {}
    if not isinstance(stats, dict):
        return {}
    for k in keys:
        v = stats.get(k)
        if isinstance(v, dict):
            return v
    try:
        lower = {str(k).lower(): k for k in stats.keys()}
        for k in keys:
            kk = lower.get(str(k).lower())
            if kk and isinstance(stats.get(kk), dict):
                return stats.get(kk) or {}
    except Exception:
        pass
    return {}

def _sum_keys_with_prefix(d: Dict[str, Any], prefixes: List[str]) -> int:
    total = 0
    found_any = False
    for k, v in (d or {}).items():
        if not isinstance(k, str):
            continue
        if not any(k.startswith(p) for p in prefixes):
            continue
        try:
            iv = int(v)
        except Exception:
            continue
        total += iv
        found_any = True
    return total if found_any else 0

def _extract_bedwars_wins_from_player(player_obj: Dict[str, Any]) -> int:
    bedwars = _get_game_stats(player_obj, "Bedwars", "BedWars")
    if "wins_bedwars" in bedwars:
        return _safe_int(bedwars.get("wins_bedwars", 0), 0)
    if "wins" in bedwars:
        return _safe_int(bedwars.get("wins", 0), 0)
    return _sum_keys_with_prefix(bedwars, ["wins_bedwars_", "wins_"])

def _extract_bedwars_fkdr(player_obj: Dict[str, Any]) -> float:
    bedwars = _get_game_stats(player_obj, "Bedwars", "BedWars")
    fk = _safe_int(bedwars.get("final_kills_bedwars", bedwars.get("final_kills", 0)), 0)
    fd = _safe_int(bedwars.get("final_deaths_bedwars", bedwars.get("final_deaths", 0)), 0)
    if fk == 0:
        fk = _sum_keys_with_prefix(bedwars, ["final_kills_bedwars_", "final_kills_"])
    if fd == 0:
        fd = _sum_keys_with_prefix(bedwars, ["final_deaths_bedwars_", "final_deaths_"])
    return _ratio(fk, fd)

def _extract_buildbattle_score(player_obj: Dict[str, Any]) -> int:
    bb = _get_game_stats(player_obj, "BuildBattle", "BUILD_BATTLE")
    score = _safe_int(bb.get("score", 0), 0)
    if score > 0:
        return score
    for key in ("build_battle_score", "overall_score", "total_score"):
        score = _safe_int(bb.get(key, 0), 0)
        if score > 0:
            return score
    return 0

def _extract_duels_wins_losses(player_obj: Dict[str, Any]) -> Tuple[int, int]:
    duels = _get_game_stats(player_obj, "Duels", "DUELS")
    wins = _safe_int(duels.get("wins", duels.get("wins_duels", 0)), 0)
    losses = _safe_int(duels.get("losses", duels.get("losses_duels", 0)), 0)
    if wins == 0:
        wins = _sum_keys_with_prefix(duels, ["wins_"])
    if losses == 0:
        losses = _sum_keys_with_prefix(duels, ["losses_"])
    return wins, losses

def _extract_skywars_wins_kdr(player_obj: Dict[str, Any]) -> Tuple[int, float]:
    sw = _get_game_stats(player_obj, "SkyWars", "SKYWARS")
    wins = _safe_int(sw.get("wins", sw.get("wins_skywars", 0)), 0)
    if wins == 0:
        wins = _sum_keys_with_prefix(sw, ["wins_"])
    kills = _safe_int(sw.get("kills", sw.get("kills_skywars", 0)), 0)
    deaths = _safe_int(sw.get("deaths", sw.get("deaths_skywars", 0)), 0)
    if kills == 0:
        kills = _sum_keys_with_prefix(sw, ["kills_"])
    if deaths == 0:
        deaths = _sum_keys_with_prefix(sw, ["deaths_"])
    return wins, _ratio(kills, deaths)

def _extract_tnt_wins(player_obj: Dict[str, Any]) -> int:
    tnt = _get_game_stats(player_obj, "TNTGames", "TNT_GAMES", "TNT")
    wins = _safe_int(tnt.get("wins", tnt.get("wins_tntgames", 0)), 0)
    if wins > 0:
        return wins
    return _sum_keys_with_prefix(tnt, ["wins_"])

def _extract_uhc_score(player_obj: Dict[str, Any]) -> int:
    uhc = _get_game_stats(player_obj, "UHC", "UHCChampions", "UHC_CHAMPIONS")
    score = _safe_int(uhc.get("score", 0), 0)
    if score > 0:
        return score
    for key in ("uhc_score", "overall_score"):
        score = _safe_int(uhc.get(key, 0), 0)
        if score > 0:
            return score
    return 0

def _extract_achievement_points(player_obj: Dict[str, Any]) -> int:
    return _safe_int(player_obj.get("achievementPoints", 0), 0)

def get_player_requirements_blob(uuid: str) -> Dict[str, Any]:
    uuid = _normalize_uuid(uuid)
    if not uuid:
        return {
            "ap": 0,
            "bw_wins": 0, "bw_fkdr": 0.0,
            "bb_score": 0,
            "duels_wins": 0, "duels_wlr": 0.0,
            "sw_wins": 0, "sw_kdr": 0.0,
            "tnt_wins": 0,
            "uhc_score": 0,
            "fetched_at": 0
        }

    now = _now_ts()
    cached = PLAYER_CACHE.get(uuid)
    if isinstance(cached, dict):
        req = cached.get("req")
        fetched_at = _safe_int((req or {}).get("fetched_at", 0), 0)
        if isinstance(req, dict) and fetched_at > 0 and (now - fetched_at) < PLAYER_CACHE_TTL_HOURS * 3600:
            return req

    _throttle_hypixel()
    player_obj: Dict[str, Any] = {}
    success = False

    for attempt in range(3):
        try:
            r = hypixel_session.get(
                f"{BASE_URL}/player",
                params={"key": API_KEY, "uuid": uuid},
                timeout=15
            )
            r.raise_for_status()
            data = r.json() or {}
            if not data.get("success"):
                success = False
                break
            player_obj = data.get("player") or {}
            success = True
            break
        except Exception:
            time.sleep(0.6 * (attempt + 1))

    ap = _extract_achievement_points(player_obj)
    bw_wins = _extract_bedwars_wins_from_player(player_obj)
    bw_fkdr = _extract_bedwars_fkdr(player_obj)
    bb_score = _extract_buildbattle_score(player_obj)

    duels_wins, duels_losses = _extract_duels_wins_losses(player_obj)
    duels_wlr = _ratio(duels_wins, duels_losses)

    sw_wins, sw_kdr = _extract_skywars_wins_kdr(player_obj)

    tnt_wins = _extract_tnt_wins(player_obj)
    uhc_score = _extract_uhc_score(player_obj)

    req_blob = {
        "ap": int(ap),
        "bw_wins": int(bw_wins),
        "bw_fkdr": float(bw_fkdr),
        "bb_score": int(bb_score),
        "duels_wins": int(duels_wins),
        "duels_wlr": float(duels_wlr),
        "sw_wins": int(sw_wins),
        "sw_kdr": float(sw_kdr),
        "tnt_wins": int(tnt_wins),
        "uhc_score": int(uhc_score),
        # ✅ don't poison cache if the fetch failed
        "fetched_at": int(now) if success else 0,
    }

    if success:
        base = PLAYER_CACHE.get(uuid)
        if not isinstance(base, dict):
            base = {}
        base["req"] = req_blob
        PLAYER_CACHE[uuid] = base

    return req_blob

def get_skyblock_level(uuid: str) -> int:
    uuid = _normalize_uuid(uuid)
    if not uuid or not ENABLE_SKYBLOCK_LEVEL or not ENABLE_REQUIREMENT_CHECKS:
        return 0

    now = _now_ts()
    cached = PLAYER_CACHE.get(uuid)
    if isinstance(cached, dict):
        sb = cached.get("sb")
        fetched_at = _safe_int((sb or {}).get("fetched_at", 0), 0)
        if isinstance(sb, dict) and fetched_at > 0 and (now - fetched_at) < SKYBLOCK_CACHE_TTL_HOURS * 3600:
            return _safe_int(sb.get("level", 0), 0)

    level = 0
    success = False
    for attempt in range(3):
        try:
            _throttle_hypixel()
            r = hypixel_session.get(
                f"{BASE_URL}/skyblock/profiles",
                params={"key": API_KEY, "uuid": uuid},
                timeout=20
            )
            r.raise_for_status()
            data = r.json() or {}
            if not data.get("success"):
                success = False
                break

            profiles = data.get("profiles") or []
            best_xp = 0
            for p in profiles:
                members = (p or {}).get("members") or {}
                # profiles members keys are usually uuid-without-dashes
                me = members.get(uuid) or members.get(uuid.replace("-", "")) or {}
                leveling = (me or {}).get("leveling") or {}
                xp = _safe_int(leveling.get("experience", 0), 0)
                if xp > best_xp:
                    best_xp = xp

            # NOTE: this is an approximation; keeping your existing behavior.
            level = int(best_xp // 100)
            success = True
            break
        except Exception:
            time.sleep(0.8 * (attempt + 1))

    if success:
        base = PLAYER_CACHE.get(uuid)
        if not isinstance(base, dict):
            base = {}
        base["sb"] = {"level": int(level), "fetched_at": int(now)}
        PLAYER_CACHE[uuid] = base

    return int(level)

def get_bedwars_wins(uuid: str) -> int:
    uuid = _normalize_uuid(uuid)
    if not uuid or not ENABLE_BEDWARS_WINS:
        return 0
    req = get_player_requirements_blob(uuid)
    return _safe_int(req.get("bw_wins", 0), 0)

# ============================================================
# TIMEZONE SETUP (EST fixed)
# ============================================================
EST = timezone(timedelta(hours=-5))

# ============================================================
# DATE + PREDICTED GEXP LOGIC
# ============================================================
def calculate_days_in_guild(joined_ms: Optional[int]) -> int:
    if not joined_ms:
        return 1
    joined_date = datetime.fromtimestamp(joined_ms / 1000, tz=EST).date()
    today_est = datetime.now(EST).date()
    return max((today_est - joined_date).days + 1, 1)

def scale_gexp(weekly_gexp: int, days_in_guild: int) -> int:
    if days_in_guild >= 7:
        return int(weekly_gexp)
    return int((weekly_gexp / max(days_in_guild, 1)) * 7)

def format_join_date(joined_ms: Optional[int]) -> str:
    if not joined_ms:
        return "??/??/??"
    return datetime.fromtimestamp(joined_ms / 1000, tz=EST).strftime("%d/%m/%y")

# ============================================================
# DATA PROCESSING
# ============================================================
RANK_ORDER = ["Guild Master", "Master", "Senate", "Elder", "Rookie", "Legion"]
RANK_GAP = {"Guild Master": 1, "Master": 1, "Senate": 2, "Elder": 1, "Rookie": 2, "Legion": 0}

def rank_priority(rank: Optional[str]) -> int:
    if not rank:
        return len(RANK_ORDER)
    return RANK_ORDER.index(rank) if rank in RANK_ORDER else len(RANK_ORDER)

def _sum_exp_history(exp_history: Dict[str, Any]) -> int:
    total = 0
    for v in exp_history.values():
        total += _safe_int(v, 0)
    return total

def _exp_history_values_sorted(exp_history: Dict[str, Any]) -> List[int]:
    """
    Hypixel expHistory keys are date strings. We sort them ascending to get oldest->newest.
    """
    if not isinstance(exp_history, dict) or not exp_history:
        return [0] * 7
    try:
        keys = sorted(exp_history.keys())
        vals = [_safe_int(exp_history.get(k, 0), 0) for k in keys]
    except Exception:
        vals = [_safe_int(v, 0) for v in (exp_history.values() or [])]

    if len(vals) >= 7:
        return vals[-7:]
    return ([0] * (7 - len(vals))) + vals

def days_until_weekly_hits_zero_if_no_more_gexp(exp_history: Dict[str, Any]) -> int:
    """
    Estimate "days until weekly becomes 0" assuming the player earns 0 GEXP from now on.
    Based on current 7-day expHistory window:
      - if weekly already 0 -> 0 days
      - else -> last_nonzero_index + 1  (oldest->newest, index 0..6)
    """
    vals = _exp_history_values_sorted(exp_history)  # oldest->newest, len 7
    if sum(vals) == 0:
        return 0
    last_nz = -1
    for i, v in enumerate(vals):
        if _safe_int(v, 0) > 0:
            last_nz = i
    if last_nz < 0:
        return 0
    return last_nz + 1

def extract_weekly_gexp(guild: Dict[str, Any]) -> List[Dict[str, Any]]:
    members = guild.get("members", []) or []
    results: List[Dict[str, Any]] = []

    for member in members:
        exp_history = member.get("expHistory", {}) or {}
        raw_weekly_gexp = _sum_exp_history(exp_history)

        joined_ms = member.get("joined")
        days_in_guild = calculate_days_in_guild(joined_ms)
        predicted_gexp = scale_gexp(raw_weekly_gexp, days_in_guild)

        uuid = _normalize_uuid((member.get("uuid") or ""))

        results.append({
            "ign": uuid_to_ign(uuid),
            "uuid": uuid,
            "rank": member.get("rank") or "Unknown",
            "joined_ms": joined_ms or 0,
            "join_date": format_join_date(joined_ms),
            "days_in_guild": days_in_guild,
            "weekly_gexp": raw_weekly_gexp,
            "predicted_gexp": predicted_gexp,
            "bw_wins": 0,
            "bw_bonus": 0,
            "kick_priority": "",
            "kick_breakdown": [],
            "expHistory": exp_history,
            "reqs_met": "-",
            "reqs_met_count": 0,
            "pseudo_codes": [],
        })

    results.sort(key=lambda m: (rank_priority(m["rank"]), -int(m["predicted_gexp"])))
    return results

# ============================================================
# REQUIREMENTS (real + pseudo)
# ============================================================
REAL_REQS = [
    ("AP",   "AP 15,000",                 "15,000 Achievement Points"),
    ("BW",   "BW 4FKDR + 2,000W",          "Bed Wars: 4 FKDR and 2,000 wins"),
    ("BB",   "BB 50,000",                 "Build Battle: 50,000 score"),
    ("DU",   "DU 10,000W + 3.5WLR",        "Duels: 10,000 wins and 3.5 WLR"),
    ("SW",   "SW 2,000W + 2.0KDR",         "SkyWars: 2,000 wins and 2.0 KDR"),
    ("TNT",  "TNT 1,500W",                 "TNT Games: 1,500 wins"),
    ("UHC",  "UHC 460",                    "UHC: 460 score"),
    ("SB",   "SB 200",                     "SkyBlock: 200 levels"),
]

def _compute_real_reqs(uuid: str) -> List[str]:
    uuid = _normalize_uuid(uuid)
    if not ENABLE_REQUIREMENT_CHECKS or not uuid:
        return []

    out_codes: List[str] = []
    req_blob = get_player_requirements_blob(uuid)

    ap = _safe_int(req_blob.get("ap", 0), 0)
    bw_wins = _safe_int(req_blob.get("bw_wins", 0), 0)
    bw_fkdr = _safe_float(req_blob.get("bw_fkdr", 0.0), 0.0)
    bb_score = _safe_int(req_blob.get("bb_score", 0), 0)
    du_wins = _safe_int(req_blob.get("duels_wins", 0), 0)
    du_wlr = _safe_float(req_blob.get("duels_wlr", 0.0), 0.0)
    sw_wins = _safe_int(req_blob.get("sw_wins", 0), 0)
    sw_kdr = _safe_float(req_blob.get("sw_kdr", 0.0), 0.0)
    tnt_wins = _safe_int(req_blob.get("tnt_wins", 0), 0)
    uhc_score = _safe_int(req_blob.get("uhc_score", 0), 0)
    sb_level = get_skyblock_level(uuid) if ENABLE_SKYBLOCK_LEVEL else 0

    if ap >= 15000:
        out_codes.append("AP")
    if bw_wins >= 2000 and bw_fkdr >= 4.0:
        out_codes.append("BW")
    if bb_score >= 50000:
        out_codes.append("BB")
    if du_wins >= 10000 and du_wlr >= 3.5:
        out_codes.append("DU")
    if sw_wins >= 2000 and sw_kdr >= 2.0:
        out_codes.append("SW")
    if tnt_wins >= 1500:
        out_codes.append("TNT")
    if uhc_score >= 460:
        out_codes.append("UHC")
    if sb_level >= 200:
        out_codes.append("SB")

    return out_codes

def _reqs_to_str(codes: List[str]) -> str:
    return "-" if not codes else ",".join(codes)

def apply_requirements_to_members(members: List[Dict[str, Any]]) -> None:
    for i, m in enumerate(members, start=1):
        uuid = _normalize_uuid(m.get("uuid") or "")
        if not uuid:
            m["reqs_met"] = "-"
            m["reqs_met_count"] = 0
            m["pseudo_codes"] = []
            m["real_reqs_count"] = 0

            continue

        pseudo = get_member_pseudo_codes(uuid)
        m["pseudo_codes"] = pseudo[:]
        real = _compute_real_reqs(uuid) if ENABLE_REQUIREMENT_CHECKS else []
        m["real_reqs_count"] = len(real)   # ✅ real-only (excluding pseudo)
        combined = list(real)

        for c in pseudo:
            if c and c not in combined:
                combined.append(c)

        if ENABLE_REQUIREMENT_CHECKS:
            req_blob = get_player_requirements_blob(uuid)
            m["bw_wins"] = _safe_int(req_blob.get("bw_wins", 0), 0)

        m["reqs_met"] = _reqs_to_str(combined)
        m["reqs_met_count"] = len(combined)

        if ENABLE_REQUIREMENT_CHECKS and (i % 25 == 0):
            print(f"{DIM}{GRAY}... requirements {i}/{len(members)}{RESET}")

# ============================================================
# KICK RECOMMENDATION (with breakdown)
# ============================================================
def bedwars_wins_bonus(wins: int) -> int:
    wins = _safe_int(wins, 0)
    if wins >= 10000:
        return 2
    if 8000 <= wins <= 9999:
        return 1
    elif wins >8000:
        return 0
    

def _apply_bw_bonus(m: Dict[str, Any], breakdown: List[Dict[str, Any]]) -> int:
    if not ENABLE_BEDWARS_WINS:
        m["bw_wins"] = 0
        m["bw_bonus"] = 0
        breakdown.append({"label": "BW Wins", "delta": 0, "detail": "disabled"})
        return 0

    uuid = _normalize_uuid(m.get("uuid") or "")
    wins = get_bedwars_wins(uuid) if uuid else 0

    bonus = _safe_int(bedwars_wins_bonus(wins), 0)  # ✅ hard guarantee int
    m["bw_wins"] = wins
    m["bw_bonus"] = bonus
    breakdown.append({"label": "BW Wins", "delta": bonus, "detail": f"{wins:,} wins"})
    return bonus

def _score_entry(label: str, delta: int, detail: str = "") -> Dict[str, Any]:
    return {"label": label, "delta": int(delta), "detail": str(detail)}

def recommend_kicks(members: List[Dict[str, Any]], min_days_in_guild: int = 0) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    nine_months_days = 30 * 9
    six_months_days = 30 * 6
    three_months_days = 30 * 3

    for m in members:
        if str(m.get("ign", "")).lower() == "undisplayed":
            continue

        # ✅ Permanent whitelist: never include in kick candidates
        if is_whitelisted_member(m):
            continue

        if int(m.get("days_in_guild", 0)) < int(min_days_in_guild):
            continue

        breakdown: List[Dict[str, Any]] = []
        gexp = int(m.get("predicted_gexp", 0))
        priority = 0

        if m["rank"] in ["Guild Master", "Master", "Senate"]:
            priority += 10000
            breakdown.append(_score_entry("Rank (protected)", +10000, m["rank"]))
        elif m["rank"] == "Elder":
            priority += 5
            breakdown.append(_score_entry("Rank (Elder)", +5, ""))

        d = int(m.get("days_in_guild", 0))

        if d >= 365:
            # 1+ year: do NOT also grant the 6m–1y tenure points
            breakdown.append(_score_entry("Tenure", 0, "1+ year (no extra tenure bonus)"))
            # priority += 0

        elif d > six_months_days:
            # 6 months to < 1 year
            breakdown.append(_score_entry("Tenure", 2, "6 Months to 1 Year"))
            priority += 2

        elif 7 <= d <= 30:
            priority -= 1
            breakdown.append(_score_entry("Tenure", -1, "7–30 days"))

        else:
            breakdown.append(_score_entry("Tenure", 0, ""))
            # priority += 0


        if gexp == 0:
            priority -= 15
            breakdown.append(_score_entry("Pred GEXP", -15, "0"))
        elif 0 < gexp <= 7500:
            priority -= 12
            breakdown.append(_score_entry("Pred GEXP", -12, "1–7,500"))
        elif 7500 < gexp <= 15000:
            priority -= 9
            breakdown.append(_score_entry("Pred GEXP", -9, "7,501–15,000"))
        elif 15000 < gexp <= 25000:
            priority -= 6
            breakdown.append(_score_entry("Pred GEXP", -6, "15,001–25,000"))
        elif 25000 < gexp <= 35000:
            priority -= 3
            breakdown.append(_score_entry("Pred GEXP", -3, "25,001–35,000"))
        elif 35000 < gexp <= 50000:
            priority -= 1
            breakdown.append(_score_entry("Pred GEXP", -1, "35,001–50,000"))

        pseudo_codes = m.get("pseudo_codes")
        if not isinstance(pseudo_codes, list):
            uuid = _normalize_uuid(m.get("uuid") or "")
            pseudo_codes = get_member_pseudo_codes(uuid)

        pseudo_bonus = _pseudo_priority_bonus_for_codes(pseudo_codes or [])
        if pseudo_bonus != 0:
            priority += pseudo_bonus
            breakdown.append(_score_entry("Pseudo bonus", pseudo_bonus, _pseudo_bonus_detail(pseudo_codes or [])))
        else:
            breakdown.append(_score_entry("Pseudo bonus", 0, ""))

        req_count = _safe_int(m.get("reqs_met_count", 0), 0)

        req_bonus = 0
        if req_count == 0:
            req_bonus = REQ_ZERO_PENALTY          # -5

        priority += req_bonus

        label = "Reqs"
        detail = "no reqs" if req_count == 0 else f"{req_count} met"
        breakdown.append(_score_entry(label, req_bonus, detail))


        candidates.append({**m, "kick_priority": priority, "kick_breakdown": breakdown})

    # ------------------------------------------------------------
    # Candidate pool selection:
    #   Prefer <50k predicted GEXP, but if that yields <10 members,
    #   top-up from <100k, then from everyone if still short.
    # ------------------------------------------------------------
    pool_50k = [m for m in candidates if int(m.get("predicted_gexp", 0)) < 50000]
    pool_100k = [m for m in candidates if int(m.get("predicted_gexp", 0)) < 100000]

    # Start with <50k
    selected = list(pool_50k)

    # If we have fewer than 10, add <100k members not already included
    if len(selected) < 10:
        have = {(_normalize_uuid(x.get("uuid") or "")) for x in selected}
        for m in pool_100k:
            u = _normalize_uuid(m.get("uuid") or "")
            if u and u not in have:
                selected.append(m)
                have.add(u)
            if len(selected) >= 10:
                break

    # If STILL fewer than 10, add from all candidates (rare, but safe)
    if len(selected) < 10:
        have = {(_normalize_uuid(x.get("uuid") or "")) for x in selected}
        for m in candidates:
            u = _normalize_uuid(m.get("uuid") or "")
            if u and u not in have:
                selected.append(m)
                have.add(u)
            if len(selected) >= 10:
                break

    # Apply BW bonus only to the selected pool (so priority math matches what you display)
    for m in selected:
        bonus = _apply_bw_bonus(m, m["kick_breakdown"])
        m["kick_priority"] = int(m["kick_priority"]) + int(bonus)

    selected.sort(key=lambda m: (int(m.get("kick_priority", 0)), int(m.get("predicted_gexp", 0))))
    return selected[:10]

# ============================================================
# OUTPUT HELPERS
# ============================================================
def _strip_ansi(s: str) -> str:
    out = []
    i = 0
    while i < len(s):
        if s[i] == "\033":
            j = i + 1
            while j < len(s) and s[j] != "m":
                j += 1
            i = j + 1
        else:
            out.append(s[i])
            i += 1
    return "".join(out)

def _pad(text: str, width: int) -> str:
    plain = _strip_ansi(text)
    return text + (" " * max(width - len(plain), 0))

def section_break(title: str, color: str = BLUE, width: int = 110) -> None:
    title_text = f" {title} "
    left = (width - len(title_text)) // 2
    right = width - len(title_text) - left
    print(f"{RESET}")
    print(f"{DIM}{color}{'═' * left}{RESET}{BOLD}{color}{title_text}{RESET}{DIM}{color}{'═' * right}{RESET}")

def _delta_str(delta: int) -> str:
    if delta > 0:
        return f"{POS}+{delta}{RESET}"
    if delta < 0:
        return f"{NEG}{delta}{RESET}"
    return f"{NEU}+0{RESET}"

def _priority_color(priority: int) -> str:
    if priority <= -10:
        return RED
    if priority <= -6:
        return ORANGE
    if priority <= -3:
        return YELLOW
    return GREEN

def print_kick_cards(title: str, recs: List[Dict[str, Any]], columns: int = 2) -> None:
    print(f"{BOLD}{WHITE}{title}{RESET}")
    if not recs:
        print(f"{YELLOW}None{RESET}")
        return

    ORDER = [
        "Rank (protected)",
        "Rank (Elder)",
        "Tenure",
        "Pred GEXP",
        "Pseudo bonus",
        "Reqs",
        "BW Wins",
    ]

    def normalize_breakdown(bd: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out = {k: {"label": k, "delta": 0, "detail": ""} for k in ORDER}
        for e in bd:
            lab = str(e.get("label", ""))
            if lab in out:
                out[lab] = {
                    "label": lab,
                    "delta": _safe_int(e.get("delta", 0), 0),
                    "detail": str(e.get("detail", "")),
                }
        return out

    cards: List[List[str]] = []
    for idx, m in enumerate(recs, start=1):
        ign = str(m.get("ign", "unknown"))
        rank = str(m.get("rank", "Unknown"))
        pred = int(m.get("predicted_gexp", 0))
        join = str(m.get("join_date", "??/??/??"))
        prio = int(m.get("kick_priority", 0))
        days = int(m.get("days_in_guild", 0))

        prio_col = _priority_color(prio)
        bd_map = normalize_breakdown(m.get("kick_breakdown", []) or [])

        lines = []
        lines.append(f"{BOLD}{CYAN}{idx:>2}. {ign}{RESET} {DIM}({rank}){RESET}")
        lines.append(f"{WHITE}Pred:{RESET} {CYAN}{pred:,}{RESET}   {WHITE}Days:{RESET} {WHITE}{days}{RESET}   {WHITE}Join:{RESET} {JOIN_DATE_COLOR}{join}{RESET}")
        lines.append(f"{WHITE}Priority:{RESET} {prio_col}{prio}{RESET}")
        lines.append(f"{DIM}{'─' * 44}{RESET}")

        def row(label: str, entry: Dict[str, Any]) -> str:
            delta = _safe_int(entry.get("delta", 0), 0)
            detail = str(entry.get("detail", ""))
            if detail:
                detail = f"{DIM}{detail}{RESET}"
            return f"{WHITE}{label:<13}{RESET} {_delta_str(delta):>6}  {detail}"

        if rank in ["Guild Master", "Master", "Senate"]:
            lines.append(row("Rank", bd_map["Rank (protected)"]))
        elif rank == "Elder":
            lines.append(row("Rank", bd_map["Rank (Elder)"]))
        else:
            lines.append(row("Rank", {"delta": 0, "detail": ""}))

        lines.append(row("Tenure", bd_map["Tenure"]))
        lines.append(row("Pred GEXP", bd_map["Pred GEXP"]))
        lines.append(row("Pseudo", bd_map["Pseudo bonus"]))
        lines.append(row("Reqs", bd_map["Reqs"]))
        lines.append(row("BW Wins", bd_map["BW Wins"]))
        cards.append(lines)

    max_line_len = max(len(_strip_ansi(line)) for card in cards for line in card)
    card_width = max(max_line_len, 48) + 4

    for row_start in range(0, len(cards), columns):
        row_cards = cards[row_start:row_start + columns]
        max_lines = max(len(c) for c in row_cards)

        for line_idx in range(max_lines):
            for c in row_cards:
                line = c[line_idx] if line_idx < len(c) else ""
                print(_pad(line, card_width), end="")
            print()
        print()

def _grid_print(title: str, items: List[str], cols: int = 5, title_color: str = CYAN) -> None:
    print(f"{BOLD}{title_color}{title}{RESET} {DIM}{GRAY}({len(items)}){RESET}")
    if not items:
        print(f"{DIM}{GRAY}  none{RESET}\n")
        return

    # Clean + sort (A→Z)
    clean = [str(x).strip() for x in items if str(x).strip()]
    clean.sort(key=lambda s: s.lower())

    # Auto width based on longest name (cap so it doesn't get silly)
    max_len = max(len(_strip_ansi(x)) for x in clean)
    col_width = min(max(max_len + 3, 14), 26)

    for i, name in enumerate(clean):
        print(_pad(f"{CYAN}{name}{RESET}", col_width), end="")
        if (i + 1) % cols == 0:
            print()
    if len(clean) % cols != 0:
        print()
    print()


def _format_member_cell(m: Dict[str, Any]) -> str:
    return str(m.get("ign", "")).strip()



# ============================================================
# DISPLAY ORDER MENU
# ============================================================
def get_display_order_choice() -> str:
    section_break("LEADERBOARD ORDER", color=CYAN)
    print(f"{BOLD}{CYAN}Choose DISPLAY order for the leaderboard:{RESET}")
    print(f"{WHITE}1{RESET} - Rank, then Predicted GEXP (high → low)  {DIM}(default){RESET}")
    print(f"{WHITE}2{RESET} - Rank, then Weekly GEXP (high → low)")
    print(f"{WHITE}3{RESET} - Rank, then Days in guild (high → low)")
    print(f"{WHITE}4{RESET} - IGN (A → Z)  {DIM}(within rank){RESET}")
    print(f"{WHITE}5{RESET} - Kick priority (worst → best)  {DIM}(for review, within rank){RESET}")
    choice = input(f"{DIM}Enter choice number [default 1]: {RESET}").strip()

    mapping = {
        "1": "rank_pred",
        "2": "rank_weekly",
        "3": "rank_days",
        "4": "ign_az",
        "5": "kick_worst",
    }
    return mapping.get(choice, "rank_pred")

def apply_display_order(members: List[Dict[str, Any]], mode: str) -> List[Dict[str, Any]]:
    ms = list(members)

    if mode == "rank_weekly":
        ms.sort(key=lambda m: (rank_priority(m.get("rank")), -int(m.get("weekly_gexp", 0))))
        return ms

    if mode == "rank_days":
        ms.sort(key=lambda m: (rank_priority(m.get("rank")), -int(m.get("days_in_guild", 0))))
        return ms

    if mode == "ign_az":
        # ✅ KEEP rank groups, but sort IGN A→Z inside each rank
        ms.sort(key=lambda m: (rank_priority(m.get("rank")), str(m.get("ign", "")).lower()))
        return ms

    if mode == "kick_worst":
        def kp(m: Dict[str, Any]) -> int:
            v = m.get("kick_priority", "")
            try:
                return int(v)
            except Exception:
                return 999999

        # ✅ KEEP rank groups, sort worst→best inside each rank
        ms.sort(key=lambda m: (
            rank_priority(m.get("rank")),
            kp(m),
            int(m.get("predicted_gexp", 0))
        ))
        return ms

    # default: rank + predicted high→low
    ms.sort(key=lambda m: (rank_priority(m.get("rank")), -int(m.get("predicted_gexp", 0))))
    return ms

# ============================================================
# PSEUDOROLE MENU (case-insensitive picking + cancel + reliable saving)
# ============================================================
def _sync_member_pseudo_cache(members: List[Dict[str, Any]]) -> None:
    for m in members:
        uuid = _normalize_uuid(m.get("uuid") or "")
        if uuid:
            m["pseudo_codes"] = get_member_pseudo_codes(uuid)
        else:
            m["pseudo_codes"] = []

def _find_member_indices_by_name(members: List[Dict[str, Any]], query: str) -> List[int]:
    q = (query or "").strip().lower()
    if not q:
        return []

    exact = [i for i, m in enumerate(members) if str(m.get("ign", "")).strip().lower() == q]
    if exact:
        return exact

    contains = [i for i, m in enumerate(members) if q in str(m.get("ign", "")).strip().lower()]
    return contains

def pick_member_by_name_or_number(members: List[Dict[str, Any]]) -> int:
    print(f"{DIM}{GRAY}Tip: type an IGN (not case sensitive) or a member #. Blank/0 cancels.{RESET}")
    preview_n = min(len(members), 25)
    for idx in range(preview_n):
        m = members[idx]
        tags = (m.get("pseudo_codes") or [])
        tag_txt = f"{DIM}{GRAY} [{','.join(tags)}]{RESET}" if tags else ""
        print(f"{WHITE}{idx+1:>3}{RESET} - {CYAN}{m['ign']}{RESET} {DIM}({m['rank']}){RESET}{tag_txt}")
    if len(members) > preview_n:
        print(f"{DIM}{GRAY}... and {len(members) - preview_n} more (you can still type their IGN).{RESET}")

    raw = input(f"{DIM}Pick member (IGN or #): {RESET}").strip()
    if raw == "" or raw == "0":
        return -1

    if raw.isdigit():
        mi = int(raw)
        if 1 <= mi <= len(members):
            return mi - 1
        print(f"{RED}Invalid member number.{RESET}\n")
        return -1

    hits = _find_member_indices_by_name(members, raw)
    if not hits:
        print(f"{RED}No members matched '{raw}'.{RESET}\n")
        return -1

    if len(hits) == 1:
        return hits[0]

    section_break("MULTIPLE MATCHES", color=PURPLE)
    for j, i in enumerate(hits, start=1):
        m = members[i]
        tags = (m.get("pseudo_codes") or [])
        tag_txt = f"{DIM}{GRAY} [{','.join(tags)}]{RESET}" if tags else ""
        print(f"{WHITE}{j:>2}{RESET} - {CYAN}{m['ign']}{RESET} {DIM}({m['rank']}){RESET}{tag_txt}")

    pick = input(f"{DIM}Choose 1-{len(hits)} (blank/0 cancel): {RESET}").strip()
    if pick == "" or pick == "0":
        return -1
    if pick.isdigit():
        jj = int(pick)
        if 1 <= jj <= len(hits):
            return hits[jj - 1]

    print(f"{RED}Invalid selection.{RESET}\n")
    return -1

def _choose_pseudorole_code_from_defs(defs: Dict[str, Any], prompt: str = "Pick pseudorole") -> str:
    if not defs:
        print(f"{YELLOW}No pseudoroles exist yet. Create one first.{RESET}\n")
        return ""

    codes_sorted = sorted(defs.keys())
    print(f"{BOLD}{WHITE}{prompt}:{RESET}")
    for i, c in enumerate(codes_sorted, start=1):
        meta = defs.get(c) or {}
        short = str(meta.get("short", "")).strip()
        bonus = int(PSEUDO_PRIORITY_BONUSES.get(c, 0))
        if c == "LB":
            bonus = max(bonus, 10)
        bonus_txt = f"{DIM}{GRAY}(+{bonus}){RESET} " if bonus else ""
        print(f"{WHITE}{i:>2}{RESET} - {CYAN}{c:<12}{RESET} {bonus_txt}{GRAY}{short}{RESET}")

    raw = input(f"{DIM}Enter number OR code (blank/0 cancel): {RESET}").strip()
    if raw == "" or raw == "0":
        return ""

    if raw.isdigit():
        ii = int(raw)
        if 1 <= ii <= len(codes_sorted):
            return codes_sorted[ii - 1]
        return ""

    cc = _normalize_code(raw)
    return cc if cc in defs else ""

def pseudo_reqs_menu(members: List[Dict[str, Any]]) -> None:
    while True:
        _sync_member_pseudo_cache(members)
        defs = PSEUDO_REQS.get("defs", {}) or {}

        section_break("PSEUDOROLES", color=ORANGE)
        assigned_count = sum(1 for m in members if (m.get("pseudo_codes") or []))
        print(f"{DIM}{GRAY}File: {PSEUDO_REQS_FILE}{RESET}")
        print(f"{DIM}{GRAY}Roles: {len(defs)} | Members with roles: {assigned_count}{RESET}\n")

        print(f"{WHITE}1{RESET} - {GREEN}Give{RESET} member a pseudorole")
        print(f"{WHITE}2{RESET} - {RED}Remove{RESET} pseudorole from member")
        print(f"{WHITE}3{RESET} - Create pseudorole")
        print(f"{WHITE}4{RESET} - Delete pseudorole")
        print(f"{WHITE}5{RESET} - View members with pseudoroles")
        print(f"{WHITE}0{RESET} - Finish\n")

        choice = input(f"{DIM}Enter choice: {RESET}").strip()

        if choice == "0":
            return

        if choice == "5":
            section_break("MEMBERS WITH PSEUDOROLES", color=PURPLE)
            any_found = False
            for m in members:
                tags = m.get("pseudo_codes") or []
                if tags:
                    any_found = True
                    print(f"{CYAN}{m['ign']:<16}{RESET} {DIM}({m['rank']}){RESET}  ->  {YELLOW}{','.join(tags)}{RESET}")
            if not any_found:
                print(f"{GRAY}None.{RESET}")
            print()
            input(f"{DIM}Press Enter to go back...{RESET}")
            continue

        if choice == "3":
            section_break("CREATE PSEUDOROLE", color=GREEN)
            code = _normalize_code(input(f"{DIM}Code (e.g. VIP, EVENT, TRIAL, LB): {RESET}"))
            if not code:
                print(f"{RED}Invalid code.{RESET}\n")
                continue
            short = input(f"{DIM}Short label: {RESET}").strip()
            desc = input(f"{DIM}Description: {RESET}").strip()
            final = add_or_update_pseudo_def(code, short or code, desc or "")
            print(f"{GREEN}Saved:{RESET} {CYAN}{final}{RESET}\n")
            continue

        if choice == "4":
            section_break("DELETE PSEUDOROLE", color=RED)
            code = _choose_pseudorole_code_from_defs(defs, prompt="Pick pseudorole to delete")
            if not code:
                print(f"{YELLOW}No deletion made.{RESET}\n")
                continue
            if code not in PSEUDO_REQS.get("defs", {}):
                print(f"{YELLOW}That code isn't in defs.{RESET}\n")
                continue
            delete_pseudo_def(code)
            print(f"{GREEN}Deleted:{RESET} {CYAN}{code}{RESET}\n")
            continue

        if choice in ("1", "2"):
            idx = pick_member_by_name_or_number(members)
            if idx < 0:
                print(f"{DIM}{GRAY}Cancelled.{RESET}\n")
                continue

            m = members[idx]
            uuid = _normalize_uuid(m.get("uuid") or "")
            if not uuid:
                print(f"{RED}Selected member has no UUID?!{RESET}\n")
                continue

            current = get_member_pseudo_codes(uuid)
            defs = PSEUDO_REQS.get("defs", {}) or {}

            if choice == "1":
                section_break("GIVE PSEUDOROLE", color=GREEN)
                if not defs:
                    print(f"{YELLOW}No pseudoroles exist yet. Create one first.{RESET}\n")
                    continue

                code = _choose_pseudorole_code_from_defs(defs, prompt=f"Pick pseudorole for {m['ign']}")
                if not code:
                    print(f"{DIM}{GRAY}Cancelled role selection.{RESET}\n")
                    continue

                if code in current:
                    print(f"{YELLOW}{m['ign']} already has {code}.{RESET}\n")
                    continue

                new_codes = current + [code]
                set_member_pseudo_codes(uuid, new_codes)
                m["pseudo_codes"] = new_codes[:]

                saved = get_member_pseudo_codes(uuid)
                ok = (code in saved)

                if ok:
                    print(f"{GREEN}Added:{RESET} {CYAN}{code}{RESET} -> {CYAN}{m['ign']}{RESET}  {DIM}({','.join(saved)}){RESET}\n")
                else:
                    print(f"{RED}Tried to add {code} but it did not persist. Check file write permissions.{RESET}\n")
                continue

            if choice == "2":
                section_break("REMOVE PSEUDOROLE", color=RED)
                if not current:
                    print(f"{YELLOW}{m['ign']} has no pseudoroles.{RESET}\n")
                    continue

                print(f"{BOLD}{WHITE}Pick pseudorole to remove from {m['ign']}:{RESET}")
                for i, c in enumerate(current, start=1):
                    print(f"{WHITE}{i:>2}{RESET} - {CYAN}{c}{RESET}")
                raw = input(f"{DIM}Enter number OR code (blank/0 cancel): {RESET}").strip()

                if raw == "" or raw == "0":
                    print(f"{DIM}{GRAY}Cancelled.{RESET}\n")
                    continue

                code = ""
                if raw.isdigit():
                    ii = int(raw)
                    if 1 <= ii <= len(current):
                        code = current[ii - 1]
                else:
                    cc = _normalize_code(raw)
                    if cc in current:
                        code = cc

                if not code:
                    print(f"{YELLOW}No removal made.{RESET}\n")
                    continue

                new_codes = [c for c in current if c != code]
                set_member_pseudo_codes(uuid, new_codes)
                m["pseudo_codes"] = new_codes[:]

                print(f"{GREEN}Removed:{RESET} {CYAN}{code}{RESET} <- {CYAN}{m['ign']}{RESET}  {DIM}({','.join(new_codes) if new_codes else '-'}){RESET}\n")
                continue

        print(f"{YELLOW}Unknown option.{RESET}\n")

# ============================================================
# WHITELIST MENU
# ============================================================
def kick_whitelist_menu(members: List[Dict[str, Any]]) -> None:
    while True:
        section_break("KICKWAVE WHITELIST", color=GREEN)

        uuids = KICK_WHITELIST.get("uuids", []) or []
        in_guild = sum(1 for m in members if is_whitelisted_member(m))

        print(f"{DIM}{GRAY}File: {WHITELIST_FILE}{RESET}")
        print(f"{DIM}{GRAY}Whitelisted UUIDs: {len(uuids)} | In current guild: {in_guild}{RESET}\n")

        print(f"{WHITE}1{RESET} - Add member to kickwave whitelist (pick by IGN/#)")
        print(f"{WHITE}2{RESET} - Remove member from kickwave whitelist (pick by IGN/#)")
        print(f"{WHITE}3{RESET} - Show whitelisted members (matched to current guild)")
        print(f"{WHITE}0{RESET} - Back\n")

        c = input(f"{DIM}Enter choice: {RESET}").strip()

        if c == "0":
            return

        if c == "3":
            section_break("KICKWAVE WHITELISTED MEMBERS (FOUND IN GUILD)", color=CYAN)
            found_any = False
            for m in members:
                if is_whitelisted_member(m):
                    found_any = True
                    print(
                        f"{CYAN}{str(m.get('ign','')):<16}{RESET} "
                        f"{DIM}({str(m.get('rank',''))}){RESET}  "
                        f"{GRAY}{str(m.get('uuid',''))}{RESET}"
                    )
            if not found_any:
                print(f"{GRAY}None found in current guild list (but UUIDs may still be saved).{RESET}")
            print()
            input(f"{DIM}Press Enter to continue...{RESET}")
            continue

        if c in ("1", "2"):
            idx = pick_member_by_name_or_number(members)
            if idx < 0:
                print(f"{DIM}{GRAY}Cancelled.{RESET}\n")
                continue

            m = members[idx]
            uuid = _normalize_uuid(m.get("uuid") or "")
            if not uuid:
                print(f"{RED}Selected member has no UUID?!{RESET}\n")
                continue

            if c == "1":
                added = _whitelist_add_uuid(uuid)
                if added:
                    print(f"{GREEN}Whitelisted for kickwaves:{RESET} {CYAN}{m.get('ign','')}{RESET} {DIM}({uuid}){RESET}\n")
                else:
                    print(f"{YELLOW}Already whitelisted for kickwaves:{RESET} {CYAN}{m.get('ign','')}{RESET}\n")
                continue

            if c == "2":
                removed = _whitelist_remove_uuid(uuid)
                if removed:
                    print(f"{GREEN}Removed from kickwave whitelist:{RESET} {CYAN}{m.get('ign','')}{RESET} {DIM}({uuid}){RESET}\n")
                else:
                    print(f"{YELLOW}That member isn't kickwave-whitelisted:{RESET} {CYAN}{m.get('ign','')}{RESET}\n")
                continue

        print(f"{YELLOW}Unknown option.{RESET}\n")


def requirement_whitelist_menu(members: List[Dict[str, Any]]) -> None:
    while True:
        section_break("REQUIREMENT CHECK WHITELIST", color=PURPLE)
        uuids = REQ_WHITELIST.get("uuids", []) or []
        in_guild = sum(1 for m in members if is_req_whitelisted_member(m))

        print(f"{DIM}{GRAY}File: {REQ_WHITELIST_FILE}{RESET}")
        print(f"{DIM}{GRAY}Whitelisted UUIDs: {len(uuids)} | In current guild: {in_guild}{RESET}\n")

        print(f"{WHITE}1{RESET} - Add member to whitelist (pick by IGN/#)")
        print(f"{WHITE}2{RESET} - Remove member from whitelist (pick by IGN/#)")
        print(f"{WHITE}3{RESET} - Show whitelisted members")
        print(f"{WHITE}0{RESET} - Back\n")

        c = input(f"{DIM}Enter choice: {RESET}").strip()

        if c == "0":
            return

        if c == "3":
            section_break("REQ-WHITELISTED MEMBERS (FOUND IN GUILD)", color=CYAN)
            found_any = False
            for m in members:
                if is_req_whitelisted_member(m):
                    found_any = True
                    print(f"{CYAN}{str(m.get('ign','')):<16}{RESET} {DIM}({str(m.get('rank',''))}){RESET}  {GRAY}{str(m.get('uuid',''))}{RESET}")
            if not found_any:
                print(f"{GRAY}None found in current guild list (but UUIDs may still be saved).{RESET}")
            print()
            input(f"{DIM}Press Enter to continue...{RESET}")
            continue

        if c in ("1", "2"):
            idx = pick_member_by_name_or_number(members)
            if idx < 0:
                print(f"{DIM}{GRAY}Cancelled.{RESET}\n")
                continue

            m = members[idx]
            uuid = _normalize_uuid(m.get("uuid") or "")
            if not uuid:
                print(f"{RED}Selected member has no UUID?!{RESET}\n")
                continue

            if c == "1":
                added = _req_whitelist_add_uuid(uuid)
                if added:
                    print(f"{GREEN}Whitelisted for requirements:{RESET} {CYAN}{m.get('ign','')}{RESET} {DIM}({uuid}){RESET}\n")
                else:
                    print(f"{YELLOW}Already whitelisted for requirements:{RESET} {CYAN}{m.get('ign','')}{RESET}\n")
                continue

            if c == "2":
                removed = _req_whitelist_remove_uuid(uuid)
                if removed:
                    print(f"{GREEN}Removed from requirement whitelist:{RESET} {CYAN}{m.get('ign','')}{RESET} {DIM}({uuid}){RESET}\n")
                else:
                    print(f"{YELLOW}That member isn't requirement-whitelisted:{RESET} {CYAN}{m.get('ign','')}{RESET}\n")
                continue

        print(f"{YELLOW}Unknown option.{RESET}\n")

def manage_whitelists_menu(members: List[Dict[str, Any]]) -> None:
    while True:
        section_break("MANAGE WHITELISTS", color=GREEN)
        print(f"{WHITE}1{RESET} - Manage kickwave whitelist")
        print(f"{WHITE}2{RESET} - Manage requirement check whitelist")
        print(f"{WHITE}0{RESET} - Back\n")

        c = input(f"{DIM}Enter choice: {RESET}").strip()

        if c == "0":
            return
        if c == "1":
            kick_whitelist_menu(members)
            continue
        if c == "2":
            requirement_whitelist_menu(members)
            continue

        print(f"{YELLOW}Unknown option.{RESET}\n")

def _zero_soon_badge_for_member(m: Dict[str, Any]) -> str:
    """
    Returns a coloured badge string to prepend in the leaderboard IGN column:
      - 1 day -> RED
      - 2 days -> ORANGE
      - 3 days -> YELLOW
    Matches the colours used in menu 1 -> 4.
    """
    exp_history = m.get("expHistory") or {}
    d0 = days_until_weekly_hits_zero_if_no_more_gexp(exp_history)

    if d0 == 1:
        return f"{BOLD}{RED}!{RESET}"
    if d0 == 2:
        return f"{BOLD}{ORANGE}!{RESET}"
    if d0 == 3:
        return f"{BOLD}{YELLOW}!{RESET}"
    return " "  # keep alignment when no badge

# ============================================================
# LEADERBOARD / CSV
# ============================================================
def print_leaderboard(guild_name: str, members: List[Dict[str, Any]]) -> None:
    print(f"\n{BOLD}{PURPLE}Guild:{RESET} {BOLD}{guild_name}{RESET}")
    print(f"{BOLD}{PURPLE}Members:{RESET} {BOLD}{len(members)}{RESET}\n")

    print(
        f"{BOLD}{WHITE}"
        f"{'IGN':<17} | {'Rank':<12} | {'Weekly GEXP':>12} | {'Predicted GEXP':>15} | {'Days':>5} | {'Joined':>10} | {'Reqs':<16}"
        f"{RESET}"
    )
    print(f"{DIM}{'-' * 117}{RESET}")

    current_rank = None
    for m in members:
        if current_rank != m["rank"]:
            if current_rank is not None:
                gap_lines = RANK_GAP.get(current_rank, 1)
                for _ in range(gap_lines):
                    print()
            current_rank = m["rank"]
            print(f"{BOLD}{BLUE}--- {m['rank']} ---{RESET}")

        reqs = str(m.get("reqs_met", "-"))
        cnt = _safe_int(m.get("reqs_met_count", 0), 0)
        req_col = GREEN if cnt >= 3 else (YELLOW if cnt == 2 else (ORANGE if cnt == 1 else GRAY))
        
        # ✅ Highlight kickwave whitelist + ALSO tag 0-GEXP soon (1/2/3 days) like menu 1 -> 4
        ign_plain = str(m.get("ign", "")).strip()
        badge = _zero_soon_badge_for_member(m)  # coloured "!" for 1/2/3 days, or " " otherwise

        if is_whitelisted_member(m):
            ign_cell = _pad(f"{badge}{WHITELIST_HIGHLIGHT}{ign_plain}{RESET}", 17)
        else:
            ign_cell = _pad(f"{badge}{DEFAULT_COLOR}{ign_plain}{RESET}", 17)


        print(
            f"{ign_cell} | "
            f"{WHITE}{m['rank']:<12}{RESET} | "
            f"{GREEN}{int(m['weekly_gexp']):>12,}{RESET} | "
            f"{CYAN}{int(m['predicted_gexp']):>15,}{RESET} | "
            f"{WHITE}{int(m['days_in_guild']):>5}{RESET} | "
            f"{JOIN_DATE_COLOR}{m['join_date']:>10}{RESET} | "
            f"{req_col}{reqs:<16}{RESET}"
        )

def export_to_csv(members: List[Dict[str, Any]]) -> None:
    csv_path = _p("guild_weekly_gexp.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ROLE", "NAME", "WEEKLY GEXP", "PREDICTED GEXP",
            "DAYS IN GUILD", "JOIN DATE", "UUID",
            "BEDWARS WINS", "BW WINS BONUS", "KICK PRIORITY",
            "REQUIREMENTS MET"
        ])

        current_rank = None
        for m in members:
            if current_rank != m["rank"]:
                if current_rank is not None:
                    gap_lines = RANK_GAP.get(current_rank, 1)
                    for _ in range(gap_lines):
                        writer.writerow([])
                current_rank = m["rank"]

            writer.writerow([
                m["rank"],
                m["ign"],
                int(m.get("weekly_gexp", 0)),
                int(m.get("predicted_gexp", 0)),
                int(m.get("days_in_guild", 0)),
                m.get("join_date", ""),
                m.get("uuid", ""),
                _safe_int(m.get("bw_wins", 0), 0),
                _safe_int(m.get("bw_bonus", 0), 0),
                m.get("kick_priority", ""),
                m.get("reqs_met", "-"),
            ])

    print(f"\n{GREEN}CSV exported:{RESET} {csv_path}")

# ============================================================
# 0-GEXP SOON LISTS
# ============================================================
def members_hitting_zero_in_days(members: List[Dict[str, Any]], target_days: int) -> List[Dict[str, Any]]:
    out = []
    for m in members:
        exp_history = m.get("expHistory") or {}
        d0 = days_until_weekly_hits_zero_if_no_more_gexp(exp_history)
        if d0 == target_days:
            out.append({**m, "days_until_zero": d0})
    out.sort(key=lambda x: (rank_priority(x.get("rank")), int(x.get("predicted_gexp", 0)), str(x.get("ign", "")).lower()))
    return out

def print_zero_soon_grouped(members: List[Dict[str, Any]], days_list: List[int] = [0, 1, 2, 3]) -> None:
    section_break("WEEKLY GEXP HITS 0 SOON (IF THEY KEEP DOING 0)", color=YELLOW)
    any_found = False

    for d in days_list:
        lst = members_hitting_zero_in_days(members, d)

        if d == 0:
            header_col = RED
            label = "0 day(s) (already 0)"
        else:
            header_col = RED if d == 1 else (ORANGE if d == 2 else YELLOW)
            label = f"{d} day(s)"

        print(f"{BOLD}{header_col}{label}:{RESET} {DIM}{GRAY}({len(lst)} members){RESET}")
        if not lst:
            print(f"{GRAY}  none{RESET}\n")
            continue

        any_found = True
        for m in lst:
            ign = str(m.get("ign", ""))
            rank = str(m.get("rank", ""))
            pred = int(m.get("predicted_gexp", 0))
            weekly = int(m.get("weekly_gexp", 0))
            join = str(m.get("join_date", "??/??/??"))

            print(
                f"  {CYAN}{ign:<16}{RESET} {DIM}({rank}){RESET}  "
                f"{WHITE}Weekly:{RESET} {GREEN}{weekly:>8,}{RESET}  "
                f"{WHITE}Pred:{RESET} {CYAN}{pred:>8,}{RESET}  "
                f"{WHITE}Join:{RESET} {JOIN_DATE_COLOR}{join}{RESET}"
            )
        print()

    if not any_found:
        print(f"{GRAY}No one is projected to hit weekly 0 within 0–3 days (under the '0 from now on' assumption).{RESET}\n")

# ============================================================
# EXTRA: MEMBERS + CODES VIEW
# ============================================================
def print_members_with_codes(members: List[Dict[str, Any]]) -> None:
    section_break("MEMBERS + CODES (REQS + PSEUDO)", color=PURPLE)
    print(f"{BOLD}{WHITE}{'IGN':<16} | {'Rank':<12} | {'Reqs':<18} | {'Pseudo':<18}{RESET}")
    print(f"{DIM}{'-' * 80}{RESET}")

    for m in members:
        ign = str(m.get("ign", ""))
        rank = str(m.get("rank", ""))
        reqs = str(m.get("reqs_met", "-"))
        pseudo = m.get("pseudo_codes") or []
        pseudo_txt = ",".join(pseudo) if pseudo else "-"
        req_cnt = _safe_int(m.get("reqs_met_count", 0), 0)
        req_col = GREEN if req_cnt >= 3 else (YELLOW if req_cnt == 2 else (ORANGE if req_cnt == 1 else GRAY))
        pseudo_col = YELLOW if pseudo else GRAY

        print(
            f"{CYAN}{ign:<16}{RESET} | "
            f"{WHITE}{rank:<12}{RESET} | "
            f"{req_col}{reqs:<18}{RESET} | "
            f"{pseudo_col}{pseudo_txt:<18}{RESET}"
        )
    print()

def print_requirements_summary(members: List[Dict[str, Any]]) -> None:
    section_break("REQUIREMENTS SUMMARY", color=PURPLE)

    total_including = len(members)
    filtered = [m for m in members if not is_req_whitelisted_member(m)]
    total = len(filtered)  # ✅ this is what % uses (excludes req-whitelist)

    def bucketize(get_count):
        b = {"0": 0, "1": 0, "2": 0, "3": 0, "3+": 0}
        for m in filtered:
            c = _safe_int(get_count(m), 0)
            if c <= 0:
                b["0"] += 1
            elif c == 1:
                b["1"] += 1
            elif c == 2:
                b["2"] += 1
            elif c == 3:
                b["3"] += 1
            else:
                b["3+"] += 1
        return b

    def pct(n: int) -> float:
        return (n / total * 100.0) if total > 0 else 0.0

    # ✅ tight row format: "1 REQ:  79 | (63.7%)"
    def row(label: str, n: int, label_col: str = WHITE, pct_col: str = GRAY) -> None:
        # keep percent visually quieter (dark grey)
        print(
            f"{label_col}{label:<7}{RESET} "
            f"{WHITE}{n:>4}{RESET} {GRAY}|{RESET} "
            f"{pct_col}({pct(n):5.1f}%){RESET}"
        )

    inc = bucketize(lambda m: m.get("reqs_met_count", 0))     # includes pseudo
    exc = bucketize(lambda m: m.get("real_reqs_count", 0))    # excludes pseudo

    print(f"{WHITE}Total Members:{RESET} {CYAN}{total}{RESET} {DIM}{GRAY}({total_including}){RESET}\n")

    def block(title: str, b: Dict[str, int]) -> None:
        meets_at_least_1 = total - b["0"]
        meets_0 = b["0"]

        # Header
        print(f"{BOLD}{CYAN}{title}{RESET}")

        # Main focus lines
        row("≥1 REQ:", meets_at_least_1, label_col=GREEN, pct_col=GRAY)
        row("0 REQ:",  meets_0,          label_col=RED,   pct_col=GRAY)

        print(f"{DIM}{GRAY}────────────────────────{RESET}")

        # Breakdown lines
        row("1 REQ:",  b["1"],  label_col=WHITE, pct_col=GRAY)
        row("2 REQ:",  b["2"],  label_col=WHITE, pct_col=GRAY)
        row("3 REQ:",  b["3"],  label_col=WHITE, pct_col=GRAY)
        # slightly different colour for 3+
        row("3+ REQ:", b["3+"], label_col=YELLOW, pct_col=GRAY)
        print()

    block("Including Pseudo", inc)
    block("Excluding Pseudo", exc)

def print_requirement_mode_counts(members: List[Dict[str, Any]]) -> None:
    section_break("REQUIREMENT MODE COUNTS", color=PURPLE)

    # ✅ Exclude requirement-whitelisted members from % totals (same as summary)
    filtered = [m for m in members if not is_req_whitelisted_member(m)]
    total = len(filtered)
    total_including = len(members)

    def pct(n: int) -> float:
        return (n / total * 100.0) if total > 0 else 0.0

    # Map requirement code -> display name + colour
    # (match your overall scheme: BW red, DU orange, SW yellow, AP green, SB purple, etc.)
    MODE_META: Dict[str, Tuple[str, str]] = {
        "BW": ("BEDWARS", RED),
        "DU": ("DUELS", ORANGE),
        "SW": ("SKYWARS", YELLOW),
        "AP": ("ACH PTS", GREEN),
        "BB": ("BUILD B", CYAN),
        "TNT": ("TNT", CYAN),
        "UHC": ("UHC", CYAN),
        "SB": ("SKYBLOCK", PURPLE),
    }

    # Count real requirements met (ignores pseudo)
    counts: Dict[str, int] = {k: 0 for k in MODE_META.keys()}

    for m in filtered:
        uuid = _normalize_uuid(m.get("uuid") or "")
        if not uuid:
            continue

        # We already computed these earlier in _prepare_members_for_outputs()
        real_cnt = _safe_int(m.get("real_reqs_count", 0), 0)
        if real_cnt <= 0:
            continue

        # Easiest/cleanest: recompute real req codes using the same logic
        # (uses cached blobs + throttling already built in)
        codes = _compute_real_reqs(uuid)

        # Dedup just in case
        for c in set(codes):
            if c in counts:
                counts[c] += 1

    # Row formatter: "BEDWARS:  30 | (24.1%)"
    def row(label: str, n: int, col: str) -> None:
        print(
            f"{BOLD}{col}{label:<10}{RESET} "
            f"{WHITE}{n:>4}{RESET} {GRAY}|{RESET} "
            f"{GRAY}({pct(n):5.1f}%){RESET}"
        )

    print(f"{WHITE}Total Members:{RESET} {CYAN}{total}{RESET} {DIM}{GRAY}({total_including}){RESET}\n")

    # Print in the same order as your REAL_REQS list
    order = ["AP", "BW", "BB", "DU", "SW", "TNT", "UHC", "SB"]
    for code in order:
        label, col = MODE_META.get(code, (code, WHITE))
        row(label + ":", counts.get(code, 0), col)

    print()
    print(f"{DIM}{GRAY}Note: counts are REAL requirements only (pseudo roles not included).{RESET}\n")

def show_zero_req_grids(members: List[Dict[str, Any]]) -> None:
    section_break("0-REQUIREMENT MEMBERS", color=RED)

    # ✅ ONLY remove requirement-whitelisted members
    base = [m for m in members if not is_req_whitelisted_member(m)]

    zero_inc = [m for m in base if _safe_int(m.get("reqs_met_count", 0), 0) == 0]
    zero_exc = [m for m in base if _safe_int(m.get("real_reqs_count", 0), 0) == 0]

    zero_inc.sort(key=lambda m: (rank_priority(m.get("rank")), str(m.get("ign", "")).lower()))
    zero_exc.sort(key=lambda m: (rank_priority(m.get("rank")), str(m.get("ign", "")).lower()))

    inc_cells = [_format_member_cell(m) for m in zero_inc]
    exc_cells = [_format_member_cell(m) for m in zero_exc]

    print(f"{DIM}{GRAY}Note: requirement-whitelisted members are excluded from these lists.{RESET}\n")

    _grid_print("Meet 0 requirements (INCLUDING pseudo)", inc_cells, cols=5, title_color=ORANGE)
    _grid_print("Meet 0 requirements (EXCLUDING pseudo)", exc_cells, cols=5, title_color=RED)


def print_requirements_legend() -> None:
    section_break("REQUIREMENTS LEGEND", color=PURPLE)
    print(f"{DIM}{WHITE}Real requirements:{RESET}")
    for code, short, desc in REAL_REQS:
        print(f"{DIM}{WHITE}- {code:<3}{RESET} {GRAY}{short:<20}{RESET} {DIM}{desc}{RESET}")

    defs = PSEUDO_REQS.get("defs", {})
    if defs:
        print(f"\n{DIM}{WHITE}Pseudo requirements (manual):{RESET}")
        for code in sorted(defs.keys()):
            meta = defs.get(code) or {}
            short = str(meta.get("short", "")).strip()
            desc = str(meta.get("desc", "")).strip()
            bonus = int(PSEUDO_PRIORITY_BONUSES.get(code, 0))
            if code == "LB":
                bonus = max(bonus, 10)
            bonus_txt = f"{GRAY}(+{bonus} prio){RESET} " if bonus else ""
            print(f"{DIM}{WHITE}- {code:<3}{RESET} {bonus_txt}{GRAY}{short:<20}{RESET} {DIM}{desc}{RESET}")
    print()


# ============================================================
# MAIN MENU
# ============================================================
def main_menu() -> str:
    while True:
        section_break("MAIN MENU", color=BLUE)
        print(f"{WHITE}1{RESET} - Show lists")
        print(f"{WHITE}2{RESET} - Enter pseudorole menu")
        print(f"{WHITE}3{RESET} - Manage whitelists")
        print(f"{WHITE}0{RESET} - Exit\n")

        choice = input(f"{DIM}Enter choice: {RESET}").strip()
        if choice in ("0", "1", "2", "3"):
            return choice
        print(f"{YELLOW}Unknown option.{RESET}\n")

# ============================================================
# SHOW LISTS MENU (pick which output you want)
# ============================================================
def show_lists_menu() -> str:
    while True:
        section_break("SHOW LISTS", color=CYAN)

        print(f"{DIM}{GRAY}Pick an option:{RESET}\n")

        print(f"{BOLD}{WHITE}1{RESET} - {BOLD}{PURPLE}Full leaderboard{RESET} {DIM}(choose order, ~125 lines){RESET}")

        print(f"{BOLD}{WHITE}2{RESET} - {BOLD}{CYAN}Kick recommendations — Wave 1{RESET}  {DIM}(cards, top 10){RESET}")
        print(f"{BOLD}{WHITE}3{RESET} - {BOLD}{ORANGE}Kick recommendations — Wave 2{RESET}  {DIM}(cards, top 10, joined > 7d){RESET}")

        print(
            f"{BOLD}{WHITE}4{RESET} - {BOLD}{YELLOW}0-GEXP soon list{RESET}  "
            f"{DIM}(0/{RESET}{BOLD}{RED}1{RESET}{DIM}/{RESET}{BOLD}{ORANGE}2{RESET}{DIM}/{RESET}{BOLD}{YELLOW}3{RESET}{DIM} grouped, includes already 0){RESET}"
        )

        print(f"{BOLD}{WHITE}5{RESET} - {BOLD}{YELLOW}Requirements legend{RESET}  {DIM}(codes + pseudo){RESET}")
        print(f"{BOLD}{WHITE}6{RESET} - {BOLD}{PURPLE}Requirements summary{RESET}  {DIM}(how many meet / don’t){RESET}")
        print(f"{BOLD}{WHITE}7{RESET} - {BOLD}{CYAN}Members + requirements{RESET}  {DIM}(per member list){RESET}")

        print(f"{BOLD}{WHITE}0{RESET} - {BOLD}{GRAY}Back{RESET}\n")

        c = input(f"{DIM}Enter choice: {RESET}").strip()
        if c in ("0", "1", "2", "3", "4", "5", "6", "7"):
            return c
        print(f"{YELLOW}Unknown option.{RESET}\n")

# ============================================================
# LISTS RUNNERS
# ============================================================
def _prepare_members_for_outputs(members: List[Dict[str, Any]]) -> None:
    if ENABLE_REQUIREMENT_CHECKS:
        print(f"{DIM}{GRAY}Checking real requirements (cached, throttled)...{RESET}")
    apply_requirements_to_members(members)
    print()

def run_kick_wave_1(members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    section_break("KICK RECOMMENDATIONS — WAVE 1", color=CYAN)
    recs = recommend_kicks(members, min_days_in_guild=0)
    print_kick_cards(
        title=f"Top {len(recs)} recommended members to kick (priority breakdown):",
        recs=recs,
        columns=2
    )
    return recs

def run_kick_wave_2(members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    section_break("KICK RECOMMENDATIONS — WAVE 2 (JOINED > 7 DAYS)", color=ORANGE)
    recs = recommend_kicks(members, min_days_in_guild=8)
    print_kick_cards(
        title=f"Top {len(recs)} recommended members to kick (joined > 7 days):",
        recs=recs,
        columns=2
    )
    return recs

def apply_kick_priority_into_members(members: List[Dict[str, Any]], *recs_lists: List[Dict[str, Any]]) -> None:
    # ✅ use UUID (not IGN) so duplicates / name-changes can't collide
    kick_priority_map: Dict[str, int] = {}
    for recs in recs_lists:
        for rec in (recs or []):
            uuid = _normalize_uuid(rec.get("uuid") or "")
            if not uuid:
                continue
            try:
                kp = int(rec.get("kick_priority", 0))
            except Exception:
                kp = 0
            if uuid not in kick_priority_map:
                kick_priority_map[uuid] = kp
            else:
                kick_priority_map[uuid] = min(kick_priority_map[uuid], kp)

    for m in members:
        uuid = _normalize_uuid(m.get("uuid") or "")
        m["kick_priority"] = kick_priority_map.get(uuid, "")

def run_full_leaderboard(guild: Dict[str, Any], members: List[Dict[str, Any]]) -> None:
    mode = get_display_order_choice()
    display_members = apply_display_order(members, mode)
    section_break("LEADERBOARD", color=BLUE)
    print_leaderboard(guild.get("name", "Lucid"), display_members)
    export_to_csv(display_members)

# ============================================================
# MAIN
# ============================================================
def main():
    if not API_KEY or API_KEY.strip() == "":
        print(f"{RED}HYPIXEL_API_KEY is missing. Set it in env to avoid hardcoding.{RESET}")
        return

    guild_name = "Lucid"

    while True:
        top_choice = main_menu()

        if top_choice == "0":
            break

        # Fresh fetch before entering either lists or pseudoroles/whitelist
        guild = get_guild_by_name(guild_name)
        members = extract_weekly_gexp(guild)

        if top_choice == "2":
            pseudo_reqs_menu(members)
            save_ign_cache(IGN_CACHE)
            save_player_cache(PLAYER_CACHE)
            print(f"{DIM}{GRAY}Returned to main menu.{RESET}\n")
            continue

        if top_choice == "3":
            manage_whitelists_menu(members)
            save_ign_cache(IGN_CACHE)
            save_player_cache(PLAYER_CACHE)
            save_kick_whitelist(KICK_WHITELIST)
            save_req_whitelist(REQ_WHITELIST)
            print(f"{DIM}{GRAY}Returned to main menu.{RESET}\n")
            continue


        # top_choice == "1" -> Show lists submenu
        while True:
            list_choice = show_lists_menu()
            if list_choice == "0":
                print(f"{DIM}{GRAY}Back to main menu.{RESET}\n")
                break

            # fresh again for each list action
            guild = get_guild_by_name(guild_name)
            members = extract_weekly_gexp(guild)

            _prepare_members_for_outputs(members)

            rec1: List[Dict[str, Any]] = []
            rec2: List[Dict[str, Any]] = []

            if list_choice == "1":
                run_full_leaderboard(guild, members)

            elif list_choice == "2":
                rec1 = run_kick_wave_1(members)
                apply_kick_priority_into_members(members, rec1)

            elif list_choice == "3":
                rec2 = run_kick_wave_2(members)
                apply_kick_priority_into_members(members, rec2)

            elif list_choice == "4":
                print_zero_soon_grouped(members, [0, 1, 2, 3])

            elif list_choice == "5":
                print_requirements_legend()

            elif list_choice == "6":
                print_requirements_summary(members)

                print()
                input(f"{DIM}{GRAY}Press Enter to show 0-requirement grids...{RESET}")
                show_zero_req_grids(members)

                print()
                input(f"{DIM}{GRAY}Press Enter to show requirement mode counts...{RESET}")
                print_requirement_mode_counts(members)

        
            elif list_choice == "7":
                print_members_with_codes(members)
            


            save_ign_cache(IGN_CACHE)
            save_player_cache(PLAYER_CACHE)
            save_kick_whitelist(KICK_WHITELIST)

            print()
            input(f"{DIM}Press Enter to continue...{RESET}")

    save_ign_cache(IGN_CACHE)
    save_player_cache(PLAYER_CACHE)
    save_kick_whitelist(KICK_WHITELIST)
    save_req_whitelist(REQ_WHITELIST)
    print(f"{DIM}{GRAY}Exiting.{RESET}")

if __name__ == "__main__":
    main()
