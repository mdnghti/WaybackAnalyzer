"""
analyzer.py — Website topic analyzer via Web Archive CDX API.

Pipeline per domain:
  1. CDX API  → up to 5 recent snapshots
  2. URL check  → skip if non-Latin characters in original URL
  3. Redirect check → skip if snapshot redirects to a different domain
  4. Language check → keep only 'en' or 'de'; others → "Wrong Language" / "Bad"
  5. Bad-keyword count → >3 matches marks domain "Bad"
  6. Topic scoring  → Game | Business | Other (majority vote)

Input : sites_to_check.xlsx  (Domain Rating, Backlinks, Followed, Domain)
Filter: Domain Rating >= 7  AND  Backlinks / Followed >= 10
Output: final_results.xlsx  — only sites with final Topic == "Game" or "Business"
"""

import re
import sys
import time
import logging
import warnings
import difflib
import collections
import requests
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse

try:
    import waybackpy
    import wayback
    FAILOVER_LIBS_OK = True
except ImportError:
    FAILOVER_LIBS_OK = False

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

import random

def _random_ua() -> str:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Edge/122.0.0.0"
    ]
    return random.choice(user_agents)

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

try:
    from langdetect import detect as _ld_detect
    LANGDETECT_OK = True
except ImportError:
    LANGDETECT_OK = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Force UTF-8 stdout to avoid Windows console UnicodeEncodeError for "→"
if sys.stdout.encoding.lower() != "utf-8" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
import os
import sys

def get_base_path():
    # PyInstaller
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    
    # Nuitka Onefile (ведет себя так, что __file__ в Temp, а sys.argv[0] - это исходный .exe)
    if 'NUITKA_ONEFILE_PARENT' in os.environ or '__compiled__' in globals() or (sys.argv and sys.argv[0].lower().endswith('.exe')):
        return os.path.dirname(os.path.abspath(sys.argv[0]))
        
    # Обычный запуск (и если Nuitka Standalone, где __file__ указывает куда надо)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_path()

# Теперь все пути строим ОТ BASE_DIR
INPUT_FILE = os.path.join(BASE_DIR, "sites_to_check.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "final_results.xlsx")

MIN_DOMAIN_RATING = 7
MIN_BL_FOLLOWED   = 10      # Backlinks / Followed ratio

SNAPSHOT_LIMIT    = 40       # CDX snapshots per domain
THREADS           = 2        # lowered — Wayback Machine blocks too many parallel connections
API_SLEEP         = 1.0      # seconds before each CDX call (avoid 429)
REQ_TIMEOUT       = 60       # seconds — Wayback sometimes thinks for a long time
RETRY_ATTEMPTS    = 3        # how many times to retry on connection error
RETRY_BACKOFF     = 2        # exponential base: 2, 4, 8 seconds

CDX_API  = "https://web.archive.org/cdx/search/cdx"
WB_FETCH = "https://web.archive.org/web/{ts}id_/{url}"

BASE_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Languages to keep (all others → skip snapshot / mark Bad)
ALLOWED_LANGS = {"en", "de"}

# Asian language codes that are hard signals for spam-drop
ASIAN_LANGS = {"zh-cn", "zh-tw", "zh", "ja", "ko", "vi", "th"}

# Bad-keyword raw-match threshold (>N matches → Bad)
BAD_COUNT_THRESHOLD = 3

# Zone weights for topic scoring
ZONE_WEIGHTS = {"title": 3, "h1": 3, "meta": 2, "body": 1}

# Only allow ASCII + common URL characters in original_url from CDX
_URL_SAFE_RE = re.compile(r"^[\x00-\x7F]*$")

# ---------------------------------------------------------------------------
# Keywords
# ---------------------------------------------------------------------------
GAME_KEYWORDS = [
    "game", "games", "gaming", "gamer", "gamers", "videogame", "videogames",
    "video game", "video games", "esports", "e-sports", "e sports",
    "game industry", "game development", "game reviews", "game news",
    "game engines", "game design", "game mechanics", "multiplayer",
    "single player", "rpg", "fps", "mmorpg", "strategy game", "action game",
    "adventure game", "indie game", "mobile game", "pc game", "console game",
    "virtual reality", "vr gaming", "augmented reality",
    "the legend of zelda", "tloz", "red dead redemption 2", "rdr2", "god of war",
    "assassin's creed valhalla", "horizon forbidden west", "uncharted 4",
    "the last of us", "spider-man", "star wars jedi", "tomb raider", "doom",
    "halo infinite", "battlefield", "far cry", "elden ring", "the witcher",
    "cyberpunk 2077", "the elder scrolls", "dragon age", "mass effect",
    "disco elysium", "persona 5 royal", "divinity: original sin 2", "dos2",
    "black desert online", "bdo online", "guild wars 2", "fortnite",
    "minecraft", "roblox", "warframe", "stardew valley", "hades",
    "among us", "hollow knight", "celeste", "cuphead", "undertale",
    "terraria", "slay the spire", "outer wilds", "world of warcraft",
    "wow", "wow classic", "wowgold", "the war within", "pandaria",
    "cataclysm classic", "wowcata", "cataclysm", "burning crusade", "wotlk",
    "wow gold", "call of duty", "cod", "final fantasy xiv", "ffxvi",
    "final fantasy", "diablo", "destiny 2", "new world", "path of exile",
    "poe", "escape from tarkov", "tarkov", "eft", "the first descendant",
    "throne and liberty", "ea sports fc", "fifa", "apex",
    "the finals", "last epoch", "overwatch 2", "overwatch",
    "albion online", "world of tanks", "wot", "lost ark", "deadlock",
    "rainbow 6 siege", "r6", "runescape", "osrs",
    "genshin impact", "league of legends", "lol", "dota 2", "dota",
    "valorant", "counter strike 2", "cs2", "csgo", "tarisland",
    "game engine", "unity", "unreal engine", "game programming",
    "pc building", "gaming pc", "graphics card", "gpu", "cpu",
    "vr headset", "gaming accessories", "streaming", "twitch",
    "youtube gaming", "game livestream", "modding", "game patches",
    "game updates", "game dlc", "game community", "game forums",
    "speedrun", "lets play", "game soundtrack",
    "grand theft auto", "gta", "baldur's gate 3", "bg3", "palworld",
    "fallout", "starfield", "manor lords", "gray zone warfare",
    "s.t.a.l.k.e.r. 2", "moba", "rts", "roguelike", "metroidvania",
    "jrpg", "crpg", "battle royale", "survival", "sandbox",
    "playstation", "xbox", "nintendo switch", "steam", "steam deck",
    "epic games store", "nvidia", "amd", "intel", "rtx", "gaming monitor",
    "gg", "afk", "noob", "glitch", "walkthrough", "guide",
    "discord", "subreddit", "e3", "gamescom", "the game awards",
    "blizzard", "valve", "riot games", "cd projekt red", "fromsoftware",
    "ubisoft", "ea", "activision", "bethesda", "rockstar games",
]

BUSINESS_KEYWORDS = [
    "business", "entrepreneur", "startup", "management", "corporate",
    "company", "industry", "enterprise", "ceo", "strategy", "consulting",
    "finance", "investment", "investing", "stocks", "trading", "economics",
    "market", "financial", "accounting", "audit", "venture capital",
    "assets", "portfolio", "marketing", "advertising", "sales", "seo",
    "smm", "digital marketing", "content marketing", "brand", "analytics",
    "e-commerce", "b2b", "b2c", "saas", "fintech", "agile", "scrum",
    "project management", "data analysis", "real estate", "property",
    "realty", "business news", "financial news", "forbes", "bloomberg",
    "wall street journal", "financial times",
]

BAD_KEYWORDS = [
    "casino", "casinos", "gambling", "bet", "bets", "betting", "poker",
    "slots", "1win", "porn", "cbd", "drugs", "marijuana", "weed", "kratom",
    "thc", "slot online", "slot gacor", "situs judi slot online",
    "situs slot online", "slot deposit pulsa tanpa potongan", "escort",
    "online poker", "blackjack", "roulette", "bookmaker", "sports betting",
    "lottery", "bingo", "judi online", "agen slot", "sex", "erotic", "xxx",
    "nude", "adult content", "camgirl", "forex", "crypto", "binary options",
    "get rich quick", "loan", "credit", "pharmacy", "viagra", "cialis",
    "essay service", "gun", "weapon", "firearms", "rifle", "pistol",
    "vape", "politics", "hate speech", "racism", "gore",
]


def _make_pattern(words: list[str]) -> re.Pattern:
    return re.compile(
        r"\b(" + "|".join(map(re.escape, words)) + r")\b",
        re.IGNORECASE,
    )


GAME_PAT     = _make_pattern(GAME_KEYWORDS)
BUSINESS_PAT = _make_pattern(BUSINESS_KEYWORDS)
BAD_PAT      = _make_pattern(BAD_KEYWORDS)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def _normalize_domain(domain: str) -> str:
    """Strip scheme, www., trailing slash for comparison."""
    d = domain.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.rstrip("/")


def _url_is_clean(url: str) -> bool:
    """Return False if URL contains non-ASCII characters (Cyrillic, CJK, etc.)."""
    return bool(_URL_SAFE_RE.match(url))


def _url_domain(url: str) -> str:
    """Extract normalized domain from a URL string."""
    try:
        parsed = urlparse(url if url.startswith("http") else "http://" + url)
        return _normalize_domain(parsed.netloc or parsed.path)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# HTTP session (shared, with retry-safe adapter)
# ---------------------------------------------------------------------------
_session = requests.Session()
_adapter = HTTPAdapter(max_retries=5)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)
_session.headers.update(BASE_HEADERS)


def _make_headers() -> dict:
    """Return headers with a fresh random User-Agent for each request."""
    return {**BASE_HEADERS, "User-Agent": _random_ua()}


# Sentinel: CDX returned a network-level error (not an empty archive)
_CDX_CONN_ERROR = object()


# ---------------------------------------------------------------------------
# CDX API
# ---------------------------------------------------------------------------
def get_snapshots(domain: str):
    """
    Query CDX for up to SNAPSHOT_LIMIT snapshots.

    Returns:
      - list of (timestamp, original_url) tuples on success
      - empty list [] if API replied 200 but found no snapshots
      - sentinel _CDX_CONN_ERROR on network / connection failure
    """
    domain = _normalize_domain(domain)   # strip trailing slash / scheme / www.
    # Build url param: avoid double slash when domain has no trailing slash
    cdx_url_param = domain.rstrip("/") + "/*"
    params = {
        "url":      cdx_url_param,
        "output":   "json",
        "fl":       "timestamp,original",
        "filter":   ["statuscode:200", "mimetype:text/html"],
        "collapse": "timestamp:10",
        "limit":    SNAPSHOT_LIMIT,
    }
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = _session.get(
                CDX_API,
                params=params,
                timeout=REQ_TIMEOUT,
                headers=_make_headers(),   # fresh random UA each attempt
            )
            if resp.status_code in (429, 503):
                wait = 10 * attempt
                log.warning(
                    f"  CDX {resp.status_code} for {domain} "
                    f"(attempt {attempt}/{RETRY_ATTEMPTS}), sleeping {wait}s"
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not data or len(data) < 2:
                return []          # 200 OK but truly no snapshots
            return [(row[0], row[1]) for row in data[1:]]
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait = 15
            log.warning(
                f"  Сетевая ошибка CDX для {domain} "
                f"(попытка {attempt}/{RETRY_ATTEMPTS}): {exc}. Принудительная пауза {wait}с"
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(wait)
            else:
                return _CDX_CONN_ERROR   # exhausted all retries
        except Exception as exc:
            log.warning(f"  Ошибка CDX для {domain}: {exc}")
            return _CDX_CONN_ERROR
    return _CDX_CONN_ERROR


# ---------------------------------------------------------------------------
# Snapshot fetcher with redirect / timeout handling
# ---------------------------------------------------------------------------
def _base_domain(normalized: str) -> str:
    """Return the last two labels of a normalized domain (base domain).

    Examples:
        www.site.com  →  site.com
        site.com      →  site.com
        sub.site.co.uk → site.co.uk  (best-effort; good enough for redirect detection)
    """
    parts = normalized.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else normalized


def clean_url(u: str) -> str:
    """Extract clean base domain from a URL (handles Wayback id_/ wrapping)."""
    if "id_/" in u:
        u = u.split("id_/", 1)[-1]
    
    u = u.lower().strip()
    if not u.startswith("http"):
        u = "http://" + u
        
    netloc = urlparse(u).netloc
    netloc = netloc.split(":")[0]  # remove port if present
    if netloc.startswith("www."):
        netloc = netloc[4:]
        
    parts = netloc.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else netloc


def fetch_snapshot(target_domain: str, timestamp: str, original_url: str) -> tuple[str | None, str]:
    """
    Download clean HTML via id_ modifier.

    Redirect rules (based on base-domain comparison):
      • protocol change (http → https)        → VALID, proceed to analysis
      • subdomain hop  (site.com → www.site.com) → VALID, proceed to analysis
      • path change    (site.com → site.com/en/) → VALID, proceed to analysis
      • cross-domain   (site.com → other.ru)     → SKIP snapshot

    Returns (html_or_None, skip_reason_or_redirect_note).
      - html is None  → snapshot skipped; skip_reason describes why
      - html is set   → proceed; redirect_note is '' (clean) or describes the move type
    """
    fetch_url = WB_FETCH.format(ts=timestamp, url=original_url)
    
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = _session.get(
                fetch_url,
                timeout=REQ_TIMEOUT,
                allow_redirects=True,
                headers=_make_headers(),
            )
            
            if resp.status_code in (429, 503):
                wait = 10 * attempt
                log.warning(
                    f"  Fetch {resp.status_code} for {target_domain} "
                    f"(attempt {attempt}/{RETRY_ATTEMPTS}), sleeping {wait}s"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()

            # Redirect check: compare clean base domains
            original_clean = clean_url(original_url)
            final_clean    = clean_url(resp.url)

            if original_clean != final_clean:
                # Real cross-domain redirect → skip
                return None, f"External Redirect: {final_clean}"

            # Check for internal redirect
            orig_embedded = urlparse(original_url.split("id_/", 1)[-1] if "id_/" in original_url else original_url)
            final_embedded = urlparse(resp.url.split("id_/", 1)[-1] if "id_/" in resp.url else resp.url)
            
            if orig_embedded.netloc != final_embedded.netloc or orig_embedded.path != final_embedded.path:
                redirect_note = "Internal redirect"
            else:
                redirect_note = ""

            return resp.text, redirect_note

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait = 15
            log.warning(
                f"  Сетевая ошибка при загрузке {target_domain} "
                f"(попытка {attempt}/{RETRY_ATTEMPTS}): {exc}. Принудительная пауза {wait}с"
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(wait)
            else:
                return None, "ConnectionError/Timeout"
        except requests.exceptions.TooManyRedirects:
            return None, "TooManyRedirects"
        except requests.exceptions.HTTPError as exc:
            return None, f"HTTP {exc.response.status_code}"
        except Exception as exc:
            return None, f"Fetch error: {exc}"
            
    return None, "Max retries exceeded"


# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------
def _extract_zones(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "lxml")

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    h1    = " ".join(t.get_text(" ", strip=True) for t in soup.find_all("h1"))

    meta_parts = []
    meta_refresh_url = ""
    for m in soup.find_all("meta"):
        # Check for meta refresh
        http_equiv = (m.get("http-equiv") or "").lower()
        if http_equiv == "refresh":
            content = m.get("content", "")
            # e.g. "0; url=http://example.com"
            match = re.search(r"url=[\"']?(https?://[^\s\"']+)[\"']?", content, re.IGNORECASE)
            if match:
                meta_refresh_url = match.group(1).strip()

        name = (m.get("name") or "").lower()
        if name in ("keywords", "description"):
            meta_parts.append(m.get("content", ""))
    meta = " ".join(meta_parts)

    for tag in soup(["script", "style", "meta", "noscript", "header", "footer", "nav"]):
        tag.decompose()
    body = re.sub(r"\s+", " ", soup.get_text(" ")).strip()

    return {"title": title, "h1": h1, "meta": meta, "body": body, "meta_refresh_url": meta_refresh_url}


def _weighted_count(pattern: re.Pattern, zones: dict[str, str]) -> int:
    return sum(
        len(pattern.findall(text)) * ZONE_WEIGHTS.get(zone, 1)
        for zone, text in zones.items()
    )


def _raw_count(pattern: re.Pattern, zones: dict[str, str]) -> int:
    """Total pattern matches across all zones (unweighted)."""
    return sum(len(pattern.findall(text)) for text in zones.values())


def _detect_language(text: str) -> str:
    if not LANGDETECT_OK or not text.strip():
        return "unknown"
    try:
        return _ld_detect(text[:2000])
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Per-snapshot classify
# ---------------------------------------------------------------------------
def classify_snapshot(html: str, target_base: str) -> tuple[str, str, str]:
    """
    Classify a single snapshot's HTML.
    Returns (topic, language, body_text).
    topic ∈ {Game, Business, Bad, Other, Wrong Language, Meta Refresh Redirect, Empty/Parked}
    """
    zones    = _extract_zones(html)
    
    # 1. Meta Refresh Check
    if zones["meta_refresh_url"]:
        refresh_netloc = urlparse(zones["meta_refresh_url"]).netloc
        if refresh_netloc:
            refresh_base = _base_domain(_normalize_domain(refresh_netloc))
            if refresh_base and refresh_base != target_base:
                return "Meta Refresh Redirect", "unknown", ""

    # 2. Empty/Parked Check
    if len(zones["body"]) < 400:
        return "Empty/Parked", _detect_language(zones["body"]), zones["body"]

    all_text = " ".join(val for k, val in zones.items() if k != "meta_refresh_url")
    language = _detect_language(zones["body"] or all_text)

    # Language gate: only en/de are accepted
    if language not in ALLOWED_LANGS and language != "unknown":
        if language in ASIAN_LANGS:
            return "Bad", language, zones["body"]
        return "Wrong Language", language, zones["body"]

    # Bad-keyword raw count (>3 → Bad)
    bad_count = _raw_count(BAD_PAT, zones)
    if bad_count > BAD_COUNT_THRESHOLD:
        return "Bad", language, zones["body"]

    # Topic scoring
    game_score = _weighted_count(GAME_PAT, zones)
    biz_score  = _weighted_count(BUSINESS_PAT, zones)

    if game_score > 0 and game_score >= biz_score:
        return "Game", language, zones["body"]
    if biz_score > 0:
        return "Business", language, zones["body"]
    return "Other", language, zones["body"]


# ---------------------------------------------------------------------------
# Per-domain pipeline
# ---------------------------------------------------------------------------
def analyze_domain(domain: str) -> dict:
    """
    Full multi-step pipeline for one domain.
    Returns result dict.
    """
    log.info(f"[START] {domain}")
    target = _normalize_domain(domain)
    target_base = _base_domain(target)

    base_result = {
        "Domain":             domain,
        "Topic":              "No archive",
        "Language":           "unknown",
        "Snapshots_analyzed": 0,
        "Snapshots_skipped":  0,
        "Skip_reasons":       "",
        "Frozen":             "No",
        "Success_Level":      0,
    }

    # Step 1: CDX (Level 1)
    time.sleep(API_SLEEP)
    success_level = 1
    snapshots = get_snapshots(domain)

    # Failover Mechanics (Level 2 & 3)
    if snapshots is _CDX_CONN_ERROR:
        if not FAILOVER_LIBS_OK:
            log.warning(f"[DONE]  {domain}  →  Connection Error (Failover libs 'waybackpy', 'wayback' missing)")
            return {**base_result, "Topic": "Connection Error"}

        # Level 2
        log.warning(f"[LEVEL 2] CDX не ответил, пробую Waybackpy для {domain}...")
        time.sleep(20)
        success_level = 2
        try:
            wb = waybackpy.Url(domain, _random_ua())
            newest = wb.newest()
            # extract string elements needed for fetch_snapshot wrapper
            match = re.search(r"/web/(\d{14})i?d?_?/(.+)$", newest.archive_url)
            if match:
                snapshots = [(match.group(1), match.group(2))]
            else:
                snapshots = []
        except Exception as e2:
            # Level 3
            log.warning(f"[LEVEL 3] Waybackpy не справился, пробую официальный Wayback Client для {domain}...")
            time.sleep(20)
            success_level = 3
            try:
                client = wayback.WaybackClient()
                results = client.search(domain)
                # Keep up to the 5 newest
                queue = collections.deque(results, maxlen=5)
                snapshots = []
                for r in queue:
                    ts = r.timestamp.strftime("%Y%m%d%H%M%S")
                    snapshots.append((ts, r.url))
            except Exception as e3:
                snapshots = _CDX_CONN_ERROR

    # Absolute network-level failure across all channels
    if snapshots is _CDX_CONN_ERROR:
        log.warning(f"[DONE]  {domain}  →  Connection Error (All 3 levels failed)")
        return {**base_result, "Topic": "Connection Error", "Success_Level": 0}

    if not snapshots:
        log.info(f"[DONE]  {domain}  →  No archive")
        return {**base_result, "Success_Level": success_level}

    votes:        list[str] = []
    langs:        list[str] = []
    skip_reasons: list[str] = []
    valid_bodies: list[str] = []

    # Keep track of valid languages to detect switches
    seen_langs:   set[str] = set()
    has_bad_snapshot = False

    for ts, orig_url in snapshots:
        date_str = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"

        # Step 2: URL character check
        if not _url_is_clean(orig_url):
            reason = "Non-ASCII URL"
            log.info(f"  Site {domain}: Снапшот от {date_str} — пропущен ({reason})")
            skip_reasons.append(f"{date_str}: {reason}")
            continue

        # Step 3: Fetch + redirect check
        html, redirect_note = fetch_snapshot(target, ts, orig_url)
        if html is None:
            # redirect_note is the skip reason when html is None
            log.info(f"  Сайт {domain}: Снапшот от {date_str} — пропущен ({redirect_note})")
            skip_reasons.append(f"{date_str}: {redirect_note}")
            continue

        if redirect_note:
            log.info(
                f"  Сайт {domain}: Снапшот принят "
                f"({redirect_note}) → Переход к анализу"
            )

        # Step 4 & 5: Classify (language + bad-keyword + topic)
        topic, lang, body = classify_snapshot(html, target_base)

        if topic == "Meta Refresh Redirect":
            reason = "Meta Refresh Redirect"
            log.info(f"  Site {domain}: Snapshot {date_str} — skipped ({reason})")
            skip_reasons.append(f"{date_str}: {reason}")
            continue

        if topic == "Empty/Parked":
            reason = "Empty or Parked (<400 chars)"
            log.info(f"  Site {domain}: Snapshot {date_str} — skipped ({reason})")
            skip_reasons.append(f"{date_str}: {reason}")
            continue

        # Language Switch check
        if lang != "unknown":
            seen_langs.add(lang)
            if len(seen_langs) > 1:
                reason = f"Смена языка: {seen_langs}"
                log.warning(f"[{domain}] → Спам-смена языка (Language Switch)")
                return {**base_result, "Topic": "Bad (Language Switch)", "Language": lang}

        if topic in ("Wrong Language", "Bad") and lang not in ALLOWED_LANGS and lang != "unknown":
            reason = f"Язык/Мусор: {lang}"
            log.info(f"  Site {domain}: Снапшот от {date_str} — пропущен ({reason}) → {topic}")
            skip_reasons.append(f"{date_str}: {reason}")
            has_bad_snapshot = True  # Veto: if it was EVER bad, it can't win majority vote
            # Still vote — if ALL snapshots are bad-lang it bubbles up
            votes.append(topic)
            langs.append(lang)
            continue

        log.info(f"  Site {domain}: Снапшот от {date_str} — принят → {topic} ({lang})")
        votes.append(topic)
        langs.append(lang)
        valid_bodies.append(body)

    skipped = len(skip_reasons)
    analyzed = len(votes)

    if not votes:
        base_result["Snapshots_skipped"] = skipped
        base_result["Skip_reasons"]      = " | ".join(skip_reasons)
        log.info(f"[DONE]  {domain}  →  No usable snapshots")
        return base_result

    # Priority Vote + Veto Override
    game_count = votes.count("Game")
    biz_count  = votes.count("Business")
    
    if has_bad_snapshot:
        # VETO: if the site ever hosted bad/wrong language content, deny it completely
        final_topic = "Bad"
    elif game_count >= 2 and game_count >= biz_count:
        final_topic = "Game"
    elif biz_count >= 2:
        final_topic = "Business"
    else:
        final_topic = max(set(votes), key=votes.count)

    final_lang  = max(set(langs),  key=langs.count)
    
    # Frozen check: strictly only compares valid (accepted, non-spam) bodies
    frozen_status = "No"
    if not has_bad_snapshot and len(valid_bodies) >= 2:
        # Compare oldest and newest accepted snapshots
        similarity = difflib.SequenceMatcher(None, valid_bodies[0], valid_bodies[-1]).ratio()
        if similarity >= 0.95:
            frozen_status = "Yes"

    log.info(f"[DONE]  {domain}  →  {final_topic}  (lang={final_lang}, votes={votes}, frozen={frozen_status}, level={success_level})")

    return {
        "Domain":             domain,
        "Topic":              final_topic,
        "Language":           final_lang,
        "Snapshots_analyzed": analyzed,
        "Snapshots_skipped":  skipped,
        "Skip_reasons":       " | ".join(skip_reasons),
        "Frozen":             frozen_status,
        "Success_Level":      success_level,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # 1. Read & filter Excel
    log.info(f"Reading {INPUT_FILE} …")
    df = pd.read_excel(INPUT_FILE)
    df.columns = df.columns.str.strip()

    def _col(candidates: list[str]) -> str:
        for c in candidates:
            for col in df.columns:
                if col.lower() == c.lower():
                    return col
        raise KeyError(f"None of {candidates} found in columns: {list(df.columns)}")

    col_domain = _col(["Domain", "domain", "site", "url", "Target"])
    col_dr     = _col(["Domain Rating", "DR", "domain_rating"])
    col_bl     = _col(["Backlinks / All", "Backlinks", "backlinks"])
    col_follow = _col(["Backlinks / Followed", "Followed", "followed", "dofollow"])

    log.info(f"Total rows: {len(df)}")

    # --- Debug: column dtypes right after load ---
    log.info(f"Column dtypes:\n{df.dtypes.to_string()}")

    # Coerce metric columns to numeric (handles "7+", blanks, etc.)
    df[col_dr]     = pd.to_numeric(df[col_dr],     errors="coerce")
    df[col_bl]     = pd.to_numeric(df[col_bl],     errors="coerce")
    df[col_follow] = pd.to_numeric(df[col_follow], errors="coerce")

    # Drop rows where key metrics couldn't be parsed
    df = df.dropna(subset=[col_dr, col_follow]).copy()
    log.info(f"Rows after dropna on [{col_dr}, {col_follow}]: {len(df)}")

    # --- Debug: DR range actually present in the file ---
    if not df.empty:
        log.info(
            f"Domain Rating range in file: "
            f"min={df[col_dr].min()}, max={df[col_dr].max()}"
        )

    # Filter: DR >= 7 AND Backlinks / Followed >= 10
    filtered = df[
        (df[col_dr] >= MIN_DOMAIN_RATING) &
        (df[col_follow] >= MIN_BL_FOLLOWED)
    ].copy()

    log.info(
        f"After filter (DR≥{MIN_DOMAIN_RATING}, {col_follow}≥{MIN_BL_FOLLOWED}): "
        f"{len(filtered)} sites"
    )

    if filtered.empty:
        log.warning("No sites passed the filter. Exiting.")
        log.warning(f"First 5 rows of source data for diagnosis:\n{df[[col_domain, col_dr, col_follow]].head(5).to_string()}")
        return

    # Normalize domain strings and deduplicate while preserving order
    raw_domains = filtered[col_domain].astype(str).str.strip().tolist()
    domains = list(dict.fromkeys(_normalize_domain(d) for d in raw_domains))
    log.info(f"Unique domains to analyze: {len(domains)}")

    # 2. Parallel analysis
    log.info(f"Launching {THREADS} worker threads …")
    analysis: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_map = {executor.submit(analyze_domain, d): d for d in domains}
        for future in as_completed(future_map):
            domain = future_map[future]
            try:
                analysis[domain] = future.result()
            except Exception as exc:
                log.error(f"Unhandled error for {domain}: {exc}")
                analysis[domain] = {
                    "Domain": domain, "Topic": "Error",
                    "Language": "unknown", "Snapshots_analyzed": 0,
                    "Snapshots_skipped": 0, "Skip_reasons": str(exc),
                }

    # 3. Merge results
    def _get(d, key):
        # Must normalize Excel domain to match analysis dictionary keys
        normalized_key = _normalize_domain(str(d))
        return analysis.get(normalized_key, {}).get(key, "")

    filtered = filtered.copy()
    for col in ("Topic", "Language", "Snapshots_analyzed", "Snapshots_skipped", "Skip_reasons", "Frozen", "Success_Level"):
        filtered[col] = filtered[col_domain].map(lambda d, c=col: _get(d, c))

    unique_topics = tuple(filtered["Topic"].unique())
    log.info(f"Detected topics in dataframe: {unique_topics}")

    # 4. Save final: only Game or Business (case-insensitive check)
    final = filtered[filtered["Topic"].str.lower().isin(["game", "business"])].copy()
    
    if final.empty:
        log.warning("No sites matched 'Game' or 'Business' after analysis.")
        log.warning(f"Debug: Top 5 rows of merged table:\n{filtered[[col_domain, 'Topic']].head(5).to_string()}")
    else:
        log.info(f"Saving {len(final)} sites (Topic=Game|Business) to {OUTPUT_FILE} …")
        final.to_excel(OUTPUT_FILE, index=False)
        
    log.info("Done!")

    # Summary
    topic_counts = filtered["Topic"].value_counts()
    log.info(f"\n{'='*40}\nSummary (all filtered sites):\n{topic_counts.to_string()}\n{'='*40}")


if __name__ == "__main__":
    main()
