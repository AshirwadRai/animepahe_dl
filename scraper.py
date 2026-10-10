"""
scraper.py  —  animepahe API client, session management and HLS downloader.

Cloudflare on animepahe uses IUAM/Turnstile (real browser fingerprint check),
which plain HTTP clients cannot pass. Session setup tries, in order:
  1. Cached session  - cookies + user-agent saved in .session_cache.json
                       by a previous run (reused for up to 3 hours).
  2. Manual cookie   - the ANIMEPAHE_CF_CLEARANCE env var, copied from your
                       own browser.
  3. nodriver        - launches undetected Chrome, waits for the challenge to
                       clear and captures the clearance cookies.

All later requests go through curl_cffi with a Chrome TLS fingerprint and the
same user-agent, so Cloudflare accepts the captured cookies.

Download pipeline (see download_vid):
  m3u8 playlist -> parallel segment download (resumable) -> AES key ->
  local m3u8 -> ffmpeg decrypt + mux -> MP4 with cover art and chapters.

See README.md for setup instructions.
"""

import re
import asyncio
import time
import os
import json
import subprocess
import sys
import tempfile
import shutil
import warnings
import concurrent.futures
import threading
import random
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

from curl_cffi import requests as cffi_requests
from urllib.parse import urlparse, quote
from colorama import Fore

# ── Windows asyncio cleanup fix ───────────────────────────────────────────
# ProactorEventLoop (default on Windows) raises "I/O operation on closed pipe"
# during garbage collection of pipe transports after the event loop closes.
# This is harmless noise (https://bugs.python.org/issue39232). Silence it.
if sys.platform == "win32":
    from functools import wraps
    try:
        from asyncio.proactor_events import _ProactorBasePipeTransport

        _orig_del = _ProactorBasePipeTransport.__del__

        @wraps(_orig_del)
        def _silenced_del(self, *args, **kwargs):
            try:
                _orig_del(self, *args, **kwargs)
            except (ValueError, OSError, RuntimeError):
                pass

        _ProactorBasePipeTransport.__del__ = _silenced_del
    except (ImportError, AttributeError):
        pass  # non-Windows or future Python version without this class

    # Also silence asyncio RuntimeWarning about dangling event loops / pipes
    warnings.filterwarnings("ignore", message=".*I/O operation on closed pipe.*")
    warnings.filterwarnings("ignore", category=ResourceWarning, message=".*unclosed.*")

# ── Config ────────────────────────────────────────────────────────────────

BASE_URL = "https://animepahe.pw"    # confirmed live domain (animepahe.org redirects here)

# ── Manual cookie fallback ────────────────────────────────────────────────────
# If nodriver doesn't work, set the ANIMEPAHE_CF_CLEARANCE environment variable
# with your cf_clearance cookie value.  It expires in ~30 minutes so you may
# need to refresh it.
#
# Example:   set ANIMEPAHE_CF_CLEARANCE=abc123xyz...   (Windows)
#            export ANIMEPAHE_CF_CLEARANCE=abc123xyz... (Linux/macOS)
MANUAL_CF_CLEARANCE = os.environ.get("ANIMEPAHE_CF_CLEARANCE", "")

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.5

# Session cache file - stores cookies + user-agent between runs so you
# don't have to wait for Chrome to solve Cloudflare every time.
_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".session_cache.json")

# ── Download configuration ───────────────────────────────────────────────────
_DL_WORKERS = 8             # segment-download threads per episode
_DL_RATE_LIMIT = 12         # max concurrent CDN connections across ALL episodes
_DL_MAX_RETRIES = 5         # per-segment max retries
_DL_BASE_BACKOFF = 1.0      # initial backoff seconds
_DL_MAX_BACKOFF = 30.0      # max backoff seconds
_DL_JITTER = 0.3            # jitter factor (0-1)
_DL_TIMEOUT = 60            # per-segment timeout (seconds)

# Resume/state file
_STATE_FILE_SUFFIX = ".download_state.json"

# Structured logging
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads.log.jsonl")


@dataclass
class EpisodeMeta:
    """Episode metadata embedded into the final MP4."""
    episode: int | float
    title: str = ""
    duration: str = ""  # HH:MM:SS (filled from the m3u8 playlist at download time)
    snapshot_url: str = ""
    audio: str = ""

    def duration_seconds(self) -> int:
        """Parse duration string (HH:MM:SS or MM:SS) to seconds."""
        try:
            parts = [int(p) for p in self.duration.split(':')]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
        except ValueError:
            pass
        return 0

    def chapter_times(self) -> list[int]:
        """
        Approximate chapter boundaries in seconds, giving three chapters:
        Opening, Episode, Ending.

        animepahe doesn't expose real OP/ED timings, so these are estimates
        based on the typical TV-anime layout (OP ends ~1:30 in, ED starts
        ~1:30 before the end). They make seeking easier, nothing more.
        """
        total = self.duration_seconds()
        if total <= 0:
            return []

        chapters = [0]
        if total > 300:  # only episodes of 5+ minutes have a recognisable OP/ED
            chapters.append(min(90, total // 4))
            chapters.append(max(total - 90, chapters[-1] + 60))
        chapters.append(total)
        return sorted(set(chapters))


# Play pages are needed twice per episode (download links, then metadata).
# Caching the HTML halves the requests to animepahe, which rate-limits hard.
_play_page_cache: dict[str, str] = {}


def _get_play_page(anime_id: str, ep_session: str) -> str:
    """Return the HTML of an episode's play page (cached per run)."""
    url = f"{BASE_URL}/play/{anime_id}/{ep_session}"
    if url not in _play_page_cache:
        resp = _get(url, referer=f"{BASE_URL}/")
        resp.raise_for_status()
        _play_page_cache[url] = resp.text
    return _play_page_cache[url]


def fetch_episode_meta(anime_id: str, ep_session: str) -> EpisodeMeta | None:
    """Extract title, thumbnail and audio type from the episode's play page."""
    try:
        html = _get_play_page(anime_id, ep_session)

        # Episode thumbnail, used as cover art
        snapshot_match = re.search(
            r'(https?://i\.animepahe\.[a-z]+/uploads/snapshots/[^\s"\'`]+)', html, re.IGNORECASE
        )
        snapshot_url = snapshot_match.group(1) if snapshot_match else ""

        # Page <title>, e.g. "Death Note Ep. 1 :: animepahe"
        title_match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else ""
        title = re.sub(r'\s*::\s*animepahe.*$', '', title, flags=re.IGNORECASE)

        aud_match = re.search(r'audio[\s:="\'`]*(eng|jpn|sub|dub)', html, re.IGNORECASE)
        audio = aud_match.group(1).upper() if aud_match else ""

        return EpisodeMeta(episode=0, title=title, snapshot_url=snapshot_url, audio=audio)
    except Exception as e:
        _log_event("meta_fetch_failed", error=str(e), anime_id=anime_id, ep_session=ep_session)
        return None


def fetch_all_episode_meta(anime_id: str, eps: dict, requested_eps: list) -> dict:
    """Return {episode: EpisodeMeta} for the requested episodes.

    Normally costs no extra requests: the play pages were already fetched
    (and cached) while collecting download links.
    """
    meta = {}
    for ep_num in requested_eps:
        ep_meta = fetch_episode_meta(anime_id, eps[ep_num])
        if ep_meta:
            ep_meta.episode = ep_num
            meta[ep_num] = ep_meta
    return meta


def download_thumbnail(url: str, tmp_dir: str) -> str | None:
    """Download the episode thumbnail and return a JPEG path usable as MP4 cover art.

    MP4 can't hold WebP as an attached picture, so other formats are
    converted to JPEG (Pillow first, ffmpeg as fallback). Returns None if
    anything fails — the episode is then simply saved without a cover.
    """
    if not url:
        return None
    try:
        resp = _session.get(url, headers={"User-Agent": _user_agent, "Referer": BASE_URL + "/"},
                            timeout=30)
        resp.raise_for_status()

        ext = os.path.splitext(urlparse(url).path)[1].lower() or ".img"
        raw_path = os.path.join(tmp_dir, f"cover_src{ext}")
        with open(raw_path, "wb") as f:
            f.write(resp.content)

        if ext in (".jpg", ".jpeg"):
            return raw_path

        jpg_path = os.path.join(tmp_dir, "cover.jpg")
        try:
            from PIL import Image
            with Image.open(raw_path) as img:
                img.convert("RGB").save(jpg_path, "JPEG", quality=90)
        except Exception:
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", raw_path, jpg_path],
                           capture_output=True, check=False)
        return jpg_path if os.path.exists(jpg_path) else None
    except Exception as e:
        _log_event("thumb_download_failed", error=str(e), url=url)
        return None


# Caps simultaneous connections to the video CDN. Shared by every episode
# downloading in parallel, so the total load stays polite (the CDN answers
# with 429s when hammered).
_cdn_slots = threading.Semaphore(_DL_RATE_LIMIT)


@dataclass
class SegmentState:
    """State for a single segment download."""
    index: int
    url: str
    filename: str
    downloaded: bool = False
    size: int = 0
    retries: int = 0
    last_error: str = ""
    checksum: str = ""  # unused; kept so state files from older versions still load


@dataclass
class DownloadState:
    """Download progress saved next to the temp dir, so an interrupted
    episode can resume instead of starting over."""
    m3u8_url: str
    title: str
    ep: str
    total_segments: int
    segments: List[SegmentState]
    key_urls: List[str]
    key_map: Dict[str, str]  # remote_url -> local_filename
    started_at: float
    updated_at: float
    temp_dir: str
    local_m3u8_path: str
    output_file: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, data: str) -> "DownloadState":
        d = json.loads(data)
        d["segments"] = [SegmentState(**s) for s in d["segments"]]
        return cls(**d)


def _log_event(event_type: str, **kwargs):
    """Append a structured entry to downloads.log.jsonl (for debugging failures)."""
    entry = {"timestamp": time.time(), "event": event_type, **kwargs}
    try:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # Logging failures should never break the download


def _load_download_state(state_path: str) -> Optional[DownloadState]:
    """Load download state from sidecar file."""
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return DownloadState.from_json(f.read())
    except Exception:
        return None


def _save_download_state(state: DownloadState):
    """Atomically write download state (write temp file, then replace)."""
    state.updated_at = time.time()
    path = state.temp_dir + _STATE_FILE_SUFFIX
    try:
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            f.write(state.to_json())
        os.replace(path + ".tmp", path)
    except Exception as e:
        _log_event("state_save_failed", error=str(e), state_path=path)


def _verify_segment(path: str, expected_size: int = 0) -> bool:
    """Verify segment file exists, is non-empty and (if known) has the expected size."""
    try:
        actual_size = os.path.getsize(path)
    except OSError:
        return False
    if expected_size and actual_size != expected_size:
        return False
    return actual_size > 0


# ── Session state ─────────────────────────────────────────────────────────────
_session = cffi_requests.Session(impersonate="chrome")

# Retry config (applied manually in _get since curl_cffi doesn't support HTTPAdapter)
_RETRY_TOTAL = 3
_RETRY_BACKOFF = 1
_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_RETRY_429_BASE_BACKOFF = 5      # longer base backoff specifically for rate-limit responses
_RETRY_429_MAX_BACKOFF = 60      # cap for 429 backoff

_cf_cookies: dict = {}      # populated by nodriver or manual mode
_user_agent: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_session_initialised = False


# ── Session cache ─────────────────────────────────────────────────────────────

def _save_session_cache():
    """Save current session cookies + user-agent to disk for reuse."""
    cache = {
        "user_agent": _user_agent,
        "cookies": dict(_session.cookies),
        "base_url": BASE_URL,
        "saved_at": time.time(),
    }
    try:
        with open(_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
        print(Fore.GREEN + "[cache] session saved -> reuse on next run" + Fore.RESET)
    except OSError as e:
        print(Fore.YELLOW + f"[cache] could not save: {e}" + Fore.RESET)


def _try_cached_session() -> bool:
    """
    Try to load a cached session from disk. If the cookies are still valid
    (tested with a quick API call), return True and skip all solvers.
    """
    global _user_agent, _cf_cookies

    if not os.path.exists(_CACHE_FILE):
        return False

    try:
        with open(_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False

    # Check age - cf_clearance usually expires in 30 min, but can last longer.
    # Reject caches older than 3 hours to be safe.
    age_s = time.time() - cache.get("saved_at", 0)
    if age_s > 3 * 3600:
        print(Fore.YELLOW + f"[cache] expired ({age_s/3600:.1f}h old), re-solving..." + Fore.RESET)
        return False

    cookies = cache.get("cookies", {})
    if "cf_clearance" not in cookies:
        return False

    # Load cached values into the session
    _user_agent = cache.get("user_agent", _user_agent)
    _session.cookies.update(cookies)
    _cf_cookies = cookies

    # Quick validation - try a lightweight API call
    age_min = age_s / 60
    try:
        resp = _session.get(
            f"{BASE_URL}/api?m=search&q=test",
            headers=_build_headers(),
            timeout=30,
        )
        if resp.status_code == 200:
            print(Fore.GREEN + f"[cache] loaded saved session ({age_min:.0f}m old) [OK]" + Fore.RESET)
            return True
        elif resp.status_code == 403:
            print(Fore.YELLOW + "[cache] cookies expired (403), re-solving..." + Fore.RESET)
            _session.cookies.clear()
            return False
        else:
            # Other errors (500, etc.) - assume cookies are fine, server is just being flaky
            print(Fore.YELLOW + f"[cache] server returned {resp.status_code}, using cached session anyway ({age_min:.0f}m old)" + Fore.RESET)
            return True
    except cffi_requests.exceptions.Timeout:
        # Server slow but not blocked - trust the cache if it's recent
        if age_s < 30 * 60:  # less than 30 min old
            print(Fore.YELLOW + f"[cache] server slow, using cached session ({age_min:.0f}m old)" + Fore.RESET)
            return True
        else:
            print(Fore.YELLOW + f"[cache] server timeout + old cache ({age_min:.0f}m), re-solving..." + Fore.RESET)
            _session.cookies.clear()
            return False
    except Exception:
        # Network error - trust recent cache
        if age_s < 30 * 60:
            print(Fore.YELLOW + f"[cache] network error, using cached session ({age_min:.0f}m old)" + Fore.RESET)
            return True
        _session.cookies.clear()
        return False


def _build_headers(referer: str = None) -> dict:
    headers = {
        "User-Agent": _user_agent,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Referer": referer or f"{BASE_URL}/",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
    }
    return headers


# ── nodriver integration ──────────────────────────────────────────────────────

def _try_nodriver() -> bool:
    """
    Use nodriver (undetected Chrome) to solve the Cloudflare challenge locally.
    Opens a real Chrome window, waits for the challenge to clear, grabs cookies.
    Returns True on success.
    """
    try:
        import nodriver as uc
    except ImportError:
        print(Fore.YELLOW + "[nodriver] not installed. Run: pip install nodriver" + Fore.RESET)
        return False

    print(Fore.CYAN + "[nodriver] launching Chrome to solve Cloudflare challenge..." + Fore.RESET)
    print(Fore.CYAN + "[nodriver] a browser window will open - please don't close it" + Fore.RESET)

    async def _solve():
        global _cf_cookies, _user_agent, BASE_URL

        browser = await uc.start()
        page = await browser.get(f"{BASE_URL}/")

        # Wait for Cloudflare to clear (up to 120 seconds)
        max_wait = 120
        poll_interval = 3
        elapsed = 0

        while elapsed < max_wait:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            # The challenge page is titled "Just a moment..."; once it clears,
            # Cloudflare redirects to the real site (title "animepahe :: ...").
            # page.title is often stale in nodriver, so ask the DOM directly.
            try:
                doc_title = str(await page.evaluate("document.title") or "")
            except Exception:
                doc_title = ""

            if doc_title and "just a moment" not in doc_title.lower():
                # cf_clearance can show up a few seconds after the redirect
                try:
                    names = {c.name for c in await browser.cookies.get_all()}
                except Exception:
                    names = set()
                if "cf_clearance" in names:
                    print(Fore.GREEN + f"[nodriver] challenge cleared in {elapsed}s [OK]" + Fore.RESET)
                    break

            if elapsed % 15 == 0:
                print(Fore.YELLOW + f"[nodriver] still solving challenge... ({elapsed}s)" + Fore.RESET)
        else:
            print(Fore.RED + f"[nodriver] timed out after {max_wait}s" + Fore.RESET)
            try:
                browser.stop()
            except Exception:
                pass
            return False

        # Update BASE_URL if we got redirected to a new domain
        try:
            final_url = str(await page.evaluate("location.href") or "")
            if final_url.startswith("http"):
                parsed = urlparse(final_url)
                new_base = f"{parsed.scheme}://{parsed.netloc}"
                if new_base != BASE_URL:
                    BASE_URL = new_base
                    print(Fore.CYAN + f"[nodriver] updated base URL to {BASE_URL}" + Fore.RESET)
        except Exception:
            pass

        # Extract cookies
        try:
            all_cookies = await browser.cookies.get_all()
            cookie_dict = {}
            for c in all_cookies:
                cookie_dict[c.name] = c.value
            _cf_cookies = cookie_dict
        except Exception as e:
            print(Fore.YELLOW + f"[nodriver] cookie extraction error: {e}" + Fore.RESET)
            try:
                browser.stop()
            except Exception:
                pass
            return False

        # Extract user agent
        try:
            ua_result = await page.evaluate("navigator.userAgent")
            if ua_result:
                _user_agent = str(ua_result)
        except Exception:
            pass

        try:
            browser.stop()
        except Exception:
            pass

        if "cf_clearance" in _cf_cookies:
            _session.cookies.update(_cf_cookies)
            print(Fore.GREEN + f"[nodriver] {len(_cf_cookies)} cookies captured (cf_clearance [OK])" + Fore.RESET)
            return True
        else:
            print(Fore.YELLOW + f"[nodriver] no cf_clearance found in cookies: {list(_cf_cookies.keys())}" + Fore.RESET)
            # Still load whatever cookies we got
            _session.cookies.update(_cf_cookies)
            return True  # Try anyway - some pages work without cf_clearance

    try:
        return asyncio.run(_solve())
    except Exception as e:
        print(Fore.RED + f"[nodriver] error: {e}" + Fore.RESET)
        return False
    finally:
        # Suppress harmless asyncio cleanup warnings on Python 3.14+ / Windows
        warnings.filterwarnings("ignore", message="unclosed transport", category=ResourceWarning)


def _use_manual_cookies():
    """Load the manually-pasted cf_clearance into the session."""
    global _cf_cookies
    if not MANUAL_CF_CLEARANCE:
        return
    _cf_cookies = {
        "cf_clearance": MANUAL_CF_CLEARANCE,
    }
    _session.cookies.update(_cf_cookies)
    print(Fore.GREEN + "[auth] manual cf_clearance loaded (from ANIMEPAHE_CF_CLEARANCE env var)" + Fore.RESET)


def init_session():
    """
    Initialise the session. Called once at startup.
    Priority: cached session -> manual cookies -> nodriver -> bare session.
    """
    global _session_initialised
    if _session_initialised:
        return

    # 0. Try cached session first (instant if cookies are still valid)
    if not MANUAL_CF_CLEARANCE and _try_cached_session():
        _session_initialised = True
        return

    if MANUAL_CF_CLEARANCE:
        _use_manual_cookies()
    else:
        ok = _try_nodriver()
        if ok:
            # Save session for reuse on next run
            _save_session_cache()
        else:
            # No solver available - bare session
            print(
                Fore.YELLOW +
                "[warn] nodriver failed and no manual cookie set.\n"
                "       Requests may be blocked by Cloudflare.\n"
                "       See README.md for setup instructions." +
                Fore.RESET
            )

    _session_initialised = True


def _get(url: str, **kwargs):
    """Rate-limited GET using the initialised session, with manual retry logic.

    429 (Too Many Requests) gets a dedicated longer backoff (5s base,
    exponential up to 60s) and respects the server's Retry-After header.
    Other retryable status codes keep the original short backoff.
    """
    time.sleep(REQUEST_DELAY)
    init_session()
    headers = _build_headers(kwargs.pop("referer", None))
    last_err = None
    for attempt in range(_RETRY_TOTAL + 1):
        try:
            resp = _session.get(url, headers=headers, timeout=30, **kwargs)
            if resp.status_code in _RETRY_STATUS_CODES and attempt < _RETRY_TOTAL:
                last_err = Exception(f"HTTP {resp.status_code}")
                if resp.status_code == 429:
                    # Respect Retry-After header if the server sends one
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = min(float(retry_after), _RETRY_429_MAX_BACKOFF)
                        except ValueError:
                            wait = _RETRY_429_BASE_BACKOFF * (2 ** attempt)
                    else:
                        wait = min(_RETRY_429_BASE_BACKOFF * (2 ** attempt),
                                   _RETRY_429_MAX_BACKOFF)
                    # Add jitter to avoid thundering herd
                    wait += random.uniform(0, wait * 0.25)
                else:
                    wait = _RETRY_BACKOFF * (2 ** attempt)
                time.sleep(wait)
                continue
            return resp
        except Exception as e:
            last_err = e
            if attempt < _RETRY_TOTAL:
                time.sleep(_RETRY_BACKOFF * (2 ** attempt))
                continue
            raise
    # Unreachable: the final attempt always returns or raises above
    raise last_err or RuntimeError(f"request failed: {url}")


# ── Search ────────────────────────────────────────────────────────────────────

def get_query(query: str) -> dict:
    """
    Search animepahe. Returns { title: [status, episodes, score, session_id] }
    """
    url = f"{BASE_URL}/api?m=search&q={quote(query)}"
    resp = _get(url)
    resp.raise_for_status()
    data = resp.json()

    results = {}
    for item in data.get("data", []):
        results[item["title"]] = [
            item["status"],
            item["episodes"],
            item["score"],
            item["session"],
        ]
    return results


def show_results_get_id(results: dict) -> list:
    """Pretty-print search results, return list of session ids in order."""
    print("\nAvailable results:")
    ids = []
    for n, (title, info) in enumerate(results.items(), start=1):
        status, eps, score, session = info
        if status == "Currently Airing":
            print(
                Fore.MAGENTA + f"{n}. " +
                Fore.CYAN + f"{title}" +
                Fore.WHITE + " | Status -> " +
                Fore.RED + f"{status}" +
                Fore.WHITE + " | Rating -> " +
                Fore.LIGHTRED_EX + f"{score}" +
                Fore.RESET
            )
        else:
            print(
                Fore.MAGENTA + f"{n}. " +
                Fore.CYAN + f"{title}" +
                Fore.WHITE + " | Status -> " +
                Fore.GREEN + f"{status}" +
                Fore.WHITE + " | Eps -> " +
                Fore.LIGHTYELLOW_EX + f"{eps}" +
                Fore.WHITE + " | Rating -> " +
                Fore.LIGHTRED_EX + f"{score}" +
                Fore.RESET
            )
        ids.append(session)
    return ids


# ── Episode list ──────────────────────────────────────────────────────────────

_EP_PAGE_CONCURRENCY = 4   # parallel release-page requests (more triggers 429s)


async def _fetch_page_async(session, sem: asyncio.Semaphore, url: str) -> str | None:
    """Fetch one release-API page, retrying rate-limit/server errors with backoff."""
    headers = _build_headers()
    async with sem:
        for attempt in range(_RETRY_TOTAL + 1):
            try:
                resp = await session.get(url, headers=headers, timeout=20)
                if resp.status_code in _RETRY_STATUS_CODES and attempt < _RETRY_TOTAL:
                    base = _RETRY_429_BASE_BACKOFF if resp.status_code == 429 else _RETRY_BACKOFF
                    await asyncio.sleep(base * (2 ** attempt) + random.uniform(0, 1))
                    continue
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                if attempt < _RETRY_TOTAL:
                    await asyncio.sleep(_RETRY_BACKOFF * (2 ** attempt))
                    continue
                print(Fore.RED + f"[warn] episode page fetch failed for {url}: {exc}" + Fore.RESET)
    return None


async def get_episode_list(anime_id: str, last_page: int) -> dict:
    """
    Fetch every page of the release API (30 episodes per page) concurrently.
    Returns { episode_number(int|float): session_id(str) }.
    """
    init_session()
    urls = [
        f"{BASE_URL}/api?m=release&id={anime_id}&sort=episode_asc&page={p}"
        for p in range(1, last_page + 1)
    ]

    sem = asyncio.Semaphore(_EP_PAGE_CONCURRENCY)
    async with cffi_requests.AsyncSession(impersonate="chrome",
                                          cookies=dict(_session.cookies)) as session:
        responses = await asyncio.gather(*(_fetch_page_async(session, sem, u) for u in urls))

    missing = sum(1 for r in responses if not r)
    if missing:
        print(Fore.YELLOW + f"[warn] {missing}/{len(urls)} episode page(s) could not be "
              f"fetched - some episodes may be missing from the list" + Fore.RESET)

    eps: dict = {}
    for text in responses:
        if not text:
            continue
        try:
            for item in json.loads(text).get("data", []):
                num = float(item["episode"])
                # Whole numbers as int (5, not 5.0); specials keep their fraction (12.5)
                eps[int(num) if num == int(num) else num] = item["session"]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            print(Fore.YELLOW + f"[warn] failed to parse episode page: {e}" + Fore.RESET)

    return eps


# ── Episode page -> kwik links ─────────────────────────────────────────────────

def get_ep_links(anime_id: str, ep_session: str) -> list:
    """
    Scrape the episode's play page for its kwik embed links.
    Returns a list of (kwik_url, quality_label, audio_type) tuples,
    e.g. ("https://kwik.cx/e/abc", "1080p", "jpn").
    """
    html = _get_play_page(anime_id, ep_session)

    # 2026 format: <button data-src="https://kwik.cx/e/xxx" data-resolution="1080" data-audio="jpn">
    matches = re.findall(
        r'data-src="(https://kwik\.[a-z]+/e/[^"]+)"'
        r'[^>]*data-(?:fansub|resolution|audio)=[^>]*'
        r'data-resolution="([^"]*)"'
        r'[^>]*data-audio="([^"]*)"',
        html,
    )

    # Simpler fallback: just grab data-src + data-resolution + data-audio separately
    if not matches:
        matches = []
        for btn_match in re.finditer(
            r'<button[^>]*data-src="(https://kwik\.[a-z]+/e/[^"]+)"[^>]*>',
            html,
        ):
            btn_tag = btn_match.group(0)
            kwik_url = btn_match.group(1)
            res_m = re.search(r'data-resolution="([^"]*)"', btn_tag)
            aud_m = re.search(r'data-audio="([^"]*)"', btn_tag)
            quality = res_m.group(1) + "p" if res_m else "unknown"
            audio = aud_m.group(1) if aud_m else ""
            matches.append((kwik_url, quality, audio))

    # Legacy fallback: href-based links (old format)
    if not matches:
        matches = re.findall(
            r'href="(https://kwik\.[a-z]+/e/[^"]+)"[^>]*>\s*'
            r'<span[^>]*>([^<]+)</span>\s*<span[^>]*>([^<]*)</span>',
            html,
        )

    # Last resort: any kwik URL
    if not matches:
        raw = re.findall(r'(?:data-src|href|src)="(https://kwik\.[a-z]+/[^"]+)"', html)
        matches = [(m, "unknown", "") for m in raw]

    # Normalise quality labels ("1080" -> "1080p") so every format displays alike
    return [(url, q.strip() + "p" if q.strip().isdigit() else q.strip(), aud.strip())
            for url, q, aud in matches]


def show_dl_opts(links: list) -> list:
    """Print quality options, return ordered list of kwik URLs."""
    urls = []
    for n, item in enumerate(links, start=1):
        kwik_url = item[0]
        quality  = item[1].strip() if len(item) > 1 else "unknown"
        audio    = item[2].strip() if len(item) > 2 else ""

        if audio.lower() in ("dub", "eng"):
            tag = Fore.LIGHTGREEN_EX + f"{quality} Dub"
        elif audio.lower() in ("jpn", "sub", ""):
            tag = Fore.LIGHTBLUE_EX + f"{quality} Sub"
        else:
            tag = Fore.LIGHTYELLOW_EX + f"{quality} ({audio})"

        print(Fore.MAGENTA + f"{n}. " + tag + Fore.RESET)
        urls.append(kwik_url)
    return urls


# ── Filename sanitisation ─────────────────────────────────────────────────────

def _sanitize(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "", name).strip()


# ── Parallel HLS downloader with resume & rate limiting ───────────────────────

_STATE_SAVE_INTERVAL = 2.0   # seconds between progress-state writes during a download


def _format_hms(seconds: float) -> str:
    s = int(round(seconds))
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def _build_ffmpeg_cmd(local_m3u8: str, out_path: str, tmp_dir: str, show_title: str,
                      meta: EpisodeMeta | None) -> list:
    """Build the ffmpeg command that decrypts + muxes the local playlist into MP4.

    Inputs: 0 = local m3u8, then optionally cover art and a chapters file.
    Streams are copied (no re-encode), so this takes seconds.
    """
    inputs = ["-allowed_extensions", "ALL", "-i", local_m3u8]
    maps = ["-map", "0:v:0", "-map", "0:a?"]
    codecs = ["-c", "copy"]
    extra = []
    next_input = 1

    if meta:
        # Cover art -> attached picture stream
        cover = download_thumbnail(meta.snapshot_url, tmp_dir)
        if cover:
            inputs += ["-i", cover]
            maps += ["-map", f"{next_input}:v"]
            codecs += ["-c:v:1", "mjpeg", "-disposition:v:1", "attached_pic"]
            next_input += 1

        # Chapters, written in ffmpeg's FFMETADATA format
        chapters = meta.chapter_times()
        if len(chapters) > 1:
            names = ["Opening", "Episode", "Ending"] if len(chapters) == 4 else ["Episode"]
            chapter_file = os.path.join(tmp_dir, "chapters.txt")
            with open(chapter_file, "w", encoding="utf-8") as f:
                f.write(";FFMETADATA1\n")
                for i, (start, end) in enumerate(zip(chapters, chapters[1:])):
                    name = names[i] if i < len(names) else f"Part {i + 1}"
                    f.write(f"[CHAPTER]\nTIMEBASE=1/1000\nSTART={start * 1000}\n"
                            f"END={end * 1000}\ntitle={name}\n")
            inputs += ["-i", chapter_file]
            extra += ["-map_chapters", str(next_input)]
            next_input += 1

        if meta.title:
            extra += ["-metadata", f"title={meta.title}"]
        extra += ["-metadata", f"episode_id={meta.episode}", "-metadata", f"show={show_title}"]

    return (["ffmpeg"] + inputs + maps + codecs + extra + [
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "+faststart",
        "-f", "mp4",                 # explicit: the .part extension says nothing
        "-loglevel", "error",
        "-y", out_path,
    ])


def download_vid(m3u8_url: str, title: str, ep, referer: str = "https://kwik.cx/",
                 meta: EpisodeMeta | None = None, no_resume: bool = False,
                 output_dir: str | None = None) -> bool:
    """
    Download an HLS stream to  <output_dir>/<title>/<title> ep_XX.mp4.
    Returns True on success (or if the file already exists), False on failure.

    Segments are AES-128 encrypted, so the flow is:
      1. Fetch + parse the remote m3u8 playlist
      2. Download the encryption key(s)
      3. Download all segments in parallel (rate-limited, resumable)
      4. Build a LOCAL m3u8 referencing the local segments + key
      5. ffmpeg decrypts + muxes it to MP4, adding cover art and chapters

    The MP4 is written as "<name>.mp4.part" and renamed when complete, so an
    interrupted run never leaves a broken file that looks finished. On
    failure the temp segments are kept, so the next run resumes.
    """
    safe_title = _sanitize(title)
    ep_str = f"{int(ep):02d}" if isinstance(ep, (int, float)) and ep == int(ep) else str(ep)
    show_dir = os.path.join(output_dir or ".", safe_title)
    os.makedirs(show_dir, exist_ok=True)
    out_file = os.path.join(show_dir, f"{safe_title} ep_{ep_str}.mp4")
    out_file_abs = os.path.abspath(out_file)
    part_file_abs = out_file_abs + ".part"

    # ── Skip if already downloaded ────────────────────────────────────────────
    if os.path.exists(out_file_abs) and os.path.getsize(out_file_abs) > 0:
        print(Fore.GREEN + f"\n[skip] Ep {ep_str} already downloaded -> {out_file}" + Fore.RESET)
        _log_event("skip_existing", episode=ep_str, output=out_file)
        return True

    print(Fore.LIGHTYELLOW_EX + f"\nDownloading Ep {ep_str} -> {out_file}" + Fore.RESET)
    _log_event("download_start", episode=ep_str, m3u8_url=m3u8_url, output=out_file)

    # ── Step 1: Fetch and parse m3u8 playlist ─────────────────────────────────
    dl_headers = {"User-Agent": _user_agent, "Referer": referer}
    try:
        m3u8_resp = cffi_requests.get(m3u8_url, headers=dl_headers, timeout=30, impersonate="chrome")
        m3u8_resp.raise_for_status()
    except Exception as e:
        print(Fore.RED + f"[error] failed to fetch m3u8 playlist: {e}" + Fore.RESET)
        _log_event("m3u8_fetch_failed", episode=ep_str, error=str(e))
        return False

    m3u8_text = m3u8_resp.text
    m3u8_lines = m3u8_text.strip().split('\n')

    # Segment URLs are the non-comment lines; #EXTINF:<seconds>, gives each one's length
    segments = []
    total_duration = 0.0
    for line in m3u8_lines:
        line = line.strip()
        if line.startswith('#EXTINF:'):
            try:
                total_duration += float(line.split(':', 1)[1].split(',')[0])
            except (ValueError, IndexError):
                pass
        elif line and not line.startswith('#'):
            segments.append(line)

    if not segments:
        print(Fore.RED + "[error] no segments found in m3u8 playlist" + Fore.RESET)
        _log_event("no_segments", episode=ep_str)
        return False

    # The playlist knows the exact runtime - more reliable than scraping the page
    if meta and total_duration > 0:
        meta.duration = _format_hms(total_duration)

    key_urls = re.findall(r'#EXT-X-KEY:.*?URI="([^"]+)"', m3u8_text)
    total_segments = len(segments)
    encrypted = "encrypted, " if key_urls else ""
    print(Fore.WHITE + f"  {total_segments} segments ({encrypted}{_format_hms(total_duration)} long)"
          + Fore.RESET)

    # ── Step 2: Temp dir + resume state ───────────────────────────────────────
    # Named after the episode, so a re-run finds the same dir and resumes
    tmp_dir = os.path.join(tempfile.gettempdir(), f"animepahe_{safe_title}_ep{ep_str}")
    state_path = tmp_dir + _STATE_FILE_SUFFIX
    existing_state = _load_download_state(state_path)
    resume = (not no_resume and existing_state is not None
              and existing_state.m3u8_url == m3u8_url
              and existing_state.total_segments == total_segments)

    if not resume:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir, exist_ok=True)

    # Download encryption key(s) into the temp dir
    key_map = {}  # remote_url -> local_filename
    for i, key_url in enumerate(key_urls):
        key_filename = f"key_{i}.key"
        key_path = os.path.join(tmp_dir, key_filename)
        if not _verify_segment(key_path):
            try:
                key_resp = cffi_requests.get(key_url, headers=dl_headers, timeout=30, impersonate="chrome")
                key_resp.raise_for_status()
                with open(key_path, "wb") as f:
                    f.write(key_resp.content)
            except Exception as e:
                print(Fore.RED + f"[error] failed to download encryption key: {e}" + Fore.RESET)
                _log_event("key_download_failed", episode=ep_str, key_url=key_url, error=str(e))
                return False
        key_map[key_url] = key_filename

    seg_filenames = {url: f"seg_{idx:05d}.ts" for idx, url in enumerate(segments)}

    if resume:
        segment_states = existing_state.segments
        completed = 0
        for seg_state in segment_states:
            seg_state.downloaded = _verify_segment(os.path.join(tmp_dir, seg_state.filename),
                                                   seg_state.size)
            completed += seg_state.downloaded
        print(Fore.CYAN + f"  [resume] {completed}/{total_segments} segments already downloaded"
              + Fore.RESET)
        _log_event("resume", episode=ep_str, completed=completed, total=total_segments)
    else:
        segment_states = [SegmentState(index=idx, url=url, filename=seg_filenames[url])
                          for idx, url in enumerate(segments)]
        completed = 0

    local_m3u8_path = os.path.join(tmp_dir, "local.m3u8")
    download_state = DownloadState(
        m3u8_url=m3u8_url,
        title=safe_title,
        ep=ep_str,
        total_segments=total_segments,
        segments=segment_states,
        key_urls=key_urls,
        key_map=key_map,
        started_at=existing_state.started_at if resume else time.time(),
        updated_at=time.time(),
        temp_dir=tmp_dir,
        local_m3u8_path=local_m3u8_path,
        output_file=out_file_abs,
    )
    _save_download_state(download_state)

    # ── Step 3: Download segments in parallel ─────────────────────────────────
    lock = threading.Lock()
    progress = {"done": completed, "bytes": 0, "failed": 0, "last_save": time.time()}
    start_wall = time.time()

    def _save_state_throttled(force: bool = False):
        """Persist progress at most every few seconds (thousands of segments
        would otherwise mean thousands of JSON rewrites)."""
        with lock:
            if not force and time.time() - progress["last_save"] < _STATE_SAVE_INTERVAL:
                return
            progress["last_save"] = time.time()
            _save_download_state(download_state)

    def _download_segment(seg_state: SegmentState) -> bool:
        """Download one segment with retries, exponential backoff and jitter."""
        seg_path = os.path.join(tmp_dir, seg_state.filename)
        backoff = _DL_BASE_BACKOFF

        for attempt in range(1, _DL_MAX_RETRIES + 1):
            error = None
            wait = backoff
            try:
                with _cdn_slots:  # hold a CDN slot only while the request is in flight
                    resp = cffi_requests.get(seg_state.url, headers=dl_headers,
                                             timeout=_DL_TIMEOUT, impersonate="chrome")
                if resp.status_code in _RETRY_STATUS_CODES:
                    error = f"HTTP {resp.status_code}"
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait = min(float(retry_after), _DL_MAX_BACKOFF)
                else:
                    resp.raise_for_status()
                    if not resp.content:
                        raise IOError("empty segment")
                    with open(seg_path, "wb") as f:
                        f.write(resp.content)
            except Exception as e:
                error = str(e)

            seg_state.retries = attempt
            if error is None:
                seg_state.downloaded = True
                seg_state.size = len(resp.content)
                seg_state.last_error = ""
                with lock:
                    progress["done"] += 1
                    progress["bytes"] += seg_state.size
                    done, total_bytes = progress["done"], progress["bytes"]
                elapsed = time.time() - start_wall
                size_mb = total_bytes / (1024 * 1024)
                speed = size_mb / elapsed if elapsed > 0.5 else 0.0
                sys.stdout.write(
                    f"\r  {Fore.WHITE}{done}/{total_segments} ({done / total_segments * 100:4.1f}%)  "
                    f"size= {size_mb:7.1f} MB  speed= {speed:5.2f} MB/s{Fore.RESET}"
                )
                sys.stdout.flush()
                _save_state_throttled()
                return True

            seg_state.last_error = error
            if attempt < _DL_MAX_RETRIES:
                time.sleep(wait + random.uniform(0, wait * _DL_JITTER))
                backoff = min(backoff * 2, _DL_MAX_BACKOFF)

        _log_event("segment_failed", episode=ep_str, segment=seg_state.index,
                   error=seg_state.last_error, url=seg_state.url)
        with lock:
            progress["failed"] += 1
        return False

    remaining_segments = [s for s in segment_states if not s.downloaded]
    if remaining_segments:
        print(Fore.WHITE + f"  Downloading {len(remaining_segments)} segments "
              f"({_DL_WORKERS} workers)..." + Fore.RESET)
        with concurrent.futures.ThreadPoolExecutor(max_workers=_DL_WORKERS) as executor:
            list(executor.map(_download_segment, remaining_segments))
        print()  # newline after the \r progress line
    _save_state_throttled(force=True)

    # Every segment is needed - muxing with gaps would produce a broken video.
    # Downloaded segments stay in the temp dir, so a re-run only fetches the rest.
    failed = progress["failed"]
    if failed:
        print(Fore.RED + f"[error] {failed}/{total_segments} segment(s) failed to download. "
              f"Run again to resume this episode." + Fore.RESET)
        _log_event("download_aborted", episode=ep_str, failed=failed, total=total_segments)
        return False

    # ── Step 4: Build local m3u8 pointing at the local files ──────────────────
    with open(local_m3u8_path, "w", encoding="utf-8") as f:
        for line in m3u8_lines:
            stripped = line.strip()
            if stripped in seg_filenames:
                stripped = seg_filenames[stripped]
            elif stripped.startswith('#EXT-X-KEY:'):
                for remote_url, local_name in key_map.items():
                    stripped = stripped.replace(remote_url, local_name)
            f.write(stripped + "\n")

    # ── Step 5: ffmpeg decrypts + muxes to mp4 (local I/O, fast) ──────────────
    print(Fore.WHITE + "  Decrypting + muxing to mp4..." + Fore.RESET, end=" ", flush=True)
    cmd = _build_ffmpeg_cmd(local_m3u8_path, part_file_abs, tmp_dir, safe_title, meta)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=tmp_dir)
    except FileNotFoundError:
        print(Fore.RED + "failed\n[error] ffmpeg not found - install it and add it to PATH"
              + Fore.RESET)
        return False

    if result.returncode != 0:
        print(Fore.RED + f"failed (code {result.returncode})" + Fore.RESET)
        if result.stderr.strip():
            print(Fore.RED + result.stderr.strip()[-2000:] + Fore.RESET)
        _log_event("ffmpeg_failed", episode=ep_str, code=result.returncode,
                   stderr=result.stderr.strip()[-2000:])
        try:
            os.remove(part_file_abs)
        except OSError:
            pass
        return False  # temp dir kept -> next run skips straight to muxing

    os.replace(part_file_abs, out_file_abs)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        os.remove(state_path)
    except OSError:
        pass

    final_mb = os.path.getsize(out_file_abs) / (1024 * 1024)
    wall_total = time.time() - start_wall
    print(Fore.GREEN + "done" + Fore.RESET)
    print(Fore.GREEN + f"  Done -> {out_file}  ({final_mb:.1f} MB in {wall_total:.0f}s)" + Fore.RESET)
    _log_event("download_complete", episode=ep_str, output=out_file,
               size_mb=round(final_mb, 1), duration_s=round(wall_total))
    return True
