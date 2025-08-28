import time
import logging
from typing import Optional, Dict
import requests
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

MIN_DELAY = 0.5
MAX_DELAY = 60.0
BACKOFF = 2.0
MAX_RETRIES = 3
TIMEOUT = 0.5


def build_headers() -> Dict[str, str]:
    return {"User-Agent": UserAgent().random}


def _increase_delay(current: float) -> float:
    return min(current * BACKOFF, MAX_DELAY)


def _retry_after(resp: Optional[requests.Response]) -> Optional[float]:
    try:
        if not resp:
            return None
        v = resp.headers.get("Retry-After")
        return float(v) if v else None
    except Exception:
        return None


def get_html(
    url: str,
    timeout: int = TIMEOUT,
    start_delay: float = MIN_DELAY,
    max_retries: int = MAX_RETRIES,
) -> Optional[str]:
    delay = max(start_delay, MIN_DELAY)
    attempts = 0

    while attempts <= max_retries:
        try:
            time.sleep(delay)
            r = requests.get(url, headers=build_headers(), timeout=timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            html = r.text
            if delay > MIN_DELAY:
                delay = max(MIN_DELAY, delay / BACKOFF)
            return html
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            ra = _retry_after(getattr(e, "response", None))
            logger.warning(f"HTTP {status} for {url}: {e}")
            if status in (429, 503) and attempts < max_retries:
                delay = max(delay, ra) if ra is not None else _increase_delay(delay)
                attempts += 1
                continue
            return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for {url} (attempt {attempts + 1}/{max_retries})")
            if attempts < max_retries:
                delay = _increase_delay(delay)
                attempts += 1
                continue
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {e}")
            return None
    return None
