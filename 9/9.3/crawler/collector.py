import logging
from collections import deque
from urllib.parse import urlsplit, urljoin
from typing import Set
from bs4 import BeautifulSoup

from .http_client import get_html

logger = logging.getLogger(__name__)


def collect_links(
    start_url: str,
    max_pages: int = 1000,
) -> Set[str]:
    base_domain = urlsplit(start_url).netloc
    # очередь для посещения
    to_visit = deque([start_url])
    # посещенные урлы
    visited: Set[str] = set()
    # все урлы
    all_urls: Set[str] = set()

    while to_visit and len(visited) < max_pages:
        url = to_visit.popleft()
        if url in visited:
            continue

        html = get_html(url)
        if not html:
            logger.warning(f"Skip (no html): {url}")
            continue

        visited.add(url)
        all_urls.add(url)

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href or href.startswith("#"):
                continue
            full_url = urljoin(url, href)
            if urlsplit(full_url).netloc == base_domain:
                if full_url not in visited and full_url not in to_visit:
                    to_visit.append(full_url)
                all_urls.add(full_url)

    return all_urls
