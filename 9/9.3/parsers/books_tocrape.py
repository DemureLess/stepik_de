import re
from typing import Callable, Dict, Any, Iterable, Optional
from bs4 import BeautifulSoup
from crawler.http_client import get_html


def is_category(prefix_url: str, url: str) -> bool:
    """определяем является ли url категорией"""
    return prefix_url in url


def get_product_name(soup: BeautifulSoup) -> str:
    """получаем название товара из h1"""
    return soup.select_one("h1").get_text(strip=True)


def get_price_currency(soup: BeautifulSoup) -> str:
    """получаем валюту из p.price_color"""
    text = soup.select_one("p.price_color").get_text(strip=True)
    m = re.match(r"^\s*([^\d.,\s]+)", text)
    return (m.group(1) if m else "").strip()


def get_price_amount(soup: BeautifulSoup) -> float:
    """получаем цену из p.price_color"""
    text = soup.select_one("p.price_color").get_text(strip=True)
    num_str = re.sub(r"[^0-9.,]", "", text).replace(",", ".")
    return float(num_str) if num_str else 0.0


def get_breadcrumbs_no_last(soup: BeautifulSoup) -> str:
    """получаем хлебные крошки без последнего элемента"""
    cats = soup.select("ul.breadcrumb li a")
    names = [a.get_text(strip=True) for a in cats]
    return " > ".join(names)


def get_instock_text(soup: BeautifulSoup) -> str:
    """получаем текст из p.instock.availability"""
    return soup.select_one("p.instock.availability").get_text(strip=True)


def get_instock_qty(soup: BeautifulSoup) -> Optional[int]:
    """получаем количество товара из p.instock.availability"""
    text = get_instock_text(soup).replace("\xa0", " ")
    m = re.search(r"\((\d+)\s+available\)", text, flags=re.I)
    return int(m.group(1)) if m else None


def get_instock_flag(soup: BeautifulSoup) -> bool:
    """получаем флаг наличия товара из p.instock.availability"""
    return "in stock" in get_instock_text(soup).lower()


# словарь с функциями для извлечения полей
EXTRACTORS: Dict[str, Callable[[BeautifulSoup], Any]] = {
    "product_name": get_product_name,
    "price_currency": get_price_currency,
    "price_amount": get_price_amount,
    "breadcrumbs": get_breadcrumbs_no_last,
    "instock_text": get_instock_text,
    "instock_qty": get_instock_qty,
    "instock": get_instock_flag,
}


def extract_fields(
    soup: BeautifulSoup, fields: Optional[Iterable[str]] = None
) -> Dict[str, Any]:
    """Кобмайн для извлчения списка полей"""
    fields = list(fields) if fields is not None else list(EXTRACTORS.keys())
    out: Dict[str, Any] = {}
    for f in fields:
        func = EXTRACTORS.get(f)
        if not func:
            out[f] = None
            continue
        try:
            out[f] = func(soup)
        except Exception:
            out[f] = None
    return out


def parse_item(
    url: str,
    fields: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Парсим одну карточку товара"""
    html = get_html(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    data = extract_fields(soup, fields=fields)
    data["url"] = url
    return data
