# modules/parser.py

import re
from urllib.parse import urlparse

import validators
from bs4 import BeautifulSoup  # kept for possible future HTML parsing

URL_REGEX = re.compile(r"https?://[^\s,;'\"]+")
# US-style address: "123 Main St, City, ST 12345"
ADDRESS_PATTERN = re.compile(
    r"\d{1,5}[\w\s\.\-#]*,\s*[\w\s\.\-]+,\s*[A-Z]{2}\s*\d{5}"
)


def extract_urls(text: str) -> list:
    if not text:
        return []
    urls = URL_REGEX.findall(text)
    cleaned = []
    for u in urls:
        u = u.rstrip(".,)")
        if validators.url(u):
            cleaned.append(u)
    # deduplicate, preserve order
    return list(dict.fromkeys(cleaned))


def extract_addresses(text: str) -> list:
    if not text:
        return []
    matches = ADDRESS_PATTERN.findall(text)
    return list(dict.fromkeys(matches))


def extract_domains_from_urls(urls: list) -> list:
    domains = []
    for u in urls:
        try:
            p = urlparse(u)
            domains.append(p.netloc)
        except Exception:
            continue
    return list(dict.fromkeys(domains))


def extract_named_locations(text: str, window: int = 140) -> list:
    """
    Extract candidate (name, address) pairs from free text.

    For each address match, look at the preceding context (up to `window`
    characters), take the last non-empty line as the probable location name.

    Returns a list of dicts:
        { "name": "", "address": "" }
    """
    results = []
    if not text:
        return results

    for match in ADDRESS_PATTERN.finditer(text):
        addr = match.group(0)
        start = max(0, match.start() - window)
        context = text[start:match.start()]

        # last non-empty line in the context is taken as the "name"
        lines = [l.strip() for l in context.splitlines() if l.strip()]
        name = lines[-1] if lines else ""
        if not name:
            name = ""
        results.append({"name": name, "address": addr})

    # dedupe by (name, address) while preserving order
    seen = set()
    unique = []
    for loc in results:
        key = (loc["name"], loc["address"])
        if key not in seen:
            seen.add(key)
            unique.append(loc)

    return unique
