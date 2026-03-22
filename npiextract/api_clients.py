# api_clients.py - API Clients with NPI Registry Fallback
# When scraping returns empty/blocked → searches NPI Registry by name from URL

import requests
import time
import random
import logging
import json
import re
from urllib.parse import urlparse
from config import (
    SCRAPINGANT_CONFIG, WEBSCRAPINGAI_CONFIG,
    SCRAPING_CONFIG, DOMAIN_SETTINGS, NPI_REGISTRY_CONFIG
)

logger = logging.getLogger(__name__)


# ============================================
# HELPER FUNCTIONS
# ============================================

def clean_url(url):
    """Trim huge query strings from problematic URLs"""
    if len(url) > 2000:
        parsed = urlparse(url)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        logger.warning(f"URL trimmed from {len(url)} to {len(clean)} chars")
        return clean
    return url


def extract_provider_name_from_url(url):
    """
    Extract provider first/last name from URL path.

    Examples:
      .../physicians/jaya-francis-0339353       → Jaya, Francis
      .../providers/david-charette-md/          → David, Charette
      .../our-providers/daniel-piazza           → Daniel, Piazza
      .../doctor/aakashagarwal                  → aakashagarwal
      .../sakina-khan-1184911406                → Sakina, Khan
      .../gwendolyn-mabel-casanova-felix-md     → Gwendolyn, Felix
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        last_segment = path.split('/')[-1]

        # Remove trailing numbers (kaiser IDs, NPIs, detail IDs)
        cleaned = re.sub(r'-?\d{5,}$', '', last_segment)

        # Remove underscores that some sites use
        cleaned = cleaned.replace('_', '-')

        credentials = [
            'md', 'do', 'np', 'pa', 'dpm', 'dds', 'rn', 'phd',
            'dnp', 'aprn', 'fnp', 'mph', 'ms', 'pharmd', 'dpt',
            'od', 'dc', 'nd', 'lcsw', 'psyd', 'dr',
            'facp', 'facep', 'facs', 'faap', 'facog', 'faan',
            'qualifications', 'profile', 'details', 'biography',
            'family-medicine', 'primary-care', 'internal-medicine',
        ]

        parts = cleaned.split('-')
        name_parts = [
            p for p in parts
            if p and p.lower() not in credentials and len(p) > 1
            and not p.isdigit()
        ]

        if len(name_parts) >= 2:
            first_name = name_parts[0].capitalize()
            last_name = name_parts[-1].capitalize()
            logger.info(f"  Name from URL: {first_name} {last_name}")
            return {"first_name": first_name, "last_name": last_name, "all_parts": name_parts}

        elif len(name_parts) == 1:
            name = name_parts[0]
            # Try to split camelCase like "aakashagarwal"
            # Common pattern: firstlast → try known first name lengths
            logger.info(f"  Single name segment: {name}")
            return {"first_name": "", "last_name": name.capitalize(), "combined": name}

    except Exception as e:
        logger.debug(f"Name extraction failed: {e}")

    return None


def extract_state_from_url(url):
    """Determine US state from URL keywords"""
    state_map = {
        "northern-california": "CA", "southern-california": "CA",
        "california": "CA", "hawaii": "HI", "colorado": "CO",
        "georgia": "GA", "washington": "WA", "oregon": "OR",
        "new-york": "NY", "texas": "TX", "florida": "FL",
        "massachusetts": "MA", "illinois": "IL", "ohio": "OH",
        "pennsylvania": "PA", "michigan": "MI", "virginia": "VA",
        "maryland": "MD", "new-jersey": "NJ", "connecticut": "CT",
        "arizona": "AZ", "tennessee": "TN", "indiana": "IN",
        "missouri": "MO", "wisconsin": "WI", "minnesota": "MN",
        "north-carolina": "NC", "south-carolina": "SC",
        "alabama": "AL", "louisiana": "LA", "kentucky": "KY",
        "oklahoma": "OK", "iowa": "IA", "arkansas": "AR",
        "kansas": "KS", "mississippi": "MS", "nebraska": "NE",
        "nevada": "NV", "utah": "UT", "new-mexico": "NM",
        "west-virginia": "WV", "idaho": "ID", "maine": "ME",
        "new-hampshire": "NH", "montana": "MT", "rhode-island": "RI",
        "delaware": "DE", "alaska": "AK", "vermont": "VT",
        "wyoming": "WY", "north-dakota": "ND", "south-dakota": "SD",
        "/ncal/": "CA", "/scal/": "CA", "/nw/": "WA",
        "/hi/": "HI", "/co/": "CO", "/ga/": "GA", "/mas/": "MA",
    }
    url_lower = url.lower()
    for region, st in state_map.items():
        if region in url_lower:
            return st
    return ""


def npi_registry_search(first_name="", last_name="", state="", limit=5):
    """
    Search NPI Registry by provider name.
    FREE official CMS API - no key needed!
    Endpoint: https://npiregistry.cms.hhs.gov/api/
    """
    try:
        params = {
            "version": NPI_REGISTRY_CONFIG.get("version", "2.1"),
            "limit": limit,
            "enumeration_type": "NPI-1",
        }
        if first_name:
            params["first_name"] = first_name
        if last_name:
            params["last_name"] = last_name
        if state:
            params["state"] = state

        if not first_name and not last_name:
            return []

        logger.info(f"  NPI Registry search: first='{first_name}' last='{last_name}' state='{state}'")

        response = requests.get(
            NPI_REGISTRY_CONFIG["base_url"],
            params=params,
            timeout=NPI_REGISTRY_CONFIG.get("timeout", 10),
        )

        if response.status_code == 200:
            data = response.json()
            results = []
            for provider in data.get("results", []):
                basic = provider.get("basic", {})
                taxonomies = provider.get("taxonomies", [])
                addresses = provider.get("addresses", [])

                practice_state = ""
                for addr in addresses:
                    if addr.get("address_purpose") == "LOCATION":
                        practice_state = addr.get("state", "")
                        break
                if not practice_state and addresses:
                    practice_state = addresses[0].get("state", "")

                results.append({
                    "npi": provider.get("number"),
                    "first_name": basic.get("first_name", ""),
                    "last_name": basic.get("last_name", ""),
                    "credential": basic.get("credential", ""),
                    "specialty": taxonomies[0].get("desc", "") if taxonomies else "",
                    "state": practice_state,
                    "status": basic.get("status", ""),
                })

            logger.info(f"  NPI Registry found: {len(results)} results")
            return results

    except Exception as e:
        logger.error(f"  NPI Registry search failed: {e}")
    return []


def npi_registry_fallback(url):
    """
    When scraping fails:
    1. Extract provider name from URL
    2. Search NPI Registry by name
    3. Return best match with confidence score
    """
    name_info = extract_provider_name_from_url(url)
    if not name_info:
        logger.warning(f"  Cannot extract name from URL")
        return None

    first = name_info.get("first_name", "")
    last = name_info.get("last_name", "")
    if not last:
        return None

    state = extract_state_from_url(url)

    # Search 1: Full name + state
    results = npi_registry_search(first_name=first, last_name=last, state=state, limit=5)

    # Search 2: Full name without state
    if not results and state:
        logger.info(f"  Retry without state...")
        results = npi_registry_search(first_name=first, last_name=last, limit=5)

    # Search 3: Partial first name
    if not results and first and len(first) > 3:
        logger.info(f"  Retry partial first name...")
        results = npi_registry_search(first_name=first[:3] + "*", last_name=last, limit=5)

    # Search 4: Last name only
    if not results and first:
        logger.info(f"  Retry last name only...")
        results = npi_registry_search(last_name=last, limit=10)

    if not results:
        logger.warning(f"  Registry: No results for {first} {last}")
        return None

    # Pick best match
    best = results[0]
    if len(results) > 1 and first:
        for r in results:
            if r["first_name"].lower() == first.lower():
                best = r
                break

    # Calculate confidence
    confidence = 45
    if len(results) == 1:
        confidence = 75
    elif len(results) <= 3:
        confidence = 60

    # Boost if exact name match
    if (first and
            best["first_name"].lower() == first.lower() and
            best["last_name"].lower() == last.lower()):
        confidence = min(85, confidence + 15)

    name_str = f"{best['first_name']} {best['last_name']} {best.get('credential', '')}".strip()

    logger.info(
        f"  REGISTRY SUCCESS: NPI={best['npi']} | {name_str} | "
        f"{best.get('specialty', '')} | {best.get('state', '')} | "
        f"Confidence={confidence}%"
    )

    return {
        "success": True,
        "npi": best["npi"],
        "name": name_str,
        "specialty": best.get("specialty", ""),
        "state": best.get("state", ""),
        "method": "npi_registry_name_search",
        "confidence": confidence,
        "total_matches": len(results),
        "all_matches": results,
    }


def build_registry_html(registry_data, url):
    """Build HTML with NPI data so parser/extractor can find it"""
    npi = registry_data.get("npi", "")
    name = registry_data.get("name", "")
    specialty = registry_data.get("specialty", "")
    state = registry_data.get("state", "")

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>{name} - Provider Profile</title>
    <meta name="npi" content="{npi}">
    <meta name="provider-name" content="{name}">
    <script type="application/ld+json">
    {{
        "@type": "Physician",
        "name": "{name}",
        "npi": "{npi}",
        "npiNumber": "{npi}",
        "specialty": "{specialty}",
        "address": {{"addressRegion": "{state}"}}
    }}
    </script>
</head>
<body>
    <div class="provider-profile">
        <h1>{name}</h1>
        <p>NPI: {npi}</p>
        <p>National Provider Identifier: {npi}</p>
        <p>Specialty: {specialty}</p>
        <p>State: {state}</p>
        <p>Source: NPI Registry (npiregistry.cms.hhs.gov)</p>
        <p>Original URL: {url}</p>
    </div>
</body>
</html>"""


def _is_blocked_response(html, content_len):
    """Check if response is a blocked/empty anti-bot page"""
    if content_len < 10:
        return True
    if not html or not html.strip():
        return True
    if '<html><head></head><body></body></html>' in html:
        return True
    if content_len < 100 and '<body></body>' in html:
        return True
    return False


# ============================================
# SCRAPINGANT CLIENT
# ============================================
class ScrapingAntClient:
    """ScrapingAnt API + NPI Registry Fallback"""

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.scrapingant.com/v2/general"
        self.session = requests.Session()
        self.request_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.registry_fallback_count = 0
        logger.info("ScrapingAnt client initialized")
        logger.info(f"  Endpoint: {self.base_url}")
        if api_key and len(api_key) > 12:
            logger.info(f"  API Key: {api_key[:8]}...{api_key[-4:]}")

    def _get_domain_settings(self, url):
        for domain, settings in DOMAIN_SETTINGS.items():
            if domain in url:
                return settings
        return {"needs_js": True}

    def test_connection(self):
        try:
            logger.info("Testing ScrapingAnt connection...")
            response = self.session.get(
                self.base_url,
                headers={"x-api-key": self.api_key},
                params={"url": "https://httpbin.org/get", "browser": "false"},
                timeout=30,
            )
            if response.status_code == 200:
                logger.info("ScrapingAnt connection test: SUCCESS")
                return {"success": True, "status": 200}
            elif response.status_code == 401:
                return {"success": False, "error": "Invalid API key"}
            elif response.status_code == 402:
                return {"success": False, "error": "No credits remaining"}
            else:
                return {"success": False, "error": f"Status {response.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def scrape(self, url, **kwargs):
        """Scrape URL → if blocked → NPI Registry fallback"""
        self.request_count += 1
        original_url = url
        url = clean_url(url)

        delay = random.uniform(
            SCRAPING_CONFIG["delay_between_requests"],
            SCRAPING_CONFIG["max_delay"]
        )
        logger.info(f"Waiting {delay:.1f}s before request...")
        time.sleep(delay)

        domain_settings = self._get_domain_settings(url)
        needs_js = kwargs.get("browser", domain_settings.get("needs_js", True))
        headers = {"x-api-key": self.api_key}

        logger.info(f"ScrapingAnt Request:")
        logger.info(f"  URL: {url[:100]}...")
        logger.info(f"  Browser: {needs_js}")

        # Attempt 1: browser=true
        if needs_js:
            result = self._make_request(
                url=url, headers=headers,
                params={"url": url, "browser": "true", "proxy_country": "US"},
                attempt_label="browser=true", original_url=original_url,
            )
            if result["success"]:
                return result
            logger.info("Browser mode failed, trying without...")

        # Attempt 2: browser=false
        result = self._make_request(
            url=url, headers=headers,
            params={"url": url, "browser": "false", "proxy_country": "US"},
            attempt_label="browser=false", original_url=original_url,
        )
        if result["success"]:
            return result

        # Attempt 3: minimal
        logger.info("Trying minimal parameters...")
        result = self._make_request(
            url=url, headers=headers,
            params={"url": url},
            attempt_label="minimal", original_url=original_url,
        )
        if result["success"]:
            return result

        # ── ALL SCRAPING FAILED → NPI REGISTRY FALLBACK ──
        logger.info("  ALL SCRAPING FAILED → Trying NPI Registry fallback...")
        registry_result = npi_registry_fallback(original_url)

        if registry_result and registry_result.get("success"):
            self.registry_fallback_count += 1
            self.success_count += 1
            fake_html = build_registry_html(registry_result, original_url)

            return {
                "success": True,
                "html": fake_html,
                "status_code": 200,
                "url": original_url,
                "api_used": "ScrapingAnt+RegistryFallback",
                "registry_fallback": True,
                "registry_data": registry_result,
                "error": None,
            }

        # Everything failed
        self.fail_count += 1
        return {
            "success": False,
            "html": "",
            "url": original_url,
            "api_used": "ScrapingAnt",
            "error": "Scraping blocked + Registry fallback found no match",
        }

    def _make_request(self, url, headers, params, attempt_label, original_url):
        """Single API request with retries. Stops early for blocked sites."""
        last_error = None

        for attempt in range(1, SCRAPING_CONFIG["max_retries"] + 1):
            try:
                logger.info(f"  [{attempt_label}] Attempt {attempt}/{SCRAPING_CONFIG['max_retries']}...")

                response = self.session.get(
                    self.base_url, headers=headers, params=params,
                    timeout=SCRAPING_CONFIG["timeout"],
                )

                status = response.status_code
                content_len = len(response.text)
                logger.info(f"  Status: {status} | Length: {content_len} chars")

                if status == 200:
                    html = response.text

                    # Check if blocked/empty
                    if _is_blocked_response(html, content_len):
                        logger.warning(f"  Blocked/empty response ({content_len} chars)")
                        last_error = f"Blocked ({content_len} chars)"
                        # Don't retry blocked sites - they won't change
                        break

                    # Check minimum useful content
                    if content_len < 200:
                        logger.warning(f"  Very short response: {html[:80]}")
                        last_error = f"Too short ({content_len} chars)"
                        continue

                    credits = response.headers.get("Ant-credits-cost", "?")
                    remaining = response.headers.get("Ant-request-limit-remaining", "?")
                    self.success_count += 1
                    logger.info(f"  SUCCESS | Credits: {credits} | Remaining: {remaining}")

                    return {
                        "success": True, "html": html, "status_code": 200,
                        "url": original_url, "api_used": "ScrapingAnt",
                        "credits_used": credits, "error": None,
                    }

                elif status == 401:
                    self.fail_count += 1
                    return {"success": False, "html": "", "url": original_url,
                            "api_used": "ScrapingAnt", "error": "INVALID API KEY", "critical": True}

                elif status == 402:
                    self.fail_count += 1
                    return {"success": False, "html": "", "url": original_url,
                            "api_used": "ScrapingAnt", "error": "NO CREDITS", "critical": True}

                elif status == 422:
                    self.fail_count += 1
                    return {"success": False, "html": "", "url": original_url,
                            "api_used": "ScrapingAnt",
                            "error": f"Bad request (422): {response.text[:200]}"}

                elif status == 429:
                    time.sleep(30 * attempt)
                    continue

                elif status >= 500:
                    last_error = f"Server error {status}"
                    time.sleep(5 * attempt)
                    continue

                else:
                    last_error = f"HTTP {status}"
                    time.sleep(3 * attempt)
                    continue

            except requests.exceptions.Timeout:
                last_error = "Timeout"
                time.sleep(5 * attempt)
            except requests.exceptions.ConnectionError:
                last_error = "Connection error"
                time.sleep(5 * attempt)
            except Exception as e:
                last_error = str(e)[:100]
                time.sleep(3)

        return {
            "success": False, "html": "", "url": original_url,
            "api_used": "ScrapingAnt",
            "error": f"Failed [{attempt_label}]: {last_error}",
        }

    def get_stats(self):
        return {
            "total": self.request_count,
            "success": self.success_count,
            "failed": self.fail_count,
            "registry_fallbacks": self.registry_fallback_count,
        }


# ============================================
# WEBSCRAPING.AI CLIENT
# ============================================
class WebScrapingAIClient:
    """WebScraping.AI API + NPI Registry Fallback"""

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.webscraping.ai/html"
        self.session = requests.Session()
        self.request_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.registry_fallback_count = 0
        logger.info("WebScraping.AI client initialized")
        logger.info(f"  Endpoint: {self.base_url}")
        if api_key and len(api_key) > 12:
            logger.info(f"  API Key: {api_key[:8]}...{api_key[-4:]}")

    def _get_domain_settings(self, url):
        for domain, settings in DOMAIN_SETTINGS.items():
            if domain in url:
                return settings
        return {"needs_js": True}

    def test_connection(self):
        try:
            logger.info("Testing WebScraping.AI connection...")
            response = self.session.get(
                self.base_url,
                params={"api_key": self.api_key, "url": "https://httpbin.org/get",
                         "js": "false", "timeout": "10000"},
                timeout=30,
            )
            if response.status_code == 200:
                logger.info("WebScraping.AI connection test: SUCCESS")
                return {"success": True, "status": 200}
            elif response.status_code == 401:
                return {"success": False, "error": "Invalid API key"}
            else:
                return {"success": False, "error": f"Status {response.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def scrape(self, url, **kwargs):
        """Scrape URL → if blocked → NPI Registry fallback"""
        self.request_count += 1
        original_url = url
        url = clean_url(url)

        delay = random.uniform(
            SCRAPING_CONFIG["delay_between_requests"],
            SCRAPING_CONFIG["max_delay"]
        )
        logger.info(f"Waiting {delay:.1f}s before request...")
        time.sleep(delay)

        domain_settings = self._get_domain_settings(url)
        needs_js = kwargs.get("js", domain_settings.get("needs_js", True))

        logger.info(f"WebScraping.AI Request:")
        logger.info(f"  URL: {url[:100]}...")

        # Attempt 1: js=true
        if needs_js:
            result = self._make_request(
                url=url,
                params={"api_key": self.api_key, "url": url,
                         "js": "true", "proxy": "datacenter", "timeout": "20000"},
                attempt_label="js=true", original_url=original_url,
            )
            if result["success"]:
                return result

        # Attempt 2: js=false
        result = self._make_request(
            url=url,
            params={"api_key": self.api_key, "url": url,
                     "js": "false", "proxy": "datacenter", "timeout": "15000"},
            attempt_label="js=false", original_url=original_url,
        )
        if result["success"]:
            return result

        # Attempt 3: minimal
        result = self._make_request(
            url=url,
            params={"api_key": self.api_key, "url": url},
            attempt_label="minimal", original_url=original_url,
        )
        if result["success"]:
            return result

        # ── REGISTRY FALLBACK ──
        logger.info("  ALL SCRAPING FAILED → Trying NPI Registry fallback...")
        registry_result = npi_registry_fallback(original_url)

        if registry_result and registry_result.get("success"):
            self.registry_fallback_count += 1
            self.success_count += 1
            fake_html = build_registry_html(registry_result, original_url)

            return {
                "success": True, "html": fake_html, "status_code": 200,
                "url": original_url,
                "api_used": "WebScrapingAI+RegistryFallback",
                "registry_fallback": True,
                "registry_data": registry_result,
                "error": None,
            }

        self.fail_count += 1
        return {
            "success": False, "html": "", "url": original_url,
            "api_used": "WebScrapingAI",
            "error": "Scraping blocked + Registry fallback found no match",
        }

    def _make_request(self, url, params, attempt_label, original_url):
        """Single API request with retries"""
        last_error = None

        for attempt in range(1, SCRAPING_CONFIG["max_retries"] + 1):
            try:
                logger.info(f"  [{attempt_label}] Attempt {attempt}/{SCRAPING_CONFIG['max_retries']}...")

                response = self.session.get(
                    self.base_url, params=params,
                    timeout=SCRAPING_CONFIG["timeout"],
                )

                status = response.status_code
                content_len = len(response.text)
                logger.info(f"  Status: {status} | Length: {content_len} chars")

                if status == 200:
                    html = response.text

                    if _is_blocked_response(html, content_len):
                        logger.warning(f"  Blocked/empty response")
                        last_error = f"Blocked ({content_len} chars)"
                        break

                    if content_len < 200:
                        last_error = f"Too short ({content_len} chars)"
                        continue

                    self.success_count += 1
                    logger.info(f"  SUCCESS")
                    return {
                        "success": True, "html": html, "status_code": 200,
                        "url": original_url, "api_used": "WebScrapingAI", "error": None,
                    }

                elif status in [401, 402]:
                    self.fail_count += 1
                    return {"success": False, "html": "", "url": original_url,
                            "api_used": "WebScrapingAI",
                            "error": f"Auth error {status}", "critical": True}

                elif status == 429:
                    time.sleep(30 * attempt)
                    continue

                elif status >= 500:
                    last_error = f"Server error {status}"
                    time.sleep(5 * attempt)
                    continue

                else:
                    last_error = f"HTTP {status}"
                    time.sleep(3 * attempt)
                    continue

            except requests.exceptions.Timeout:
                last_error = "Timeout"
                time.sleep(5 * attempt)
            except requests.exceptions.ConnectionError:
                last_error = "Connection error"
                time.sleep(5 * attempt)
            except Exception as e:
                last_error = str(e)[:100]
                time.sleep(3)

        return {
            "success": False, "html": "", "url": original_url,
            "api_used": "WebScrapingAI",
            "error": f"Failed [{attempt_label}]: {last_error}",
        }

    def get_stats(self):
        return {
            "total": self.request_count,
            "success": self.success_count,
            "failed": self.fail_count,
            "registry_fallbacks": self.registry_fallback_count,
        }


# ============================================
# DUAL API CLIENT
# ============================================
class DualAPIClient:
    """Primary + Secondary. Both have registry fallback built in."""

    def __init__(self, primary_name, ant_key, wai_key):
        self.primary_name = primary_name
        self.ant_client = ScrapingAntClient(ant_key) if ant_key else None
        self.wai_client = WebScrapingAIClient(wai_key) if wai_key else None

        if primary_name == "scrapingant":
            self.primary = self.ant_client
            self.secondary = self.wai_client
            self.primary_label = "ScrapingAnt"
            self.secondary_label = "WebScrapingAI"
        else:
            self.primary = self.wai_client
            self.secondary = self.ant_client
            self.primary_label = "WebScrapingAI"
            self.secondary_label = "ScrapingAnt"

        logger.info(f"DualAPI: Primary={self.primary_label}, Fallback={self.secondary_label}")

    def test_connection(self):
        results = {}
        if self.ant_client:
            results["scrapingant"] = self.ant_client.test_connection()
        if self.wai_client:
            results["webscrapingai"] = self.wai_client.test_connection()
        return results

    def scrape(self, url, **kwargs):
        if self.primary:
            logger.info(f"Trying {self.primary_label}...")
            result = self.primary.scrape(url, **kwargs)
            if result["success"]:
                return result
            logger.warning(f"{self.primary_label} failed: {result.get('error', '?')}")

        if self.secondary:
            logger.info(f"Trying {self.secondary_label}...")
            result = self.secondary.scrape(url, **kwargs)
            result["used_fallback"] = True
            return result

        return {"success": False, "html": "", "url": url,
                "api_used": "none", "error": "No API available"}

    def get_stats(self):
        stats = {}
        if self.ant_client:
            stats["ScrapingAnt"] = self.ant_client.get_stats()
        if self.wai_client:
            stats["WebScrapingAI"] = self.wai_client.get_stats()
        return stats