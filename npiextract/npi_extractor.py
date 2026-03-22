# npi_extractor.py - Multi-Method NPI Extraction Engine

import re
import json
import requests
import logging
from config import NPI_PATTERNS, NPI_REGISTRY_CONFIG

logger = logging.getLogger(__name__)


class NPIExtractor:
    """
    8-Method NPI Extraction Engine
    
    Priority Order:
    1. URL Parameter (fastest, highest confidence)
    2. JSON-LD Structured Data (most reliable on medical sites)
    3. Data Attributes (very reliable)
    4. Meta Tags (reliable)
    5. Script Content (good for SPAs)
    6. Visible Text (labeled patterns)
    7. Raw HTML regex (broad search)
    8. NPI Registry fallback (by name from URL)
    """

    def __init__(self):
        self.registry_cache = {}
        logger.info("NPI Extractor initialized")

    def extract(self, url: str, parsed_content: dict, raw_html: str = "") -> dict:
        """
        Master extraction - runs all methods, returns best result
        """
        all_candidates = []

        # Method 1: URL-based extraction
        url_npis = self._from_url(url)
        for npi in url_npis:
            all_candidates.append({
                "npi": npi,
                "method": "url_parameter",
                "confidence": 85,
                "source": url
            })

        # Method 2: JSON-LD
        jl_npis = self._from_json_ld(parsed_content.get("json_ld_data", []))
        for npi in jl_npis:
            all_candidates.append({
                "npi": npi,
                "method": "json_ld",
                "confidence": 92,
                "source": "JSON-LD structured data"
            })

        # Method 3: Data attributes
        da_npis = self._from_data_attrs(parsed_content.get("data_attributes", {}))
        for npi in da_npis:
            all_candidates.append({
                "npi": npi,
                "method": "data_attributes",
                "confidence": 90,
                "source": "HTML data attributes"
            })

        # Method 4: Meta tags
        meta_npis = self._from_meta(parsed_content.get("meta_data", {}))
        for npi in meta_npis:
            all_candidates.append({
                "npi": npi,
                "method": "meta_tags",
                "confidence": 88,
                "source": "Meta tags"
            })

        # Method 5: Script content
        script_npis = self._from_scripts(parsed_content.get("script_contents", []))
        for npi in script_npis:
            all_candidates.append({
                "npi": npi,
                "method": "script_content",
                "confidence": 82,
                "source": "JavaScript content"
            })

        # Method 6: Visible text (labeled)
        text_npis = self._from_text_labeled(parsed_content.get("visible_text", ""))
        for npi in text_npis:
            all_candidates.append({
                "npi": npi,
                "method": "visible_text",
                "confidence": 78,
                "source": "Page visible text"
            })

        # Method 7: Context snippets
        context_npis = self._from_context(
            parsed_content.get("npi_context_snippets", [])
        )
        for npi in context_npis:
            all_candidates.append({
                "npi": npi,
                "method": "context_snippet",
                "confidence": 75,
                "source": "NPI context snippet"
            })

        # Method 8: Raw HTML (broad)
        if raw_html and not all_candidates:
            html_npis = self._from_raw_html(raw_html)
            for npi in html_npis:
                all_candidates.append({
                    "npi": npi,
                    "method": "raw_html",
                    "confidence": 60,
                    "source": "Raw HTML pattern"
                })

        # Deduplicate and sort by confidence
        all_candidates = self._dedupe(all_candidates)
        all_candidates.sort(key=lambda x: x["confidence"], reverse=True)

        # Build result
        result = {
            "npi_found": None,
            "extraction_method": None,
            "confidence": 0,
            "all_candidates": all_candidates,
            "validation_status": None,
            "registry_name": None,
            "registry_specialty": None,
            "registry_state": None,
        }

        if all_candidates:
            best = all_candidates[0]
            
            # Validate with NPI Registry API
            if NPI_REGISTRY_CONFIG["enabled"]:
                validation = self._validate_npi(best["npi"])
                result.update({
                    "validation_status": validation.get("status"),
                    "registry_name": validation.get("name"),
                    "registry_specialty": validation.get("specialty"),
                    "registry_state": validation.get("state"),
                })
                
                # Boost confidence if registry validates
                if validation.get("status") == "valid":
                    best["confidence"] = min(99, best["confidence"] + 10)

            result.update({
                "npi_found": best["npi"],
                "extraction_method": best["method"],
                "confidence": best["confidence"],
            })

        return result

    # ==============================
    # Method 1: URL Extraction
    # ==============================
    def _from_url(self, url: str) -> list:
        """Extract NPI directly from URL"""
        npis = []
        
        patterns = [
            # Direct NPI in URL path (wmchealth style: name-1184911406)
            r'[a-z\-]+-(\d{10})(?:\?|&|$|/)',
            r'/([1-9]\d{9})(?:\?|&|/|$)',
            r'npi[=_]([1-9]\d{9})',
            r'physician[_-](\d{10})',
            r'provider[_-](\d{10})',
            r'doctor[_-](\d{10})',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, url, re.IGNORECASE)
            for m in matches:
                if self._valid_format(m):
                    npis.append(m)
        
        return list(set(npis))

    # ==============================
    # Method 2: JSON-LD
    # ==============================
    def _from_json_ld(self, json_ld_list: list) -> list:
        """Extract NPI from JSON-LD structured data"""
        npis = []
        npi_keys = ['npi', 'npinumber', 'npi_number', 'nationalprovideridentifier',
                    'providerid', 'providernpi', 'identifier', 'id']

        def _search(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k.lower().replace('-', '').replace('_', '') in npi_keys:
                        val_str = str(v).strip()
                        if self._valid_format(val_str):
                            npis.append(val_str)
                    _search(v)
            elif isinstance(obj, list):
                for item in obj:
                    _search(item)
            elif isinstance(obj, str):
                for pat in NPI_PATTERNS[:8]:
                    for m in re.findall(pat, obj, re.IGNORECASE):
                        m = m[0] if isinstance(m, tuple) else m
                        if self._valid_format(m):
                            npis.append(m)

        for jld in json_ld_list:
            _search(jld)

        return list(set(npis))

    # ==============================
    # Method 3: Data Attributes
    # ==============================
    def _from_data_attrs(self, attrs: dict) -> list:
        """Extract NPI from HTML data attributes"""
        npis = []
        for key, value in attrs.items():
            values = value if isinstance(value, list) else [value]
            for v in values:
                v_str = str(v).strip()
                if self._valid_format(v_str):
                    npis.append(v_str)
                else:
                    # Search within value
                    for pat in NPI_PATTERNS[:5]:
                        for m in re.findall(pat, v_str, re.IGNORECASE):
                            m = m[0] if isinstance(m, tuple) else m
                            if self._valid_format(m):
                                npis.append(m)
        return list(set(npis))

    # ==============================
    # Method 4: Meta Tags
    # ==============================
    def _from_meta(self, meta: dict) -> list:
        """Extract NPI from meta tags"""
        npis = []
        npi_meta_keys = ['npi', 'provider-id', 'physician-id', 'npi-number',
                         'provider_npi', 'doctor-id']
        
        for key, value in meta.items():
            if any(nk in key.lower() for nk in npi_meta_keys):
                if self._valid_format(str(value)):
                    npis.append(str(value))
            
            # Search in all meta values for labeled NPI
            for pat in NPI_PATTERNS[:6]:
                for m in re.findall(pat, str(value), re.IGNORECASE):
                    m = m[0] if isinstance(m, tuple) else m
                    if self._valid_format(m):
                        npis.append(m)
        
        return list(set(npis))

    # ==============================
    # Method 5: Scripts
    # ==============================
    def _from_scripts(self, scripts: list) -> list:
        """Extract NPI from JavaScript content"""
        npis = []
        for script in scripts:
            for pat in NPI_PATTERNS:
                for m in re.findall(pat, script, re.IGNORECASE):
                    m = m[0] if isinstance(m, tuple) else m
                    if self._valid_format(m):
                        npis.append(m)
        return list(set(npis))

    # ==============================
    # Method 6: Visible Text (Labeled)
    # ==============================
    def _from_text_labeled(self, text: str) -> list:
        """Extract NPI from visible text - only labeled patterns"""
        if not text:
            return []
        
        npis = []
        # Only use explicitly labeled patterns for visible text
        labeled = [
            r'NPI[:\s#\-\.]*([1-9]\d{9})',
            r'National\s+Provider\s+Identifier[:\s]*([1-9]\d{9})',
            r'NPI\s+Number[:\s]*([1-9]\d{9})',
            r'Provider\s+NPI[:\s]*([1-9]\d{9})',
            r'NPI\s*#\s*([1-9]\d{9})',
            r'NPI[:\s]*([1-9]\d{9})',
        ]
        
        for pat in labeled:
            for m in re.findall(pat, text, re.IGNORECASE):
                m = m[0] if isinstance(m, tuple) else m
                if self._valid_format(m):
                    npis.append(m)
        
        return list(set(npis))

    # ==============================
    # Method 7: Context Snippets
    # ==============================
    def _from_context(self, snippets: list) -> list:
        """Extract NPI from context snippets around NPI keywords"""
        npis = []
        for snippet in snippets:
            for pat in NPI_PATTERNS:
                for m in re.findall(pat, snippet, re.IGNORECASE):
                    m = m[0] if isinstance(m, tuple) else m
                    if self._valid_format(m):
                        npis.append(m)
        return list(set(npis))

    # ==============================
    # Method 8: Raw HTML
    # ==============================
    def _from_raw_html(self, html: str) -> list:
        """Broad regex search on raw HTML - last resort"""
        npis = []
        for pat in NPI_PATTERNS:
            for m in re.findall(pat, html, re.IGNORECASE):
                m = m[0] if isinstance(m, tuple) else m
                if self._valid_format(m):
                    npis.append(m)
        return list(set(npis))

    # ==============================
    # Validation
    # ==============================
    def _valid_format(self, value: str) -> bool:
        """Validate NPI format + Luhn check"""
        value = str(value).strip()
        if not re.match(r'^[12]\d{9}$', value):
            return False
        return self._luhn_check(value)

    def _luhn_check(self, npi: str) -> bool:
        """NPI Luhn algorithm with 80840 prefix"""
        try:
            full = "80840" + npi
            total = 0
            for i, ch in enumerate(reversed(full)):
                n = int(ch)
                if i % 2 == 1:
                    n *= 2
                    if n > 9:
                        n -= 9
                total += n
            return total % 10 == 0
        except:
            return False

    def _validate_npi(self, npi: str) -> dict:
        """Validate NPI against CMS Registry API (free)"""
        if npi in self.registry_cache:
            return self.registry_cache[npi]

        try:
            url = (f"{NPI_REGISTRY_CONFIG['base_url']}"
                   f"?number={npi}&version={NPI_REGISTRY_CONFIG['version']}")
            
            resp = requests.get(url, timeout=NPI_REGISTRY_CONFIG["timeout"])
            
            if resp.status_code == 200:
                data = resp.json()
                
                if data.get("result_count", 0) > 0:
                    provider = data["results"][0]
                    basic = provider.get("basic", {})
                    taxonomies = provider.get("taxonomies", [])
                    addresses = provider.get("addresses", [])

                    if basic.get("organization_name"):
                        name = basic["organization_name"]
                    else:
                        name = (
                            f"{basic.get('first_name','')} "
                            f"{basic.get('last_name','')} "
                            f"{basic.get('credential','')}".strip()
                        )

                    result = {
                        "status": "valid",
                        "name": name,
                        "specialty": taxonomies[0].get("desc", "") if taxonomies else "",
                        "state": addresses[0].get("state", "") if addresses else "",
                        "npi_type": provider.get("enumeration_type", ""),
                    }
                else:
                    result = {"status": "not_found_in_registry"}
            else:
                result = {"status": "api_error"}

        except Exception as e:
            logger.warning(f"NPI registry validation error: {e}")
            result = {"status": "validation_failed"}

        self.registry_cache[npi] = result
        return result

    def _dedupe(self, candidates: list) -> list:
        """Deduplicate keeping highest confidence per NPI"""
        seen = {}
        for c in candidates:
            npi = c["npi"]
            if npi not in seen or c["confidence"] > seen[npi]["confidence"]:
                seen[npi] = c
        return list(seen.values())

    def registry_search_by_name(self, first: str, last: str,
                                 state: str = "") -> list:
        """Search NPI registry by name (fallback method)"""
        try:
            params = {
                "version": "2.1",
                "first_name": first,
                "last_name": last,
                "limit": 5,
            }
            if state:
                params["state"] = state

            resp = requests.get(
                NPI_REGISTRY_CONFIG["base_url"],
                params=params,
                timeout=NPI_REGISTRY_CONFIG["timeout"]
            )

            if resp.status_code == 200:
                data = resp.json()
                results = []
                for p in data.get("results", []):
                    basic = p.get("basic", {})
                    taxonomies = p.get("taxonomies", [])
                    results.append({
                        "npi": p.get("number"),
                        "name": f"{basic.get('first_name','')} {basic.get('last_name','')}".strip(),
                        "credential": basic.get("credential", ""),
                        "specialty": taxonomies[0].get("desc", "") if taxonomies else "",
                    })
                return results
        except Exception as e:
            logger.error(f"Registry name search failed: {e}")
        return []