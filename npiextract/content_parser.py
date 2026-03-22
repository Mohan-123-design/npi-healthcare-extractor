# content_parser.py - HTML Content Parser

import re
import json
import os
import hashlib
import logging
from bs4 import BeautifulSoup
from config import PATHS, NPI_PATTERNS

logger = logging.getLogger(__name__)


class ContentParser:
    """Parse HTML and extract all NPI-relevant content"""

    def __init__(self):
        self._ensure_dirs()

    def _ensure_dirs(self):
        for path in [PATHS["html_dir"], PATHS["json_dir"]]:
            os.makedirs(path, exist_ok=True)

    def _url_to_filename(self, url: str) -> str:
        """Safe unique filename from URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        domain_match = re.search(r'(?:https?://)?(?:www\.)?([^/]+)', url)
        domain = domain_match.group(1).replace('.', '_')[:25] if domain_match else "unknown"
        return f"{domain}_{url_hash}"

    def parse(self, html: str, url: str) -> dict:
        """Full HTML parsing - returns all extractable data"""
        if not html:
            return self._empty_result(url)

        soup = BeautifulSoup(html, 'lxml')

        # Extract components
        json_ld_data = self._extract_json_ld(soup)
        meta_data = self._extract_meta(soup)
        script_contents = self._extract_scripts(soup)
        data_attributes = self._extract_data_attrs(soup)
        visible_text = self._extract_text(soup)
        npi_context = self._extract_npi_context(visible_text)
        page_title = soup.title.string.strip() if soup.title and soup.title.string else ""

        return {
            "url": url,
            "page_title": page_title,
            "visible_text": visible_text,
            "json_ld_data": json_ld_data,
            "meta_data": meta_data,
            "script_contents": script_contents,
            "data_attributes": data_attributes,
            "npi_context_snippets": npi_context,
            "html_length": len(html),
        }

    def _extract_json_ld(self, soup) -> list:
        """Extract JSON-LD structured data (schema.org)"""
        results = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                if script.string:
                    data = json.loads(script.string.strip())
                    results.append(data)
            except json.JSONDecodeError:
                # Try to fix and re-parse
                try:
                    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', script.string or "")
                    data = json.loads(cleaned)
                    results.append(data)
                except:
                    pass
        return results

    def _extract_meta(self, soup) -> dict:
        """Extract all meta tags"""
        meta = {}
        for tag in soup.find_all('meta'):
            name = (tag.get('name') or tag.get('property') or 
                   tag.get('itemprop') or tag.get('id', ''))
            content = tag.get('content', '')
            if name and content:
                meta[name] = content
        return meta

    def _extract_scripts(self, soup) -> list:
        """Extract script contents relevant to NPI/provider"""
        relevant = []
        keywords = ['npi', 'provider', 'physician', 'doctor', 'identifier']
        
        for script in soup.find_all('script'):
            content = script.string or ""
            if content and any(kw in content.lower() for kw in keywords):
                # Truncate very long scripts
                relevant.append(content[:5000] if len(content) > 5000 else content)
        
        return relevant[:15]  # Max 15 scripts

    def _extract_data_attrs(self, soup) -> dict:
        """Extract data-* attributes related to providers"""
        attrs = {}
        npi_related = ['data-npi', 'data-provider', 'data-physician',
                       'data-doctor', 'data-identifier', 'data-id']
        
        for attr in npi_related:
            elements = soup.find_all(attrs={attr: True})
            for elem in elements:
                if elem.get(attr):
                    attrs[attr] = attrs.get(attr, [])
                    attrs[attr].append(elem.get(attr))
        
        # Also check itemprop
        for elem in soup.find_all(itemprop=True):
            prop = elem.get('itemprop', '')
            if any(kw in prop.lower() for kw in ['npi', 'identifier', 'provider']):
                content = elem.get('content') or elem.get_text(strip=True)
                if content:
                    attrs[f"itemprop_{prop}"] = content

        return attrs

    def _extract_text(self, soup) -> str:
        """Extract clean visible text"""
        # Remove non-content elements
        for tag in soup(['style', 'nav', 'footer', 'head',
                         'script[type!="application/ld+json"]']):
            tag.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        return text

    def _extract_npi_context(self, text: str) -> list:
        """Extract text snippets around potential NPI mentions"""
        contexts = []
        
        npi_markers = [
            r'NPI', r'National Provider', r'Provider ID',
            r'Provider Number', r'Identifier'
        ]
        
        for marker in npi_markers:
            for match in re.finditer(marker, text, re.IGNORECASE):
                start = max(0, match.start() - 20)
                end = min(len(text), match.end() + 50)
                snippet = text[start:end].strip()
                if snippet:
                    contexts.append(snippet)
        
        return contexts[:20]  # Max 20 snippets

    def _empty_result(self, url: str) -> dict:
        return {
            "url": url,
            "page_title": "",
            "visible_text": "",
            "json_ld_data": [],
            "meta_data": {},
            "script_contents": [],
            "data_attributes": {},
            "npi_context_snippets": [],
            "html_length": 0,
        }

    def save_content(self, url: str, html: str, parsed: dict) -> dict:
        """Save raw HTML and parsed JSON to files"""
        filename = self._url_to_filename(url)
        saved = {}

        try:
            # Save HTML
            html_path = os.path.join(PATHS["html_dir"], f"{filename}.html")
            with open(html_path, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(html)
            saved["html"] = html_path

            # Save parsed JSON
            json_path = os.path.join(PATHS["json_dir"], f"{filename}.json")
            json_data = {
                "url": url,
                "page_title": parsed.get("page_title", ""),
                "json_ld_data": parsed.get("json_ld_data", []),
                "meta_data": parsed.get("meta_data", {}),
                "data_attributes": parsed.get("data_attributes", {}),
                "npi_context_snippets": parsed.get("npi_context_snippets", []),
                "script_count": len(parsed.get("script_contents", [])),
                "visible_text_preview": parsed.get("visible_text", "")[:2000],
            }
            with open(json_path, 'w', encoding='utf-8', errors='ignore') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            saved["json"] = json_path

        except Exception as e:
            logger.error(f"Error saving content for {url}: {e}")

        return saved