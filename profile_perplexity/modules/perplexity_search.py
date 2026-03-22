# modules/perplexity_search.py

import os
import json
import threading
import requests
from dotenv import load_dotenv

from .web_utils import throttle, requests_session, logger

load_dotenv()

PPLX_KEY_SINGLE = os.getenv("PERPLEXITY_API_KEY")
PPLX_KEYS_ENV = os.getenv("PERPLEXITY_API_KEYS", "")

# Build list of keys: PERPLEXITY_API_KEYS takes precedence if present
_PPLX_KEYS = [k.strip() for k in PPLX_KEYS_ENV.split(",") if k.strip()]
if not _PPLX_KEYS and PPLX_KEY_SINGLE:
    _PPLX_KEYS = [PPLX_KEY_SINGLE]

_KEY_LOCK = threading.Lock()
_KEY_INDEX = 0

PPLX_ENDPOINT = "https://api.perplexity.ai/chat/completions"


def _get_next_key():
    """
    Rotate through available Perplexity keys in a thread-safe way.
    Returns (api_key, index). If no keys, returns (None, -1).
    """
    global _KEY_INDEX
    with _KEY_LOCK:
        if not _PPLX_KEYS:
            return None, -1
        api_key = _PPLX_KEYS[_KEY_INDEX]
        idx = _KEY_INDEX
        _KEY_INDEX = (_KEY_INDEX + 1) % len(_PPLX_KEYS)
    return api_key, idx


def _classify_http_error(resp: requests.Response) -> str:
    """
    Inspect HTTP error and response body to classify error type.

    Returns one of:
      'quota_exceeded', 'rate_limited', 'auth_error',
      'server_error', 'http_error'
    """
    status = resp.status_code
    body_text = ""
    try:
        body_text = resp.text or ""
    except Exception:
        body_text = ""

    body_l = body_text.lower()

    # Auth / key problems
    if status in (401, 403):
        return "auth_error"

    # Quota / rate
    if status in (402, 429):
        if "quota" in body_l or "billing" in body_l or "plan and billing" in body_l:
            return "quota_exceeded"
        return "rate_limited"

    # Server side
    if 500 <= status < 600:
        return "server_error"

    return "http_error"


@throttle()
def perplexity_search(
    npi_id: str,
    email: str = "",
    name: str = "",
    work_address: str = "",
) -> dict:
    """
    Use Perplexity to find the provider's workplace (company/clinic/hospital)
    name from ID, email, and physical address.

    Returns dict:
        "found": bool,
        "text": str,       # ideally just the name, one line or "N/A"
        "error": str | None,
        "error_type": str  # machine-friendly code (see above)
    """
    if not _PPLX_KEYS:
        logger.error("Perplexity key(s) not set")
        return {
            "found": False,
            "text": "",
            "error": "no_pplx_key",
            "error_type": "auth_missing",
        }

    prompt = f"""
You are given information about a person and their physical working address.

Person identifiers:
- NPI ID: {npi_id}
- Email: {email}
- Name: {name}
- Physical / working address: {work_address}

Task:
- Identify the official name of the organization / company / clinic / hospital
  located at this address where this person most likely works.

Use reputable web sources such as:
- Company / hospital websites
- Business listings
- Provider directories
- Professional profiles

Return ONLY the workplace name, as a single short line.
If you are not confident or nothing is found, return exactly: N/A
No address and no explanation.
"""

    payload = {
        "model": "sonar",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 200,
    }

    last_error_type = None
    last_error_msg = None

    # Try each key at most once for this call
    for _ in range(len(_PPLX_KEYS)):
        api_key, key_idx = _get_next_key()
        if not api_key:
            break

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            s = requests_session()
            r = s.post(PPLX_ENDPOINT, headers=headers, json=payload, timeout=30)

            # If non-2xx, classify and maybe try another key
            if r.status_code < 200 or r.status_code >= 300:
                err_type = _classify_http_error(r)
                last_error_type = err_type
                last_error_msg = (
                    f"HTTP {r.status_code} for Perplexity key index {key_idx} "
                    f"({err_type})"
                )
                logger.error(last_error_msg)

                # For quota/rate/auth, try next key; for server/http, break
                if err_type in ("quota_exceeded", "rate_limited", "auth_error"):
                    continue
                else:
                    break

            # 2xx OK
            j = r.json()
            content = ""
            if isinstance(j, dict):
                if "choices" in j and j["choices"]:
                    content = j["choices"][0].get("message", {}).get("content", "")
                elif "text" in j:
                    content = j.get("text", "")

            text = (content or "").strip()
            return {
                "found": bool(text),
                "text": text,
                "error": None,
                "error_type": "ok",
            }

        except requests.exceptions.RequestException as e:
            last_error_type = "network_error"
            last_error_msg = (
                f"Network error for Perplexity key index {key_idx}: {e}"
            )
            logger.exception(last_error_msg)
            # Try next key if any

        except Exception as e:
            last_error_type = "unknown_error"
            last_error_msg = (
                f"Unexpected error for Perplexity key index {key_idx}: {e}"
            )
            logger.exception(last_error_msg)
            break

    # All keys failed
    if last_error_type in ("quota_exceeded", "rate_limited"):
        combined_type = "quota_exceeded_all_keys"
    elif last_error_type in ("auth_error", "auth_missing"):
        combined_type = "auth_error_all_keys"
    else:
        combined_type = last_error_type or "unknown_error"

    return {
        "found": False,
        "text": "",
        "error": last_error_msg or combined_type,
        "error_type": combined_type,
    }
