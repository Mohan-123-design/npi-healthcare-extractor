# modules/google_search.py

import os
import time
from dotenv import load_dotenv
import google.generativeai as genai

from .web_utils import throttle, logger

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)


@throttle()
def google_grounded_search(
    npi_id: str,
    email: str = "",
    name: str = "",
    work_address: str = "",
) -> dict:
    """
    Uses Gemini (optionally with Google search grounding) to find the
    provider's workplace (company/clinic/hospital/organization) name.

    Returns dict:
        'found': bool,
        'text': str,  # very short plain text, ideally just the name,
        'mode': 'grounded' | 'ungrounded',
        'error': str (optional)
    """
    if not GOOGLE_API_KEY:
        logger.error("Google API key not set")
        return {"found": False, "error": "no_google_key"}

    prompt = f"""
Use Google search grounding (if available) to identify the *workplace name*
for this person.

Person identifiers:
- NPI: {npi_id}
- Email: {email}
- Name: {name}
- Physical / working address: {work_address}

Goal:
- Find the official name of the organization / company / clinic / hospital
  at this address where this person most likely works.

Sources to consider:
- Company / hospital websites
- Maps / business listings
- Professional profiles

Return ONLY the workplace name, as a single short line.
If you are not confident, return exactly: N/A
No address, no explanation, no extra text.
"""

    try:
        model = genai.GenerativeModel(model_name="gemini-2.0-flash")

        # First attempt: with grounding (if SDK supports it)
        try:
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.0,
                    "grounding": {"enable": True},
                },
            )
            text = response.text if hasattr(response, "text") else str(response)
            return {
                "found": bool(text and text.strip()),
                "text": text,
                "mode": "grounded",
            }

        except ValueError:
            # SDK/protobuf doesn't accept 'grounding' field — fall back
            logger.warning(
                "Google SDK rejected 'grounding' field; retrying without grounding. "
                "To enable grounding, upgrade google-generativeai/proto packages."
            )
            time.sleep(0.2)
            response = model.generate_content(
                prompt,
                generation_config={"temperature": 0.0},
            )
            text = response.text if hasattr(response, "text") else str(response)
            return {
                "found": bool(text and text.strip()),
                "text": text,
                "mode": "ungrounded",
            }

    except Exception as e:
        logger.exception("Google grounded search failed")
        return {"found": False, "error": str(e)}
