# modules/web_utils.py

import time
import logging
from functools import wraps
from dotenv import load_dotenv
import os
import requests
from requests.adapters import HTTPAdapter, Retry
import threading

load_dotenv()

logger = logging.getLogger("profile_locator")

if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(ch)

logger.setLevel(logging.INFO)

# Default delay chosen so Perplexity calls stay under ~40 RPM per function
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.5"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))


def throttle(delay=REQUEST_DELAY):
    """
    Per-function global rate limiter.

    Guarantees at least `delay` seconds between two calls to the same
    decorated function, across all threads.
    """
    def deco(func):
        last_call = 0.0
        lock = threading.Lock()

        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal last_call
            with lock:
                now = time.time()
                wait = last_call + delay - now
                if wait > 0:
                    time.sleep(wait)
                last_call = time.time()
            return func(*args, **kwargs)

        return inner

    return deco


def requests_session():
    s = requests.Session()
    retries = Retry(
        total=RETRY_COUNT,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s
