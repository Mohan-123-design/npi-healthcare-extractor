# config.py - FIXED Configuration with CORRECT API endpoints

import os
from dotenv import load_dotenv

load_dotenv()

# ============================================
# SCRAPINGANT API - CORRECT CONFIG
# Docs: https://docs.scrapingant.com/
# ============================================
SCRAPINGANT_CONFIG = {
    "name": "ScrapingAnt",
    "api_key": os.getenv("SCRAPINGANT_API_KEY", ""),

    # CORRECT official endpoint
    "base_url": "https://api.scrapingant.com/v2/general",

    # Default parameters from official docs
    "default_params": {
        "browser": True,
        "proxy_country": "US",
        "block_resource": "stylesheet,image,font,media",
        "return_page_source": False,
    },

    "rate_limit": {
        "requests_per_minute": 10,
        "delay_between": 3,
    },
}

# ============================================
# WEBSCRAPING.AI API - CORRECT CONFIG
# Docs: https://webscraping.ai/docs
# ============================================
WEBSCRAPINGAI_CONFIG = {
    "name": "WebScrapingAI",
    "api_key": os.getenv("WEBSCRAPINGAI_API_KEY", ""),

    # CORRECT official endpoint (NOT api.scraping.ai!)
    "base_url": "https://api.webscraping.ai/html",

    # Default parameters from official docs
    "default_params": {
        "js": True,
        "proxy": "datacenter",
        "timeout": 15000,
        "wait_until": "networkidle",
    },

    "rate_limit": {
        "requests_per_minute": 15,
        "delay_between": 4,
    },
}

# ============================================
# NPI EXTRACTION PATTERNS
# ============================================
NPI_PATTERNS = [
    r'(?:NPI|npi)[:\s#\-\.]*([1-9]\d{9})',
    r'National\s+Provider\s+Identifier[:\s]*([1-9]\d{9})',
    r'National\s+Provider\s+ID[:\s]*([1-9]\d{9})',
    r'NPI\s+Number[:\s]*([1-9]\d{9})',
    r'Provider\s+NPI[:\s]*([1-9]\d{9})',
    r'NPI[:\s]*#\s*([1-9]\d{9})',
    r'"npi"\s*:\s*"?([1-9]\d{9})"?',
    r'"npiNumber"\s*:\s*"?([1-9]\d{9})"?',
    r'"npi_number"\s*:\s*"?([1-9]\d{9})"?',
    r'"nationalProviderIdentifier"\s*:\s*"?([1-9]\d{9})"?',
    r'"providerNpi"\s*:\s*"?([1-9]\d{9})"?',
    r'"NPI"\s*:\s*"?([1-9]\d{9})"?',
    r'data-npi[="\s]+([1-9]\d{9})',
    r'data-provider-npi="([1-9]\d{9})"',
    r'npi[=_]([1-9]\d{9})',
    r'physician[_-]([1-9]\d{9})',
    r'provider[=_]([1-9]\d{9})',
    r'(?:^|\D)([1-9]\d{9})(?:\D|$)',
]

# ============================================
# NPI REGISTRY API (FREE)
# ============================================
NPI_REGISTRY_CONFIG = {
    "base_url": "https://npiregistry.cms.hhs.gov/api/",
    "version": "2.1",
    "timeout": 10,
    "enabled": os.getenv("VALIDATE_NPI_REGISTRY", "true").lower() == "true",
}

# ============================================
# FILE PATHS
# ============================================
BASE_OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
INPUT_DIR = os.getenv("INPUT_DIR", "input")
INPUT_FILENAME = os.getenv("INPUT_FILENAME", "input_urls.xlsx")

PATHS = {
    "input_dir": INPUT_DIR,
    "input_file": os.path.join(INPUT_DIR, INPUT_FILENAME),
    "output_dir": BASE_OUTPUT_DIR,
    "checkpoints_dir": os.path.join(BASE_OUTPUT_DIR, "checkpoints"),
    "scraped_dir": os.path.join(BASE_OUTPUT_DIR, "scraped_content"),
    "html_dir": os.path.join(BASE_OUTPUT_DIR, "scraped_content", "html"),
    "json_dir": os.path.join(BASE_OUTPUT_DIR, "scraped_content", "json"),
    "results_dir": os.path.join(BASE_OUTPUT_DIR, "results"),
    "logs_dir": os.path.join(BASE_OUTPUT_DIR, "logs"),
    "master_checkpoint": os.path.join(BASE_OUTPUT_DIR, "checkpoints", "master_checkpoint.json"),
    "progress_file": os.path.join(BASE_OUTPUT_DIR, "checkpoints", "progress.json"),
    "results_excel": os.path.join(BASE_OUTPUT_DIR, "results", "npi_results.xlsx"),
    "results_json": os.path.join(BASE_OUTPUT_DIR, "results", "npi_results.json"),
    "results_csv": os.path.join(BASE_OUTPUT_DIR, "results", "npi_results.csv"),
    "log_file": os.path.join(BASE_OUTPUT_DIR, "logs", "npi_extractor.log"),
}

# ============================================
# SCRAPING SETTINGS
# ============================================
SCRAPING_CONFIG = {
    "timeout": 60,
    "max_retries": int(os.getenv("MAX_RETRIES", "3")),
    "delay_between_requests": float(os.getenv("DELAY_BETWEEN_REQUESTS", "4")),
    "max_delay": 10,
    "checkpoint_every": 1,
    "excel_save_every": 1,
    "max_url_length": 2000,
}

# ============================================
# DOMAIN SPECIFIC SETTINGS
# ============================================
DOMAIN_SETTINGS = {
    "archwellhealth.com": {"needs_js": True, "wait_selector": "body"},
    "villagemedical.com": {"needs_js": True, "wait_selector": "body"},
    "convivacarecenters.com": {"needs_js": False},
    "wmchealth.org": {"needs_js": False},
    "umassmemorial.org": {"needs_js": False},
    "centralushealth.org": {"needs_js": True},
    "kaiserpermanente.org": {"needs_js": True},
}