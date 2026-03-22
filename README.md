# NPI Healthcare Data Extractor

A production-grade healthcare provider data extraction system that scrapes NPI numbers, credentials, specialties, and organization details from healthcare websites — with automatic API fallback and fault-tolerant design.

## What It Does

Given a list of healthcare provider URLs, this system:
1. Scrapes provider profile pages using commercial scraping APIs (ScrapingAnt + WebScraping.AI)
2. Falls back to the **NPI Registry API** if scraping returns no usable data
3. Normalizes and validates the extracted data
4. Exports a clean Excel file ready for downstream use

## Projects Inside

| Folder | Description |
|--------|-------------|
| `npiextract/` | Main NPI extraction system with DataGuard + checkpoint/resume |
| `nppesapi/` | Direct NPPES API pipeline for bulk NPI data fetching |
| `profile_google/` | Provider profile locator using Google Search |
| `profile_locator/` | Multi-source profile locator (Google + Perplexity) |
| `profile_perplexity/` | Profile locator using Perplexity AI search |

## Key Features

- **Dual-API scraping** — ScrapingAnt + WebScraping.AI with automatic failover
- **NPI Registry fallback** — if scraping fails, falls back to official NPI API
- **DataGuard module** — prevents writing partial or corrupted records
- **Checkpoint/resume** — long batch runs can be stopped and resumed without re-processing
- **Structured logging** — every URL processed is logged with success/failure reason
- **Output** — clean Excel with: Provider Name, NPI, Specialty, Credentials, Organization

## Tech Stack

- Python, Pandas, openpyxl
- ScrapingAnt API, WebScraping.AI API
- NPI Registry API (NPPES)
- Perplexity API, Google Search API
- Requests, BeautifulSoup

## Setup

```bash
pip install -r npiextract/requirements.txt
cp npiextract/.env.example npiextract/.env
# Add your API keys to .env
python npiextract/main.py
```

## Environment Variables

```
SCRAPINGANT_API_KEY=your_key
WEBSCRAPING_AI_KEY=your_key
PERPLEXITY_API_KEY=your_key
GOOGLE_API_KEY=your_key
GOOGLE_CSE_ID=your_cse_id
```

## Output

Clean Excel file with columns:
`Provider Name | NPI | Specialty | Credentials | Organization | Source | Status`
