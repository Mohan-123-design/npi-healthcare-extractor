# Profile Locator - NPI + Email -> Organization/Profile Pages (Perplexity + Google)

This project reads an input CSV with `npi_id` and `email`, runs Google Gemini (grounded)
and Perplexity grounded searches in parallel to find public organization/profile pages
and extract practice locations/clinic addresses.

## Setup
1. Create a Python 3.10+ virtual environment and activate it.
   ```bash
   python -m venv venv
   source venv/bin/activate   # windows: venv\Scripts\activate
   ```
2. Copy `.env.example` to `.env` and fill your API keys.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Put your input CSV at `input/npi_email_input.csv` (header: npi_id,email,doctor_name).
5. Run:
   ```bash
   python main.py
   ```
6. Results are written to `output/results_<timestamp>.csv`.

## Notes
- Tune `BATCH_SIZE` and `MAX_WORKERS` in `.env` to match API quota.
- The repo intentionally **does not** scrape profile pages automatically (to respect robots). It uses grounded AI outputs and parses URLs/address text.
