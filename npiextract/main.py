# main.py - Master Controller with Batch + Registry Fallback

import os
import sys
import json
import logging
import time
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DIRS_TO_CREATE = [
    "input", "output", "output/results", "output/checkpoints",
    "output/scraped_content", "output/scraped_content/html",
    "output/scraped_content/json", "output/logs"
]
for d in DIRS_TO_CREATE:
    os.makedirs(d, exist_ok=True)

from config import PATHS, SCRAPINGANT_CONFIG, WEBSCRAPINGAI_CONFIG, SCRAPING_CONFIG
from api_clients import ScrapingAntClient, WebScrapingAIClient, DualAPIClient
from content_parser import ContentParser
from npi_extractor import NPIExtractor
from excel_manager import ExcelManager
from resume_manager import ResumeManager
from data_guard import DataGuard

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(PATHS["log_file"], encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

DEFAULT_URLS = [
    "https://archwellhealth.com/providers/david-charette-md/",
    "https://www.villagemedical.com/our-providers/daniel-piazza",
    "https://www.convivacarecenters.com/en/physicians/gwendolyn-mabel-casanova-felix-md",
    "https://www.wmchealth.org/physician-locator/sakina-khan-1184911406",
    "https://physicians.umassmemorial.org/details/187670/andrew-dilernia-family_medicine-primary_care-framingham-milford-worcester",
    "https://mydoctor.kaiserpermanente.org/ncal/doctor/aakashagarwal/qualifications",
    "https://healthy.kaiserpermanente.org/northern-california/physicians/jaya-francis-0339353",
]


def print_banner():
    print("\n" + "=" * 60)
    print("   NPI EXTRACTOR - Healthcare Provider Tool")
    print("   ScrapingAnt + WebScraping.AI + NPI Registry Fallback")
    print("   With Resume, Batch & Data Loss Prevention")
    print("=" * 60 + "\n")


def choose_api():
    print("=" * 50)
    print("  SELECT SCRAPING API")
    print("=" * 50)
    print()
    print("  [1] ScrapingAnt")
    print("  [2] WebScraping.AI")
    print("  [3] BOTH (Primary + Fallback) [RECOMMENDED]")
    print()

    while True:
        choice = input("  Your choice [1/2/3] (default=1): ").strip() or "1"
        if choice in ["1", "2", "3"]:
            break
        print("  Invalid choice.")

    api_map = {"1": "scrapingant", "2": "webscrapingai", "3": "both"}
    selected = api_map[choice]
    print(f"\n  Selected: {selected.upper()}\n")

    print("=" * 50)
    print("  API KEY CONFIGURATION")
    print("=" * 50)

    ant_key = os.getenv("SCRAPINGANT_API_KEY", "")
    wai_key = os.getenv("WEBSCRAPINGAI_API_KEY", "")

    if selected in ["scrapingant", "both"]:
        if ant_key and ant_key != "your_scrapingant_key_here":
            print(f"  ScrapingAnt key loaded from .env")
        else:
            ant_key = input("  Enter ScrapingAnt API Key: ").strip()
            if not ant_key and selected == "scrapingant":
                sys.exit("  No key. Exiting.")

    if selected in ["webscrapingai", "both"]:
        if wai_key and wai_key != "your_webscraping_ai_key_here":
            print(f"  WebScraping.AI key loaded from .env")
        else:
            wai_key = input("  Enter WebScraping.AI API Key: ").strip()
            if not wai_key and selected == "webscrapingai":
                sys.exit("  No key. Exiting.")

    return {
        "choice": selected,
        "scrapingant_key": ant_key,
        "webscrapingai_key": wai_key,
        "primary": "scrapingant" if choice in ["1", "3"] else "webscrapingai",
    }


def choose_input():
    print()
    print("=" * 50)
    print("  SELECT INPUT SOURCE")
    print("=" * 50)
    print()
    print(f"  [1] Use default sample URLs ({len(DEFAULT_URLS)} providers)")
    print(f"  [2] Load from Excel file")
    print(f"  [3] Load from CSV file")
    print()

    choice = input("  Your choice [1/2/3] (default=1): ").strip() or "1"

    if choice == "1":
        print(f"\n  Using {len(DEFAULT_URLS)} default URLs\n")
        return DEFAULT_URLS
    elif choice == "2":
        filepath = PATHS.get("input_file", "input/input_urls.xlsx")
        custom = input(f"  File path [{filepath}]: ").strip()
        if custom:
            filepath = custom
        if not os.path.exists(filepath):
            print(f"  File not found. Using defaults.\n")
            return DEFAULT_URLS
        try:
            import pandas as pd
            df = pd.read_excel(filepath)
            url_col = None
            for col in df.columns:
                if col.lower() in ['url', 'urls', 'link', 'links', 'website', 'profile_url']:
                    url_col = col
                    break
            if url_col is None:
                url_col = df.columns[0]
            urls = df[url_col].dropna().astype(str).tolist()
            urls = [u.strip() for u in urls if u.strip().startswith('http')]
            print(f"\n  Loaded {len(urls)} URLs from {filepath}\n")
            return urls
        except Exception as e:
            print(f"  Error: {e}. Using defaults.\n")
            return DEFAULT_URLS
    elif choice == "3":
        filepath = input("  CSV file path: ").strip()
        if not os.path.exists(filepath):
            return DEFAULT_URLS
        try:
            import csv
            with open(filepath, 'r', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            url_col = None
            for col in rows[0].keys():
                if col.lower() in ['url', 'urls', 'link']:
                    url_col = col
                    break
            if not url_col:
                url_col = list(rows[0].keys())[0]
            urls = [r[url_col].strip() for r in rows if r.get(url_col, '').strip().startswith('http')]
            print(f"\n  Loaded {len(urls)} URLs\n")
            return urls
        except Exception as e:
            print(f"  Error: {e}. Using defaults.\n")
            return DEFAULT_URLS
    return DEFAULT_URLS


# ============================================
# BATCH PROCESSING
# ============================================
def choose_batch_mode(total_urls):
    print()
    print("=" * 60)
    print("  BATCH PROCESSING CONFIGURATION")
    print("=" * 60)
    print(f"  Total URLs loaded: {total_urls}")
    print()
    print("  [1] Process ALL URLs")
    print("  [2] Custom range (e.g., rows 100-500)")
    print("  [3] Batch mode with fixed size")
    print("  [4] Single URL by row number")
    print()

    choice = input("  Your choice [1/2/3/4] (default=1): ").strip() or "1"

    if choice == "1":
        return {"mode": "all", "start": 0, "end": total_urls,
                "batch_size": total_urls, "description": f"All {total_urls} URLs"}
    elif choice == "2":
        return _get_custom_range(total_urls)
    elif choice == "3":
        return _get_batch_config(total_urls)
    elif choice == "4":
        return _get_single_url(total_urls)
    return {"mode": "all", "start": 0, "end": total_urls,
            "batch_size": total_urls, "description": f"All {total_urls} URLs"}


def _get_custom_range(total_urls):
    print(f"\n  Enter row range (1 to {total_urls})")
    while True:
        try:
            start = int(input(f"  Start row (default=1): ").strip() or "1")
            end = int(input(f"  End row (default={total_urls}): ").strip() or str(total_urls))
            if start < 1 or end > total_urls or start > end:
                print(f"  Invalid range.")
                continue
            count = end - start + 1
            print(f"  Range: Row {start}-{end} ({count} URLs)")
            if input(f"  Confirm? [Y/n]: ").strip().lower() == 'n':
                continue
            return {"mode": "range", "start": start - 1, "end": end,
                    "batch_size": count, "start_display": start, "end_display": end,
                    "description": f"Rows {start}-{end} ({count} URLs)"}
        except ValueError:
            print("  Enter valid numbers.")


def _get_batch_config(total_urls):
    while True:
        try:
            batch_size = int(input(f"  Batch size (default=500): ").strip() or "500")
            if batch_size < 1:
                continue
            total_batches = (total_urls + batch_size - 1) // batch_size
            print(f"\n  Batch size: {batch_size} | Total batches: {total_batches}\n")
            for b in range(total_batches):
                s = b * batch_size + 1
                e = min((b + 1) * batch_size, total_urls)
                print(f"    Batch {b+1}: Rows {s:>5} - {e:>5}  ({e-s+1} URLs)")
            print()
            batch_num = int(input(f"  Which batch? [1-{total_batches}] (default=1): ").strip() or "1")
            if batch_num < 1 or batch_num > total_batches:
                continue
            start_row = (batch_num - 1) * batch_size + 1
            end_row = min(batch_num * batch_size, total_urls)
            continuous = input(f"  Continue remaining batches? [y/N]: ").strip().lower()
            if continuous == 'y':
                end_row = total_urls
            if input(f"  Confirm? [Y/n]: ").strip().lower() == 'n':
                continue
            return {"mode": "batch", "start": start_row - 1, "end": end_row,
                    "batch_size": batch_size, "batch_num": batch_num,
                    "total_batches": total_batches, "continuous": continuous == 'y',
                    "start_display": start_row, "end_display": end_row,
                    "description": f"Batch {batch_num}: Rows {start_row}-{end_row}"}
        except ValueError:
            print("  Enter valid numbers.")


def _get_single_url(total_urls):
    while True:
        try:
            row = int(input(f"  Row number [1-{total_urls}]: ").strip())
            if 1 <= row <= total_urls:
                return {"mode": "single", "start": row - 1, "end": row,
                        "batch_size": 1, "description": f"Single URL: Row {row}"}
        except ValueError:
            pass
        print(f"  Must be 1-{total_urls}")


def create_api_client(api_config):
    choice = api_config["choice"]
    ant_key = api_config.get("scrapingant_key", "")
    wai_key = api_config.get("webscrapingai_key", "")
    if choice == "scrapingant":
        return ScrapingAntClient(ant_key), "ScrapingAnt"
    elif choice == "webscrapingai":
        return WebScrapingAIClient(wai_key), "WebScrapingAI"
    else:
        return DualAPIClient(api_config["primary"], ant_key, wai_key), "Dual"


def test_api_connection(client, name):
    print("\n  Testing API connection...")
    result = client.test_connection()
    if isinstance(result, dict) and result.get("success"):
        print(f"  {name} - Connection OK!\n")
        return True
    if isinstance(result, dict):
        any_ok = False
        for key in ["scrapingant", "webscrapingai"]:
            if key in result:
                if result[key].get("success"):
                    print(f"  {key} - OK")
                    any_ok = True
                else:
                    print(f"  {key} - FAILED: {result[key].get('error', '?')}")
        if any_ok:
            print()
            return True
    print(f"  Connection failed")
    return input("  Continue anyway? [y/N]: ").strip().lower() == 'y'


def process_url(url, api_client, api_name, parser, extractor):
    """Process single URL with registry fallback handling"""
    result = {
        "url": url, "npi_found": None, "extraction_method": None,
        "confidence": 0, "validation_status": None,
        "registry_name": None, "registry_specialty": None,
        "registry_state": None, "all_candidates": [],
        "api_used": api_name, "fetch_success": False,
        "error": None, "processed_at": datetime.now().isoformat(),
    }

    try:
        logger.info(f"Fetching: {url[:80]}...")
        fetch_result = api_client.scrape(url)

        result["api_used"] = fetch_result.get("api_used", api_name)
        result["fetch_success"] = fetch_result.get("success", False)

        # ── REGISTRY FALLBACK RESULT ──
        if fetch_result.get("registry_fallback") and fetch_result.get("registry_data"):
            reg = fetch_result["registry_data"]
            result["npi_found"] = reg.get("npi")
            result["extraction_method"] = "npi_registry_name_search"
            result["confidence"] = reg.get("confidence", 60)
            result["registry_name"] = reg.get("name")
            result["registry_specialty"] = reg.get("specialty")
            result["registry_state"] = reg.get("state")
            result["validation_status"] = "valid"
            result["api_used"] = fetch_result.get("api_used", "RegistryFallback")

            logger.info(
                f"NPI via Registry: {result['npi_found']} | "
                f"{result['registry_name']} | "
                f"Matches: {reg.get('total_matches', '?')} | "
                f"Confidence: {result['confidence']}%"
            )

            # Still save content
            html = fetch_result.get("html", "")
            if html:
                parsed = parser.parse(html, url)
                parser.save_content(url, html, parsed)

            return result

        # ── SCRAPING FAILED (no registry match either) ──
        if not fetch_result.get("success"):
            result["error"] = fetch_result.get("error", "Fetch failed")
            logger.warning(f"Fetch failed: {result['error']}")

            # Last resort: try URL parameter extraction
            url_only = extractor.extract(url, {}, "")
            if url_only.get("npi_found"):
                result.update(url_only)
                result["extraction_method"] = "url_parameter_only"
            return result

        # ── NORMAL SCRAPING SUCCESS ──
        html = fetch_result.get("html", "")
        if not html or len(html) < 100:
            result["error"] = f"Empty response ({len(html) if html else 0} chars)"
            return result

        parsed = parser.parse(html, url)
        parser.save_content(url, html, parsed)

        npi_result = extractor.extract(url, parsed, html)
        result.update(npi_result)

        if result.get("npi_found"):
            logger.info(
                f"NPI FOUND: {result['npi_found']} | "
                f"Method: {result['extraction_method']} | "
                f"Confidence: {result['confidence']}%"
            )
        else:
            logger.info(f"NPI not found for: {url[:60]}")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error processing {url}: {e}", exc_info=True)

    return result


def process_batch(urls, row_offset, api_client, api_name,
                  parser, extractor, data_guard, resume_mgr, batch_label=""):
    """Process a batch of URLs"""
    total = len(urls)
    batch_found = 0

    if batch_label:
        print(f"\n  >>> {batch_label}")

    for idx, url in enumerate(urls, 1):
        original_row = row_offset + idx

        print(f"\n{'─' * 60}")
        print(f"  [{idx}/{total}] Row #{original_row}:")
        print(f"  {url[:80]}{'...' if len(url) > 80 else ''}")
        print(f"{'─' * 60}")

        result = process_url(url, api_client, api_name, parser, extractor)
        result["original_row"] = original_row

        total_done = len(data_guard.running_results) + 1
        data_guard.save_result(result, total_done)

        if result.get("npi_found"):
            resume_mgr.mark_completed(url, result)
            method = result.get('extraction_method', '')
            name = result.get('registry_name', '')
            print(f"  NPI: {result['npi_found']} ({method})")
            if name:
                print(f"  Name: {name}")
            batch_found += 1
        else:
            if result.get("error"):
                resume_mgr.mark_failed(url, result.get("error", ""))
                print(f"  Failed: {result.get('error', '?')[:60]}")
            else:
                resume_mgr.mark_completed(url, result)
                print(f"  NPI not found")

        total_found = sum(1 for r in data_guard.running_results if r.get("npi_found"))
        total_processed = len(data_guard.running_results)
        rate = total_found / total_processed * 100 if total_processed else 0
        print(f"  Progress: {idx}/{total} | Row {original_row} | Found: {total_found} | Rate: {rate:.0f}%")

    return batch_found


# ============================================
# MAIN
# ============================================
def main():
    print_banner()
    start_time = time.time()

    resume_mgr = ResumeManager()
    excel_mgr = ExcelManager()
    do_resume = False
    batch_config = None

    if resume_mgr.has_existing_session():
        info = resume_mgr.get_session_info()
        print("  PREVIOUS SESSION FOUND:")
        print(f"    Completed: {info.get('completed_count', 0)}/{info.get('total_urls', 0)}")
        print(f"    Remaining: {info.get('remaining', 0)}")
        print(f"    NPI Found: {info.get('npi_found_count', 0)}")
        print()
        do_resume = input("  Resume? [Y/n]: ").strip().lower() != 'n'

    if do_resume:
        resume_mgr.resume_session()
        api_config = choose_api()
        pending = resume_mgr.get_pending_urls()
        print(f"  Resuming: {len(pending)} URLs remaining")
        if len(pending) > 50:
            if input(f"  Use batch mode? [y/N]: ").strip().lower() == 'y':
                batch_config = choose_batch_mode(len(pending))
                pending = pending[batch_config["start"]:batch_config["end"]]
        all_urls = pending
        row_offset = 0
    else:
        api_config = choose_api()
        all_urls_full = choose_input()
        if not all_urls_full:
            print("  No URLs. Exiting.")
            return
        batch_config = choose_batch_mode(len(all_urls_full))
        start = batch_config["start"]
        end = batch_config["end"]
        all_urls = all_urls_full[start:end]
        row_offset = start
        resume_mgr.start_new_session(all_urls, api_config["choice"])

    api_client, api_display_name = create_api_client(api_config)
    if not test_api_connection(api_client, api_display_name):
        print("  Exiting.")
        return

    parser = ContentParser()
    extractor = NPIExtractor()
    data_guard = DataGuard(excel_manager=excel_mgr)

    if do_resume:
        existing = resume_mgr.get_completed_results()
        data_guard.running_results = existing
        print(f"  Loaded {len(existing)} previous results")

    total = len(all_urls)
    batch_desc = batch_config["description"] if batch_config else f"All {total} URLs"

    print()
    print("=" * 60)
    print(f"  STARTING: {batch_desc}")
    print(f"  API: {api_display_name}")
    print(f"  NPI Registry Fallback: ENABLED")
    print("=" * 60)

    # Continuous batch mode
    if batch_config and batch_config.get("mode") == "batch" and batch_config.get("continuous"):
        batch_size = batch_config["batch_size"]
        batch_num = batch_config.get("batch_num", 1)
        i = 0
        while i < total:
            chunk_end = min(i + batch_size, total)
            chunk = all_urls[i:chunk_end]
            chunk_offset = row_offset + i
            label = f"BATCH {batch_num}: Rows {chunk_offset+1}-{chunk_offset+len(chunk)} ({len(chunk)} URLs)"
            process_batch(chunk, chunk_offset, api_client, api_display_name,
                          parser, extractor, data_guard, resume_mgr, batch_label=label)
            batch_num += 1
            i = chunk_end
            excel_mgr.write_results(data_guard.running_results, PATHS["results_excel"])
            if i < total:
                remaining = total - i
                print(f"\n  BATCH COMPLETE | Remaining: {remaining} URLs")
                cont = input(f"  Continue? [Y/n/q]: ").strip().lower()
                if cont in ['n', 'q']:
                    print(f"  Paused. Run again to continue.")
                    break
    else:
        process_batch(all_urls, row_offset, api_client, api_display_name,
                      parser, extractor, data_guard, resume_mgr, batch_label=batch_desc)

    # Final save
    print("\n" + "=" * 60)
    print("  SAVING FINAL RESULTS...")
    print("=" * 60)

    all_results = data_guard.running_results
    excel_mgr.write_results(all_results, PATHS["results_excel"])
    resume_mgr.mark_session_complete()

    elapsed = time.time() - start_time
    found_total = sum(1 for r in all_results if r.get("npi_found"))
    registry_count = sum(1 for r in all_results
                         if r.get("extraction_method") == "npi_registry_name_search")

    print()
    print("=" * 60)
    print("  EXTRACTION COMPLETE!")
    print("=" * 60)
    print(f"  Batch:             {batch_desc}")
    print(f"  Total Processed:   {len(all_results)}")
    print(f"  NPI Found:         {found_total}")
    print(f"    Via Scraping:    {found_total - registry_count}")
    print(f"    Via Registry:    {registry_count}")
    print(f"  NPI Not Found:     {len(all_results) - found_total}")
    if all_results:
        print(f"  Success Rate:      {found_total/len(all_results)*100:.1f}%")
    print(f"  Time:              {elapsed:.1f}s")
    print(f"  API:               {api_display_name}")
    print()
    print(f"  OUTPUT:")
    print(f"    Excel: {PATHS['results_excel']}")
    print(f"    CSV:   {PATHS['results_csv']}")
    print(f"    JSON:  {PATHS['results_json']}")
    print()

    print("  RESULTS:")
    print(f"  {'─' * 70}")
    print(f"  {'Row':>5} | {'NPI':>12} | {'Method':<25} | URL")
    print(f"  {'─' * 70}")

    for r in all_results:
        url_short = r['url'][:28] + "..." if len(r['url']) > 28 else r['url']
        npi = r.get('npi_found') or 'NOT FOUND'
        method = r.get('extraction_method') or ''
        row = r.get('original_row', '?')
        name = r.get('registry_name') or ''
        s = "+" if npi != 'NOT FOUND' else "-"
        print(f"  {s} {row:>4} | {npi:>12} | {method:<25} | {url_short}")
        if name:
            print(f"         |              | {name}")

    print(f"  {'─' * 70}")

    if hasattr(api_client, 'get_stats'):
        print(f"\n  API STATS: {json.dumps(api_client.get_stats(), indent=4)}")

    print(f"\n  Done! Open: {PATHS['results_excel']}")

    if batch_config and batch_config.get("mode") in ["range", "batch"]:
        if input("\n  Run another batch? [y/N]: ").strip().lower() == 'y':
            main()

    return all_results


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Interrupted. Data saved. Run again to resume.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n  Fatal error: {e}")
        print("  Check: output/logs/npi_extractor.log")