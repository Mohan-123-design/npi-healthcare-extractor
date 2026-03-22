# main.py

import os
import concurrent.futures
import time
import json

from dotenv import load_dotenv

from modules.sheet_csv_handler import read_input_csv
from modules.google_search import google_grounded_search
from modules.web_utils import logger

load_dotenv()

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "4"))  # small threaded batch
BIG_BATCH_SIZE = int(os.getenv("BIG_BATCH_SIZE", "1000"))  # ask every 1000 rows

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
INPUT_CSV = os.getenv("INPUT_CSV", "input/npi_email_input.csv")
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))

PROCESSED_JSON = os.path.join(OUTPUT_DIR, "processed_ids.json")
LATEST_CSV = os.path.join(OUTPUT_DIR, "latest_results.csv")


def load_processed_set():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    if os.path.exists(PROCESSED_JSON):
        try:
            with open(PROCESSED_JSON, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_processed_set(s):
    with open(PROCESSED_JSON, "w", encoding="utf-8") as f:
        json.dump(list(s), f)


def append_results_csv(rows, filename=None):
    """
    Append results to latest_results.csv and also write a timestamped snapshot.
    """
    import pandas as pd
    import datetime

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ts_file = os.path.join(OUTPUT_DIR, f"results_{timestamp}.csv")

    df = pd.DataFrame(rows)

    # append to latest_results (if exists append, else write)
    if os.path.exists(LATEST_CSV):
        df.to_csv(LATEST_CSV, mode="a", header=False, index=False)
    else:
        df.to_csv(LATEST_CSV, index=False)

    # write timestamped file as snapshot of this append
    df.to_csv(ts_file, index=False)

    logger.info(
        f"Appended {len(rows)} rows to {LATEST_CSV} and wrote snapshot to {ts_file}"
    )

    return LATEST_CSV


def _extract_workplace_name_from_text(text: str) -> str:
    """
    Given the plain-text response, extract a single workplace/organization name.
    """
    if not text:
        return ""

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return ""

    def is_disclaimer(line_l: str) -> bool:
        patterns = [
            "cannot determine",
            "cannot identify",
            "unable to",
            "not able to",
            "no information",
            "do not contain",
            "insufficient information",
            "based on the provided search results",
            "based on the search results",
        ]
        return any(p in line_l for p in patterns)

    def strip_prefix(line: str, prefixes: list):
        line_l = line.lower()
        for p in prefixes:
            if line_l.startswith(p):
                return line[len(p):].strip()
        return None

    practice_prefixes = [
        "practice name:",
        "workplace name:",
        "clinic name:",
        "hospital name:",
        "company name:",
    ]
    for line in lines:
        line_l = line.lower()
        if is_disclaimer(line_l):
            continue
        val = strip_prefix(line, practice_prefixes)
        if val is not None and val.strip() and not is_disclaimer(val.lower()):
            return val.strip()

    org_prefixes = [
        "organization name:",
        "employer name:",
        "facility name:",
    ]
    for line in lines:
        line_l = line.lower()
        if is_disclaimer(line_l):
            continue
        val = strip_prefix(line, org_prefixes)
        if val is not None and val.strip() and not is_disclaimer(val.lower()):
            return val.strip()

    for line in lines:
        line_l = line.lower()
        if is_disclaimer(line_l):
            continue
        generic_val = strip_prefix(line, ["name:"])
        return (generic_val or line).strip()

    return ""


def process_single(row):
    npi = (row.get("npi_id") or "").strip()
    email = (row.get("email") or "").strip()
    name = (row.get("doctor_name") or "").strip()
    work_address = (
        row.get("work_address")
        or row.get("address")
        or row.get("work_addr")
        or ""
    ).strip()

    result = {
        "npi_id": npi,
        "email": email,
        "doctor_name": name,
        "work_address": work_address,
        "google_name": "N/A",
        "workplace_name": "N/A",
        "status": "NOT_STARTED",
    }

    if not npi or not email:
        result["status"] = "MISSING_INPUT"
        return result

    # Google with small retries
    g = {"found": False, "text": "", "error": None}
    for attempt in range(RETRY_COUNT + 1):
        g = google_grounded_search(npi, email, name, work_address)
        if g.get("found") or not g.get("error"):
            break
        time.sleep(0.5 * (attempt + 1))

    g_text = g.get("text") or ""
    g_name = _extract_workplace_name_from_text(g_text)
    result["google_name"] = g_name or "N/A"

    # Final workplace name: Google > N/A
    final_name = g_name or ""
    if not final_name:
        final_name = "N/A"
    result["workplace_name"] = final_name

    if final_name != "N/A":
        result["status"] = "FOUND"
    else:
        result["status"] = "NOT_FOUND"

    return result


def run_batches(rows):
    results_accum = []
    processed = load_processed_set()

    to_process = [
        r for r in rows if (r.get("npi_id") or "").strip() not in processed
    ]
    total = len(to_process)
    logger.info(
        f"Will process {total} rows (skipping {len(rows) - total} already processed)"
    )

    if total == 0:
        return results_accum

    overall_idx = 0
    big_batch_number = 0

    try:
        while overall_idx < total:
            big_start = overall_idx
            big_end = min(big_start + BIG_BATCH_SIZE, total)
            big_batch_number += 1

            # Ask user before starting each big batch after the first
            if big_batch_number > 1:
                this_big_size = big_end - big_start
                prompt = (
                    f"\nReady to process next {this_big_size} rows "
                    f"(rows {big_start + 1} to {big_end} of {total}). "
                    f"Proceed? [y/N]: "
                )
                ans = input(prompt).strip().lower()
                if ans not in ("y", "yes"):
                    logger.info(
                        f"User chose to stop before big batch {big_batch_number}. "
                        f"Processed {big_start} of {total} rows so far."
                    )
                    break

            logger.info(
                f"Starting big batch {big_batch_number}: "
                f"rows {big_start + 1} to {big_end} (size {big_end - big_start})"
            )

            # Inner loop: same 4-row threaded batches as before, limited to this big batch
            while overall_idx < big_end:
                batch = to_process[overall_idx: overall_idx + BATCH_SIZE]
                logger.info(
                    f"Processing micro-batch with {len(batch)} rows "
                    f"(overall rows {overall_idx + 1} to "
                    f"{min(overall_idx + len(batch), total)})"
                )

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=MAX_WORKERS
                ) as exe:
                    futures = {exe.submit(process_single, r): r for r in batch}
                    batch_results = []

                    for fut in concurrent.futures.as_completed(futures):
                        try:
                            res = fut.result()
                            batch_results.append(res)
                            pid = res.get("npi_id")
                            if pid:
                                processed.add(pid)
                            logger.info(
                                f"Processed NPI {res.get('npi_id')}: {res.get('status')}"
                            )
                        except Exception:
                            logger.exception("Row processing error")

                if batch_results:
                    append_results_csv(batch_results)
                    save_processed_set(processed)
                    results_accum.extend(batch_results)

                overall_idx += BATCH_SIZE

    except KeyboardInterrupt:
        logger.warning("Interrupted by user — flushing progress to disk.")
        save_processed_set(processed)
        if results_accum:
            append_results_csv(results_accum)
        raise

    return results_accum


def main():
    rows = read_input_csv(INPUT_CSV)
    logger.info(f"Loaded {len(rows)} input rows from {INPUT_CSV}")
    run_batches(rows)
    logger.info("Completed all batches")
    print(
        "Output latest file:",
        os.path.abspath(os.path.join(OUTPUT_DIR, "latest_results.csv")),
    )


if __name__ == "__main__":
    main()
