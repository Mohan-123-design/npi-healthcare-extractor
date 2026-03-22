import os
import json
import time
import requests
import pandas as pd
from datetime import datetime

# Default paths (base directory)
DEFAULT_INPUT_EXCEL = "npi_results.xlsx"
DEFAULT_OUTPUT_DIR = "nppes_data"

DELAY = 1  # seconds (NPPES is public but be polite)


def get_user_confirmation():
    """Ask user to confirm or provide custom paths"""
    print("\n" + "="*60)
    print("NPPES DATA FETCHER - Configuration")
    print("="*60)
    
    print(f"\n📂 Default Input File: {DEFAULT_INPUT_EXCEL}")
    print(f"📂 Default Output Directory: {DEFAULT_OUTPUT_DIR}/")
    
    choice = input("\nUse default locations? (y/n): ").strip().lower()
    
    if choice == 'y':
        input_file = DEFAULT_INPUT_EXCEL
        output_dir = DEFAULT_OUTPUT_DIR
    else:
        input_file = input("Enter Excel file path: ").strip() or DEFAULT_INPUT_EXCEL
        output_dir = input("Enter output directory: ").strip() or DEFAULT_OUTPUT_DIR
    
    return input_file, output_dir


def get_batch_settings(total_rows):
    """Ask user for batch processing preferences"""
    print("\n" + "="*60)
    print("BATCH PROCESSING OPTIONS")
    print("="*60)
    print(f"\nTotal rows in Excel: {total_rows}")
    print(f"Data rows (excluding header): {total_rows - 1}")
    
    print("\nOptions:")
    print("  1. Process ALL rows")
    print("  2. Process SPECIFIC RANGE (e.g., rows 100-500)")
    print("  3. Process in BATCHES (e.g., 1000 rows at a time)")
    
    choice = input("\nSelect option (1/2/3): ").strip()
    
    if choice == "1":
        # Process all rows (skip header row 0)
        return 1, total_rows - 1, None
    
    elif choice == "2":
        # Specific range
        print("\n📌 Note: Row numbers include header (Row 1 = Header, Row 2 = First data row)")
        while True:
            try:
                start = input(f"Enter START row (2-{total_rows}): ").strip()
                end = input(f"Enter END row ({start}-{total_rows}): ").strip()
                
                start = int(start)
                end = int(end)
                
                # Convert Excel row numbers to DataFrame index (0-based)
                start_idx = start - 2  # Row 2 in Excel = index 0
                end_idx = end - 2      # Row 10 in Excel = index 8
                
                if start < 2 or end > total_rows or start > end:
                    print(f"❌ Invalid range! Must be between 2 and {total_rows}")
                    continue
                
                print(f"\n✓ Will process Excel rows {start} to {end} (DataFrame indices {start_idx} to {end_idx})")
                return start_idx, end_idx, None
                
            except ValueError:
                print("❌ Please enter valid numbers!")
    
    elif choice == "3":
        # Batch processing
        while True:
            try:
                batch_size = int(input("\nEnter batch size (e.g., 1000): ").strip())
                
                if batch_size < 1:
                    print("❌ Batch size must be at least 1")
                    continue
                
                start = input(f"Start from which row? (2-{total_rows}, press Enter for row 2): ").strip()
                start_idx = (int(start) - 2) if start else 0
                
                if start_idx < 0 or start_idx >= total_rows - 1:
                    print(f"❌ Start row must be between 2 and {total_rows}")
                    continue
                
                return start_idx, total_rows - 2, batch_size
                
            except ValueError:
                print("❌ Please enter valid numbers!")
    
    else:
        print("❌ Invalid option, defaulting to process all rows")
        return 1, total_rows - 1, None


def load_progress(progress_file):
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            data = json.load(f)
            return data.get("last_index", -1), data.get("batch_info", {})
    return -1, {}


def save_progress(progress_file, index, batch_info=None):
    data = {"last_index": index, "timestamp": datetime.now().isoformat()}
    if batch_info:
        data["batch_info"] = batch_info
    
    with open(progress_file, "w") as f:
        json.dump(data, f, indent=2)


def process_batch(df, start_idx, end_idx, responses_dir, progress_file, batch_num=None, total_batches=None):
    """Process a single batch of rows"""
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    batch_label = f"Batch {batch_num}/{total_batches}" if batch_num else "Processing"
    
    print(f"\n{'='*60}")
    print(f"🔄 {batch_label}")
    print(f"   Rows: {start_idx + 2} to {end_idx + 2} (Excel) | Indices: {start_idx} to {end_idx}")
    print(f"{'='*60}\n")
    
    last_done, _ = load_progress(progress_file)
    
    for idx in range(start_idx, end_idx + 1):
        
        if idx > len(df) - 1:
            break
        
        if idx <= last_done:
            continue
        
        row = df.iloc[idx]
        npi = str(row.get("NPI", "")).strip()
        api_url = str(row.get("nppes api", "")).strip()
        
        excel_row = idx + 2  # Convert to Excel row number
        progress = idx - start_idx + 1
        total = end_idx - start_idx + 1

        if not npi or not api_url or api_url == "NONE" or not api_url.startswith("http"):
            print(f"[{progress}/{total}] ⚠ Row {excel_row}: Skipping invalid NPI or API URL")
            skip_count += 1
            save_progress(progress_file, idx)
            continue

        print(f"[{progress}/{total}] 🔍 Row {excel_row}: Fetching NPI {npi}")

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            output_file = f"{responses_dir}/npi_{npi}.json"

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"            ✅ Saved successfully")
            success_count += 1

        except Exception as e:
            print(f"            ❌ Error: {str(e)}")
            error_count += 1

        batch_info = {
            "batch_num": batch_num,
            "total_batches": total_batches,
            "start_idx": start_idx,
            "end_idx": end_idx,
            "current_idx": idx
        }
        save_progress(progress_file, idx, batch_info)
        time.sleep(DELAY)
    
    return success_count, skip_count, error_count


def run():
    # Get user confirmation for paths
    input_excel, output_base = get_user_confirmation()
    
    # Setup directories
    responses_dir = f"{output_base}/responses"
    progress_file = f"{output_base}/progress.json"
    
    os.makedirs(responses_dir, exist_ok=True)
    
    # Check if input file exists
    if not os.path.exists(input_excel):
        print(f"\n❌ Error: Input file '{input_excel}' not found!")
        return
    
    # Load Excel
    print(f"\n⏳ Loading Excel file...")
    df = pd.read_excel(input_excel)
    
    # Validate required columns
    if "NPI" not in df.columns or "nppes api" not in df.columns:
        print("❌ Error: Excel must contain 'NPI' and 'nppes api' columns!")
        return
    
    total_rows = len(df)
    
    # Get batch settings
    start_idx, end_idx, batch_size = get_batch_settings(total_rows)
    
    print(f"\n{'='*60}")
    print(f"✓ Input File: {input_excel}")
    print(f"✓ Output Directory: {responses_dir}/")
    print(f"✓ Progress File: {progress_file}")
    print(f"{'='*60}\n")
    
    # Check for resume option
    last_done, batch_info = load_progress(progress_file)
    if last_done >= 0 and batch_info:
        print(f"📌 Previous progress detected:")
        print(f"   Last completed index: {last_done} (Excel row {last_done + 2})")
        if batch_info.get("batch_num"):
            print(f"   Last batch: {batch_info.get('batch_num')}/{batch_info.get('total_batches')}")
        
        resume = input("\nResume from last position? (y/n): ").strip().lower()
        if resume != 'y':
            # Clear progress to start fresh
            save_progress(progress_file, -1, None)
            print("✓ Starting fresh (progress cleared)")
    
    total_success = 0
    total_skip = 0
    total_error = 0
    
    start_time = datetime.now()
    
    # Process based on batch settings
    if batch_size:
        # Batch processing
        total_batches = ((end_idx - start_idx + 1) + batch_size - 1) // batch_size
        
        for batch_num in range(1, total_batches + 1):
            batch_start = start_idx + (batch_num - 1) * batch_size
            batch_end = min(batch_start + batch_size - 1, end_idx)
            
            success, skip, error = process_batch(
                df, batch_start, batch_end, responses_dir, progress_file,
                batch_num, total_batches
            )
            
            total_success += success
            total_skip += skip
            total_error += error
            
            print(f"\n📊 Batch {batch_num} Summary: ✅ {success} | ⚠ {skip} | ❌ {error}")
            
            if batch_num < total_batches:
                print(f"\n⏸ Batch {batch_num} complete. Continue to next batch? (y/n): ", end="")
                if input().strip().lower() != 'y':
                    print("\n🛑 Stopped by user. Progress saved. Run again to resume.")
                    return
    else:
        # Single range processing
        success, skip, error = process_batch(
            df, start_idx, end_idx, responses_dir, progress_file
        )
        total_success = success
        total_skip = skip
        total_error = error
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("✔ NPPES API FETCH COMPLETED!")
    print("="*60)
    print(f"📊 Final Summary:")
    print(f"   ✅ Successfully fetched: {total_success}")
    print(f"   ⚠  Skipped: {total_skip}")
    print(f"   ❌ Errors: {total_error}")
    print(f"   ⏱  Duration: {duration}")
    print(f"   📁 Output: {responses_dir}/")
    print("="*60)


if __name__ == "__main__":
    run()