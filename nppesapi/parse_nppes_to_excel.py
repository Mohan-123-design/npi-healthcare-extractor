import os
import json
import pandas as pd
from datetime import datetime

# Default paths
DEFAULT_INPUT_EXCEL = "npi_results.xlsx"
DEFAULT_NPPES_DIR = "nppes_data/responses"


def get_user_confirmation():
    """Ask user to confirm or provide custom paths"""
    print("\n" + "="*60)
    print("NPPES DATA PARSER - Configuration")
    print("="*60)
    
    print(f"\n📂 Default Excel File: {DEFAULT_INPUT_EXCEL}")
    print(f"📂 Default NPPES Data Directory: {DEFAULT_NPPES_DIR}/")
    
    choice = input("\nUse default locations? (y/n): ").strip().lower()
    
    if choice == 'y':
        excel_file = DEFAULT_INPUT_EXCEL
        nppes_dir = DEFAULT_NPPES_DIR
    else:
        excel_file = input("Enter Excel file path: ").strip() or DEFAULT_INPUT_EXCEL
        nppes_dir = input("Enter NPPES data directory: ").strip() or DEFAULT_NPPES_DIR
    
    return excel_file, nppes_dir


def run():
    # Get user confirmation
    excel_file, nppes_dir = get_user_confirmation()
    
    # Check if files exist
    if not os.path.exists(excel_file):
        print(f"\n❌ Error: Excel file '{excel_file}' not found!")
        return
    
    if not os.path.exists(nppes_dir):
        print(f"\n❌ Error: NPPES directory '{nppes_dir}' not found!")
        return
    
    print(f"\n{'='*60}")
    print(f"✓ Excel File: {excel_file}")
    print(f"✓ NPPES Directory: {nppes_dir}/")
    print(f"{'='*60}\n")
    
    print("▶ Loading Excel file...")
    df = pd.read_excel(excel_file)

    # Ensure columns exist (matching your Excel structure)
    required_columns = {
        "Credentials": "",
        "Specialty": "",
        "Address": "",
        "City": "",
        "State": "",
        "Zip": ""
    }

    for col, default_val in required_columns.items():
        if col not in df.columns:
            df[col] = default_val

    print("▶ Parsing NPPES JSON files...\n")
    
    updated_count = 0
    missing_count = 0
    error_count = 0

    for idx, row in df.iterrows():
        npi = str(row.get("NPI", "")).strip()

        if not npi or npi == "NONE":
            continue

        json_file = f"{nppes_dir}/npi_{npi}.json"

        if not os.path.exists(json_file):
            print(f"⚠ Row {idx+2}: Missing JSON for NPI {npi}")
            missing_count += 1
            continue

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not data.get("results"):
                print(f"⚠ Row {idx+2}: No results in JSON for NPI {npi}")
                error_count += 1
                continue

            person = data["results"][0]
            basic = person.get("basic", {})

            # -------- Credential (with fallback) --------
            credential = basic.get("credential", "").strip()
            if not credential:
                credential = basic.get("authorized_official_credential", "").strip()

            # -------- Specialty (PRIMARY ONLY) --------
            specialty = ""
            for tax in person.get("taxonomies", []):
                if tax.get("primary") is True:
                    specialty = tax.get("desc", "")
                    break

            # -------- Address (LOCATION ONLY, SPLIT COLUMNS) --------
            address_1 = city = state = zip_code = ""

            for addr in person.get("addresses", []):
                if addr.get("address_purpose") == "LOCATION":
                    address_1 = addr.get("address_1", "")
                    city = addr.get("city", "")
                    state = addr.get("state", "")
                    zip_code = addr.get("postal_code", "")
                    break

            # Update Excel row (using correct column names)
            df.at[idx, "Credentials"] = credential
            df.at[idx, "Specialty"] = specialty
            df.at[idx, "Address"] = address_1
            df.at[idx, "City"] = city
            df.at[idx, "State"] = state
            df.at[idx, "Zip"] = zip_code

            print(f"✅ Row {idx+2}: Updated NPI {npi}")
            updated_count += 1

        except Exception as e:
            print(f"❌ Row {idx+2}: Error parsing NPI {npi} - {str(e)}")
            error_count += 1

    # Create backup before saving
    backup_file = excel_file.replace(".xlsx", f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    df_original = pd.read_excel(excel_file)
    df_original.to_excel(backup_file, index=False)
    print(f"\n💾 Backup created: {backup_file}")

    # Save updated Excel
    df.to_excel(excel_file, index=False)

    print("\n" + "="*60)
    print("✔ Excel enrichment completed!")
    print(f"  • Updated: {updated_count} rows")
    print(f"  • Missing: {missing_count} files")
    print(f"  • Errors: {error_count} rows")
    print("="*60)


if __name__ == "__main__":
    run()