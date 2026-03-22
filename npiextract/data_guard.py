# data_guard.py - Data Loss Prevention System

import os
import json
import csv
import shutil
import logging
import threading
from datetime import datetime
from config import PATHS

logger = logging.getLogger(__name__)


class DataGuard:
    """
    Multi-layer data loss prevention:
    
    Layer 1: Immediate JSON save after every URL
    Layer 2: Running CSV save (append mode)
    Layer 3: Periodic Excel save every N records
    Layer 4: Auto-backup of all output files
    Layer 5: Emergency dump on crash (via atexit)
    """

    def __init__(self, excel_manager=None):
        self.excel_manager = excel_manager
        self.running_results = []
        self.save_lock = threading.Lock()
        self._ensure_dirs()
        self._setup_emergency_handler()
        
        # Initialize running CSV
        self._init_csv()
        logger.info("DataGuard initialized - multi-layer protection active")

    def _ensure_dirs(self):
        for path in [PATHS["results_dir"], PATHS["checkpoints_dir"]]:
            os.makedirs(path, exist_ok=True)

    def _init_csv(self):
        """Initialize running CSV file with headers"""
        csv_path = PATHS["results_csv"]
        
        if not os.path.exists(csv_path):
            headers = [
                "S.No", "URL", "NPI_Number", "Extraction_Method",
                "Confidence", "Validation_Status", "Registry_Name",
                "Registry_Specialty", "Registry_State",
                "API_Used", "Fetch_Status", "Error", "Timestamp"
            ]
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            logger.info(f"Running CSV initialized: {csv_path}")

    def _setup_emergency_handler(self):
        """Setup emergency save on unexpected exit"""
        import atexit
        import signal
        
        atexit.register(self._emergency_save)
        
        # Handle Ctrl+C and system signals
        def signal_handler(sig, frame):
            logger.warning("Signal received - performing emergency save...")
            self._emergency_save()
            raise SystemExit(0)
        
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except:
            pass

    def save_result(self, result: dict, index: int):
        """
        Save a single result - called after EVERY URL
        Thread-safe multi-layer save
        """
        with self.save_lock:
            self.running_results.append(result)
            
            # Layer 1: Append to JSON
            self._append_json(result, index)
            
            # Layer 2: Append to CSV (always)
            self._append_csv(result, index)
            
            # Layer 3: Save Excel every record
            if self.excel_manager:
                self._save_excel_safely()
            
            # Layer 4: Backup every 10 records
            if index % 10 == 0:
                self._backup_results()

    def _append_json(self, result: dict, index: int):
        """Append result to running JSON file"""
        json_path = PATHS["results_json"]
        
        try:
            # Read existing
            existing = []
            if os.path.exists(json_path):
                with open(json_path, 'r', encoding='utf-8') as f:
                    try:
                        existing = json.load(f)
                    except:
                        existing = []
            
            # Append new result
            existing.append({**result, "record_index": index})
            
            # Atomic write
            temp = json_path + ".tmp"
            with open(temp, 'w', encoding='utf-8') as f:
                json.dump(existing, f, indent=2, ensure_ascii=False, default=str)
            os.replace(temp, json_path)
            
        except Exception as e:
            logger.error(f"JSON append failed: {e}")
            # Emergency fallback
            emergency_path = json_path + f".emergency_{index}.json"
            try:
                with open(emergency_path, 'w') as f:
                    json.dump(result, f, default=str)
            except:
                pass

    def _append_csv(self, result: dict, index: int):
        """Append single result to running CSV"""
        try:
            with open(PATHS["results_csv"], 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    index,
                    result.get("url", ""),
                    result.get("npi_found", "NOT FOUND"),
                    result.get("extraction_method", ""),
                    result.get("confidence", 0),
                    result.get("validation_status", ""),
                    result.get("registry_name", ""),
                    result.get("registry_specialty", ""),
                    result.get("registry_state", ""),
                    result.get("api_used", ""),
                    "SUCCESS" if result.get("fetch_success") else "FAILED",
                    result.get("error", ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ])
        except Exception as e:
            logger.error(f"CSV append failed: {e}")

    def _save_excel_safely(self):
        """Save Excel with error handling"""
        if not self.excel_manager or not self.running_results:
            return
        
        try:
            self.excel_manager.write_results(
                self.running_results,
                PATHS["results_excel"]
            )
        except Exception as e:
            logger.error(f"Excel save failed: {e}")
            # Create timestamped backup
            timestamp = datetime.now().strftime("%H%M%S")
            backup_path = PATHS["results_excel"].replace(
                '.xlsx', f'_backup_{timestamp}.xlsx'
            )
            try:
                self.excel_manager.write_results(
                    self.running_results, backup_path
                )
                logger.info(f"Excel backup saved: {backup_path}")
            except:
                pass

    def _backup_results(self):
        """Create timestamped backup of all result files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(PATHS["checkpoints_dir"], f"backup_{timestamp}")
        
        try:
            os.makedirs(backup_dir, exist_ok=True)
            
            for src in [PATHS["results_json"], PATHS["results_csv"]]:
                if os.path.exists(src):
                    dest = os.path.join(backup_dir, os.path.basename(src))
                    shutil.copy2(src, dest)
            
            # Keep only last 5 backups
            self._cleanup_old_backups()
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")

    def _cleanup_old_backups(self):
        """Keep only recent backups"""
        try:
            backup_dirs = sorted([
                d for d in os.listdir(PATHS["checkpoints_dir"])
                if d.startswith("backup_")
            ])
            
            # Remove old ones if more than 5
            while len(backup_dirs) > 5:
                old = os.path.join(PATHS["checkpoints_dir"], backup_dirs.pop(0))
                shutil.rmtree(old, ignore_errors=True)
        except:
            pass

    def _emergency_save(self):
        """Emergency save of all current data"""
        if not self.running_results:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Emergency JSON
        emergency_json = os.path.join(
            PATHS["results_dir"], f"EMERGENCY_SAVE_{timestamp}.json"
        )
        try:
            with open(emergency_json, 'w', encoding='utf-8') as f:
                json.dump(self.running_results, f, indent=2, default=str)
            logger.info(f"Emergency JSON saved: {emergency_json}")
        except Exception as e:
            logger.error(f"Emergency JSON save failed: {e}")

        # Emergency CSV
        emergency_csv = os.path.join(
            PATHS["results_dir"], f"EMERGENCY_SAVE_{timestamp}.csv"
        )
        try:
            with open(emergency_csv, 'w', newline='', encoding='utf-8') as f:
                if self.running_results:
                    writer = csv.DictWriter(
                        f, 
                        fieldnames=self.running_results[0].keys(),
                        extrasaction='ignore'
                    )
                    writer.writeheader()
                    writer.writerows(self.running_results)
            logger.info(f"Emergency CSV saved: {emergency_csv}")
        except:
            pass

        # Emergency Excel
        if self.excel_manager:
            emergency_xlsx = os.path.join(
                PATHS["results_dir"], f"EMERGENCY_SAVE_{timestamp}.xlsx"
            )
            try:
                self.excel_manager.write_results(
                    self.running_results, emergency_xlsx
                )
                logger.info(f"Emergency Excel saved: {emergency_xlsx}")
            except:
                pass

    def load_existing_results(self):
        """Load results from previous run"""
        json_path = PATHS["results_json"]
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.running_results = data
                logger.info(f"Loaded {len(data)} existing results")
                return data
            except:
                pass
        return []