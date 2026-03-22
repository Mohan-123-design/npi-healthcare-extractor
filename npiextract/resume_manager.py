# resume_manager.py - Resume + Checkpoint Logic

import os
import json
import logging
import hashlib
from datetime import datetime
from config import PATHS, SCRAPING_CONFIG

logger = logging.getLogger(__name__)


class ResumeManager:
    """
    Handles resume logic and checkpointing.
    
    Every URL processed = checkpoint saved immediately.
    If script crashes/stops → resume from exact last position.
    
    Checkpoint structure:
    {
        "session_id": "unique_session_id",
        "api_choice": "scrapingant",
        "started_at": "2024-01-01T10:00:00",
        "last_updated": "2024-01-01T10:05:00",
        "total_urls": 100,
        "processed_urls": {
            "https://url1...": {
                "status": "completed",
                "npi_found": "1234567890",
                "processed_at": "..."
            },
            ...
        },
        "failed_urls": ["url1", "url2"],
        "completed_count": 45,
        "results": [...]
    }
    """

    def __init__(self):
        os.makedirs(PATHS["checkpoints_dir"], exist_ok=True)
        self.checkpoint_file = PATHS["master_checkpoint"]
        self.progress_file = PATHS["progress_file"]
        self.checkpoint = None

    def has_existing_session(self) -> bool:
        """Check if a previous unfinished session exists"""
        if not os.path.exists(self.checkpoint_file):
            return False
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            
            # Check if session was completed
            if data.get("completed"):
                return False
            
            # Check if there are unprocessed URLs
            total = data.get("total_urls", 0)
            done = data.get("completed_count", 0)
            return total > 0 and done < total
            
        except:
            return False

    def get_session_info(self) -> dict:
        """Get info about existing session for user display"""
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            
            return {
                "session_id": data.get("session_id", ""),
                "api_used": data.get("api_choice", ""),
                "started_at": data.get("started_at", ""),
                "last_updated": data.get("last_updated", ""),
                "total_urls": data.get("total_urls", 0),
                "completed_count": data.get("completed_count", 0),
                "failed_count": len(data.get("failed_urls", [])),
                "npi_found_count": sum(
                    1 for r in data.get("results", [])
                    if r.get("npi_found")
                ),
                "remaining": (
                    data.get("total_urls", 0) - data.get("completed_count", 0)
                ),
            }
        except:
            return {}

    def start_new_session(self, urls: list, api_choice: str) -> str:
        """Start a fresh session"""
        session_id = hashlib.md5(
            f"{datetime.now().isoformat()}{len(urls)}".encode()
        ).hexdigest()[:12]
        
        self.checkpoint = {
            "session_id": session_id,
            "api_choice": api_choice,
            "started_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_urls": len(urls),
            "all_urls": urls,
            "processed_urls": {},
            "failed_urls": [],
            "results": [],
            "completed_count": 0,
            "completed": False,
        }
        
        self._save_checkpoint()
        logger.info(f"New session started: {session_id}")
        return session_id

    def resume_session(self) -> dict:
        """Load existing session for resume"""
        try:
            with open(self.checkpoint_file, 'r') as f:
                self.checkpoint = json.load(f)
            logger.info(
                f"Session resumed: {self.checkpoint['session_id']} | "
                f"Done: {self.checkpoint['completed_count']}/{self.checkpoint['total_urls']}"
            )
            return self.checkpoint
        except Exception as e:
            logger.error(f"Failed to resume session: {e}")
            return {}

    def get_pending_urls(self) -> list:
        """Get list of URLs not yet processed"""
        if not self.checkpoint:
            return []
        
        all_urls = self.checkpoint.get("all_urls", [])
        processed = set(self.checkpoint.get("processed_urls", {}).keys())
        
        # Return URLs in original order, skipping processed
        pending = [url for url in all_urls if url not in processed]
        logger.info(f"Pending URLs: {len(pending)} / Total: {len(all_urls)}")
        return pending

    def get_completed_results(self) -> list:
        """Get all results from current/resumed session"""
        if not self.checkpoint:
            return []
        return self.checkpoint.get("results", [])

    def mark_completed(self, url: str, result: dict):
        """
        Mark a URL as processed - SAVES IMMEDIATELY
        This is called after every single URL
        """
        if not self.checkpoint:
            logger.error("No active checkpoint - cannot mark completed")
            return

        # Update checkpoint data
        self.checkpoint["processed_urls"][url] = {
            "status": "completed",
            "npi_found": result.get("npi_found"),
            "extraction_method": result.get("extraction_method"),
            "processed_at": datetime.now().isoformat(),
        }
        
        # Add to results list
        self.checkpoint["results"].append(result)
        self.checkpoint["completed_count"] += 1
        self.checkpoint["last_updated"] = datetime.now().isoformat()
        
        # Save immediately - no data loss
        self._save_checkpoint()
        
        logger.debug(
            f"Checkpoint saved: {self.checkpoint['completed_count']}/"
            f"{self.checkpoint['total_urls']} | NPI: {result.get('npi_found', 'NOT FOUND')}"
        )

    def mark_failed(self, url: str, error: str):
        """Mark URL as failed - saves immediately"""
        if not self.checkpoint:
            return

        self.checkpoint["processed_urls"][url] = {
            "status": "failed",
            "error": error,
            "processed_at": datetime.now().isoformat(),
        }
        self.checkpoint["failed_urls"].append(url)
        self.checkpoint["completed_count"] += 1
        self.checkpoint["last_updated"] = datetime.now().isoformat()
        
        self._save_checkpoint()
        logger.debug(f"Failed URL saved to checkpoint: {url[:60]}")

    def mark_session_complete(self):
        """Mark entire session as done"""
        if self.checkpoint:
            self.checkpoint["completed"] = True
            self.checkpoint["completed_at"] = datetime.now().isoformat()
            self._save_checkpoint()
            
            # Archive checkpoint
            archive_name = (
                f"completed_{self.checkpoint['session_id']}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            archive_path = os.path.join(PATHS["checkpoints_dir"], archive_name)
            try:
                import shutil
                shutil.copy2(self.checkpoint_file, archive_path)
                logger.info(f"Session archived: {archive_path}")
            except:
                pass

    def _save_checkpoint(self):
        """Save checkpoint to file with backup"""
        # Save to temp file first (atomic write pattern)
        temp_file = self.checkpoint_file + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint, f, indent=2, ensure_ascii=False, default=str)
            
            # Atomic replace
            os.replace(temp_file, self.checkpoint_file)
            
            # Update progress file (lightweight summary)
            self._save_progress()
            
        except Exception as e:
            logger.error(f"Checkpoint save failed: {e}")
            # Try backup location
            backup = self.checkpoint_file + ".backup"
            try:
                with open(backup, 'w') as f:
                    json.dump(self.checkpoint, f, default=str)
                logger.info(f"Backup checkpoint saved: {backup}")
            except:
                pass

    def _save_progress(self):
        """Save lightweight progress summary"""
        if not self.checkpoint:
            return
        
        progress = {
            "session_id": self.checkpoint.get("session_id"),
            "total": self.checkpoint.get("total_urls", 0),
            "completed": self.checkpoint.get("completed_count", 0),
            "percent": (
                f"{(self.checkpoint.get('completed_count', 0) / max(self.checkpoint.get('total_urls', 1), 1) * 100):.1f}%"
            ),
            "npi_found": sum(
                1 for r in self.checkpoint.get("results", [])
                if r.get("npi_found")
            ),
            "last_updated": self.checkpoint.get("last_updated"),
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except:
            pass