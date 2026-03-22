"""
NPI EXTRACTOR - PRODUCTION VERSION
- Resume capability
- Data loss prevention
- Automatic backups
- Progress tracking
- Error handling
- Validation
"""

import pandas as pd
import re
import os
import shutil
from datetime import datetime
from pathlib import Path
import json
from tqdm import tqdm
from colorama import init, Fore, Style

# Initialize colorama for colored output
init(autoreset=True)

class NPIExtractor:
    def __init__(self, input_file, url_column='URL', output_file=None):
        """
        Initialize NPI Extractor
        
        Args:
            input_file: Path to input Excel file
            url_column: Name of column containing URLs
            output_file: Path to output file (optional)
        """
        self.input_file = input_file
        self.url_column = url_column
        self.output_file = output_file or f"NPI_Extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Progress tracking
        self.progress_file = 'npi_extraction_progress.json'
        self.backup_folder = 'npi_backups'
        
        # Statistics
        self.stats = {
            'total_rows': 0,
            'processed': 0,
            'extracted': 0,
            'failed': 0,
            'skipped': 0
        }
        
        # Create backup folder
        Path(self.backup_folder).mkdir(exist_ok=True)
        
    def print_header(self):
        """Print fancy header"""
        print("\n" + "="*70)
        print(Fore.CYAN + Style.BRIGHT + "🔍 NPI NUMBER EXTRACTOR - PRODUCTION VERSION 🔍".center(70))
        print("="*70 + "\n")
        
    def create_backup(self):
        """Create backup of input file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self.backup_folder, f"backup_{timestamp}.xlsx")
            shutil.copy2(self.input_file, backup_file)
            print(Fore.GREEN + f"✓ Backup created: {backup_file}")
            return backup_file
        except Exception as e:
            print(Fore.YELLOW + f"⚠ Backup creation failed: {e}")
            return None
            
    def save_progress(self, current_index):
        """Save progress for resume capability"""
        progress_data = {
            'last_processed_index': current_index,
            'timestamp': datetime.now().isoformat(),
            'input_file': self.input_file,
            'stats': self.stats
        }
        
        with open(self.progress_file, 'w') as f:
            json.dump(progress_data, f, indent=4)
            
    def load_progress(self):
        """Load previous progress"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    
                if progress_data['input_file'] == self.input_file:
                    print(Fore.YELLOW + f"\n⚡ PREVIOUS SESSION FOUND!")
                    print(f"Last processed: Row {progress_data['last_processed_index']}")
                    print(f"Timestamp: {progress_data['timestamp']}")
                    
                    response = input(Fore.CYAN + "\nDo you want to RESUME from last position? (y/n): ").strip().lower()
                    
                    if response == 'y':
                        self.stats = progress_data.get('stats', self.stats)
                        return progress_data['last_processed_index']
            except Exception as e:
                print(Fore.RED + f"⚠ Could not load progress: {e}")
        
        return 0
        
    def extract_npi(self, url):
        """
        Extract 10-digit NPI number from URL
        
        Args:
            url: URL string
            
        Returns:
            tuple: (npi_number, status, method)
        """
        if pd.isna(url) or url == '':
            return None, 'EMPTY', 'NA'
            
        url_str = str(url).strip()
        
        # Method 1: Exact 10-digit pattern (most common)
        pattern1 = r'\b(\d{10})\b'
        match = re.search(pattern1, url_str)
        if match:
            npi = match.group(1)
            if self.validate_npi(npi):
                return npi, 'SUCCESS', 'EXACT_MATCH'
        
        # Method 2: NPI parameter in URL
        pattern2 = r'[?&]npi[=:](\d{10})\b'
        match = re.search(pattern2, url_str, re.IGNORECASE)
        if match:
            npi = match.group(1)
            if self.validate_npi(npi):
                return npi, 'SUCCESS', 'PARAMETER_MATCH'
        
        # Method 3: After 'npi/' or 'npi-'
        pattern3 = r'npi[/-](\d{10})\b'
        match = re.search(pattern3, url_str, re.IGNORECASE)
        if match:
            npi = match.group(1)
            if self.validate_npi(npi):
                return npi, 'SUCCESS', 'PATH_MATCH'
        
        # Method 4: Any 10-digit sequence
        pattern4 = r'(\d{10})'
        match = re.search(pattern4, url_str)
        if match:
            npi = match.group(1)
            if self.validate_npi(npi):
                return npi, 'WARNING', 'GENERIC_MATCH'
        
        return None, 'NOT_FOUND', 'NO_MATCH'
        
    def validate_npi(self, npi):
        """
        Validate NPI number
        - Must be exactly 10 digits
        - Should not be all same digits (1111111111)
        - Should not be sequential (1234567890)
        """
        if not npi or len(npi) != 10:
            return False
            
        # Check if all same digit
        if len(set(npi)) == 1:
            return False
            
        # Check if sequential
        if npi in ['0123456789', '1234567890', '9876543210', '0987654321']:
            return False
            
        return True
        
    def process_excel(self):
        """Main processing function"""
        
        self.print_header()
        
        # Step 1: Verify input file
        print(Fore.CYAN + "📂 STEP 1: Verifying Input File...")
        if not os.path.exists(self.input_file):
            print(Fore.RED + f"✗ Error: File '{self.input_file}' not found!")
            return None
            
        print(Fore.GREEN + f"✓ Input file found: {self.input_file}")
        
        # Step 2: Create backup
        print(Fore.CYAN + "\n💾 STEP 2: Creating Backup...")
        self.create_backup()
        
        # Step 3: Load Excel
        print(Fore.CYAN + "\n📊 STEP 3: Loading Excel Data...")
        try:
            df = pd.read_excel(self.input_file)
            print(Fore.GREEN + f"✓ Loaded {len(df)} rows")
        except Exception as e:
            print(Fore.RED + f"✗ Error loading Excel: {e}")
            return None
            
        # Step 4: Verify column
        print(Fore.CYAN + "\n🔍 STEP 4: Verifying URL Column...")
        if self.url_column not in df.columns:
            print(Fore.RED + f"✗ Column '{self.url_column}' not found!")
            print(Fore.YELLOW + f"Available columns: {', '.join(df.columns)}")
            
            # Auto-detect URL column
            url_columns = [col for col in df.columns if 'url' in col.lower() or 'link' in col.lower()]
            if url_columns:
                print(Fore.CYAN + f"\nFound possible URL columns: {', '.join(url_columns)}")
                self.url_column = url_columns[0]
                print(Fore.GREEN + f"✓ Auto-selected: {self.url_column}")
            else:
                return None
        else:
            print(Fore.GREEN + f"✓ Column '{self.url_column}' found")
            
        # Step 5: Check for resume
        print(Fore.CYAN + "\n⚡ STEP 5: Checking Resume Status...")
        start_index = self.load_progress()
        
        # Initialize NPI columns if not exists
        if 'NPI_Number' not in df.columns:
            df['NPI_Number'] = None
        if 'Extraction_Status' not in df.columns:
            df['Extraction_Status'] = None
        if 'Extraction_Method' not in df.columns:
            df['Extraction_Method'] = None
            
        # Step 6: Process URLs
        print(Fore.CYAN + "\n🔄 STEP 6: Extracting NPI Numbers...")
        print(Fore.YELLOW + f"Starting from row: {start_index + 1}")
        
        self.stats['total_rows'] = len(df)
        
        # Progress bar
        with tqdm(total=len(df) - start_index, 
                  desc="Processing", 
                  unit="row",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            
            for idx in range(start_index, len(df)):
                try:
                    url = df.loc[idx, self.url_column]
                    
                    # Extract NPI
                    npi, status, method = self.extract_npi(url)
                    
                    # Update dataframe
                    df.loc[idx, 'NPI_Number'] = npi
                    df.loc[idx, 'Extraction_Status'] = status
                    df.loc[idx, 'Extraction_Method'] = method
                    
                    # Update statistics
                    self.stats['processed'] += 1
                    if status == 'SUCCESS' or status == 'WARNING':
                        self.stats['extracted'] += 1
                    elif status == 'NOT_FOUND':
                        self.stats['failed'] += 1
                    elif status == 'EMPTY':
                        self.stats['skipped'] += 1
                    
                    # Save progress every 100 rows
                    if (idx + 1) % 100 == 0:
                        self.save_progress(idx)
                        # Incremental save
                        df.to_excel(self.output_file, index=False)
                    
                    pbar.update(1)
                    
                except KeyboardInterrupt:
                    print(Fore.YELLOW + "\n\n⚠ Process interrupted by user!")
                    print(Fore.CYAN + "Saving progress...")
                    self.save_progress(idx)
                    df.to_excel(self.output_file, index=False)
                    print(Fore.GREEN + f"✓ Progress saved. You can resume later.")
                    return df
                    
                except Exception as e:
                    print(Fore.RED + f"\n✗ Error at row {idx + 1}: {e}")
                    df.loc[idx, 'Extraction_Status'] = f'ERROR: {str(e)[:50]}'
                    self.stats['failed'] += 1
                    continue
        
        # Step 7: Save final output
        print(Fore.CYAN + "\n💾 STEP 7: Saving Final Output...")
        try:
            df.to_excel(self.output_file, index=False)
            print(Fore.GREEN + f"✓ Output saved: {self.output_file}")
        except Exception as e:
            print(Fore.RED + f"✗ Error saving output: {e}")
            # Try alternative format
            csv_output = self.output_file.replace('.xlsx', '.csv')
            df.to_csv(csv_output, index=False)
            print(Fore.YELLOW + f"⚠ Saved as CSV instead: {csv_output}")
            
        # Step 8: Clean up progress file
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
            
        # Step 9: Display results
        self.display_results(df)
        
        return df
        
    def display_results(self, df):
        """Display extraction results"""
        print("\n" + "="*70)
        print(Fore.GREEN + Style.BRIGHT + "✓ EXTRACTION COMPLETE!".center(70))
        print("="*70 + "\n")
        
        print(Fore.CYAN + "📊 STATISTICS:")
        print(f"   Total Rows:        {self.stats['total_rows']}")
        print(f"   Processed:         {self.stats['processed']}")
        print(Fore.GREEN + f"   ✓ Extracted:       {self.stats['extracted']}")
        print(Fore.RED + f"   ✗ Not Found:       {self.stats['failed']}")
        print(Fore.YELLOW + f"   ⊝ Empty/Skipped:   {self.stats['skipped']}")
        
        if self.stats['processed'] > 0:
            success_rate = (self.stats['extracted'] / self.stats['processed']) * 100
            print(Fore.CYAN + f"\n   Success Rate:      {success_rate:.2f}%")
        
        print(Fore.CYAN + f"\n📁 OUTPUT FILE:")
        print(f"   {os.path.abspath(self.output_file)}")
        
        # Show sample results
        print(Fore.CYAN + "\n📋 SAMPLE RESULTS (First 5 rows):")
        print("-" * 70)
        
        sample_cols = [self.url_column, 'NPI_Number', 'Extraction_Status']
        sample_df = df[sample_cols].head(5)
        
        for idx, row in sample_df.iterrows():
            url = str(row[self.url_column])[:50] + "..." if len(str(row[self.url_column])) > 50 else str(row[self.url_column])
            npi = row['NPI_Number'] if pd.notna(row['NPI_Number']) else 'N/A'
            status = row['Extraction_Status']
            
            status_color = Fore.GREEN if status == 'SUCCESS' else Fore.YELLOW if status == 'WARNING' else Fore.RED
            
            print(f"{idx + 1}. URL: {url}")
            print(f"   NPI: {npi}")
            print(status_color + f"   Status: {status}\n")
        
        print("="*70 + "\n")
        
        # Export statistics
        stats_file = self.output_file.replace('.xlsx', '_stats.txt')
        with open(stats_file, 'w') as f:
            f.write("NPI EXTRACTION STATISTICS\n")
            f.write("="*50 + "\n\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input File: {self.input_file}\n")
            f.write(f"Output File: {self.output_file}\n\n")
            f.write(f"Total Rows: {self.stats['total_rows']}\n")
            f.write(f"Processed: {self.stats['processed']}\n")
            f.write(f"Extracted: {self.stats['extracted']}\n")
            f.write(f"Not Found: {self.stats['failed']}\n")
            f.write(f"Skipped: {self.stats['skipped']}\n")
            if self.stats['processed'] > 0:
                f.write(f"Success Rate: {(self.stats['extracted'] / self.stats['processed']) * 100:.2f}%\n")
        
        print(Fore.GREEN + f"✓ Statistics saved: {stats_file}\n")


def main():
    """Main execution function"""
    
    print(Fore.CYAN + Style.BRIGHT + """
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║           NPI EXTRACTOR - PRODUCTION VERSION              ║
    ║                                                           ║
    ║  Features:                                                ║
    ║  ✓ Resume capability                                      ║
    ║  ✓ Automatic backups                                      ║
    ║  ✓ Data loss prevention                                   ║
    ║  ✓ Progress tracking                                      ║
    ║  ✓ Multiple extraction methods                            ║
    ║  ✓ Validation & error handling                            ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Get input file
    print(Fore.YELLOW + "Please provide the input Excel file name.")
    print(Fore.CYAN + "Examples: input.xlsx, data.xlsx, C:\\Users\\Name\\Desktop\\file.xlsx\n")
    
    input_file = input(Fore.WHITE + "Enter input file path: ").strip().strip('"').strip("'")
    
    if not input_file:
        input_file = 'withinurls.xlsx'  # Default
        print(Fore.YELLOW + f"Using default: {input_file}")
    
    # Get URL column name
    print(Fore.CYAN + "\nEnter the column name containing URLs.")
    print(Fore.YELLOW + "(Press Enter to use 'URL' or auto-detect)\n")
    
    url_column = input(Fore.WHITE + "URL Column Name: ").strip()
    
    if not url_column:
        url_column = 'URL'  # Default
        print(Fore.YELLOW + f"Using default: {url_column}")
    
    # Get output file (optional)
    print(Fore.CYAN + "\nEnter output file name (optional).")
    print(Fore.YELLOW + "(Press Enter for auto-generated name)\n")
    
    output_file = input(Fore.WHITE + "Output file name: ").strip()
    
    if not output_file:
        output_file = None
        print(Fore.YELLOW + "Will use auto-generated name")
    
    # Confirm and start
    print(Fore.CYAN + "\n" + "="*60)
    print(Fore.WHITE + "CONFIGURATION:")
    print(f"  Input File:  {input_file}")
    print(f"  URL Column:  {url_column}")
    print(f"  Output File: {output_file or 'Auto-generated'}")
    print("="*60 + "\n")
    
    input(Fore.GREEN + "Press ENTER to start extraction...")
    
    # Create extractor and process
    extractor = NPIExtractor(
        input_file=input_file,
        url_column=url_column,
        output_file=output_file
    )
    
    result = extractor.process_excel()
    
    if result is not None:
        print(Fore.GREEN + Style.BRIGHT + "\n🎉 ALL DONE! 🎉\n")
    else:
        print(Fore.RED + "\n❌ Extraction failed. Please check errors above.\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n\n⚠ Program terminated by user.")
    except Exception as e:
        print(Fore.RED + f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        input(Fore.CYAN + "\nPress ENTER to exit...")