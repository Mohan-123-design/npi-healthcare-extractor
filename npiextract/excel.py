import pandas as pd
import re
import os
from datetime import datetime

class URLTransformer:
    """Class to handle URL transformation from Excel files"""
    
    def __init__(self, input_file, output_file=None):
        """
        Initialize the transformer
        
        Args:
            input_file (str): Path to input Excel file
            output_file (str): Path to output Excel file (optional)
        """
        self.input_file = input_file
        self.output_file = output_file or f"transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def validate_file(self):
        """Check if input file exists"""
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"❌ File not found: {self.input_file}")
        print(f"✅ File found: {self.input_file}")
        
    def transform_url(self, url):
        """
        Transform individual URL
        
        Args:
            url (str): Original URL
            
        Returns:
            str: Transformed URL
        """
        if pd.isna(url):  # Handle empty cells
            return ""
        
        # Convert to string
        url = str(url).strip()
        
        # Pattern: extract base URL with numeric ID
        pattern = r'(https://physicians\.umassmemorial\.org/details/\d+)'
        match = re.search(pattern, url)
        
        if match:
            return match.group(1) + '/@url'
        else:
            print(f"⚠️  Warning: Could not transform: {url[:50]}...")
            return url
    
    def process(self, column_name=None, column_index=0):
        """
        Process the Excel file
        
        Args:
            column_name (str): Name of column containing URLs
            column_index (int): Index of column (0-based) if name not provided
        """
        print("\n🔄 Starting URL transformation...")
        
        # Validate file
        self.validate_file()
        
        # Read Excel
        print("📖 Reading Excel file...")
        df = pd.read_excel(self.input_file)
        
        print(f"📊 Found {len(df)} rows")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Determine which column to use
        if column_name and column_name in df.columns:
            url_col = column_name
        else:
            url_col = df.columns[column_index]
        
        print(f"🎯 Processing column: '{url_col}'")
        
        # Transform URLs
        print("⚙️  Transforming URLs...")
        df['Transformed_URL'] = df[url_col].apply(self.transform_url)
        
        # Add statistics
        successful = df['Transformed_URL'].str.endswith('/@url').sum()
        
        # Save output
        print(f"💾 Saving to: {self.output_file}")
        df.to_excel(self.output_file, index=False)
        
        # Print results
        print("\n" + "="*50)
        print("✅ TRANSFORMATION COMPLETE!")
        print("="*50)
        print(f"📁 Input file:  {self.input_file}")
        print(f"📁 Output file: {self.output_file}")
        print(f"📊 Total rows:  {len(df)}")
        print(f"✅ Successful:  {successful}")
        print(f"⚠️  Warnings:    {len(df) - successful}")
        print("="*50)
        
        return df

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = 'input_urls.xlsx'      # ⬅️ CHANGE THIS
    OUTPUT_FILE = 'transformed_urls.xlsx'  # ⬅️ CHANGE THIS (optional)
    
    # Option 1: Specify column by name
    COLUMN_NAME = 'URL'  # ⬅️ CHANGE THIS if your column has a specific name
    
    # Option 2: Or use column index (0 = first column, 1 = second, etc.)
    COLUMN_INDEX = 0
    
    try:
        # Create transformer instance
        transformer = URLTransformer(INPUT_FILE, OUTPUT_FILE)
        
        # Process the file
        # Use column_name OR column_index
        result_df = transformer.process(column_name=None, column_index=COLUMN_INDEX)
        
        # Optional: Preview results
        print("\n📋 Preview of results:")
        print(result_df[['Transformed_URL']].head(10))
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()