import pandas as pd
import json
from pathlib import Path
from datetime import datetime

def calculate_shift_stats(df):
    """Calculate number of shifts and average shift duration per employee"""
    # Convert time columns to datetime
    df['t_start'] = pd.to_datetime(df['t_start'], format='%H:%M')
    df['t_end'] = pd.to_datetime(df['t_end'], format='%H:%M')
    
    # Calculate shift duration in hours
    df['shift_duration'] = (df['t_end'] - df['t_start']).dt.total_seconds() / 3600
    
    # Group by employee and calculate statistics
    stats = df.groupby('employee_number').agg({
        'shift_duration': ['count', 'mean']
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['employee_number', 'number_of_shifts', 'average_shift_hours']
    
    # Round average hours to 2 decimal places
    stats['average_shift_hours'] = stats['average_shift_hours'].round(2)
    
    return stats

def get_duplicate_info(df, id_column):
    """Get information about duplicate IDs"""
    duplicate_counts = df[id_column].value_counts()
    duplicates = duplicate_counts[duplicate_counts > 1]
    if not duplicates.empty:
        return {
            'count': len(duplicates),
            'ids': duplicates.index.astype(str).tolist(),
            'appearances': duplicates.iloc[0]
        }
    return None

def load_json_data(json_file):
    """Load data from JSON file"""
    with open(json_file, 'r', encoding='utf-8') as f:
        # Read the entire file as JSON
        data = json.load(f)
    
    # Extract the data array from the JSON
    data_array = data.get('data', [])
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data_array)
    print(f"\nJSON file contains {len(df)} total employees")
    
    # Store original data for summary
    original_count = len(df)
    duplicate_info = get_duplicate_info(df, 'id_number')
    
    # Check for empty IDs
    empty_ids = df['id_number'].isna() | (df['id_number'].astype(str).str.strip() == '')
    if empty_ids.any():
        print(f"Found {empty_ids.sum()} empty IDs in JSON file")
        print("Removing empty IDs...")
        df = df[~empty_ids]
        print(f"After removing empty IDs: {len(df)} rows")
    
    # Check for duplicate IDs
    duplicates = df['id_number'].duplicated(keep=False)
    if duplicates.any():
        duplicate_ids = df[duplicates]['id_number'].unique()
        print(f"Warning: Found {len(duplicate_ids)} duplicate IDs in JSON file")
        print("Duplicate IDs and their counts:")
        for id_num in duplicate_ids:
            count = (df['id_number'] == id_num).sum()
            print(f"  {id_num}: {count} times")
        # Keep only the first occurrence of each ID
        df = df.drop_duplicates(subset=['id_number'], keep='first')
        print(f"After removing duplicates: {len(df)} unique employees")
    
    return df, original_count, duplicate_info

def load_excel_data(excel_file):
    """Load data from Excel file with Hebrew support"""
    # Read Excel with encoding that supports Hebrew, skipping the first row
    df = pd.read_excel(excel_file, engine='openpyxl', skiprows=1)
    print(f"\nExcel file contains {len(df)} total rows")
    
    # Store original data for summary
    original_count = len(df)
    
    # Remove unnamed columns
    unnamed_cols = [col for col in df.columns if 'Unnamed' in col]
    if unnamed_cols:
        print(f"Removing {len(unnamed_cols)} unnamed columns")
        df = df.drop(columns=unnamed_cols)
    
    # The ID column is the first column (index 0)
    id_column = df.columns[0]
    print(f"Using first column '{id_column}' for IDs")
    
    duplicate_info = get_duplicate_info(df, id_column)
    
    # Only remove empty IDs
    empty_ids = df[id_column].isna() | (df[id_column].astype(str).str.strip() == '')
    if empty_ids.any():
        print(f"Found {empty_ids.sum()} empty IDs")
        print("Removing empty IDs...")
        df = df[~empty_ids]
    
    # Convert IDs to string and clean whitespace only
    df[id_column] = df[id_column].astype(str).str.strip()
    
    # Remove any remaining empty strings after cleaning
    df = df[df[id_column] != '']
    print(f"After removing empty IDs: {len(df)} rows")
    
    # Check for duplicate IDs
    duplicates = df[id_column].duplicated(keep=False)
    if duplicates.any():
        duplicate_ids = df[duplicates][id_column].unique()
        print(f"Warning: Found {len(duplicate_ids)} duplicate IDs in Excel file")
        print("Duplicate IDs and their counts:")
        for id_num in duplicate_ids:
            count = (df[id_column] == id_num).sum()
            print(f"  {id_num}: {count} times")
        # Keep only the first occurrence of each ID
        df = df.drop_duplicates(subset=[id_column], keep='first')
        print(f"After removing duplicates: {len(df)} unique employees")
    
    return df, id_column, original_count, duplicate_info

def compare_tables(json_df, excel_df, json_id_col='id_number', excel_id_col=None):
    """Compare two tables based on ID numbers"""
    if excel_df is None or excel_id_col is None:
        raise ValueError("Could not process Excel file correctly")
    
    # Convert ID columns to string type and clean whitespace only
    json_df[json_id_col] = json_df[json_id_col].astype(str).str.strip()
    excel_df[excel_id_col] = excel_df[excel_id_col].astype(str).str.strip()
    
    # Find matches
    matches = pd.merge(json_df, excel_df, 
                      left_on=json_id_col, 
                      right_on=excel_id_col,
                      how='inner')
    
    # Find IDs in JSON but not in Excel
    json_only = json_df[~json_df[json_id_col].isin(excel_df[excel_id_col])]
    
    # Find IDs in Excel but not in JSON
    excel_only = excel_df[~excel_df[excel_id_col].isin(json_df[json_id_col])]
    
    return {
        'matches': matches,
        'json_only': json_only,
        'excel_only': excel_only
    }

def main():
    # File paths
    json_file = 'alfon-api-response.txt'
    excel_file = '/Users/who/keshet_data/alfon-manual.xlsx'
    
    try:
        # Load data
        print("Loading data...")
        json_df, json_original_count, json_duplicate_info = load_json_data(json_file)
        excel_df, excel_id_col, excel_original_count, excel_duplicate_info = load_excel_data(excel_file)
        
        # Compare tables
        print("\nComparing tables...")
        results = compare_tables(json_df, excel_df, json_id_col='id_number', excel_id_col=excel_id_col)
        
        # Print results summary first
        print("\nComparison Results Summary:")
        print(f"Number of matching IDs: {len(results['matches'])}")
        print(f"Number of IDs only in JSON (API): {len(results['json_only'])}")
        print(f"Number of IDs only in Excel (Manual): {len(results['excel_only'])}")
        
        # Print detailed summary for both files
        print("\nJSON File Summary:")
        print(f"Total rows: {json_original_count}")
        print(f"Empty IDs: {len(json_df[json_df['id_number'].isna() | (json_df['id_number'].astype(str).str.strip() == '')])}")
        if json_duplicate_info:
            print(f"Duplicate IDs: {json_duplicate_info['count']} ({', '.join(json_duplicate_info['ids'])}) appears {json_duplicate_info['appearances']} times")
        else:
            print("Duplicate IDs: 0")
        print(f"Final unique IDs: {len(json_df)}")
        
        print("\nExcel File Summary:")
        print(f"Total rows: {excel_original_count}")
        print(f"Empty IDs: {len(excel_df[excel_df[excel_id_col].isna() | (excel_df[excel_id_col].astype(str).str.strip() == '')])}")
        if excel_duplicate_info:
            print(f"Duplicate IDs: {excel_duplicate_info['count']} ({', '.join(excel_duplicate_info['ids'])}) appears {excel_duplicate_info['appearances']} times")
        else:
            print("Duplicate IDs: 0")
        print(f"Final unique IDs: {len(excel_df)}")
        
        # Verify the numbers add up
        total_compared = len(results['matches']) + len(results['json_only']) + len(results['excel_only'])
        print(f"\nVerification:")
        print(f"Total unique IDs compared: {total_compared}")
        print(f"Total unique IDs in JSON: {len(json_df)}")
        print(f"Total unique IDs in Excel: {len(excel_df)}")
        print(f"Appear in both: {len(results['matches'])}")
        print(f"Only in JSON: {len(results['json_only'])}")
        print(f"Only in Excel: {len(results['excel_only'])}")
        
        # Convert DataFrames to dictionaries for JSON output
        output_data = {
            'summary': {
                'matching_ids_count': len(results['matches']),
                'json_only_count': len(results['json_only']),
                'excel_only_count': len(results['excel_only']),
                'total_json': len(json_df),
                'total_excel': len(excel_df),
                'empty_ids_json': len(json_df[json_df['id_number'].isna() | (json_df['id_number'].astype(str).str.strip() == '')]),
                'empty_ids_excel': len(excel_df[excel_df[excel_id_col].isna() | (excel_df[excel_id_col].astype(str).str.strip() == '')])
            },
            'matching_ids': results['matches'].to_dict(orient='records'),
            'json_only': results['json_only'].to_dict(orient='records'),
            'excel_only': results['excel_only'].to_dict(orient='records')
        }
        
        # Save results to JSON
        output_file = 'comparison_results.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
        
        print(f"\nDetailed results have been saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 