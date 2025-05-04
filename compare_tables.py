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

def load_attendance_data(attendance_file):
    """Load and process attendance data"""
    with open(attendance_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract the data array from the JSON
    data_array = data.get('data', [])
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data_array)
    
    # Function to convert time string to hours
    def time_to_hours(time_str):
        if pd.isna(time_str):
            return None
        try:
            hours, minutes = map(int, time_str.split(':'))
            return hours + minutes / 60
        except:
            return None
    
    # Convert time columns to hours
    df['t_start_hours'] = df['t_start'].apply(time_to_hours)
    df['t_end_hours'] = df['t_end'].apply(time_to_hours)
    
    # Calculate shift duration in hours
    df['shift_duration'] = df['t_end_hours'] - df['t_start_hours']
    
    # Group by employee and calculate statistics
    stats = df.groupby('employee_number').agg({
        'shift_duration': ['count', 'mean']
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['employee_number', 'number_of_shifts', 'average_shift_hours']
    
    # Round average hours to 2 decimal places
    stats['average_shift_hours'] = stats['average_shift_hours'].round(2)
    
    return stats

def main():
    # File paths
    json_file = 'alfon-api-response.txt'
    excel_file = '/Users/who/keshet_data/alfon-manual.xlsx'
    attendance_file = 'attendance-api-response.txt'
    
    try:
        # Load data
        print("Loading data...")
        json_df, json_original_count, json_duplicate_info = load_json_data(json_file)
        excel_df, excel_id_col, excel_original_count, excel_duplicate_info = load_excel_data(excel_file)
        
        # Load attendance data
        print("\nLoading attendance data...")
        attendance_stats = load_attendance_data(attendance_file)
        
        # Compare tables
        print("\nComparing tables...")
        results = compare_tables(json_df, excel_df, json_id_col='id_number', excel_id_col=excel_id_col)
        
        # Add attendance statistics to results
        for group in ['matches', 'json_only', 'excel_only']:
            df = results[group]
            if group == 'matches':
                # For matches, use employee_number
                df = df.merge(attendance_stats, 
                            left_on='employee_number', 
                            right_on='employee_number',
                            how='left')
            elif group == 'json_only':
                # For JSON only, use employee_number
                df = df.merge(attendance_stats, 
                            left_on='employee_number', 
                            right_on='employee_number',
                            how='left')
            else:  # excel_only
                # For Excel only, we can't merge with attendance data
                df['number_of_shifts'] = None
                df['average_shift_hours'] = None
            results[group] = df
        
        # Print results summary first
        print("\nComparison Results Summary:")
        print(f"Number of matching IDs: {len(results['matches'])}")
        print(f"Number of IDs only in JSON (API): {len(results['json_only'])}")
        print(f"Number of IDs only in Excel (Manual): {len(results['excel_only'])}")
        
        # Print detailed summary for both files
        print("\nJSON (API) File Summary:")
        print(f"Total rows: {json_original_count}")
        print(f"Empty IDs: {len(json_df[json_df['id_number'].isna() | (json_df['id_number'].astype(str).str.strip() == '')])}")
        if json_duplicate_info:
            print(f"Duplicate IDs: {json_duplicate_info['count']} ({', '.join(json_duplicate_info['ids'])}) appears {json_duplicate_info['appearances']} times")
        else:
            print("Duplicate IDs: 0")
        print(f"Final unique IDs: {len(json_df)}")
        
        print("\nExcel (Manual) File Summary:")
        print(f"Total rows: {excel_original_count}")
        print(f"Empty IDs: {len(excel_df[excel_df[excel_id_col].isna() | (excel_df[excel_id_col].astype(str).str.strip() == '')])}")
        if excel_duplicate_info:
            print(f"Duplicate IDs: {excel_duplicate_info['count']} ({', '.join(excel_duplicate_info['ids'])}) appears {excel_duplicate_info['appearances']} times")
        else:
            print("Duplicate IDs: 0")
        print(f"Final unique IDs: {len(excel_df)}")
        
        # Print attendance statistics for each group
        print("\nAttendance Statistics:")
        attendance_summary = {}
        for group_name, group_df in results.items():
            # Format group name for display
            display_name = {
                'matches': 'Matches (Appear in both sources)',
                'json_only': 'JSON (API) Only',
                'excel_only': 'Excel (Manual) Only'
            }[group_name]
            
            print(f"\n{display_name}:")
            group_stats = {}
            if 'number_of_shifts' in group_df.columns:
                total_shifts = group_df['number_of_shifts'].sum()
                avg_shifts = group_df['number_of_shifts'].mean()
                avg_hours = group_df['average_shift_hours'].mean()
                print(f"Total shifts: {total_shifts}")
                print(f"Average shifts per employee: {avg_shifts:.2f}")
                print(f"Average hours per shift: {avg_hours:.2f}")
                group_stats.update({
                    'total_shifts': float(total_shifts) if pd.notna(total_shifts) else 0,
                    'avg_shifts_per_employee': float(avg_shifts) if pd.notna(avg_shifts) else 0,
                    'avg_hours_per_shift': float(avg_hours) if pd.notna(avg_hours) else 0
                })
            
            # Count employees with bank account details
            if 'bank_account' in group_df.columns:
                # Check if bank account details are actually filled (not empty string)
                bank_account_count = group_df[
                    (group_df['bank_account'].notna()) & 
                    (group_df['bank_account'].astype(str).str.strip() != '')
                ].shape[0]
                total_employees = len(group_df)
                print(f"Employees with bank account details: {bank_account_count} out of {total_employees}")
                group_stats['employees_with_bank_account'] = int(bank_account_count)
                group_stats['total_employees'] = int(total_employees)
            
            # Count employees with at least one shift
            if 'number_of_shifts' in group_df.columns:
                total_employees = len(group_df)  # Get total employees for current group
                employees_with_shifts = group_df[group_df['number_of_shifts'].notna() & (group_df['number_of_shifts'] > 0)].shape[0]
                print(f"Employees with at least one shift: {employees_with_shifts} out of {total_employees}")
                group_stats['employees_with_shifts'] = int(employees_with_shifts)
            else:
                total_employees = len(group_df)  # Get total employees for current group
                print(f"Employees with at least one shift: 0 out of {total_employees}")
                group_stats['employees_with_shifts'] = 0
            
            attendance_summary[group_name] = group_stats
        
        # Verify the numbers add up
        total_compared = len(results['matches']) + len(results['json_only']) + len(results['excel_only'])
        print(f"\nVerification:")
        print(f"Total unique IDs compared: {total_compared}")
        print(f"Total unique IDs in JSON (API): {len(json_df)}")
        print(f"Total unique IDs in Excel (Manual): {len(excel_df)}")
        print(f"Appear in both: {len(results['matches'])}")
        print(f"Only in JSON (API): {len(results['json_only'])}")
        print(f"Only in Excel (Manual): {len(results['excel_only'])}")
        
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
            'attendance_summary': attendance_summary,
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
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 