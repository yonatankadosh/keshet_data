# Keshet Data Comparison Tool

This tool compares employee data from different sources (API and manual Excel files) and provides detailed statistics about matches, attendance, and bank account details.

## Features

- Compares employee data between API (JSON) and manual (Excel) sources
- Identifies matching and unique records in each source
- Calculates attendance statistics (number of shifts, average shift duration)
- Tracks bank account details completeness
- Generates detailed comparison reports

## Requirements

- Python 3.x
- Required Python packages:
  - pandas
  - openpyxl

## Input Files

1. `alfon-api-response.txt` - API response containing employee data
2. `alfon-manual.xlsx` - Manual Excel file with employee data
3. `attendance-api-response.txt` - API response containing attendance data

## Output

The script generates:
1. Console output with detailed comparison statistics
2. `comparison_results.json` file containing:
   - Summary of matches and unique records
   - Attendance statistics for each group
   - Detailed records for matching and unique employees

## Usage

1. Place the input files in the same directory as the script
2. Run the script:
   ```bash
   python3 compare_tables.py
   ```

## Output Format

The script provides the following information:

### Comparison Results
- Number of matching IDs
- Number of IDs only in API
- Number of IDs only in Excel

### File Summaries
- Total rows
- Empty IDs
- Duplicate IDs
- Final unique IDs

### Attendance Statistics
For each group (Matches, API Only, Excel Only):
- Total shifts
- Average shifts per employee
- Average hours per shift
- Employees with bank account details (as ratio)

## Data Fields

### API Data
- employee_number
- id_number
- bank_account
- Other employee details

### Excel Data
- תעודת זהות (ID number)
- שם עובד (Employee name)
- Other employee details

### Attendance Data
- t_start (shift start time)
- t_end (shift end time)
- employee_number 