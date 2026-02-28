"""
Motor OE Pricing File Comparison Script
Compares _USA files across monthly folders in 2025 to identify update issues.

Usage:
    python compare_motor_pricing_files.py

Output:
    - Console summary
    - Detailed CSV report: motor_pricing_comparison_report.csv
    - Monthly summary CSV: motor_pricing_monthly_summary.csv
"""

import os
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import argparse

# Configuration
BASE_PATH = r"\\crpfiles\Dept_Files\Automotive R and D\Automobile information\Motor Information OE data\All Motor OE Pricing\ToLoad\2025"


def get_monthly_folders(base_path):
    """Get all monthly folders sorted chronologically."""
    folders = []
    try:
        for item in os.listdir(base_path):
            full_path = os.path.join(base_path, item)
            if os.path.isdir(full_path):
                # Parse folder name like "JAN_05_2025", "FEB_06_2025", etc.
                parts = item.split('_')
                if len(parts) >= 3:
                    month_abbr = parts[0]
                    month_map = {
                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                        'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                        'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                    }
                    if month_abbr in month_map:
                        folders.append({
                            'name': item,
                            'path': full_path,
                            'month_num': month_map[month_abbr],
                            'month_abbr': month_abbr
                        })

        # Sort by month number
        folders.sort(key=lambda x: x['month_num'])
    except Exception as e:
        print(f"Error reading base path: {e}")

    return folders


def get_usa_files(folder_path):
    """Get all _USA files in a folder."""
    usa_files = []
    try:
        for item in os.listdir(folder_path):
            if '_USA' in item and (item.endswith('.txt') or item.endswith('.TXT') or
                                   item.endswith('.csv') or item.endswith('.CVS')):
                usa_files.append(item)
    except Exception as e:
        print(f"Error reading folder {folder_path}: {e}")

    return sorted(usa_files)


def analyze_file(file_path):
    """Analyze a single pricing file and return statistics."""
    stats = {
        'record_count': 0,
        'file_size_kb': 0,
        'min_date': None,
        'max_date': None,
        'unique_dates': set(),
        'date_counts': defaultdict(int),
        'error': None
    }

    try:
        stats['file_size_kb'] = round(os.path.getsize(file_path) / 1024, 2)

        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252']
        lines = None

        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    lines = f.readlines()
                break
            except UnicodeDecodeError:
                continue

        if lines is None:
            stats['error'] = "Could not decode file"
            return stats

        for line in lines:
            line = line.strip()
            if not line:
                continue

            stats['record_count'] += 1

            # Parse pipe-delimited line
            # Format appears to be: Make|PartNumber|Description|Price|Date|...|Flag
            parts = line.split('|')

            if len(parts) >= 5:
                date_str = parts[4].strip()

                # Try to parse date (format appears to be YYYY-MM-DD)
                try:
                    if date_str and len(date_str) >= 10:
                        date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                        stats['unique_dates'].add(date_str[:10])
                        stats['date_counts'][date_str[:10]] += 1

                        if stats['min_date'] is None or date_obj < stats['min_date']:
                            stats['min_date'] = date_obj
                        if stats['max_date'] is None or date_obj > stats['max_date']:
                            stats['max_date'] = date_obj
                except ValueError:
                    pass

    except Exception as e:
        stats['error'] = str(e)

    # Convert dates to strings for easier handling
    stats['min_date_str'] = stats['min_date'].strftime('%Y-%m-%d') if stats['min_date'] else 'N/A'
    stats['max_date_str'] = stats['max_date'].strftime('%Y-%m-%d') if stats['max_date'] else 'N/A'
    stats['unique_date_count'] = len(stats['unique_dates'])

    return stats


def compare_files_across_months(base_path):
    """Main comparison logic."""
    print("=" * 80)
    print("MOTOR OE PRICING FILE COMPARISON REPORT")
    print(f"Base Path: {base_path}")
    print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()

    # Get all monthly folders
    folders = get_monthly_folders(base_path)

    if not folders:
        print("ERROR: No monthly folders found!")
        print(f"Please verify the path exists: {base_path}")
        return

    print(f"Found {len(folders)} monthly folders:")
    for f in folders:
        print(f"  - {f['name']}")
    print()

    # Collect all unique USA file names across all months
    all_usa_files = set()
    folder_files = {}

    for folder in folders:
        files = get_usa_files(folder['path'])
        folder_files[folder['name']] = files
        all_usa_files.update(files)

    all_usa_files = sorted(all_usa_files)
    print(f"Found {len(all_usa_files)} unique _USA files across all months")
    print()

    # Detailed comparison data
    comparison_data = []
    monthly_summary = defaultdict(lambda: {
        'total_files': 0,
        'total_records': 0,
        'total_size_kb': 0,
        'files_with_new_data': 0,
        'max_date_in_month': None
    })

    # Analyze each file across months
    print("Analyzing files (this may take a while)...")
    print("-" * 80)

    for file_name in all_usa_files:
        # Extract manufacturer name
        manufacturer = file_name.replace('_USA.txt', '').replace('_USA.TXT', '')
        manufacturer = manufacturer.replace('_USA.csv', '').replace('_USA.CVS', '')

        file_comparison = {
            'manufacturer': manufacturer,
            'file_name': file_name,
            'months': {}
        }

        prev_stats = None
        prev_month = None

        for folder in folders:
            month_name = folder['name']

            if file_name in folder_files[month_name]:
                file_path = os.path.join(folder['path'], file_name)
                stats = analyze_file(file_path)

                file_comparison['months'][month_name] = stats

                # Update monthly summary
                monthly_summary[month_name]['total_files'] += 1
                monthly_summary[month_name]['total_records'] += stats['record_count']
                monthly_summary[month_name]['total_size_kb'] += stats['file_size_kb']

                if stats['max_date']:
                    current_max = monthly_summary[month_name]['max_date_in_month']
                    if current_max is None or stats['max_date'] > current_max:
                        monthly_summary[month_name]['max_date_in_month'] = stats['max_date']

                # Compare with previous month
                if prev_stats and prev_stats.get('max_date') and stats.get('max_date'):
                    if stats['max_date'] > prev_stats['max_date']:
                        monthly_summary[month_name]['files_with_new_data'] += 1

                prev_stats = stats
                prev_month = month_name
            else:
                file_comparison['months'][month_name] = {'missing': True}

        comparison_data.append(file_comparison)

    # Print Monthly Summary
    print()
    print("=" * 80)
    print("MONTHLY SUMMARY")
    print("=" * 80)
    print()
    print(f"{'Month':<15} {'Files':<8} {'Records':<12} {'Size (KB)':<12} {'New Data':<10} {'Max Date':<12}")
    print("-" * 80)

    for folder in folders:
        month = folder['name']
        summary = monthly_summary[month]
        max_date_str = summary['max_date_in_month'].strftime('%Y-%m-%d') if summary['max_date_in_month'] else 'N/A'

        print(f"{month:<15} {summary['total_files']:<8} {summary['total_records']:<12,} "
              f"{summary['total_size_kb']:<12,.1f} {summary['files_with_new_data']:<10} {max_date_str:<12}")

    # Identify potential issues
    print()
    print("=" * 80)
    print("POTENTIAL ISSUES IDENTIFIED")
    print("=" * 80)
    print()

    issues_found = False

    # Check for stale data (files where max date hasn't changed between months)
    stale_files = []
    for comp in comparison_data:
        months_sorted = [f['name'] for f in folders if f['name'] in comp['months']]

        if len(months_sorted) >= 2:
            # Check last two available months
            last_months = months_sorted[-2:]

            stats1 = comp['months'].get(last_months[0], {})
            stats2 = comp['months'].get(last_months[1], {})

            if not stats1.get('missing') and not stats2.get('missing'):
                max1 = stats1.get('max_date')
                max2 = stats2.get('max_date')

                if max1 and max2 and max1 == max2:
                    stale_files.append({
                        'file': comp['file_name'],
                        'month1': last_months[0],
                        'month2': last_months[1],
                        'date': max1.strftime('%Y-%m-%d')
                    })

    if stale_files:
        issues_found = True
        print(f"⚠️  STALE DATA: {len(stale_files)} files have identical max dates across consecutive months:")
        print()
        for sf in stale_files[:20]:  # Show first 20
            print(f"   - {sf['file']}: Same max date ({sf['date']}) in {sf['month1']} and {sf['month2']}")
        if len(stale_files) > 20:
            print(f"   ... and {len(stale_files) - 20} more")
        print()

    # Check for missing files in recent months
    missing_recent = []
    if len(folders) >= 2:
        last_folder = folders[-1]['name']
        second_last = folders[-2]['name']

        for comp in comparison_data:
            in_second_last = comp['months'].get(second_last) and not comp['months'][second_last].get('missing')
            in_last = comp['months'].get(last_folder) and not comp['months'][last_folder].get('missing')

            if in_second_last and not in_last:
                missing_recent.append({
                    'file': comp['file_name'],
                    'present_in': second_last,
                    'missing_in': last_folder
                })

    if missing_recent:
        issues_found = True
        print(f"⚠️  MISSING FILES: {len(missing_recent)} files present in earlier month but missing in latest:")
        print()
        for mf in missing_recent[:10]:
            print(f"   - {mf['file']}: Present in {mf['present_in']}, Missing in {mf['missing_in']}")
        if len(missing_recent) > 10:
            print(f"   ... and {len(missing_recent) - 10} more")
        print()

    # Check for significant record count changes
    print("=" * 80)
    print("RECORD COUNT CHANGES (Month over Month)")
    print("=" * 80)
    print()

    for comp in comparison_data[:10]:  # Show first 10 manufacturers as sample
        print(f"\n{comp['manufacturer']}:")
        for folder in folders:
            month = folder['name']
            if month in comp['months'] and not comp['months'][month].get('missing'):
                stats = comp['months'][month]
                print(f"   {month}: {stats['record_count']:,} records, "
                      f"Max Date: {stats['max_date_str']}, "
                      f"Size: {stats['file_size_kb']:.1f} KB")

    if not issues_found:
        print("✓ No obvious issues detected in the file comparison.")

    # Generate CSV reports
    generate_csv_reports(folders, comparison_data, monthly_summary)

    return comparison_data, monthly_summary


def generate_csv_reports(folders, comparison_data, monthly_summary):
    """Generate detailed CSV reports."""

    # Determine output directory (same as script location or current directory)
    output_dir = os.path.dirname(os.path.abspath(__file__))

    # Detailed comparison report
    detail_file = os.path.join(output_dir, 'motor_pricing_comparison_report.csv')

    try:
        with open(detail_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Header
            header = ['Manufacturer', 'File Name']
            for folder in folders:
                month = folder['month_abbr']
                header.extend([
                    f'{month}_Records',
                    f'{month}_Size_KB',
                    f'{month}_MaxDate',
                    f'{month}_UniqueDates'
                ])
            writer.writerow(header)

            # Data rows
            for comp in comparison_data:
                row = [comp['manufacturer'], comp['file_name']]

                for folder in folders:
                    month_name = folder['name']
                    if month_name in comp['months'] and not comp['months'][month_name].get('missing'):
                        stats = comp['months'][month_name]
                        row.extend([
                            stats['record_count'],
                            stats['file_size_kb'],
                            stats['max_date_str'],
                            stats['unique_date_count']
                        ])
                    else:
                        row.extend(['MISSING', '', '', ''])

                writer.writerow(row)

        print()
        print(f"✓ Detailed report saved to: {detail_file}")

    except Exception as e:
        print(f"Error writing detailed report: {e}")

    # Monthly summary report
    summary_file = os.path.join(output_dir, 'motor_pricing_monthly_summary.csv')

    try:
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Month', 'Total Files', 'Total Records', 'Total Size (KB)',
                             'Files With New Data', 'Max Date in Month'])

            for folder in folders:
                month = folder['name']
                summary = monthly_summary[month]
                max_date_str = summary['max_date_in_month'].strftime('%Y-%m-%d') if summary[
                    'max_date_in_month'] else 'N/A'

                writer.writerow([
                    month,
                    summary['total_files'],
                    summary['total_records'],
                    round(summary['total_size_kb'], 2),
                    summary['files_with_new_data'],
                    max_date_str
                ])

        print(f"✓ Monthly summary saved to: {summary_file}")

    except Exception as e:
        print(f"Error writing summary report: {e}")


def main():
    parser = argparse.ArgumentParser(description='Compare Motor OE Pricing files across months')
    parser.add_argument('--path', '-p', type=str, default=BASE_PATH,
                        help='Base path to the 2025 folder')

    args = parser.parse_args()

    compare_files_across_months(args.path)


if __name__ == '__main__':
    main()