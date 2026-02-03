import os
import pandas as pd
from collections import Counter, defaultdict

def main():
    raw_data_dir = os.path.join(os.path.dirname(__file__), 'raw_data')
    xls_files = []
    for root, _, files in os.walk(raw_data_dir):
        for f in files:
            if f.endswith('.xls'):
                xls_files.append(os.path.join(root, f))
    header_counts = Counter()
    file_headers = defaultdict(list)
    all_dfs = []

    for xls in xls_files:
        file_path = os.path.join(raw_data_dir, xls)
        try:
            df = pd.read_excel(file_path)
            all_dfs.append(df)
            for col in df.columns:
                header_counts[col] += 1
                file_headers[col].append(xls)
        except Exception as e:
            print(f"Error reading {xls}: {e}")

    # Save header counts report
    report_path = os.path.join(os.path.dirname(__file__), '../clean_data/agg_data_report.txt')
    with open(report_path, 'w') as f:
        for header, count in header_counts.most_common():
            f.write(f'Header: {header} | Count: {count}\n')
            f.write(f'  Files: {file_headers[header]}\n')
    print(f"Report written to {report_path}")

    # Concatenate all dataframes (with different headers) and save as CSV
    if all_dfs:
        all_data = pd.concat(all_dfs, ignore_index=True, sort=False)
        agg_csv_path = os.path.join(os.path.dirname(__file__), '../clean_data/agg_data.csv')
        all_data.to_csv(agg_csv_path, index=False)
        print(f"Aggregated data saved to {agg_csv_path}")
    else:
        print("No dataframes to aggregate.")

if __name__ == "__main__":
    main()
