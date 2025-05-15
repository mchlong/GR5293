#!/apps/anaconda3/bin/python
# combine_parquets.py

import os
import sys
import argparse
import pandas as pd

def combine_lists(series_of_lists):
    """
    Given a column (for a single ticker) containing lists of news,
    merge them into a single list if multiple entries exist.
    """
    combined = []
    for val in series_of_lists.dropna():
        # Each val is a list of news items; extend the final list
        combined.extend(val)
    # If there's nothing, return None (or empty list if you prefer)
    return combined if combined else None

def combine_parquet_files(input_folder, output_file):
    """
    1. Reads all *.parquet files in input_folder.
    2. Concatenates them row-wise (stack).
    3. Groups by index (trading_day), merges overlapping rows.
    4. Sorts by date index and writes to output_file.
    """
    # Find all Parquet files
    parquet_files = [
        os.path.join(input_folder, f)
        for f in os.listdir(input_folder)
        if f.endswith('.parquet')
    ]
    if not parquet_files:
        print(f"[WARN] No Parquet files found in {input_folder}. Exiting.")
        return
    
    # Read each Parquet into a list of DataFrames
    df_list = []
    for pf in parquet_files:
        print(f"[INFO] Reading {pf} ...")
        df = pd.read_parquet(pf)
        df_list.append(df)
    
    # Concatenate row-wise
    big_df = pd.concat(df_list, axis=0)
    del df_list
    
    # If your index is a DatetimeIndex or a string of dates,
    # ensure it's properly set as the index. 
    # (Most likely it already is from your pipeline, but just in case:)
    # big_df.index = pd.to_datetime(big_df.index)
    
    # Combine duplicates by grouping on the index (trading_day)
    # and applying combine_lists to each column.
    big_df = big_df.groupby(level=0).agg(combine_lists)
    
    # Sort by index (chronological order)
    big_df = big_df.sort_index()
    
    # Optionally ensure we only have 45 columns (the tickers),
    # e.g., if you want to drop any unexpected columns:
    # TICKER_UNIVERSE = [ ... your 45 tickers ... ]
    # big_df = big_df.reindex(columns=TICKER_UNIVERSE)
    
    # Write the final combined file
    big_df.to_parquet(output_file)
    print(f"[INFO] Wrote combined Parquet to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Combine all monthly Parquet files into one DataFrame."
    )
    parser.add_argument("--input_folder", default=".", help="Folder with .parquet files.")
    parser.add_argument("--output_file", default="combined.parquet", help="Output Parquet file.")
    args = parser.parse_args()
    
    combine_parquet_files(args.input_folder, args.output_file)

if __name__ == "__main__":
    main()



#### python combine_parquets.py \
#    --input_folder "/path/to/parquet_files" \
#    --output_file "all_combined.parquet"

