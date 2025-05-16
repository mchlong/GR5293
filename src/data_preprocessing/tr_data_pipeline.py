#!/apps/anaconda3/bin/python
# tr_data_pipeline.py

import os
import sys
import re
import json
import gc
import argparse
import pytz
import traceback
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# --------------------------------------------------------------------------------
# ------------------------- 1. Pipeline Configuration -----------------------------
# --------------------------------------------------------------------------------

TICKER_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'AVGO', 'ADBE', 'UNH', 'JNJ', 'PFE', 'MRK', 'ABBV',
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'GOOGL',
    'META', 'DIS', 'VZ', 'CMCSA', 'PG', 'KO', 'PEP', 'WMT', 'COST', 'XOM', 'CVX',
    'COP', 'BA', 'UNP', 'HON', 'NEE', 'DUK', 'SO', 'PLD', 'AMT', 'CCI', 'LIN', 'SHW', 'DOW'
]

DATE_MASK = '[DATE]'
COMPANY_MASK = '[COMPANY]'

COMPANY_MASK_PATTERNS = {
    'AAPL': r'\b(Apple(?:\s+Inc\.?)?|AAPL)\b',
    'MSFT': r'\b(Microsoft(?:\s+Corp(?:oration)?\.?)?|MSFT)\b',
    'NVDA': r'\b(Nvidia(?:\s+Corporation)?|NVIDIA|NVDA)\b',
    'AVGO': r'\b(Broadcom(?:\s+Inc\.?)?|AVGO)\b',
    'ADBE': r'\b(Adobe(?:\s+Inc\.?)?|ADBE)\b',
    'UNH': r'\b(UnitedHealth(?:\s+Group)?(?:\s+Inc\.?)?|UNH)\b',
    'JNJ': r'\b(Johnson\s*&?\s*Johnson|JNJ)\b',
    'PFE': r'\b(Pfizer(?:\s+Inc\.?)?|PFE)\b',
    'MRK': r'\b(Merck(?:\s+&\s+Co\.?|\s+Co\.?)?|MRK)\b',
    'ABBV': r'\b(AbbVie(?:\s+Inc\.?)?|ABBV)\b',
    'JPM': r'\b(JPMorgan(?:\s+Chase(?:\s+&\s+Co\.?)?)?|JPM)\b',
    'BAC': r'\b(Bank\s+of\s+America(?:\s+Corp\.?)?|BAC)\b',
    'WFC': r'\b(Wells\s+Fargo(?:\s+&\s+Company)?|WFC)\b',
    'GS': r'\b(Goldman\s+Sachs(?:\s+Group)?|GS)\b',
    'MS': r'\b(Morgan\s+Stanley|MS)\b',
    'AMZN': r'\b(Amazon(?:\.com)?(?:\s+Inc\.?)?|AMZN)\b',
    'TSLA': r'\b(Tesla(?:\s+Inc\.?)?|TSLA)\b',
    'HD': r'\b(Home\s+Depot(?:\s+Inc\.?)?|HD)\b',
    'MCD': r'\b(McDonald(?:\'?s)?(?:\s+Corporation)?|MCD)\b',
    'NKE': r'\b(Nike(?:\s+Inc\.?)?|NKE)\b',
    'GOOGL': r'\b(Alphabet(?:\s+Inc\.?)?|Google(?:\s+LLC)?|GOOGL)\b',
    'META': r'\b(Meta(?:\s+Platforms)?(?:\s+Inc\.?)?|META)\b',
    'DIS': r'\b(Walt\s+Disney(?:\s+Company)?|DIS)\b',
    'VZ': r'\b(Verizon(?:\s+Communications)?(?:\s+Inc\.?)?|VZ)\b',
    'CMCSA': r'\b(Comcast(?:\s+Corporation)?|CMCSA)\b',
    'PG': r'\b(Procter\s*&?\s*Gamble(?:\s+Co\.?)?|PG)\b',
    'KO': r'\b(Coca[-\s]?Cola(?:\s+Company)?|KO)\b',
    'PEP': r'\b(PepsiCo(?:\s+Inc\.?)?|PEP)\b',
    'WMT': r'\b(Walmart(?:\s+Inc\.?)?|WMT)\b',
    'COST': r'\b(Costco(?:\s+Wholesale(?:\s+Corporation)?)?|COST)\b',
    'XOM': r'\b(ExxonMobil|XOM)\b',
    'CVX': r'\b(Chevron(?:\s+Corporation)?|CVX)\b',
    'COP': r'\b(ConocoPhillips|COP)\b',
    'BA': r'\b(Boeing(?:\s+Co\.?)?|BA)\b',
    'UNP': r'\b(Union\s+Pacific(?:\s+Corporation)?|UNP)\b',
    'HON': r'\b(Honeywell(?:\s+International)?(?:\s+Inc\.?)?|HON)\b',
    'NEE': r'\b(NextEra(?:\s+Energy)?(?:\s+Inc\.?)?|NEE)\b',
    'DUK': r'\b(Duke\s+Energy(?:\s+Corp\.?)?|DUK)\b',
    'SO': r'\b(Southern\s+Company(?:\s+Inc\.?)?|SO)\b',
    'PLD': r'\b(Prologis(?:\s+Inc\.?)?|PLD)\b',
    'AMT': r'\b(American\s+Tower(?:\s+Corporation)?|AMT)\b',
    'CCI': r'\b(Crown\s+Castle(?:\s+International)?(?:\s+Inc\.?)?|CCI)\b',
    'LIN': r'\b(Linde(?:\s+plc)?|LIN)\b',
    'SHW': r'\b(Sherwin[-\s]?Williams(?:\s+Co\.?)?|SHW)\b',
    'DOW': r'\b(Dow(?:\s+Inc\.?)?|DOW)\b'
}

PRODUCT_MASK_PATTERNS = {
    'AAPL': {
        r'\b(iPhone)\b': '[Product 1]',
        r'\b(iPad)\b': '[Product 2]',
        r'\b(MacBook|Mac)\b': '[Product 3]',
        r'\b(Apple\s+Watch)\b': '[Product 4]',
        r'\b(AirPods)\b': '[Product 5]'
    },
    'MSFT': {
        r'\b(Windows)\b': '[Product 1]',
        r'\b(Office)\b': '[Product 2]',
        r'\b(Azure)\b': '[Product 3]',
        r'\b(Xbox)\b': '[Product 4]',
        r'\b(Surface)\b': '[Product 5]'
    },
    'NVDA': {
        r'\b(GeForce)\b': '[Product 1]',
        r'\b(Quadro)\b': '[Product 2]',
        r'\b(Tesla)\b': '[Product 3]'
    },
    'AVGO': {
        r'\b(chip|semiconductor)\b': '[Product 1]'
    },
    'ADBE': {
        r'\b(Photoshop)\b': '[Product 1]',
        r'\b(Illustrator)\b': '[Product 2]',
        r'\b(Premiere)\b': '[Product 3]',
        r'\b(Acrobat)\b': '[Product 4]'
    },
    'UNH': {
        r'\b(Optum)\b': '[Product 1]',
        r'\b(UnitedHealthcare)\b': '[Product 2]'
    },
    'JNJ': {
        r'\b(Tylenol)\b': '[Product 1]',
        r'\b(Neutrogena)\b': '[Product 2]',
        r'\b(Band[-\s]?Aid)\b': '[Product 3]'
    },
    'PFE': {
        r'\b(Lipitor)\b': '[Product 1]',
        r'\b(Viagra)\b': '[Product 2]',
        r'\b(Pfizer\s+vaccine)\b': '[Product 3]'
    },
    'MRK': {
        r'\b(Keytruda)\b': '[Product 1]',
        r'\b(Gardasil)\b': '[Product 2]'
    },
    'ABBV': {
        r'\b(Humira)\b': '[Product 1]'
    },
    'JPM': {
        r'\b(Chase Sapphire)\b': '[Product 1]',
        r'\b(Chase Freedom)\b': '[Product 2]'
    },
    'BAC': {
        r'\b(Chase)\b': '[Product 1]'
    },
    'GS': {
        r'\b(Marcus)\b': '[Product 1]'
    },
    'MS': {
        r'\b(Wealth Management)\b': '[Product 1]'
    },
    'AMZN': {
        r'\b(Amazon\s+Prime)\b': '[Product 1]',
        r'\b(AWS|Amazon\s+Web\s+Services)\b': '[Product 2]',
        r'\b(Kindle)\b': '[Product 3]',
        r'\b(Echo)\b': '[Product 4]'
    },
    'TSLA': {
        r'\b(Model\s*S)\b': '[Product 1]',
        r'\b(Model\s*3)\b': '[Product 2]',
        r'\b(Model\s*X)\b': '[Product 3]',
        r'\b(Model\s*Y)\b': '[Product 4]',
        r'\b(Cybertruck)\b': '[Product 5]'
    },
    'MCD': {
        r'\b(Big\s+Mac)\b': '[Product 1]',
        r'\b(McCafe)\b': '[Product 2]',
        r'\b(McFlurry)\b': '[Product 3]'
    },
    'NKE': {
        r'\b(Air\s+Jordan)\b': '[Product 1]',
        r'\b(Nike\s+Air)\b': '[Product 2]',
        r'\b(Dunk)\b': '[Product 3]'
    },
    'GOOGL': {
        r'\b(Google\s+Search)\b': '[Product 1]',
        r'\b(Android)\b': '[Product 2]',
        r'\b(YouTube)\b': '[Product 3]',
        r'\b(Pixel)\b': '[Product 4]'
    },
    'META': {
        r'\b(Facebook)\b': '[Product 1]',
        r'\b(Instagram)\b': '[Product 2]',
        r'\b(WhatsApp)\b': '[Product 3]',
        r'\b(Oculus)\b': '[Product 4]'
    },
    'DIS': {
        r'\b(Disney\+)\b': '[Product 1]',
        r'\b(Marvel)\b': '[Product 2]',
        r'\b(Pixar)\b': '[Product 3]',
        r'\b(Star\s+Wars)\b': '[Product 4]'
    },
    'VZ': {
        r'\b(5G)\b': '[Product 1]',
        r'\b(Fios)\b': '[Product 2]'
    },
    'CMCSA': {
        r'\b(Xfinity)\b': '[Product 1]'
    },
    'PG': {
        r'\b(Tide)\b': '[Product 1]',
        r'\b(Pampers)\b': '[Product 2]',
        r'\b(Gillette)\b': '[Product 3]'
    },
    'KO': {
        r'\b(Coke|Diet\s+Coke)\b': '[Product 1]',
        r'\b(Sprite)\b': '[Product 2]'
    },
    'PEP': {
        r'\b(Pepsi)\b': '[Product 1]',
        r'\b(Lay\'?s)\b': '[Product 2]',
        r'\b(Gatorade)\b': '[Product 3]'
    },
    'WMT': {
        r'\b(Walmart\+?)\b': '[Product 1]',
        r'\b(Sam\'?s\s+Club)\b': '[Product 2]'
    },
    'BA': {
        r'\b(737|787|777)\b': '[Product 1]'
    },
    'HON': {
        r'\b(Honeywell\s+Aerospace)\b': '[Product 1]'
    }
    # Others omitted for brevity
}

# --------------------------------------------------------------------------------
# ------------------------- 2. Helper Functions -----------------------------------
# --------------------------------------------------------------------------------

def load_json_data(file_path):
    """Load JSON data from file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def extract_tickers_from_subjects(subjects):
    """
    Extract ticker symbols from TR subject codes.
    Subject codes that refer to tickers start with "R:" and may have suffixes (e.g. "R:MSFT.O").
    """
    tickers = []
    for subj in subjects:
        if subj.startswith("R:"):
            ticker_raw = subj[2:]
            match = re.match(r'([A-Z]+)', ticker_raw)
            if match:
                tickers.append(match.group(1))
    return tickers

def filter_article_by_universe(article, universe):
    """
    Return True if the article's subject codes (tickers) intersect with our universe.
    """
    subjects = article.get('data', {}).get('subjects', [])
    tickers_in_article = extract_tickers_from_subjects(subjects)
    return any(ticker in universe for ticker in tickers_in_article)

def convert_to_est_trading_day(utc_timestamp_str):
    """
    Convert a UTC timestamp (ISO string) to EST and determine the trading day.
    If the local (EST) time is after 4pm, the news is assigned to the next trading day.
    Also, if the trading day falls on a weekend, shift to Monday.
    """
    utc_timestamp_str = utc_timestamp_str.rstrip('Z')
    utc_dt = datetime.fromisoformat(utc_timestamp_str)
    # Attach UTC tzinfo
    utc_dt = utc_dt.replace(tzinfo=pytz.UTC)

    est_tz = pytz.timezone('America/New_York')
    est_dt = utc_dt.astimezone(est_tz)

    # 4pm cutoff
    cutoff = est_dt.replace(hour=16, minute=0, second=0, microsecond=0)
    if est_dt >= cutoff:
        trading_day = est_dt.date() + pd.Timedelta(days=1)
    else:
        trading_day = est_dt.date()

    # Shift forward if Saturday/Sunday
    while trading_day.weekday() >= 5:
        trading_day += pd.Timedelta(days=1)

    return trading_day

def mask_dates(text):
    """
    Replace date/time patterns with stricter rules, plus month/year detection.
    """
    # 1) Basic numeric date: "12/31/2020" or "2020-12-31"
    date_pattern = r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b'
    text = re.sub(date_pattern, DATE_MASK, text)

    # 2) Time patterns "13:40", "1:30:45"
    time_pattern = r'\b\d{1,2}:\d{2}(?::\d{2})?\b'
    text = re.sub(time_pattern, '[TIME]', text)

    # 3) Full date expressions (month name + numeric day or ordinal)
    month_pattern = r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|' \
                    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    numeric_day_pattern = r'\d{1,2}(?:st|nd|rd|th)?'
    ordinal_words = r'(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|' \
                    r'eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|' \
                    r'eighteenth|nineteenth|twentieth|twenty[-\s]first|twenty[-\s]second|' \
                    r'twenty[-\s]third|twenty[-\s]fourth)'
    full_date_pattern_numeric = rf'\b{month_pattern}\s+{numeric_day_pattern}\b'
    full_date_pattern_words = rf'\b{month_pattern}\s+{ordinal_words}\b'
    text = re.sub(full_date_pattern_numeric, DATE_MASK, text, flags=re.IGNORECASE)
    text = re.sub(full_date_pattern_words, DATE_MASK, text, flags=re.IGNORECASE)

    # 4) Standalone month names
    text = re.sub(rf'\b{month_pattern}\b', '[MONTH]', text, flags=re.IGNORECASE)

    # 5) Years: 19xx or 20xx
    text = re.sub(r'\b(19\d{2}|20\d{2})\b', '[YEAR]', text)

    return text

def mask_company(text, ticker):
    """
    Replace company-related words with COMPANY_MASK using the ticker-specific pattern.
    """
    pattern = COMPANY_MASK_PATTERNS.get(ticker, rf'\b{re.escape(ticker)}\b')
    return re.sub(pattern, COMPANY_MASK, text, flags=re.IGNORECASE)

def mask_products(text, ticker):
    """
    Replace product-related words for the given ticker with product mask tokens.
    """
    product_patterns = PRODUCT_MASK_PATTERNS.get(ticker, {})
    for pat, product_mask in product_patterns.items():
        text = re.sub(pat, product_mask, text, flags=re.IGNORECASE)
    return text

def mask_text(text, ticker):
    """
    Apply date masking, company masking, and product masking.
    """
    text = mask_dates(text)
    text = mask_company(text, ticker)
    text = mask_products(text, ticker)
    return text

# --------------------------------------------------------------------------------
# ------------------------- 3. Main Processing Functions --------------------------
# --------------------------------------------------------------------------------

def process_articles(data, do_mask=True):
    """
    Process Thomson Reuters JSON:
      - Filter by English language and ticker universe.
      - Convert UTC to EST trading day with 4pm cutoff.
      - Mask date/company/product in headlines/bodies (if do_mask=True).
    Returns a list of dicts with 'trading_day', 'ticker', 'masked_headline', 'masked_body'.
    """
    processed = []
    items = data.get('Items', [])
    for article in items:
        # Must be English
        if article.get('data', {}).get('language', '').lower() != 'en':
            continue
        
        # Must have body text
        if not article.get('data', {}).get('body', '').strip():
            continue
        
        # Must have at least one ticker in universe
        if not filter_article_by_universe(article, TICKER_UNIVERSE):
            continue
        
        # Timestamps
        timestamps = article.get('timestamps', [])
        if not timestamps:
            continue
        utc_timestamp = timestamps[0].get('timestamp')
        if not utc_timestamp:
            continue
        
        # Convert time
        trading_day = convert_to_est_trading_day(utc_timestamp)
        
        # Identify tickers
        subjects = article.get('data', {}).get('subjects', [])
        article_tickers = [t for t in extract_tickers_from_subjects(subjects) if t in TICKER_UNIVERSE]
        if not article_tickers:
            continue
        
        headline = article.get('data', {}).get('headline', '')
        body = article.get('data', {}).get('body', '')
        
        for ticker in article_tickers:
            if do_mask:
                mh = mask_text(headline, ticker)
                mb = mask_text(body, ticker)
            else:
                mh = headline
                mb = body
            
            processed.append({
                'trading_day': trading_day,
                'ticker': ticker,
                'masked_headline': mh,
                'masked_body': mb
            })
    
    return processed

def aggregate_articles(processed_articles):
    """
    Group processed articles by (trading_day, ticker) and produce a list of unique masked news items.
    """
    df = pd.DataFrame(processed_articles)
    if df.empty:
        return pd.DataFrame()  # no articles
    
    # Identify duplicates by the exact combined text
    df['combined'] = df['masked_headline'] + " " + df['masked_body']
    df.drop_duplicates(subset=['combined'], inplace=True)
    df['masked_text'] = df['masked_headline'] + " " + df['masked_body']
    
    agg = df.groupby(['trading_day', 'ticker'])['masked_text'].apply(list).reset_index()
    pivot_df = agg.pivot(index='trading_day', columns='ticker', values='masked_text')
    return pivot_df

def process_pipeline(filepath, do_mask=True, convert_to_parquet=True):
    """
    Full pipeline on a single file:
      - Load JSON
      - Process articles
      - Aggregate articles
      - Optionally write a Parquet file
    Returns the pivot DataFrame.
    """
    data = load_json_data(filepath)
    processed_articles = process_articles(data, do_mask=do_mask)
    del data
    gc.collect()
    
    pivot_df = aggregate_articles(processed_articles)
    del processed_articles
    gc.collect()
    
    if convert_to_parquet and not pivot_df.empty:
        outname = os.path.basename(filepath) + '_sentiment_news.parquet'
        pivot_df.to_parquet(outname)
        print(f"[INFO] Wrote {outname}")
    
    return pivot_df

# --------------------------------------------------------------------------------
# ------------------------- 4. Month-by-Month Driver ------------------------------
# --------------------------------------------------------------------------------

def monthly_date_range(start_date: date, end_date: date):
    """
    Generator for year-month from start_date to end_date (inclusive).
    Yields (year, month).
    """
    current = start_date
    while current <= end_date:
        yield current.year, current.month
        current += relativedelta(months=1)

def main():
    """
    Parse arguments and run the pipeline from a start date to an end date.
    For each year-month, first check for:
      1) STORY.RTRS.YYYY-MM.REC.JSON.txt
      2) News.RTRS.YYYYMM.0214.txt
    If neither exists, process every .txt file in the directory.
    Detailed error logging is printed to pinpoint issues.
    """
    parser = argparse.ArgumentParser(
        description="Process Thomson Reuters JSON news from a date range into masked Parquet outputs."
    )
    parser.add_argument("--base_dir", default="/data/ThomsonReuters_NewsArchive",
                        help="Base directory containing year subfolders with JSON files.")
    parser.add_argument("--start_date", default="2018-01-01",
                        help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end_date", default="2024-12-14",
                        help="End date (YYYY-MM-DD).")
    parser.add_argument("--mask", action="store_true", default=False,
                        help="Enable masking (if specified, masking is applied).")
    
    args = parser.parse_args()
    
    start_dt = pd.to_datetime(args.start_date).date()
    end_dt = pd.to_datetime(args.end_date).date()
    
    if start_dt > end_dt:
        print("[ERROR] Start date cannot be after end date.")
        sys.exit(1)
    
    do_mask = args.mask
    print(f"[INFO] Processing from {start_dt} to {end_dt} in monthly increments.")
    print(f"[INFO] Masking enabled? {do_mask}")
    
    for (year, month) in monthly_date_range(start_dt, end_dt):
        subdir = os.path.join(args.base_dir, str(year))
        # Candidate filenames:
        story_name = f"STORY.RTRS.{year}-{month:02d}.REC.JSON.txt"
        news_name = f"News.RTRS.{year}{month:02d}.0214.txt"
        candidates = [story_name, news_name]
        
        any_found = False
        for fname in candidates:
            filepath = os.path.join(subdir, fname)
            if os.path.isfile(filepath):
                any_found = True
                print(f"[INFO] Processing {filepath} ...")
                try:
                    process_pipeline(filepath, do_mask=do_mask, convert_to_parquet=True)
                except Exception as e:
                    print(f"[ERROR] Failed on {filepath}: {e}")
                    traceback.print_exc()
                finally:
                    gc.collect()
        
        if not any_found:
            # Fallback: process every .txt file in the subdirectory
            if os.path.isdir(subdir):
                txt_files = [f for f in os.listdir(subdir) if f.endswith('.txt')]
                if txt_files:
                    for fname in txt_files:
                        filepath = os.path.join(subdir, fname)
                        print(f"[INFO] Processing fallback file {filepath} ...")
                        try:
                            process_pipeline(filepath, do_mask=do_mask, convert_to_parquet=True)
                        except Exception as e:
                            print(f"[ERROR] Failed on {filepath}: {e}")
                            traceback.print_exc()
                        finally:
                            gc.collect()
                else:
                    print(f"[WARN] No .txt files found in {subdir}. Skipping.")
            else:
                print(f"[WARN] Directory {subdir} does not exist. Skipping.")
    
    print("[INFO] All done. One Parquet file per processed file is written (if data existed).")

if __name__ == "__main__":
    main()



#### ./tr_data_pipeline.py --base_dir "/data/ThomsonReuters_NewsArchive" --start_date "2018-01-01" --end_date "2024-12-14" --mask
#### ./tr_data_pipeline.py --base_dir "/data/ThomsonReuters_NewsArchive" --start_date "2018-01-01" --end_date "2024-12-14"
