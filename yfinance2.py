import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
import tempfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ose_stock_fetcher.log'),
        logging.StreamHandler()
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID      = '1vtYTky_0BhIeIWPMhnJyBmC5ghEw6zFaZVNpcdmlaUY'
LOCAL_CREDS_PATH    = '/Users/mac/pythonweb/datafeed_api_secret.json'
DN_URL              = 'https://investor.dn.no/#!/Kurser/Aksjer/'
START_DATE          = '2020-03-16'
DN_PRICE_DIFF_THRESHOLD = 0.02   # 2 % — flag if DN vs yfinance differ by more


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets auth
# ─────────────────────────────────────────────────────────────────────────────

def setup_google_sheets():
    """
    Setup Google Sheets connection.
    In CI (GitHub Actions) the full JSON is stored in the env-var
    GOOGLE_CREDENTIALS_JSON.  Locally we fall back to the file on disk.
    """
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    env_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if env_json:
        # Write to a temp file so oauth2client can read it
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(env_json)
            creds_path = f.name
        logging.info("Using credentials from environment variable")
    else:
        creds_path = LOCAL_CREDS_PATH
        logging.info(f"Using local credentials file: {creds_path}")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(credentials)
    return client


# ─────────────────────────────────────────────────────────────────────────────
# DN Investor scraper  (constituents + live snapshot)
# ─────────────────────────────────────────────────────────────────────────────

def _build_chrome_options(use_headless=True):
    options = webdriver.ChromeOptions()
    if use_headless:
        options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    return options


def get_dn_full_snapshot(use_headless=True):
    """
    Scrape the DN Investor stock table in one browser pass and return:
      - tickers     : list of '<TICKER>.OL' strings  (for building the OSE list)
      - dn_snapshot : dict  { '<TICKER>.OL': {'price': float, 'market_cap': float,
                                               'volume': float, 'change_pct': float} }

    Column layout observed on investor.dn.no/#!/Kurser/Aksjer/ :
      0: Company name
      1: Ticker
      2: Price  (NOK, formatted like "123,45")
      3: Change (NOK)
      4: Change %
      5: Volume
      6: Market cap  (NOK millions)
      (additional columns may exist but are ignored)
    """
    logging.info("Scraping DN Investor for constituents + live snapshot …")

    options = _build_chrome_options(use_headless)
    tickers = []
    dn_snapshot = {}

    try:
        driver = webdriver.Chrome(options=options)
        driver.get(DN_URL)

        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(4)   # Let Angular finish rendering

        stock_rows = driver.find_elements(By.CSS_SELECTOR, 'table tbody tr')
        logging.info(f"Found {len(stock_rows)} rows on DN Investor")

        for i, row in enumerate(stock_rows, 1):
            try:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) < 2:
                    continue

                raw_ticker = cells[1].text.strip()
                if not raw_ticker:
                    continue

                full_ticker = f"{raw_ticker}.OL"
                tickers.append(full_ticker)

                snapshot = {}

                # Price (col 2)
                if len(cells) > 2:
                    snapshot['price'] = _parse_dn_number(cells[2].text)

                # Change % (col 4)
                if len(cells) > 4:
                    snapshot['change_pct'] = _parse_dn_number(cells[4].text)

                # Volume (col 5)
                if len(cells) > 5:
                    snapshot['volume'] = _parse_dn_number(cells[5].text)

                # Market cap (col 6) — value includes a Norwegian unit suffix:
                # 'bill.' = billioner = 10^12, 'mrd.' = milliarder = 10^9, 'mill.' = millioner = 10^6
                if len(cells) > 6:
                    snapshot['market_cap'] = _parse_dn_market_cap(cells[6].text)

                dn_snapshot[full_ticker] = snapshot

            except (IndexError, NoSuchElementException) as e:
                logging.warning(f"Could not extract row {i}: {e}")
                continue

        driver.quit()
        logging.info(f"DN snapshot: {len(tickers)} tickers captured")

    except Exception as e:
        logging.error(f"Error scraping DN: {e}")
        raise

    return tickers, dn_snapshot


def _parse_dn_number(text: str):
    """
    Convert a DN-formatted plain number string to float.
    Handles Norwegian thousand separators and comma decimals.
    Examples: '1 234,56' → 1234.56,  '300,50' → 300.5
    """
    if not text or text.strip() in ('', '-', 'N/A'):
        return None
    try:
        # Strip any trailing unit suffix (e.g. 'bill.', 'mrd.', 'mill.') before parsing
        cleaned = text.strip()
        for suffix in ('bill.', 'mrd.', 'mill.'):
            if suffix in cleaned:
                cleaned = cleaned.replace(suffix, '').strip()
        cleaned = cleaned.replace('\xa0', '').replace('\u202f', '').replace(' ', '').replace('.', '').replace(',', '.')
        return float(cleaned)
    except ValueError:
        return None


def _parse_dn_market_cap(text: str):
    """
    Parse a DN Investor market-cap string that includes a Norwegian unit suffix.

    Norwegian naming convention (long scale):
      'mill.'  = millioner   = 1 000 000          (10^6)
      'mrd.'   = milliarder  = 1 000 000 000       (10^9)
      'bill.'  = billioner   = 1 000 000 000 000   (10^12)

    Examples:
      '452,15 mill.' → 452_150_000
      '449,64 mrd.'  → 449_640_000_000
      '1,02 bill.'   → 1_020_000_000_000
    """
    if not text or text.strip() in ('', '-', 'N/A'):
        return None

    text = text.strip().replace('\xa0', '').replace('\u202f', '')

    # Determine multiplier from suffix
    multiplier = 1
    if 'bill.' in text:
        multiplier = 1_000_000_000_000
        text = text.replace('bill.', '').strip()
    elif 'mrd.' in text:
        multiplier = 1_000_000_000
        text = text.replace('mrd.', '').strip()
    elif 'mill.' in text:
        multiplier = 1_000_000
        text = text.replace('mill.', '').strip()

    try:
        # Remove thousand separators (space or period) then swap comma→dot for decimal
        cleaned = text.replace(' ', '').replace('.', '').replace(',', '.')
        return float(cleaned) * multiplier
    except ValueError:
        logging.warning(f"Could not parse DN market cap: '{text}'")
        return None


# Legacy wrapper so old test scripts still work
def get_ose_constituents_from_dn(use_headless=True):
    """Return just the ticker list (backward-compatible wrapper)."""
    tickers, _ = get_dn_full_snapshot(use_headless=use_headless)
    return tickers


# ─────────────────────────────────────────────────────────────────────────────
# Ticker validation
# ─────────────────────────────────────────────────────────────────────────────

def verify_ticker(ticker, min_data_points=5):
    """
    Verify if a ticker is valid and has sufficient recent data.
    Returns True if valid, False otherwise.
    """
    try:
        stock = yf.Ticker(ticker)
        test_data = stock.history(period='2mo')

        if len(test_data) >= min_data_points and not test_data.empty:
            most_recent = test_data.index[-1]
            days_since_trade = (datetime.now() - most_recent.to_pydatetime().replace(tzinfo=None)).days
            if days_since_trade > 30:
                logging.warning(f"{ticker}: Last trade {days_since_trade}d ago — may be delisted")
                return False
            return True
        else:
            logging.warning(f"{ticker}: Insufficient data ({len(test_data)} points)")
            return False

    except Exception as e:
        logging.warning(f"Verification error for {ticker}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Corporate actions
# ─────────────────────────────────────────────────────────────────────────────

def get_corporate_actions(ticker, start_date, end_date):
    """
    Fetch dividends and splits for a ticker in [start_date, end_date].
    Returns DataFrame with columns: Date, Ticker, Action_Type, Details, Value.
    """
    try:
        stock = yf.Ticker(ticker)
        dividends = stock.dividends
        splits = stock.splits

        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        for attr in ('tz',):
            if hasattr(start_date, attr) and start_date.tz is not None:
                start_date = start_date.tz_localize(None)
            if hasattr(end_date, attr) and end_date.tz is not None:
                end_date = end_date.tz_localize(None)

        if hasattr(dividends.index, 'tz') and dividends.index.tz is not None:
            dividends.index = dividends.index.tz_localize(None)
        if hasattr(splits.index, 'tz') and splits.index.tz is not None:
            splits.index = splits.index.tz_localize(None)

        dividends = dividends[(dividends.index >= start_date) & (dividends.index <= end_date)]
        splits    = splits[(splits.index >= start_date) & (splits.index <= end_date)]

        actions = []
        for date, value in dividends.items():
            actions.append({'Date': date, 'Ticker': ticker,
                            'Action_Type': 'Dividend', 'Details': 'Dividend payment', 'Value': value})
        for date, value in splits.items():
            actions.append({'Date': date, 'Ticker': ticker,
                            'Action_Type': 'Split', 'Details': f'{value}-for-1 split', 'Value': value})

        if actions:
            df = pd.DataFrame(actions).sort_values('Date')
            return df
        return pd.DataFrame(columns=['Date', 'Ticker', 'Action_Type', 'Details', 'Value'])

    except Exception as e:
        logging.warning(f"Error fetching corporate actions for {ticker}: {e}")
        return pd.DataFrame(columns=['Date', 'Ticker', 'Action_Type', 'Details', 'Value'])


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets — state reading
# ─────────────────────────────────────────────────────────────────────────────

def read_sheet_state(sheet):
    """
    Read the existing 'Prices' worksheet to determine:
      - latest_date     : the most recent date row (pd.Timestamp), or None if empty
      - existing_tickers: list of ticker column headers already in the sheet

    Returns (latest_date, existing_tickers).
    """
    try:
        ws = sheet.worksheet('Prices')
        all_values = ws.get_all_values()

        if not all_values or len(all_values) < 2:
            logging.info("Prices sheet is empty — will do a full initial load")
            return None, []

        header = all_values[0]          # ['Date', 'EQNR.OL', 'DNB.OL', ...]
        existing_tickers = header[1:]   # drop the 'Date' column

        # Find the last non-empty date row
        latest_date = None
        for row in reversed(all_values[1:]):
            if row and row[0]:
                try:
                    latest_date = pd.to_datetime(row[0])
                    break
                except Exception:
                    continue

        logging.info(f"Sheet state — latest date: {latest_date}, "
                     f"existing tickers: {len(existing_tickers)}")
        return latest_date, existing_tickers

    except gspread.exceptions.WorksheetNotFound:
        logging.info("'Prices' worksheet not found — will create fresh")
        return None, []
    except Exception as e:
        logging.error(f"Error reading sheet state: {e}")
        return None, []


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets — column sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_columns(sheet, current_tickers, existing_tickers):
    """
    For each data worksheet ('Prices', 'Market Caps') check if any tickers in
    current_tickers are missing from existing_tickers.  If so, append them as
    new columns to the right — never removing any existing column.

    Returns the full ordered ticker list (existing + any new ones).
    """
    new_tickers = [t for t in current_tickers if t not in existing_tickers]
    if not new_tickers:
        logging.info("No new tickers to add to sheets")
        return existing_tickers   # unchanged

    logging.info(f"Adding {len(new_tickers)} new ticker column(s): {new_tickers}")

    for ws_name in ('Prices', 'Market Caps'):
        try:
            ws = sheet.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound:
            continue   # will be created during full write

        # Append a header cell value for each new ticker in the next empty column
        header_row = ws.row_values(1)
        start_col = len(header_row) + 1
        for i, ticker in enumerate(new_tickers):
            col_letter = _col_letter(start_col + i)
            ws.update(f'{col_letter}1', [[ticker]])

    full_ticker_list = existing_tickers + new_tickers
    return full_ticker_list


def _col_letter(n: int) -> str:
    """Convert 1-based column number to spreadsheet letter (1→A, 27→AA …)."""
    result = ''
    while n:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets — metadata / legacy tracking
# ─────────────────────────────────────────────────────────────────────────────

def update_metadata_sheet(sheet, current_tickers, existing_tickers):
    """
    Maintain a 'Metadata' worksheet tracking each ticker's status.
    Columns: Ticker | Status | Date_Added | Date_Removed
    Status is 'active' or 'legacy'.
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    current_set  = set(current_tickers)
    existing_set = set(existing_tickers)

    try:
        try:
            ws = sheet.worksheet('Metadata')
            rows = ws.get_all_records()          # list of dicts
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet('Metadata', rows=2000, cols=4)
            rows = []

        # Build a dict from existing metadata
        meta = {}
        for r in rows:
            meta[r['Ticker']] = {
                'Status':       r.get('Status', 'active'),
                'Date_Added':   r.get('Date_Added', today_str),
                'Date_Removed': r.get('Date_Removed', '')
            }

        # All tickers ever seen = union of existing sheet cols + current DN list
        all_tickers = existing_set | current_set

        for ticker in all_tickers:
            if ticker not in meta:
                meta[ticker] = {
                    'Status':       'active' if ticker in current_set else 'legacy',
                    'Date_Added':   today_str,
                    'Date_Removed': '' if ticker in current_set else today_str
                }
            else:
                if ticker in current_set:
                    meta[ticker]['Status'] = 'active'
                    meta[ticker]['Date_Removed'] = ''
                else:
                    if meta[ticker]['Status'] != 'legacy':
                        meta[ticker]['Status'] = 'legacy'
                        meta[ticker]['Date_Removed'] = today_str

        # Write back
        header = [['Ticker', 'Status', 'Date_Added', 'Date_Removed']]
        data_rows = [[t, v['Status'], v['Date_Added'], v['Date_Removed']]
                     for t, v in sorted(meta.items())]
        ws.clear()
        ws.update('A1', header + data_rows)
        logging.info(f"Metadata sheet updated: {len(data_rows)} tickers")

    except Exception as e:
        logging.error(f"Error updating metadata sheet: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# DN vs yfinance price comparison
# ─────────────────────────────────────────────────────────────────────────────

def compare_dn_vs_yfinance(sheet, dn_snapshot: dict, prices_df: pd.DataFrame,
                            threshold=DN_PRICE_DIFF_THRESHOLD):
    """
    Compare the CURRENT live DN prices against the most recent yfinance close.

    DN Investor only provides real-time data — there is no historical price
    feed on the site.  Therefore this comparison is inherently a latest-snapshot
    check only: it tells you whether today's DN price agrees with yfinance's
    most recent weekly close.  The 'DN Comparison' tab is always fully
    overwritten on each run (not appended) because it reflects the current
    moment, not a growing time series.

    Flags rows where abs % difference > threshold (default 2 %).
    """
    today = datetime.now().strftime('%Y-%m-%d %H:%M')
    rows = [['Ticker', 'DN_Price', 'YF_Last_Close', 'Diff_NOK', 'Diff_Pct', 'Flag', 'Checked_At']]

    for ticker, snap in dn_snapshot.items():
        dn_price = snap.get('price')
        yf_price = None

        if ticker in prices_df.columns:
            col = prices_df[ticker].dropna()
            if not col.empty:
                yf_price = col.iloc[-1]

        if dn_price is None or yf_price is None:
            rows.append([ticker, dn_price or '', yf_price or '', '', '', 'NO_DATA', today])
            continue

        diff_nok = dn_price - yf_price
        diff_pct = diff_nok / yf_price if yf_price else 0
        flag = '⚠️ MISMATCH' if abs(diff_pct) > threshold else 'OK'
        rows.append([ticker,
                     round(dn_price, 2),
                     round(yf_price, 2),
                     round(diff_nok, 2),
                     f"{diff_pct*100:.2f}%",
                     flag,
                     today])

    try:
        try:
            ws = sheet.worksheet('DN Comparison')
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet('DN Comparison', rows=2000, cols=7)

        ws.clear()
        ws.update('A1', rows)
        mismatches = sum(1 for r in rows[1:] if r[5] == '⚠️ MISMATCH')
        logging.info(f"DN Comparison written — {mismatches} mismatch(s) flagged")

    except Exception as e:
        logging.error(f"Error writing DN Comparison sheet: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_format_norwegian_stocks(
        current_tickers,
        all_tickers,        # current_tickers ∪ existing sheet tickers (legacy preserved)
        since_date=None,    # if set, only fetch rows AFTER this date
        auto_adjust=True,
        validate_tickers=True,
        track_corporate_actions=True):
    """
    Fetch weekly OHLCV data for all_tickers.

    - current_tickers : tickers active on DN today  (will be fetched from yfinance)
    - all_tickers     : superset including legacy ones (columns are always created)
    - since_date      : only fetch bars after this date (incremental mode)

    Returns (prices_df, marketcap_df, corporate_actions_df)
    """
    end_date = datetime.now()

    if since_date is not None:
        if isinstance(since_date, str):
            since_date = pd.to_datetime(since_date)
        fetch_start = since_date + timedelta(days=1)   # day AFTER last known date
        logging.info(f"Incremental mode — fetching from {fetch_start.date()} to {end_date.date()}")
    else:
        fetch_start = pd.to_datetime(START_DATE)
        logging.info(f"Full mode — fetching from {fetch_start.date()} to {end_date.date()}")

    # Validate only active tickers (legacy ones are preserved as NaN)
    tickers_to_fetch = list(current_tickers)
    if validate_tickers:
        logging.info("Validating active tickers …")
        valid = [t for t in tickers_to_fetch if verify_ticker(t)]
        logging.info(f"Validated {len(valid)}/{len(tickers_to_fetch)} tickers")
        tickers_to_fetch = valid

    # Weekly date range for new rows only
    date_range = pd.date_range(
        start=fetch_start,
        end=end_date,
        freq='W-MON',
        tz='Europe/Oslo'
    )

    if len(date_range) == 0:
        logging.info("No new weeks to add — sheet is already up to date")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Build empty frames with ALL tickers (including legacy → NaN columns)
    prices_df    = pd.DataFrame(index=date_range, columns=all_tickers, dtype=float)
    marketcap_df = pd.DataFrame(index=date_range, columns=all_tickers, dtype=float)

    all_corporate_actions = []

    logging.info(f"Fetching data for {len(tickers_to_fetch)} active tickers …")
    for idx, ticker in enumerate(tickers_to_fetch, 1):
        try:
            logging.info(f"  [{idx}/{len(tickers_to_fetch)}] {ticker}")
            stock = yf.Ticker(ticker)

            # Fetch with retries
            stock_data = pd.DataFrame()
            for attempt in range(3):
                try:
                    stock_data = stock.history(
                        start=fetch_start.strftime('%Y-%m-%d'),
                        end=end_date.strftime('%Y-%m-%d'),
                        interval='1wk',
                        auto_adjust=auto_adjust,
                        actions=True
                    )
                    if not stock_data.empty:
                        break
                except Exception as fe:
                    if attempt == 2:
                        raise fe
                    logging.warning(f"    Retry {attempt+1} for {ticker}")
                    time.sleep(2)

            if stock_data.empty:
                logging.warning(f"  No data for {ticker}")
                continue

            # Normalise timezone
            if stock_data.index.tz is None:
                stock_data.index = stock_data.index.tz_localize('Europe/Oslo')
            else:
                stock_data.index = stock_data.index.tz_convert('Europe/Oslo')

            prices_df[ticker] = stock_data['Close']

            # Corporate actions
            if track_corporate_actions:
                ca = get_corporate_actions(ticker, fetch_start, end_date)
                if not ca.empty:
                    all_corporate_actions.append(ca)

            # Market cap
            try:
                shares = stock.info.get('sharesOutstanding')
                if shares:
                    marketcap_df[ticker] = stock_data['Close'] * shares
            except Exception:
                pass

            time.sleep(0.8)

        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            continue

    # Strip timezone from index
    prices_df.index    = prices_df.index.tz_localize(None)
    marketcap_df.index = marketcap_df.index.tz_localize(None)

    if all_corporate_actions:
        corp_df = pd.concat(all_corporate_actions, ignore_index=True)
        corp_df = corp_df.sort_values(['Date', 'Ticker'])
        if not corp_df.empty and hasattr(corp_df['Date'].iloc[0], 'tz'):
            corp_df['Date'] = corp_df['Date'].dt.tz_localize(None)
    else:
        corp_df = pd.DataFrame(columns=['Date', 'Ticker', 'Action_Type', 'Details', 'Value'])

    logging.info(f"Fetch complete — {len(date_range)} new week(s), "
                 f"{corp_df.shape[0]} corporate actions")
    return prices_df, marketcap_df, corp_df


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets — write (incremental append or full initial load)
# ─────────────────────────────────────────────────────────────────────────────

def _df_to_rows(df, value_scale=1.0, decimals=2):
    """Convert a DataFrame to a list-of-lists for gspread, with date in col 0."""
    rows = []
    for idx, row in df.iterrows():
        date_str = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)
        vals = []
        for v in row:
            if pd.notna(v):
                vals.append(round(float(v) * value_scale, decimals))
            else:
                vals.append('')
        rows.append([date_str] + vals)
    return rows


def update_google_sheet(sheet, prices_df, marketcap_df, corporate_actions_df,
                        all_tickers, is_initial_load=False):
    """
    Write data to Google Sheets.
    - is_initial_load=True  → clears and rewrites everything
    - is_initial_load=False → appends only new rows (the df already contains
                              only post-latest_date data)
    """
    header = ['Date'] + list(all_tickers)

    for ws_name, df, scale in [('Prices', prices_df, 1.0),
                                ('Market Caps', marketcap_df, 1e-9)]:  # mktcap in billions
        try:
            try:
                ws = sheet.worksheet(ws_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sheet.add_worksheet(ws_name, rows=5000, cols=len(header) + 5)

            if is_initial_load:
                ws.clear()
                ws.update('A1', [header])

            if df.empty:
                logging.info(f"{ws_name}: nothing new to append")
                continue

            # Re-order columns in df to match header (place NaN for missing)
            df_ordered = df.reindex(columns=all_tickers)
            rows = _df_to_rows(df_ordered, value_scale=scale, decimals=2)

            if is_initial_load:
                ws.append_rows(rows, value_input_option='USER_ENTERED')
            else:
                ws.append_rows(rows, value_input_option='USER_ENTERED')

            logging.info(f"  {ws_name}: appended {len(rows)} row(s)")

        except Exception as e:
            logging.error(f"Error writing {ws_name}: {e}")

    # Corporate Actions (always append new actions)
    if not corporate_actions_df.empty:
        try:
            try:
                ca_ws = sheet.worksheet('Corporate Actions')
                if is_initial_load:
                    ca_ws.clear()
                    ca_ws.update('A1', [['Date', 'Ticker', 'Action_Type', 'Details', 'Value']])
            except gspread.exceptions.WorksheetNotFound:
                ca_ws = sheet.add_worksheet('Corporate Actions', rows=5000, cols=5)
                ca_ws.update('A1', [['Date', 'Ticker', 'Action_Type', 'Details', 'Value']])

            ca_rows = []
            for _, row in corporate_actions_df.iterrows():
                ca_rows.append([
                    row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date']),
                    row['Ticker'],
                    row['Action_Type'],
                    row['Details'],
                    round(float(row['Value']), 4)
                ])
            ca_ws.append_rows(ca_rows, value_input_option='USER_ENTERED')
            logging.info(f"  Corporate Actions: appended {len(ca_rows)} event(s)")

        except Exception as e:
            logging.error(f"Error writing Corporate Actions: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logging.info("=" * 65)
    logging.info("OSE Stock Data Fetcher — Incremental + DN-comparison mode")
    logging.info("=" * 65)

    # ── 1. Scrape DN for current constituents and live prices ──────────────
    current_tickers, dn_snapshot = get_dn_full_snapshot(use_headless=True)
    logging.info(f"DN constituents: {len(current_tickers)} tickers")

    # ── 2. Connect to Google Sheets ────────────────────────────────────────
    client = setup_google_sheets()
    sheet  = client.open_by_key(SPREADSHEET_ID)

    # ── 3. Read current sheet state ────────────────────────────────────────
    latest_date, existing_tickers = read_sheet_state(sheet)
    is_initial_load = (latest_date is None)

    # ── 4. Sync columns (add new tickers, never remove legacy) ────────────
    all_tickers = sync_columns(sheet, current_tickers, existing_tickers)
    # all_tickers is now: original_existing + any_new_ones

    # Ensure current tickers are all in all_tickers
    # (handles very first run where existing_tickers is empty)
    for t in current_tickers:
        if t not in all_tickers:
            all_tickers.append(t)

    # ── 5. Determine legacy tickers ────────────────────────────────────────
    current_set = set(current_tickers)

    # ── 6. Fetch new price data ────────────────────────────────────────────
    prices_df, marketcap_df, corp_df = fetch_and_format_norwegian_stocks(
        current_tickers=current_tickers,
        all_tickers=all_tickers,
        since_date=latest_date,           # None → full load
        auto_adjust=True,
        validate_tickers=True,
        track_corporate_actions=True
    )

    # ── 7. Write to Google Sheets ──────────────────────────────────────────
    if not prices_df.empty or is_initial_load:
        update_google_sheet(
            sheet, prices_df, marketcap_df, corp_df,
            all_tickers=all_tickers,
            is_initial_load=is_initial_load
        )
    else:
        logging.info("Sheet already up to date — no rows written")

    # ── 8. Update Metadata tab (active vs legacy status) ──────────────────
    update_metadata_sheet(sheet, current_tickers, existing_tickers)

    # ── 9. DN vs yfinance comparison ──────────────────────────────────────
    compare_dn_vs_yfinance(sheet, dn_snapshot, prices_df)

    # ── 10. Log summary ───────────────────────────────────────────────────
    legacy_count = len([t for t in all_tickers if t not in current_set])
    logging.info("=" * 65)
    logging.info("Run complete")
    logging.info(f"  Active tickers  : {len(current_tickers)}")
    logging.info(f"  Legacy tickers  : {legacy_count}")
    logging.info(f"  New rows added  : {len(prices_df)}")
    logging.info(f"  Corporate events: {len(corp_df)}")
    logging.info("=" * 65)


if __name__ == "__main__":
    main()
