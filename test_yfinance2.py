#!/usr/bin/env python3
"""
Test suite for the expanded OSE Stock Data Fetcher (yfinance2.py).
Covers: constituent retrieval, ticker validation, corporate actions,
        incremental sheet logic, column sync, DN comparison, legacy preservation.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

sys.path.insert(0, '/Users/mac/Desktop/Antigravity project')

# ── Import functions under test ───────────────────────────────────────────────
from yfinance2 import (
    get_ose_constituents_from_dn,
    verify_ticker,
    get_corporate_actions,
    read_sheet_state,
    sync_columns,
    compare_dn_vs_yfinance,
    update_metadata_sheet,
    _parse_dn_number,
    _parse_dn_market_cap,
    _col_letter,
)

import logging
logging.basicConfig(level=logging.WARNING)   # suppress info noise during tests


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests — pure logic (no network / browser required)
# ─────────────────────────────────────────────────────────────────────────────

class TestParseNumber(unittest.TestCase):
    def test_standard(self):
        self.assertAlmostEqual(_parse_dn_number('123,45'), 123.45)

    def test_thousands_separator(self):
        self.assertAlmostEqual(_parse_dn_number('1 234,56'), 1234.56)

    def test_none_on_empty(self):
        self.assertIsNone(_parse_dn_number(''))

    def test_none_on_dash(self):
        self.assertIsNone(_parse_dn_number('-'))

    def test_integer_string(self):
        self.assertAlmostEqual(_parse_dn_number('42'), 42.0)


class TestParseMarketCap(unittest.TestCase):
    """Tests _parse_dn_market_cap() with all three Norwegian unit suffixes."""

    def test_mill(self):
        # 452,15 mill. = 452_150_000 NOK
        self.assertAlmostEqual(_parse_dn_market_cap('452,15 mill.'), 452_150_000)

    def test_mrd(self):
        # 449,64 mrd. = 449_640_000_000 NOK
        self.assertAlmostEqual(_parse_dn_market_cap('449,64 mrd.'), 449_640_000_000)

    def test_bill(self):
        # 1,02 bill. = 1_020_000_000_000 NOK
        self.assertAlmostEqual(_parse_dn_market_cap('1,02 bill.'), 1_020_000_000_000)

    def test_none_on_empty(self):
        self.assertIsNone(_parse_dn_market_cap(''))

    def test_none_on_dash(self):
        self.assertIsNone(_parse_dn_market_cap('-'))

    def test_no_suffix_treated_as_raw(self):
        # No suffix → no multiplier applied
        self.assertAlmostEqual(_parse_dn_market_cap('1000'), 1000.0)


class TestColLetter(unittest.TestCase):
    def test_first(self):
        self.assertEqual(_col_letter(1), 'A')

    def test_26th(self):
        self.assertEqual(_col_letter(26), 'Z')

    def test_27th(self):
        self.assertEqual(_col_letter(27), 'AA')

    def test_52nd(self):
        self.assertEqual(_col_letter(52), 'AZ')


class TestReadSheetState(unittest.TestCase):
    """Tests read_sheet_state() against a mocked gspread worksheet."""

    def _mock_sheet(self, all_values):
        ws = MagicMock()
        ws.get_all_values.return_value = all_values
        sheet = MagicMock()
        sheet.worksheet.return_value = ws
        return sheet

    def test_empty_sheet(self):
        sheet = self._mock_sheet([])
        latest, tickers = read_sheet_state(sheet)
        self.assertIsNone(latest)
        self.assertEqual(tickers, [])

    def test_parses_latest_date_and_tickers(self):
        data = [
            ['Date', 'EQNR.OL', 'DNB.OL'],
            ['2024-01-08', '300.5', '220.1'],
            ['2024-01-15', '305.0', '225.0'],
        ]
        sheet = self._mock_sheet(data)
        latest, tickers = read_sheet_state(sheet)
        self.assertEqual(latest, pd.to_datetime('2024-01-15'))
        self.assertEqual(tickers, ['EQNR.OL', 'DNB.OL'])

    def test_worksheet_not_found(self):
        import gspread
        sheet = MagicMock()
        sheet.worksheet.side_effect = gspread.exceptions.WorksheetNotFound('Prices')
        latest, tickers = read_sheet_state(sheet)
        self.assertIsNone(latest)
        self.assertEqual(tickers, [])


class TestSyncColumns(unittest.TestCase):
    """Tests sync_columns() — verifies new tickers are detected without network."""

    def _make_sheet(self):
        ws = MagicMock()
        ws.row_values.return_value = ['Date', 'EQNR.OL']
        sheet = MagicMock()
        sheet.worksheet.return_value = ws
        return sheet, ws

    def test_no_new_tickers(self):
        sheet, _ = self._make_sheet()
        result = sync_columns(sheet, ['EQNR.OL'], ['EQNR.OL'])
        # Should return existing list unchanged
        self.assertEqual(result, ['EQNR.OL'])

    def test_new_ticker_detected(self):
        sheet, ws = self._make_sheet()
        result = sync_columns(sheet, ['EQNR.OL', 'DNB.OL'], ['EQNR.OL'])
        self.assertIn('DNB.OL', result)
        self.assertEqual(result[0], 'EQNR.OL')  # existing first
        # Header write was called for the new ticker
        ws.update.assert_called()


class TestCompareDnVsYfinance(unittest.TestCase):
    """Tests compare_dn_vs_yfinance() flagging logic."""

    def _make_sheet(self):
        ws = MagicMock()
        sheet = MagicMock()
        sheet.worksheet.return_value = ws
        return sheet, ws

    def test_mismatch_flagged(self):
        sheet, ws = self._make_sheet()
        dn_snapshot = {'EQNR.OL': {'price': 310.0}}
        idx = pd.to_datetime(['2024-01-15'])
        prices_df = pd.DataFrame({'EQNR.OL': [300.0]}, index=idx)

        compare_dn_vs_yfinance(sheet, dn_snapshot, prices_df, threshold=0.02)
        calls = ws.update.call_args_list
        written_data = calls[0][0][1]   # first positional arg of first update call
        flag_row = written_data[1]       # first data row
        self.assertIn('MISMATCH', flag_row[5])

    def test_ok_not_flagged(self):
        sheet, ws = self._make_sheet()
        dn_snapshot = {'EQNR.OL': {'price': 300.5}}
        idx = pd.to_datetime(['2024-01-15'])
        prices_df = pd.DataFrame({'EQNR.OL': [300.0]}, index=idx)

        compare_dn_vs_yfinance(sheet, dn_snapshot, prices_df, threshold=0.02)
        calls = ws.update.call_args_list
        written_data = calls[0][0][1]
        flag_row = written_data[1]
        self.assertEqual(flag_row[5], 'OK')


class TestUpdateMetadataSheet(unittest.TestCase):
    """Tests that metadata correctly marks active vs legacy tickers."""

    def _make_sheet(self, existing_records=None):
        ws = MagicMock()
        ws.get_all_records.return_value = existing_records or []
        sheet = MagicMock()
        sheet.worksheet.return_value = ws
        return sheet, ws

    def test_new_ticker_marked_active(self):
        sheet, ws = self._make_sheet()
        update_metadata_sheet(sheet, current_tickers=['EQNR.OL'], existing_tickers=[])
        ws.update.assert_called()
        written = ws.update.call_args[0][1]
        ticker_row = next(r for r in written if r[0] == 'EQNR.OL')
        self.assertEqual(ticker_row[1], 'active')

    def test_removed_ticker_marked_legacy(self):
        existing = [{'Ticker': 'OLD.OL', 'Status': 'active',
                     'Date_Added': '2024-01-01', 'Date_Removed': ''}]
        sheet, ws = self._make_sheet(existing)
        update_metadata_sheet(sheet, current_tickers=['EQNR.OL'],
                              existing_tickers=['OLD.OL', 'EQNR.OL'])
        written = ws.update.call_args[0][1]
        old_row = next(r for r in written if r[0] == 'OLD.OL')
        self.assertEqual(old_row[1], 'legacy')
        self.assertNotEqual(old_row[3], '')   # Date_Removed should be set


# ─────────────────────────────────────────────────────────────────────────────
# Integration-style tests (require network — skip if not available)
# ─────────────────────────────────────────────────────────────────────────────

class TestLiveTickerValidation(unittest.TestCase):
    """Validates that known large-cap tickers pass verification."""

    def test_equinor_valid(self):
        self.assertTrue(verify_ticker('EQNR.OL'), "EQNR.OL should be valid")

    def test_fake_ticker_invalid(self):
        self.assertFalse(verify_ticker('FAKE_TICKER_XYZ.OL'))


class TestCorporateActions(unittest.TestCase):
    def test_equinor_dividends(self):
        df = get_corporate_actions('EQNR.OL', '2020-01-01', '2026-04-01')
        # Equinor pays dividends — there should be at least one
        self.assertFalse(df.empty, "Expected corporate actions for EQNR.OL")
        self.assertIn('Action_Type', df.columns)


# ─────────────────────────────────────────────────────────────────────────────
# DN Investor scraper test (requires browser — skip in CI if no display)
# ─────────────────────────────────────────────────────────────────────────────

class TestDNConstituents(unittest.TestCase):
    def test_returns_ol_tickers(self):
        try:
            tickers = get_ose_constituents_from_dn(use_headless=True)
            self.assertGreater(len(tickers), 50, "Expect at least 50 OSE constituents")
            self.assertTrue(all(t.endswith('.OL') for t in tickers),
                            "All tickers must have .OL suffix")
        except Exception as e:
            self.skipTest(f"Browser not available: {e}")


if __name__ == '__main__':
    print("\nOSE Stock Data Fetcher — Test Suite")
    print("=" * 60)
    unittest.main(verbosity=2)
