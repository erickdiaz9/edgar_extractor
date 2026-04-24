"""
SEC Filings Explorer
────────────────────
Browse and download 10-K, 10-Q, and JSON filings from SEC EDGAR.
Explore financial KPIs with interactive charts — powered by SEC EDGAR XBRL data.
Deployed via Streamlit Cloud — no server or API key needed.
"""

import io
import os
import zipfile

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC Filings Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "SEC Filings Explorer — powered by SEC EDGAR public APIs"},
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Tighten top padding */
.block-container { padding-top: 1.5rem !important; }

/* File row cards */
.file-card {
    background: #f8fafc;
    border: 1.5px solid #e2e8f0;
    border-radius: 8px;
    padding: 14px 16px;
    margin-bottom: 8px;
    transition: border-color .15s;
}
.file-card:hover { border-color: #93c5fd; }

/* Colored form badges */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 100px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .04em;
}
.b-10k  { background: #dbeafe; color: #1e40af; }
.b-10q  { background: #ede9fe; color: #5b21b6; }
.b-json { background: #d1fae5; color: #065f46; }

/* Column header text */
.col-hdr {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .07em;
    color: #94a3b8;
}

/* Tight horizontal rules */
.row-sep { border: none; border-top: 1px solid #f1f5f9; margin: 3px 0 6px; }

/* Downloaded indicator */
.dl-tick { color: #059669; font-weight: 700; font-size: 12px; }

/* Landing hero */
.hero {
    text-align: center;
    padding: 90px 0 60px;
}
.hero h2 { font-size: 2rem; font-weight: 800; color: #0f172a; margin-bottom: .5rem; }
.hero p  { color: #64748b; max-width: 500px; margin: 0 auto; font-size: 1rem; line-height: 1.6; }
.hero .hint { color: #94a3b8; font-size: .875rem; margin-top: 1.5rem; }

/* KPI metric cards */
.kpi-card {
    background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
    border: 1px solid #bae6fd;
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    margin-bottom: 8px;
}
.kpi-card .kpi-label { font-size: 11px; font-weight: 700; color: #0369a1; text-transform: uppercase; letter-spacing: .06em; }
.kpi-card .kpi-value { font-size: 1.7rem; font-weight: 800; color: #0c4a6e; margin: 6px 0 4px; }
.kpi-card .kpi-sub   { font-size: 11px; color: #0284c7; }
.kpi-card-green { background: linear-gradient(135deg,#f0fdf4,#dcfce7); border-color:#86efac; }
.kpi-card-green .kpi-label { color:#15803d; }
.kpi-card-green .kpi-value { color:#14532d; }
.kpi-card-green .kpi-sub   { color:#16a34a; }
.kpi-card-red { background: linear-gradient(135deg,#fff1f2,#ffe4e6); border-color:#fca5a5; }
.kpi-card-red .kpi-label { color:#b91c1c; }
.kpi-card-red .kpi-value { color:#7f1d1d; }
.kpi-card-red .kpi-sub   { color:#dc2626; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
HEADERS = {"User-Agent": "SEC-Filings-Explorer streamlit@secexplorer.app"}

FOLDER_INFO = {
    "10-K": {"icon": "📑", "label": "10-K Annual Reports",    "badge": "b-10k"},
    "10-Q": {"icon": "📋", "label": "10-Q Quarterly Reports", "badge": "b-10q"},
    "JSON": {"icon": "📦", "label": "JSON & XBRL Data",       "badge": "b-json"},
}

BADGE_STYLE = {
    "10-K": "background:#dbeafe;color:#1e40af",
    "10-Q": "background:#ede9fe;color:#5b21b6",
    "JSON": "background:#d1fae5;color:#065f46",
}

# Popular KPIs shown at the top of the selector
POPULAR_KPIS = [
    "us-gaap/Revenues",
    "us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax",
    "us-gaap/NetIncomeLoss",
    "us-gaap/GrossProfit",
    "us-gaap/OperatingIncomeLoss",
    "us-gaap/Assets",
    "us-gaap/AssetsCurrent",
    "us-gaap/StockholdersEquity",
    "us-gaap/LiabilitiesAndStockholdersEquity",
    "us-gaap/CashAndCashEquivalentsAtCarryingValue",
    "us-gaap/LongTermDebt",
    "us-gaap/EarningsPerShareBasic",
    "us-gaap/EarningsPerShareDiluted",
    "us-gaap/CommonStockSharesOutstanding",
    "us-gaap/ResearchAndDevelopmentExpense",
    "us-gaap/OperatingLeaseRightOfUseAsset",
    "dei/EntityPublicFloat",
]

CHART_COLORS = [
    "#3b82f6", "#ef4444", "#10b981", "#f59e0b",
    "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16",
]

# ── Standardized XBRL tag mappings ────────────────────────────────────────────
# For each canonical metric, list XBRL tags in priority order.
# The first tag that yields annual FY data wins.
METRIC_TAGS: dict[str, list[str]] = {
    # Income Statement
    "Revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet", "SalesRevenueGoodsNet",
        "RevenuesNetOfInterestExpense",
        "HealthCareOrganizationRevenue",
        "RealEstateRevenueNet", "OilAndGasRevenue",
    ],
    "Gross Profit":      ["GrossProfit"],
    "Operating Income":  ["OperatingIncomeLoss"],
    "Net Income": [
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "ProfitLoss",
    ],
    "EPS Diluted": [
        "EarningsPerShareDiluted",
        "EarningsPerShareBasic",
    ],
    "EPS Basic":   ["EarningsPerShareBasic"],
    "Diluted Shares": [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",  # dei namespace, very broadly available
    ],
    # Balance Sheet
    "Total Assets": ["Assets"],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
        "CashAndDueFromBanks",
    ],
    "Accounts Receivable": [
        "AccountsReceivableNetCurrent", "ReceivablesNetCurrent",
        "AccountsReceivableNet",
    ],
    "Inventory":       ["InventoryNet", "InventoryGross"],
    "Current Assets":  ["AssetsCurrent"],
    "PP&E Net":        ["PropertyPlantAndEquipmentNet"],
    "Goodwill":        ["Goodwill"],
    "Intangibles": [
        "IntangibleAssetsNetExcludingGoodwill",
        "FiniteLivedIntangibleAssetsNet",
    ],
    "Total Liabilities":   ["Liabilities"],
    "Current Liabilities": ["LiabilitiesCurrent"],
    "Long Term Debt": [
        "LongTermDebt", "LongTermDebtNoncurrent", "LongTermNotesPayable",
    ],
    "Total Equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "PartnersCapital",
    ],
    # Cash Flow Statement
    "Operating Cash Flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "CapEx": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireProductiveAssets",
    ],
    "D&A": [
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "Depreciation",
    ],
    "Share Repurchases": [
        "PaymentsForRepurchaseOfCommonStock",
        "TreasuryStockValueAcquiredCostMethod",
    ],
    "Dividends Paid": [
        "PaymentsOfDividendsCommonStock",
        "PaymentsOfDividends",
    ],
    # ── Extended Income Statement (core financial ontology) ────────────────────
    "COGS": [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfServices",
        "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
    ],
    "SG&A": [
        "SellingGeneralAndAdministrativeExpense",
        "GeneralAndAdministrativeExpense",
        "SellingAndMarketingExpense",
    ],
    "R&D": [
        "ResearchAndDevelopmentExpense",
        "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
    ],
    "Interest Expense": [
        "InterestExpense",
        "InterestAndDebtExpense",
        "InterestExpenseDebt",
        "InterestExpenseRelatedParty",
    ],
    "Income Tax": [
        "IncomeTaxExpenseBenefit",
        "CurrentIncomeTaxExpenseBenefit",
        "DeferredIncomeTaxExpenseBenefit",
    ],
    # ── Extended Balance Sheet ──────────────────────────────────────────────────
    "Retained Earnings": [
        "RetainedEarningsAccumulatedDeficit",
        "RetainedEarningsUnappropriated",
    ],
    "Accounts Payable": [
        "AccountsPayableCurrent",
        "AccountsPayableTradeCurrent",
        "AccountsPayableCurrentAndNoncurrent",
    ],
    # ── Extended Cash Flow ─────────────────────────────────────────────────────
    "Investing CF": [
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations",
    ],
    "Financing CF": [
        "NetCashProvidedByUsedInFinancingActivities",
        "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations",
    ],
    "Acquisitions": [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
        "PaymentsToAcquireBusinessesAndInterestInAffiliates",
    ],
    "Debt Issued": [
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromIssuanceOfDebt",
        "ProceedsFromDebtNetOfIssuanceCosts",
        "ProceedsFromBorrowings",
    ],
    "Debt Repaid": [
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfDebt",
        "RepaymentsOfLongTermDebtAndCapitalSecurities",
    ],
    "Change in Cash": [
        "CashAndCashEquivalentsPeriodIncreaseDecrease",
        "CashCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect",
        "NetCashProvidedByUsedInContinuingOperations",
    ],
}

INCOME_METRICS   = [
    "Revenue", "COGS", "Gross Profit",
    "SG&A", "R&D",
    "Operating Income",
    "Interest Expense", "Income Tax",
    "Net Income",
    "EPS Diluted", "EPS Basic", "Diluted Shares",
]
BALANCE_METRICS  = [
    "Total Assets", "Current Assets", "Cash",
    "Accounts Receivable", "Inventory",
    "PP&E Net", "Goodwill", "Intangibles",
    "Total Liabilities", "Current Liabilities",
    "Long Term Debt", "Accounts Payable",
    "Total Equity", "Retained Earnings",
]
CASHFLOW_METRICS = [
    "Operating Cash Flow", "Investing CF", "Financing CF",
    "CapEx", "Acquisitions",
    "D&A", "Share Repurchases", "Dividends Paid",
    "Debt Issued", "Debt Repaid",
    "Change in Cash",
]
DERIVED_METRICS  = [
    "Gross Margin", "Operating Margin", "Net Margin",
    "EBITDA", "EBITDA Margin",
    "Revenue Growth", "Net Income Growth",
    "FCF", "FCF Margin", "ROA", "ROE",
    "Net Debt", "Debt/Equity", "Debt/Assets",
    # Price-based (populated when yfinance data is available)
    "Market Cap", "P/E Ratio", "Price/Book", "Dividend Yield",
]
ALL_STD_METRICS  = INCOME_METRICS + BALANCE_METRICS + CASHFLOW_METRICS + DERIVED_METRICS

# Display metadata: fmt codes = usd_b | usd_share | shares_b | pct | pct_signed | ratio
# negate=True  → flip sign before display (CapEx, dividends are stored as positive outflows)
# key=True     → bold + highlight row in the table
METRIC_DISPLAY: dict[str, dict] = {
    "Revenue":             {"fmt": "usd_b",    "key": True},
    "Gross Profit":        {"fmt": "usd_b"},
    "Operating Income":    {"fmt": "usd_b"},
    "Net Income":          {"fmt": "usd_b",    "key": True},
    "EPS Diluted":         {"fmt": "usd_share"},
    "EPS Basic":           {"fmt": "usd_share"},
    "Diluted Shares":      {"fmt": "shares_b"},
    "Total Assets":        {"fmt": "usd_b",    "key": True},
    "Cash":                {"fmt": "usd_b"},
    "Accounts Receivable": {"fmt": "usd_b"},
    "Inventory":           {"fmt": "usd_b"},
    "Current Assets":      {"fmt": "usd_b"},
    "PP&E Net":            {"fmt": "usd_b"},
    "Goodwill":            {"fmt": "usd_b"},
    "Intangibles":         {"fmt": "usd_b"},
    "Total Liabilities":   {"fmt": "usd_b"},
    "Current Liabilities": {"fmt": "usd_b"},
    "Long Term Debt":      {"fmt": "usd_b"},
    "Total Equity":        {"fmt": "usd_b",    "key": True},
    "Operating Cash Flow": {"fmt": "usd_b",    "key": True},
    "Investing CF":        {"fmt": "usd_b"},
    "Financing CF":        {"fmt": "usd_b"},
    "CapEx":               {"fmt": "usd_b",    "negate": True},
    "Acquisitions":        {"fmt": "usd_b",    "negate": True},
    "D&A":                 {"fmt": "usd_b"},
    "Share Repurchases":   {"fmt": "usd_b",    "negate": True},
    "Dividends Paid":      {"fmt": "usd_b"},
    "Debt Issued":         {"fmt": "usd_b"},
    "Debt Repaid":         {"fmt": "usd_b",    "negate": True},
    "Change in Cash":      {"fmt": "usd_b"},
    # Extended Income Statement
    "COGS":                {"fmt": "usd_b"},
    "SG&A":                {"fmt": "usd_b"},
    "R&D":                 {"fmt": "usd_b"},
    "Interest Expense":    {"fmt": "usd_b"},
    "Income Tax":          {"fmt": "usd_b"},
    # Extended Balance Sheet
    "Retained Earnings":   {"fmt": "usd_b"},
    "Accounts Payable":    {"fmt": "usd_b"},
    # Derived metrics
    "Gross Margin":        {"fmt": "pct"},
    "Operating Margin":    {"fmt": "pct"},
    "Net Margin":          {"fmt": "pct"},
    "EBITDA":              {"fmt": "usd_b",    "key": True},
    "EBITDA Margin":       {"fmt": "pct"},
    "Revenue Growth":      {"fmt": "pct_signed"},
    "Net Income Growth":   {"fmt": "pct_signed"},
    "FCF":                 {"fmt": "usd_b",    "key": True},
    "FCF Margin":          {"fmt": "pct"},
    "ROA":                 {"fmt": "pct"},
    "ROE":                 {"fmt": "pct"},
    "Net Debt":            {"fmt": "usd_b"},
    "Debt/Equity":         {"fmt": "ratio"},
    "Debt/Assets":         {"fmt": "ratio"},
    # Price-based derived metrics
    "Market Cap":          {"fmt": "usd_b",    "key": True},
    "P/E Ratio":           {"fmt": "ratio"},
    "Price/Book":          {"fmt": "ratio"},
    "Dividend Yield":      {"fmt": "pct"},
}

# ── Methodology documentation (shown in the 📖 Methodology tab) ───────────────
METHODOLOGY_MD = """
## 📖 Financial Metrics — Methodology Reference

This reference explains every metric shown in **Standardized Financials**, including
the formula used and the primary SEC EDGAR XBRL tags queried.

> **Tag resolution**: For each metric, XBRL tags are tried **in priority order**.
> The first tag that yields valid annual data wins.
> All financial data comes from the SEC EDGAR `companyfacts` API.
> Stock prices are fetched via **yfinance** (last monthly close per calendar year).
> All dollar figures are shown in USD Billions (B) unless noted.

---

### 📈 Income Statement

| Metric | Formula / Source | Primary XBRL Tags | Notes |
|:-------|:----------------|:------------------|:------|
| **Revenue** | Direct from XBRL | `Revenues` · `RevenueFromContractWithCustomerExcludingAssessedTax` · `SalesRevenueNet` · `RevenuesNetOfInterestExpense` | Net revenues / net sales. 8 tags tried. |
| **COGS** | Direct from XBRL | `CostOfRevenue` · `CostOfGoodsAndServicesSold` · `CostOfGoodsSold` | Cost of goods/services. Revenue − COGS = Gross Profit. |
| **Gross Profit** | Direct from XBRL *(≈ Revenue − COGS)* | `GrossProfit` | Sourced directly from XBRL when available. |
| **SG&A** | Direct from XBRL | `SellingGeneralAndAdministrativeExpense` · `GeneralAndAdministrativeExpense` | Selling, general & administrative overhead. |
| **R&D** | Direct from XBRL | `ResearchAndDevelopmentExpense` | Research & development investment. |
| **Operating Income** | Direct from XBRL *(≈ Gross Profit − SG&A − R&D − D&A)* | `OperatingIncomeLoss` | Also known as **EBIT** (Earnings Before Interest & Taxes). |
| **Interest Expense** | Direct from XBRL | `InterestExpense` · `InterestAndDebtExpense` | Cost of debt financing. Used in EBITDA fallback. |
| **Income Tax** | Direct from XBRL | `IncomeTaxExpenseBenefit` | Tax provision (positive = expense). Used in EBITDA fallback. |
| **Net Income** | Direct from XBRL | `NetIncomeLoss` · `NetIncomeLossAvailableToCommonStockholdersBasic` · `ProfitLoss` | After-tax bottom-line profit. |
| **EPS Diluted** | Direct *(or Net Income ÷ Diluted Shares)* | `EarningsPerShareDiluted` · `EarningsPerShareBasic` | Computed ratio if direct tag is absent. |
| **EPS Basic** | Direct from XBRL | `EarningsPerShareBasic` | Undiluted earnings per share. |
| **Diluted Shares** | Direct from XBRL | `WeightedAverageNumberOfDilutedSharesOutstanding` · `WeightedAverageNumberOfSharesOutstandingBasic` · `CommonStockSharesOutstanding` · `EntityCommonStockSharesOutstanding` | Weighted-average diluted share count; EPS denominator. |

---

### 🏦 Balance Sheet

| Metric | Formula / Source | Primary XBRL Tags | Notes |
|:-------|:----------------|:------------------|:------|
| **Total Assets** | Direct from XBRL | `Assets` | All assets at period end. |
| **Current Assets** | Direct from XBRL | `AssetsCurrent` | Convertible to cash within 12 months. |
| **Cash** | Direct from XBRL | `CashAndCashEquivalentsAtCarryingValue` · `CashCashEquivalentsAndShortTermInvestments` | Unrestricted cash & equivalents. |
| **Accounts Receivable** | Direct from XBRL | `AccountsReceivableNetCurrent` · `ReceivablesNetCurrent` | Customer receivables, net of allowances. |
| **Inventory** | Direct from XBRL | `InventoryNet` · `InventoryGross` | Goods held for sale. |
| **PP&E Net** | Direct from XBRL | `PropertyPlantAndEquipmentNet` | Fixed assets net of accumulated depreciation. |
| **Goodwill** | Direct from XBRL | `Goodwill` | Premium paid over fair value in acquisitions. |
| **Intangibles** | Direct from XBRL | `IntangibleAssetsNetExcludingGoodwill` · `FiniteLivedIntangibleAssetsNet` | Patents, trademarks, customer lists, etc. |
| **Total Liabilities** | Direct from XBRL | `Liabilities` | All obligations at period end. |
| **Current Liabilities** | Direct from XBRL | `LiabilitiesCurrent` | Due within 12 months. |
| **Long Term Debt** | Direct from XBRL | `LongTermDebt` · `LongTermDebtNoncurrent` · `LongTermNotesPayable` | Debt maturing beyond 12 months. |
| **Accounts Payable** | Direct from XBRL | `AccountsPayableCurrent` · `AccountsPayableTradeCurrent` | Amounts owed to suppliers. |
| **Total Equity** | Direct from XBRL | `StockholdersEquity` · `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | Shareholders' book value. |
| **Retained Earnings** | Direct from XBRL | `RetainedEarningsAccumulatedDeficit` | Cumulative profits retained *(negative = accumulated deficit)*. |

---

### 💵 Cash Flow Statement

| Metric | Formula / Source | Primary XBRL Tags | Notes |
|:-------|:----------------|:------------------|:------|
| **Operating Cash Flow** | Direct from XBRL | `NetCashProvidedByUsedInOperatingActivities` | Cash from core operations (indirect method). |
| **Investing CF** | Direct from XBRL | `NetCashProvidedByUsedInInvestingActivities` | Net cash used in investing (usually negative). |
| **Financing CF** | Direct from XBRL | `NetCashProvidedByUsedInFinancingActivities` | Net cash from debt/equity activity. |
| **CapEx** | Direct *(displayed as outflow)* | `PaymentsToAcquirePropertyPlantAndEquipment` | Capital expenditures. XBRL positive → shown negative. |
| **Acquisitions** | Direct *(displayed as outflow)* | `PaymentsToAcquireBusinessesNetOfCashAcquired` | M&A cash payments, net of acquired cash. |
| **D&A** | Direct from XBRL | `DepreciationDepletionAndAmortization` · `DepreciationAndAmortization` · `Depreciation` | Non-cash add-back. Also the D&A component of EBITDA. |
| **Share Repurchases** | Direct *(displayed as outflow)* | `PaymentsForRepurchaseOfCommonStock` | Stock buybacks. XBRL positive → shown negative. |
| **Dividends Paid** | Direct from XBRL | `PaymentsOfDividendsCommonStock` · `PaymentsOfDividends` | Cash dividends *(positive = income to investor)*. |
| **Debt Issued** | Direct from XBRL | `ProceedsFromIssuanceOfLongTermDebt` · `ProceedsFromIssuanceOfDebt` | New debt proceeds (inflow). |
| **Debt Repaid** | Direct *(displayed as outflow)* | `RepaymentsOfLongTermDebt` · `RepaymentsOfDebt` | Principal repayments. XBRL positive → shown negative. |
| **Change in Cash** | Direct from XBRL | `CashAndCashEquivalentsPeriodIncreaseDecrease` | Net: Operating CF + Investing CF + Financing CF. |

---

### 📐 Derived Metrics

| Metric | Formula | Inputs | Notes |
|:-------|:--------|:-------|:------|
| **Gross Margin** | Gross Profit ÷ Revenue | Income | Raw product/service profitability. |
| **Operating Margin** | Operating Income ÷ Revenue | Income | Efficiency after all operating costs *(EBIT Margin)*. |
| **Net Margin** | Net Income ÷ Revenue | Income | After-tax bottom-line profitability. |
| **EBITDA** | *Method 1:* Operating Income + D&A | Income + CF | Non-GAAP proxy for cash earnings. Method 1 preferred. |
|  | *Fallback:* Net Income + Interest + Tax + D&A | Income + CF | Used when Operating Income unavailable. |
| **EBITDA Margin** | EBITDA ÷ Revenue | Derived | Sector-neutral profitability benchmark. |
| **Revenue Growth** | Revenue_t ÷ Revenue_{t−1} − 1 | Income | Year-over-year top-line growth rate. |
| **Net Income Growth** | Net Income_t ÷ Net Income_{t−1} − 1 | Income | Year-over-year bottom-line growth. |
| **FCF** | Operating Cash Flow − CapEx | Cash Flow | Cash left after maintaining the asset base. |
| **FCF Margin** | FCF ÷ Revenue | Derived | Cash conversion efficiency. |
| **ROA** | Net Income ÷ Total Assets | Income + Balance | Profit generated per dollar of assets. |
| **ROE** | Net Income ÷ Total Equity | Income + Balance | Return on shareholders' book equity. |
| **Net Debt** | Long-Term Debt − Cash | Balance | Leverage net of liquidity *(negative = net cash)*. |
| **Debt/Equity** | Long-Term Debt ÷ Total Equity | Balance | Financial leverage ratio. |
| **Debt/Assets** | Long-Term Debt ÷ Total Assets | Balance | Balance-sheet leverage as share of assets. |
| **Market Cap** | Stock Price × Diluted Shares | yfinance + XBRL | Last calendar-year close price from yfinance. |
| **P/E Ratio** | Stock Price ÷ EPS Diluted | yfinance + XBRL | Price-to-earnings multiple *(positive values only)*. |
| **Price/Book** | Market Cap ÷ Total Equity | yfinance + XBRL | Market vs. book value *(positive values only)*. |
| **Dividend Yield** | (Dividends Paid ÷ Diluted Shares) ÷ Stock Price | yfinance + XBRL | Annual dividend as % of stock price. |

---
> **FY alignment**: Fiscal year data is mapped by the calendar year of the period-end date.
> For September fiscal years (e.g., Visa), FY2024 ends Sep 30, 2024 → mapped to year **2024**.
> The stock price for year 2024 = the last monthly close within calendar year 2024 (December 2024).
"""

# ── SEC EDGAR API (all cached) ─────────────────────────────────────────────────
@st.cache_data(ttl=3_600, show_spinner=False)
def load_ticker_map() -> dict:
    """Fetch full ticker→CIK map from EDGAR (~600 KB, cached 1 h)."""
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=HEADERS, timeout=20,
    )
    r.raise_for_status()
    return r.json()


def find_company(ticker: str) -> dict | None:
    """Return EDGAR company entry for a given ticker, or None."""
    upper = ticker.strip().upper()
    for entry in load_ticker_map().values():
        if entry["ticker"].upper() == upper:
            return entry
    return None


@st.cache_data(ttl=1_800, show_spinner=False)
def load_submissions(cik) -> dict:
    """
    Fetch ALL EDGAR submissions for a CIK, including paginated history.

    EDGAR only puts the most-recent ~1 000 filings in the main JSON.
    Long-standing companies (e.g. AXP since 1993) have their older
    filings split across extra pages listed in sub['filings']['files'].
    We fetch every page and merge its arrays into sub['filings']['recent']
    so the rest of the app sees the complete filing history in one place.
    """
    padded = str(cik).zfill(10)
    r = requests.get(
        f"https://data.sec.gov/submissions/CIK{padded}.json",
        headers=HEADERS, timeout=20,
    )
    r.raise_for_status()
    sub = r.json()

    for file_meta in sub.get("filings", {}).get("files", []):
        page_url = f"https://data.sec.gov/submissions/{file_meta['name']}"
        pr = requests.get(page_url, headers=HEADERS, timeout=20)
        if not pr.ok:
            continue
        for field, values in pr.json().items():
            if isinstance(values, list):
                sub["filings"]["recent"].setdefault(field, [])
                sub["filings"]["recent"][field].extend(values)

    return sub


@st.cache_data(ttl=3_600, show_spinner=False)
def load_company_facts(cik) -> dict:
    """Fetch XBRL Company Facts JSON from EDGAR (all historical financial data)."""
    padded = str(cik).zfill(10)
    r = requests.get(
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json",
        headers=HEADERS, timeout=60,
    )
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=3_600, show_spinner=False)
def load_stock_prices(ticker: str) -> dict:
    """
    Fetch year-end closing prices via yfinance.
    Returns {year: price} dict, or {} on failure.
    """
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="max", interval="1mo")
        if hist.empty:
            return {}
        # Strip timezone if present (yfinance returns tz-aware index)
        if getattr(hist.index, "tz", None) is not None:
            hist.index = hist.index.tz_convert(None)
        hist.index = pd.to_datetime(hist.index)
        # Last monthly close per calendar year ≈ year-end price
        yr_price = hist["Close"].groupby(hist.index.year).last()
        return {int(yr): float(px) for yr, px in yr_price.items()}
    except Exception:
        return {}


@st.cache_data(ttl=600, show_spinner=False, max_entries=40)
def fetch_file_bytes(url: str) -> bytes:
    """Download a filing document (cached 10 min, max 40 files in memory)."""
    r = requests.get(url, headers=HEADERS, timeout=90)
    r.raise_for_status()
    return r.content


@st.cache_data(show_spinner=False)
def build_zip(file_tuples: tuple[tuple[str, str], ...]) -> bytes:
    """
    Pack a set of already-cached filing documents into a single ZIP.
    `file_tuples` is an immutable ((url, filename), ...) so @st.cache_data
    can key on it — the ZIP is only rebuilt when the set of ready files changes.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for url, filename in file_tuples:
            try:
                zf.writestr(filename, fetch_file_bytes(url))
            except Exception:
                pass
    return buf.getvalue()


# ── KPI helpers ────────────────────────────────────────────────────────────────
def get_all_concepts(facts_list: list[dict]) -> list[tuple[str, str]]:
    """
    Union all concept paths from one or more company facts dicts.
    Returns list of (concept_path, display_label), popular KPIs first.
    """
    seen: dict[str, str] = {}   # path → label
    for facts in facts_list:
        for ns, concepts in facts.get("facts", {}).items():
            for name, meta in concepts.items():
                path = f"{ns}/{name}"
                if path not in seen:
                    seen[path] = meta.get("label", name)

    popular_set = set(POPULAR_KPIS)
    popular     = [(p, seen[p]) for p in POPULAR_KPIS if p in seen]
    remaining   = sorted(
        [(p, lbl or p) for p, lbl in seen.items() if p not in popular_set],
        key=lambda x: (x[1] or x[0]).lower(),
    )
    return popular + remaining


def get_concept_series(
    facts: dict,
    concept_path: str,
    period_type: str = "annual",
) -> tuple[pd.DataFrame, str, str]:
    """
    Extract a time-series for one concept from one company's facts dict.

    period_type: 'annual'    → fp == 'FY' entries only
                 'quarterly' → fp in Q1..Q4 entries only

    Returns (df, label, unit_key).
    df columns: date (datetime), value (float), fy, fp, form, filed
    """
    ns, name = concept_path.split("/", 1)
    concept  = facts.get("facts", {}).get(ns, {}).get(name, {})
    label    = concept.get("label", name)
    units    = concept.get("units", {})

    # Pick the most meaningful unit (USD > shares > USD/shares > first)
    unit_key = None
    for preferred in ("USD", "shares", "USD/shares"):
        if preferred in units:
            unit_key = preferred
            break
    if unit_key is None and units:
        unit_key = next(iter(units))

    if not unit_key:
        return pd.DataFrame(), label, ""

    rows = []
    for e in units[unit_key]:
        fp   = e.get("fp", "")
        form = e.get("form", "")

        if period_type == "annual":
            # Primary: explicit FY flag
            # Fallback: 10-K forms that didn't get tagged
            if fp != "FY" and form not in ("10-K", "10-K/A"):
                continue
        else:
            if fp not in ("Q1", "Q2", "Q3", "Q4"):
                continue

        rows.append({
            "date":  e.get("end", ""),
            "value": e.get("val"),
            "fy":    e.get("fy"),
            "fp":    fp,
            "form":  form,
            "filed": e.get("filed", ""),
        })

    if not rows:
        return pd.DataFrame(), label, unit_key

    df = pd.DataFrame(rows)
    df["date"]  = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])

    # Deduplicate: same (fy, fp) period → keep the latest-filed revision
    df = df.sort_values(["fy", "fp", "filed"], ascending=[True, True, False],
                        na_position="last")
    df = df.drop_duplicates(subset=["fy", "fp"], keep="first")
    df = df.sort_values("date").reset_index(drop=True)

    return df, label, unit_key


def fmt_value(val, unit: str) -> str:
    """Format a numeric KPI value into a human-readable string."""
    try:
        if val is None or pd.isna(val):
            return "—"
        val = float(val)
    except Exception:
        return str(val)

    a    = abs(val)
    sign = "−" if val < 0 else ""

    if unit == "USD":
        if a >= 1e12: return f"{sign}${a/1e12:.2f}T"
        if a >= 1e9:  return f"{sign}${a/1e9:.2f}B"
        if a >= 1e6:  return f"{sign}${a/1e6:.1f}M"
        if a >= 1e3:  return f"{sign}${a/1e3:.0f}K"
        return f"{sign}${a:.0f}"

    if unit == "shares":
        if a >= 1e9: return f"{sign}{a/1e9:.2f}B"
        if a >= 1e6: return f"{sign}{a/1e6:.1f}M"
        if a >= 1e3: return f"{sign}{a/1e3:.0f}K"
        return f"{sign}{a:.0f}"

    if unit == "USD/shares":
        return f"${val:.2f}"

    if a >= 1e6: return f"{sign}{a/1e6:.2f}M"
    return f"{sign}{a:,.2f}"


def calc_cagr(df: pd.DataFrame, n_years: int | None = None) -> float | None:
    """Calculate CAGR over the last n_years of data (or full history if None)."""
    if df.empty or len(df) < 2:
        return None
    d = df.copy()
    if n_years:
        cutoff = d["date"].max() - pd.DateOffset(years=n_years)
        d = d[d["date"] >= cutoff]
    if len(d) < 2:
        return None
    sv    = float(d.iloc[0]["value"])
    ev    = float(d.iloc[-1]["value"])
    years = (d.iloc[-1]["date"] - d.iloc[0]["date"]).days / 365.25
    if sv <= 0 or ev <= 0 or years <= 0:
        return None
    return (ev / sv) ** (1.0 / years) - 1.0


def fmt_fiscal_year_end(code: str) -> str:
    """Convert EDGAR's MMDD fiscal-year-end code (e.g. '0930') to 'Sep 30'."""
    try:
        from datetime import date
        d = date(2000, int(code[:2]), int(code[2:]))
        return d.strftime("%b %d")
    except Exception:
        return code or "—"


def yaxis_tickformat(unit: str) -> str:
    if unit == "USD":        return "$,.3s"
    if unit == "shares":     return ",.3s"
    if unit == "USD/shares": return "$,.2f"
    return ",.2f"


# ── Standardized financial statement processing ────────────────────────────────
def extract_annual_metric(facts: dict, tags: list[str]) -> tuple[pd.Series, str]:
    """
    Try each XBRL tag in priority order; return the first with FY-period data.
    Deduplicates: keeps the most recently filed observation per fiscal year.
    Returns (Series with int-year index, unit_str), or (empty Series, "").
    """
    for tag in tags:
        for ns in ("us-gaap", "dei"):
            concept = facts.get("facts", {}).get(ns, {}).get(tag, {})
            if not concept:
                continue
            units    = concept.get("units", {})
            unit_key = next((u for u in ("USD", "shares", "USD/shares") if u in units), None)
            if unit_key is None and units:
                unit_key = next(iter(units))
            if not unit_key:
                continue

            rows = []
            for e in units[unit_key]:
                fp   = e.get("fp",   "")
                form = e.get("form", "")
                # Keep fp=FY entries, plus 10-K entries as fallback
                if fp != "FY" and form not in ("10-K", "10-K/A"):
                    continue
                if e.get("end") is None or e.get("val") is None:
                    continue
                rows.append({
                    "end":   e["end"],
                    "val":   float(e["val"]),
                    "filed": e.get("filed", ""),
                    "is_fy": fp == "FY",
                })
            if not rows:
                continue

            df = pd.DataFrame(rows)
            df["end"] = pd.to_datetime(df["end"], errors="coerce")
            df["val"] = pd.to_numeric(df["val"], errors="coerce")
            df = df.dropna(subset=["end", "val"])

            # Per period-end: keep the most-recently-filed revision
            df = df.sort_values(["end", "filed"], ascending=[True, False])
            df = df.drop_duplicates(subset=["end"], keep="first")

            # Map period-end → calendar year
            df["year"] = df["end"].dt.year

            # Per year: PREFER fp=FY entries; only fall back to 10-K if no FY exists
            fy_years = set(df.loc[df["is_fy"], "year"])
            if fy_years:
                df = df[df["is_fy"] | ~df["year"].isin(fy_years)]

            # If multiple non-FY entries remain for a year, take the latest end date
            df = df.sort_values(["year", "end"], ascending=[True, False])
            df = df.drop_duplicates(subset=["year"], keep="first")

            result = df.set_index("year")["val"].sort_index()
            if not result.empty:
                return result, unit_key

    return pd.Series(dtype=float), ""


def build_financial_statements(facts: dict) -> dict[str, pd.DataFrame]:
    """
    Convert raw EDGAR companyfacts into clean standardized annual DataFrames.
    Implements Stages 1-9 of the standardization pipeline.
    Returns dict: 'income', 'balance', 'cashflow', 'derived'.
    Each DataFrame: index = int year newest-first, columns = metric names.
    """
    raw: dict[str, pd.Series] = {}
    for metric, tags in METRIC_TAGS.items():
        s, _ = extract_annual_metric(facts, tags)
        raw[metric] = s

    def _df(metrics: list[str]) -> pd.DataFrame:
        parts = {m: raw[m] for m in metrics if not raw.get(m, pd.Series()).empty}
        if not parts:
            return pd.DataFrame()
        out = pd.DataFrame(parts)
        out.index.name = "Year"
        return out.sort_index(ascending=False)   # newest year first

    income   = _df(INCOME_METRICS)
    balance  = _df(BALANCE_METRICS)
    cashflow = _df(CASHFLOW_METRICS)

    # ── Derived metrics (Stage 7) ───────────────────────────────────────────
    def g(m: str) -> pd.Series:
        return raw.get(m, pd.Series(dtype=float))

    rev = g("Revenue");  gp = g("Gross Profit");  oi = g("Operating Income")
    ni  = g("Net Income"); ocf = g("Operating Cash Flow"); cx = g("CapEx")
    ta  = g("Total Assets"); te = g("Total Equity")
    ltd = g("Long Term Debt"); cas = g("Cash")
    da  = g("D&A"); iex = g("Interest Expense"); itx = g("Income Tax")

    def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
        return a.div(b.replace(0, float("nan")))

    drv: dict[str, pd.Series] = {}
    if not rev.empty and not gp.empty:       drv["Gross Margin"]      = safe_div(gp, rev)
    if not rev.empty and not oi.empty:       drv["Operating Margin"]  = safe_div(oi, rev)
    if not rev.empty and not ni.empty:       drv["Net Margin"]        = safe_div(ni, rev)

    # ── EBITDA ──────────────────────────────────────────────────────────────────
    # Method 1 (preferred): Operating Income + D&A  (EBIT + D&A)
    # Method 2 (fallback):  Net Income + Interest + Tax + D&A
    _ebitda: pd.Series | None = None
    if not oi.empty and not da.empty:
        _ebitda = oi.add(da.abs(), fill_value=0)
        drv["EBITDA"] = _ebitda
    elif not ni.empty and not da.empty:
        _ebitda = ni.add(da.abs(), fill_value=0)
        if not iex.empty: _ebitda = _ebitda.add(iex.abs(), fill_value=0)
        if not itx.empty: _ebitda = _ebitda.add(itx.abs(), fill_value=0)
        drv["EBITDA"] = _ebitda
    if _ebitda is not None and not rev.empty:
        drv["EBITDA Margin"] = safe_div(_ebitda, rev)

    if len(rev) > 1:                         drv["Revenue Growth"]    = rev.sort_index().pct_change()
    if len(ni) > 1:                          drv["Net Income Growth"] = ni.sort_index().pct_change()

    if not ocf.empty and not cx.empty:
        aln = pd.concat([ocf, cx], axis=1, keys=["ocf", "cx"]).dropna(how="all")
        aln["fcf"]   = aln["ocf"] - aln["cx"].abs()   # cx is a positive outflow in XBRL
        drv["FCF"]   = aln["fcf"]
        if not rev.empty:
            drv["FCF Margin"] = safe_div(aln["fcf"], rev)

    if not ni.empty and not ta.empty:    drv["ROA"]        = safe_div(ni, ta)
    if not ni.empty and not te.empty:    drv["ROE"]        = safe_div(ni, te)
    if not cas.empty and not ltd.empty:
        nd = pd.concat([ltd, cas], axis=1, keys=["ltd", "cas"]).dropna(how="all")
        drv["Net Debt"]   = nd["ltd"].sub(nd["cas"], fill_value=0)
    if not ltd.empty and not te.empty:   drv["Debt/Equity"] = safe_div(ltd, te)
    if not ltd.empty and not ta.empty:   drv["Debt/Assets"] = safe_div(ltd, ta)

    if drv:
        all_yrs = sorted({yr for s in drv.values() for yr in s.index}, reverse=True)
        derived  = pd.DataFrame(drv, index=all_yrs)
        derived.index.name = "Year"
    else:
        derived = pd.DataFrame()

    return {"income": income, "balance": balance, "cashflow": cashflow, "derived": derived}


def fmt_stmt_val(v, fmt: str) -> str:
    """Format one financial statement cell value for table display."""
    try:
        if v is None or pd.isna(v):
            return "—"
        v = float(v)
    except Exception:
        return "—"

    if fmt == "usd_b":
        a, neg = abs(v), v < 0
        s = (f"${a/1e12:.2f}T" if a >= 1e12 else
             f"${a/1e9:.1f}B"  if a >= 1e9  else
             f"${a/1e6:.0f}M"  if a >= 1e6  else f"${a:,.0f}")
        return f"({s})" if neg else s
    if fmt == "usd_share":  return f"${v:.2f}"
    if fmt == "shares_b":
        a = abs(v)
        return (f"{v/1e9:.2f}B" if a >= 1e9 else
                f"{v/1e6:.1f}M" if a >= 1e6 else f"{v:,.0f}")
    if fmt == "pct":        return f"{v * 100:.1f}%"
    if fmt == "pct_signed": return f"{v * 100:+.1f}%"
    if fmt == "ratio":      return f"{v:.2f}x"
    return f"{v:.2f}"


def make_stmt_html(df: pd.DataFrame, metric_list: list[str], max_years: int = 11) -> str:
    """
    Render a financial statement DataFrame as an HTML table.
    Rows = metrics (newest at left), Columns = fiscal years.
    """
    if df.empty:
        return "<p style='color:#94a3b8;padding:20px'>No data available.</p>"
    avail = [m for m in metric_list if m in df.columns]
    if not avail:
        return "<p style='color:#94a3b8;padding:20px'>No data found for these metrics.</p>"

    years = list(df.index[:max_years])   # df already newest-first

    yr_heads = "".join(
        f'<th style="text-align:right;padding:6px 14px;color:#475569;'
        f'font-weight:600;font-size:12px;white-space:nowrap">{y}</th>'
        for y in years
    )
    header = (
        '<tr style="background:#f1f5f9;border-bottom:2px solid #cbd5e1">'
        '<th style="text-align:left;padding:6px 14px;color:#475569;font-weight:600;'
        'font-size:12px;white-space:nowrap">Metric</th>'
        + yr_heads + "</tr>"
    )

    rows_html = []
    for idx, m in enumerate(avail):
        meta   = METRIC_DISPLAY.get(m, {"fmt": "usd_b"})
        fmt    = meta.get("fmt", "usd_b")
        negate = meta.get("negate", False)
        is_key = meta.get("key", False)

        cells = []
        for yr in years:
            raw_v = df.loc[yr, m] if yr in df.index else None
            if raw_v is not None and negate:
                try: raw_v = -float(raw_v)
                except Exception: pass
            s = fmt_stmt_val(raw_v, fmt)
            c = ""
            if fmt == "pct_signed":
                c = "color:#059669" if s.startswith("+") else ("color:#dc2626" if not s.startswith("—") else "")
            elif fmt == "usd_b" and s.startswith("("):
                c = "color:#dc2626"
            weight = "font-weight:700;" if is_key else ""
            cells.append(f'<td style="text-align:right;padding:5px 14px;{c};{weight}">{s}</td>')

        row_bg = "background:#f0f9ff;" if is_key else ("background:#f8fafc;" if idx % 2 == 0 else "")
        rows_html.append(
            f'<tr style="{row_bg}">'
            f'<td style="padding:5px 14px;color:#0f172a;white-space:nowrap;'
            f'{"font-weight:700;" if is_key else ""}">{m}</td>'
            + "".join(cells) + "</tr>"
        )

    return (
        '<div style="overflow-x:auto;margin-bottom:4px">'
        '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        f"<thead>{header}</thead><tbody>{''.join(rows_html)}</tbody>"
        "</table></div>"
    )


def get_stmt_series(stmts: dict, metric: str) -> pd.Series | None:
    """Pull a metric's time-series from any statement dict."""
    for key in ("income", "balance", "cashflow", "derived"):
        df = stmts.get(key, pd.DataFrame())
        if not df.empty and metric in df.columns:
            return df[metric].sort_index()   # ascending for chart
    return None


def compute_price_metrics(stmts: dict, prices: dict) -> None:
    """
    Enrich the 'derived' DataFrame in stmts with price-based metrics:
    Market Cap, P/E Ratio, Price/Book, Dividend Yield.
    Modifies stmts in-place. No-op if prices is empty.
    Also stores stmts["price_debug"] with availability info for diagnostics.
    """
    debug: dict[str, str] = {}

    if not prices:
        stmts["price_debug"] = {"prices": "❌ no price data from yfinance"}
        return
    if not stmts:
        return

    def to_s64(m: str) -> pd.Series:
        """Return a metric as pd.Series with guaranteed int64 index, NaN-cleaned."""
        s = get_stmt_series(stmts, m)
        if s is None or (hasattr(s, "empty") and s.empty):
            return pd.Series(dtype=float)
        # Use explicit dict rebuild with int() keys to guarantee Python int → int64
        return pd.Series(
            {int(k): float(v) for k, v in s.items() if pd.notna(v)},
            dtype=float,
        )

    def _yr(s: pd.Series, extra: str = "") -> str:
        s2 = s.dropna()
        if s2.empty:
            return "❌ not found"
        lo, hi = int(s2.index.min()), int(s2.index.max())
        return f"✅ {len(s2)} yrs ({lo}–{hi}){extra}"

    # ── Build all input Series with guaranteed int64 index ────────────────────
    px  = pd.Series({int(k): float(v) for k, v in prices.items()}, dtype=float)
    sh  = to_s64("Diluted Shares")
    te  = to_s64("Total Equity")
    div = to_s64("Dividends Paid")
    eps = to_s64("EPS Diluted")
    ni  = to_s64("Net Income")

    # EPS fallback: Net Income / Diluted Shares (when direct tag not available)
    computed_eps = False
    if eps.empty and not sh.empty and not ni.empty:
        eps = ni.div(sh.replace(0, float("nan"))).dropna()
        computed_eps = True

    debug["prices"]         = _yr(px)
    debug["Diluted Shares"] = _yr(sh)
    debug["EPS Diluted"]    = _yr(eps, " [computed]" if computed_eps else "")
    debug["Total Equity"]   = _yr(te)
    debug["Dividends Paid"] = _yr(div)
    # Explicit year lists — helps diagnose fiscal-year mapping mismatches
    debug["→ px years"]     = ", ".join(str(y) for y in sorted(px.index)) if not px.empty else "—"
    debug["→ sh years"]     = ", ".join(str(y) for y in sorted(sh.index)) if not sh.empty else "—"
    _overlap = sorted(set(px.index) & set(sh.index))
    debug["→ overlap yrs"]  = f"{len(_overlap)} yrs: " + (", ".join(str(y) for y in _overlap) if _overlap else "none")

    new: dict[str, pd.Series] = {}

    # Market Cap = Price × Shares  (pandas aligns on int64 index automatically)
    mc = px.mul(sh).dropna()
    if not mc.empty:
        new["Market Cap"] = mc
        debug["Market Cap"] = f"✅ {len(mc)} yrs ({int(mc.index.min())}–{int(mc.index.max())})"
        debug["→ mc years"] = ", ".join(str(y) for y in sorted(mc.index))
    else:
        debug["Market Cap"] = "❌ needs Diluted Shares"
        debug["→ mc years"] = "empty — px × sh produced no overlap"

    # P/E Ratio = Price / EPS  (keep only positive, sensible values)
    if not eps.empty:
        pe = px.div(eps.replace(0, float("nan"))).dropna()
        pe = pe[pe > 0]
        if not pe.empty:
            new["P/E Ratio"] = pe
            debug["P/E Ratio"] = f"✅ {len(pe)} yrs ({int(pe.index.min())}–{int(pe.index.max())})"
        else:
            debug["P/E Ratio"] = "❌ no positive P/E values found"
    else:
        debug["P/E Ratio"] = "❌ needs EPS Diluted"

    # Price/Book = Market Cap / Total Equity  (positive only)
    if not mc.empty and not te.empty:
        pb = mc.div(te.replace(0, float("nan"))).dropna()
        pb = pb[pb > 0]
        if not pb.empty:
            new["Price/Book"] = pb
            debug["Price/Book"] = f"✅ {len(pb)} yrs ({int(pb.index.min())}–{int(pb.index.max())})"
        else:
            debug["Price/Book"] = "❌ no positive P/B values found"
    else:
        debug["Price/Book"] = "❌ needs Diluted Shares + Total Equity"

    # Dividend Yield = (Dividends Paid / Shares) / Price  (non-negative)
    if not div.empty and not sh.empty:
        dps = div.div(sh.replace(0, float("nan")))          # dividends per share
        dy  = dps.div(px.replace(0, float("nan"))).dropna()
        dy  = dy[dy >= 0]
        if not dy.empty:
            new["Dividend Yield"] = dy
            debug["Dividend Yield"] = f"✅ {len(dy)} yrs ({int(dy.index.min())}–{int(dy.index.max())})"
        else:
            debug["Dividend Yield"] = "❌ no non-negative yield values found"
    else:
        debug["Dividend Yield"] = "❌ needs Dividends Paid + Diluted Shares"

    stmts["price_debug"] = debug

    if not new:
        return

    # Build new_df with int64 index
    new_df = pd.DataFrame(new).sort_index(ascending=False)

    # Merge with existing derived DataFrame, ensuring matching int64 indices
    drv = stmts.get("derived", pd.DataFrame())
    if drv.empty:
        stmts["derived"] = new_df
    else:
        # Cast drv.index to int64 to match new_df (XBRL yields int32; price data int64)
        drv_64 = drv.copy()
        drv_64.index = drv_64.index.astype("int64")
        combined = pd.concat([drv_64, new_df], axis=1)
        stmts["derived"] = combined.sort_index(ascending=False)


# ── DCF helper functions ───────────────────────────────────────────────────────

@st.cache_data(ttl=3_600, show_spinner=False)
def load_beta(ticker: str) -> float:
    """Fetch beta from yfinance."""
    try:
        import yfinance as yf
        b = yf.Ticker(ticker).info.get("beta")
        return float(b) if b and b > 0 else 1.0
    except Exception:
        return 1.0


def _dcf_series(stmts: dict, metric: str) -> pd.Series:
    """Pull metric as pd.Series with guaranteed int64 index, NaN-cleaned."""
    s = get_stmt_series(stmts, metric)
    if s is None or s.empty:
        return pd.Series(dtype=float)
    return pd.Series({int(k): float(v) for k, v in s.items() if pd.notna(v)}, dtype=float)


def compute_wacc_auto(stmts: dict, market_cap: float, beta: float,
                      rf: float, erp: float) -> dict:
    """Compute WACC components from EDGAR data + market inputs."""
    r_e = rf + beta * erp   # CAPM cost of equity

    # Cost of Debt = avg(Interest Expense) / avg(Long Term Debt) — last 3 yrs
    iex = _dcf_series(stmts, "Interest Expense").sort_index(ascending=False).head(3)
    ltd = _dcf_series(stmts, "Long Term Debt").sort_index(ascending=False).head(3)
    iex_mean = iex[iex > 0].mean() if not iex.empty else 0
    ltd_mean = ltd[ltd > 0].mean() if not ltd.empty else 0
    r_d = float(iex_mean / ltd_mean) if ltd_mean > 0 and iex_mean > 0 else 0.04
    r_d = min(max(r_d, 0.01), 0.25)

    # Effective Tax Rate = Income Tax / (Net Income + Income Tax)
    itx = _dcf_series(stmts, "Income Tax").sort_index(ascending=False)
    ni  = _dcf_series(stmts, "Net Income").sort_index(ascending=False)
    itx1 = itx.iloc[0] if not itx.empty else None
    ni1  = ni.iloc[0]  if not ni.empty  else None
    if itx1 and itx1 > 0 and ni1 is not None and abs(ni1 + itx1) > 0:
        tax_rate = abs(itx1) / abs(ni1 + itx1)
        tax_rate = min(max(tax_rate, 0.0), 0.50)
    else:
        tax_rate = 0.21

    # Capital structure: market equity + book debt
    ltd1     = (ltd.iloc[0] if not ltd.empty else 0.0) or 0.0
    total_v  = market_cap + ltd1
    e_wt = market_cap / total_v if total_v > 0 else 1.0
    d_wt = ltd1       / total_v if total_v > 0 else 0.0

    wacc = e_wt * r_e + d_wt * r_d * (1.0 - tax_rate)
    return dict(wacc=wacc, r_e=r_e, r_d=r_d, tax_rate=tax_rate,
                e_wt=e_wt, d_wt=d_wt, ltd=ltd1)


def reverse_dcf_fcf(market_cap: float, fcf: float, wacc: float) -> float | None:
    """Implied growth: MC = FCF(1+g)/(WACC-g)  => g = (MC·WACC - FCF)/(MC + FCF)"""
    denom = market_cap + fcf
    if denom <= 0 or wacc <= 0:
        return None
    g = (market_cap * wacc - fcf) / denom
    return g if (-0.30 < g < wacc) else None


def reverse_dcf_ddm(price: float, dps: float, r_e: float) -> float | None:
    """Implied growth: P = DPS(1+g)/(r_e-g)  => g = (P·r_e - DPS)/(P + DPS)"""
    denom = price + dps
    if denom <= 0 or r_e <= 0 or dps <= 0:
        return None
    g = (price * r_e - dps) / denom
    return g if (-0.30 < g < r_e) else None


def build_fcf_bridge(stmts: dict, n: int = 7, tax_rate: float = 0.21) -> pd.DataFrame:
    """
    Build FCFF bridge with full tax step:
      Revenue − COGS − SG&A  = EBITDA (simplified)
      EBITDA  − D&A           = EBIT
      EBIT    × tax_rate      = Taxes
      EBIT    × (1−tax_rate)  = NOPAT
      NOPAT   + D&A − CapEx − ΔNWC = FCFF

    Year selection anchors on the series with the most-recent data among
    Revenue, FCF (derived), CapEx — handles financial-sector companies
    (e.g. AXP) whose Revenue XBRL tags only appear in older filings.
    Returns DataFrame indexed by fiscal year (newest first).
    """
    rev        = _dcf_series(stmts, "Revenue")
    cogs       = _dcf_series(stmts, "COGS")
    sga        = _dcf_series(stmts, "SG&A")
    da_s       = _dcf_series(stmts, "D&A")
    capex      = _dcf_series(stmts, "CapEx")
    ca         = _dcf_series(stmts, "Current Assets")
    cash       = _dcf_series(stmts, "Cash")
    cl         = _dcf_series(stmts, "Current Liabilities")
    fcf_direct = _dcf_series(stmts, "FCF")          # derived FCF = OpCF − CapEx

    # ── Pick the anchor series: whichever has the most-recent fiscal year ──────
    candidates = [(s, s.index.max()) for s in [rev, fcf_direct, capex] if not s.empty]
    if not candidates:
        return pd.DataFrame()
    anchor = max(candidates, key=lambda t: t[1])[0]
    all_years = sorted(anchor.index, reverse=True)[:n]

    rows, nwc_prev = [], None

    for yr in sorted(all_years):        # ascending to compute delta
        r  = rev.get(yr);   c = cogs.get(yr);  s = sga.get(yr)
        cx = capex.get(yr);  da_raw = da_s.get(yr)

        # ── EBITDA (simplified) ────────────────────────────────────────────────
        ebitda_s = r
        if c is not None: ebitda_s = ebitda_s - c if ebitda_s is not None else None
        if s is not None: ebitda_s = ebitda_s - s if ebitda_s is not None else None

        # ── D&A (always positive — add back later) ────────────────────────────
        da_abs = abs(da_raw) if da_raw is not None else None

        # ── EBIT = EBITDA − D&A ───────────────────────────────────────────────
        if ebitda_s is not None and da_abs is not None:
            ebit = ebitda_s - da_abs
        else:
            ebit = ebitda_s   # fallback when D&A unavailable

        # ── Taxes = max(EBIT, 0) × tax_rate  (no tax benefit on losses) ───────
        taxes = max(float(ebit), 0.0) * tax_rate if ebit is not None else None

        # ── NOPAT = EBIT × (1 − tax_rate) ────────────────────────────────────
        nopat = ebit * (1.0 - tax_rate) if ebit is not None else None

        # ── NWC & delta ───────────────────────────────────────────────────────
        _ca = ca.get(yr); _cas = cash.get(yr); _cl = cl.get(yr)
        nwc = (_ca - (_cas or 0)) - _cl if (_ca is not None and _cl is not None) else None
        d_nwc = (nwc - nwc_prev) if (nwc is not None and nwc_prev is not None) else None
        nwc_prev = nwc

        cx_abs = abs(cx) if cx is not None else None

        # ── FCFF = NOPAT + D&A − CapEx − ΔNWC ───────────────────────────────
        fcf_b = nopat
        if fcf_b is not None and da_abs  is not None: fcf_b += da_abs   # add back
        if fcf_b is not None and cx_abs  is not None: fcf_b -= cx_abs
        if fcf_b is not None and d_nwc   is not None: fcf_b -= d_nwc
        # Fall back to derived FCF (OpCF − CapEx) if bridge cannot compute
        if fcf_b is None:
            fcf_b = fcf_direct.get(yr)

        rows.append(dict(year=yr, Revenue=r, COGS=c, SGA=s,
                         EBITDA_s=ebitda_s, DA=da_abs, EBIT=ebit,
                         Taxes=taxes, NOPAT=nopat,
                         CapEx=cx_abs, dNWC=d_nwc, FCF=fcf_b))

    df = pd.DataFrame(rows).set_index("year").sort_index(ascending=False)
    return df.head(n)


# ── Filings data helpers ───────────────────────────────────────────────────────
def period_label(form: str, date_str: str) -> str:
    if not date_str:
        return "—"
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        if form == "10-K":
            return f"FY {d.year}"
        q = "Q1" if d.month <= 3 else "Q2" if d.month <= 6 else "Q3" if d.month <= 9 else "Q4"
        return f"{q} {d.year}"
    except Exception:
        return date_str[:7]


def fmt_size(b: int) -> str:
    if not b:
        return "—"
    if b < 1_024:
        return f"{b} B"
    if b < 1_048_576:
        return f"{b / 1_024:.0f} KB"
    return f"{b / 1_048_576:.1f} MB"


def parse_filings(sub: dict, ticker: str) -> dict[str, list[dict]]:
    """Convert raw EDGAR submissions JSON into organised filing lists."""
    r   = sub["filings"]["recent"]
    cik = int(sub["cik"])
    n   = len(r["form"])
    out: dict[str, list] = {"10-K": [], "10-Q": [], "JSON": []}

    for i in range(n):
        form = r["form"][i]
        if form not in ("10-K", "10-Q"):
            continue

        def g(field: str, default=""):
            lst = r.get(field, [])
            return lst[i] if i < len(lst) else default

        acc   = g("accessionNumber")
        date  = g("filingDate")
        rep   = g("reportDate")
        prim  = g("primaryDocument")
        desc  = g("primaryDocDescription") or form
        size  = int(g("size") or 0)
        plain = acc.replace("-", "")

        doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{plain}/{prim}"
            if prim else ""
        )
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{plain}/{acc}-index.htm"

        out[form].append({
            "form":      form,
            "period":    period_label(form, rep or date),
            "filed":     date,
            "desc":      desc,
            "size":      size,
            "doc_url":   doc_url,
            "index_url": idx_url,
            "filename":  f"{ticker}_{form.replace('-', '')}_{(rep or date)[:7]}.htm",
            "id":        f"{ticker}_{acc}",
        })

    padded = str(cik).zfill(10)
    out["JSON"] = [
        {
            "form": "JSON", "period": "All years", "filed": "—",
            "desc": "Company Facts  (XBRL financial data)",
            "size": 0,
            "doc_url":   f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json",
            "index_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json",
            "filename":  f"{ticker}_companyfacts.json",
            "id":        f"{ticker}_companyfacts",
        },
        {
            "form": "JSON", "period": "All filings", "filed": "—",
            "desc": "Submissions & Filing Metadata",
            "size": 0,
            "doc_url":   f"https://data.sec.gov/submissions/CIK{padded}.json",
            "index_url": f"https://data.sec.gov/submissions/CIK{padded}.json",
            "filename":  f"{ticker}_submissions.json",
            "id":        f"{ticker}_submissions",
        },
    ]
    return out


# ── Session-state init ─────────────────────────────────────────────────────────
_DEFAULTS = {
    # Filings state
    "ticker":       "",
    "company":      None,
    "sub":          None,
    "filings":      None,
    "error":        None,
    "folder":       "10-K",
    "ready":        set(),
    # KPI Explorer state
    "kpi_tickers":  [],        # list of ticker strings with loaded facts
    "kpi_facts":    {},        # {ticker: facts_dict}
    "kpi_subs":     {},        # {ticker: submissions_dict}  ← company profile info
    "kpi_stmts":    {},        # {ticker: {"income":df,"balance":df,"cashflow":df,"derived":df}}
    "kpi_prices":   {},        # {ticker: {year: price}}
    "kpi_concept":  None,      # selected concept path
    "kpi_period":   "annual",
    "kpi_error":    None,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Action: search filings ──────────────────────────────────────────────────────
def run_search(raw: str) -> None:
    ticker = raw.strip().upper()
    if not ticker:
        return

    st.session_state.ticker  = ticker
    st.session_state.error   = None
    st.session_state.filings = None
    st.session_state.company = None
    st.session_state.sub     = None
    st.session_state.ready   = set()

    with st.spinner(f"Looking up **{ticker}** on SEC EDGAR…"):
        company = find_company(ticker)
        if not company:
            st.session_state.error = (
                f"Ticker **{ticker}** was not found in SEC EDGAR. "
                f"[Search EDGAR directly]"
                f"(https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22)"
            )
            return

        try:
            sub = load_submissions(company["cik_str"])
        except Exception as exc:
            st.session_state.error = (
                f"Could not load EDGAR data for **{ticker}**: {exc}"
            )
            return

    st.session_state.company = company
    st.session_state.sub     = sub
    st.session_state.filings = parse_filings(sub, ticker)


# ── Action: load KPI facts ──────────────────────────────────────────────────────
def run_kpi_load(tickers_raw: str) -> None:
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    st.session_state.kpi_error   = None
    st.session_state.kpi_facts   = {}
    st.session_state.kpi_subs    = {}
    st.session_state.kpi_stmts   = {}
    st.session_state.kpi_prices  = {}
    st.session_state.kpi_tickers = []
    st.session_state.kpi_concept = None   # reset so selector defaults to first

    errors = []
    n = len(tickers)
    prog = st.progress(0, text="Loading company facts…")
    for idx, tk in enumerate(tickers):
        prog.progress(idx / n, text=f"Loading {tk}…")
        company = find_company(tk)
        if not company:
            errors.append(f"**{tk}** not found in EDGAR.")
            continue
        try:
            facts  = load_company_facts(company["cik_str"])
            sub    = load_submissions(company["cik_str"])
            stmts  = build_financial_statements(facts)
            prices = load_stock_prices(tk)
            compute_price_metrics(stmts, prices)
            st.session_state.kpi_facts[tk]   = facts
            st.session_state.kpi_subs[tk]    = sub
            st.session_state.kpi_stmts[tk]   = stmts
            st.session_state.kpi_prices[tk]  = prices
            st.session_state.kpi_tickers.append(tk)
        except Exception as exc:
            errors.append(f"**{tk}**: {exc}")
    prog.progress(1.0, text="Done!")
    prog.empty()

    if errors:
        st.session_state.kpi_error = "  \n".join(errors)


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📊 SEC Filings Explorer")
    st.caption("Data from [SEC EDGAR](https://www.sec.gov/edgar) public APIs")
    st.divider()

    # ── Top-level navigation (persists across reruns) ────────────────────────
    page = st.radio(
        "Page",
        options=["📁  Filings", "📈  KPI Explorer", "💰  Model DCF", "📉  Drawdown", "📊  Returns", "🎯  Scorecard"],
        label_visibility="collapsed",
        horizontal=True,
        key="nav_page",
    )
    st.divider()

    # ── Filings sidebar ──────────────────────────────────────────────────────
    if page == "📁  Filings":

        ticker_input = st.text_input(
            "Ticker",
            value=st.session_state.ticker,
            placeholder="AAPL, MSFT, TSLA, JPM…",
            max_chars=10,
            label_visibility="collapsed",
        ).strip().upper()

        if st.button("🔍  Search", type="primary", use_container_width=True):
            run_search(ticker_input)

        if st.session_state.company:
            co  = st.session_state.company
            sub = st.session_state.sub
            fi  = st.session_state.filings

            st.divider()
            st.markdown(f"### {co['title']}")
            st.caption(
                f"**Ticker:** `{co['ticker']}`  \n"
                f"**CIK:** `{co['cik_str']}`  \n"
                f"**Exchange:** {', '.join(sub.get('exchanges') or ['—'])}  \n"
                f"**SIC:** {sub.get('sic', '—')} · {sub.get('sicDescription', '')}"
            )

            st.divider()

            st.session_state.folder = st.radio(
                "📁 Browse folder",
                options=["10-K", "10-Q", "JSON"],
                format_func=lambda x: (
                    f"{FOLDER_INFO[x]['icon']}  {x}   ({len(fi.get(x, []))} files)"
                ),
                index=["10-K", "10-Q", "JSON"].index(st.session_state.folder),
            )

            st.divider()

            n_ready = len(st.session_state.ready)
            if n_ready:
                st.success(f"✅  {n_ready} file{'s' if n_ready > 1 else ''} ready to save")
            else:
                st.info("Click **⬇ Fetch** on any file to prepare it for download.")

            # Track recent searches
            if "recent" not in st.session_state:
                st.session_state.recent = []
            rec: list = st.session_state.recent
            t = st.session_state.ticker
            if t and (not rec or rec[0] != t):
                rec = [t] + [x for x in rec if x != t]
                st.session_state.recent = rec[:8]

        # Recent tickers
        if st.session_state.get("recent"):
            st.divider()
            st.caption("Recent searches")
            cols = st.columns(3)
            for idx, tk in enumerate(st.session_state.recent[:6]):
                if cols[idx % 3].button(tk, key=f"rec_{tk}", use_container_width=True):
                    run_search(tk)
                    st.rerun()

    # ── KPI Explorer sidebar ─────────────────────────────────────────────────
    elif page == "📈  KPI Explorer":
        if st.session_state.kpi_tickers:
            st.markdown("**Loaded companies**")
            for tk in st.session_state.kpi_tickers:
                facts = st.session_state.kpi_facts.get(tk, {})
                n_concepts = sum(
                    len(c) for c in facts.get("facts", {}).values()
                )
                entity = facts.get("entityName", tk)
                st.caption(f"**{tk}** — {entity}  \n`{n_concepts:,}` KPIs available")
            st.divider()

        st.info(
            "Enter one or more tickers in the main area and click **📊 Load** "
            "to start exploring financial KPIs."
        )

    # ── DCF Model sidebar ─────────────────────────────────────────────────────
    elif page == "💰  Model DCF":
        _dcf_tks = st.session_state.get("kpi_tickers", [])
        _dcf_fts = st.session_state.get("kpi_facts",   {})
        if _dcf_tks:
            _sel_tk = st.session_state.get("dcf_company_sel", _dcf_tks[0])
            if _sel_tk not in _dcf_tks:
                _sel_tk = _dcf_tks[0]
            _ename = _dcf_fts.get(_sel_tk, {}).get("entityName", _sel_tk)
            st.markdown(f"### 🏢 {_ename}")
            st.caption(f"`{_sel_tk}`")
            st.divider()
        st.info("Uses data loaded in **📈 KPI Explorer**")

    # ── Drawdown sidebar ──────────────────────────────────────────────────────
    elif page == "📉  Drawdown":
        st.info("Enter a ticker to analyze historical drawdowns and option strike levels.")

    # ── Total Return sidebar ──────────────────────────────────────────────────
    elif page == "📊  Returns":
        st.info(
            "Enter any ticker to compute buy-and-hold IRR vs S&P 500, "
            "with optional dividend reinvestment and bootstrap simulation."
        )

    # ── Scorecard sidebar ─────────────────────────────────────────────────────
    elif page == "🎯  Scorecard":
        st.markdown("**Base de datos**")
        try:
            from scorecard_db import init_db, sp500_count, get_all_runs, _gcs_client
            init_db()
            n_co  = sp500_count()
            n_run = len([r for r in get_all_runs() if r["status"] == "complete"])
            st.caption(f"🏢 {n_co} empresas cargadas")
            st.caption(f"🎯 {n_run} scorecards completos")
            # GCS status
            try:
                from scorecard_db import gcs_ok, gcs_last_error, _gcs_client
                _c, _b = _gcs_client()
                if _b is None:
                    st.caption("☁️ GCS no configurado")
                elif gcs_ok:
                    st.caption("☁️ GCS ✅ sincronizado")
                else:
                    st.caption(f"☁️ GCS ⚠️ error de sync")
                    st.warning(f"GCS sync falló: {gcs_last_error}", icon="⚠️")
            except Exception:
                st.caption("☁️ GCS error ⚠️")
        except Exception:
            st.caption("Base de datos lista")
        st.divider()
        st.info(
            "Selecciona una empresa del S&P 500 / 400 / 600, configura el LLM y "
            "ejecuta el algoritmo de scoring con tus 74 preguntas."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: FILINGS
# ══════════════════════════════════════════════════════════════════════════════
if page == "📁  Filings":

    # ── Error ────────────────────────────────────────────────────────────────
    if st.session_state.error:
        st.error(st.session_state.error)
        st.stop()

    # ── Landing ──────────────────────────────────────────────────────────────
    if not st.session_state.filings:
        st.markdown("""
        <div class="hero">
            <div style="font-size:64px;margin-bottom:16px">📊</div>
            <h2>SEC Filings Explorer</h2>
            <p>
                Search any US public company by ticker to browse and download
                <strong>10-K</strong> annual reports, <strong>10-Q</strong> quarterly
                reports, and <strong>XBRL&nbsp;JSON</strong> data files — all directly
                from SEC EDGAR.
            </p>
            <p class="hint">← Enter a ticker symbol in the sidebar to get started</p>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    # ── File list ─────────────────────────────────────────────────────────────
    folder  = st.session_state.folder
    filings = st.session_state.filings.get(folder, [])
    info    = FOLDER_INFO[folder]
    ticker  = st.session_state.ticker
    ready   = st.session_state.ready

    # Section header row
    hc1, hc2, hc3, hc4 = st.columns([4, 1.2, 1.2, 1.7])
    with hc1:
        bs = BADGE_STYLE[folder]
        st.markdown(
            f"## {info['icon']}  {info['label']} "
            f"<span style='{bs};padding:3px 13px;border-radius:100px;"
            f"font-size:13px;font-weight:700'>{len(filings)}</span>",
            unsafe_allow_html=True,
        )
    with hc2:
        edgar_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
            f"&CIK={st.session_state.company['cik_str']}"
            f"&type={folder}&dateb=&owner=include&count=40"
        )
        st.link_button("↗ View on EDGAR", edgar_url, use_container_width=True)
    with hc3:
        unfetched = [f for f in filings if f["id"] not in ready and f["doc_url"]]
        if unfetched and st.button("⬇ Fetch All", use_container_width=True, key="fetch_all"):
            prog = st.progress(0, text="Fetching files…")
            errors = []
            for idx, f in enumerate(unfetched):
                try:
                    fetch_file_bytes(f["doc_url"])
                    ready.add(f["id"])
                except Exception as exc:
                    errors.append(f"{f['filename']}: {exc}")
                prog.progress((idx + 1) / len(unfetched), text=f"Fetched {idx+1}/{len(unfetched)}")
            st.session_state.ready = ready
            prog.empty()
            if errors:
                st.warning(f"{len(errors)} file(s) failed:\n" + "\n".join(errors))
            st.rerun()
    with hc4:
        ready_here = [(f["doc_url"], f["filename"]) for f in filings if f["id"] in ready and f["doc_url"]]
        if ready_here:
            zip_bytes = build_zip(tuple(ready_here))
            st.download_button(
                label=f"💾 Save All ({len(ready_here)}) as ZIP",
                data=zip_bytes,
                file_name=f"{ticker}_{folder}_filings.zip",
                mime="application/zip",
                use_container_width=True,
                type="primary",
            )

    # Empty state
    if not filings:
        st.info(
            f"No **{folder}** filings found in EDGAR's records for **{ticker}**. "
            "They may be filed under a different entity or CIK."
        )
        st.stop()

    # Column headers
    c1, c2, c3, c4, c5, c6, c7 = st.columns([0.9, 2.8, 1.4, 1.1, 0.9, 0.8, 1.5])
    for col, lbl in zip(
        [c1, c2, c3, c4, c5, c6, c7],
        ["Type", "Description", "Period", "Filed", "Size", "", "Actions"],
    ):
        col.markdown(f"<span class='col-hdr'>{lbl}</span>", unsafe_allow_html=True)

    st.markdown("<hr style='margin:4px 0 10px;border-color:#e2e8f0'>", unsafe_allow_html=True)

    # File rows
    for f in filings:
        fid      = f["id"]
        is_ready = fid in ready
        has_url  = bool(f["doc_url"])
        bs       = BADGE_STYLE.get(f["form"], "background:#f1f5f9;color:#334155")

        c1, c2, c3, c4, c5, c6, c7 = st.columns([0.9, 2.8, 1.4, 1.1, 0.9, 0.8, 1.5])

        c1.markdown(
            f"<span style='{bs};padding:3px 9px;border-radius:4px;"
            f"font-weight:700;font-size:11px'>{f['form']}</span>",
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"<span style='font-size:13px;color:#1e293b'>{f['desc']}</span>",
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"<span style='font-size:13px;font-weight:600;color:#334155'>{f['period']}</span>",
            unsafe_allow_html=True,
        )
        c4.markdown(
            f"<span style='font-size:12px;color:#64748b'>{f['filed']}</span>",
            unsafe_allow_html=True,
        )
        c5.markdown(
            f"<span style='font-size:12px;color:#94a3b8'>{fmt_size(f['size'])}</span>",
            unsafe_allow_html=True,
        )
        if is_ready:
            c6.markdown("<span class='dl-tick'>✓</span>", unsafe_allow_html=True)

        with c7:
            if not has_url:
                st.link_button("↗ EDGAR", f["index_url"], use_container_width=True)
            elif is_ready:
                try:
                    data = fetch_file_bytes(f["doc_url"])
                    mime = (
                        "application/json"
                        if f["filename"].endswith(".json")
                        else "text/html"
                    )
                    st.download_button(
                        "💾 Save file",
                        data=data,
                        file_name=f["filename"],
                        mime=mime,
                        key=f"save_{fid}",
                        use_container_width=True,
                        type="primary",
                    )
                except Exception as exc:
                    st.error(str(exc))
                    ready.discard(fid)
                    st.session_state.ready = ready
            else:
                if st.button("⬇ Fetch", key=f"fetch_{fid}", use_container_width=True):
                    with st.spinner("Fetching…"):
                        try:
                            fetch_file_bytes(f["doc_url"])
                            st.session_state.ready = ready | {fid}
                        except Exception as exc:
                            st.error(f"Failed: {exc}")
                    st.rerun()

        st.markdown("<hr class='row-sep'>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: KPI EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈  KPI Explorer":
    st.markdown("## 📈 KPI Explorer")
    st.caption(
        "Load one or more companies, then explore any financial KPI from their "
        "XBRL data — plot trends, calculate CAGR, compare across firms."
    )

    # ── Company loader ────────────────────────────────────────────────────────
    # Pre-fill with current filings ticker if KPI explorer is empty
    default_kpi_input = (
        ", ".join(st.session_state.kpi_tickers)
        if st.session_state.kpi_tickers
        else st.session_state.ticker
    )

    inp_col, btn_col, ref_col = st.columns([5, 1, 1])
    with inp_col:
        kpi_tickers_raw = st.text_input(
            "Companies",
            value=default_kpi_input,
            placeholder="e.g.  AAPL, MSFT, GOOGL, AMZN",
            label_visibility="collapsed",
            key="kpi_ticker_input",
        )
    with btn_col:
        load_clicked = st.button(
            "📊 Load", type="primary", use_container_width=True, key="kpi_load_btn"
        )
    with ref_col:
        refresh_clicked = st.button(
            "🔄 Refresh", use_container_width=True, key="kpi_refresh_btn",
            help="Clear cached EDGAR data and fetch the latest — use this if recent filings are missing",
        )

    if refresh_clicked:
        load_company_facts.clear()
        load_submissions.clear()
        load_stock_prices.clear()
        st.session_state.kpi_facts   = {}
        st.session_state.kpi_subs    = {}
        st.session_state.kpi_stmts   = {}
        st.session_state.kpi_prices  = {}
        st.session_state.kpi_tickers = []
        st.info("Cache cleared. Click **📊 Load** to fetch fresh data from EDGAR.")

    if load_clicked:
        run_kpi_load(kpi_tickers_raw)

    # ── Error / empty state ───────────────────────────────────────────────────
    if st.session_state.kpi_error:
        st.warning(st.session_state.kpi_error)

    kpi_facts   = st.session_state.kpi_facts
    kpi_tickers = st.session_state.kpi_tickers

    if not kpi_tickers:
        st.markdown("""
        <div style='text-align:center;padding:70px 0 40px;color:#94a3b8'>
            <div style='font-size:52px;margin-bottom:14px'>📈</div>
            <p style='font-size:1rem;max-width:440px;margin:0 auto;line-height:1.7;color:#64748b'>
                Enter one or more ticker symbols above (comma-separated) and click
                <strong>📊 Load</strong> to start exploring financial KPIs.
            </p>
            <p style='margin-top:12px;font-size:.85rem'>
                Supports any US-listed company in SEC EDGAR — try
                <strong>AAPL, MSFT, GOOGL</strong> or <strong>JPM, BAC, GS</strong>
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    # ── Company profile cards ─────────────────────────────────────────────────
    kpi_subs = st.session_state.kpi_subs
    n_co     = len(kpi_tickers)
    co_cols  = st.columns(n_co)

    for i, tk in enumerate(kpi_tickers):
        sub   = kpi_subs.get(tk, {})
        facts = kpi_facts.get(tk, {})

        name        = sub.get("name") or facts.get("entityName") or tk
        cik         = sub.get("cik", "—")
        exchanges   = ", ".join(sub.get("exchanges") or ["—"])
        tickers_all = ", ".join(sub.get("tickers") or [tk])
        sic         = sub.get("sic", "—")
        sic_desc    = sub.get("sicDescription", "—")
        state_inc   = sub.get("stateOfIncorporation", "—")
        fy_end_raw  = sub.get("fiscalYearEnd", "")
        fy_end      = fmt_fiscal_year_end(fy_end_raw) if fy_end_raw else "—"
        category    = sub.get("category", "—")
        phone       = sub.get("phone", "—")
        ein         = sub.get("ein", "—")

        # Business address
        addr_block  = sub.get("addresses", {}).get("business", {})
        city        = addr_block.get("city", "")
        state       = addr_block.get("stateOrCountry", "")
        zipcode     = addr_block.get("zipCode", "")
        address_str = ", ".join(filter(None, [city, state, zipcode])) or "—"

        # Count available KPIs
        n_usgaap = len(facts.get("facts", {}).get("us-gaap", {}))
        n_dei    = len(facts.get("facts", {}).get("dei", {}))

        with co_cols[i]:
            st.markdown(
                f"""
                <div style="background:#f8fafc;border:1.5px solid #e2e8f0;
                            border-radius:10px;padding:16px 18px;margin-bottom:12px">
                  <div style="font-size:1.1rem;font-weight:800;color:#0f172a;
                              margin-bottom:4px">{name}</div>
                  <div style="margin-bottom:10px">
                    <span style="background:#dbeafe;color:#1e40af;padding:2px 9px;
                                 border-radius:100px;font-size:11px;font-weight:700;
                                 margin-right:6px">{tk}</span>
                    <span style="background:#f1f5f9;color:#475569;padding:2px 9px;
                                 border-radius:100px;font-size:11px">{exchanges}</span>
                  </div>
                  <table style="font-size:12px;border-collapse:collapse;width:100%">
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0;
                                   white-space:nowrap">CIK</td>
                        <td style="color:#1e293b;font-weight:600">{cik}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">Ticker(s)</td>
                        <td style="color:#1e293b">{tickers_all}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">EIN</td>
                        <td style="color:#1e293b">{ein}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">SIC</td>
                        <td style="color:#1e293b">{sic} — {sic_desc}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">Category</td>
                        <td style="color:#1e293b">{category}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">State Inc.</td>
                        <td style="color:#1e293b">{state_inc}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">FY End</td>
                        <td style="color:#1e293b">{fy_end}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">Phone</td>
                        <td style="color:#1e293b">{phone}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">HQ</td>
                        <td style="color:#1e293b">{address_str}</td></tr>
                    <tr><td style="color:#94a3b8;padding:2px 6px 2px 0">KPIs</td>
                        <td style="color:#1e293b">{n_usgaap:,} us-gaap
                            · {n_dei} dei</td></tr>
                  </table>
                </div>
                """,
                unsafe_allow_html=True,
            )
            prices_tk = st.session_state.kpi_prices.get(tk, {})
            if prices_tk:
                latest_yr  = max(prices_tk)
                latest_px  = prices_tk[latest_yr]
                st.caption(f"💹 Stock price loaded · {len(prices_tk)} yrs · latest {latest_yr}: ${latest_px:,.2f}")

    st.divider()

    # ── Mode toggle ───────────────────────────────────────────────────────────
    view_mode = st.radio(
        "View mode",
        options=["📊  Standardized Financials", "🔬  Raw KPI Explorer"],
        horizontal=True,
        label_visibility="collapsed",
        key="kpi_view_mode",
    )

    # ══════════════════════════════════════════════════════════════════════════
    #  MODE A — STANDARDIZED FINANCIALS
    # ══════════════════════════════════════════════════════════════════════════
    if view_mode == "📊  Standardized Financials":
        kpi_stmts = st.session_state.kpi_stmts

        # Company selector when multiple loaded
        if len(kpi_tickers) > 1:
            sel_co = st.selectbox(
                "Company to view",
                options=kpi_tickers,
                key="kpi_std_company",
            )
        else:
            sel_co = kpi_tickers[0]

        stmts = kpi_stmts.get(sel_co, {})

        if not stmts:
            st.warning(
                f"No standardized data built for **{sel_co}**. "
                "Click **📊 Load** above to (re-)load the company."
            )
            st.stop()

        # ── Statement tabs ────────────────────────────────────────────────────
        tab_is, tab_bs, tab_cf, tab_drv, tab_cmp, tab_meth = st.tabs([
            "📈 Income Statement",
            "🏦 Balance Sheet",
            "💵 Cash Flow",
            "📐 Derived Metrics",
            "🔀 Compare Companies",
            "📖 Methodology",
        ])

        def _stmt_tab(tab, df: pd.DataFrame, metric_list: list[str],
                      tab_key: str, chart_title: str) -> None:
            """Render one statement tab: HTML table + optional chart."""
            with tab:
                st.markdown(make_stmt_html(df, metric_list), unsafe_allow_html=True)

                avail = [m for m in metric_list if not df.empty and m in df.columns]
                if not avail:
                    return

                st.markdown("---")
                chart_col, _ = st.columns([3, 2])
                with chart_col:
                    chart_m = st.selectbox(
                        "Chart metric",
                        options=avail,
                        key=f"kpi_chart_{tab_key}",
                    )

                # Build chart for selected metric across ALL loaded companies
                fig = go.Figure()
                for ci, tk in enumerate(kpi_tickers):
                    s = get_stmt_series(kpi_stmts.get(tk, {}), chart_m)
                    if s is None or s.empty:
                        continue
                    meta  = METRIC_DISPLAY.get(chart_m, {"fmt": "usd_b"})
                    negate = meta.get("negate", False)
                    y = -s.astype(float) if negate else s.astype(float)
                    fmt  = meta.get("fmt", "usd_b")
                    hover_vals = [fmt_stmt_val(v, fmt) for v in (y if not negate else s)]
                    color = CHART_COLORS[ci % len(CHART_COLORS)]
                    fig.add_trace(go.Scatter(
                        x=s.index.astype(str),
                        y=y,
                        name=tk,
                        mode="lines+markers",
                        line=dict(color=color, width=2.5),
                        marker=dict(size=6, color=color,
                                    line=dict(width=1.5, color="white")),
                        customdata=hover_vals,
                        hovertemplate=(
                            f"<b>{tk}</b>  %{{customdata}}<br>"
                            "Year: %{x}<extra></extra>"
                        ),
                    ))
                fig.update_layout(
                    title=dict(text=f"<b>{chart_m}</b>",
                               font=dict(size=16, color="#0f172a")),
                    xaxis=dict(showgrid=True, gridcolor="#f1f5f9",
                               title="Fiscal Year"),
                    yaxis=dict(showgrid=True, gridcolor="#f1f5f9",
                               tickformat=yaxis_tickformat(
                                   "USD" if "usd" in meta.get("fmt","") else
                                   "shares" if "shares" in meta.get("fmt","") else ""
                               )),
                    legend=dict(orientation="h", yanchor="bottom",
                                y=1.02, xanchor="right", x=1),
                    plot_bgcolor="white", paper_bgcolor="white",
                    margin=dict(l=0, r=0, t=50, b=0), height=370,
                )
                st.plotly_chart(fig, use_container_width=True)

                # CSV export
                if not df.empty:
                    avail_df = df[[m for m in metric_list if m in df.columns]]
                    csv = avail_df.to_csv().encode("utf-8")
                    st.download_button(
                        f"⬇ Download {chart_title} CSV",
                        data=csv,
                        file_name=f"{sel_co}_{tab_key}.csv",
                        mime="text/csv",
                        key=f"dl_{tab_key}",
                    )

        _stmt_tab(tab_is,  stmts.get("income",   pd.DataFrame()),
                  INCOME_METRICS,   "income",   "Income Statement")
        _stmt_tab(tab_bs,  stmts.get("balance",  pd.DataFrame()),
                  BALANCE_METRICS,  "balance",  "Balance Sheet")
        _stmt_tab(tab_cf,  stmts.get("cashflow", pd.DataFrame()),
                  CASHFLOW_METRICS, "cashflow", "Cash Flow Statement")
        _stmt_tab(tab_drv, stmts.get("derived",  pd.DataFrame()),
                  DERIVED_METRICS,  "derived",  "Derived Metrics")

        # ── Price metrics diagnostics (inside Derived Metrics tab) ────────────
        with tab_drv:
            dbg = stmts.get("price_debug", {})
            if dbg:
                with st.expander("🔍 Price metrics diagnostics", expanded=False):
                    for k, v in dbg.items():
                        st.markdown(f"**{k}**: {v}")

        # ── Compare Companies tab ─────────────────────────────────────────────
        with tab_cmp:
            if len(kpi_tickers) < 2:
                st.info(
                    "Load two or more companies to compare them here.  \n"
                    "Example: type **AAPL, MSFT, GOOGL** and click 📊 Load."
                )
            else:
                cmp_metric = st.selectbox(
                    "Metric to compare",
                    options=ALL_STD_METRICS,
                    key="kpi_cmp_metric",
                )

                # Gather series from each company
                cmp_series: dict[str, pd.Series] = {}
                for tk in kpi_tickers:
                    s = get_stmt_series(kpi_stmts.get(tk, {}), cmp_metric)
                    if s is not None and not s.empty:
                        cmp_series[tk] = s

                if not cmp_series:
                    st.warning(f"No data found for **{cmp_metric}** in any loaded company.")
                else:
                    meta   = METRIC_DISPLAY.get(cmp_metric, {"fmt": "usd_b"})
                    negate = meta.get("negate", False)
                    fmt    = meta.get("fmt", "usd_b")

                    # Chart
                    fig2 = go.Figure()
                    for ci, (tk, s) in enumerate(cmp_series.items()):
                        y = -s.astype(float) if negate else s.astype(float)
                        hover_vals = [fmt_stmt_val(v, fmt) for v in y]
                        color = CHART_COLORS[ci % len(CHART_COLORS)]
                        fig2.add_trace(go.Scatter(
                            x=s.index.astype(str), y=y, name=tk,
                            mode="lines+markers",
                            line=dict(color=color, width=2.5),
                            marker=dict(size=6, color=color,
                                        line=dict(width=1.5, color="white")),
                            customdata=hover_vals,
                            hovertemplate=(
                                f"<b>{tk}</b>  %{{customdata}}<br>"
                                "Year: %{x}<extra></extra>"
                            ),
                        ))
                    fig2.update_layout(
                        title=dict(text=f"<b>{cmp_metric}</b> — Multi-Company",
                                   font=dict(size=16, color="#0f172a")),
                        xaxis=dict(showgrid=True, gridcolor="#f1f5f9",
                                   title="Fiscal Year"),
                        yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
                        legend=dict(orientation="h", yanchor="bottom",
                                    y=1.02, xanchor="right", x=1),
                        plot_bgcolor="white", paper_bgcolor="white",
                        margin=dict(l=0, r=0, t=50, b=0), height=380,
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                    # Side-by-side table
                    cmp_table = pd.DataFrame(
                        {tk: s.apply(lambda v: fmt_stmt_val(
                            (-v if negate else v), fmt))
                         for tk, s in cmp_series.items()}
                    ).sort_index(ascending=False)
                    cmp_table.index.name = "Year"
                    st.dataframe(cmp_table, use_container_width=True)

                    csv2 = pd.DataFrame(cmp_series).sort_index(ascending=False).to_csv().encode("utf-8")
                    st.download_button(
                        "⬇ Download comparison CSV",
                        data=csv2,
                        file_name=f"{'_'.join(kpi_tickers)}_{cmp_metric.replace('/','-')}.csv",
                        mime="text/csv",
                        key="dl_cmp",
                    )

        # ── Methodology tab ───────────────────────────────────────────────────
        with tab_meth:
            st.markdown(METHODOLOGY_MD)

    # ══════════════════════════════════════════════════════════════════════════
    #  MODE B — RAW KPI EXPLORER
    # ══════════════════════════════════════════════════════════════════════════
    else:
        # ── Build concept list ────────────────────────────────────────────────
        all_concepts   = get_all_concepts(list(kpi_facts.values()))
        concept_paths  = [p for p, _ in all_concepts]
        concept_labels = [
            f"⭐ {lbl}  ·  {p}" if p in POPULAR_KPIS else f"{lbl}  ·  {p}"
            for p, lbl in all_concepts
        ]

        # ── Controls row ──────────────────────────────────────────────────────────
        ctrl1, ctrl2, ctrl3 = st.columns([4, 2, 1.5])

        with ctrl1:
            default_idx = 0
            if st.session_state.kpi_concept in concept_paths:
                default_idx = concept_paths.index(st.session_state.kpi_concept)

            sel_idx = st.selectbox(
                "KPI",
                options=range(len(concept_paths)),
                format_func=lambda i: concept_labels[i],
                index=default_idx,
                key="kpi_concept_selector",
            )
            st.session_state.kpi_concept = concept_paths[sel_idx]

        with ctrl2:
            period_type = st.radio(
                "Period type",
                options=["annual", "quarterly"],
                format_func=lambda x: "📅 Annual" if x == "annual" else "📆 Quarterly",
                index=0 if st.session_state.kpi_period == "annual" else 1,
                horizontal=True,
                key="kpi_period_radio",
            )
            st.session_state.kpi_period = period_type

        with ctrl3:
            normalize = st.checkbox(
                "Index to 100",
                value=False,
                key="kpi_normalize",
                help="Normalize each company to 100 at a chosen base year — useful for relative growth comparison",
            )
            show_yoy = st.checkbox(
                "Show YoY %",
                value=False,
                key="kpi_yoy",
                help="Display Year-over-Year percentage change instead of absolute values",
            )

        concept_path = st.session_state.kpi_concept

        # ── Gather series data ────────────────────────────────────────────────────
        series_data: dict[str, pd.DataFrame] = {}
        unit_str  = ""
        label_str = ""

        for tk in kpi_tickers:
            facts = kpi_facts.get(tk)
            if not facts:
                continue
            df, label, unit = get_concept_series(facts, concept_path, period_type)
            label_str = label   # same concept → same label across companies
            unit_str  = unit
            if not df.empty:
                series_data[tk] = df

        # ── No data fallback ──────────────────────────────────────────────────────
        if not series_data:
            period_word = "annual" if period_type == "annual" else "quarterly"
            st.warning(
                f"No {period_word} data found for **{concept_path.split('/')[-1]}** "
                f"in the loaded companies. "
                "Try switching between Annual / Quarterly, or choose a different KPI."
            )
            st.stop()

        # ── Date-range + base-year controls ──────────────────────────────────────
        all_years = sorted({
            int(row["date"].year)
            for df in series_data.values()
            for _, row in df.iterrows()
        })

        if len(all_years) >= 2:
            yr_min, yr_max = all_years[0], all_years[-1]

            # Show year-range slider and optional base-year picker in the same row
            if normalize:
                range_col, base_col = st.columns([3, 2])
            else:
                range_col, base_col = st.columns([3, 2])   # base_col unused when not normalizing

            with range_col:
                yr_from, yr_to = st.slider(
                    "Year range",
                    min_value=yr_min,
                    max_value=yr_max,
                    value=(yr_min, yr_max),
                    key="kpi_year_range",
                )

            base_year = yr_from   # default: first year of the visible range
            if normalize:
                with base_col:
                    # Only show years within the selected range
                    base_year = st.select_slider(
                        "Index base year  (= 100)",
                        options=list(range(yr_from, yr_to + 1)),
                        value=yr_from,
                        key="kpi_base_year",
                    )

            # Apply year-range filter
            series_data = {
                tk: df[(df["date"].dt.year >= yr_from) & (df["date"].dt.year <= yr_to)]
                for tk, df in series_data.items()
            }
            series_data = {tk: df for tk, df in series_data.items() if not df.empty}
        else:
            base_year = all_years[0] if all_years else None

        if not series_data:
            st.info("No data in the selected year range.")
            st.stop()

        # ── Plotly chart ──────────────────────────────────────────────────────────
        fig = go.Figure()
        index_warnings: list[str] = []   # collect per-company issues for normalize mode

        for i, (tk, df) in enumerate(series_data.items()):
            color  = CHART_COLORS[i % len(CHART_COLORS)]
            y_vals = df["value"].copy().astype(float)

            if normalize:
                # Find value at the chosen base year (exact match → nearest → first positive)
                base_rows = df[df["date"].dt.year == base_year]
                if not base_rows.empty:
                    base_val = float(base_rows.iloc[0]["value"])
                    base_lbl = f"{base_year}"
                else:
                    # No data at base_year; fall back to first positive value
                    pos = df[df["value"] > 0]
                    if pos.empty:
                        index_warnings.append(
                            f"**{tk}**: all values are ≤ 0 — cannot index to 100, shown as-is."
                        )
                        base_val = None
                    else:
                        base_val = float(pos.iloc[0]["value"])
                        base_lbl = str(int(pos.iloc[0]["date"].year))
                        index_warnings.append(
                            f"**{tk}**: no data for {base_year}, indexed from first positive year "
                            f"({base_lbl} = {fmt_value(base_val, unit_str)})."
                        )

                if base_val and base_val != 0:
                    y_vals = y_vals / base_val * 100
                elif base_val == 0:
                    index_warnings.append(f"**{tk}**: value is exactly 0 in {base_year} — cannot index.")

            if show_yoy:
                pct = y_vals.pct_change() * 100
                pct.iloc[0] = None   # no prior year for first point
                y_plot = pct
            else:
                y_plot = y_vals

            # Custom hover: always show the raw formatted value
            custom = df["value"].apply(lambda v: fmt_value(v, unit_str)).tolist()

            if show_yoy:
                hover = (
                    f"<b>{tk}</b>  YoY: %{{y:.1f}}%<br>"
                    f"Raw: %{{customdata}}<br>"
                    f"Date: %{{x|%Y-%m-%d}}<extra></extra>"
                )
            elif normalize:
                hover = (
                    f"<b>{tk}</b>  Index: %{{y:.1f}}<br>"
                    f"Raw: %{{customdata}}<br>"
                    f"Date: %{{x|%Y-%m-%d}}<extra></extra>"
                )
            else:
                hover = (
                    f"<b>{tk}</b>  %{{customdata}}<br>"
                    f"Date: %{{x|%Y-%m-%d}}<extra></extra>"
                )

            fig.add_trace(go.Scatter(
                x=df["date"],
                y=y_plot,
                name=tk,
                mode="lines+markers",
                line=dict(color=color, width=2.5),
                marker=dict(size=6, color=color, line=dict(width=1.5, color="white")),
                customdata=custom,
                hovertemplate=hover,
            ))

        # Show index warnings if any
        if index_warnings:
            st.info("ℹ️ " + "  \n".join(index_warnings))

        # Y-axis config
        if show_yoy:
            ytitle = "YoY Growth (%)"
            yfmt   = ".1f"
            ysuf   = "%"
        elif normalize:
            ytitle = f"Indexed (base year {base_year} = 100)"
            yfmt   = ".0f"
            ysuf   = ""
        else:
            ytitle = f"{label_str}  ({unit_str})"
            yfmt   = yaxis_tickformat(unit_str)
            ysuf   = ""

        fig.update_layout(
            title=dict(text=f"<b>{label_str}</b>", font=dict(size=18, color="#0f172a")),
            xaxis=dict(
                showgrid=True, gridcolor="#f1f5f9",
                tickformat="%Y", title="",
            ),
            yaxis=dict(
                title=ytitle, showgrid=True, gridcolor="#f1f5f9",
                tickformat=yfmt, ticksuffix=ysuf,
                zeroline=True, zerolinecolor="#cbd5e1", zerolinewidth=1.5,
            ),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02,
                xanchor="right", x=1,
            ),
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(l=0, r=0, t=56, b=0),
            height=430,
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)

        # ── Data coverage note ────────────────────────────────────────────────────
        coverage_parts = []
        for tk, df in series_data.items():
            if df.empty:
                continue
            yr_start = int(df["date"].dt.year.min())
            yr_end   = int(df["date"].dt.year.max())
            n        = len(df)
            coverage_parts.append(f"**{tk}** {yr_start}–{yr_end} ({n} pts)")
        if coverage_parts:
            st.caption(
                "📅 Data coverage for this KPI:  " + "  ·  ".join(coverage_parts)
                + "  —  Missing recent years? Click **🔄 Refresh** above to fetch latest EDGAR data."
            )

        # ── Metric cards (one per company) ────────────────────────────────────────
        if not show_yoy and not normalize and series_data:
            st.markdown("---")
            st.markdown("#### 📊 Key Metrics")

            metric_cols = st.columns(max(len(series_data), 1))
            for i, (tk, df) in enumerate(series_data.items()):
                cagr_all = calc_cagr(df)
                cagr_5y  = calc_cagr(df, 5)
                cagr_3y  = calc_cagr(df, 3)
                latest   = float(df.iloc[-1]["value"]) if not df.empty else None
                latest_y = int(df.iloc[-1]["date"].year) if not df.empty else "—"
                n_pts    = len(df)

                cagr_all_str = f"{cagr_all*100:+.1f}%" if cagr_all is not None else "N/A"
                cagr_5y_str  = f"{cagr_5y*100:+.1f}%"  if cagr_5y  is not None else "N/A"
                cagr_3y_str  = f"{cagr_3y*100:+.1f}%"  if cagr_3y  is not None else "N/A"

                card_cls = ""
                if cagr_all is not None:
                    card_cls = "kpi-card-green" if cagr_all >= 0 else "kpi-card-red"

                with metric_cols[i]:
                    st.markdown(
                        f"<div class='kpi-card {card_cls}'>"
                        f"<div class='kpi-label'>{tk}</div>"
                        f"<div class='kpi-value'>{fmt_value(latest, unit_str)}</div>"
                        f"<div class='kpi-sub'>Latest ({latest_y}) · {n_pts} data points</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    m1, m2, m3 = st.columns(3)
                    m1.metric("CAGR (all)", cagr_all_str)
                    m2.metric("CAGR (5Y)",  cagr_5y_str)
                    m3.metric("CAGR (3Y)",  cagr_3y_str)

        # ── Data table with YoY ───────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 📋 Data Table")

        table_parts = []
        for tk, df in series_data.items():
            tdf = df[["date", "value"]].copy()
            tdf["Ticker"]  = tk
            tdf["Year"]    = tdf["date"].dt.year.astype(int)
            tdf["YoY %"]   = tdf["value"].pct_change() * 100
            tdf["Value"]   = tdf["value"].apply(lambda v: fmt_value(v, unit_str))
            tdf["YoY %"]   = tdf["YoY %"].apply(
                lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
            )
            table_parts.append(tdf[["Ticker", "Year", "Value", "YoY %"]])

        if table_parts:
            combined = pd.concat(table_parts, ignore_index=True)
            combined = combined.sort_values(["Ticker", "Year"], ascending=[True, False])

            st.dataframe(
                combined,
                use_container_width=True,
                hide_index=True,
                height=max(200, min(600, 40 + 35 * len(combined))),
            )

            csv_bytes = combined.to_csv(index=False).encode("utf-8")
            safe_concept = concept_path.replace("/", "_").replace(" ", "_")
            st.download_button(
                "⬇ Download table as CSV",
                data=csv_bytes,
                file_name=f"{'_'.join(kpi_tickers)}_{safe_concept}.csv",
                mime="text/csv",
            )


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: DCF MODEL
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💰  Model DCF":

    # ── Guard: need companies loaded ─────────────────────────────────────────
    dcf_tickers: list = st.session_state.get("kpi_tickers", [])
    dcf_stmts:   dict = st.session_state.get("kpi_stmts",   {})
    dcf_prices:  dict = st.session_state.get("kpi_prices",  {})

    if not dcf_tickers or not dcf_stmts:
        st.markdown("## 💰 Model DCF")
        st.info(
            "Load companies in **📈 KPI Explorer** first → click 📈 KPI Explorer "
            "in the navigation bar above, enter a ticker, and click 📊 Load."
        )
        st.stop()

    # ── Company selector ─────────────────────────────────────────────────────
    if len(dcf_tickers) > 1:
        dcf_tk = st.selectbox(
            "Select company",
            options=dcf_tickers,
            key="dcf_company_sel",
        )
    else:
        dcf_tk = dcf_tickers[0]

    # ── Page header with company name ─────────────────────────────────────────
    _dcf_facts   = st.session_state.get("kpi_facts", {})
    _dcf_ename   = _dcf_facts.get(dcf_tk, {}).get("entityName", dcf_tk)
    st.markdown(
        f"## 💰 DCF Model — "
        f"<span style='color:#0369a1'>{_dcf_ename}</span> "
        f"<span style='font-size:0.75em;color:#94a3b8;font-weight:400'>({dcf_tk})</span>",
        unsafe_allow_html=True,
    )

    stmts_d = dcf_stmts.get(dcf_tk, {})
    prices_d = dcf_prices.get(dcf_tk, {})

    if not stmts_d:
        st.warning(f"No statement data found for **{dcf_tk}**. Load it in KPI Explorer first.")
        st.stop()

    # ── Gather latest price & market cap ─────────────────────────────────────
    price_series = _dcf_series(stmts_d, "Market Cap")
    mc_series    = price_series  # Market Cap already in derived

    # Latest market cap (most recent year)
    latest_mc = None
    if not mc_series.empty:
        latest_mc = float(mc_series.sort_index(ascending=False).iloc[0])

    # Latest stock price from kpi_prices
    latest_price = None
    if prices_d:
        px_s = pd.Series({int(k): float(v) for k, v in prices_d.items()}, dtype=float)
        if not px_s.empty:
            latest_price = float(px_s.sort_index(ascending=False).iloc[0])

    # ── FCF: prefer derived["FCF"], else bridge ───────────────────────────────
    fcf_series = _dcf_series(stmts_d, "FCF")
    latest_fcf = None
    if not fcf_series.empty:
        latest_fcf = float(fcf_series.sort_index(ascending=False).iloc[0])

    # ── DPS (Dividends Per Share) ─────────────────────────────────────────────
    div_series = _dcf_series(stmts_d, "Dividends Paid")
    sh_series  = _dcf_series(stmts_d, "Diluted Shares")
    latest_dps = None
    if not div_series.empty and not sh_series.empty:
        yr_div = div_series.sort_index(ascending=False)
        yr_sh  = sh_series.sort_index(ascending=False)
        common = sorted(set(yr_div.index) & set(yr_sh.index), reverse=True)
        if common:
            yr0 = common[0]
            d0  = yr_div.get(yr0)
            s0  = yr_sh.get(yr0)
            if d0 is not None and s0 is not None and s0 != 0:
                latest_dps = abs(float(d0)) / abs(float(s0))

    # ── Net Debt & Enterprise Value ───────────────────────────────────────────
    # Net Debt = Long Term Debt − Cash  (negative ⟹ net cash position)
    # Enterprise Value (EV) = Market Cap (equity) + Net Debt
    # EV is the correct denominator for FCFF-based models discounted at WACC.
    nd_series  = _dcf_series(stmts_d, "Net Debt")
    ltd_series = _dcf_series(stmts_d, "Long Term Debt")
    csh_series = _dcf_series(stmts_d, "Cash")

    latest_net_debt: float | None = None
    if not nd_series.empty:
        latest_net_debt = float(nd_series.sort_index(ascending=False).iloc[0])
    elif not ltd_series.empty:
        ltd0 = float(ltd_series.sort_index(ascending=False).iloc[0])
        csh0 = float(csh_series.sort_index(ascending=False).iloc[0]) if not csh_series.empty else 0.0
        latest_net_debt = ltd0 - csh0

    # EV = Market Cap + Net Debt  (if net cash, net_debt < 0, so EV < MC)
    latest_ev: float | None = None
    if latest_mc is not None:
        nd_adj = latest_net_debt if latest_net_debt is not None else 0.0
        latest_ev = latest_mc + nd_adj

    # ── Auto-compute WACC defaults ─────────────────────────────────────────────
    beta_auto = load_beta(dcf_tk)
    rf_def    = 4.5   # %
    erp_def   = 5.5   # %
    mc_for_wacc = latest_mc if latest_mc and latest_mc > 0 else 1e11
    wacc_auto_dict = compute_wacc_auto(
        stmts_d, mc_for_wacc, beta_auto, rf_def / 100, erp_def / 100
    )

    # ── Inner tabs: Reverse DCF  |  Forward DCF ──────────────────────────────
    tab_rev, tab_fwd = st.tabs(["⏪ Reverse DCF", "📊 Forward DCF Model"])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — REVERSE DCF
    # ══════════════════════════════════════════════════════════════════════════
    with tab_rev:

        # ── WACC Inputs expander ──────────────────────────────────────────────
        with st.expander("⚙️ WACC & Discount Rate Inputs", expanded=True):

            st.markdown("**Cost of Equity (CAPM)**")
            coe_c1, coe_c2, coe_c3 = st.columns(3)

            with coe_c1:
                dcf_rf = st.number_input(
                    "Risk-Free Rate %",
                    min_value=0.0, max_value=20.0,
                    value=rf_def, step=0.1, format="%.1f",
                    key="dcf_rf",
                )
            with coe_c2:
                dcf_beta = st.number_input(
                    "Beta",
                    min_value=0.0, max_value=5.0,
                    value=round(beta_auto, 2), step=0.05, format="%.2f",
                    key="dcf_beta",
                )
            with coe_c3:
                dcf_erp = st.number_input(
                    "Equity Risk Premium %",
                    min_value=0.0, max_value=20.0,
                    value=erp_def, step=0.1, format="%.1f",
                    key="dcf_erp",
                )

            r_e_computed = (dcf_rf + dcf_beta * dcf_erp) / 100
            st.info(
                f"**Cost of Equity** = Risk-Free Rate + Beta × ERP = "
                f"{dcf_rf:.1f}% + {dcf_beta:.2f} × {dcf_erp:.1f}% = "
                f"**{r_e_computed*100:.2f}%**"
            )

            st.markdown("**Cost of Debt & Tax Rate**")
            cod_c1, cod_c2, cod_c3 = st.columns(3)

            with cod_c1:
                dcf_cod = st.number_input(
                    "Cost of Debt %",
                    min_value=0.0, max_value=25.0,
                    value=round(wacc_auto_dict["r_d"] * 100, 1),
                    step=0.1, format="%.1f",
                    key="dcf_cod",
                )
            with cod_c2:
                dcf_tax = st.number_input(
                    "Tax Rate %",
                    min_value=0.0, max_value=50.0,
                    value=round(wacc_auto_dict["tax_rate"] * 100, 1),
                    step=0.5, format="%.1f",
                    key="dcf_tax",
                )
            with cod_c3:
                st.metric(
                    "Equity Weight",
                    f"{wacc_auto_dict['e_wt']*100:.1f}%",
                    delta=f"Debt: {wacc_auto_dict['d_wt']*100:.1f}%",
                )

            # Recompute WACC with user inputs
            r_e_user  = r_e_computed
            r_d_user  = dcf_cod / 100
            tax_user  = dcf_tax / 100
            e_wt_user = wacc_auto_dict["e_wt"]
            d_wt_user = wacc_auto_dict["d_wt"]
            wacc_user = e_wt_user * r_e_user + d_wt_user * r_d_user * (1.0 - tax_user)

            st.markdown(
                f"**WACC** = {e_wt_user*100:.1f}% × {r_e_user*100:.2f}% + "
                f"{d_wt_user*100:.1f}% × {r_d_user*100:.1f}% × (1 − {tax_user*100:.1f}%) = "
                f'<span style="font-size:1.15em;font-weight:700;color:#3b82f6">'
                f"**{wacc_user*100:.2f}%**</span>",
                unsafe_allow_html=True,
            )

        # ── Key metrics row ───────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### Key Inputs")

        km1, km2, km3, km4, km5, km6 = st.columns(6)
        km1.metric("Stock Price (latest)",
                   f"${latest_price:,.2f}" if latest_price else "—")
        km2.metric("Market Cap (Equity)",
                   f"${latest_mc/1e9:.1f}B" if latest_mc else "—")
        km3.metric("Net Debt",
                   (f"${latest_net_debt/1e9:.1f}B" if latest_net_debt is not None else "—"),
                   delta="Net Cash" if (latest_net_debt is not None and latest_net_debt < 0) else None)
        km4.metric("Enterprise Value",
                   f"${latest_ev/1e9:.1f}B" if latest_ev else "—",
                   help="EV = Market Cap + Net Debt. Used as denominator in FCF/WACC reverse DCF.")
        km5.metric("Latest FCF",
                   f"${latest_fcf/1e9:.1f}B" if latest_fcf is not None else "—")
        km6.metric("DPS (latest)",
                   f"${latest_dps:.2f}" if latest_dps else "—")

        # ── Reverse DCF Results ───────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### ⏪ Implied Growth Rates")

        rcol1, rcol2 = st.columns(2)

        # Model A — FCF (discounted at WACC)  →  solves for EV-implied growth
        with rcol1:
            st.markdown("##### Model A — FCF / WACC")
            st.info(
                "**EV** = FCF × (1+g) / (WACC − g)\n\n"
                "Solving: **g = (EV × WACC − FCF) / (EV + FCF)**\n\n"
                "where EV = Market Cap + Net Debt"
            )
            if latest_ev and latest_fcf is not None and wacc_user > 0:
                g_fcf = reverse_dcf_fcf(latest_ev, latest_fcf, wacc_user)
                if g_fcf is not None:
                    color = "#10b981" if g_fcf >= 0 else "#ef4444"
                    st.markdown(
                        f'<div style="text-align:center;padding:18px;'
                        f'background:#f0f9ff;border-radius:10px;'
                        f'border:2px solid #3b82f6">'
                        f'<p style="margin:0;color:#64748b;font-size:13px">'
                        f'Implied FCF Growth Rate (EV-based)</p>'
                        f'<p style="margin:4px 0 0;font-size:2.2em;'
                        f'font-weight:800;color:{color}">'
                        f'{g_fcf*100:+.1f}%</p>'
                        f'<p style="margin:4px 0 0;font-size:11px;color:#94a3b8">'
                        f'EV ${latest_ev/1e9:.1f}B = MC ${latest_mc/1e9:.1f}B'
                        f' + Net Debt ${(latest_net_debt or 0)/1e9:.1f}B</p>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.warning("Cannot compute — check inputs (WACC > g required)")
            else:
                st.caption("Requires Enterprise Value, FCF, and WACC > 0")

        # Model B — DDM (discounted at Cost of Equity)
        with rcol2:
            st.markdown("##### Model B — Dividends / DDM")
            st.info(
                "Price = DPS × (1+g) / (Cost of Equity − g)\n\n"
                "Solving: **g = (P × r_e − DPS) / (P + DPS)**"
            )
            if latest_dps and latest_dps > 0 and latest_price and r_e_user > 0:
                g_ddm = reverse_dcf_ddm(latest_price, latest_dps, r_e_user)
                if g_ddm is not None:
                    color = "#10b981" if g_ddm >= 0 else "#ef4444"
                    st.markdown(
                        f'<div style="text-align:center;padding:18px;'
                        f'background:#f0fff4;border-radius:10px;'
                        f'border:2px solid #10b981">'
                        f'<p style="margin:0;color:#64748b;font-size:13px">'
                        f'Implied Dividend Growth Rate</p>'
                        f'<p style="margin:4px 0 0;font-size:2.2em;'
                        f'font-weight:800;color:{color}">'
                        f'{g_ddm*100:+.1f}%</p>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.warning("Cannot compute — check inputs (r_e > g required)")
            else:
                st.markdown(
                    '<div style="text-align:center;padding:18px;'
                    'background:#f8fafc;border-radius:10px;'
                    'border:1px dashed #cbd5e1;color:#94a3b8">'
                    'Company does not pay dividends</div>',
                    unsafe_allow_html=True,
                )

        # ── FCF Model Sensitivity Table ───────────────────────────────────────
        if latest_ev and latest_fcf is not None and wacc_user > 0:
            st.markdown("---")
            st.markdown("#### 🔢 Sensitivity — Implied FCF Growth Rate")
            st.caption(
                "Rows = WACC ± 1pp / ±0.5pp | "
                "Columns = Enterprise Value ± 10% / ±5%"
            )

            wacc_offsets  = [-0.010, -0.005, 0.000, +0.005, +0.010]
            ev_multipliers = [0.90, 0.95, 1.00, 1.05, 1.10]

            sens_rows = []
            for w_off in wacc_offsets:
                w_val = wacc_user + w_off
                row_data = {"WACC": f"{w_val*100:.2f}%"}
                for ev_mult in ev_multipliers:
                    ev_var = latest_ev * ev_mult
                    g_var  = reverse_dcf_fcf(ev_var, latest_fcf, w_val)
                    row_data[f"EV ×{ev_mult:.2f}"] = (
                        f"{g_var*100:+.1f}%" if g_var is not None else "—"
                    )
                sens_rows.append(row_data)

            sens_df = pd.DataFrame(sens_rows).set_index("WACC")
            st.dataframe(sens_df, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — FORWARD DCF MODEL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_fwd:

        # tax_user is set in tab_rev WACC inputs above; default 21% if not yet rendered
        _bridge_tax = tax_user if "tax_user" in dir() else (wacc_auto_dict["tax_rate"])
        bridge_df = build_fcf_bridge(stmts_d, n=7, tax_rate=_bridge_tax)

        if bridge_df.empty:
            st.warning(
                "Not enough Revenue data to build the FCF bridge for this company. "
                "Make sure it is loaded in KPI Explorer."
            )
        else:
            # ── FCF Bridge table (HTML styled) ───────────────────────────────
            st.markdown("#### 📊 FCF Bridge — Historical")

            bridge_years = list(bridge_df.index)   # newest first
            # Tuple: (label, df_col, negate_display, is_subtotal)
            # negate_display=True ⟹ show value as negative (deduction row)
            bridge_metrics = [
                ("Revenue",                     "Revenue",  False, True),
                ("COGS",                        "COGS",     False, False),
                ("SG&A",                        "SGA",      False, False),
                ("EBITDA*",                     "EBITDA_s", False, True),
                (f"− D&A",                      "DA",       True,  False),
                ("EBIT",                        "EBIT",     False, True),
                (f"Taxes ({_bridge_tax*100:.0f}%)", "Taxes", True,  False),
                ("NOPAT",                       "NOPAT",    False, True),
                ("+ D&A (add back)",            "DA",       False, False),
                ("− CapEx",                     "CapEx",    True,  False),
                ("ΔNWC",                        "dNWC",     False, False),
                ("FCFF",                        "FCF",      False, True),
            ]

            def _fmt_b(v, negate: bool = False) -> str:
                """Format as $XB. negate=True forces display as negative (deduction)."""
                if v is None or (isinstance(v, float) and (pd.isna(v))):
                    return "—"
                v = float(v)
                if negate:
                    v = -abs(v)   # force negative display regardless of stored sign
                neg = v < 0
                a   = abs(v)
                s   = (f"${a/1e12:.2f}T" if a >= 1e12 else
                       f"${a/1e9:.1f}B"  if a >= 1e9  else
                       f"${a/1e6:.0f}M"  if a >= 1e6  else f"${a:,.0f}")
                return f"({s})" if neg else s

            def _fmt_pct(cur, prev) -> str:
                if cur is None or prev is None:
                    return ""
                try:
                    cur_f, prev_f = float(cur), float(prev)
                    if prev_f == 0:
                        return ""
                    yoy = (cur_f - prev_f) / abs(prev_f) * 100
                    color = "#10b981" if yoy >= 0 else "#ef4444"
                    return (
                        f'<span style="font-size:10px;color:{color}">'
                        f'{yoy:+.1f}%</span>'
                    )
                except Exception:
                    return ""

            # Build HTML table header
            yr_heads_b = "".join(
                f'<th style="text-align:right;padding:6px 12px;color:#475569;'
                f'font-weight:600;font-size:12px;white-space:nowrap">{y}</th>'
                for y in bridge_years
            )
            bridge_header = (
                '<tr style="background:#f1f5f9;border-bottom:2px solid #cbd5e1">'
                '<th style="text-align:left;padding:6px 12px;color:#475569;'
                'font-weight:600;font-size:12px;white-space:nowrap">Metric ($B)</th>'
                + yr_heads_b + "</tr>"
            )

            bridge_rows_html = []
            # Rows that get a top separator (new subtotal section)
            _sep_cols = {"EBIT", "NOPAT", "FCF"}

            for display_lbl, col, negate_disp, is_key in bridge_metrics:
                if col not in bridge_df.columns:
                    continue
                cells_b = []
                yr_vals = {yr: bridge_df.loc[yr, col] if yr in bridge_df.index else None
                           for yr in bridge_years}

                # Build cells newest-first
                for i, yr in enumerate(bridge_years):
                    val    = yr_vals.get(yr)
                    prev_yr = bridge_years[i + 1] if i + 1 < len(bridge_years) else None
                    prev_v  = yr_vals.get(prev_yr) if prev_yr else None
                    # Show YoY only on subtotal/result rows, not deductions
                    show_yoy = is_key and col not in ("dNWC",)
                    yoy_str  = _fmt_pct(val, prev_v) if show_yoy else ""
                    val_str  = _fmt_b(val, negate=negate_disp)
                    weight   = "font-weight:700;" if is_key else ""
                    # Background hints per row type
                    bg_hint = ""
                    if col == "EBITDA_s": bg_hint = "background:#eff6ff;"
                    elif col == "EBIT":   bg_hint = "background:#fef9c3;"
                    elif col == "NOPAT":  bg_hint = "background:#fef3c7;"
                    elif col == "FCF":    bg_hint = "background:#f0fdf4;"
                    cells_b.append(
                        f'<td style="text-align:right;padding:5px 12px;'
                        f'{weight}{bg_hint}white-space:nowrap">'
                        f'{val_str}'
                        f'{"<br>" + yoy_str if yoy_str else ""}'
                        f'</td>'
                    )

                # Top separator before key subtotal rows
                sep    = "border-top:2px solid #cbd5e1;" if col in _sep_cols else ""
                row_bg = "background:#f0f9ff;" if is_key else ""
                lbl_color = "#0f172a"
                bridge_rows_html.append(
                    f'<tr style="{row_bg}{sep}">'
                    f'<td style="padding:5px 12px;color:{lbl_color};white-space:nowrap;'
                    f'{"font-weight:700;" if is_key else "color:#475569;"}">'
                    f'{display_lbl}</td>'
                    + "".join(cells_b) + "</tr>"
                )

            bridge_html = (
                '<div style="overflow-x:auto;margin-bottom:8px">'
                '<table style="width:100%;border-collapse:collapse;font-size:13px">'
                f"<thead>{bridge_header}</thead>"
                f"<tbody>{''.join(bridge_rows_html)}</tbody>"
                "</table></div>"
                '<p style="font-size:11px;color:#94a3b8;margin-top:4px">'
                "* EBITDA(simplified) = Revenue − COGS − SG&A. "
                f"EBIT = EBITDA − D&A. "
                f"Taxes = EBIT × {_bridge_tax*100:.0f}% (effective rate). "
                "NOPAT = EBIT × (1 − Tax Rate). "
                "FCFF = NOPAT + D&A − CapEx − ΔNWC.</p>"
            )
            st.markdown(bridge_html, unsafe_allow_html=True)

            # ── Projection Assumptions ────────────────────────────────────────
            st.markdown("---")
            st.markdown("#### ⚙️ Projection Assumptions")

            # Propose default growth from last 3 yrs avg FCF YoY
            # Prefer derived FCF series (Operating CF - CapEx); fall back to bridge
            _fcf_for_g = fcf_series.sort_index(ascending=False)
            if _fcf_for_g.empty:
                _fcf_for_g = bridge_df["FCF"].dropna().sort_index(ascending=False)
            fcf_vals   = _fcf_for_g.dropna()
            proposed_g = 5.0   # fallback default %
            if len(fcf_vals) >= 3:
                try:
                    yoy_rates = []
                    for i in range(len(fcf_vals) - 1):
                        cur_v  = float(fcf_vals.iloc[i])
                        prev_v = float(fcf_vals.iloc[i + 1])
                        if prev_v != 0 and not pd.isna(cur_v) and not pd.isna(prev_v):
                            yoy_rates.append((cur_v - prev_v) / abs(prev_v) * 100)
                    if yoy_rates:
                        avg_g = float(pd.Series(yoy_rates[:3]).mean())
                        proposed_g = round(max(-5.0, min(35.0, avg_g)), 1)
                except Exception:
                    pass

            pa_c1, pa_c2, pa_c3, pa_c4 = st.columns(4)
            with pa_c1:
                fwd_g1 = st.number_input(
                    "Year 1 Growth %",
                    min_value=-50.0, max_value=100.0,
                    value=proposed_g, step=0.5, format="%.1f",
                    key="dcf_fwd_g1",
                )
            with pa_c2:
                fwd_g2 = st.number_input(
                    "Year 2 Growth %",
                    min_value=-50.0, max_value=100.0,
                    value=proposed_g, step=0.5, format="%.1f",
                    key="dcf_fwd_g2",
                )
            with pa_c3:
                fwd_g3 = st.number_input(
                    "Year 3 Growth %",
                    min_value=-50.0, max_value=100.0,
                    value=max(proposed_g * 0.7, 2.0), step=0.5, format="%.1f",
                    key="dcf_fwd_g3",
                )
            with pa_c4:
                fwd_gt = st.number_input(
                    "Terminal Growth %",
                    min_value=0.0, max_value=10.0,
                    value=3.0, step=0.1, format="%.1f",
                    key="dcf_fwd_gt",
                )

            # ── DCF Output Table ──────────────────────────────────────────────
            st.markdown("---")
            st.markdown("#### 📋 DCF Projection")

            base_fcf = latest_fcf if latest_fcf is not None else 0.0
            growth_rates = [fwd_g1 / 100, fwd_g2 / 100, fwd_g3 / 100]
            terminal_g   = fwd_gt / 100
            wacc_fwd     = wacc_user

            # Project FCFs
            proj_fcf = []
            fcf_cur  = base_fcf
            for g in growth_rates:
                fcf_cur = fcf_cur * (1 + g)
                proj_fcf.append(fcf_cur)

            # Terminal value (Gordon Growth)
            if wacc_fwd > terminal_g:
                terminal_fcf = proj_fcf[-1] * (1 + terminal_g)
                tv = terminal_fcf / (wacc_fwd - terminal_g)
            else:
                tv = None

            # Present values
            pv_fcfs = []
            for t_idx, fcf_t in enumerate(proj_fcf, start=1):
                pv = fcf_t / ((1 + wacc_fwd) ** t_idx)
                pv_fcfs.append(pv)

            pv_tv = tv / ((1 + wacc_fwd) ** len(proj_fcf)) if tv is not None else None

            # Build output table
            out_cols = ["Base"] + [f"Year {i}" for i in range(1, len(proj_fcf) + 1)] + ["Terminal"]
            fcf_row  = (
                [f"${base_fcf/1e9:.2f}B"]
                + [f"${v/1e9:.2f}B" for v in proj_fcf]
                + [f"${tv/1e9:.2f}B" if tv is not None else "—"]
            )
            g_row = (
                ["—"]
                + [f"{g*100:.1f}%" for g in growth_rates]
                + [f"{terminal_g*100:.1f}%"]
            )
            pv_factor_row = (
                ["1.000"]
                + [f"{1/((1+wacc_fwd)**t):.4f}" for t in range(1, len(proj_fcf) + 1)]
                + [f"{1/((1+wacc_fwd)**len(proj_fcf)):.4f}" if pv_tv is not None else "—"]
            )
            pv_row = (
                ["—"]
                + [f"${v/1e9:.2f}B" for v in pv_fcfs]
                + [f"${pv_tv/1e9:.2f}B" if pv_tv is not None else "—"]
            )

            proj_table = pd.DataFrame(
                {
                    "Metric":       ["FCF ($B)", "Growth Rate", "PV Factor", "Present Value ($B)"],
                    **{col: [fcf_row[i], g_row[i], pv_factor_row[i], pv_row[i]]
                       for i, col in enumerate(out_cols)},
                }
            ).set_index("Metric")

            st.dataframe(proj_table, use_container_width=True)

            # ── Intrinsic Value Summary ───────────────────────────────────────
            st.markdown("---")
            st.markdown("#### 💡 Intrinsic Value Summary")

            sum_pv_fcf = sum(pv_fcfs)
            # PV of FCFF discounted at WACC = Enterprise Value (EV)
            implied_ev = sum_pv_fcf + (pv_tv if pv_tv is not None else 0.0)

            # Bridge: Equity Value = EV − Net Debt
            nd_fwd = latest_net_debt if latest_net_debt is not None else 0.0
            equity_iv = implied_ev - nd_fwd

            # Per-share value
            sh_latest = None
            sh_s2 = _dcf_series(stmts_d, "Diluted Shares")
            if not sh_s2.empty:
                sh_latest = float(sh_s2.sort_index(ascending=False).iloc[0])

            iv_per_share = equity_iv / sh_latest if sh_latest and sh_latest > 0 else None

            # Row 1: DCF components (EV build-up)
            sv1, sv2, sv3 = st.columns(3)
            sv1.metric("Sum of PV (FCFs)",
                       f"${sum_pv_fcf/1e9:.1f}B")
            sv2.metric("PV of Terminal Value",
                       f"${pv_tv/1e9:.1f}B" if pv_tv is not None else "—")
            sv3.metric("Implied EV  (= PV of FCFF @ WACC)",
                       f"${implied_ev/1e9:.1f}B")

            # Row 2: EV → Equity bridge
            st.markdown(
                '<div style="background:#f8fafc;border:1px solid #e2e8f0;'
                'border-radius:10px;padding:14px 20px;margin:10px 0 4px">'
                '<p style="margin:0 0 8px;font-weight:700;color:#0f172a;font-size:13px">'
                '🔗 EV → Equity Bridge</p>'
                '<table style="width:100%;border-collapse:collapse;font-size:13px">'
                '<tr>'
                f'<td style="padding:4px 8px;color:#475569">Implied Enterprise Value (EV)</td>'
                f'<td style="padding:4px 8px;text-align:right;font-weight:600">'
                f'${implied_ev/1e9:.1f}B</td></tr>'
                f'<tr><td style="padding:4px 8px;color:#475569">'
                f'Less: Net Debt (Debt − Cash)</td>'
                f'<td style="padding:4px 8px;text-align:right;font-weight:600;color:#ef4444">'
                f'(${nd_fwd/1e9:.1f}B)</td></tr>'
                f'<tr style="border-top:2px solid #cbd5e1">'
                f'<td style="padding:6px 8px;font-weight:700;color:#0f172a">'
                f'= Intrinsic Equity Value</td>'
                f'<td style="padding:6px 8px;text-align:right;font-weight:700;color:#0f172a">'
                f'${equity_iv/1e9:.1f}B</td></tr>'
                f'<tr><td style="padding:4px 8px;color:#475569">'
                f'÷ Diluted Shares Outstanding</td>'
                f'<td style="padding:4px 8px;text-align:right;font-weight:600">'
                f'{sh_latest/1e6:.0f}M shares</td></tr>'
                f'<tr style="border-top:2px solid #3b82f6">'
                f'<td style="padding:6px 8px;font-weight:800;color:#1d4ed8;font-size:14px">'
                f'= Intrinsic Value / Share</td>'
                f'<td style="padding:6px 8px;text-align:right;font-weight:800;'
                f'color:#1d4ed8;font-size:14px">'
                f'${iv_per_share:.2f}</td></tr>'
                f'</table></div>'
                if (sh_latest and iv_per_share) else
                '<p style="color:#94a3b8">Cannot compute per-share value — missing share count.</p>',
                unsafe_allow_html=True,
            )

            # Upside / downside vs current price
            if latest_price and iv_per_share:
                upside = (iv_per_share - latest_price) / latest_price * 100
                color_up = "#10b981" if upside >= 0 else "#ef4444"
                label_up = "Upside" if upside >= 0 else "Downside"
                st.markdown(
                    f'<div style="text-align:center;margin:12px 0;padding:14px;'
                    f'background:#f8fafc;border-radius:10px;'
                    f'border:2px solid {color_up}">'
                    f'<p style="margin:0;color:#64748b;font-size:13px">'
                    f'Current Price: <strong>${latest_price:,.2f}</strong> | '
                    f'Intrinsic Value / Share: <strong>${iv_per_share:.2f}</strong></p>'
                    f'<p style="margin:6px 0 0;font-size:1.8em;'
                    f'font-weight:800;color:{color_up}">'
                    f'{label_up}: {upside:+.1f}%</p>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # ── Bar chart: PV breakdown ───────────────────────────────────────
            st.markdown("---")
            st.markdown("#### 📊 PV Breakdown")

            import plotly.graph_objects as go  # already imported, safe re-import

            bar_labels = [f"Year {i+1}" for i in range(len(pv_fcfs))]
            bar_values = [v / 1e9 for v in pv_fcfs]
            if pv_tv is not None:
                bar_labels.append("Terminal Value")
                bar_values.append(pv_tv / 1e9)

            bar_colors = ["#3b82f6"] * len(pv_fcfs) + (
                ["#10b981"] if pv_tv is not None else []
            )

            fig_pv = go.Figure(
                go.Bar(
                    x=bar_labels,
                    y=bar_values,
                    marker_color=bar_colors,
                    text=[f"${v:.1f}B" for v in bar_values],
                    textposition="outside",
                )
            )
            fig_pv.update_layout(
                title="Present Value Breakdown ($B)",
                yaxis_title="Present Value ($B)",
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(family="sans-serif", size=13),
                margin=dict(t=50, b=40, l=50, r=20),
                showlegend=False,
            )
            fig_pv.update_yaxes(gridcolor="#f1f5f9")
            st.plotly_chart(fig_pv, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: DRAWDOWN ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📉  Drawdown":

    st.markdown("## 📉 Drawdown Analysis")
    st.caption(
        "Historical intra-year drawdown distribution and option strike price calculator. "
        "Data sourced from yfinance."
    )

    # ── Ticker input ──────────────────────────────────────────────────────────
    dd_c1, dd_c2, dd_c3 = st.columns([2, 1, 4])
    with dd_c1:
        dd_ticker = st.text_input(
            "Stock Ticker",
            value=st.session_state.get("dd_ticker", ""),
            placeholder="RCL, AAPL, TSLA…",
            max_chars=10,
            key="dd_ticker_input",
        ).strip().upper()
    with dd_c2:
        st.markdown("<br>", unsafe_allow_html=True)
        dd_run = st.button("📊 Analyze", type="primary", use_container_width=True)

    if dd_run and dd_ticker:
        st.session_state["dd_ticker"] = dd_ticker
        st.session_state["dd_data"]   = None   # force re-download

    dd_ticker_active = st.session_state.get("dd_ticker", "")
    if not dd_ticker_active:
        st.info("Enter a ticker above and click **📊 Analyze** to begin.")
        st.stop()

    # ── Download / cache price data ───────────────────────────────────────────
    @st.cache_data(ttl=3600, show_spinner=False)
    def _dd_fetch(ticker: str) -> pd.DataFrame | None:
        try:
            import yfinance as yf
            tk_obj = yf.Ticker(ticker)
            hist   = tk_obj.history(period="max", interval="1d", auto_adjust=True)
            if hist.empty:
                return None
            hist.index = pd.to_datetime(hist.index).tz_localize(None)
            df = hist[["Close"]].rename(columns={"Close": "price"}).copy()
            df.index.name = "date"
            return df
        except Exception:
            return None

    with st.spinner(f"Downloading price history for **{dd_ticker_active}**…"):
        dd_df = _dd_fetch(dd_ticker_active)

    if dd_df is None or dd_df.empty:
        st.error(f"Could not download price data for **{dd_ticker_active}**. Check the ticker and try again.")
        st.stop()

    # ── Compute drawdown metrics ──────────────────────────────────────────────
    dd_df = dd_df.copy()
    dd_df["year"] = dd_df.index.year

    # Cumulative all-time drawdown: (price / running_max) - 1
    dd_df["cum_max"]         = dd_df["price"].cummax()
    dd_df["drawdown_cum"]    = (dd_df["price"] / dd_df["cum_max"] - 1).clip(upper=0)

    # Intra-year (YTD) drawdown: (price / ytd_max_up_to_this_day) - 1
    dd_df["ytd_max"] = dd_df.groupby("year")["price"].transform(
        lambda s: s.expanding().max()
    )
    dd_df["drawdown_yr"] = (dd_df["price"] / dd_df["ytd_max"] - 1).clip(upper=0)

    # Zero → blank (only keep true drawdowns)
    dd_df["drawdown_yr_neg"] = dd_df["drawdown_yr"].where(dd_df["drawdown_yr"] < 0)

    # ── Year-by-year worst drawdown table ────────────────────────────────────
    yr_table = (
        dd_df.groupby("year")
        .agg(
            min_drawdown_cum=("drawdown_cum",    "min"),
            min_drawdown_yr =("drawdown_yr_neg", "min"),
        )
        .reset_index()
    )
    # Only keep years with at least some data (full or partial)
    yr_table = yr_table.dropna(subset=["min_drawdown_yr"])
    yr_table = yr_table.sort_values("year", ascending=False)

    # ── Frequency table ───────────────────────────────────────────────────────
    # Buckets based on intra-year worst drawdown per year
    _dd_buckets = [
        ("0% to -5%",      0.0,  -0.05),
        ("-5% to -10%",   -0.05, -0.10),
        ("-10% to -15%",  -0.10, -0.15),
        ("-15% to -20%",  -0.15, -0.20),
        ("-20% to -25%",  -0.20, -0.25),
        ("-25% to -30%",  -0.25, -0.30),
        ("-30% to -40%",  -0.30, -0.40),
        ("-40% to -60%",  -0.40, -0.60),
        ("< -60%",        -0.60, -1.00),
    ]

    worst_yr = yr_table["min_drawdown_yr"].dropna().values
    n_years  = len(worst_yr)

    freq_rows = []
    for label, lo, hi in _dd_buckets:
        count = int(((worst_yr <= lo) & (worst_yr > hi)).sum())
        prob  = count / n_years if n_years > 0 else 0.0
        freq_rows.append({"Bucket": label, "Years": count, "Probability": prob})
    freq_df = pd.DataFrame(freq_rows)

    # ── Strike price inputs ───────────────────────────────────────────────────
    st.markdown("---")
    sp_c1, sp_c2 = st.columns([1, 2])
    with sp_c1:
        # 52-week high as reference
        one_yr_ago  = dd_df.index.max() - pd.DateOffset(years=1)
        recent_data = dd_df[dd_df.index >= one_yr_ago]
        ref_high    = float(recent_data["price"].max()) if not recent_data.empty else float(dd_df["price"].iloc[-1])
        ref_date    = recent_data["price"].idxmax() if not recent_data.empty else dd_df.index[-1]
        last_price  = float(dd_df["price"].iloc[-1])

        st.markdown(f"**Reference Price (52-wk High)**")
        st.metric(
            label=f"{dd_ticker_active} — 52-week High",
            value=f"${ref_high:,.2f}",
            delta=f"Last: ${last_price:,.2f} ({(last_price/ref_high - 1)*100:+.1f}%)",
        )
        st.caption(f"High reached: {ref_date.strftime('%Y-%m-%d')}")

    with sp_c2:
        st.markdown("**Select Drawdown Levels for Strike Prices**")
        dd_levels_raw = st.multiselect(
            "Drawdown levels",
            options=[5, 10, 15, 20, 25, 30, 35, 40, 50, 60],
            default=[15, 20, 30, 40],
            format_func=lambda x: f"-{x}%",
            label_visibility="collapsed",
            key="dd_levels",
        )
        dd_levels = sorted(dd_levels_raw) if dd_levels_raw else [15, 20, 30, 40]

    # Build strike price table
    strike_rows = []
    for lvl in dd_levels:
        pct      = lvl / 100
        strike   = ref_high * (1 - pct)
        dist_cur = (strike / last_price - 1) * 100
        # probability: P(worst intra-year ≤ -pct) from historical data
        hist_prob = float((worst_yr <= -pct).mean()) if n_years > 0 else 0.0
        strike_rows.append({
            "Drawdown": f"-{lvl}%",
            "Strike Price": f"${strike:,.2f}",
            "From Current": f"{dist_cur:+.1f}%",
            "Hist. Prob (annual)": f"{hist_prob:.0%}",
        })
    strike_df = pd.DataFrame(strike_rows)

    # ══════════════════════════════════════════════════════════════════════════
    # CHART SECTION (shown at the top of results)
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📈 Price History & Intra-Year Drawdown")

    # Filter to last N years for chart readability
    chart_years = st.slider(
        "Chart history (years)", min_value=3, max_value=max(10, int(dd_df["year"].nunique())),
        value=min(10, int(dd_df["year"].nunique())),
        key="dd_chart_years",
    )
    chart_cutoff = dd_df.index.max() - pd.DateOffset(years=chart_years)
    chart_df     = dd_df[dd_df.index >= chart_cutoff].copy()

    fig_dd = go.Figure()

    # Trace 1: Price (primary y-axis)
    fig_dd.add_trace(go.Scatter(
        x=chart_df.index,
        y=chart_df["price"],
        name="Price",
        line=dict(color="#3b82f6", width=1.5),
        yaxis="y1",
    ))

    # Trace 2: Strike levels (horizontal lines on price axis)
    for lvl in dd_levels:
        strike_val = ref_high * (1 - lvl / 100)
        fig_dd.add_hline(
            y=strike_val,
            line=dict(color="#f59e0b", width=1, dash="dot"),
            annotation_text=f"-{lvl}%  ${strike_val:,.0f}",
            annotation_position="right",
            annotation_font_size=10,
            annotation_font_color="#b45309",
        )

    # 52-wk high line
    fig_dd.add_hline(
        y=ref_high,
        line=dict(color="#10b981", width=1.5, dash="dash"),
        annotation_text=f"52-wk High ${ref_high:,.0f}",
        annotation_position="right",
        annotation_font_size=10,
        annotation_font_color="#065f46",
    )

    # Trace 3: Intra-year drawdown (secondary y-axis, filled area)
    fig_dd.add_trace(go.Scatter(
        x=chart_df.index,
        y=chart_df["drawdown_yr"] * 100,
        name="Intra-Year Drawdown",
        fill="tozeroy",
        fillcolor="rgba(239,68,68,0.15)",
        line=dict(color="rgba(239,68,68,0.6)", width=1),
        yaxis="y2",
    ))

    fig_dd.update_layout(
        xaxis=dict(title="", showgrid=False, rangeslider=dict(visible=False)),
        yaxis=dict(
            title="Price ($)",
            side="left",
            showgrid=True,
            gridcolor="#f1f5f9",
        ),
        yaxis2=dict(
            title="Drawdown (%)",
            side="right",
            overlaying="y",
            showgrid=False,
            tickformat=".0f",
            ticksuffix="%",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="sans-serif", size=12),
        margin=dict(t=40, b=40, l=60, r=120),
        hovermode="x unified",
    )
    st.plotly_chart(fig_dd, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # RESULTS: Strike Prices, Frequency Table, Year-by-Year Table
    # ══════════════════════════════════════════════════════════════════════════
    res_c1, res_c2 = st.columns([1, 1])

    with res_c1:
        st.markdown("### 🎯 Strike Prices")
        st.caption(f"Based on 52-week high of **${ref_high:,.2f}** ({ref_date.strftime('%Y-%m-%d')})")
        st.dataframe(
            strike_df,
            use_container_width=True,
            hide_index=True,
        )

    with res_c2:
        st.markdown("### 📊 Drawdown Frequency Table")
        st.caption(
            f"Intra-year worst drawdown per calendar year · "
            f"{n_years} years of data"
        )
        # Style the frequency table
        freq_display = freq_df.copy()
        freq_display["Probability"] = freq_display["Probability"].map(lambda x: f"{x:.1%}")
        st.dataframe(
            freq_display,
            use_container_width=True,
            hide_index=True,
        )

    # ── Bar chart: frequency distribution ────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📉 Drawdown Distribution (by Year)")

    fig_freq = go.Figure()
    bar_colors_freq = []
    for _, row in freq_df.iterrows():
        bucket = row["Bucket"]
        if "<" in bucket or "-40" in bucket or "-60" in bucket:
            bar_colors_freq.append("#dc2626")
        elif "-25" in bucket or "-30" in bucket:
            bar_colors_freq.append("#f97316")
        elif "-15" in bucket or "-20" in bucket:
            bar_colors_freq.append("#f59e0b")
        else:
            bar_colors_freq.append("#10b981")

    fig_freq.add_trace(go.Bar(
        x=freq_df["Bucket"],
        y=freq_df["Years"],
        marker_color=bar_colors_freq,
        text=freq_df.apply(
            lambda r: f"{r['Years']}y<br>{r['Probability']:.0%}", axis=1
        ),
        textposition="outside",
        name="# of Years",
    ))
    fig_freq.update_layout(
        xaxis=dict(title="Max Intra-Year Drawdown Bucket"),
        yaxis=dict(title="Number of Years", gridcolor="#f1f5f9"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="sans-serif", size=12),
        margin=dict(t=40, b=60, l=50, r=20),
        showlegend=False,
    )
    st.plotly_chart(fig_freq, use_container_width=True)

    # ── Year-by-year table ────────────────────────────────────────────────────
    with st.expander("📅 Year-by-Year Worst Drawdown", expanded=False):
        yr_display = yr_table.copy()
        yr_display["min_drawdown_cum"] = yr_display["min_drawdown_cum"].map(
            lambda x: f"{x:.1%}" if pd.notna(x) else "—"
        )
        yr_display["min_drawdown_yr"] = yr_display["min_drawdown_yr"].map(
            lambda x: f"{x:.1%}" if pd.notna(x) else "—"
        )
        yr_display.columns = ["Year", "Worst Cumulative Drawdown", "Worst Intra-Year Drawdown"]
        st.dataframe(yr_display, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: TOTAL RETURN ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊  Returns":
    import datetime as _dt
    import numpy as _np

    st.markdown("## 📊 Total Return Analysis")
    st.caption(
        "Buy on a specific date, hold 3Y / 5Y / 10Y, compute CAGR (IRR) vs S&P 500. "
        "Bootstrap simulation estimates expected returns across all historical entry points."
    )

    # ── Inputs ────────────────────────────────────────────────────────────────
    inp_c1, inp_c2, inp_c3 = st.columns([2, 2, 1])
    with inp_c1:
        tr_ticker = st.text_input(
            "Stock Ticker",
            value=st.session_state.get("tr_ticker_val", ""),
            placeholder="RCL, AAPL, MSFT…",
            max_chars=10,
            key="tr_ticker_inp",
        ).strip().upper()
    with inp_c2:
        _max_entry = _dt.date.today() - _dt.timedelta(days=3 * 365)
        tr_date = st.date_input(
            "Entry Date",
            value=st.session_state.get("tr_date_val", _dt.date(2015, 1, 2)),
            min_value=_dt.date(1990, 1, 1),
            max_value=_max_entry,
            key="tr_date_inp",
        )
    with inp_c3:
        st.markdown("<br>", unsafe_allow_html=True)
        tr_run = st.button("📊 Analyze", type="primary", use_container_width=True)

    # ── Settings ──────────────────────────────────────────────────────────────
    set_c1, set_c2, set_c3 = st.columns([2, 2, 2])
    with set_c1:
        tr_periods = st.multiselect(
            "Holding Periods",
            options=[3, 5, 10],
            default=[3, 5, 10],
            format_func=lambda x: f"{x}Y",
            key="tr_periods",
        )
    with set_c2:
        inc_div = st.checkbox("Include Dividends & Distributions", value=True, key="tr_inc_div")
        reinvest_div = False
        if inc_div:
            reinvest_div = st.checkbox("Reinvest Dividends (DRIP)", value=True, key="tr_reinvest")
    with set_c3:
        tr_cash_rate = 0.0
        if inc_div and not reinvest_div:
            tr_cash_rate = st.number_input(
                "Cash Rate (%/yr)",
                min_value=0.0, max_value=20.0, value=4.0, step=0.1,
                key="tr_cash_rate",
                help="Interest rate at which un-reinvested dividends accumulate to the exit date",
            ) / 100.0
        tr_n_boot = int(st.number_input(
            "Bootstrap Samples", min_value=100, max_value=2000, value=500, step=100, key="tr_n_boot",
        ))

    if tr_run and tr_ticker:
        st.session_state["tr_ticker_val"] = tr_ticker
        st.session_state["tr_date_val"]   = tr_date

    tr_ticker_active = st.session_state.get("tr_ticker_val", "")
    if not tr_ticker_active:
        st.info("Enter a ticker and click **📊 Analyze** to begin.")
        st.stop()

    tr_periods_active = sorted(tr_periods) if tr_periods else [3, 5, 10]
    _TR_BENCH = "SPY"
    _TR_INIT  = 10_000.0

    # ── Data fetch ────────────────────────────────────────────────────────────
    @st.cache_data(ttl=3600, show_spinner=False)
    def _tr_fetch(ticker: str) -> pd.DataFrame | None:
        try:
            import yfinance as yf
            hist = yf.Ticker(ticker).history(
                period="max", interval="1d", auto_adjust=True, actions=True,
            )
            if hist.empty:
                return None
            hist.index = pd.to_datetime(hist.index).tz_localize(None)
            return hist[["Close", "Dividends"]].copy()
        except Exception:
            return None

    with st.spinner(f"Downloading {tr_ticker_active} & {_TR_BENCH}…"):
        tr_df_stock = _tr_fetch(tr_ticker_active)
        tr_df_bench = _tr_fetch(_TR_BENCH)

    if tr_df_stock is None or tr_df_stock.empty:
        st.error(f"No data for **{tr_ticker_active}**.")
        st.stop()
    if tr_df_bench is None or tr_df_bench.empty:
        st.error(f"No data for benchmark **{_TR_BENCH}**.")
        st.stop()

    # ── Core return helper (used for single-date analysis) ────────────────────
    def _tr_compute(hist: pd.DataFrame, start, end, incl: bool, drip: bool, cr: float):
        """Compute total return and CAGR for a buy-hold interval."""
        dates = hist.index
        if pd.Timestamp(end) > dates[-1]:
            return None
        si = min(dates.searchsorted(pd.Timestamp(start)), len(dates) - 1)
        ei = min(dates.searchsorted(pd.Timestamp(end)),   len(dates) - 1)
        s_ts, e_ts = dates[si], dates[ei]
        if e_ts <= s_ts:
            return None
        sp = float(hist.loc[s_ts, "Close"])
        ep = float(hist.loc[e_ts, "Close"])
        yrs = (e_ts - s_ts).days / 365.25
        if not incl:
            terminal = ep
            div_val  = 0.0
        else:
            mask = (hist.index > s_ts) & (hist.index <= e_ts)
            divs = hist.loc[mask, "Dividends"]
            divs = divs[divs > 0]
            if drip:
                shares = 1.0
                for dd, da in divs.items():
                    px = float(hist.loc[dd, "Close"])
                    if px > 0:
                        shares += shares * da / px
                terminal = shares * ep
                div_val  = terminal - ep
            else:
                acc = sum(float(da) * (1 + cr) ** ((e_ts - dd).days / 365.25)
                          for dd, da in divs.items())
                terminal = ep + acc
                div_val  = acc
        tr   = terminal / sp - 1
        cagr = (1 + tr) ** (1 / yrs) - 1 if yrs > 0 else 0.0
        return dict(
            start_ts=s_ts, end_ts=e_ts,
            start_price=sp, end_price=ep,
            terminal=terminal, total_return=tr, cagr=cagr,
            years=yrs, div_contribution=div_val,
        )

    # ── Bootstrap (self-contained for cache compatibility) ────────────────────
    @st.cache_data(ttl=3600, show_spinner=False)
    def _tr_bootstrap(
        df_s: pd.DataFrame, df_b: pd.DataFrame,
        yrs: int, n: int,
        incl: bool, drip: bool, cr: float, seed: int = 42,
    ):
        import numpy as _np2

        def _ret(hist, start, end):
            dates = hist.index
            if pd.Timestamp(end) > dates[-1]:
                return None
            si = min(dates.searchsorted(pd.Timestamp(start)), len(dates) - 1)
            ei = min(dates.searchsorted(pd.Timestamp(end)),   len(dates) - 1)
            s_ts, e_ts = dates[si], dates[ei]
            if e_ts <= s_ts:
                return None
            sp = float(hist.loc[s_ts, "Close"])
            ep = float(hist.loc[e_ts, "Close"])
            yh = (e_ts - s_ts).days / 365.25
            if not incl:
                terminal = ep
            else:
                mask = (hist.index > s_ts) & (hist.index <= e_ts)
                divs = hist.loc[mask, "Dividends"]
                divs = divs[divs > 0]
                if drip:
                    sh = 1.0
                    for dd, da in divs.items():
                        px = float(hist.loc[dd, "Close"])
                        if px > 0:
                            sh += sh * da / px
                    terminal = sh * ep
                else:
                    acc = sum(float(da) * (1 + cr) ** ((e_ts - dd).days / 365.25)
                              for dd, da in divs.items())
                    terminal = ep + acc
            tr = terminal / sp - 1
            return (1 + tr) ** (1 / yh) - 1 if yh > 0 else 0.0

        _np2.random.seed(seed)
        cutoff = df_s.index[-1] - pd.DateOffset(years=yrs)
        valid  = df_s.index[df_s.index <= cutoff]
        if len(valid) < 30:
            return None
        replace = n > len(valid)
        idxs    = _np2.random.choice(len(valid), size=n, replace=replace)
        sampled = valid[idxs]

        sc, bc = [], []
        for d in sampled:
            end_d = d + pd.DateOffset(years=yrs)
            rs = _ret(df_s, d, end_d)
            rb = _ret(df_b, d, end_d)
            if rs is not None:
                sc.append(rs)
            if rb is not None:
                bc.append(rb)
        return {"stock": sc, "bench": bc, "n_valid": len(valid)}

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — SINGLE-DATE ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown(f"### 📅 Single Entry: {tr_date.strftime('%B %d, %Y')}")

    def _tr_pct_color(v):
        return "#16a34a" if v >= 0 else "#dc2626"

    def _tr_card(res, label, ticker_color="#0369a1"):
        tr = res["total_return"]
        cr = res["cagr"]
        return (
            f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:18px'>"
            f"<div style='font-size:1.05em;font-weight:700;color:{ticker_color}'>{label}</div>"
            f"<div style='display:flex;justify-content:space-between;margin-top:12px;font-size:0.85em'>"
            f"<div><div style='color:#94a3b8;font-size:0.78em'>Entry</div>"
            f"<div style='font-weight:600'>${res['start_price']:,.2f}</div>"
            f"<div style='color:#94a3b8;font-size:0.72em'>{res['start_ts'].strftime('%Y-%m-%d')}</div></div>"
            f"<div style='text-align:right'><div style='color:#94a3b8;font-size:0.78em'>Exit</div>"
            f"<div style='font-weight:600'>${res['end_price']:,.2f}</div>"
            f"<div style='color:#94a3b8;font-size:0.72em'>{res['end_ts'].strftime('%Y-%m-%d')}</div></div>"
            f"</div>"
            f"<div style='display:flex;justify-content:space-around;margin-top:14px;"
            f"padding-top:12px;border-top:1px solid #f1f5f9'>"
            f"<div style='text-align:center'>"
            f"<div style='font-size:0.7em;color:#94a3b8'>Total Return</div>"
            f"<div style='font-size:1.45em;font-weight:800;color:{_tr_pct_color(tr)}'>{tr:+.1%}</div>"
            f"</div>"
            f"<div style='text-align:center'>"
            f"<div style='font-size:0.7em;color:#94a3b8'>CAGR</div>"
            f"<div style='font-size:1.45em;font-weight:800;color:{_tr_pct_color(cr)}'>{cr:+.1%}</div>"
            f"</div>"
            f"<div style='text-align:center'>"
            f"<div style='font-size:0.7em;color:#94a3b8'>$10k →</div>"
            f"<div style='font-size:1.2em;font-weight:700;color:{_tr_pct_color(tr)}'>"
            f"${_TR_INIT * (1 + tr):,.0f}</div>"
            f"</div>"
            f"</div></div>"
        )

    def _tr_growth_series(hist, start_ts, end_ts, incl, drip, cr, label):
        """Portfolio growth of $_TR_INIT over time."""
        mask = (hist.index >= start_ts) & (hist.index <= end_ts)
        sub  = hist.loc[mask].copy()
        if sub.empty:
            return None
        sp0    = float(sub["Close"].iloc[0])
        shares = _TR_INIT / sp0
        acc_cash = 0.0
        vals = []
        for dt, row in sub.iterrows():
            div = float(row.get("Dividends", 0.0))
            if incl and div > 0:
                if drip:
                    px = float(row["Close"])
                    if px > 0:
                        shares += shares * div / px
                else:
                    acc_cash += shares * div   # cash collected (no time-value for chart)
            vals.append(shares * float(row["Close"]) + (acc_cash if (incl and not drip) else 0.0))
        return pd.Series(vals, index=sub.index, name=label)

    for yrs in tr_periods_active:
        exit_d = pd.Timestamp(tr_date) + pd.DateOffset(years=yrs)
        rs = _tr_compute(tr_df_stock, tr_date, exit_d, inc_div, reinvest_div, tr_cash_rate)
        rb = _tr_compute(tr_df_bench, tr_date, exit_d, inc_div, reinvest_div, tr_cash_rate)

        st.markdown(f"#### {yrs}-Year Hold")

        if rs is None:
            avail = (tr_df_stock.index[-1] - pd.Timestamp(tr_date)).days / 365.25
            st.warning(
                f"**{yrs}Y not available** — only {avail:.1f}Y of history past this entry date."
            )
            st.markdown("---")
            continue

        # Cards row
        cc1, cc2 = st.columns(2)
        with cc1:
            st.markdown(_tr_card(rs, tr_ticker_active, "#0369a1"), unsafe_allow_html=True)
        with cc2:
            if rb:
                st.markdown(_tr_card(rb, f"{_TR_BENCH}  (S&P 500)", "#374151"), unsafe_allow_html=True)
            else:
                st.info("No benchmark data for this period.")

        # Alpha row
        if rb:
            alpha = rs["cagr"] - rb["cagr"]
            alpha_color = _tr_pct_color(alpha)
            alpha_label = "outperformed" if alpha >= 0 else "underperformed"
            st.markdown(
                f"<div style='text-align:center;padding:8px;background:#f8fafc;"
                f"border-radius:8px;margin-top:6px'>"
                f"<span style='font-size:0.85em;color:#64748b'>{tr_ticker_active} "
                f"{alpha_label} {_TR_BENCH} by </span>"
                f"<span style='font-size:1.1em;font-weight:700;color:{alpha_color}'>"
                f"{abs(alpha):.2%}/yr</span>"
                f"<span style='font-size:0.85em;color:#64748b'> (alpha = "
                f"<b style='color:{alpha_color}'>{alpha:+.2%}</b> CAGR)</span></div>",
                unsafe_allow_html=True,
            )

        # Portfolio growth chart
        st.markdown("<br>", unsafe_allow_html=True)
        s_gr = _tr_growth_series(tr_df_stock, rs["start_ts"], rs["end_ts"], inc_div, reinvest_div, tr_cash_rate, tr_ticker_active)
        b_gr = _tr_growth_series(tr_df_bench, rs["start_ts"], rs["end_ts"], inc_div, reinvest_div, tr_cash_rate, _TR_BENCH) if rb else None

        if s_gr is not None and not s_gr.empty:
            fig_gr = go.Figure()
            fig_gr.add_trace(go.Scatter(
                x=s_gr.index, y=s_gr.values, name=tr_ticker_active,
                line=dict(color="#3b82f6", width=2),
                hovertemplate="%{x|%Y-%m-%d}<br>$%{y:,.0f}<extra></extra>",
            ))
            if b_gr is not None:
                fig_gr.add_trace(go.Scatter(
                    x=b_gr.index, y=b_gr.values, name=f"{_TR_BENCH} (S&P 500)",
                    line=dict(color="#94a3b8", width=1.5, dash="dash"),
                    hovertemplate="%{x|%Y-%m-%d}<br>$%{y:,.0f}<extra></extra>",
                ))
            fig_gr.add_hline(
                y=_TR_INIT, line=dict(color="#cbd5e1", width=1, dash="dot"),
                annotation_text=f"Initial ${_TR_INIT:,.0f}",
                annotation_font_size=10, annotation_font_color="#94a3b8",
            )
            fig_gr.update_layout(
                title=f"${_TR_INIT:,.0f} invested {rs['start_ts'].strftime('%Y-%m-%d')} → {rs['end_ts'].strftime('%Y-%m-%d')} ({yrs}Y)",
                yaxis=dict(title="Portfolio Value ($)", tickprefix="$", gridcolor="#f1f5f9"),
                xaxis=dict(showgrid=False),
                plot_bgcolor="white", paper_bgcolor="white",
                font=dict(family="sans-serif", size=12),
                margin=dict(t=50, b=30, l=70, r=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )
            st.plotly_chart(fig_gr, use_container_width=True, key=f"tr_gr_{yrs}")

        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — BOOTSTRAP EXPECTED-RETURN DISTRIBUTION
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("### 🎲 Bootstrap: Expected-Return Distribution")
    st.caption(
        f"**{tr_n_boot:,}** random entry dates sampled uniformly from the full price history. "
        f"Each sample uses the same settings (dividends, DRIP) as the single-date analysis above."
    )

    with st.spinner(f"Running bootstrap ({tr_n_boot:,} samples × {len(tr_periods_active)} periods)…"):
        _boot_results = {
            yrs: _tr_bootstrap(
                tr_df_stock, tr_df_bench,
                yrs, tr_n_boot,
                inc_div, reinvest_div, tr_cash_rate,
            )
            for yrs in tr_periods_active
        }

    for yrs in tr_periods_active:
        br = _boot_results.get(yrs)
        if br is None or not br["stock"]:
            st.warning(f"Insufficient history for {yrs}Y bootstrap.")
            continue

        st.markdown(f"#### {yrs}-Year Holding Period")
        sc = _np.array(br["stock"]) * 100   # CAGR in %
        bc = _np.array(br["bench"]) * 100

        # Pairwise beat-SPY (same sampled dates, so arrays are aligned)
        n_pair   = min(len(sc), len(bc))
        pct_beat = float((sc[:n_pair] > bc[:n_pair]).mean()) if n_pair > 0 else float("nan")

        # Stats table
        def _tr_stats(arr, label, is_stock):
            row = {
                "Ticker":      label,
                "Samples":     len(arr),
                "Mean CAGR":   f"{_np.mean(arr):+.1f}%",
                "Median":      f"{_np.median(arr):+.1f}%",
                "Std Dev":     f"{_np.std(arr):.1f}%",
                "P10":         f"{_np.percentile(arr, 10):+.1f}%",
                "P25":         f"{_np.percentile(arr, 25):+.1f}%",
                "P75":         f"{_np.percentile(arr, 75):+.1f}%",
                "P90":         f"{_np.percentile(arr, 90):+.1f}%",
                "% Positive":  f"{(arr > 0).mean():.0%}",
            }
            if is_stock:
                row["% Beats SPY"] = f"{pct_beat:.0%}" if not _np.isnan(pct_beat) else "—"
            else:
                row["% Beats SPY"] = "—"
            return row

        st.dataframe(
            pd.DataFrame([
                _tr_stats(sc, tr_ticker_active, True),
                _tr_stats(bc, _TR_BENCH,         False),
            ]),
            use_container_width=True, hide_index=True,
        )

        # Overlapping histogram
        fig_bt = go.Figure()
        fig_bt.add_trace(go.Histogram(
            x=sc, name=tr_ticker_active, nbinsx=50,
            marker_color="rgba(59,130,246,0.70)", opacity=0.80,
        ))
        fig_bt.add_trace(go.Histogram(
            x=bc, name=f"{_TR_BENCH} (S&P 500)", nbinsx=50,
            marker_color="rgba(148,163,184,0.65)", opacity=0.75,
        ))
        # Mean verticals
        fig_bt.add_vline(
            x=float(_np.mean(sc)), line=dict(color="#2563eb", width=2, dash="dash"),
            annotation_text=f" {tr_ticker_active} mean: {_np.mean(sc):+.1f}%",
            annotation_position="top right", annotation_font_color="#2563eb", annotation_font_size=11,
        )
        fig_bt.add_vline(
            x=float(_np.mean(bc)), line=dict(color="#475569", width=2, dash="dash"),
            annotation_text=f" {_TR_BENCH} mean: {_np.mean(bc):+.1f}%",
            annotation_position="top left",  annotation_font_color="#475569", annotation_font_size=11,
        )
        fig_bt.add_vline(
            x=0, line=dict(color="#ef4444", width=1, dash="dot"),
            annotation_text=" 0%", annotation_position="bottom right",
            annotation_font_color="#ef4444", annotation_font_size=10,
        )
        fig_bt.update_layout(
            barmode="overlay",
            title=f"{yrs}Y CAGR Distribution — {len(sc):,} bootstrap samples  |  {tr_ticker_active} vs {_TR_BENCH}",
            xaxis=dict(title="Annualized Return CAGR (%)", ticksuffix="%"),
            yaxis=dict(title="# Samples", gridcolor="#f1f5f9"),
            plot_bgcolor="white", paper_bgcolor="white",
            font=dict(family="sans-serif", size=12),
            margin=dict(t=60, b=40, l=60, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_bt, use_container_width=True, key=f"tr_bt_{yrs}")
        st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: SCORECARD
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🎯  Scorecard":
    import json as _json
    import re as _re
    import time as _time
    import threading as _threading

    # ── Imports & init ─────────────────────────────────────────────────────────
    from scorecard_db import (
        init_db, get_sp500_list, sp500_count, upsert_sp500_companies, upsert_kpis,
        get_all_runs, get_run, create_run, get_or_create_partial_run,
        get_answered_question_ids, get_answered_categories, set_run_partial,
        save_answer, finalize_run, mark_run_failed, get_answers, compute_scores,
        CATEGORY_WEIGHTS, gcs_download,
    )

    # Download DB from GCS once per session (no-op if GCS not configured)
    if not st.session_state.get("_sc_gcs_loaded"):
        gcs_download()
        st.session_state["_sc_gcs_loaded"] = True

    init_db()

    # ── Questions data ─────────────────────────────────────────────────────────
    _QUESTIONS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scorecard_questions.json")

    @st.cache_data(ttl=None)
    def _load_questions():
        with open(_QUESTIONS_PATH, encoding="utf-8") as f:
            return _json.load(f)

    SC_QUESTIONS = _load_questions()
    SC_SCORED_QS = [q for q in SC_QUESTIONS if q["categoria"] != "Circulo de Competencia"]
    SC_CIRCULO_QS = [q for q in SC_QUESTIONS if q["categoria"] == "Circulo de Competencia"]

    # ── Constants ─────────────────────────────────────────────────────────────
    CAT_COLORS = {
        "Fuerzas":              "#3b82f6",
        "Industria":            "#8b5cf6",
        "MOAT Company":         "#f59e0b",
        "Management & Culture": "#10b981",
        "Brand":                "#ec4899",
        "Finance":              "#ef4444",
    }
    LLM_OPTIONS  = ["Gemini", "Claude"]
    PVER_OPTIONS = ["v1", "v2"]
    SCORE_SUFFIX = (
        "\n\n---\n"
        "INSTRUCCIÓN FINAL: Responde en español. "
        "Al final de tu respuesta, en una línea separada, escribe ÚNICAMENTE:\n"
        "CALIFICACION: [número entero entre 0 y 10]"
    )

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _extract_score(text: str):
        """Parse CALIFICACION: N from LLM response. Returns int 0-10 or None."""
        m = _re.search(r"CALIFICACION\s*:\s*(\d+)", text, _re.IGNORECASE)
        if m:
            v = int(m.group(1))
            return max(0, min(10, v))
        # Fallback: last integer 0-10 in the last 300 chars
        nums = _re.findall(r"\b(\d+)\b", text[-300:])
        for n in reversed(nums):
            if 0 <= int(n) <= 10:
                return int(n)
        return None

    def _build_prompt(q: dict, ticker: str, version: str, company_info: dict | None = None) -> str:
        raw = q[f"prompt_{version}"] if f"prompt_{version}" in q and q[f"prompt_{version}"] else q["pregunta"]
        prompt = raw.replace("[EMPRESA]", ticker).replace("[empresa]", ticker)
        # Inject company context header so the LLM knows exactly which company
        if company_info:
            name    = company_info.get("name", "")
            index   = company_info.get("index_member", "")
            sector  = company_info.get("sector", "")
            sic     = company_info.get("sic_code", "")
            sic_d   = company_info.get("sic_desc", "")
            sic_str = f"{sic} — {sic_d}" if sic and sic_d else sic or "N/D"
            header  = (
                f"=== EMPRESA A ANALIZAR ===\n"
                f"Ticker: {ticker}  |  Nombre: {name}  |  Índice: {index}\n"
                f"Sector: {sector}  |  SIC: {sic_str}\n"
                f"==========================\n\n"
            )
            prompt = header + prompt
        return prompt + SCORE_SUFFIX

    def _score_color(s):
        if s is None: return "#94a3b8"
        if s >= 8:    return "#16a34a"
        if s >= 6:    return "#d97706"
        if s >= 4:    return "#ea580c"
        return "#dc2626"

    def _fmt_score(s, decimals=1):
        if s is None: return "—"
        return f"{s:.{decimals}f}"

    def _call_gemini(api_key: str, model: str, prompt: str) -> str:
        import google.genai as _genai
        client = _genai.Client(api_key=api_key)
        resp = client.models.generate_content(model=model, contents=prompt)
        return resp.text

    def _call_claude(api_key: str, model: str, prompt: str) -> str:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text

    def _call_with_retry(fn, max_retries: int = 4, base_delay: int = 15,
                         status_placeholder=None) -> str:
        """
        Call fn() with exponential backoff on temporary rate-limit (429) errors.
        Raises immediately on permanent quota exhaustion (daily/free-tier limits).
        """
        PERMANENT_SIGNALS = [
            # Gemini
            "per_day", "perday", "free_tier", "limit: 0",
            "GenerateRequestsPerDay", "InputTokensPerModelPerDay",
            "permodelperday", "daily",
            # Claude / Anthropic
            "credit balance", "spend limit", "billing", "insufficient_quota",
        ]
        for attempt in range(max_retries):
            try:
                return fn()
            except Exception as e:
                err_str = str(e)
                err_low = err_str.lower()
                is_429  = any(x in err_low for x in ["429", "529", "resource_exhausted", "too many", "overloaded", "rate_limit"])

                if not is_429:
                    raise  # Non-rate-limit error — surface immediately

                # Check if it's a permanent daily/free-tier quota (retrying won't help)
                is_permanent = any(x.lower() in err_low for x in PERMANENT_SIGNALS)
                if is_permanent:
                    raise RuntimeError(
                        "⛔ Cuota diaria agotada para este modelo. "
                        "Espera 24h o cambia de modelo.\n\n"
                        f"Detalle: {err_str[:300]}"
                    )

                # Temporary RPM rate limit — retry with backoff
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)   # 15, 30, 60, 120 s
                    for remaining in range(wait, 0, -1):
                        if status_placeholder:
                            status_placeholder.warning(
                                f"⚠️ Rate limit temporal — esperando {remaining}s "
                                f"(intento {attempt+1}/{max_retries})…"
                            )
                        _time.sleep(1)
                    if status_placeholder:
                        status_placeholder.empty()
                else:
                    raise
        raise RuntimeError("Máximo de reintentos alcanzado")

    # ── S&P 500 list load / refresh ────────────────────────────────────────────
    _SP500_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sp500_list.csv")

    @st.cache_data(ttl=None, show_spinner=False)
    def _load_sp500_csv() -> list[dict]:
        df = pd.read_csv(_SP500_CSV, encoding="utf-8", dtype=str)
        # Normalise column names (strip spaces)
        df.columns = [c.strip() for c in df.columns]
        # Fill optional columns that may be missing in older CSV versions
        for col in ("index_member", "cik", "sic_code", "sic_desc"):
            if col not in df.columns:
                df[col] = ""
        df = df.fillna("")
        return df.to_dict("records")

    def _ensure_sp500_loaded():
        csv_rows = _load_sp500_csv()
        # Reload whenever the DB has fewer companies than the CSV
        # (handles first run AND upgrades from SP500-only to SP500+SP400+SP600).
        # upload=False: seeding local cache from CSV must NOT overwrite GCS —
        # GCS already has the authoritative DB (with all scorecard runs).
        if sp500_count() < len(csv_rows):
            upsert_sp500_companies(csv_rows, upload=False)

    _ensure_sp500_loaded()

    # ── Collect all existing run scores into a lookup dict ─────────────────────
    _all_runs = get_all_runs()
    _run_lookup: dict[str, dict] = {}     # key = "TICKER|llm|v1"
    for _r in _all_runs:
        _k = f"{_r['ticker']}|{_r['llm'].lower()}|{_r['prompt_version'].lower()}"
        _run_lookup[_k] = _r

    def _run_score(ticker, llm, ver):
        k = f"{ticker}|{llm.lower()}|{ver.lower()}"
        r = _run_lookup.get(k)
        if r and r["status"] == "complete":
            return r["total_score"]
        return None

    # ══════════════════════════════════════════════════════════════════════════
    # MAIN LAYOUT: two-panel  (list left / detail right)
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("## 🎯 Scorecard — S&P 500 · S&P 400 · S&P 600")
    st.caption(
        "74 preguntas de Value Investing · 6 categorías ponderadas · "
        "Gemini y Claude · Prompt V1 y V2"
    )

    # ── Top controls: search + filters + KPI refresh ───────────────────────────
    ctrl_c1, ctrl_c2, ctrl_c3, ctrl_c4 = st.columns([2, 1, 1, 1])
    with ctrl_c1:
        sc_search = st.text_input(
            "Buscar empresa",
            placeholder="Ticker o nombre…",
            label_visibility="collapsed",
            key="sc_search",
        ).strip().upper()
    with ctrl_c2:
        sc_sector = st.selectbox(
            "Sector",
            options=["Todos"] + sorted({r["sector"] for r in get_sp500_list() if r.get("sector")}),
            key="sc_sector",
            label_visibility="collapsed",
        )
    with ctrl_c3:
        sc_index_filter = st.selectbox(
            "Índice",
            options=["Todos", "SP500", "SP400", "SP600"],
            key="sc_index_filter",
            label_visibility="collapsed",
        )
    with ctrl_c4:
        sc_refresh_kpi = st.button("🔄 Refresh KPIs", use_container_width=True, key="sc_refresh_kpi")

    # ── KPI batch refresh ─────────────────────────────────────────────────────
    if sc_refresh_kpi:
        import yfinance as yf
        all_tickers = [r["ticker"] for r in get_sp500_list()]
        prog = st.progress(0, text="Descargando precios…")
        batch_size = 50
        kpi_rows = []
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i: i + batch_size]
            prog.progress((i + batch_size) / len(all_tickers), text=f"KPIs {i}–{i+batch_size} / {len(all_tickers)}…")
            try:
                raw = yf.Tickers(" ".join(batch))
                for tk in batch:
                    try:
                        info = raw.tickers[tk].fast_info
                        kpi_rows.append({
                            "ticker":     tk,
                            "last_price": getattr(info, "last_price",  None),
                            "market_cap": getattr(info, "market_cap",  None),
                            "pe_ratio":   getattr(info, "pe_ratio",    None),
                        })
                    except Exception:
                        pass
            except Exception:
                pass
        if kpi_rows:
            upsert_kpis(kpi_rows)
        prog.empty()
        st.success(f"KPIs actualizados para {len(kpi_rows)} empresas.")
        st.rerun()

    # ── Build display dataframe ────────────────────────────────────────────────
    sp500_rows = get_sp500_list()

    # Filter
    if sc_search:
        sp500_rows = [r for r in sp500_rows
                      if sc_search in r["ticker"].upper()
                      or sc_search in (r.get("name") or "").upper()]
    if sc_sector != "Todos":
        sp500_rows = [r for r in sp500_rows if r.get("sector") == sc_sector]
    if sc_index_filter != "Todos":
        sp500_rows = [r for r in sp500_rows if r.get("index_member") == sc_index_filter]

    def _fmt_mktcap(v):
        if v is None: return "—"
        v = float(v)
        if v >= 1e12: return f"${v/1e12:.1f}T"
        if v >= 1e9:  return f"${v/1e9:.1f}B"
        return f"${v/1e6:.0f}M"

    def _fmt_pe(v):
        return "—" if v is None or float(v) <= 0 else f"{float(v):.1f}x"

    def _score_badge(s):
        if s is None: return "—"
        c = _score_color(s)
        return f"🟢 {s:.1f}" if s >= 7 else (f"🟡 {s:.1f}" if s >= 5 else f"🔴 {s:.1f}")

    display_rows = []
    for r in sp500_rows:
        tk = r["ticker"]
        display_rows.append({
            "Ticker":     tk,
            "Empresa":    r.get("name", ""),
            "Sector":     r.get("sector", ""),
            "Precio":     f"${r['last_price']:,.2f}" if r.get("last_price") else "—",
            "Mkt Cap":    _fmt_mktcap(r.get("market_cap")),
            "P/E":        _fmt_pe(r.get("pe_ratio")),
            "Gemini V1":  _score_badge(_run_score(tk, "gemini", "v1")),
            "Gemini V2":  _score_badge(_run_score(tk, "gemini", "v2")),
            "Claude V1":  _score_badge(_run_score(tk, "claude", "v1")),
            "Claude V2":  _score_badge(_run_score(tk, "claude", "v2")),
        })

    display_df = pd.DataFrame(display_rows)

    st.caption(f"**{len(display_rows)}** empresas")

    # Interactive table with row selection
    sc_sel = st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=380,
        selection_mode="single-row",
        on_select="rerun",
        key="sc_table",
    )

    # Determine selected ticker
    sc_selected_ticker = None
    sel_rows = sc_sel.selection.get("rows", []) if sc_sel.selection else []
    if sel_rows:
        sc_selected_ticker = display_rows[sel_rows[0]]["Ticker"]
        st.session_state["sc_active_ticker"] = sc_selected_ticker
    else:
        sc_selected_ticker = st.session_state.get("sc_active_ticker")

    # ══════════════════════════════════════════════════════════════════════════
    # DETAIL PANEL — shown when a ticker is selected
    # ══════════════════════════════════════════════════════════════════════════
    if not sc_selected_ticker:
        st.info("Selecciona una empresa de la tabla para ver su scorecard o ejecutar el algoritmo.")
        st.stop()

    st.markdown("---")

    # Company info row
    _co_info = next((r for r in sp500_rows if r["ticker"] == sc_selected_ticker), None)
    if _co_info is None:
        # may be outside filtered view; reload
        _co_info = next((r for r in get_sp500_list() if r["ticker"] == sc_selected_ticker), {})

    det_c1, det_c2 = st.columns([2, 1])
    with det_c1:
        st.markdown(
            f"### 🏢 {_co_info.get('name', sc_selected_ticker)} "
            f"<span style='font-size:0.75em;color:#94a3b8'>({sc_selected_ticker})</span>",
            unsafe_allow_html=True,
        )
        st.caption(f"{_co_info.get('sector','—')} · {_co_info.get('industry','—')}")
    with det_c2:
        px  = _co_info.get("last_price")
        mc  = _co_info.get("market_cap")
        pe  = _co_info.get("pe_ratio")
        st.metric("Precio", f"${px:,.2f}" if px else "—")
        kpi_c1, kpi_c2 = st.columns(2)
        kpi_c1.metric("Mkt Cap", _fmt_mktcap(mc))
        kpi_c2.metric("P/E", _fmt_pe(pe))

    # ── Run configuration ──────────────────────────────────────────────────────
    st.markdown("#### ▶ Ejecutar Scorecard")
    run_c1, run_c2, run_c3, run_c4 = st.columns([1, 1, 2, 1])
    with run_c1:
        sc_llm  = st.selectbox("LLM", LLM_OPTIONS,  key="sc_llm")
    with run_c2:
        sc_pver = st.selectbox("Prompts", PVER_OPTIONS, key="sc_pver",
                               format_func=lambda x: f"Versión {x.upper()}")
    with run_c3:
        sc_api_key = st.text_input(
            f"API Key ({sc_llm})",
            type="password",
            placeholder=f"Ingresa tu {sc_llm} API Key…",
            key="sc_api_key",
        )
    with run_c4:
        if sc_llm == "Gemini":
            _GEMINI_MODELS = {
                "⚡ Rápido (~7 MXN) — gemini-3-flash-preview": "gemini-3-flash-preview",
                "🧠 Pensar (~35 MXN) — gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
            }
            _model_label = st.selectbox("Modelo", list(_GEMINI_MODELS.keys()), key="sc_model_label")
            sc_model = _GEMINI_MODELS[_model_label]
        else:
            sc_model = st.text_input("Modelo", value="claude-opus-4-5", key="sc_model")

    # ── Run mode & category selector ──────────────────────────────────────────
    ALL_SCORED_CATS = list(CATEGORY_WEIGHTS.keys())   # 6 scored categories
    llm_key_preview = sc_llm.lower()

    # Auto-finalize ANY partial/running run for this ticker (not just current UI combo)
    _all_q_ids_global = {q["id"] for q in SC_QUESTIONS}
    _did_finalize_any = False
    for _r in _all_runs:
        if (_r["ticker"] == sc_selected_ticker
                and _r["status"] in ("partial", "running")):
            _answered = get_answered_question_ids(_r["run_id"])
            if _all_q_ids_global <= _answered:
                _p_answers = get_answers(_r["run_id"])
                _p_cat_avgs, _p_total = compute_scores(_p_answers)
                finalize_run(_r["run_id"], _p_cat_avgs, _p_total)
                _did_finalize_any = True
    if _did_finalize_any:
        st.rerun()

    # Check if there's already a partial run for the currently selected combo
    _existing_partial = None
    _done_cats: set[str] = set()
    for _r in _all_runs:
        if (_r["ticker"] == sc_selected_ticker
                and _r["llm"] == llm_key_preview
                and _r["prompt_version"] == sc_pver
                and _r["status"] in ("partial", "running")):
            _existing_partial = _r
            _done_cats = get_answered_categories(_r["run_id"])
            break

    mode_col, delay_col = st.columns([2, 1])
    with mode_col:
        sc_run_mode = st.radio(
            "Modo de ejecución",
            ["Todas las categorías", "Por categoría"],
            horizontal=True,
            key="sc_run_mode",
        )
    with delay_col:
        sc_delay = st.slider(
            "Pausa entre preguntas (s)",
            min_value=0, max_value=15, value=2,
            key="sc_delay",
            help="Aumenta si recibes errores 429 (Too Many Requests)",
        )

    # Show progress if partial run exists for current combo
    if _existing_partial:
        _partial_run_id   = _existing_partial["run_id"]
        _already_answered = get_answered_question_ids(_partial_run_id)
        _missing_count    = len(_all_q_ids_global - _already_answered)

        if _missing_count > 0:
            st.info(f"📂 Ejecución parcial — faltan {_missing_count} preguntas para finalizar.")
            # Per-category breakdown: answered vs total
            _status_rows = []
            for cat in ["Circulo de Competencia"] + ALL_SCORED_CATS:
                _cat_total    = sum(1 for q in SC_QUESTIONS if q["categoria"] == cat)
                _cat_answered = sum(1 for q in SC_QUESTIONS
                                    if q["categoria"] == cat and q["id"] in _already_answered)
                if _cat_answered == _cat_total:
                    _estado = "✅ Completa"
                elif _cat_answered == 0:
                    _estado = "⏳ Pendiente"
                else:
                    _estado = f"⚠️ Parcial ({_cat_answered}/{_cat_total})"
                _status_rows.append({
                    "Categoría":  cat,
                    "Respondidas": f"{_cat_answered} / {_cat_total}",
                    "Estado":     _estado,
                })
            st.dataframe(pd.DataFrame(_status_rows), hide_index=True, use_container_width=True)

        if st.button("🗑️ Descartar ejecución parcial y empezar de cero",
                     key="sc_discard_partial"):
            st.session_state["sc_confirm_discard"] = True

        if st.session_state.get("sc_confirm_discard"):
            st.warning("¿Estás seguro? Se eliminarán todas las respuestas guardadas para esta combinación.")
            _conf_c1, _conf_c2 = st.columns(2)
            with _conf_c1:
                if st.button("✅ Sí, empezar de cero", key="sc_confirm_yes", type="primary", use_container_width=True):
                    create_run(sc_selected_ticker, llm_key_preview, sc_pver, sc_model)
                    st.session_state.pop("sc_confirm_discard", None)
                    st.rerun()
            with _conf_c2:
                if st.button("❌ Cancelar", key="sc_confirm_no", use_container_width=True):
                    st.session_state.pop("sc_confirm_discard", None)
                    st.rerun()

    # Category multi-select (only shown in category mode)
    ALL_CATS_WITH_CIRCULO = ["Circulo de Competencia"] + ALL_SCORED_CATS
    sc_selected_cats = ALL_SCORED_CATS  # default: all
    sc_run_circulo = True
    if sc_run_mode == "Por categoría":
        pending_cats = [c for c in ALL_CATS_WITH_CIRCULO if c not in _done_cats]
        sc_selected_all = st.multiselect(
            "Categorías a ejecutar",
            options=ALL_CATS_WITH_CIRCULO,
            default=pending_cats,
            key="sc_cat_select",
        )
        sc_run_circulo  = "Circulo de Competencia" in sc_selected_all
        sc_selected_cats = [c for c in sc_selected_all if c != "Circulo de Competencia"]

    sc_run_btn = st.button(
        f"▶ Ejecutar — {sc_llm} {sc_pver.upper()} para {sc_selected_ticker}",
        type="primary",
        use_container_width=True,
        key="sc_run_btn",
    )

    # ── Execute scorecard run ──────────────────────────────────────────────────
    if sc_run_btn:
        if not sc_api_key:
            st.error("Ingresa una API Key para continuar.")
        elif sc_run_mode == "Por categoría" and not sc_selected_cats and not sc_run_circulo:
            st.error("Selecciona al menos una categoría.")
        else:
            llm_key = sc_llm.lower()

            # Get or create a partial run (reuse existing if resuming)
            if sc_run_mode == "Por categoría":
                run_id, _ = get_or_create_partial_run(
                    sc_selected_ticker, llm_key, sc_pver, sc_model
                )
            else:
                run_id = create_run(sc_selected_ticker, llm_key, sc_pver, sc_model)

            already_answered = get_answered_question_ids(run_id)
            st.session_state["sc_last_run_id"] = run_id

            # Filter questions to execute
            circulo_to_run = (
                [q for q in SC_CIRCULO_QS if q["id"] not in already_answered]
                if sc_run_circulo else []
            )
            scored_to_run = [
                q for q in SC_SCORED_QS
                if q["id"] not in already_answered
                and (sc_run_mode == "Todas las categorías" or q["categoria"] in sc_selected_cats)
            ]
            total_to_run = len(circulo_to_run) + len(scored_to_run)

            if total_to_run == 0:
                st.info("Todas las preguntas seleccionadas ya tienen respuesta.")
            else:
                progress_bar = st.progress(0, text="Iniciando…")
                status_box   = st.empty()
                retry_box    = st.empty()
                errors = []
                done_count = 0

                try:
                    # Circulo de Competencia
                    for q in circulo_to_run:
                        status_box.caption(f"🔍 Contexto: {q['pregunta'][:70]}…")
                        prompt = _build_prompt(q, sc_selected_ticker, sc_pver, company_info=_co_info)
                        try:
                            if llm_key == "gemini":
                                ans = _call_with_retry(
                                    lambda p=prompt: _call_gemini(sc_api_key, sc_model, p),
                                    status_placeholder=retry_box,
                                )
                            else:
                                ans = _call_with_retry(
                                    lambda p=prompt: _call_claude(sc_api_key, sc_model, p),
                                    status_placeholder=retry_box,
                                )
                        except Exception as e:
                            ans = f"[Error: {e}]"
                            errors.append(str(e))
                        save_answer(run_id, q["id"], q["categoria"], q["pregunta"],
                                    None, ans, sc_pver)
                        done_count += 1
                        progress_bar.progress(done_count / total_to_run,
                                              text=f"Contexto ({done_count}/{len(circulo_to_run)})")
                        if sc_delay > 0:
                            _time.sleep(sc_delay)

                    # Scored questions
                    for i, q in enumerate(scored_to_run):
                        status_box.caption(
                            f"📊 [{q['categoria']}] {q['pregunta'][:65]}…"
                        )
                        prompt = _build_prompt(q, sc_selected_ticker, sc_pver, company_info=_co_info)
                        try:
                            if llm_key == "gemini":
                                ans = _call_with_retry(
                                    lambda p=prompt: _call_gemini(sc_api_key, sc_model, p),
                                    status_placeholder=retry_box,
                                )
                            else:
                                ans = _call_with_retry(
                                    lambda p=prompt: _call_claude(sc_api_key, sc_model, p),
                                    status_placeholder=retry_box,
                                )
                            score = _extract_score(ans)
                        except Exception as e:
                            ans   = f"[Error: {e}]"
                            score = None
                            errors.append(str(e))
                        save_answer(run_id, q["id"], q["categoria"], q["pregunta"],
                                    score, ans, sc_pver)
                        done_count += 1
                        progress_bar.progress(done_count / total_to_run,
                                              text=f"[{q['categoria']}] {i+1}/{len(scored_to_run)} — score: {score}")
                        if sc_delay > 0:
                            _time.sleep(sc_delay)

                    # Check if ALL 74 questions are now answered → finalize
                    all_answered = get_answered_question_ids(run_id)
                    all_q_ids   = {q["id"] for q in SC_QUESTIONS}
                    progress_bar.empty()
                    status_box.empty()
                    retry_box.empty()

                    if all_q_ids <= all_answered:
                        answers = get_answers(run_id)
                        cat_avgs, total = compute_scores(answers)
                        finalize_run(run_id, cat_avgs, total)
                        if errors:
                            st.warning(
                                f"Completado con {len(errors)} errores. "
                                f"Score total: **{total:.2f}/10**"
                            )
                        else:
                            st.success(
                                f"✅ Scorecard completado · Score total: **{total:.2f} / 10**"
                            )
                    else:
                        set_run_partial(run_id)
                        remaining = len(all_q_ids) - len(all_answered)
                        if errors:
                            st.warning(
                                f"Categorías ejecutadas con {len(errors)} errores. "
                                f"Faltan {remaining} preguntas para completar el scorecard."
                            )
                        else:
                            st.info(
                                f"✅ Categorías guardadas. "
                                f"Faltan {remaining} preguntas para completar el scorecard."
                            )
                    st.rerun()

                except Exception as ex:
                    set_run_partial(run_id)
                    progress_bar.empty()
                    status_box.empty()
                    retry_box.empty()
                    st.error(f"Error fatal: {ex}")

    # ══════════════════════════════════════════════════════════════════════════
    # SCORE RESULTS — show complete runs + any partial runs with answers
    # ══════════════════════════════════════════════════════════════════════════
    st.divider()

    # ── DB inspector ──────────────────────────────────────────────────────────
    with st.expander("🗄️ Ver estado en base de datos", expanded=False):
        _all_ticker_runs = [r for r in _all_runs if r["ticker"] == sc_selected_ticker]
        if not _all_ticker_runs:
            st.info("No hay ningún run en la base de datos para esta empresa.")
        else:
            _db_rows = []
            for _r in _all_ticker_runs:
                _ans_count = len(get_answered_question_ids(_r["run_id"]))
                _db_rows.append({
                    "Run ID":      _r["run_id"],
                    "LLM":         _r["llm"].capitalize(),
                    "Versión":     _r["prompt_version"].upper(),
                    "Modelo":      _r.get("model_name", "—"),
                    "Estado":      _r["status"],
                    "Preguntas":   f"{_ans_count}/74",
                    "Score Total": f"{_r['total_score']:.2f}" if _r.get("total_score") else "—",
                    "Fecha":       _r["run_date"][:16] if _r.get("run_date") else "—",
                })
            st.dataframe(pd.DataFrame(_db_rows), hide_index=True, use_container_width=True)
    _ticker_runs = [
        r for r in _all_runs
        if r["ticker"] == sc_selected_ticker
        and r["status"] in ("complete", "partial", "running")
    ]

    if not _ticker_runs:
        st.info("Sin scorecards para esta empresa todavía. Configura el LLM y ejecuta el algoritmo.")
        st.stop()

    # Tabs: one per run (complete or partial)
    def _run_tab_label(r):
        status_icon = "✅" if r["status"] == "complete" else "⏳"
        return f"{status_icon} {r['llm'].capitalize()} {r['prompt_version'].upper()} ({r['run_date'][:10]})"

    _run_tabs = st.tabs([_run_tab_label(r) for r in _ticker_runs])

    for _tab, _run in zip(_run_tabs, _ticker_runs):
        with _tab:
            # For partial runs, compute scores live from saved answers
            if _run["status"] != "complete":
                st.warning("⏳ Ejecución parcial — resultados incompletos. Ejecuta las categorías faltantes para finalizar.")
                _partial_answers = get_answers(_run["run_id"])
                _partial_cat_avgs, _partial_total = compute_scores(_partial_answers)
                _ts = _partial_total if _partial_answers else None
                # Inject computed values into run dict for display
                _run = {**_run,
                        "total_score":     _partial_total,
                        "score_fuerzas":   _partial_cat_avgs.get("Fuerzas"),
                        "score_industria": _partial_cat_avgs.get("Industria"),
                        "score_moat":      _partial_cat_avgs.get("MOAT Company"),
                        "score_mgmt":      _partial_cat_avgs.get("Management & Culture"),
                        "score_brand":     _partial_cat_avgs.get("Brand"),
                        "score_finance":   _partial_cat_avgs.get("Finance"),
                        }
            _ts = _run["total_score"]
            _tc = _score_color(_ts)

            # ── Overall score banner ───────────────────────────────────────────
            sc_ov_c1, sc_ov_c2 = st.columns([1, 2])
            with sc_ov_c1:
                st.markdown(
                    f"<div style='text-align:center;padding:24px 0'>"
                    f"<div style='font-size:0.8em;color:#64748b;text-transform:uppercase;letter-spacing:.08em'>Score Total</div>"
                    f"<div style='font-size:3.5em;font-weight:900;color:{_tc}'>{_fmt_score(_ts)}</div>"
                    f"<div style='font-size:0.75em;color:#94a3b8'>/ 10</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with sc_ov_c2:
                # Category scores horizontal bar
                cat_data = [
                    ("Fuerzas",              _run.get("score_fuerzas"),   0.05),
                    ("Industria",            _run.get("score_industria"),  0.05),
                    ("MOAT Company",         _run.get("score_moat"),       0.35),
                    ("Management & Culture", _run.get("score_mgmt"),       0.20),
                    ("Brand",                _run.get("score_brand"),      0.05),
                    ("Finance",              _run.get("score_finance"),    0.30),
                ]
                fig_cat = go.Figure()
                for cat, score, weight in cat_data:
                    if score is None: continue
                    fig_cat.add_trace(go.Bar(
                        x=[score],
                        y=[f"{cat} ({weight:.0%})"],
                        orientation="h",
                        marker_color=CAT_COLORS.get(cat, "#94a3b8"),
                        text=[f"{score:.1f}"],
                        textposition="inside",
                        showlegend=False,
                        name=cat,
                    ))
                fig_cat.add_vline(x=5, line=dict(color="#e2e8f0", width=1, dash="dot"))
                fig_cat.update_layout(
                    xaxis=dict(range=[0, 10], title="Score (0–10)", gridcolor="#f1f5f9"),
                    yaxis=dict(autorange="reversed"),
                    plot_bgcolor="white", paper_bgcolor="white",
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=220,
                    font=dict(size=11),
                )
                st.plotly_chart(fig_cat, use_container_width=True, key=f"sc_cat_{_run['run_id']}")

            # ── Export ────────────────────────────────────────────────────────
            _answers = get_answers(_run["run_id"])

            def _build_export_df(answers, run, questions_lookup, co_info=None):
                import io
                rows = []
                for a in answers:
                    q_data = questions_lookup.get(a["question_id"])
                    pver   = a.get("prompt_used", run["prompt_version"])
                    full_prompt = (
                        _build_prompt(q_data, run["ticker"], pver, company_info=co_info)
                        if q_data else ""
                    )
                    rows.append({
                        "Categoria":    a["categoria"],
                        "Pregunta":     a["pregunta"],
                        "Score":        a["score"],
                        "Prompt_Usado": pver.upper(),
                        "Prompt_Completo": full_prompt,
                        "Respuesta":    a.get("answer_text", ""),
                    })
                return pd.DataFrame(rows)

            _q_lookup = {q["id"]: q for q in SC_QUESTIONS}
            _exp_df   = _build_export_df(_answers, _run, _q_lookup, co_info=_co_info)

            exp_c1, exp_c2 = st.columns(2)
            with exp_c1:
                # CSV download
                _csv_bytes = _exp_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="⬇️ Descargar CSV",
                    data=_csv_bytes,
                    file_name=f"scorecard_{_run['ticker']}_{_run['llm']}_{_run['prompt_version']}_{_run['run_date'][:10]}.csv",
                    mime="text/csv",
                    key=f"dl_csv_{_run['run_id']}",
                    use_container_width=True,
                )
            with exp_c2:
                # Excel download
                import io as _io
                _xl_buf = _io.BytesIO()
                with pd.ExcelWriter(_xl_buf, engine="openpyxl") as _xw:
                    # Summary sheet
                    _summary = pd.DataFrame([{
                        "Empresa":         _run["ticker"],
                        "LLM":             _run["llm"].capitalize(),
                        "Prompt Version":  _run["prompt_version"].upper(),
                        "Modelo":          _run.get("model_name", ""),
                        "Fecha":           _run["run_date"][:10],
                        "Score Total":     _run.get("total_score"),
                        "Score Fuerzas":   _run.get("score_fuerzas"),
                        "Score Industria": _run.get("score_industria"),
                        "Score MOAT":      _run.get("score_moat"),
                        "Score Mgmt":      _run.get("score_mgmt"),
                        "Score Brand":     _run.get("score_brand"),
                        "Score Finance":   _run.get("score_finance"),
                    }])
                    _summary.to_excel(_xw, sheet_name="Resumen", index=False)
                    _exp_df.to_excel(_xw, sheet_name="Prompts y Respuestas", index=False)
                _xl_buf.seek(0)
                st.download_button(
                    label="⬇️ Descargar Excel",
                    data=_xl_buf.read(),
                    file_name=f"scorecard_{_run['ticker']}_{_run['llm']}_{_run['prompt_version']}_{_run['run_date'][:10]}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_xlsx_{_run['run_id']}",
                    use_container_width=True,
                )

            # ── Detailed Q&A per category ──────────────────────────────────────
            st.markdown("#### Respuestas por Categoría")
            _by_cat: dict[str, list] = {}
            for _a in _answers:
                _by_cat.setdefault(_a["categoria"], []).append(_a)

            for _cat, _ans_list in _by_cat.items():
                _cat_score = _run.get({
                    "Fuerzas":              "score_fuerzas",
                    "Industria":            "score_industria",
                    "MOAT Company":         "score_moat",
                    "Management & Culture": "score_mgmt",
                    "Brand":                "score_brand",
                    "Finance":              "score_finance",
                }.get(_cat, ""))
                _cat_label = (
                    f"{_cat}  ·  Score: {_fmt_score(_cat_score)}/10"
                    if _cat_score is not None else _cat
                )
                with st.expander(_cat_label, expanded=False):
                    for _a in _ans_list:
                        _s = _a.get("score")
                        _sc_str = f"**{_s}/10**" if _s is not None else "*Sin calificación*"
                        _s_color = _score_color(_s)
                        st.markdown(
                            f"<div style='border-left:4px solid {_s_color};"
                            f"padding:6px 12px;margin-bottom:10px;background:#f8fafc;"
                            f"border-radius:0 6px 6px 0'>"
                            f"<div style='font-weight:600;font-size:0.9em;color:#1e293b'>"
                            f"{_a['pregunta']}</div>"
                            f"<div style='font-size:0.78em;color:{_s_color};font-weight:700;margin-top:4px'>"
                            f"Calificación: {_s}/10</div>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                        _q_data = next(
                            (q for q in SC_QUESTIONS if q["id"] == _a["question_id"]), None
                        )
                        _prompt_ver = _a.get("prompt_used", _run["prompt_version"])
                        _full_prompt = (
                            _build_prompt(_q_data, _run["ticker"], _prompt_ver, company_info=_co_info)
                            if _q_data else "(prompt no disponible)"
                        )
                        qa_col1, qa_col2 = st.columns(2)
                        with qa_col1:
                            with st.expander("Ver prompt enviado al modelo", expanded=False):
                                st.code(_full_prompt, language=None, wrap_lines=True)
                        with qa_col2:
                            with st.expander("Ver respuesta completa", expanded=False):
                                st.markdown(_a.get("answer_text", ""), unsafe_allow_html=False)
                        st.markdown("<hr style='margin:4px 0;border-color:#f1f5f9'>", unsafe_allow_html=True)
