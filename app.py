"""
SEC Filings Explorer
────────────────────
Browse and download 10-K, 10-Q, and JSON filings from SEC EDGAR.
Explore financial KPIs with interactive charts — powered by SEC EDGAR XBRL data.
Deployed via Streamlit Cloud — no server or API key needed.
"""

import io
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
}

INCOME_METRICS   = ["Revenue", "Gross Profit", "Operating Income", "Net Income",
                     "EPS Diluted", "EPS Basic", "Diluted Shares"]
BALANCE_METRICS  = ["Total Assets", "Cash", "Accounts Receivable", "Inventory",
                     "Current Assets", "PP&E Net", "Goodwill", "Intangibles",
                     "Total Liabilities", "Current Liabilities", "Long Term Debt", "Total Equity"]
CASHFLOW_METRICS = ["Operating Cash Flow", "CapEx", "D&A", "Share Repurchases", "Dividends Paid"]
DERIVED_METRICS  = ["Gross Margin", "Operating Margin", "Net Margin",
                     "Revenue Growth", "Net Income Growth",
                     "FCF", "FCF Margin", "ROA", "ROE",
                     "Net Debt", "Debt/Equity", "Debt/Assets",
                     # Price-based (populated when yfinance data is available)
                     "Market Cap", "P/E Ratio", "Price/Book", "Dividend Yield"]
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
    "CapEx":               {"fmt": "usd_b",    "negate": True},
    "D&A":                 {"fmt": "usd_b"},
    "Share Repurchases":   {"fmt": "usd_b",    "negate": True},
    "Dividends Paid":      {"fmt": "usd_b"},
    "Gross Margin":        {"fmt": "pct"},
    "Operating Margin":    {"fmt": "pct"},
    "Net Margin":          {"fmt": "pct"},
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

    def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
        return a.div(b.replace(0, float("nan")))

    drv: dict[str, pd.Series] = {}
    if not rev.empty and not gp.empty:       drv["Gross Margin"]      = safe_div(gp, rev)
    if not rev.empty and not oi.empty:       drv["Operating Margin"]  = safe_div(oi, rev)
    if not rev.empty and not ni.empty:       drv["Net Margin"]        = safe_div(ni, rev)
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

    # ── Convert everything to plain Python {int_year: float_value} dicts ─────
    # This completely sidesteps pandas int32/int64 index alignment bugs.
    def to_pydict(s: pd.Series | None) -> dict[int, float]:
        if s is None or (hasattr(s, "empty") and s.empty):
            return {}
        return {
            int(k): float(v)
            for k, v in s.items()
            if v is not None and not (isinstance(v, float) and v != v)  # skip NaN
        }

    def gs_dict(m: str) -> dict[int, float]:
        return to_pydict(get_stmt_series(stmts, m))

    px  = to_pydict(pd.Series({int(k): float(v) for k, v in prices.items()}))
    sh  = gs_dict("Diluted Shares")
    te  = gs_dict("Total Equity")
    div = gs_dict("Dividends Paid")
    eps = gs_dict("EPS Diluted")

    # EPS fallback: Net Income / Diluted Shares
    if not eps and sh:
        ni = gs_dict("Net Income")
        eps = {
            yr: ni[yr] / sh[yr]
            for yr in set(ni) & set(sh)
            if sh[yr] != 0 and ni.get(yr) is not None
        }

    def _yr(d: dict, label: str = "") -> str:
        if not d: return "❌ not found"
        yrs = sorted(d)
        return f"✅ {len(d)} yrs ({yrs[0]}–{yrs[-1]})"

    debug["prices"]        = _yr(px)
    debug["Diluted Shares"]= _yr(sh)
    debug["EPS Diluted"]   = _yr(eps) + (" [computed]" if eps and not gs_dict("EPS Diluted") else "")
    debug["Total Equity"]  = _yr(te)
    debug["Dividends Paid"]= _yr(div)

    new: dict[str, pd.Series] = {}

    def make_series(d: dict) -> pd.Series:
        return pd.Series(d, dtype=float).sort_index() if d else pd.Series(dtype=float)

    # Market Cap = Price × Shares
    mc_d = {yr: px[yr] * sh[yr] for yr in set(px) & set(sh)}
    if mc_d:
        new["Market Cap"] = make_series(mc_d)
        debug["Market Cap"] = f"✅ {len(mc_d)} yrs"
    else:
        debug["Market Cap"] = "❌ needs Diluted Shares"

    # P/E Ratio = Price / EPS (positive only)
    pe_d = {yr: px[yr] / eps[yr] for yr in set(px) & set(eps) if eps.get(yr, 0) > 0 and px[yr] / eps[yr] > 0}
    if pe_d:
        new["P/E Ratio"] = make_series(pe_d)
        debug["P/E Ratio"] = f"✅ {len(pe_d)} yrs"
    else:
        debug["P/E Ratio"] = "❌ needs EPS Diluted"

    # Price/Book = (Price × Shares) / Total Equity (positive only)
    pb_d = {
        yr: (px[yr] * sh[yr]) / te[yr]
        for yr in set(px) & set(sh) & set(te)
        if te.get(yr, 0) != 0 and (px[yr] * sh[yr]) / te[yr] > 0
    }
    if pb_d:
        new["Price/Book"] = make_series(pb_d)
        debug["Price/Book"] = f"✅ {len(pb_d)} yrs"
    else:
        debug["Price/Book"] = "❌ needs Diluted Shares + Total Equity"

    # Dividend Yield = (Dividends Paid / Shares) / Price
    dy_d = {
        yr: (div[yr] / sh[yr]) / px[yr]
        for yr in set(px) & set(sh) & set(div)
        if sh.get(yr, 0) != 0 and px.get(yr, 0) != 0 and (div[yr] / sh[yr]) / px[yr] >= 0
    }
    if dy_d:
        new["Dividend Yield"] = make_series(dy_d)
        debug["Dividend Yield"] = f"✅ {len(dy_d)} yrs"
    else:
        debug["Dividend Yield"] = "❌ needs Dividends Paid + Diluted Shares"

    stmts["price_debug"] = debug

    if not new:
        return

    new_df = pd.DataFrame(new).sort_index(ascending=False)
    drv = stmts.get("derived", pd.DataFrame())
    if drv.empty:
        stmts["derived"] = new_df
    else:
        combined = pd.concat([drv, new_df], axis=1)
        stmts["derived"] = combined.sort_index(ascending=False)


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
        options=["📁  Filings", "📈  KPI Explorer"],
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
    else:
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
else:
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
        tab_is, tab_bs, tab_cf, tab_drv, tab_cmp = st.tabs([
            "📈 Income Statement",
            "🏦 Balance Sheet",
            "💵 Cash Flow",
            "📐 Derived Metrics",
            "🔀 Compare Companies",
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
