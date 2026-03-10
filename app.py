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
        [(p, lbl) for p, lbl in seen.items() if p not in popular_set],
        key=lambda x: x[1].lower(),
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


def yaxis_tickformat(unit: str) -> str:
    if unit == "USD":        return "$,.3s"
    if unit == "shares":     return ",.3s"
    if unit == "USD/shares": return "$,.2f"
    return ",.2f"


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
    st.session_state.kpi_tickers = []
    st.session_state.kpi_concept = None   # reset so selector defaults to first

    errors = []
    n = len(tickers)
    prog = st.progress(0, text="Loading company facts…")
    for idx, tk in enumerate(tickers):
        prog.progress((idx) / n, text=f"Loading {tk}…")
        company = find_company(tk)
        if not company:
            errors.append(f"**{tk}** not found in EDGAR.")
            continue
        try:
            facts = load_company_facts(company["cik_str"])
            st.session_state.kpi_facts[tk]   = facts
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

    inp_col, btn_col = st.columns([5, 1])
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

    # ── Build concept list ────────────────────────────────────────────────────
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
            help="Set first data point = 100 for each company — useful for relative comparison",
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

    # ── Date-range filter ─────────────────────────────────────────────────────
    all_years = sorted({
        int(row["date"].year)
        for df in series_data.values()
        for _, row in df.iterrows()
    })
    if len(all_years) >= 2:
        yr_min, yr_max = all_years[0], all_years[-1]
        range_col, _ = st.columns([3, 2])
        with range_col:
            yr_from, yr_to = st.slider(
                "Year range",
                min_value=yr_min,
                max_value=yr_max,
                value=(yr_min, yr_max),
                key="kpi_year_range",
            )
        # Apply filter
        series_data = {
            tk: df[(df["date"].dt.year >= yr_from) & (df["date"].dt.year <= yr_to)]
            for tk, df in series_data.items()
            if not df.empty
        }
        series_data = {tk: df for tk, df in series_data.items() if not df.empty}

    if not series_data:
        st.info("No data in the selected year range.")
        st.stop()

    # ── Plotly chart ──────────────────────────────────────────────────────────
    fig = go.Figure()

    for i, (tk, df) in enumerate(series_data.items()):
        color  = CHART_COLORS[i % len(CHART_COLORS)]
        y_vals = df["value"].copy().astype(float)

        if normalize:
            first = y_vals.iloc[0]
            if first != 0:
                y_vals = y_vals / first * 100

        if show_yoy:
            pct = y_vals.pct_change() * 100
            pct.iloc[0] = None   # no prior year for first point
            y_plot = pct
        else:
            y_plot = y_vals

        # Custom hover data: always show the raw formatted value
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

    # Y-axis config
    if show_yoy:
        ytitle = "YoY Growth (%)"
        yfmt   = ".1f"
        ysuf   = "%"
    elif normalize:
        ytitle = "Indexed (base = 100)"
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
