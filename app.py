"""
SEC Filings Explorer
────────────────────
Browse and download 10-K, 10-Q, and JSON filings from SEC EDGAR.
Deployed via Streamlit Cloud — no server or API key needed.
"""

import io
import zipfile

import streamlit as st
import requests
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
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
HEADERS = {"User-Agent": "SEC-Filings-Explorer streamlit@secexplorer.app"}

FOLDER_INFO = {
    "10-K": {"icon": "📑", "label": "10-K Annual Reports",   "badge": "b-10k"},
    "10-Q": {"icon": "📋", "label": "10-Q Quarterly Reports","badge": "b-10q"},
    "JSON": {"icon": "📦", "label": "JSON & XBRL Data",      "badge": "b-json"},
}

BADGE_STYLE = {
    "10-K": "background:#dbeafe;color:#1e40af",
    "10-Q": "background:#ede9fe;color:#5b21b6",
    "JSON": "background:#d1fae5;color:#065f46",
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
    """Fetch the EDGAR submissions JSON for a given CIK (cached 30 min)."""
    padded = str(cik).zfill(10)
    r = requests.get(
        f"https://data.sec.gov/submissions/CIK{padded}.json",
        headers=HEADERS, timeout=20,
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
                pass          # skip files that fail; already cached ones won't
    return buf.getvalue()


# ── Data helpers ───────────────────────────────────────────────────────────────
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
    "ticker":  "",
    "company": None,
    "sub":     None,
    "filings": None,
    "error":   None,
    "folder":  "10-K",
    "ready":   set(),   # IDs of files already fetched and ready to save
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Search action ──────────────────────────────────────────────────────────────
def run_search(raw: str) -> None:
    ticker = raw.strip().upper()
    if not ticker:
        return

    # Reset state
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
                f"Double-check the symbol, or search EDGAR directly: "
                f"[efts.sec.gov](https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22)"
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


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📊 SEC Filings Explorer")
    st.caption("Data from [SEC EDGAR](https://www.sec.gov/edgar) public APIs")
    st.divider()

    # ── Search bar ──────────────────────────────────────────────────────────
    ticker_input = st.text_input(
        "Ticker",
        value=st.session_state.ticker,
        placeholder="AAPL, MSFT, TSLA, JPM…",
        max_chars=10,
        label_visibility="collapsed",
    ).strip().upper()

    if st.button("🔍  Search", type="primary", use_container_width=True):
        run_search(ticker_input)

    # ── Company card (after search) ─────────────────────────────────────────
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

        # Folder picker
        st.session_state.folder = st.radio(
            "📁 Browse folder",
            options=["10-K", "10-Q", "JSON"],
            format_func=lambda x: (
                f"{FOLDER_INFO[x]['icon']}  {x}   "
                f"({len(fi.get(x, []))} files)"
            ),
            index=["10-K", "10-Q", "JSON"].index(st.session_state.folder),
        )

        st.divider()

        # Download summary
        n_ready = len(st.session_state.ready)
        if n_ready:
            st.success(f"✅  {n_ready} file{'s' if n_ready > 1 else ''} ready to save")
        else:
            st.info("Click **⬇ Fetch** on any file to prepare it for download.")

        # Recent tickers (stored as a simple list in session state)
        if "recent" not in st.session_state:
            st.session_state.recent = []
        rec: list = st.session_state.recent
        t = st.session_state.ticker
        if t and (not rec or rec[0] != t):
            rec = [t] + [x for x in rec if x != t]
            st.session_state.recent = rec[:8]

    # ── Recent tickers ───────────────────────────────────────────────────────
    if st.session_state.get("recent"):
        st.divider()
        st.caption("Recent searches")
        cols = st.columns(3)
        for idx, tk in enumerate(st.session_state.recent[:6]):
            if cols[idx % 3].button(tk, key=f"rec_{tk}", use_container_width=True):
                run_search(tk)
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CONTENT
# ══════════════════════════════════════════════════════════════════════════════

# ── Error ──────────────────────────────────────────────────────────────────────
if st.session_state.error:
    st.error(st.session_state.error)
    st.stop()

# ── Landing ────────────────────────────────────────────────────────────────────
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

# ── File list ──────────────────────────────────────────────────────────────────
folder  = st.session_state.folder
filings = st.session_state.filings.get(folder, [])
info    = FOLDER_INFO[folder]
ticker  = st.session_state.ticker
ready   = st.session_state.ready

# Section header
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
    if unfetched and st.button(
        "⬇ Fetch All", use_container_width=True, key="fetch_all"
    ):
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
    # Files in this folder that are already fetched
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
        f"No **{folder}** filings found in EDGAR's recent records for **{ticker}**. "
        "They may be filed under a different entity or CIK."
    )
    st.stop()

# Column headers
c1, c2, c3, c4, c5, c6, c7 = st.columns([0.9, 2.8, 1.4, 1.1, 0.9, 0.8, 1.5])
for col, lbl in zip(
    [c1, c2, c3, c4, c5, c6, c7],
    ["Type", "Description", "Period", "Filed", "Size", "", "Actions"],
):
    col.markdown(
        f"<span class='col-hdr'>{lbl}</span>", unsafe_allow_html=True
    )

st.markdown("<hr style='margin:4px 0 10px;border-color:#e2e8f0'>", unsafe_allow_html=True)

# File rows
for f in filings:
    fid        = f["id"]
    is_ready   = fid in ready
    has_url    = bool(f["doc_url"])
    bs         = BADGE_STYLE.get(f["form"], "background:#f1f5f9;color:#334155")

    c1, c2, c3, c4, c5, c6, c7 = st.columns([0.9, 2.8, 1.4, 1.1, 0.9, 0.8, 1.5])

    # Type badge
    c1.markdown(
        f"<span style='{bs};padding:3px 9px;border-radius:4px;"
        f"font-weight:700;font-size:11px'>{f['form']}</span>",
        unsafe_allow_html=True,
    )

    # Description
    c2.markdown(
        f"<span style='font-size:13px;color:#1e293b'>{f['desc']}</span>",
        unsafe_allow_html=True,
    )

    # Period
    c3.markdown(
        f"<span style='font-size:13px;font-weight:600;color:#334155'>{f['period']}</span>",
        unsafe_allow_html=True,
    )

    # Filed date
    c4.markdown(
        f"<span style='font-size:12px;color:#64748b'>{f['filed']}</span>",
        unsafe_allow_html=True,
    )

    # Size
    c5.markdown(
        f"<span style='font-size:12px;color:#94a3b8'>{fmt_size(f['size'])}</span>",
        unsafe_allow_html=True,
    )

    # Downloaded tick
    if is_ready:
        c6.markdown("<span class='dl-tick'>✓</span>", unsafe_allow_html=True)

    # Action buttons
    with c7:
        if not has_url:
            st.link_button("↗ EDGAR", f["index_url"], use_container_width=True)
        elif is_ready:
            # File already fetched — offer immediate save
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
            # Not yet fetched
            if st.button("⬇ Fetch", key=f"fetch_{fid}", use_container_width=True):
                with st.spinner("Fetching…"):
                    try:
                        fetch_file_bytes(f["doc_url"])
                        st.session_state.ready = ready | {fid}
                    except Exception as exc:
                        st.error(f"Failed: {exc}")
                st.rerun()

    st.markdown("<hr class='row-sep'>", unsafe_allow_html=True)
