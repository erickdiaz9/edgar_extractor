"""
scorecard_db.py — SQLite persistence layer for the Scorecard feature.
All database access for: S&P 500 cache, scorecard runs, and individual answers.

GCS sync: if st.secrets contains [gcs] bucket + credentials, the SQLite file is
downloaded from GCS on startup and re-uploaded after every write operation.
"""
import sqlite3
import os
import time
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "scorecard.db")
GCS_BLOB_NAME = "scorecard.db"

# Module-level GCS status — read by the sidebar to show connection health
gcs_last_error: str = ""
gcs_ok: bool = True


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _gcs_client():
    """Return (client, bucket) or (None, None) if GCS is not configured."""
    global gcs_ok, gcs_last_error
    try:
        import streamlit as st
        from google.cloud import storage
        from google.oauth2 import service_account
        import json

        cfg = st.secrets.get("gcs", {})
        bucket_name = cfg.get("bucket", "")
        creds_raw = cfg.get("credentials", "")
        if not bucket_name or not creds_raw:
            return None, None

        creds_dict = json.loads(creds_raw) if isinstance(creds_raw, str) else dict(creds_raw)
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = storage.Client(credentials=creds, project=creds_dict.get("project_id"))
        return client, client.bucket(bucket_name)
    except Exception as e:
        gcs_ok = False
        gcs_last_error = f"Config error: {e}"
        return None, None


def gcs_enable_versioning():
    """Enable object versioning on the bucket (idempotent — safe to call every startup)."""
    try:
        client, bucket = _gcs_client()
        if bucket is None:
            return
        bucket.reload()          # fetch current bucket metadata
        if not bucket.versioning_enabled:
            bucket.versioning_enabled = True
            bucket.patch()       # persist the change
    except Exception:
        pass                     # non-fatal — don't break the app


def _is_valid_sqlite(path: str) -> bool:
    """Return True if path is a readable, non-corrupt SQLite database."""
    if not os.path.exists(path) or os.path.getsize(path) < 100:
        return False
    try:
        conn = sqlite3.connect(path)
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


def gcs_download():
    """Download scorecard.db from GCS if it exists. Call once at app startup."""
    global gcs_ok, gcs_last_error
    gcs_enable_versioning()      # ensure versioning is on so future uploads are recoverable
    client, bucket = _gcs_client()
    if bucket is None:
        return
    for attempt in range(3):
        try:
            blob = bucket.blob(GCS_BLOB_NAME)
            if blob.exists():
                blob.download_to_filename(DB_PATH)
                # Validate — a corrupt/partial download must not replace a good local DB
                if not _is_valid_sqlite(DB_PATH):
                    os.remove(DB_PATH)
                    raise RuntimeError("Downloaded file is not a valid SQLite database")
            gcs_ok = True
            gcs_last_error = ""
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                gcs_ok = False
                gcs_last_error = f"Download failed: {e}"


def gcs_upload():
    """Upload current scorecard.db to GCS with retry. Called after every write."""
    global gcs_ok, gcs_last_error
    client, bucket = _gcs_client()
    if bucket is None:
        return
    # Ensure all pending writes are flushed to the main DB file before uploading
    try:
        with get_conn() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    for attempt in range(3):
        try:
            blob = bucket.blob(GCS_BLOB_NAME)
            blob.upload_from_filename(DB_PATH)
            gcs_ok = True
            gcs_last_error = ""
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                gcs_ok = False
                gcs_last_error = f"Upload failed: {e}"

SCHEMA = """
CREATE TABLE IF NOT EXISTS sp500_cache (
    ticker          TEXT PRIMARY KEY,
    name            TEXT,
    sector          TEXT,
    industry        TEXT,
    index_member    TEXT,          -- 'SP500' | 'SP400' | 'SP600'
    cik             TEXT,
    sic_code        TEXT,
    sic_desc        TEXT,
    last_price      REAL,
    market_cap      REAL,
    pe_ratio        REAL,
    kpi_updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS scorecard_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    llm             TEXT NOT NULL,            -- 'gemini' | 'claude'
    prompt_version  TEXT NOT NULL,            -- 'v1' | 'v2'
    model_name      TEXT,
    run_date        TEXT,
    status          TEXT DEFAULT 'running',   -- 'running' | 'complete' | 'failed'
    score_fuerzas   REAL,
    score_industria REAL,
    score_moat      REAL,
    score_mgmt      REAL,
    score_brand     REAL,
    score_finance   REAL,
    total_score     REAL,
    circulo_notes   TEXT
);

CREATE TABLE IF NOT EXISTS scorecard_answers (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL,
    question_id   INTEGER NOT NULL,
    categoria     TEXT,
    pregunta      TEXT,
    score         INTEGER,        -- 0-10, NULL for Circulo de Competencia
    answer_text   TEXT,
    prompt_used   TEXT,           -- 'v1' or 'v2'
    FOREIGN KEY (run_id) REFERENCES scorecard_runs(run_id) ON DELETE CASCADE
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Migrate existing DB: add new columns if missing
        existing = {row[1] for row in conn.execute("PRAGMA table_info(sp500_cache)").fetchall()}
        for col, typedef in [
            ("index_member", "TEXT"),
            ("cik",          "TEXT"),
            ("sic_code",     "TEXT"),
            ("sic_desc",     "TEXT"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE sp500_cache ADD COLUMN {col} {typedef}")


# ── S&P 500 cache ─────────────────────────────────────────────────────────────

def upsert_sp500_companies(rows: list[dict], upload: bool = True):
    """Insert or replace company metadata. rows: [{ticker, name, sector, industry, index_member, cik, sic_code, sic_desc}]
    Pass upload=False when seeding the local cache from CSV on startup — avoids overwriting GCS
    with a DB that may not yet have the runs downloaded from GCS."""
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO sp500_cache (ticker, name, sector, industry, index_member, cik, sic_code, sic_desc)
            VALUES (:ticker, :name, :sector, :industry, :index_member, :cik, :sic_code, :sic_desc)
            ON CONFLICT(ticker) DO UPDATE SET
                name         = excluded.name,
                sector       = excluded.sector,
                industry     = excluded.industry,
                index_member = excluded.index_member,
                cik          = excluded.cik,
                sic_code     = excluded.sic_code,
                sic_desc     = excluded.sic_desc
            """,
            [{**r,
              "index_member": r.get("index_member", "SP500"),
              "cik":          r.get("cik", ""),
              "sic_code":     r.get("sic_code", ""),
              "sic_desc":     r.get("sic_desc", ""),
              } for r in rows],
        )
    if upload:
        gcs_upload()


def upsert_kpis(rows: list[dict]):
    """Update KPI columns for tickers. rows: [{ticker, last_price, market_cap, pe_ratio}]"""
    ts = datetime.now().isoformat(timespec="minutes")
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO sp500_cache (ticker, last_price, market_cap, pe_ratio, kpi_updated_at)
            VALUES (:ticker, :last_price, :market_cap, :pe_ratio, :ts)
            ON CONFLICT(ticker) DO UPDATE SET
                last_price     = excluded.last_price,
                market_cap     = excluded.market_cap,
                pe_ratio       = excluded.pe_ratio,
                kpi_updated_at = excluded.kpi_updated_at
            """,
            [{**r, "ts": ts} for r in rows],
        )
    gcs_upload()


def get_sp500_list() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM sp500_cache ORDER BY ticker"
        ).fetchall()
        return [dict(r) for r in rows]


def sp500_count() -> int:
    try:
        with get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM sp500_cache").fetchone()[0]
    except Exception:
        return 0


# ── Scorecard runs ────────────────────────────────────────────────────────────

def get_all_runs() -> list[dict]:
    """All completed/failed runs — used to build the score columns in the list."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT run_id, ticker, llm, prompt_version, model_name,
                   run_date, status, total_score,
                   score_fuerzas, score_industria, score_moat,
                   score_mgmt, score_brand, score_finance
            FROM scorecard_runs
            ORDER BY ticker, llm, prompt_version
            """
        ).fetchall()
        return [dict(r) for r in rows]


def get_run(ticker: str, llm: str, prompt_version: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM scorecard_runs
            WHERE ticker = ? AND llm = ? AND prompt_version = ?
            """,
            (ticker, llm, prompt_version),
        ).fetchone()
        return dict(row) if row else None


def create_run(ticker: str, llm: str, prompt_version: str, model_name: str) -> int:
    """Delete any prior run for this combo, create fresh one. Returns run_id."""
    with get_conn() as conn:
        conn.execute(
            """
            DELETE FROM scorecard_runs
            WHERE ticker = ? AND llm = ? AND prompt_version = ?
            """,
            (ticker, llm, prompt_version),
        )
        cur = conn.execute(
            """
            INSERT INTO scorecard_runs
                (ticker, llm, prompt_version, model_name, run_date, status)
            VALUES (?, ?, ?, ?, ?, 'running')
            """,
            (ticker, llm, prompt_version, model_name, datetime.now().isoformat()),
        )
        run_id = cur.lastrowid
    gcs_upload()
    return run_id


def get_or_create_partial_run(
    ticker: str, llm: str, prompt_version: str, model_name: str
) -> tuple[int, bool]:
    """
    Find an existing in-progress run for this combo, or create a new one.
    Returns (run_id, is_new).  Used for category-by-category execution.
    """
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT run_id FROM scorecard_runs
            WHERE ticker=? AND llm=? AND prompt_version=?
              AND status IN ('running', 'partial')
            ORDER BY run_date DESC LIMIT 1
            """,
            (ticker, llm, prompt_version),
        ).fetchone()
        if row:
            return row[0], False
        cur = conn.execute(
            """
            INSERT INTO scorecard_runs
                (ticker, llm, prompt_version, model_name, run_date, status)
            VALUES (?, ?, ?, ?, ?, 'partial')
            """,
            (ticker, llm, prompt_version, model_name, datetime.now().isoformat()),
        )
        run_id = cur.lastrowid
    gcs_upload()
    return run_id, True


def get_answered_question_ids(run_id: int) -> set[int]:
    """Return set of question_ids with valid (non-error) answers for this run."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT question_id FROM scorecard_answers
               WHERE run_id=? AND (answer_text NOT LIKE '[Error:%' OR answer_text IS NULL)""",
            (run_id,),
        ).fetchall()
        return {r[0] for r in rows}


def get_answered_categories(run_id: int) -> set[str]:
    """Return set of categories that have at least one answer in this run."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT categoria FROM scorecard_answers WHERE run_id=?",
            (run_id,),
        ).fetchall()
        return {r[0] for r in rows}


def set_run_partial(run_id: int):
    """Mark run as partial (paused between category executions)."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE scorecard_runs SET status='partial' WHERE run_id=?",
            (run_id,),
        )
    gcs_upload()


def save_answer(
    run_id: int,
    question_id: int,
    categoria: str,
    pregunta: str,
    score,          # int or None
    answer_text: str,
    prompt_used: str,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO scorecard_answers
                (run_id, question_id, categoria, pregunta, score, answer_text, prompt_used)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, question_id, categoria, pregunta, score, answer_text, prompt_used),
        )
    gcs_upload()  # Upload after every answer — protects against server restart mid-run


def finalize_run(
    run_id: int,
    category_scores: dict,
    total_score: float,
    circulo_notes: str = "",
):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE scorecard_runs SET
                status          = 'complete',
                score_fuerzas   = :fuerzas,
                score_industria = :industria,
                score_moat      = :moat,
                score_mgmt      = :mgmt,
                score_brand     = :brand,
                score_finance   = :finance,
                total_score     = :total,
                circulo_notes   = :circulo
            WHERE run_id = :run_id
            """,
            {
                "fuerzas":   category_scores.get("Fuerzas"),
                "industria": category_scores.get("Industria"),
                "moat":      category_scores.get("MOAT Company"),
                "mgmt":      category_scores.get("Management & Culture"),
                "brand":     category_scores.get("Brand"),
                "finance":   category_scores.get("Finance"),
                "total":     total_score,
                "circulo":   circulo_notes,
                "run_id":    run_id,
            },
        )
    gcs_upload()


def mark_run_failed(run_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE scorecard_runs SET status = 'failed' WHERE run_id = ?",
            (run_id,),
        )
    gcs_upload()


def get_answers(run_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM scorecard_answers WHERE run_id = ? ORDER BY question_id",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# ── Score helpers ─────────────────────────────────────────────────────────────

CATEGORY_WEIGHTS = {
    "Fuerzas":             0.05,
    "Industria":           0.05,
    "MOAT Company":        0.35,
    "Management & Culture": 0.20,
    "Brand":               0.05,
    "Finance":             0.30,
}


def compute_scores(answers: list[dict]) -> tuple[dict, float]:
    """
    Given a list of answer dicts (from get_answers), return
    (category_scores_dict, total_weighted_score).
    Skips Circulo de Competencia (score=None).
    """
    from collections import defaultdict
    cat_scores: dict[str, list[int]] = defaultdict(list)
    for a in answers:
        if a["score"] is not None and a["categoria"] != "Circulo de Competencia":
            cat_scores[a["categoria"]].append(int(a["score"]))

    category_avgs = {}
    for cat, scores in cat_scores.items():
        category_avgs[cat] = sum(scores) / len(scores) if scores else 0.0

    total = sum(
        category_avgs.get(cat, 0.0) * w
        for cat, w in CATEGORY_WEIGHTS.items()
    )
    return category_avgs, total
