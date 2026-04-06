# dashboard.py
# -*- coding: utf-8 -*-
"""
PETRA — Interactive HTML Dashboard Generator
Run after main.py. Output: PETRA_Dashboard_YYYY-MM-DD.html
Features: date filter, assignee filter, team filter, severity filter,
          CSV download per table, predicted breach datetime column.
"""

from __future__ import annotations
import glob, json, os, webbrowser
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

OUT_DIR = Path(os.environ["USERPROFILE"]) / "OneDrive - PETRONAS" / "Desktop" / "PETRA Output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── helpers ───────────────────────────────────────────────────────────────────
def _safe(v):
    if v is None: return None
    if isinstance(v, float) and v != v: return None
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating,)): return round(float(v), 2)
    if isinstance(v, (pd.Timestamp, pd.Period)): return str(v)[:16]
    return v

def _records(df: pd.DataFrame, n: int = 500) -> list[dict]:
    return [{k: _safe(v) for k, v in r.items()} for _, r in df.head(n).iterrows()]

def _find_sheet(xl: pd.ExcelFile, *fragments: str) -> str | None:
    for frag in fragments:
        for s in xl.sheet_names:
            if frag.lower() in s.lower():
                return s
    return None

def _unique_vals(rows: list[dict], col: str) -> list[str]:
    seen, out = set(), []
    for r in rows:
        v = str(r.get(col, "") or "").strip()
        if v and v not in seen:
            seen.add(v); out.append(v)
    return sorted(out)


# ── data loading ──────────────────────────────────────────────────────────────
def load_data(out_dir: Path) -> dict:
    ctx: dict = dict(
        report_date=datetime.now().strftime("%d %b %Y"),
        generated_at=datetime.now().strftime("%H:%M"),
        at_risk=0, breached=0, monitor=0, low_risk=0, total_open=0,
        zombie_count=0,
        model_auc="N/A", breach_rate="N/A", training_n="N/A",
        atrisk_rows=[], breached_rows=[], monitor_rows=[],
        zombie_rows=[], all_open_rows=[], by_team_rows=[],
        cat_labels=[], cat_breach=[], cat_tickets=[],
        month_labels=[], month_tickets=[], month_breach=[],
        team_names=[], team_atrisk=[], team_breached=[],
        # filter option lists (populated below)
        filter_teams=[], filter_assignees=[], filter_severities=[], filter_categories=[],
    )

    master = out_dir / "PETRA_Master_Report.xlsx"
    if master.exists():
        try:
            xl = pd.ExcelFile(master, engine="openpyxl")
            s = _find_sheet(xl, "Category Summary")
            if s:
                df = xl.parse(s)
                if "breach_rate_pct" in df.columns:
                    df = df.sort_values("breach_rate_pct", ascending=False).head(15)
                    ctx["cat_labels"]  = df["category"].astype(str).tolist()
                    ctx["cat_breach"]  = df["breach_rate_pct"].fillna(0).round(1).tolist()
                    ctx["cat_tickets"] = df.get("tickets", pd.Series()).fillna(0).astype(int).tolist()
            s = _find_sheet(xl, "Monthly Trend")
            if s:
                df = xl.parse(s)
                if "report_month" in df.columns:
                    df = df.sort_values("report_month")
                    ctx["month_labels"]  = df["report_month"].astype(str).tolist()
                    ctx["month_tickets"] = df.get("tickets", pd.Series([0]*len(df))).fillna(0).astype(int).tolist()
                    ctx["month_breach"]  = df.get("breach_rate_pct", pd.Series([0]*len(df))).fillna(0).round(1).tolist()
            s = _find_sheet(xl, "Model Info")
            if s:
                df = xl.parse(s)
                if "Metric" in df.columns and "Value" in df.columns:
                    m = dict(zip(df["Metric"].astype(str), df["Value"].astype(str)))
                    ctx["model_auc"]   = m.get("ROC-AUC", "N/A")
                    ctx["breach_rate"] = m.get("Breach rate", "N/A")
                    ctx["training_n"]  = m.get("Training tickets", "N/A")
        except Exception as e:
            print(f"  [WARN] Master report: {e}")

    files = sorted(glob.glob(str(out_dir / "PETRA_Teams_Alert_*.xlsx")), reverse=True)
    if files:
        try:
            xl2 = pd.ExcelFile(files[0], engine="openpyxl")
            s = _find_sheet(xl2, "Summary")
            if s:
                df = xl2.parse(s)
                cols = df.columns.tolist()
                if len(cols) >= 2:
                    lc, vc = cols[0], cols[1]
                    for _, row in df.iterrows():
                        lbl = str(row.get(lc, "")).upper()
                        try: val = int(float(str(row.get(vc, 0))))
                        except: continue
                        if "AT RISK" in lbl:   ctx["at_risk"]    = val
                        elif "BREACH" in lbl:  ctx["breached"]   = val
                        elif "MONITOR" in lbl: ctx["monitor"]    = val
                        elif "LOW" in lbl:     ctx["low_risk"]   = val
                        elif "TOTAL" in lbl:   ctx["total_open"] = val

            s = _find_sheet(xl2, "AT RISK", "Act Now")
            if s: ctx["atrisk_rows"] = _records(xl2.parse(s))

            s = _find_sheet(xl2, "Breach", "Breached")
            if s: ctx["breached_rows"] = _records(xl2.parse(s))

            s = _find_sheet(xl2, "Monitor")
            if s: ctx["monitor_rows"] = _records(xl2.parse(s))

            s = _find_sheet(xl2, "Zombie")
            if s:
                df_z = xl2.parse(s)
                ctx["zombie_rows"]  = _records(df_z)
                ctx["zombie_count"] = len(df_z)

            s = _find_sheet(xl2, "All Open", "Ranked")
            if s: ctx["all_open_rows"] = _records(xl2.parse(s))

            s = _find_sheet(xl2, "By Team", "Team")
            if s:
                df_t = xl2.parse(s)
                ctx["by_team_rows"]  = _records(df_t)
                if "Team" in df_t.columns:
                    ctx["team_names"]    = df_t["Team"].astype(str).tolist()
                    ctx["team_atrisk"]   = df_t.get("at_risk", pd.Series([0]*len(df_t))).fillna(0).astype(int).tolist()
                    ctx["team_breached"] = df_t.get("already_breached", pd.Series([0]*len(df_t))).fillna(0).astype(int).tolist()
        except Exception as e:
            print(f"  [WARN] Alert file: {e}")

    if ctx["total_open"] == 0:
        ctx["total_open"] = ctx["at_risk"] + ctx["breached"] + ctx["monitor"] + ctx["low_risk"]

    # Build filter option lists from all_open_rows
    all_rows = ctx["all_open_rows"]
    ctx["filter_teams"]      = _unique_vals(all_rows, "Team")
    ctx["filter_assignees"]  = _unique_vals(all_rows, "Assignee")
    ctx["filter_severities"] = _unique_vals(all_rows, "Severity")
    ctx["filter_categories"] = _unique_vals(all_rows, "Issue Type")

    return ctx


# ── CSS ───────────────────────────────────────────────────────────────────────
_CSS = """
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0A1628;--card:#132035;--nav:#0D1B2A;--border:rgba(0,161,156,.2);
  --teal:#00A19C;--orange:#F97316;--red:#EF4444;--gold:#F59E0B;--green:#22C55E;
  --text:#94A3B8;--bright:#E2E8F0;--muted:#475569}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
/* Header */
.hdr{display:flex;align-items:center;justify-content:space-between;padding:0 24px;
  height:64px;background:var(--nav);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}
.brand{display:flex;align-items:center;gap:12px}
.brand-icon{font-size:28px}
.brand h1{font-size:20px;font-weight:700;color:var(--teal);letter-spacing:.5px}
.brand p{font-size:11px;color:var(--muted)}
.hdr-meta{display:flex;gap:8px;flex-wrap:wrap}
.badge{padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;
  background:rgba(0,161,156,.1);color:var(--teal);border:1px solid var(--border)}
.badge.red{background:rgba(239,68,68,.1);color:var(--red);border-color:rgba(239,68,68,.3)}
.badge.orange{background:rgba(249,115,22,.1);color:var(--orange);border-color:rgba(249,115,22,.3)}
/* Nav */
.nav{display:flex;background:var(--card);border-bottom:1px solid var(--border);
  padding:0 16px;gap:4px;overflow-x:auto}
.tab-btn{padding:14px 18px;font-size:13px;font-weight:500;color:var(--muted);background:none;
  border:none;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;transition:all .2s}
.tab-btn:hover{color:var(--bright)}
.tab-btn.active{color:var(--teal);border-bottom-color:var(--teal)}
/* Filter bar */
.filter-bar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;
  background:var(--card);border-radius:12px;padding:14px 18px;
  margin-bottom:18px;border:1px solid var(--border)}
.filter-bar label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-right:4px}
.filter-sel{background:var(--bg);border:1px solid var(--border);border-radius:8px;
  padding:6px 12px;font-size:12px;color:var(--bright);outline:none;cursor:pointer;min-width:140px}
.filter-sel:focus{border-color:var(--teal)}
.filter-date{background:var(--bg);border:1px solid var(--border);border-radius:8px;
  padding:6px 12px;font-size:12px;color:var(--bright);outline:none;width:140px}
.filter-date:focus{border-color:var(--teal)}
.btn-reset{background:rgba(0,161,156,.15);border:1px solid var(--teal);border-radius:8px;
  padding:6px 14px;font-size:12px;color:var(--teal);cursor:pointer;transition:all .2s}
.btn-reset:hover{background:var(--teal);color:#fff}
.btn-csv{background:rgba(34,197,94,.12);border:1px solid var(--green);border-radius:8px;
  padding:6px 14px;font-size:12px;color:var(--green);cursor:pointer;transition:all .2s;margin-left:auto}
.btn-csv:hover{background:var(--green);color:#fff}
/* Content */
.content{padding:24px;max-width:1600px;margin:0 auto}
.tab-panel{display:none}.tab-panel.active{display:block}
/* KPIs */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
.kpi{background:var(--card);border-radius:12px;padding:20px 24px;border-left:4px solid var(--border);transition:transform .2s}
.kpi:hover{transform:translateY(-2px)}
.kpi.orange{border-left-color:var(--orange)}.kpi.red{border-left-color:var(--red)}
.kpi.gold{border-left-color:var(--gold)}.kpi.teal{border-left-color:var(--teal)}.kpi.green{border-left-color:var(--green)}
.kpi-label{font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:8px}
.kpi-val{font-size:36px;font-weight:700;color:var(--bright)}
.kpi.orange .kpi-val{color:var(--orange)}.kpi.red .kpi-val{color:var(--red)}
.kpi.gold .kpi-val{color:var(--gold)}.kpi.teal .kpi-val{color:var(--teal)}
.kpi-sub{font-size:11px;color:var(--muted);margin-top:4px}
/* Charts */
.charts-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px;margin-bottom:24px}
.chart-card{background:var(--card);border-radius:12px;padding:20px;border:1px solid var(--border)}
.chart-card h3{font-size:13px;font-weight:600;color:var(--bright);margin-bottom:16px;text-transform:uppercase;letter-spacing:.5px}
.chart-wrap{position:relative;height:280px}
/* Alert box */
.alert-box{background:rgba(249,115,22,.07);border:1px solid rgba(249,115,22,.25);
  border-radius:12px;padding:20px;margin-bottom:24px}
.alert-box h3{color:var(--orange);font-size:14px;margin-bottom:12px}
/* Tables */
.tbl-wrap{background:var(--card);border-radius:12px;border:1px solid var(--border);overflow:hidden;margin-bottom:24px}
.tbl-hdr{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;
  border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.tbl-hdr h3{font-size:13px;font-weight:600;color:var(--bright);text-transform:uppercase;letter-spacing:.5px}
.tbl-controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.search-inp{background:var(--bg);border:1px solid var(--border);border-radius:8px;
  padding:7px 14px;font-size:12px;color:var(--bright);outline:none;width:200px}
.search-inp:focus{border-color:var(--teal)}
.tbl-scroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12px}
thead th{padding:10px 14px;text-align:left;font-size:11px;font-weight:600;
  color:var(--muted);text-transform:uppercase;letter-spacing:.5px;
  background:rgba(0,0,0,.2);border-bottom:1px solid var(--border);cursor:pointer;user-select:none;white-space:nowrap}
thead th:hover{color:var(--bright)}
tbody tr{border-bottom:1px solid rgba(255,255,255,.04);transition:background .15s}
tbody tr:hover{background:rgba(255,255,255,.06)}
tbody td{padding:10px 14px;color:var(--text);vertical-align:middle}
tr.row-red td{background:rgba(239,68,68,.08);color:#fca5a5}
tr.row-orange td{background:rgba(249,115,22,.07);color:#fdba74}
tr.row-gold td{background:rgba(245,158,11,.06);color:#fcd34d}
.pill{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600}
.pill-red{background:rgba(239,68,68,.2);color:#fca5a5}
.pill-orange{background:rgba(249,115,22,.2);color:#fdba74}
.pill-gold{background:rgba(245,158,11,.2);color:#fde68a}
.pill-green{background:rgba(34,197,94,.2);color:#86efac}
.breach-dt{color:#f87171;font-size:11px;font-weight:600}
.section-title{font-size:16px;font-weight:700;color:var(--bright);margin-bottom:16px;
  padding-bottom:8px;border-bottom:1px solid var(--border)}
.empty-msg{padding:32px;text-align:center;color:var(--muted);font-size:13px}
.info-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:24px}
.info-item{background:var(--card);border-radius:8px;padding:14px 16px;border:1px solid var(--border)}
.info-item .lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px}
.info-item .val{font-size:18px;font-weight:700;color:var(--teal);margin-top:4px}
.row-count{font-size:11px;color:var(--muted);padding:8px 18px;border-top:1px solid var(--border)}
"""

# ── HTML skeleton ─────────────────────────────────────────────────────────────
_HTML_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PETRA Risk Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<style>STYLE_PLACEHOLDER</style>
</head>
<body>

<header class="hdr">
  <div class="brand">
    <span class="brand-icon">⚡</span>
    <div><h1>PETRA</h1><p>Predictive Engine for TRMS Risk &amp; Alerts — Petronas Trading Digital</p></div>
  </div>
  <div class="hdr-meta" id="hdr-badges"></div>
</header>

<nav class="nav">
  <button class="tab-btn active" onclick="showTab('overview',this)">📊 Overview</button>
  <button class="tab-btn" onclick="showTab('atrisk',this)">⚠️ AT RISK</button>
  <button class="tab-btn" onclick="showTab('breached',this)">🔴 Already Breached</button>
  <button class="tab-btn" onclick="showTab('monitor',this)">🟡 Monitor</button>
  <button class="tab-btn" onclick="showTab('zombie',this)">🧟 Zombies</button>
  <button class="tab-btn" onclick="showTab('allopen',this)">📋 All Open</button>
  <button class="tab-btn" onclick="showTab('categories',this)">📊 By Category</button>
  <button class="tab-btn" onclick="showTab('teams',this)">👥 By Team</button>
  <button class="tab-btn" onclick="showTab('trends',this)">📈 Trends</button>
</nav>

<div class="content">

<!-- GLOBAL FILTER BAR -->
<div class="filter-bar" id="global-filters">
  <label>From</label>
  <input type="date" class="filter-date" id="f-date-from" onchange="applyFilters()">
  <label>To</label>
  <input type="date" class="filter-date" id="f-date-to" onchange="applyFilters()">
  <label>Team</label>
  <select class="filter-sel" id="f-team" onchange="applyFilters()"><option value="">All Teams</option></select>
  <label>Assignee</label>
  <select class="filter-sel" id="f-assignee" onchange="applyFilters()"><option value="">All Assignees</option></select>
  <label>Severity</label>
  <select class="filter-sel" id="f-severity" onchange="applyFilters()"><option value="">All Severities</option></select>
  <label>Category</label>
  <select class="filter-sel" id="f-category" onchange="applyFilters()"><option value="">All Categories</option></select>
  <button class="btn-reset" onclick="resetFilters()">↺ Reset</button>
</div>

<!-- OVERVIEW -->
<div id="tab-overview" class="tab-panel active">
  <div class="kpi-grid" id="kpi-grid"></div>
  <div class="alert-box" id="alert-box" style="display:none">
    <h3>⚠️ Top AT RISK tickets — act before SLA expires</h3>
    <div id="top5-list"></div>
  </div>
  <div class="charts-grid">
    <div class="chart-card"><h3>Risk Distribution</h3><div class="chart-wrap"><canvas id="donutChart"></canvas></div></div>
    <div class="chart-card"><h3>Top Categories — Breach Rate %</h3><div class="chart-wrap"><canvas id="catChart"></canvas></div></div>
  </div>
  <div class="info-grid" id="model-info-grid"></div>
</div>

<!-- AT RISK -->
<div id="tab-atrisk" class="tab-panel">
  <p class="section-title">⚠️ AT RISK — Still saveable. Act before predicted breach time.</p>
  <div class="tbl-wrap">
    <div class="tbl-hdr">
      <h3 id="atrisk-count">AT RISK Tickets</h3>
      <div class="tbl-controls">
        <input class="search-inp" placeholder="🔍 Search…" oninput="searchTable('atrisk-body',this.value)">
        <button class="btn-csv" onclick="downloadCSV('atrisk-body','atrisk-head','PETRA_AT_RISK')">⬇ CSV</button>
      </div>
    </div>
    <div class="tbl-scroll"><table><thead id="atrisk-head"></thead><tbody id="atrisk-body"></tbody></table></div>
    <div class="row-count" id="atrisk-rowcount"></div>
  </div>
</div>

<!-- ALREADY BREACHED -->
<div id="tab-breached" class="tab-panel">
  <p class="section-title">🔴 Already Breached — SLA missed. Escalate immediately.</p>
  <div class="tbl-wrap">
    <div class="tbl-hdr">
      <h3 id="breached-count">Breached Tickets</h3>
      <div class="tbl-controls">
        <input class="search-inp" placeholder="🔍 Search…" oninput="searchTable('breached-body',this.value)">
        <button class="btn-csv" onclick="downloadCSV('breached-body','breached-head','PETRA_Breached')">⬇ CSV</button>
      </div>
    </div>
    <div class="tbl-scroll"><table><thead id="breached-head"></thead><tbody id="breached-body"></tbody></table></div>
    <div class="row-count" id="breached-rowcount"></div>
  </div>
</div>

<!-- MONITOR -->
<div id="tab-monitor" class="tab-panel">
  <p class="section-title">🟡 Monitor — Medium risk. Review daily.</p>
  <div class="tbl-wrap">
    <div class="tbl-hdr">
      <h3 id="monitor-count">Monitor Tickets</h3>
      <div class="tbl-controls">
        <input class="search-inp" placeholder="🔍 Search…" oninput="searchTable('monitor-body',this.value)">
        <button class="btn-csv" onclick="downloadCSV('monitor-body','monitor-head','PETRA_Monitor')">⬇ CSV</button>
      </div>
    </div>
    <div class="tbl-scroll"><table><thead id="monitor-head"></thead><tbody id="monitor-body"></tbody></table></div>
    <div class="row-count" id="monitor-rowcount"></div>
  </div>
</div>

<!-- ZOMBIE -->
<div id="tab-zombie" class="tab-panel">
  <p class="section-title">🧟 Zombie Tickets — Open more than 2× SLA target. Forgotten tickets.</p>
  <div class="tbl-wrap">
    <div class="tbl-hdr">
      <h3 id="zombie-count">Zombie Tickets</h3>
      <div class="tbl-controls">
        <input class="search-inp" placeholder="🔍 Search…" oninput="searchTable('zombie-body',this.value)">
        <button class="btn-csv" onclick="downloadCSV('zombie-body','zombie-head','PETRA_Zombies')">⬇ CSV</button>
      </div>
    </div>
    <div class="tbl-scroll"><table><thead id="zombie-head"></thead><tbody id="zombie-body"></tbody></table></div>
    <div class="row-count" id="zombie-rowcount"></div>
  </div>
</div>

<!-- ALL OPEN -->
<div id="tab-allopen" class="tab-panel">
  <p class="section-title">📋 All Open Tickets — Risk Ranked</p>
  <div class="tbl-wrap">
    <div class="tbl-hdr">
      <h3 id="allopen-count">All Open Tickets</h3>
      <div class="tbl-controls">
        <input class="search-inp" placeholder="🔍 Search…" oninput="searchTable('allopen-body',this.value)">
        <button class="btn-csv" onclick="downloadCSV('allopen-body','allopen-head','PETRA_All_Open')">⬇ CSV</button>
      </div>
    </div>
    <div class="tbl-scroll"><table><thead id="allopen-head"></thead><tbody id="allopen-body"></tbody></table></div>
    <div class="row-count" id="allopen-rowcount"></div>
  </div>
</div>

<!-- BY CATEGORY -->
<div id="tab-categories" class="tab-panel">
  <p class="section-title">📊 Historical Breach Rate by Issue Category</p>
  <div class="charts-grid">
    <div class="chart-card" style="grid-column:1/-1">
      <h3>Breach Rate per Category</h3>
      <div class="chart-wrap" style="height:420px"><canvas id="catDetailChart"></canvas></div>
    </div>
  </div>
</div>

<!-- BY TEAM -->
<div id="tab-teams" class="tab-panel">
  <p class="section-title">👥 Team Performance</p>
  <div class="charts-grid">
    <div class="chart-card"><h3>AT RISK &amp; Breached by Team</h3><div class="chart-wrap"><canvas id="teamChart"></canvas></div></div>
    <div class="chart-card">
      <h3>Team Summary</h3>
      <div class="tbl-scroll"><table><thead id="team-head"></thead><tbody id="team-body"></tbody></table></div>
    </div>
  </div>
</div>

<!-- TRENDS -->
<div id="tab-trends" class="tab-panel">
  <p class="section-title">📈 Monthly Trend</p>
  <div class="charts-grid">
    <div class="chart-card" style="grid-column:1/-1">
      <h3>Ticket Volume &amp; Breach Rate by Month</h3>
      <div class="chart-wrap" style="height:340px"><canvas id="trendChart"></canvas></div>
    </div>
  </div>
</div>

</div><!-- /content -->
<script>
// ── DATA injected by Python ──────────────────────────────────────────────────
DATA_PLACEHOLDER
</script>
</body></html>
"""

# ── JavaScript ────────────────────────────────────────────────────────────────
_JS = r"""
// ── state ────────────────────────────────────────────────────────────────────
let _filtered = {
  atrisk: [], breached: [], monitor: [], zombie: [], allopen: []
};

// ── tab switching ─────────────────────────────────────────────────────────────
function showTab(name, btn) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (btn) btn.classList.add('active');
}

// ── filter helpers ────────────────────────────────────────────────────────────
function getFilters() {
  return {
    dateFrom : document.getElementById('f-date-from').value,
    dateTo   : document.getElementById('f-date-to').value,
    team     : document.getElementById('f-team').value,
    assignee : document.getElementById('f-assignee').value,
    severity : document.getElementById('f-severity').value,
    category : document.getElementById('f-category').value,
  };
}

function matchRow(row, f) {
  // Date filter — compare against "Raised On" or "Reported Date"
  const raisedRaw = row['Raised On'] || row['Reported Date'] || '';
  if (f.dateFrom || f.dateTo) {
    const d = new Date(raisedRaw);
    if (!isNaN(d)) {
      if (f.dateFrom && d < new Date(f.dateFrom)) return false;
      if (f.dateTo   && d > new Date(f.dateTo + 'T23:59:59')) return false;
    }
  }
  if (f.team     && (row['Team']       || '') !== f.team)     return false;
  if (f.assignee && (row['Assignee']   || '') !== f.assignee) return false;
  if (f.severity && (row['Severity']   || '') !== f.severity) return false;
  if (f.category && (row['Issue Type'] || '') !== f.category) return false;
  return true;
}

function applyFilters() {
  const f = getFilters();
  _filtered.atrisk   = P.atrisk_rows.filter(r => matchRow(r, f));
  _filtered.breached = P.breached_rows.filter(r => matchRow(r, f));
  _filtered.monitor  = P.monitor_rows.filter(r => matchRow(r, f));
  _filtered.zombie   = P.zombie_rows.filter(r => matchRow(r, f));
  _filtered.allopen  = P.all_open_rows.filter(r => matchRow(r, f));
  renderAllTables();
  updateKPIs();
}

function resetFilters() {
  ['f-date-from','f-date-to','f-team','f-assignee','f-severity','f-category']
    .forEach(id => { const el = document.getElementById(id); el.value = ''; });
  applyFilters();
}

// ── populate filter dropdowns ─────────────────────────────────────────────────
function buildFilters() {
  const add = (id, vals) => {
    const sel = document.getElementById(id);
    vals.forEach(v => { const o = document.createElement('option'); o.value = o.text = v; sel.appendChild(o); });
  };
  add('f-team',      P.filter_teams);
  add('f-assignee',  P.filter_assignees);
  add('f-severity',  P.filter_severities);
  add('f-category',  P.filter_categories);
}

// ── risk pill ─────────────────────────────────────────────────────────────────
function riskPill(val) {
  if (val === null || val === undefined || val === '') return '';
  const n = parseFloat(val);
  if (isNaN(n)) return String(val);
  const cls = n >= 90 ? 'pill-red' : n >= 70 ? 'pill-orange' : n >= 40 ? 'pill-gold' : 'pill-green';
  return `<span class="pill ${cls}">${n}%</span>`;
}

function rowClass(row) {
  const sev  = String(row['Severity'] || row['severity_clean'] || '');
  const risk = parseFloat(row['Breach Risk %'] || row['breach_risk_%'] || 0);
  if (sev.includes('Breach')) return 'row-red';
  if (risk >= 70) return 'row-orange';
  if (risk >= 40) return 'row-gold';
  return '';
}

// ── table renderer ────────────────────────────────────────────────────────────
const RISK_COLS    = new Set(['Breach Risk %', 'breach_risk_%']);
const BREACH_COLS  = new Set(['Predicted Breach', 'predicted_breach_dt']);
const SKIP_COLS    = new Set(['Match Debug', 'Desc Extract', 'Reason']);

function renderTable(headId, bodyId, rows, countId) {
  const tbody = document.getElementById(bodyId);
  const thead = document.getElementById(headId);
  if (!rows || !rows.length) {
    thead.innerHTML = '';
    tbody.innerHTML = `<tr><td colspan="20" class="empty-msg">No tickets match the current filters.</td></tr>`;
    if (countId) document.getElementById(countId).textContent = '0 tickets';
    return;
  }
  const cols = Object.keys(rows[0]).filter(c => !SKIP_COLS.has(c));
  thead.innerHTML = '<tr>' + cols.map((c,i) =>
    `<th onclick="sortTable('${bodyId}',${i})" title="Click to sort">${c} ↕</th>`
  ).join('') + '</tr>';
  tbody.innerHTML = rows.map(row => {
    const cls = rowClass(row);
    const cells = cols.map(c => {
      let v = row[c] ?? '';
      if (RISK_COLS.has(c))   return `<td>${riskPill(v)}</td>`;
      if (BREACH_COLS.has(c)) return `<td><span class="breach-dt">${v || '—'}</span></td>`;
      return `<td>${v}</td>`;
    }).join('');
    return `<tr class="${cls}">${cells}</tr>`;
  }).join('');
  if (countId) document.getElementById(countId).textContent = `${rows.length} ticket${rows.length !== 1 ? 's' : ''}`;
}

function renderAllTables() {
  renderTable('atrisk-head',   'atrisk-body',   _filtered.atrisk,   'atrisk-rowcount');
  renderTable('breached-head', 'breached-body', _filtered.breached, 'breached-rowcount');
  renderTable('monitor-head',  'monitor-body',  _filtered.monitor,  'monitor-rowcount');
  renderTable('zombie-head',   'zombie-body',   _filtered.zombie,   'zombie-rowcount');
  renderTable('allopen-head',  'allopen-body',  _filtered.allopen,  'allopen-rowcount');
  renderTable('team-head',     'team-body',     P.by_team_rows,     null);

  document.getElementById('atrisk-count').textContent   = `AT RISK (${_filtered.atrisk.length})`;
  document.getElementById('breached-count').textContent = `Already Breached (${_filtered.breached.length})`;
  document.getElementById('monitor-count').textContent  = `Monitor (${_filtered.monitor.length})`;
  document.getElementById('zombie-count').textContent   = `Zombie Tickets (${_filtered.zombie.length})`;
  document.getElementById('allopen-count').textContent  = `All Open (${_filtered.allopen.length})`;
}

// ── table search ──────────────────────────────────────────────────────────────
function searchTable(tbodyId, q) {
  const lq = q.toLowerCase();
  document.getElementById(tbodyId).querySelectorAll('tr').forEach(r => {
    r.style.display = r.textContent.toLowerCase().includes(lq) ? '' : 'none';
  });
}

// ── column sort ───────────────────────────────────────────────────────────────
const _sortState = {};
function sortTable(tbodyId, colIdx) {
  const tbody = document.getElementById(tbodyId);
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const asc   = !_sortState[tbodyId + colIdx];
  _sortState[tbodyId + colIdx] = asc;
  rows.sort((a, b) => {
    const av = a.cells[colIdx]?.textContent.trim() || '';
    const bv = b.cells[colIdx]?.textContent.trim() || '';
    const an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(r => tbody.appendChild(r));
}

// ── CSV download ──────────────────────────────────────────────────────────────
function downloadCSV(tbodyId, theadId, filename) {
  const thead = document.getElementById(theadId);
  const tbody = document.getElementById(tbodyId);
  const headers = Array.from(thead.querySelectorAll('th'))
    .map(th => '"' + th.textContent.replace(/\s*↕\s*$/, '').replace(/"/g, '""') + '"');
  const rows = Array.from(tbody.querySelectorAll('tr'))
    .filter(r => r.style.display !== 'none')
    .map(r => Array.from(r.cells).map(td => '"' + td.textContent.replace(/"/g, '""') + '"').join(','));
  const csv  = [headers.join(','), ...rows].join('\n');
  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename + '_' + new Date().toISOString().slice(0,10) + '.csv';
  a.click(); URL.revokeObjectURL(url);
}

// ── KPI cards (dynamic — reflect filtered counts) ─────────────────────────────
function updateKPIs() {
  const cards = [
    {label:'AT RISK — Act Now',   val:_filtered.atrisk.length,   sub:'Preventable breaches',      cls:'orange'},
    {label:'Already Breached',    val:_filtered.breached.length, sub:'SLA missed — escalate now', cls:'red'},
    {label:'Monitor',             val:_filtered.monitor.length,  sub:'Medium risk, review daily', cls:'gold'},
    {label:'🧟 Zombie Tickets',   val:_filtered.zombie.length,   sub:'>2× SLA age, forgotten',    cls:'red'},
    {label:'Low Risk',            val:P.low_risk,                sub:'On track',                  cls:'green'},
    {label:'Total Open',          val:_filtered.allopen.length,  sub:'Matching current filters',  cls:'teal'},
  ];
  document.getElementById('kpi-grid').innerHTML = cards.map(c =>
    `<div class="kpi ${c.cls}"><div class="kpi-label">${c.label}</div>
     <div class="kpi-val">${c.val}</div><div class="kpi-sub">${c.sub}</div></div>`
  ).join('');
}

// ── header badges ─────────────────────────────────────────────────────────────
function buildBadges() {
  document.getElementById('hdr-badges').innerHTML = `
    <span class="badge">📅 ${P.report_date}</span>
    <span class="badge">AUC: ${P.model_auc}</span>
    <span class="badge">Breach Rate: ${P.breach_rate}</span>
    ${P.at_risk    > 0 ? `<span class="badge orange">⚠️ ${P.at_risk} AT RISK</span>`    : ''}
    ${P.breached   > 0 ? `<span class="badge red">🔴 ${P.breached} BREACHED</span>`     : ''}
    ${P.zombie_count>0 ? `<span class="badge red">🧟 ${P.zombie_count} ZOMBIE</span>`   : ''}
  `;
}

// ── top-5 alert box ───────────────────────────────────────────────────────────
function buildAlertBox() {
  const rows = _filtered.atrisk.slice(0, 5);
  if (!rows.length) return;
  const ab = document.getElementById('alert-box');
  ab.style.display = 'block';
  const keys = Object.keys(rows[0]);
  const idCol  = keys.find(k => /ticket|incident/i.test(k));
  const sumCol = keys.find(k => /^summary$/i.test(k));
  const rCol   = keys.find(k => /risk %/i.test(k));
  const tCol   = keys.find(k => /^team$/i.test(k));
  const bCol   = keys.find(k => /predicted breach/i.test(k));
  document.getElementById('top5-list').innerHTML = rows.map(r =>
    `<div style="display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid rgba(249,115,22,.15)">
      <span style="font-weight:700;color:#fdba74;min-width:110px">${r[idCol]||''}</span>
      <span style="flex:1;color:#e2e8f0;font-size:12px">${String(r[sumCol]||'').slice(0,70)}</span>
      ${rCol ? riskPill(r[rCol]) : ''}
      ${bCol && r[bCol] ? `<span class="breach-dt">⏰ ${r[bCol]}</span>` : ''}
      <span style="font-size:11px;color:#94a3b8">${r[tCol]||''}</span>
    </div>`
  ).join('');
}

// ── model info ────────────────────────────────────────────────────────────────
function buildModelInfo() {
  document.getElementById('model-info-grid').innerHTML = [
    {lbl:'Model AUC',        val:P.model_auc},
    {lbl:'SLA Breach Rate',  val:P.breach_rate},
    {lbl:'Training Tickets', val:P.training_n},
    {lbl:'Report Date',      val:P.report_date},
    {lbl:'Generated',        val:P.generated_at},
  ].map(x => `<div class="info-item"><div class="lbl">${x.lbl}</div><div class="val">${x.val}</div></div>`).join('');
}

// ── charts ────────────────────────────────────────────────────────────────────
const C = {
  teal:'#00A19C', orange:'#F97316', red:'#EF4444',
  gold:'#F59E0B', green:'#22C55E',
  grid:'rgba(255,255,255,0.06)', text:'#94A3B8'
};
Chart.defaults.color = C.text;
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

function buildDonut() {
  new Chart(document.getElementById('donutChart'), {
    type: 'doughnut',
    data: {
      labels: ['AT RISK','Already Breached','Monitor','Low Risk'],
      datasets: [{ data:[P.at_risk,P.breached,P.monitor,P.low_risk],
        backgroundColor:[C.orange,C.red,C.gold,C.green], borderWidth:0, hoverOffset:8 }]
    },
    options: { responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{position:'bottom',labels:{padding:16,boxWidth:12}},
        tooltip:{callbacks:{label:ctx=>` ${ctx.label}: ${ctx.raw} tickets`}} } }
  });
}

function buildCatChart(id) {
  if (!P.cat_labels.length) return;
  new Chart(document.getElementById(id), {
    type:'bar',
    data:{ labels:P.cat_labels, datasets:[{ label:'Breach Rate %', data:P.cat_breach,
      backgroundColor:P.cat_breach.map(v=>v>=50?C.red:v>=25?C.orange:C.gold), borderRadius:4 }] },
    options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{ x:{grid:{color:C.grid},ticks:{callback:v=>v+'%'},max:100},
               y:{grid:{display:false},ticks:{font:{size:11}}} } }
  });
}

function buildTeamChart() {
  if (!P.team_names.length) return;
  new Chart(document.getElementById('teamChart'), {
    type:'bar',
    data:{ labels:P.team_names, datasets:[
      {label:'AT RISK',  data:P.team_atrisk,   backgroundColor:C.orange, borderRadius:4},
      {label:'Breached', data:P.team_breached, backgroundColor:C.red,    borderRadius:4}
    ]},
    options:{ responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'bottom'}},
      scales:{ x:{grid:{display:false}}, y:{grid:{color:C.grid},ticks:{stepSize:1}} } }
  });
}

function buildTrendChart() {
  if (!P.month_labels.length) return;
  new Chart(document.getElementById('trendChart'), {
    type:'bar',
    data:{ labels:P.month_labels, datasets:[
      { type:'bar',  label:'Tickets',      data:P.month_tickets,
        backgroundColor:'rgba(0,161,156,.35)', borderColor:C.teal, borderWidth:1, borderRadius:3, yAxisID:'y' },
      { type:'line', label:'Breach Rate %', data:P.month_breach,
        borderColor:C.orange, backgroundColor:'transparent',
        pointBackgroundColor:C.orange, borderWidth:2, tension:0.35, yAxisID:'y1' }
    ]},
    options:{ responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'bottom'}},
      scales:{
        x:{grid:{color:C.grid},ticks:{maxRotation:45}},
        y:{grid:{color:C.grid},title:{display:true,text:'Ticket Count'}},
        y1:{position:'right',grid:{display:false},ticks:{callback:v=>v+'%'},
            title:{display:true,text:'Breach Rate %'},max:100}
      } }
  });
}

// ── init ──────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  buildFilters();
  applyFilters();   // sets _filtered from full data, renders tables
  buildBadges();
  buildAlertBox();
  buildModelInfo();
  buildDonut();
  buildCatChart('catChart');
  buildCatChart('catDetailChart');
  buildTeamChart();
  buildTrendChart();
});
"""


# ── generate + run ────────────────────────────────────────────────────────────
def generate_html(ctx: dict) -> str:
    data_js = f"const P = {json.dumps(ctx, default=str)};\n"
    html = _HTML_HEAD.replace("STYLE_PLACEHOLDER", _CSS)
    html = html.replace("DATA_PLACEHOLDER", data_js + _JS)
    return html


def run_dashboard(open_browser: bool = True) -> Path:
    print("=" * 60)
    print("PETRA — Dashboard Generator")
    print("=" * 60)

    master_exists = (OUT_DIR / "PETRA_Master_Report.xlsx").exists()
    alert_exists  = bool(list(OUT_DIR.glob("PETRA_Teams_Alert_*.xlsx")))
    if not master_exists and not alert_exists:
        print("\n  [WARN] No PETRA output files found. Run main.py first.")

    print(f"  Reading from: {OUT_DIR}")
    ctx  = load_data(OUT_DIR)
    html = generate_html(ctx)

    out_path = OUT_DIR / f"PETRA_Dashboard_{datetime.now().strftime('%Y-%m-%d')}.html"
    out_path.write_text(html, encoding="utf-8")

    print(f"\n  ✅ Dashboard saved → {out_path}")
    print(f"     AT RISK: {ctx['at_risk']}  |  Breached: {ctx['breached']}  "
          f"|  Monitor: {ctx['monitor']}  |  Zombies: {ctx['zombie_count']}  "
          f"|  Total Open: {ctx['total_open']}")

    if open_browser:
        try:
            webbrowser.open(out_path.as_uri())
            print("  🌐 Opening in browser…")
        except Exception:
            print("  (Could not auto-open browser — open the file manually)")

    return out_path


if __name__ == "__main__":
    run_dashboard()
