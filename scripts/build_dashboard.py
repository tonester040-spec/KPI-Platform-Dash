#!/usr/bin/env python3
"""
build_dashboard.py
KPI Platform — Karissa's Salon Network

Reads the last 4 weeks of data from the Google Sheet DATA tab,
processes it into summary stats and chart data, then generates
dashboard/index.html — a self-contained, offline-capable BI dashboard.

Usage:
    python scripts/build_dashboard.py

Output:
    dashboard/index.html
"""

import os
import sys
import json
import base64
import datetime
from pathlib import Path
from collections import defaultdict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ─── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT      = Path(__file__).resolve().parent.parent
CONFIG_PATH    = REPO_ROOT / "config" / "customers" / "karissa_001.json"
OUTPUT_PATH    = REPO_ROOT / "dashboard" / "index.html"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
NUM_WEEKS      = 4


# ─── Auth / Config ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text())


def build_sheets_service():
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")
    sa_info = json.loads(base64.b64decode(raw_b64).decode())
    creds   = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


# ─── Data fetching ────────────────────────────────────────────────────────────

def fetch_data_tab(service, spreadsheet_id: str) -> list[dict]:
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="DATA!A1:T100000",
    ).execute()
    raw = result.get("values", [])
    if not raw:
        return []
    headers = raw[0]
    rows = []
    for r in raw[1:]:
        # Pad short rows
        while len(r) < len(headers):
            r.append("")
        row = dict(zip(headers, r))
        rows.append(row)
    return rows


# ─── Data processing ──────────────────────────────────────────────────────────

def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def process_data(rows: list[dict]) -> dict:
    # Get sorted unique week dates
    all_dates = sorted(set(r["Week Ending"] for r in rows if r.get("Week Ending")))
    last_4_dates = all_dates[-NUM_WEEKS:]
    if len(last_4_dates) < 2:
        raise ValueError("Need at least 2 weeks of data to compute trends.")

    current_week = last_4_dates[-1]
    prior_week   = last_4_dates[-2]

    # Filter to last 4 weeks
    recent = [r for r in rows if r.get("Week Ending") in last_4_dates]

    # Group by week
    by_week = defaultdict(list)
    for r in recent:
        by_week[r["Week Ending"]].append(r)

    # ── Summary cards: current week vs prior week ─────────────────────────────
    def week_totals(week_rows):
        total_sales  = sum(safe_float(r.get("Total Sales $")) for r in week_rows)
        total_guests = sum(safe_float(r.get("Guest Count"))   for r in week_rows)
        avg_pph      = (
            sum(safe_float(r.get("PPH $")) for r in week_rows) / len(week_rows)
            if week_rows else 0.0
        )
        avg_product_pct = (
            sum(safe_float(r.get("Product %")) for r in week_rows) / len(week_rows)
            if week_rows else 0.0
        )
        return {
            "total_sales":       total_sales,
            "total_guests":      int(total_guests),
            "avg_pph":           avg_pph,
            "avg_product_pct":   avg_product_pct,
        }

    cur  = week_totals(by_week[current_week])
    prev = week_totals(by_week[prior_week])

    def trend(cur_val, prev_val):
        if prev_val == 0:
            return 0.0
        return ((cur_val - prev_val) / prev_val) * 100

    summary_cards = [
        {
            "label":  "Network Sales",
            "value":  f"${cur['total_sales']:,.0f}",
            "trend":  trend(cur["total_sales"], prev["total_sales"]),
            "prefix": "$",
            "suffix": "",
        },
        {
            "label":  "Avg PPH",
            "value":  f"${cur['avg_pph']:.2f}",
            "trend":  trend(cur["avg_pph"], prev["avg_pph"]),
            "prefix": "$",
            "suffix": "/hr",
        },
        {
            "label":  "Avg Product %",
            "value":  f"{cur['avg_product_pct']:.1f}%",
            "trend":  trend(cur["avg_product_pct"], prev["avg_product_pct"]),
            "prefix": "",
            "suffix": "%",
        },
        {
            "label":  "Total Guests",
            "value":  f"{cur['total_guests']:,}",
            "trend":  trend(cur["total_guests"], prev["total_guests"]),
            "prefix": "",
            "suffix": "",
        },
    ]

    # ── Leaderboard: current week, ranked by Total Sales ─────────────────────
    cur_by_loc  = {r["Location Name"]: r for r in by_week[current_week]}
    prev_by_loc = {r["Location Name"]: r for r in by_week[prior_week]}

    leaderboard = []
    for loc, row in cur_by_loc.items():
        sales_cur  = safe_float(row.get("Total Sales $"))
        sales_prev = safe_float(prev_by_loc.get(loc, {}).get("Total Sales $", 0))
        delta      = sales_cur - sales_prev
        leaderboard.append({
            "name":        loc,
            "total_sales": sales_cur,
            "pph":         safe_float(row.get("PPH $")),
            "product_pct": safe_float(row.get("Product %")),
            "guests":      int(safe_float(row.get("Guest Count"))),
            "delta":       delta,
        })

    leaderboard.sort(key=lambda x: x["total_sales"], reverse=True)
    for i, loc in enumerate(leaderboard):
        loc["rank"] = i + 1

    # ── 4-week network sales trend (line chart) ───────────────────────────────
    trend_labels = last_4_dates
    trend_values = [
        round(sum(safe_float(r.get("Total Sales $")) for r in by_week[d]), 2)
        for d in last_4_dates
    ]

    # ── Product % by location this week (horizontal bar chart) ───────────────
    prod_chart = sorted(
        [{"name": r["Location Name"], "value": safe_float(r.get("Product %"))}
         for r in by_week[current_week]],
        key=lambda x: x["value"],
        reverse=True,
    )
    network_avg_product_pct = cur["avg_product_pct"]

    return {
        "current_week":          current_week,
        "summary_cards":         summary_cards,
        "leaderboard":           leaderboard,
        "trend_labels":          trend_labels,
        "trend_values":          trend_values,
        "prod_chart":            prod_chart,
        "network_avg_product_pct": network_avg_product_pct,
    }


# ─── HTML generation ──────────────────────────────────────────────────────────

def render_dashboard(data: dict) -> str:
    week        = data["current_week"]
    cards       = data["summary_cards"]
    leaderboard = data["leaderboard"]
    tl          = json.dumps(data["trend_labels"])
    tv          = json.dumps(data["trend_values"])
    pb_names    = json.dumps([p["name"]  for p in data["prod_chart"]])
    pb_values   = json.dumps([p["value"] for p in data["prod_chart"]])
    net_avg_pct = round(data["network_avg_product_pct"], 1)

    # Summary cards HTML
    def card_html(c):
        t = c["trend"]
        if t > 0:
            arrow = "▲"
            tclass = "trend-up"
            ttext  = f"+{t:.1f}%"
        elif t < 0:
            arrow = "▼"
            tclass = "trend-down"
            ttext  = f"{t:.1f}%"
        else:
            arrow = "—"
            tclass = "trend-neutral"
            ttext  = "0.0%"
        return f"""
        <div class="summary-card">
          <div class="card-label">{c['label']}</div>
          <div class="card-value">{c['value']}</div>
          <div class="card-trend {tclass}">{arrow} {ttext} vs last week</div>
        </div>"""

    cards_html = "\n".join(card_html(c) for c in cards)

    # Leaderboard rows HTML
    def lb_row(loc):
        rank   = loc["rank"]
        delta  = loc["delta"]
        dclass = "delta-pos" if delta >= 0 else "delta-neg"
        dsign  = "+" if delta >= 0 else ""
        if rank <= 3:
            row_class = "row-top"
        elif rank >= len(leaderboard) - 1:
            row_class = "row-bottom"
        else:
            row_class = ""
        return f"""
        <tr class="{row_class}">
          <td class="rank-cell">{'🥇' if rank==1 else '🥈' if rank==2 else '🥉' if rank==3 else rank}</td>
          <td class="loc-name">{loc['name']}</td>
          <td>${loc['total_sales']:,.0f}</td>
          <td>${loc['pph']:.2f}</td>
          <td>{loc['product_pct']:.1f}%</td>
          <td>{loc['guests']:,}</td>
          <td class="{dclass}">{dsign}${delta:,.0f}</td>
        </tr>"""

    lb_html = "\n".join(lb_row(loc) for loc in leaderboard)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>KPI — Karissa Performance Intelligence</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      font-family: 'Inter', -apple-system, sans-serif;
      background: #F5F7FA;
      color: #1A1A2E;
      font-size: 14px;
      line-height: 1.5;
    }}

    /* ── Header ── */
    .header {{
      background: #fff;
      border-bottom: 1px solid #E8ECF0;
      padding: 0 32px;
      height: 60px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
    }}
    .header-left {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}
    .logo {{
      font-size: 22px;
      font-weight: 800;
      color: #1E3A5F;
      letter-spacing: -0.5px;
    }}
    .logo span {{
      color: #4A90D9;
    }}
    .subtitle {{
      font-size: 13px;
      color: #7A8BA0;
      font-weight: 500;
      border-left: 1px solid #E8ECF0;
      padding-left: 12px;
    }}
    .header-right {{
      font-size: 13px;
      color: #7A8BA0;
      font-weight: 500;
    }}
    .header-right strong {{
      color: #1E3A5F;
      font-weight: 600;
    }}

    /* ── Main layout ── */
    .main {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 28px 32px;
    }}

    .section-title {{
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #7A8BA0;
      margin-bottom: 12px;
    }}

    /* ── Summary cards ── */
    .cards-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-bottom: 28px;
    }}
    .summary-card {{
      background: #fff;
      border-radius: 10px;
      padding: 20px 22px;
      border: 1px solid #E8ECF0;
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }}
    .card-label {{
      font-size: 12px;
      font-weight: 600;
      color: #7A8BA0;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 8px;
    }}
    .card-value {{
      font-size: 28px;
      font-weight: 800;
      color: #1E3A5F;
      letter-spacing: -0.5px;
      margin-bottom: 8px;
    }}
    .card-trend {{
      font-size: 12px;
      font-weight: 600;
    }}
    .trend-up   {{ color: #16A34A; }}
    .trend-down {{ color: #DC2626; }}
    .trend-neutral {{ color: #7A8BA0; }}

    /* ── Leaderboard ── */
    .leaderboard-section {{
      background: #fff;
      border-radius: 10px;
      border: 1px solid #E8ECF0;
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
      overflow: hidden;
      margin-bottom: 28px;
    }}
    .leaderboard-header {{
      padding: 18px 24px 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    thead th {{
      font-size: 11px;
      font-weight: 700;
      color: #7A8BA0;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 10px 16px;
      text-align: left;
      border-bottom: 1px solid #E8ECF0;
      background: #FAFBFC;
    }}
    tbody tr {{
      border-bottom: 1px solid #F0F3F6;
      transition: background 0.15s;
    }}
    tbody tr:last-child {{ border-bottom: none; }}
    tbody tr:hover {{ background: #F8FAFC; }}
    tbody td {{
      padding: 13px 16px;
      font-size: 13.5px;
      color: #2D3748;
    }}
    .rank-cell {{
      font-size: 16px;
      width: 48px;
      text-align: center;
    }}
    .loc-name {{
      font-weight: 600;
      color: #1E3A5F;
    }}
    .delta-pos {{ color: #16A34A; font-weight: 600; }}
    .delta-neg {{ color: #DC2626; font-weight: 600; }}

    /* Accent borders by performance tier */
    tr.row-top td:first-child {{
      border-left: 3px solid #F59E0B;
      padding-left: 13px;
    }}
    tr.row-bottom td:first-child {{
      border-left: 3px solid #EF4444;
      padding-left: 13px;
    }}

    /* ── Charts ── */
    .charts-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      margin-bottom: 28px;
    }}
    .chart-card {{
      background: #fff;
      border-radius: 10px;
      border: 1px solid #E8ECF0;
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
      padding: 22px 24px;
    }}
    .chart-title {{
      font-size: 14px;
      font-weight: 700;
      color: #1E3A5F;
      margin-bottom: 18px;
    }}
    .chart-container {{
      position: relative;
      height: 260px;
    }}

    /* ── Footer ── */
    .footer {{
      background: #fff;
      border-top: 1px solid #E8ECF0;
      padding: 16px 32px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
      color: #A0AEC0;
      margin-top: 8px;
    }}
  </style>
</head>
<body>

<!-- ── Header ── -->
<header class="header">
  <div class="header-left">
    <div class="logo">K<span>PI</span></div>
    <div class="subtitle">Karissa Performance Intelligence</div>
  </div>
  <div class="header-right">Week Ending &nbsp;<strong>{week}</strong></div>
</header>

<!-- ── Main ── -->
<main class="main">

  <!-- Summary Cards -->
  <div class="section-title">Network Summary</div>
  <div class="cards-grid">
    {cards_html}
  </div>

  <!-- Leaderboard -->
  <div class="section-title">Location Leaderboard — This Week</div>
  <div class="leaderboard-section">
    <div class="leaderboard-header"></div>
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Location</th>
          <th>Total Sales</th>
          <th>PPH</th>
          <th>Product %</th>
          <th>Guests</th>
          <th>vs Last Week</th>
        </tr>
      </thead>
      <tbody>
        {lb_html}
      </tbody>
    </table>
  </div>

  <!-- Charts -->
  <div class="section-title">Trends</div>
  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-title">4-Week Network Sales Trend</div>
      <div class="chart-container">
        <canvas id="salesTrendChart"></canvas>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Product % by Location — This Week</div>
      <div class="chart-container">
        <canvas id="productPctChart"></canvas>
      </div>
    </div>
  </div>

</main>

<!-- ── Footer ── -->
<footer class="footer">
  <span>KPI Platform &mdash; Powered by AI</span>
  <span>Data refreshes every Monday 7am CT</span>
</footer>

<script>
  const BLUE_DARK   = '#1E3A5F';
  const BLUE_MID    = '#4A90D9';
  const BLUE_LIGHT  = 'rgba(74,144,217,0.12)';
  const GRAY_BORDER = '#E8ECF0';

  Chart.defaults.font.family = "'Inter', -apple-system, sans-serif";
  Chart.defaults.font.size   = 12;
  Chart.defaults.color       = '#7A8BA0';

  // ── Sales Trend Line Chart ──────────────────────────────────────────────────
  new Chart(document.getElementById('salesTrendChart'), {{
    type: 'line',
    data: {{
      labels: {tl},
      datasets: [{{
        label: 'Total Network Sales',
        data: {tv},
        borderColor:     BLUE_MID,
        backgroundColor: BLUE_LIGHT,
        borderWidth:     2.5,
        pointBackgroundColor: BLUE_MID,
        pointRadius:     5,
        pointHoverRadius: 7,
        fill: true,
        tension: 0.35,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: ctx => ` ${{ctx.parsed.y.toLocaleString('en-US', {{style:'currency',currency:'USD',maximumFractionDigits:0}})}}`
          }}
        }}
      }},
      scales: {{
        x: {{ grid: {{ color: GRAY_BORDER }}, ticks: {{ font: {{ size: 11 }} }} }},
        y: {{
          grid: {{ color: GRAY_BORDER }},
          ticks: {{
            font: {{ size: 11 }},
            callback: v => '$' + (v/1000).toFixed(0) + 'k'
          }}
        }}
      }}
    }}
  }});

  // ── Product % Horizontal Bar Chart ─────────────────────────────────────────
  const prodColors = {pb_values}.map(v =>
    v >= {net_avg_pct} ? BLUE_MID : 'rgba(74,144,217,0.45)'
  );

  new Chart(document.getElementById('productPctChart'), {{
    type: 'bar',
    data: {{
      labels: {pb_names},
      datasets: [{{
        label: 'Product %',
        data: {pb_values},
        backgroundColor: prodColors,
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{ label: ctx => ` ${{ctx.parsed.x.toFixed(1)}}%` }}
        }},
        annotation: undefined
      }},
      scales: {{
        x: {{
          grid: {{ color: GRAY_BORDER }},
          ticks: {{ callback: v => v + '%', font: {{ size: 11 }} }},
        }},
        y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 11 }} }} }}
      }}
    }},
    plugins: [{{
      id: 'avgLine',
      afterDraw(chart) {{
        const {{ctx: c, scales: {{x, y}}}} = chart;
        const xPos = x.getPixelForValue({net_avg_pct});
        c.save();
        c.beginPath();
        c.moveTo(xPos, y.top);
        c.lineTo(xPos, y.bottom);
        c.strokeStyle = '#DC2626';
        c.lineWidth   = 1.5;
        c.setLineDash([4, 3]);
        c.stroke();
        c.fillStyle = '#DC2626';
        c.font      = '600 10px Inter, sans-serif';
        c.fillText('Avg ' + {net_avg_pct} + '%', xPos + 4, y.top + 12);
        c.restore();
      }}
    }}]
  }});
</script>
</body>
</html>
"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║  KPI Dashboard Builder — Karissa's Salon Network ║")
    print("╚══════════════════════════════════════════════════╝")

    print("\n[1/4] Loading config & authenticating ...")
    config         = load_config()
    spreadsheet_id = config["sheet_id"]
    service        = build_sheets_service()
    print("      ✓ Authenticated")

    print("\n[2/4] Reading DATA tab from Google Sheet ...")
    rows = fetch_data_tab(service, spreadsheet_id)
    print(f"      ✓ {len(rows):,} data rows fetched")

    print("\n[3/4] Processing last 4 weeks ...")
    data = process_data(rows)
    print(f"      ✓ Current week  : {data['current_week']}")
    print(f"      ✓ Network sales : ${data['trend_values'][-1]:,.0f}")
    print(f"      ✓ Leaderboard   : {len(data['leaderboard'])} locations ranked")

    print("\n[4/4] Generating dashboard/index.html ...")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_dashboard(data), encoding="utf-8")
    print(f"      ✓ Saved → {OUTPUT_PATH}")

    print(f"\n✅  Done! Open this file in your browser:")
    print(f"   {OUTPUT_PATH}\n")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        print(f"\n❌  {exc}", file=sys.stderr); sys.exit(1)
    except EnvironmentError as exc:
        print(f"\n❌  {exc}", file=sys.stderr); sys.exit(1)
    except HttpError as exc:
        print(f"\n❌  Google API error: {exc}", file=sys.stderr); sys.exit(1)
