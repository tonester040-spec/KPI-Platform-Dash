# Karissa's Golden Rules — KPI Formula Bible

> **Canonical spec.** Any model working on this codebase MUST read this before computing or displaying ANY KPI. These rules come directly from Karissa via multiple rounds of Q&A on 2026-05-27. They OVERRIDE anything in the POS-generated reports that conflicts.
>
> **First principle:** Don't trust pre-computed stats printed on POS reports when they conflict with these rules — compute from primitives. The **only exception is PPG**, which we explicitly match to the Zenoti/SU printed salon summary because Karissa's team prints those and uses them as ground truth.

---

## Quick-Reference Table

| KPI | Formula | Denominator notes |
|---|---|---|
| `guest_count` (SU) | `Serviced Guests + Retail Only Guests` | The "TOTAL Guests" line. |
| `guest_count` (Zenoti) | `Total invoices with services or product` | From the **Invoice Summary** section. NOT the "Total guest count" in the Statistics box (unique guests). NOT "Total invoices with service" (services-only). |
| `total_sales` | `service_net + product_net` | Pre-tax. Never tax-inclusive. |
| `avg_ticket` | `total_sales / guest_count` | Karissa's "TA". |
| `ppg` (salon, primary) | `product_net / invoice_count_per_salon_dashboard` | Must match the PPG that Zenoti / SU prints on its salon summary. **Override to match if our recompute differs.** See "PPG Reconciliation" below. |
| `ppg` (salon, secondary) | `product_net / guest_count` | Karissa-formula version. Shown alongside primary for transparency, NOT in the main tracker column. |
| `ppg` (per stylist) | Recompute to match salon dashboard | Override individual stylist PPGs to match printed salon summary. Reason: team verifies against printed dashboards; mismatches make them distrust the system. |
| `pph` | `service_net / production_hours` | Production hours definition differs by platform — see "Production Hours" below. |
| `product_pct` | `product_net / total_sales` | Denominator is **TOTAL** sales (service + product). Not service-only. |
| `color_pct` | `color_net / service_net` | Denominator is **SERVICE** revenue. Revenue share, NOT guest penetration. (Different denominator than `product_pct`.) |
| `wax_pct` | `wax_count / guest_count` | Guest penetration rate. `wax_count` = sum of all `Wax` + `Waxing` category quantities. |
| `treatment_pct` | `treatment_count / guest_count` | Guest penetration rate. |
| `projection_eom` | `(total_sales / 7) * 24` | Weekly extrapolation over a flat 24-day working month. NOT calendar-aware. |

---

## Report Time Model & Cumulative-MTD Mechanics — how Elaina's weekly reports accumulate

> Verified directly with Elaina via a 13-question Q&A (the same source behind the Q1–Q13 table in `CLAUDE.md`; this is the canonical version). **These rules govern HOW every number on the report accumulates — they are as load-bearing as the formulas above.** Get the time model wrong and every per-week value is wrong.

### The window (Q1–Q3)

- **Cumulative month-to-date.** Every weekly report covers **the 1st of the current month through the most recent Sunday**, and **resets when a new month starts** (Q1).
- **Hard month-START boundary.** When a month starts mid-week, those first days are **Week 1 of the NEW month** — they do NOT roll into the prior month. *April 1 (Wed) → April Week 1 = Apr 1–5 (Wed–Sun, a 5-day partial)* (Q2).
- **Hard month-END boundary.** The **last week always ends on the month's last day**, even when that's a 1–3 day partial (Q3).
- **Consequence:** weeks are calendar-bounded by the month — Week 1 and the final week can be short (1–5 days), and a week NEVER straddles two months.

### Everything accumulates — primitives AND percentages (Q4, Q6)

- **All fields are MTD** — sales, guests, AND percentages all build up over the month, at **both salon grain (Q4) and per-stylist grain (Q6)**.
- **⚠️ CALCULATION RULE:** to recover a true single-week value, **difference the raw primitives** from the prior week's cumulative snapshot and then **RECOMPUTE the ratios** from the differenced primitives. **Never subtract a percentage from a percentage** — a cumulative % is not additive.

### Numbers usually grow, but CAN dip (Q5)

- Elaina (verbatim): *"If there is a refund it is removed but there is still growth because there is never that much refunded."*
- A cumulative number **can decrease** week-over-week when a refund posts, but net growth nearly always wins (refund amounts are small).
- **⚠️ CALCULATION RULE:** differencing must handle **negative week-deltas** gracefully — no divide-by-zero, no clamping that would hide a real refund.

### Corrections & re-sends (Q9, Q10)

- **One report per Monday, never re-sent** (Q10) — each Monday's snapshot is FINAL.
- Late corrections therefore flow into the **next** week's cumulative (it's MTD), not a re-issued prior report.
- Elaina (verbatim, Q9): *"Sometimes but not always — it will for the salon total but doesn't always for staff total."* A correction **may restate the salon total but NOT always the per-stylist totals**.
- **⚠️ CALCULATION RULE:** a **stylist-sum ≠ salon-total mismatch is EXPECTED** and must be a **warning, never a hard error** (this is why `STYLIST_SUM_MISMATCH` is downgraded). Guest-count is likewise NOT reconciled stylist-vs-salon.

### Closed / holiday weeks (Q11)

- A salon that closes for a week (zero invoices) appears as a **"zero" row — everything 0 — and the row is SHOWN, never skipped**.
- **⚠️ CALCULATION RULE:** every derived KPI is None-safe; a zero-guest / zero-sales week resolves each ratio to 0 (or blank) and the row stays present in the report.

### Platform parity (Q12)

- **Zenoti and Salon Ultimate reports work identically — both cumulative-MTD** — so one pipeline handles both. Elaina (verbatim): *"both work the same but we can pick whatever time frame we want."* The "any time frame" point matters for backfill: old periods can be re-pulled on demand.

### Historical / 2025 (Q13)

- 2025 reports are the **same MTD format** and **ARE pullable** — Elaina (verbatim): *"we can pull any old data, it just takes time. In my monthly breakdown I have year-over-year comparison."*
- **Two takeaways:** (1) the weekly backfill loader works on prior-year reports too (same format — see `BACKFILL_RUNBOOK.md`); (2) Karissa **already keeps a monthly YoY breakdown by hand** — a ready-made source for the dashboard's 2025 / YoY columns, alongside (or ahead of) re-pulling weekly history.

### What the dashboard / report should show (Q7, Q8)

- **Q7 (verbatim):** *"Both — weeks in a column, then the monthly adding up as the month goes on; otherwise just month totals."* → **weekly columns + a running monthly total.** This is exactly the `report_generator` layout: cumulative weekly tabs whose Totals row IS the running MTD.
- **Q8:** Coach-card "this week vs last week" → *"you guys pick what makes sense."* We use **true week-over-week** (this week's differenced value vs last week's), consistent with Q7's weekly-column view.

---

## 1. Guest Count

### Salon Ultimate
**Formula:** `Serviced Guests + Retail Only Guests`

This is the "TOTAL Guests" line in the SU Guest Count box. Verified against Apple Valley: $2,112 product net ÷ 281 total guests = $7.52 PPG, matches the report exactly.

### Zenoti
**Formula:** `Total invoices with services or product`

Read from the **INVOICE SUMMARY** section. This is the broadest count — includes everyone who paid for anything (service OR retail), not unique guests, not services-only.

**Do NOT use:**
- ❌ "Total guest count" from the STATISTICS box (this is *unique* guests — a different number, usually lower by 1-2)
- ❌ "Total invoices with service" (services-only, excludes product-only transactions)

Example from Andover: Statistics shows 94 unique guests, Invoice Summary shows 95 invoices, "Total invoices with services or product" shows 95. Use 95.

---

## 2. Sales

| Field | Formula |
|---|---|
| `service_net` | Direct from PDF (Service sales NET, pre-tax) |
| `product_net` | Direct from PDF (Product/Retail NET, pre-tax) |
| `total_sales` | `service_net + product_net` |

**NEVER** use "Sales (Inc. Tax)" from Zenoti or any tax-inclusive figure.

### Product Sales Inclusions / Exclusions

**Include EVERYTHING under Product Sales**, even internal supply/inventory items like "KBI Supply Custom", "FMSC 2026", etc.

Why: Karissa confirmed these count. If we exclude them, our PPG drifts from her tracker and from the Zenoti salon dashboard.

---

## 3. PPG (Product Per Guest) — Two Versions

PPG is the **one exception** to the "compute from first principles" rule. The salon dashboards print a PPG number. Karissa's team uses those printed numbers as the source of truth. If our recompute drifts from the printed number, the team questions the system.

### Salon level
- **PRIMARY** (the number that goes in the tracker, on the dashboard, in coach reports): `product_net / invoice_count_per_salon_dashboard` — the version that matches Zenoti's / SU's printed PPG on the salon summary.
- **SECONDARY** (shown alongside primary for transparency, NOT used as the headline): `product_net / guest_count` (where `guest_count` is per the formulas above).

### Per stylist
- **Override per-stylist PPG to match the salon dashboard's total PPG context.**
- Reason: when the team prints individual stylist reports, the per-stylist PPG should reconcile to the salon-level dashboard PPG. Karissa believes the discrepancy comes from refunds: Zenoti's salon dashboard PPG **does not subtract refunds**, so our refund-subtracted recompute is slightly lower.
- The system replicates Karissa's logic, not Zenoti's accuracy.

### Why PPG mismatches happen
- Refunds: Zenoti's dashboard PPG appears not to subtract refunds. Our recompute does (because we subtract refunds from `product_net`). When refunds exist that week, ours is lower.
- We don't fix this by changing the math; we override the displayed PPG to match what the team sees on the printed dashboard.

---

## 4. PPH (Service Sales per Production Hour)

**Formula:** `service_net / production_hours`

### Where production hours come from

**Salon Ultimate:** "Production Hours" field directly in the Statistics box at the top of the report. Format: `287h 18m` → store as decimal (e.g., `287.30`). Calculations always use decimal; display in decimal too (Karissa's preference confirmed).

**Zenoti:** No `production_hours` field in the top Statistics section. Read from the **Employee Performance** table at the bottom — the `Production Hours` column in the **Total** row.

**Why this matters for Zenoti:** Some locations have receptionists and non-service staff clocked in. At those locations (Blaine, Crystal, Hudson confirmed), `Actual Hours` and `Production Hours` differ. PPH must use `Production Hours` — verified by back-calculating against Zenoti's own printed PPH figure.

---

## 5. Wax Categories

### Combination rule
**Sum the qty and net of ALL service categories with "wax" in the header name. No exclusions.**

This handles three patterns observed across the network:
| Pattern | Locations | What to do |
|---|---|---|
| Single `Wax` header | 5 Zenoti locations + all SU locations | Use that row as-is |
| Both `Wax` AND `Waxing` headers | Elk River FS, Crystal FS, Hudson | Sum both rows |
| Only `Waxing` header (no `Wax`) | Roseville | Use the `Waxing` row as the wax data |

The parser rule: any service category header containing the substring "wax" (case-insensitive) contributes to `wax_count` and `wax_net`. Catches `Wax`, `Waxing`, and any future variants.

### Wax %
**Formula:** `wax_count / guest_count`

Guest penetration rate (share of guests who got a wax). Denominator is `guest_count` per the rules in §1.

### Treatment %
**Formula:** `treatment_count / guest_count`

Same penetration-rate pattern as Wax %.

---

## 6. Color %

**Formula:** `color_net / service_net`

**This intentionally uses a different denominator than `product_pct`.** Color % is a **revenue share** metric — "what share of service revenue came from color services." Not a guest penetration rate.

Worked example (Blaine, Week 1): `color_net = $3,964.50`, `service_net = $11,478.25`, so `color_pct = 3964.50 / 11478.25 = 34.54%`. Using `total_sales = $12,485.55` as the denominator would give 31.75% — wrong per Karissa's tracker formula.

**Do NOT use:**
- ❌ `color_net / total_sales` (wrong denominator)
- ❌ `color_count / guest_count` (that's a penetration rate; Karissa wants revenue share for Color)
- ❌ The PDF's printed "% Sales" column for Color (uses yet another denominator)

---

## 7. Edge Cases

### Unclosed-day report
**Behavior:** Alert + attempt rerun. If the report can't be regenerated, leave that location blank for the week. **Never carry forward prior-week numbers.**

Trigger: SU reports notice "MM/DD/YYYY was an unclosed day" at the top. Zenoti has analogous indicators.

### Product header vs. detail mismatch (Salon Ultimate)
Some SU reports show inconsistent product totals between the header Sales section and the Top Product Lines detail table.

Example: Lakeville (2026-05-24) — header `Total Retail` = $534.50, Top Product Lines detail sum = $623.25, gap of $88.75.

**Rule: FLAG IT and notify Karissa.** Don't silently pick a winner. Karissa wants to know each time this happens so she can investigate with SU. Parser flags `FLAG_PRODUCT_TOTAL_MISMATCH` and emits an alert.

Header `Total Retail` stays the canonical `product_net` for downstream KPI calc (matches Karissa's existing practice). The flag is informational, not blocking.

### Roseville
Roseville's data behaves the same as other Zenoti locations **except**:
- Different service NAMES on the menu (FS Cut, FS Clipper Cut instead of Adult Custom Haircut, Adult Clipper Haircut)
- Only `Waxing` header (no `Wax` header) — handled by the wax combination rule in §5

**Categories map identically** to other locations: Wax/Waxing, Treatment, Color, Haircut. No special-casing needed beyond the wax-name handling.

### Refunds
Zenoti's salon dashboard appears to NOT subtract refunds when computing its printed PPG. This is the suspected source of the PPG mismatch (see §3). We don't "fix" Zenoti's number — we match it so the team's print-and-verify workflow stays trustworthy.

---

## 8. Format Conventions

| Field | Storage | Display |
|---|---|---|
| Production Hours | Decimal (e.g., `287.30`) | Decimal (Karissa's preference) — NOT `287h 18m` |
| Percentages (general) | Decimal fraction `0-1` in DATA/CURRENT tabs | Scaled to percent at dashboard emission boundary |
| Currency | Float dollars | `$X,XXX.XX` formatting at display |
| Dates | ISO `YYYY-MM-DD` | Display per UI context |
| Guest counts | Integer | Integer with thousands separator |

---

## 9. Platform-Specific Reads — Cheat Sheet

### Zenoti
| Field | Where to read |
|---|---|
| `guest_count` | Invoice Summary → "Total invoices with services or product" |
| `service_net` | Statistics → service sales (NET) |
| `product_net` | Statistics → product sales (NET) — include ALL line items, no exclusions |
| `production_hours` | Employee Performance table → Production Hours column → Total row |
| `rebook_pct` | Center Performance → "Rebooked X (Y.YY)" → use Y.YY |
| `req_services_count` per stylist | Employee Performance Details → REQ SERVICES QTY per stylist row |
| Color/Wax/Treatment qty | Service Categories table → Qty column (sum Wax + Waxing) |

### Salon Ultimate
| Field | Where to read |
|---|---|
| `guest_count` | Guest Count box → TOTAL Guests (Serviced + Retail Only) |
| `service_net` | Sales box → Total Service NET |
| `product_net` | Sales box → Total Retail NET. If header total disagrees with Top Product Lines detail, FLAG and use header value. |
| `production_hours` | Statistics box → Production Hours (convert `Xh Ym` → decimal) |
| `rebook_pct` | Statistics box → "Rebook %" |
| `req_pct` per stylist | Employee Summary → "Req %" column (already a percentage) |
| `avg_service_time_min` per stylist | Employee Summary → "Avg Service Time (min)" column |
| Color/Wax/Treatment | Service Categories table → Qty + Sales columns |

---

## 10. What Counts as "First Principles"

The bedrock primitives (read directly from POS, never derived):
- `service_net`
- `product_net`
- `guest_count` (per the platform-specific rule)
- `production_hours`
- Color/Wax/Treatment quantities and sales values
- `req_services_count` (Zenoti) or `req_pct` (SU) per stylist
- `rebook_pct` (salon-level)

EVERYTHING ELSE IS COMPUTED. We do not trust the POS report's pre-calculated:
- ❌ `product_pct` from PDF
- ❌ `avg_ticket` from PDF
- ❌ `color_pct` from PDF
- ❌ `wax_pct` / `treatment_pct` from PDF
- ❌ Per-stylist PPG / Avg Ticket from PDF
- ❌ Per-stylist "REQ %" bracketed value on Zenoti (it's share-of-role-group, not what we want)

**The PPG exception (§3) is the only place we override our computation to match the POS printout.**

---

## 11. Source Q&A (audit trail)

These rules came from two rounds of Karissa Q&A on 2026-05-27. Each rule above traces back to a specific answer:

### Round 1
- **Guest count Zenoti** = Invoice total from INVOICE SUMMARY (not Statistics box unique-guest count)
- **Guest count SU** = Serviced + Retail Only Guests (TOTAL Guests)
- **Wax combination** = include everything with "wax" in header name, no exclusions
- **Wax % / Treatment %** = qty / guest count
- **Color %** = Color Net / Service Net (revenue share)
- **Zenoti production hours** = Employee Performance table → Production Hours column → totals row
- **Unclosed day** = alert + attempt rerun; if can't rerun, leave blank; never carry forward
- **Roseville** = different service NAMES only; categories map the same

### Round 2 (5 remaining gaps)
- **Zenoti guest count exact line** = "Total invoices with services or product" (broadest count)
- **Internal supply items** (KBI Supply, etc.) = INCLUDE; everything under Product Sales counts
- **PPG primary/secondary** = Primary = matches salon dashboard. Secondary = Product Net / Guest Count
- **Product header/detail mismatch (SU)** = FLAG it and notify
- **SU production hours format** = decimal (`287.30`)
- **PPG mismatch reconciliation** = "I will use the PPG that matches the salon dashboard and change it on the report of the individual stylists. I don't think they calculate refunds. The team prints the salon dashboard so I need it to match so they don't question the system. Even when it is off."

---

## 12. For Future Model Sessions

If you're a model picking this up:

1. **Read this FIRST**, before CLAUDE.md's KPI section. CLAUDE.md cross-references this doc.
2. **If a KPI behavior surprises you**, check this doc before "fixing" it. The PPG-matches-printed-dashboard rule will surprise you if you don't read §3.
3. **If you're adding a new KPI**, get Karissa's formula in writing, then add a section to this doc.
4. **If you find ambiguity**, don't guess. Flag for Karissa and add the resolution back here.
5. **If a POS report changes shape** (new column, renamed section), the formulas here still hold — only the read locations in §9 change.

This doc is the contract. POS reports are the implementation.

— captured 2026-05-27 from Karissa
