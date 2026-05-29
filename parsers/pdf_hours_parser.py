"""
KPI Salon Summary parser — per-stylist PRODUCTION HOURS (Karissa Q6 source).
Handles HTML-xls and real PDF. PRODUCTION HOURS = 4th numeric after a stylist name.

Stylist rows in Zenoti's Employee Performance table always follow a role rollup row:
  STYLIST (rollup) -> then the individual stylists under it
  MANAGER (rollup) -> Danielle Carlson
  SHIFT LEADER (rollup) -> Jenna Renstrom
We identify stylist rows as: starts with a capitalized First name, has >=4 numerics after,
and is NOT a known role/rollup label. We strip any leading '(xx.xx)' REQ% that bleeds in.

VALIDATED: Forest Lake 4/1-4/5 -> 115.50 (Phipps 29.10); 4/1-4/12 -> 267.37.
"""
import re

ROLE_LABELS={"manager","shift leader","stylist","total","trainer","front desk","assistant","grand total"}
# Artifact rows that must NEVER count as a stylist (Tableau history surfaced these — bake the guard here too).
ARTIFACT_NAMES={"house sale","unknown","all"}
NUM=re.compile(r"^-?[\d,]+\.?\d*$")
PCT_PREFIX=re.compile(r"^\(\d+\.?\d*\)$")  # like (34.23)

def _extract_text(path):
    if path.lower().endswith(".pdf"):
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    from bs4 import BeautifulSoup
    return BeautifulSoup(open(path,encoding="utf-8",errors="ignore").read(),"html.parser").get_text("\n")

def parse_production_hours(path=None, known_last_names=None, text=None):
    if text is None:
        text=_extract_text(path)
    up=text.upper()
    i=up.find("EMPLOYEE PERFORMANCE")
    if i<0: return {}
    j=up.find("HOURLY WORK", i)
    region=text[i:(j if j>0 else i+4000)]
    tokens=[t.strip() for t in re.split(r"[|\n\t ]+", region) if t.strip()]
    # drop leading (xx.xx) percent tokens entirely — they belong to prior row
    tokens=[t for t in tokens if not PCT_PREFIX.match(t)]
    out={}
    k=0
    while k < len(tokens)-4:
        if NUM.match(tokens[k]): k+=1; continue
        name_words=[]; m=k
        while m<len(tokens) and not NUM.match(tokens[m]) and len(name_words)<3:
            name_words.append(tokens[m]); m+=1
        name=" ".join(name_words)
        nums=[]; p=m
        while p<len(tokens) and NUM.match(tokens[p]) and len(nums)<6:
            nums.append(tokens[p]); p+=1
        is_role = name.lower() in ROLE_LABELS or any(w.upper()==w and len(w)>3 for w in name_words)
        nl = name.lower()
        is_artifact = nl in ARTIFACT_NAMES or "unknown" in nl or "house sale" in nl
        looks_person = len(name_words)>=2 and name[0].isupper()
        if known_last_names is not None:
            looks_person = looks_person and any(ln in name for ln in known_last_names)
        if looks_person and not is_role and not is_artifact and len(nums)>=4:
            out[name]=float(nums[3].replace(",",""))
            k=p
        else:
            k+=1
    return out

# ─────────────────────────────────────────────────────────────────────────────
# Salon Summary SALES / SERVICE DETAILS / INVOICE SUMMARY parsers
# (salon-grain LOCATIONS_DATA source — replaces the Tableau revenue+guest CSVs).
#
# Regexes put \s+ between tokens so they match BOTH the PDF text (space-
# separated, two columns interleaved) AND the HTML-xls text (BeautifulSoup
# get_text inserts a newline between every cell). One Salon Summary file =
# one window; the file already carries the date range, so NO Python windowing.
# ─────────────────────────────────────────────────────────────────────────────
_MONEY = r"[\d,]+\.\d+"

# Service-category buckets we expose + accepted header-name aliases (handles Wax/Waxing
# and Colour/Treatments variants). Non-bucket categories are enumerated ONLY so the caller
# can reconcile the full category breakdown against service_net (catch silent drops).
_BUCKET_ALIASES = {
    "color": ("color", "colour"),
    "wax": ("wax", "waxing"),
    "treatment": ("treatment", "treatments"),
}
_NONBUCKET_CATEGORIES = ("haircut", "styling", "style", "perm", "texture")


def _parse_sales_from_text(text):
    """SALES block: service_net + product_net (the NET column, pre-tax)."""
    region = text[:text.find("SERVICE DETAILS")] if "SERVICE DETAILS" in text else text

    def _net(label):  # '<label> <count> <gross> <net> (pct)' -> net
        m = re.search(rf"{label}\s+[\d,]+\s+{_MONEY}\s+({_MONEY})", region)
        return float(m.group(1).replace(",", "")) if m else None

    return {"service_net": _net("Service sales"), "product_net": _net("Product sales")}


def _parse_invoice_summary_from_text(text):
    """guest_count = Zenoti 'Total invoices with services or product' (NOT the
    'with service' 607 line, NOT 'Total guest count' which is unique guests)."""
    m = re.search(r"Total invoices with services or product\s+([\d,]+)", text)
    return {"guest_count": int(m.group(1).replace(",", "")) if m else None}


def _parse_service_details_from_text(text):
    """SERVICE DETAILS category totals → schema buckets (color / wax[=Wax+Waxing] /
    treatment) via an alias map, PLUS full enumeration of all known categories so the
    caller can reconcile their sum against service_net. Matches category HEADER rows
    only ('Color 108 (..)' — not indented sub-services). Header names are clean, so
    line-anchored name matching is reliable across both HTML-xls and PDF."""
    si = text.find("SERVICE DETAILS")
    pi = text.find("PRODUCT DETAILS", si) if si >= 0 else -1
    region = text[si:pi] if (si >= 0 and pi > si) else (text[si:] if si >= 0 else text)

    def _cat(name):  # '<Category> <qty> (pct) <avgtime> <net> (pct) <disc>'
        m = re.search(rf"(?mi)^\s*{re.escape(name)}\s+(\d+)\s+\([\d.]+\)\s+[\d.]+\s+({_MONEY})", region)
        return (int(m.group(1)), float(m.group(2).replace(",", ""))) if m else (None, None)

    found = {}                                  # category label -> (qty, net), for reconciliation
    buckets = {"color": [], "wax": [], "treatment": []}
    for b, aliases in _BUCKET_ALIASES.items():
        for a in aliases:
            q, n = _cat(a)
            if n is not None:
                found[a] = (q, n); buckets[b].append((q, n))
    for c in _NONBUCKET_CATEGORIES:             # real categories that are NOT schema buckets
        q, n = _cat(c)
        if n is not None:
            found[c] = (q, n)

    def _agg(pairs):
        if not pairs:
            return (None, None)
        return (sum(q for q, _ in pairs), round(sum(n for _, n in pairs), 2))
    color_c, color_n = _agg(buckets["color"])
    wax_c, wax_n = _agg(buckets["wax"])
    treat_c, treat_n = _agg(buckets["treatment"])

    return {
        "color_count": color_c, "color_net": color_n,
        "wax_count": wax_c, "wax_net": wax_n,
        "treatment_count": treat_c, "treatment_net": treat_n,
        "categories_net_sum": round(sum(n for _, n in found.values()), 2) if found else None,
        "categories_found": sorted(found.keys()),
    }


def parse_sales(path):
    return _parse_sales_from_text(_extract_text(path))


def parse_invoice_summary(path):
    return _parse_invoice_summary_from_text(_extract_text(path))


def parse_service_details(path):
    return _parse_service_details_from_text(_extract_text(path))


def parse_salon_summary(path):
    """Full salon-grain extract from ONE Salon Summary file (PDF or HTML-xls).
    Single text extraction; everything locations_grouper needs for one window."""
    text = _extract_text(path)
    sales = _parse_sales_from_text(text)
    inv = _parse_invoice_summary_from_text(text)
    det = _parse_service_details_from_text(text)
    hrs = parse_production_hours(text=text)
    prod_hours = round(sum(hrs.values()), 2) if hrs else None
    s, p = sales["service_net"], sales["product_net"]
    total = round(s + p, 2) if (s is not None and p is not None) else None
    um = re.search(r"Total guest count\s+([\d,]+)", text)  # unique guests (NOT guest_count; not a schema col)
    unique_guests = int(um.group(1).replace(",", "")) if um else None
    # Service-mix reconciliation: the enumerated service categories must sum to service_net.
    # A gap means a category header we don't recognize (potential silent drop) — flag it loud.
    cats_sum = det.get("categories_net_sum")
    reconciled = (cats_sum is not None and s is not None and abs(cats_sum - s) <= 0.01)
    flags = []
    if cats_sum is not None and s is not None and not reconciled:
        flags.append(f"service_mix_unreconciled: categories_sum={cats_sum} service_net={s} "
                     f"gap={round(s - cats_sum, 2)} found={det.get('categories_found')}")
    return {
        "service_net": s, "product_net": p, "total_sales_net": total,
        "color_net": det["color_net"], "color_count": det["color_count"],
        "wax_net": det["wax_net"], "wax_count": det["wax_count"],
        "treatment_net": det["treatment_net"], "treatment_count": det["treatment_count"],
        "guest_count": inv["guest_count"], "unique_guests": unique_guests,
        "productive_hours": prod_hours,
        "service_mix_reconciled": reconciled,
        "service_mix_gap": (round(s - cats_sum, 2) if (cats_sum is not None and s is not None) else None),
        "categories_found": det.get("categories_found"),
        "flags": flags,
        "hours_by_stylist": hrs,
    }


if __name__=="__main__":
    import sys, json as _json
    path=sys.argv[1]
    res=parse_production_hours(path)
    tot=0
    print("PRODUCTION HOURS per stylist:")
    for n,h in res.items(): print(f"  {n:<22}{h:>8.2f}"); tot+=h
    print(f"  {'TOTAL':<22}{tot:>8.2f}")
    print("\nSALON SUMMARY:")
    ss=parse_salon_summary(path); ss.pop("hours_by_stylist", None)
    print(_json.dumps(ss, indent=2))
