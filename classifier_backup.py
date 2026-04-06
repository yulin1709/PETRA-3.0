# classifier.py
# -*- coding: utf-8 -*-
"""
PETRA — Classifier
Auto-selects latest incident raw data file, classifies each
ticket into Refined Summary + Reason, saves to PETRA/SLA folder.
"""

import re
import os
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

# ============================================================
# PATH RESOLUTION
# ============================================================
def _candidate_bases() -> list[Path]:
    bases: list[Path] = []
    user = Path(os.environ.get("USERPROFILE", ""))
    p_base = user / "PETRONAS"
    if p_base.exists():
        bases.append(p_base)
    for envvar in ("OneDriveCommercial", "OneDrive"):
        v = os.environ.get(envvar)
        if v and os.path.isdir(v):
            bases.append(Path(v))
    od_guess = user / "OneDrive - PETRONAS"
    if od_guess.exists():
        bases.append(od_guess)
    seen, uniq = set(), []
    for b in bases:
        s = str(b)
        if s not in seen:
            seen.add(s); uniq.append(b)
    return uniq

BASES = _candidate_bases()

def _first_existing(*rel_parts: str) -> Path | None:
    for b in BASES:
        p = b.joinpath(*rel_parts)
        if p.exists():
            return p
    return None

# ---- INCIDENT_ROOT ----
_inc_env = os.environ.get("INCIDENT_ROOT")
if _inc_env:
    INCIDENT_ROOT = Path(_inc_env)
else:
    INCIDENT_ROOT = _first_existing("TRMS Internal - myGenie+ Extract")
    if INCIDENT_ROOT is None:
        INCIDENT_ROOT = Path(os.environ["USERPROFILE"]) / r"PETRONAS\TRMS Internal - myGenie+ Extract"

# ---- OUTPUT ----
OUT_DIR = Path(os.environ["USERPROFILE"]) / "OneDrive - PETRONAS" / "Desktop" / "PETRA OUTPUT"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUT_DIR / "Classified_Incidents.xlsx"

print("Resolved paths:")
print(f"  INCIDENT_ROOT : {INCIDENT_ROOT}")
print(f"  OUTPUT_FILE   : {OUTPUT_FILE}")

if not INCIDENT_ROOT.exists():
    raise FileNotFoundError(f"INCIDENT_ROOT not found: {INCIDENT_ROOT}")

# ============================================================
# AUTO-SELECT LATEST FILE
# ============================================================
def find_latest_incident_file(folder: Path) -> Path:
    pattern = re.compile(
        r"^Incident Raw Data - (\d{1,2}) (\w+) (\d{4})\.xlsx$",
        re.IGNORECASE
    )
    candidates = []
    for f in folder.iterdir():
        if not f.is_file():
            continue
        m = pattern.match(f.name)
        if m:
            try:
                dt = pd.to_datetime(f"{m.group(1)} {m.group(2)} {m.group(3)}", dayfirst=True)
                candidates.append((dt, f))
            except Exception:
                continue
    if not candidates:
        raise FileNotFoundError(
            f"No 'Incident Raw Data - <dd> <Mon> <yyyy>.xlsx' found in:\n  {folder}"
        )
    candidates.sort(key=lambda x: x[0], reverse=True)
    latest_dt, latest_file = candidates[0]
    print(f"  Selected: {latest_file.name}  (date: {latest_dt.date()})")
    return latest_file

INPUT_FILE = find_latest_incident_file(INCIDENT_ROOT)

# ============================================================
# COLUMN NAMES
# ============================================================
COL_DETAILED_DESC_1 = "Detailed Decription"
COL_DETAILED_DESC_2 = "Detailed Description"
COL_PCT3            = "Product Categorization Tier 3"
COL_DESC_EXTRACT    = "Desc Extract"
ID_COLS_PREF        = ["Service Request ID", "Incident ID"]
DATE_COLS_PREF      = [
    "Last Modified Date", "Actual Resolution Date", "Closed Date",
    "Last Resolved Date", "Reported Date", "Actual Reported Date", "Re-Opened Date"
]

# ============================================================
# UTILITIES
# ============================================================
def norm_whitespace(x: str) -> str:
    x = x.replace("\u3000", " ")
    x = x.replace("\r\n", "\n").replace("\r", "\n")
    x = re.sub(r"[ \t]+", " ", x)
    x = re.sub(r"\n[ \t]+", "\n", x)
    return x.strip()

def to_datetime_smart(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    parsed   = pd.to_datetime(s, errors="coerce")
    s_num    = pd.to_numeric(s, errors="coerce")
    serial_mask = s_num.between(1, 80000)
    if serial_mask.any():
        try:
            parsed_serial = pd.to_datetime(s_num, errors="coerce", unit="D", origin="1899-12-30")
        except Exception:
            parsed_serial = pd.Series(pd.NaT, index=s.index)
    else:
        parsed_serial = pd.Series(pd.NaT, index=s.index)
    return parsed if parsed.notna().sum() >= parsed_serial.notna().sum() else parsed_serial

# ============================================================
# DESC EXTRACTOR
# ============================================================
DESC_START_PATTERNS = [
    r"(?is)description\s*[:：-]\s*(.*?)\bplease\s*provide\s*[:：-]",
    r"(?is)description\s*[:：-]\s*(.*)$",
    r"(?is)\bdesc\s*[:：-]\s*(.*?)\bplease\s*provide\s*[:：-]",
    r"(?is)\bdesc\s*[:：-]\s*(.*)$",
    r"(?is)\bissue\s*[:：-]\s*(.*?)\bplease\s*provide\s*[:：-]",
    r"(?is)\bissue\s*[:：-]\s*(.*)$",
]

def extract_desc_only(cell: str) -> str:
    if not isinstance(cell, str) or not cell.strip():
        return ""
    cell = norm_whitespace(cell)
    for patt in DESC_START_PATTERNS:
        m = re.search(patt, cell, flags=re.IGNORECASE | re.DOTALL)
        if m:
            snippet = norm_whitespace(m.group(1))
            if snippet:
                return snippet
    lines = cell.split("\n")
    desc_started = False
    chunks: List[str] = []
    for ln in lines:
        ln_clean = ln.strip()
        if re.match(r"(?i)^description\s*[:：-]?\s*$", ln_clean):
            desc_started = True
            continue
        if not desc_started:
            m2 = re.match(r"(?is)^description\s*[:：-]\s*(.+)$", ln_clean, flags=re.IGNORECASE)
            if m2:
                desc_started = True
                chunks.append(norm_whitespace(m2.group(1)))
            continue
        if re.match(r"(?i)^please\s*provide\s*[:：-]?\s*$", ln_clean) or \
           re.search(r"(?i)^please\s*provide\s*[:：-]", ln_clean):
            break
        chunks.append(ln_clean)
    snippet2 = norm_whitespace(" ".join([c for c in chunks if c]))
    return snippet2 if snippet2 else norm_whitespace(cell)

# ============================================================
# CLASSIFICATION VOCABULARY
# ============================================================
VERBS_AMEND = r"(amend|fix|correct|update|change|edit|adjust|revise|modify|add|remove|delete)"
VERBS_PUSH  = r"(push|trigger|retrigger|re-push|re push|send to)"
VERBS_POST  = r"(post|posting|posted)"
NEGATIONS   = r"(not|no|n't|unable to|cannot|can't|unsuccessful|failed|blocked)"

AMOUNT_TERMS = r"(amount|myr|usd|local\s*currency|lc\s*[0-9]+|value|total)"
PRICE_TERMS  = (
    r"(price|unit\s*price|pricing|reset|published\s*price|wma|jkm|bpp|"
    r"strike\s*price|mtm|p&l|pnl|pnl_cy|pnl_ly|"
    r"eod|settlement\s*price|forward\s*curve|ice|nymex|henry\s*hub|ttf|nbp|jcc)"
)
FX_TERMS = (
    r"(exchange\s*rate|fx\s*rates?|fx\s*rate|fx|"
    r"currency\s*conversion|eur\s*to\s*usd|usd\s*to\s*myr|myr\s*conversion|"
    r"fx\s*rates?\s*undefined|not\s*recognis(e|z)ed?\s*fx)"
)
DATE_TERMS = (
    r"(posting\s*date|invoice\s*date|accounting\s*date|period|backdate|date|"
    r"bl\s*date|movement\s*date|supplier(s'?)*\s*invoice\s*date|period\s*1)"
)
TAX_TERMS    = r"(tax|vat|gst|withholding|sst)"
DOC_TERMS    = r"(invoice|inv\.?|document|doc(ument)?\s*(no|number)?)"
QTY_TERMS    = r"(qty|quantity|volume|bbls?|barrels?|mt|tons?|mmbtu|units?|m3|kt)"
BANK_TERMS   = r"(bank\s*account|iban|swift|beneficiary\s*account|acct\s*no|account\s*number|account\s*no)"
ACC_TERMS    = r"\b(gl|account|coa|gl\s*account)\b"

MISSING_PHRASES = (
    r"(missing|not\s*(appear(ing)?|shown|sighted|populated|reflect(ed)?|flow(ing)?|found|available|visible)|"
    r"no\s*(data|vendor\s*code)\s*(appear|shown)|did\s*not\s*populate|does\s*not\s*appear|did\s*not\s*appear|"
    r"not\s*auto.?populate(d)?|not\s*pick(ed)?\s*up|not\s*include(d)?)"
)
RUN_CORR_PHRASES  = r"(run\s*corrective|corrective\s*run|please\s*run\s*corrective|fsa\s*0)"
JE_NE_SAP         = r"(je|new\s*entries|ne|sap|journal\s*entries?)(\s*tab)?"
SYSTEM_ERROR_PHRASES = (
    r"(error|failed|fail(ure)?|blocked|unsuccessful|cannot|unable to|can't|pop.?up|pops?\s*out|"
    r"exception|unresponsive|not\s*responding|access\s*denied|security\s*enforced|"
    r"nomination\s*(save\s*)?failed|save\s*failed|actuali[sz]ation\s*error|system\s*issue|"
    r"bug|glitch|citrix|login|log\s*in|access\s*issue)"
)

CATS = [
    "Discrepancies - Customer Code", "Discrepancies - Company Code", "Discrepancies - Legal Entity",
    "Discrepancies - Account", "Discrepancies - Broker broker", "Discrepancies - Strategy",
    "Discrepancies - Date", "Discrepancies - Amount", "Discrepancies - Tax", "Discrepancies - Doc",
    "Discrepancies - Address", "Discrepancies - exchange rate", "Discrepancies - vessel",
    "Discrepancies - Bank Account Issue", "Discrepancies - Details", "Discrepancies - Counterparty",
    "Discrepancies - Freight", "Discrepancies - Bunker", "run corrective", "Missing Data",
    "Duplicate Invoice/Deal Number", "SAP Posting Failures", "Others", "Question Help Support",
    "Not Update Data", "Push to SAP", "Wrong Price", "Wrong Quantity",
    "System Error / Functional Issue", "Report Issue", "Access Issue",
    "PLSB Bucket Amendment", "Matching Issue",
]

PATTERNS: Dict[str, List[Tuple[str, int, str]]] = {
    "run corrective": [
        (r"\b" + RUN_CORR_PHRASES + r"\b", 12, "run corrective keyword"),
        (r"\bmissing\s*(line|gl|item)\b", 8, "missing line/gl/item"),
        (r"\bfsa\s*\d{3}\b", 10, "FSA error code"),
        (r"(no\s*authorization|period\s*0\d{2}\s*\d{4}|not\s*authorized.*period)", 9, "period/authorization error"),
    ],
    "Push to SAP": [
        (r"\b" + VERBS_PUSH + r"\b.*\b" + JE_NE_SAP + r"\b", 12, "push to NE/JE/SAP"),
        (r"\bhelp\s*to\s*push\b", 10, "help to push"),
        (r"\bpush.*\btab\b", 8, "push tab"),
        (r"\bpush\s*doc(ument)?\s*(no|number)?\s*\d+", 11, "push doc number"),
        (r"\bpush.*\bne\b|\bne.*\bpush\b|\bpush.*\bje\b|\bje.*\bpush\b", 11, "push NE/JE"),
        (r"\binto\s*(ne|je|new\s*entries|journal\s*entries)\s*tab\b", 11, "push into NE/JE tab"),
        (r"\bpush\s*doc\b", 11, "push doc"),
        (r"\bpush\s*to\s*drafted\b", 11, "push to drafted"),
        (r"\bpush\s*(into|to)\s*(ne|je|new\s*entries|journal\s*entries)\b", 11, "push into NE/JE explicit"),
    ],
    "SAP Posting Failures": [
        (r"\b" + VERBS_POST + r"\b.*\b(fail(ed)?|unsuccessful|error|blocked)\b", 11, "posting failed/blocked"),
        (r"\bblocked\s*for\s*posting\b", 12, "blocked for posting"),
        (r"\bje\s*failed\b|\bposting\s*failed\b", 11, "JE/posting failed"),
        (r"\b(sap\s*doc|posting\s*number)\b.*\b(not\s*appear|hasn.?t\s*appear|did\s*not\s*appear)\b", 11, "SAP posting not appeared"),
        (r"\bfailed\s*(to\s*generate|entries)\b.*\bflatfile\b", 9, "failed to generate flatfile"),
        (r"\b(rw\s*\d{3}|no\s*item\s*information\s*transferred)\b", 10, "SAP transfer error code"),
        (r"\bblocked\s*account\s*for\s*posting\b|\baccount.*blocked\b", 10, "account blocked posting"),
        (r"\b(push.*sap|sap.*push)\b.*\b(doc|document|invoice)\b.*\b(not|fail|unsuccessful)\b", 10, "push to SAP failed"),
        (r"\b(unable\s*to\s*push.*sap|sap.*unable)\b", 10, "unable to push to SAP"),
        (r"\b(post(ed|ing)?)\b.*\b(block(ed|er)?)\b", 10, "posting blocked variant"),
        (r"\b(sap\s*posting|sap\s*doc(ument)?)\b.*\b(not\s*(appear|generated)|missing)\b", 10, "SAP doc not appearing/missing"),
    ],
    "Duplicate Invoice/Deal Number": [
        (r"\bduplicate\b.*\b(invoice|deal|doc)\b", 12, "duplicate invoice/deal"),
        (r"\b(invoice|deal)\b.*\bduplicate\b", 11, "invoice/deal duplicate"),
        (r"\bremove\s*duplicate\b", 10, "remove duplicate"),
        (r"\bexecuted\s*\d+\s*but\s*(in\s*endur|system)\s*\d+\b", 10, "executed vs system mismatch"),
        (r"\bduplicate\b.*\b(doc(ument)?|ifp)\b", 11, "duplicate document/IFP"),
    ],
    "Not Update Data": [
        (MISSING_PHRASES + r".*\b(ne|new\s*entries|je|journal)\b", 10, "not appearing in NE/JE"),
        (r"(selling\s*price|price)\s*(not\s*appear(ed)?|not\s*updated?|not\s*reflect(ed)?)", 9, "price not appeared/updated"),
        (r"\bnot\s*updated?\b", 8, "not updated"),
        (r"\bnot\s*appear(ing)?\b", 8, "not appearing"),
        (r"\bnot\s*sighted\b|\bnot\s*sighted\s*in\b", 8, "not sighted"),
        (r"\b(price|pricing)\s*not\s*(reflect(ed)?|shown|updated?|appear(ing)?)\b", 9, "price not reflected/shown"),
        (r"\b(selling\s*price|physical\s*pricing)\s*not\s*appear(ed)?\b", 10, "selling price not appeared"),
        (r"\b(flatfile|flat\s*file)\b.*\b(selling\s*price|price)\s*not\s*appear(ed)?\b", 10, "flatfile selling price issue"),
        (r"\b(issue|incorrect)\s*soa\s*type\b", 8, "incorrect SOA type"),
        (r"\b(price|pricing)\b.*\b(still\s*)?(unknown|not\s*known|supposed\s*to\s*be\s*known)\b", 11, "pricing still unknown"),
        (r"\b(deal|item)\b.*\b(not\s*appear|not\s*showing|does\s*not\s*appear)\b.*\b(physical\s*invoic|payable|desktop)\b", 9, "deal not appearing in desktop"),
        (r"\b(deal|cargo|item)\s*(not\s*appear|did\s*not\s*appear|does\s*not\s*appear)\b", 8, "deal/cargo not appearing"),
        (r"\bdoes\s*not\s*reflect\b|\bnot\s*reflect(ed|ing)?\b", 8, "not reflecting"),
        (r"\bstuck\s*in\s*(sent\s*to\s*lhdn|status)\b", 9, "stuck in LHDN/status"),
        (r"\b(price|pricing)\s*still\s*(remain|unchanged|not\s*updated?)\b", 9, "price not updated/remain"),
        (r"\bnot\s*flow(ing)?\s*(through|to|in)\b", 8, "not flowing through"),
        (r"\bnot\s*appear(ing)?\s*in\s*(receivable|payable)\s*standard\b", 11, "not appearing in receivable/payable standard"),
        (r"\b(delivery|deal)\s*id\b.*\b(not\s*appear|did\s*not\s*appear|does\s*not\s*appear)\b", 10, "delivery/deal id not appearing"),
        (r"\b(do(?:es)?\s*not\s*appear|missing)\b.*\b(physical\s*invoic(ing)?|payable\s*standard|desktop)\b", 10, "not appearing in invoicing desktop"),
        (r"\b(strategy|business\s*unit)\s*does\s*not\s*appear\b.*\b(matching)\b", 9, "strategy/BU not appearing in matching"),
        (r"\bnot\s*pick(ed)?\s*up\b", 8, "not picked up"),
        (r"\bnot\s*include(d)?\b.*\b(report|eod|grm)\b", 8, "not included in report"),
    ],
    "Missing Data": [
        (r"\bmissing\s*tax\s*line\b", 11, "missing tax line"),
        (r"\bmissing\s*line\s*(item|gl)?\b", 10, "missing line item/gl"),
        (r"\bmissing\b", 7, "generic missing"),
        (r"\bonly\s*(tax\s*line|adjustment\s*line)\s*populated\b", 10, "only tax/adjustment line"),
        (r"\bmain\s*line(s)?\s*missing\b", 10, "main line missing"),
        (r"\b(tax\s*code|tax\s*type)\s*(did\s*not\s*reflect|not\s*reflect(ed)?|not\s*appear(ing)?|did\s*not\s*appear|does\s*not\s*appear)\b", 11, "tax code not reflected"),
        (r"\btax\s*code\s*did\s*not\s*reflect\b", 12, "tax code did not reflect"),
        (r"\btax\s*(code|type)\s*(not\s*auto.?pop|not\s*populat|not\s*found|configuration\s*not\s*found)\b", 11, "tax code config not found"),
        (r"\bno\s*tax\s*(type|code|line)\b", 10, "no tax type/code/line"),
        (r"\b(vendor\s*code|sap\s*doc\s*type)\s*(not\s*auto.?pop|did\s*not\s*pop|not\s*reflect|does\s*not\s*appear|missing)\b", 10, "vendor code/SAP doc type missing"),
        (r"\bgl\s*(account)?\s*(disappear|missing|not\s*appear|only\s*mention|fee\s*holding)\b", 10, "GL account missing"),
        (r"\b(cost|item|fee|adjustment)\s*line\s*(missing|not\s*appear|did\s*not\s*appear)\b", 9, "cost/item line missing"),
        (r"\bmissing\s*(cost|item|fee)\s*line\b", 10, "missing cost/item/fee line"),
        (r"\bno\s*vendor\s*code\b", 9, "no vendor code"),
        (r"\bbl\s*date\s*missing\b", 9, "BL date missing"),
        (r"\bno\s*sap\s*doc\s*type\b|\bsap\s*doc\s*type.*missing\b", 10, "SAP doc type missing"),
        (r"\b(bank\s*address|beneficiary|account\s*number|acct\s*no)\s*(missing|not\s*appear|does\s*not\s*appear)\b", 9, "bank/beneficiary details missing"),
        (r"\bdelivery\s*id\s*(missing|not\s*appear|unable\s*to\s*(upload|find))\b", 9, "delivery ID missing"),
        (r"\b(unable|cannot)\s*view\s*document\b", 9, "unable to view document"),
    ],
    "Wrong Price": [
        (r"\b(wrong|incorrect|not\s*(same|match(ed)?|matching)|different|mismatch(ed)?|misalign(ed)?)\b.*\b" + PRICE_TERMS + r"\b", 11, "wrong/incorrect price"),
        (r"\b" + PRICE_TERMS + r"\b.*\b(wrong|incorrect|not\s*same|different|mismatch(ed)?)\b", 11, "price wrong/different"),
        (r"\bcategory\s*of\s*issue\s*[:：]\s*wrong\s*price\b", 12, "derivatives report wrong price"),
        (r"\bwrong\s*strike\s*price\b", 12, "wrong strike price"),
        (r"\b(strike\s*price|contract\s*price)\s*(in\s*endur|endur)\s*[:：]?\s*\d+", 11, "strike price in endur"),
        (r"\b(wrong|incorrect)\s*(mtm|p&l|pnl)\s*(price|value)\b", 10, "wrong MTM/PnL"),
        (r"\b(incorrect|wrong)\s*(eod|settlement)\s*price\b", 10, "incorrect EOD/settlement price"),
        (r"\b(decimal\s*place|3\s*dp|3dp|rounding)\b.*\b(price|strike|value|pv|fee)\b", 9, "decimal place rounding issue"),
        (r"\bdiscrepancy\b.*\b(price|mtm|pnl|p&l|strike)\b", 10, "discrepancy in price/PnL"),
        (r"\bprice\b.*\b(discrepancy|not\s*tally|does\s*not\s*tally)\b", 10, "price discrepancy"),
        (r"\b(var|value\s*at\s*risk)\b.*\b(wrong|incorrect|discrepancy|difference|huge\s*difference)\b", 9, "VaR discrepancy"),
        (r"\b(endur|system)\b.*\b(show(ing)?|reflect(ing)?)\b.*\b(wrong|incorrect|different|inaccurate)\s*(price|value|amount|mtm)\b", 10, "system showing wrong price"),
        (r"\b(captured|using)\s*(holiday|public\s*holiday)\s*price\b", 11, "captured holiday price"),
        (r"\b(please\s*)?remove\s*published\s*price\b", 11, "remove published price"),
    ],
    "Wrong Quantity": [
        (r"\b(wrong|incorrect|not\s*(same|match))\b.*\b" + QTY_TERMS + r"\b", 10, "wrong quantity"),
        (r"\b" + QTY_TERMS + r"\b.*\b(wrong|incorrect|not\s*same|mismatch)\b", 10, "quantity wrong"),
        (r"\b(volume|qty)\s*(incorrect|wrong|mismatch|not\s*correct)\b", 9, "volume/qty incorrect"),
    ],
    "Discrepancies - Amount": [
        (r"\bdiscrepanc(y|ies)\b.*\b" + AMOUNT_TERMS + r"\b", 11, "discrepancy amount"),
        (r"\b" + AMOUNT_TERMS + r"\b.*\b(discrepancy|not\s*tally|does\s*not\s*tally|different|tally)\b", 11, "amount not tally"),
        (r"\b(amend|fix|correct)\b.*\b(usd|myr).*amount\b", 10, "amend USD/MYR amount"),
        (r"\bdifferent\b.*\b(amount|myr|usd|lc|local\s*currency)\b", 9, "different amount/currency"),
        (r"\b(amount|myr|usd)\b.*\b(different|does\s*not\s*tally|not\s*tally|does\s*not\s*match|mismatch)\b", 10, "amount mismatch"),
        (r"\bdiscrepanc(y|ies)\s*of\s*(usd|myr)\s*[\d.,]+\b", 11, "discrepancy of USD/MYR value"),
        (r"\bdiscrepancy\s*of\s*(usd|myr|sgd)\s*0\.\d+\b", 11, "small discrepancy amount"),
    ],
    "Discrepancies - exchange rate": [
        (r"\b(exchange\s*rate|fx\s*rates?|fx)\b", 11, "FX/exchange rate"),
        (r"\bfx\s*rates?\s*undefined\b", 11, "FX rates undefined"),
        (r"\b(incorrect|wrong)\b.*\b(exchange\s*rate|fx|conversion)\b", 10, "incorrect exchange rate"),
        (r"\bmyr\s*(exchange\s*rate|rate)\s*(reflect(ed)?|applied|used)\s*(does\s*not\s*match|not\s*match|different|wrong|incorrect)\b", 10, "MYR exchange rate wrong"),
    ],
    "Discrepancies - Date": [
        (r"\b(posting\s*date|invoice\s*date|accounting\s*date)\b", 11, "specific date field"),
        (r"\bperiod\s*(1|block|jan|january|dec|december)\b", 10, "period change"),
        (r"\b(change|amend|correct|update)\b.{0,30}\b(date|period)\b", 9, "change date/period"),
        (r"\bbackdate\b", 9, "backdate"),
        (r"\b(update|amend|correct)\b.*\b(bl\s*date|movement\s*date|suppliers?\s*invoice\s*date)\b", 10, "amend BL/movement/supplier date"),
        (r"\b(incorrect|wrong|different)\b.*\b(invoice\s*date|posting\s*date|date)\b", 10, "incorrect date"),
        (r"\bperiod\s*1\b", 10, "period 1"),
    ],
    "Discrepancies - Tax": [
        (r"\b" + VERBS_AMEND + r"\b.*\b(tax|vat|gst|withholding|sst)\b", 11, "amend/update tax"),
        (r"\b(tax|vat|gst|withholding|sst)\b.*\b(description|code|event|type|ruling)\b", 10, "tax description/code/event"),
        (r"\badd(ed)?\s*tax\s*line\b|\badd.*tax\s*fee\b", 10, "add tax line/fee"),
        (r"\b(tax\s*(type|code|line|ruling))\s*(not\s*correct|incorrect|wrong|check|type\s*modification\s*fail)\b", 10, "tax code/type incorrect"),
        (r"\bcurrent\s*tax\s*type\s*(has\s*changed|doesn.?t\s*match|changed)\b", 11, "tax type changed/mismatch"),
        (r"\btax\s*(line|code)\s*(did\s*not\s*appear|not\s*appear|missing)\b", 11, "tax line/code not appearing"),
    ],
    "Discrepancies - Doc": [
        (r"\b" + DOC_TERMS + r"\b.*\b(number|no)\b.*\b" + VERBS_AMEND + r"\b", 10, "amend invoice/document number"),
        (r"\b(amend|fix|correct)\b.*\b(doc(ument)?|invoice)\b", 9, "amend doc/invoice"),
        (r"\b(reject|cancel)\b.*\b(invoice|ifp|doc(ument)?)\b", 8, "reject/cancel invoice/IFP"),
        (r"\b(wrong|incorrect)\s*(doc\s*type|document\s*type|sap\s*doc\s*type)\b", 10, "wrong doc type"),
        (r"\b(invoice\s*number|doc\s*number|running\s*number)\b.*\b(wrong|incorrect|amend)\b", 10, "wrong invoice/doc number"),
        (r"\b(cancel|reverse)\b.*\b(invoice|ifp|cn|dn)\b", 8, "cancel/reverse invoice"),
    ],
    "Discrepancies - Account": [
        (r"\b(gl|account|coa)\b.*\b" + VERBS_AMEND + r"\b", 9, "amend GL/account"),
        (r"\b(correct|wrong|incorrect)\b.*\b(gl|account|coa)\b", 9, "wrong GL/account"),
        (r"\b(gl|account)\s*(mapping|should\s*be|is\s*wrong|incorrect)\b", 9, "GL mapping wrong"),
    ],
    "Discrepancies - Bank Account Issue": [
        (BANK_TERMS, 11, "bank account/IBAN/SWIFT"),
        (r"\b(add|update|amend)\s*(account\s*number|acct\s*no|myr\s*account)\b", 10, "add/update account number"),
        (r"\b(beneficiary|remittance\s*bank|bank\s*address)\b.*\b(missing|not\s*appear|incorrect|no\s*dropdown)\b", 10, "beneficiary/bank details missing"),
    ],
    "Discrepancies - Customer Code":  [(r"\bcustomer\s*code\b.*\b" + VERBS_AMEND + r"\b", 10, "amend customer code")],
    "Discrepancies - Company Code":   [(r"\bcompany\s*code\b.*\b" + VERBS_AMEND + r"\b", 10, "amend company code")],
    "Discrepancies - Legal Entity":   [(r"\blegal\s*entity\b.*\b" + VERBS_AMEND + r"\b", 10, "amend legal entity")],
    "Discrepancies - Broker broker":  [(r"\b(broker|fee\s*broker)\b.*\b" + VERBS_AMEND + r"\b", 9, "amend broker/fee broker")],
    "Discrepancies - Strategy":       [(r"\bstrategy\b.*\b" + VERBS_AMEND + r"\b", 9, "amend strategy"), (r"\b(wrong|incorrect|not\s*correct)\b.*\bstrategy\b", 8, "wrong strategy")],
    "Discrepancies - Address":        [(r"\baddress\b.*\b" + VERBS_AMEND + r"\b", 9, "amend address")],
    "Discrepancies - vessel":         [(r"\bvessel\b.*\b" + VERBS_AMEND + r"\b", 9, "amend vessel")],
    "Discrepancies - Freight":        [(r"\bfreight\b.*\b" + VERBS_AMEND + r"\b", 9, "amend freight"), (r"\bfreight\s*invoice\b.*\b(wrong|incorrect|reverse|cancel)\b", 9, "freight invoice issue")],
    "Discrepancies - Bunker":         [(r"\bbunker\b.*\b" + VERBS_AMEND + r"\b", 9, "amend bunker"), (r"\bbunker\s*(expenses?|cost|fee|amount)\b.*\b(not\s*available|not\s*appear|missing)\b", 9, "bunker expenses missing")],
    "Discrepancies - Counterparty":   [(r"\b(counterparty|vendor|supplier)\b.*\b" + VERBS_AMEND + r"\b", 9, "amend counterparty/vendor"), (r"\b(vendor\s*code)\s*(not\s*in\s*endur|register|backend|amend|update)\b", 9, "vendor code update")],
    "Discrepancies - Details":        [(r"\bdetails?\b.*\b" + VERBS_AMEND + r"\b", 8, "amend details"), (r"\b(issuer\s*name|issuer)\b.*\b(change|amend|wrong|different)\b", 8, "change issuer name")],
    "Question Help Support": [
        (r"\bhow\s*to\b|\bkindly\s*advise\b|\bplease\s*advise\b|\bneed\s*guidance\b|may\s*i\s*know\s*how\s*to\b", 8, "asks how to/advise"),
        (r"\b(please|kindly)\s*(assist\s*to\s*(check|confirm|verify|explain|clarify|elaborate|look|investigate|provide))\b", 7, "assist to check/verify"),
        (r"\bwould\s*like\s*to\s*(understand|explore|know|request)\b", 7, "seeking understanding"),
        (r"\bcan\s*(you|trms)\s*(advise|confirm|explain|check|clarify)\b", 7, "can you advise/confirm"),
        (r"\b(how\s*do\s*i|what\s*should\s*i|what\s*are\s*the\s*steps?|next\s*steps?)\b", 7, "asking steps/process"),
    ],
    "System Error / Functional Issue": [
        (r"\bnomination\s*(save\s*)?failed\b", 12, "nomination save failed"),
        (r"\b(cargo\s*(id|status)|send\s*to\s*imos)\s*(failed|stuck|not\s*flow(ing)?)\b", 11, "cargo/IMOS failed"),
        (r"\bsend\s*to\s*imos\s*failed\b", 12, "send to IMOS failed"),
        (r"\b(unable\s*to\s*match|fail(ed)?\s*to\s*(match|break\s*match|actuali[sz]e?))\b", 10, "unable to match/actualize"),
        (r"\b(error|failed)\s*(pop(s?)?\s*out|pop.?up|message|appear)\b", 9, "error pops out"),
        (r"\b(security\s*enforced|security\s*object)\b", 10, "security enforced error"),
        (r"\b(can.?t\s*(run|open|access|process)|unable\s*to\s*(run|open|access|process|save|delete|view|log\s*in|login))\b", 9, "unable to run/open/access"),
        (r"\b(endur|system|imos)\s*(not\s*responding|unresponsive|keeps?\s*running|freeze|frozen|hang(ing)?)\b", 10, "system unresponsive/hanging"),
        (r"\b(flatfile|flat\s*file)\s*(keeps?\s*running|failed\s*to\s*generate|generate\s*failed)\b", 9, "flatfile generation issue"),
        (r"\b(actuali[sz]ation|actuali[sz]e)\s*(error|failed|issue|unable)\b", 10, "actualization error"),
        (r"\b(lhdn|e.?invoice|e.?document)\s*(failed|error|not\s*work|rejected|issue)\b", 9, "LHDN/e-invoice error"),
        (r"\b(unexpected\s*error|system\s*(error|issue|bug|problem))\b", 8, "system error/bug"),
        (r"\b(unable|cannot|can.?t)\s*(process|proceed)\s*document\b", 10, "cannot process document"),
    ],
    "Report Issue": [
        (r"\b(eod|end\s*of\s*day)\s*(report|pnl|p&l|grm)\b.*\b(wrong|incorrect|missing|not\s*appear|not\s*include|not\s*pick(ed)?)\b", 11, "EOD report wrong/missing"),
        (r"\b(pnl|p&l|pnl_cy|pnl_ly|var|value\s*at\s*risk)\b.*\b(report|breakdown)\b.*\b(wrong|incorrect|missing|issue|discrepancy|inconsistent)\b", 10, "PnL/VaR report issue"),
        (r"\b(grm|pnl\s*breakdown|raw\s*data\s*detail|report\s*builder)\b.*\b(wrong|incorrect|missing|issue|not\s*work|cannot\s*run|not\s*appear|not\s*reflect)\b", 10, "GRM/PnL Breakdown report issue"),
        (r"\b(cannot|unable)\s*(to\s*)?(run|generate|produce)\b.*\b(report|grm|pnl)\b", 10, "unable to run report"),
        (r"\brealised\s*derivatives\s*report\b.*\b(wrong|incorrect|missing|not\s*appear|not\s*include|not\s*pick(ed)?)\b", 11, "Realised Derivatives report issue"),
    ],
    "Access Issue": [
        (r"\b(unable|can.?t)\s*to?\s*(log\s*(in|into)|login|access|open)\b.*\b(endur|imos|system|citrix|desktop|finance\s*desktop)\b", 11, "unable to login/access system"),
        (r"\b(citrix|imos\s*id)\b.*\b(access|removed|not\s*available|issue|problem)\b", 10, "Citrix/IMOS access issue"),
        (r"\b(access\s*issue|login\s*issue|cannot\s*login)\b", 10, "access/login issue"),
        (r"\b(imos\s*id)\s*(had\s*been|was|is)\s*(removed|deleted|revoked)\b", 11, "IMOS ID removed"),
    ],
    "PLSB Bucket Amendment": [
        (r"\bplsb\s*(year\s*)?bucket\b", 12, "PLSB bucket keyword"),
        (r"\b(year\s*bucket|bucket(ing)?)\s*(2025|2026|from\s*20\d{2}\s*to\s*20\d{2})\b", 11, "year bucket 2025/2026"),
        (r"\b(amend|change|update|transfer)\b.*\b(plsb|year\s*bucket|bucket(ing)?)\b", 11, "amend PLSB/year bucket"),
    ],
    "Matching Issue": [
        (r"\b(unable\s*to\s*match|cannot\s*match|fail(ed)?\s*to\s*match)\b", 12, "unable to match"),
        (r"\b(matching\s*(issue|problem|error|failed))\b", 11, "matching issue"),
        (r"\b(deal|cargo)\s*(not\s*(matched|matche[sd])|unmatched)\b", 10, "deal not matched"),
        (r"\b(break\s*match|breakmatch)\b", 10, "break match"),
        (r"\b(unable\s*to|cannot|can.?t)\s*(break\s*match|unmatche)\b", 10, "unable to break match"),
        (r"\b(actuali[sz](e|ation))\b.*\b(error|fail(ed)?|unable|issue)\b", 11, "actualize error variant"),
    ],
}

PRIORITY = [
    "run corrective", "Push to SAP", "SAP Posting Failures", "Duplicate Invoice/Deal Number",
    "PLSB Bucket Amendment", "Matching Issue",
    "Not Update Data", "Missing Data", "Wrong Price", "Wrong Quantity",
    "Discrepancies - Amount", "Discrepancies - exchange rate", "Discrepancies - Date",
    "Discrepancies - Tax", "Discrepancies - Doc", "Discrepancies - Account",
    "Discrepancies - Bank Account Issue", "Discrepancies - Customer Code",
    "Discrepancies - Company Code", "Discrepancies - Legal Entity",
    "Discrepancies - Broker broker", "Discrepancies - Strategy", "Discrepancies - Address",
    "Discrepancies - vessel", "Discrepancies - Freight", "Discrepancies - Bunker",
    "Discrepancies - Counterparty", "Discrepancies - Details",
    "System Error / Functional Issue", "Report Issue",
    "Question Help Support", "Access Issue",
    "Others",
]

# ============================================================
# CLASSIFICATION FUNCTIONS
# ============================================================
def score_category(text_lc: str, cat: str) -> Tuple[int, List[str]]:
    total = 0
    cues: List[str] = []
    for patt, w, cue in PATTERNS.get(cat, []):
        if re.search(patt, text_lc, flags=re.IGNORECASE):
            total += w
            cues.append(cue)
    return total, cues

def pick_category(score_map: Dict[str, int]) -> str:
    if not score_map: return "Others"
    max_s = max(score_map.values())
    if max_s <= 0: return "Others"
    cands = [c for c, s in score_map.items() if s == max_s]
    for c in PRIORITY:
        if c in cands:
            return c
    return cands[0]

def build_reason(cat: str, cues: List[str], text_lc: str) -> str:
    cues_uniq = []
    for c in cues:
        if c not in cues_uniq:
            cues_uniq.append(c)
    if not cues_uniq:
        inferred = []
        if re.search(PRICE_TERMS,   text_lc): inferred.append("price terms")
        if re.search(AMOUNT_TERMS,  text_lc): inferred.append("amount terms")
        if re.search(FX_TERMS,      text_lc): inferred.append("FX terms")
        if re.search(DATE_TERMS,    text_lc): inferred.append("date/period")
        if re.search(TAX_TERMS,     text_lc): inferred.append("tax terms")
        if re.search(MISSING_PHRASES, text_lc): inferred.append("missing/visibility")
        cues_uniq = inferred[:4]
    s = f"Matched {cat} using cues: " + ", ".join(cues_uniq[:4]) + "."
    toks = s.split()
    if len(toks) > 40:
        s = " ".join(toks[:40])
    return s

def classify_text(text: str) -> Tuple[str, str, str]:
    t    = norm_whitespace(text)
    t_lc = t.lower()
    if not t_lc:
        return "Others", "Low confidence / ambiguous: empty description.", ""
    score_map: Dict[str, int]       = {}
    cues_map:  Dict[str, List[str]] = {}
    for cat in CATS:
        if cat == "Others":
            continue
        s, cues = score_category(t_lc, cat)
        if s > 0:
            score_map[cat] = s
            cues_map[cat]  = cues
    if not score_map:
        if re.search(PRICE_TERMS,        t_lc): score_map["Wrong Price"]                      = 2
        if re.search(AMOUNT_TERMS,       t_lc): score_map["Discrepancies - Amount"]           = score_map.get("Discrepancies - Amount", 0) + 2
        if re.search(FX_TERMS,           t_lc): score_map["Discrepancies - exchange rate"]    = 2
        if re.search(MISSING_PHRASES,    t_lc): score_map["Missing Data"]                     = 2
        if re.search(DATE_TERMS,         t_lc): score_map["Discrepancies - Date"]             = 2
        if re.search(SYSTEM_ERROR_PHRASES,t_lc): score_map["System Error / Functional Issue"] = 2
        if re.search(r"\b(match|matching|actuali[sz])\b", t_lc): score_map["Matching Issue"]  = 2
        if re.search(r"\b(report|pnl|p&l|var|eod)\b",    t_lc): score_map["Report Issue"]    = 2
    final_cat = pick_category(score_map) if score_map else "Others"
    cues      = cues_map.get(final_cat, [])
    reason    = build_reason(final_cat, cues, t_lc)
    top3      = sorted(score_map.items(), key=lambda kv: (-kv[1], PRIORITY.index(kv[0]) if kv[0] in PRIORITY else 999))[:3]
    match_dbg = "; ".join([f"{c}:{s}" for c, s in top3]) if top3 else ""
    return final_cat, reason, match_dbg

# ============================================================
# MAIN FUNCTION
# ============================================================
def run_classifier() -> Path:
    """
    Runs full classification pipeline.
    Returns path to the classified output Excel file.
    """
    print("=" * 60)
    print("PETRA — classifier.py")
    print("=" * 60)
    print(f"  Input  : {INPUT_FILE}")
    print(f"  Output : {OUTPUT_FILE}")

    df = pd.read_excel(INPUT_FILE, engine="openpyxl")

    if COL_DETAILED_DESC_1 in df.columns:
        src_desc_col = COL_DETAILED_DESC_1
    elif COL_DETAILED_DESC_2 in df.columns:
        src_desc_col = COL_DETAILED_DESC_2
    else:
        src_desc_col = COL_DETAILED_DESC_1
        df[src_desc_col] = ""

    df[COL_DESC_EXTRACT] = df[src_desc_col].apply(extract_desc_only)

    for c in DATE_COLS_PREF:
        if c in df.columns:
            df[c] = to_datetime_smart(df[c])

    keys_present = [k for k in ID_COLS_PREF if k in df.columns]
    dedup_keys   = keys_present + [COL_DESC_EXTRACT]
    sort_cols    = [c for c in DATE_COLS_PREF if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))

    before = len(df)
    df     = df.drop_duplicates(subset=dedup_keys, keep="first")
    print(f"  Dedup: removed {before - len(df)} rows → {len(df):,} remain")

    if COL_PCT3 not in df.columns:
        df[COL_PCT3] = ""

    print("  Classifying tickets...")
    res = df[COL_DESC_EXTRACT].apply(
        lambda s: pd.Series(
            classify_text(s),
            index=["Refined Summary", "Reason", "Match Debug"]
        )
    )
    out_df = pd.concat([df, res], axis=1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print("\n  Coverage (Refined Summary):")
    print(out_df["Refined Summary"].value_counts(dropna=False).to_string())
    print(f"\n  ✅ Classification complete → {OUTPUT_FILE}")
    return OUTPUT_FILE


if __name__ == "__main__":
    run_classifier()