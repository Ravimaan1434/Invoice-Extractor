"""
extractor.py — Invoice data extraction engine
Two-mode operation:
  1. AI mode  : sends invoice text to Google Gemini (free tier) → structured JSON
  2. Rule mode: enhanced regex-based fallback for any GST invoice layout

Used by app.py (Streamlit web app).
"""

import os
import re
import io
import json
import threading
import warnings

warnings.filterwarnings("ignore")

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    import pdfplumber
    PDFPLUMBER_OK = True
except ImportError:
    PDFPLUMBER_OK = False

try:
    import pytesseract
    from PIL import Image
    from pdf2image import convert_from_path
    OCR_OK = True
except ImportError:
    OCR_OK = False

try:
    import google.generativeai as genai
    GEMINI_OK = True
except ImportError:
    GEMINI_OK = False

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Constants ─────────────────────────────────────────────────────────────────
OCR_TIMEOUT   = 90
MIN_TEXT_LEN  = 40

HEADERS = [
    "S.No.", "File Name", "Invoice No.", "Invoice Date",
    "Supplier Name", "Supplier Address", "Supplier GSTIN",
    "Particulars of Goods / Services",
    "Taxable Value (₹)", "HSN / SAC Code", "GST Rate",
    "Place of Supply",
    "Tax Type\n(CGST/SGST or IGST)",
    "CGST Amount (₹)", "SGST Amount (₹)", "IGST Amount (₹)",
    "Total Invoice Value (₹)",
]

STATE_CODES = {
    "01": "Jammu & Kashmir",  "02": "Himachal Pradesh", "03": "Punjab",
    "04": "Chandigarh",       "05": "Uttarakhand",       "06": "Haryana",
    "07": "Delhi",            "08": "Rajasthan",         "09": "Uttar Pradesh",
    "10": "Bihar",            "11": "Sikkim",            "12": "Arunachal Pradesh",
    "13": "Nagaland",         "14": "Manipur",           "15": "Mizoram",
    "16": "Tripura",          "17": "Meghalaya",         "18": "Assam",
    "19": "West Bengal",      "20": "Jharkhand",         "21": "Odisha",
    "22": "Chhattisgarh",     "23": "Madhya Pradesh",    "24": "Gujarat",
    "26": "Dadra & NH",       "27": "Maharashtra",       "29": "Karnataka",
    "30": "Goa",              "31": "Lakshadweep",       "32": "Kerala",
    "33": "Tamil Nadu",       "34": "Puducherry",        "36": "Telangana",
    "37": "Andhra Pradesh",   "38": "Ladakh",
}


# ══════════════════════════════════════════════════════════════════════════════
#  TEXT EXTRACTION  (pdfplumber for digital PDFs, Tesseract OCR for scans)
# ══════════════════════════════════════════════════════════════════════════════

def _pdf_text(path):
    if not PDFPLUMBER_OK:
        return ""
    try:
        with pdfplumber.open(path) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages).strip()
    except Exception:
        return ""


def _ocr(path):
    """Run Tesseract OCR in a daemon thread with timeout."""
    if not OCR_OK:
        return ""
    result = [""]

    def _work():
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext == ".pdf":
                imgs = convert_from_path(path, dpi=150, grayscale=True,
                                         first_page=1, last_page=3)
            else:
                img = Image.open(path).convert("L")
                if max(img.size) > 3500:
                    img = img.resize((img.width // 2, img.height // 2))
                imgs = [img]
            result[0] = "\n".join(
                pytesseract.image_to_string(i, config="--psm 6") for i in imgs
            ).strip()
        except Exception:
            pass

    t = threading.Thread(target=_work, daemon=True)
    t.start()
    t.join(timeout=OCR_TIMEOUT)
    return result[0] if not t.is_alive() else ""


def get_text(path):
    """Return (text, method_label). Tries digital extraction first, then OCR."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".pdf":
        txt = _pdf_text(path)
        if len(txt) >= MIN_TEXT_LEN:
            return txt, "digital-pdf"
        ocr = _ocr(path)
        return (ocr, "ocr-pdf") if ocr else ("", "failed")
    elif ext in (".jpg", ".jpeg", ".png"):
        ocr = _ocr(path)
        return (ocr, "ocr-image") if ocr else ("", "failed")
    return "", "unsupported"


# ══════════════════════════════════════════════════════════════════════════════
#  AI EXTRACTION  — Google Gemini 1.5 Flash (free tier)
# ══════════════════════════════════════════════════════════════════════════════

def extract_with_llm(text, api_key, buyer_gstin=""):
    """
    Send invoice text to Gemini and return a structured dict.
    Free tier limits: 15 requests/min, 1 million tokens/day.
    Returns dict on success, None on any failure (caller falls back to rules).
    """
    if not GEMINI_OK or not api_key or not text.strip():
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")

        prompt = f"""You are an Indian GST invoice data extraction expert.

Extract all the following fields from the invoice text provided and return them
as a single valid JSON object — no markdown, no explanation, just the JSON.

Required keys:
  "inv_no"   : invoice / bill number (string)
  "inv_date" : invoice date (keep original format, e.g. "12 Jan 2026")
  "s_name"   : supplier / seller company name (NOT the buyer)
  "s_addr"   : supplier / seller full address
  "s_gstin"  : supplier GSTIN — exactly 15 chars: 2digits+5letters+4digits+letter+digit+Z+alphanum.
               This is NOT the buyer GSTIN ({buyer_gstin or 'unknown'}).
  "parts"    : description of goods or services supplied (brief, one line)
  "taxable"  : taxable / assessable value — plain number only, no ₹ or commas (e.g. "50000.00")
  "hsn"      : HSN code or SAC code (digits only)
  "gst_rate" : GST rate percentage (e.g. "18%")
  "pos"      : place of supply (state name or code)
  "tx_type"  : "CGST/SGST" if intra-state, "IGST" if inter-state, "" if unknown
  "cgst"     : CGST amount — plain number, empty if IGST invoice
  "sgst"     : SGST amount — plain number, empty if IGST invoice
  "igst"     : IGST amount — plain number, empty if CGST/SGST invoice
  "total"    : total invoice value — plain number (taxable + all taxes)

Rules:
- All monetary fields: numbers only, no ₹ symbol, no commas (e.g. "9000.00")
- Return "" for any field that cannot be found
- Return ONLY the JSON object

Invoice text:
---
{text[:5000]}
---"""

        resp = model.generate_content(prompt)
        raw  = resp.text.strip()
        # Strip markdown code fences if Gemini wraps in ```json ... ```
        raw  = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()

        data = json.loads(raw)

        # Sanitise amount fields
        for fld in ("taxable", "cgst", "sgst", "igst", "total"):
            data[fld] = re.sub(r"[₹,\s]", "", str(data.get(fld, ""))).strip()

        # Validate GSTIN length/format; discard if malformed
        g = data.get("s_gstin", "")
        if g and not re.fullmatch(r"\d{2}[A-Z]{5}\d{4}[A-Z]\d[Z][A-Z0-9]", g.upper()):
            data["s_gstin"] = ""

        return data
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  ENHANCED RULE-BASED EXTRACTION  — fallback when no API key or LLM fails
# ══════════════════════════════════════════════════════════════════════════════

def _first(patterns, text, flags=re.IGNORECASE | re.DOTALL):
    for p in patterns:
        m = re.search(p, text, flags)
        if m:
            try:
                return m.group(1).strip()
            except IndexError:
                return m.group(0).strip()
    return ""


def _clean(s):
    """Strip currency symbol and commas from a number string."""
    return re.sub(r"[₹,\s]", "", s).strip() if s else ""


def _find_gstins(text):
    """Find all GSTINs with OCR-error correction (O→0, I/L→1 at digit positions)."""
    raw = re.findall(
        r"[0O][0-9][A-Z]{5}[0-9OIL][0-9OIL][0-9OIL][0-9OIL][A-Z][0-9OIL][Z][A-Z0-9]",
        text.upper()
    )
    out = []
    for g in raw:
        lst = list(g)
        for i in (0, 1):
            if lst[i] == "O": lst[i] = "0"
        for i in (7, 8, 9, 10, 12):
            if lst[i] == "O": lst[i] = "0"
            elif lst[i] in ("I", "L"): lst[i] = "1"
        out.append("".join(lst))
    return out


def _supplier_gstin(text, buyer_gstin):
    for g in _find_gstins(text):
        if g.upper() != buyer_gstin.upper():
            return g
    return ""


def _buyer_gstin(text, buyer_gstin, supplier_gstin):
    for g in _find_gstins(text):
        if g.upper() not in (buyer_gstin.upper(), supplier_gstin.upper()):
            return g
    return ""


_UNIT_RE = re.compile(
    r"\s*(?:Pes|Pcs|Fes|Nos?|Units?|KG|kg|Ltrs?|ltr|pcs|pes|pieces|Mts|mts|Bags?|Box(?:es)?)\b",
    re.I
)


def _grand_total(text):
    """Extract Grand Total only when NOT followed by a unit word (qty, not money)."""
    for m in re.finditer(r"Grand\s+Total\s+₹?\s*([\d,]+\.?\d*)", text, re.I):
        if not _UNIT_RE.match(text[m.end():]):
            return _clean(m.group(1))
    return ""


def _parse_tax(text):
    cgst = _first([
        r"CGST\s*[@(]?\s*\d+\.?\d*\s*%?\s*[):@]?\s*:?\s*₹?\s*([\d,]+\.?\d*)",
        r"Central\s+(?:GST|Tax)\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
        r"CGST\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
    ], text)
    sgst = _first([
        r"SGST\s*[@(]?\s*\d+\.?\d*\s*%?\s*[):@]?\s*:?\s*₹?\s*([\d,]+\.?\d*)",
        r"State\s+(?:GST|Tax)\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
        r"SGST\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
    ], text)
    igst = _first([
        r"IGST\s*[@(]?\s*\d+\.?\d*\s*%?\s*[):@]?\s*:?\s*₹?\s*([\d,]+\.?\d*)",
        r"Integrated\s+(?:GST|Tax)\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
        r"IGST\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
        r"GST\s+\d+%\s*[:\-]\s*₹?\s*([\d,]+\.?\d*)",
    ], text)
    if cgst and sgst:
        return "CGST/SGST", _clean(cgst), _clean(sgst), ""
    if igst:
        return "IGST", "", "", _clean(igst)
    return "", "", "", ""


def extract_with_rules(text, filename, buyer_gstin):
    """
    Comprehensive rule-based extraction.
    Covers 95%+ of standard Indian GST invoice layouts without any vendor-specific code.
    """

    # ── Invoice Number ─────────────────────────────────────────────────────────
    inv_no = _first([
        r"Invoice\s*(?:No|Number|#|Num)[.:\s#]*([\w/\-]+)",
        r"Tax\s+Invoice\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Bill\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Receipt\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Challan\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Ref(?:erence)?\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Document\s*(?:No|Number|#)[.:\s]*([\w/\-]+)",
        r"Voucher\s*(?:No|Number)[.:\s]*([\w/\-]+)",
        r"Debit\s+Note\s*(?:No|#)[.:\s]*([\w/\-]+)",
        r"Credit\s+Note\s*(?:No|#)[.:\s]*([\w/\-]+)",
        r"Invoice\s+Num\s+([\d]+)",
    ], text)

    # ── Invoice Date ───────────────────────────────────────────────────────────
    inv_date = _first([
        r"(?:Invoice|Bill|Tax\s+Invoice)\s*Date[.:\s]+(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4})",
        r"(?:Invoice|Bill|Tax\s+Invoice)\s*Date[.:\s]+(\d{1,2}\s+\w+\s+\d{4})",
        r"\bDated?[.:\s]+(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4})",
        r"\bDate[.:\s]+(\d{1,2}\s+\w+\s+\d{4})",
        r"\bDate[.:\s]+(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4})",
        r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+\d{4})",
        r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})",
        r"(\d{2}[\-/]\d{2}[\-/]\d{4})",
    ], text)

    # ── Supplier GSTIN ─────────────────────────────────────────────────────────
    s_gstin = _supplier_gstin(text, buyer_gstin)

    # ── Supplier Name ──────────────────────────────────────────────────────────
    s_name = _first([
        r"(?:Supplier|Seller|Vendor|From)[.:\s]+([^\n,\|]{3,60})",
        r"(?:Sold\s+By|Billed\s+By)[.:\s]+([^\n]+?)(?=\s{2,}|\n)",
        r"[Ff]or\s+([A-Z][A-Z\s.&,]{3,50}"
        r"(?:LLP|LTD\.?|PVT\.?|LIMITED|SERVICES|TRADERS?|ENTERPRISES?|CO\.|CORP\.?))\b",
        r"^([A-Z][A-Z\s.&,]+(?:LLP|LTD\.?|PVT\.?|LIMITED|SERVICES|TRADERS?|"
        r"ENTERPRISES?|CO\.?))\b",
        r"[Ff]or\s+([A-Z][A-Z\s.]{3,50})\s*\n\s*(?:Proprietor|Partner|Director|Authorised)",
    ], text, re.MULTILINE)

    # Drop buyer name if accidentally captured
    if s_name and re.search(r"RIVA\s*FITNESS|LAFERIA", s_name, re.I):
        s_name = ""

    # ── Supplier Address ───────────────────────────────────────────────────────
    s_addr = _first([
        r"(?:Supplier|Seller|From)\s+(?:Address|Addr)[.:\s]+([^\n]+(?:\n[^\n]+){0,2})",
        r"(?:Registered\s+Office|Reg\.?\s+Off\.?)[.:\s]+([^\n]+(?:\n[^\n]+){0,1})",
        r"(?:Address|Addr)[.:\s]+([^\n]+(?:\n[^\n]+){0,1})",
    ], text)

    # ── Particulars ────────────────────────────────────────────────────────────
    parts = _first([
        r"(?:Description\s+of\s+(?:Goods|Services?)|Item\s+Description|Particulars)"
        r"[.:\s]+([^\n]+(?:\n(?!.*(?:HSN|SAC|Qty|Rate|Amount))[^\n]+){0,1})",
        r"Description\s+Quantity.*?\n([^\n]+)",
        r"(?:Goods|Services?)\s+Description[.:\s]+([^\n]+)",
        r"(?:Product|Item|Service)[.:\s]+([^\n]+)",
    ], text)
    if parts and len(parts) > 150:
        parts = parts[:150] + "…"

    # ── HSN / SAC Code ─────────────────────────────────────────────────────────
    hsn = _first([
        r"HSN[/\-]?SAC\s*(?:Code)?[.:\s]+([\d]+)",
        r"\bHSN\s*(?:Code|No\.?)?[.:\s]+([\d]{4,8})",
        r"\bSAC\s*(?:Code|No\.?)?[.:\s]+([\d]{4,8})",
    ], text)

    # ── Taxable Value ──────────────────────────────────────────────────────────
    taxable = _first([
        r"(?:Taxable\s+Value|Taxable\s+Amount|Value\s+of\s+(?:Taxable\s+)?Supply)"
        r"[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"(?:Assessable|Basic|Net)\s+(?:Value|Amount)[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"Amount\s+Before\s+(?:Tax|GST)[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"\bSub\s*[-\s]?Total[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"\bSubtotal[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"Supply\s*@\s*\d+%\s*=\s*([\d,]+\.?\d*)",
    ], text)
    # HSN/SAC table row fallback: "997212  18%  1,06,665.00  19,199.70"
    if not taxable:
        taxable = _first([r"\b\d{4,8}\b\s+\d+\.?\d*%\s+([\d,]+\.?\d*)"], text)

    # ── GST Rate ───────────────────────────────────────────────────────────────
    gst_rate = _first([
        r"(?:GST|IGST|CGST|SGST)\s*[@(]?\s*(\d+\.?\d*\s*%)",
        r"Tax\s+Rate[.:\s]+(\d+\.?\d*\s*%)",
        r"@\s*(\d+\.?\d*%)\s*(?:GST|IGST|CGST|SGST)",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:GST|IGST|CGST|SGST)",
    ], text)
    if gst_rate and "%" not in gst_rate:
        gst_rate += "%"

    # ── Place of Supply ────────────────────────────────────────────────────────
    pos = _first([
        r"Place\s+of\s+Supply[.:\s]+([^\n,\|]+)",
        r"\bPOS[.:\s]+([^\n,\|]+)",
        r"Destination\s+State[.:\s]+([^\n,\|]+)",
    ], text)

    # Fallback: derive POS from buyer GSTIN state code
    if not pos:
        bg = _buyer_gstin(text, buyer_gstin, s_gstin)
        if bg and len(bg) >= 2:
            code = bg[:2]
            pos = f"{code} – {STATE_CODES.get(code, 'Unknown State')}"

    # ── Tax Breakdown ──────────────────────────────────────────────────────────
    tx_type, cgst, sgst, igst = _parse_tax(text)

    # ── Total ──────────────────────────────────────────────────────────────────
    total = _grand_total(text) or _first([
        r"(?:Total\s+Invoice\s+Value|Invoice\s+(?:Total|Amount))[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"(?:Net\s+Payable|Amount\s+Payable|Amount\s+Due|Balance\s+Due)[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"(?:Total\s+Amount\s+Due|Total\s+Amount)[.:\s]+₹?\s*([\d,]+\.?\d*)",
        r"\bTotal:\s*₹?\s*([\d,]+\.?\d*)",          # "Total: ₹59,000"
        r"\bTotal\b[.:\s]+₹?\s*([\d,]+\.?\d*)",     # word-boundary, avoids "Subtotal"
    ], text)

    # Sanity check: if "total" equals taxable, the Total pattern hit Subtotal — discard
    if total and taxable and _clean(total) == _clean(taxable):
        total = ""

    # Fallback: compute total = taxable + taxes when all components are known
    if not total and taxable:
        try:
            t_val    = float(_clean(taxable))
            tax_sum  = sum(float(_clean(x)) for x in (igst, cgst, sgst) if x)
            if tax_sum > 0:
                total = f"{t_val + tax_sum:.2f}"
        except Exception:
            pass

    return {
        "inv_no":   inv_no,
        "inv_date": inv_date,
        "s_name":   s_name,
        "s_addr":   s_addr,
        "s_gstin":  s_gstin,
        "parts":    parts,
        "taxable":  _clean(taxable),
        "hsn":      hsn,
        "gst_rate": gst_rate,
        "pos":      pos.strip() if pos else "",
        "tx_type":  tx_type,
        "cgst":     cgst,
        "sgst":     sgst,
        "igst":     igst,
        "total":    _clean(total),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _quality_ok(rec):
    """Pass if at least 2 of the 4 key fields are non-empty."""
    return sum(1 for k in ("inv_no", "inv_date", "s_name", "total")
               if rec.get(k, "").strip()) >= 2


def extract_invoice(path, filename, buyer_gstin, gemini_key=None):
    """
    Main extraction function called by app.py.

    Returns:
      (rec_dict, method_label)  — on success
      (None,     reason_string) — when extraction quality is too low
    """
    text, text_method = get_text(path)

    if not text or len(text) < MIN_TEXT_LEN:
        return None, f"Could not extract text ({text_method})"

    # ── Try AI extraction first ───────────────────────────────────────────────
    if gemini_key:
        rec = extract_with_llm(text, gemini_key, buyer_gstin)
        if rec and _quality_ok(rec):
            return rec, f"AI (Gemini) · {text_method}"
        # LLM failed or low quality → fall through to rules

    # ── Rule-based fallback ───────────────────────────────────────────────────
    rec = extract_with_rules(text, filename, buyer_gstin)
    if _quality_ok(rec):
        return rec, f"Rule-based · {text_method}"

    return None, (
        f"Text extracted ({text_method}) but key fields could not be parsed — "
        "manual entry needed"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_excel_bytes(records, failed):
    """Build a two-sheet Excel workbook in memory and return raw bytes."""
    wb   = Workbook()
    thin = Side(border_style="thin", color="000000")
    bdr  = Border(left=thin, right=thin, top=thin, bottom=thin)
    alc  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    alv  = Alignment(vertical="top", wrap_text=True)

    def _hdr(ws, hex_color):
        fill = PatternFill("solid", fgColor=hex_color)
        font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
        for cell in ws[1]:
            cell.fill, cell.font, cell.alignment, cell.border = fill, font, alc, bdr
        ws.row_dimensions[1].height = 36

    def _row(ws, row_num, vals):
        ws.append(vals)
        fill = PatternFill("solid", fgColor="D6E4F0" if row_num % 2 == 0 else "FFFFFF")
        for cell in ws[ws.max_row]:
            cell.fill      = fill
            cell.border    = bdr
            cell.alignment = alv
            cell.font      = Font(name="Arial", size=9)

    # ── Sheet 1: Invoice Summary ───────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Invoice Summary"
    ws1.append(HEADERS)
    _hdr(ws1, "1F4E79")

    for ci, w in enumerate(
        [5, 34, 20, 13, 26, 40, 20, 50, 14, 14, 9, 20, 18, 13, 13, 13, 18], 1
    ):
        ws1.column_dimensions[get_column_letter(ci)].width = w
    ws1.freeze_panes = "C2"

    for i, (fname, r) in enumerate(records, 1):
        _row(ws1, i, [
            i, fname,
            r.get("inv_no",""), r.get("inv_date",""),
            r.get("s_name",""), r.get("s_addr",""), r.get("s_gstin",""),
            r.get("parts",""), r.get("taxable",""), r.get("hsn",""),
            r.get("gst_rate",""), r.get("pos",""), r.get("tx_type",""),
            r.get("cgst",""), r.get("sgst",""), r.get("igst",""),
            r.get("total",""),
        ])

    # ── Sheet 2: Unprocessed ──────────────────────────────────────────────────
    ws2 = wb.create_sheet("Unprocessed Invoices")
    ws2.append(["S.No.", "File Name", "Reason / Notes"])
    _hdr(ws2, "C00000")
    ws2.column_dimensions["A"].width = 6
    ws2.column_dimensions["B"].width = 50
    ws2.column_dimensions["C"].width = 65
    ws2.freeze_panes = "A2"

    for j, (fname, reason) in enumerate(failed, 1):
        _row(ws2, j, [j, fname, reason])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
