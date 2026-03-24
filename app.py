"""
app.py — GST Invoice Extractor  (Streamlit web app)
Deployable for free on Streamlit Community Cloud.
Team members open the URL, upload invoices, download Excel — no installs needed.
"""

import os
import tempfile
import streamlit as st
import pandas as pd

from extractor import extract_invoice, build_excel_bytes

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GST Invoice Extractor",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar — Settings ────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/invoice.png", width=64)
    st.title("GST Invoice\nExtractor")
    st.divider()

    st.subheader("⚙️ Your GSTIN")
    buyer_gstin = st.text_input(
        "Organisation GSTIN",
        placeholder="e.g. 04ABJFR1773P1ZN",
        help="Enter your GSTIN. The tool uses this to correctly identify the "
             "supplier's GSTIN on each invoice.",
    )

    st.divider()
    st.subheader("🤖 AI Extraction  *(Recommended)*")
    st.caption(
        "Provide a **free** Google Gemini API key to enable AI-powered extraction. "
        "AI handles any vendor format automatically — including new vendors you've "
        "never seen before — with no rule changes required.\n\n"
        "**Get your free key →** [aistudio.google.com](https://aistudio.google.com)  \n"
        "*(No credit card. No charges. 15 req/min, 1M tokens/day.)*"
    )
    gemini_key = st.text_input("Gemini API Key", type="password", placeholder="AIza...")
    if gemini_key:
        st.success("✅ AI extraction enabled")
    else:
        st.info("ℹ️ Rule-based mode active — works well for standard formats.")

    st.divider()
    st.caption(
        "**Supports:** PDF · JPEG · PNG  \n"
        "**OCR:** enabled for scanned files  \n"
        "**Accuracy:** AI > rule-based for new vendors"
    )

# ── Main area — header ────────────────────────────────────────────────────────
st.title("📄 GST Invoice Extractor")
st.caption(
    "Upload your invoices (PDF / JPEG / PNG) and download a formatted Excel summary "
    "with all GST fields extracted automatically."
)
st.divider()

# ── How-to (shown only before upload) ────────────────────────────────────────
if "uploaded_once" not in st.session_state:
    with st.expander("ℹ️  How to use this tool", expanded=True):
        st.markdown("""
1. **Enter your GSTIN** in the sidebar (required — identifies supplier vs buyer).
2. *(Optional but recommended)* **Add a free Gemini API key** for AI extraction.
3. **Upload your invoice files** below — you can select multiple at once.
4. Click **Extract Data**.
5. **Download the Excel** file with all fields populated.

Invoices the tool can't fully parse are flagged automatically in a second sheet
for manual review — nothing is silently skipped.
""")

# ── File uploader ─────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    "**Upload invoice files**",
    type=["pdf", "jpg", "jpeg", "png"],
    accept_multiple_files=True,
    help="Digital PDFs, scanned PDFs, JPEG photos, and PNG images are all supported.",
)

if not uploaded:
    st.stop()

st.session_state["uploaded_once"] = True
n = len(uploaded)
st.success(f"**{n} file{'s' if n > 1 else ''}** ready.")

# Warn for large batches
if n > 100:
    st.warning(
        f"⚠️  You've uploaded {n} files. Processing may take several minutes "
        "on the free server tier. Consider splitting into batches of 50–100 for faster results."
    )

# ── Extract button ────────────────────────────────────────────────────────────
col_btn, _ = st.columns([2, 8])
run = col_btn.button("🚀  Extract Data", type="primary", use_container_width=True)

if not run:
    st.stop()

if not buyer_gstin.strip():
    st.error("⚠️  Please enter your GSTIN in the sidebar before extracting.")
    st.stop()

# ── Processing ────────────────────────────────────────────────────────────────
records  = []
failed   = []
log_lines = []

progress = st.progress(0, text="Starting…")
status   = st.empty()

with tempfile.TemporaryDirectory() as tmpdir:
    # Write all uploaded files to a temp folder
    for f in uploaded:
        with open(os.path.join(tmpdir, f.name), "wb") as out:
            out.write(f.getbuffer())

    files = sorted(os.listdir(tmpdir))

    for idx, fname in enumerate(files):
        status.text(f"⏳  {idx + 1} / {len(files)}  —  {fname}")
        progress.progress((idx + 1) / len(files))

        path = os.path.join(tmpdir, fname)
        try:
            rec, method = extract_invoice(
                path,
                fname,
                buyer_gstin.strip().upper(),
                gemini_key.strip() if gemini_key else None,
            )
            if rec:
                records.append((fname, rec))
                log_lines.append(f"✅  {fname}   [{method}]")
            else:
                failed.append((fname, method))
                log_lines.append(f"⚠️   {fname}   → {method}")
        except Exception as exc:
            failed.append((fname, f"Error: {exc}"))
            log_lines.append(f"❌  {fname}   → Error: {exc}")

progress.progress(1.0, text="Done!")
status.empty()

# ── Summary metrics ───────────────────────────────────────────────────────────
st.divider()
c1, c2, c3 = st.columns(3)
c1.metric("📂 Total Files",    len(uploaded))
c2.metric("✅ Extracted",      len(records))
c3.metric("⚠️ Needs Review",  len(failed))

# Processing log (collapsed by default)
with st.expander("📋  Processing Log", expanded=False):
    st.code("\n".join(log_lines), language=None)

# ── Preview table ─────────────────────────────────────────────────────────────
if records:
    st.subheader("📊  Preview  (first 20 rows)")
    preview = [
        {
            "File":          fn,
            "Invoice No.":   r.get("inv_no",    "—"),
            "Date":          r.get("inv_date",  "—"),
            "Supplier":      r.get("s_name",    "—"),
            "GSTIN":         r.get("s_gstin",   "—"),
            "Taxable (₹)":   r.get("taxable",   "—"),
            "GST Rate":      r.get("gst_rate",  "—"),
            "Tax Type":      r.get("tx_type",   "—"),
            "IGST (₹)":      r.get("igst",      "—"),
            "Total (₹)":     r.get("total",     "—"),
        }
        for fn, r in records[:20]
    ]
    st.dataframe(pd.DataFrame(preview), use_container_width=True, hide_index=True)

    if len(records) > 20:
        st.caption(f"Showing 20 of {len(records)} rows. Full data is in the Excel download.")

    # ── Download ──────────────────────────────────────────────────────────────
    st.divider()
    excel_bytes = build_excel_bytes(records, failed)
    st.download_button(
        label="📥  Download Excel Summary",
        data=excel_bytes,
        file_name="Invoice_Summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
    )
    st.caption(
        f"Excel contains **{len(records)} extracted rows** (Sheet 1) "
        f"and **{len(failed)} unprocessed files** (Sheet 2)."
    )

# ── Unprocessed files list ────────────────────────────────────────────────────
if failed:
    st.divider()
    st.subheader("⚠️  Files Needing Manual Entry")
    st.caption(
        "These files were either unreadable or the key fields could not be "
        "reliably parsed. They appear in Sheet 2 of the Excel with the reason noted."
    )
    for fname, reason in failed:
        st.markdown(f"- **{fname}** — {reason}")
