"""
PunchlistQC.py - Punchlist Items Quality Control
=================================================

Reads all open punchlist items from PMA_PunchlistItems and uses Azure OpenAI
to detect three categories of problems:

  duplicate    - Two open items that request substantially the same thing.
  already_done - An open item whose goal is covered by a recently completed item.
  stale        - An open item that is very old, vague, or unactionable as written.

Standalone CLI:
    python PunchlistQC.py                      # print QC report, no writes
    python PunchlistQC.py --lookback 90        # compare against last 90 days of completed items

Imported by PunchlistCommander for the "🔍 QC" toolbar button.

Author: Pat Yearick
Created: June 2026
"""

import os
import sys
import json
import logging
import pyodbc
from pathlib import Path
from datetime import datetime
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load .env from project root, then fall back to PMAssistant (same as PunchlistCollator)
for _env_path in [
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent.parent / "PMAssistant" / ".env",
]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break

# =============================================================================
# CONFIGURATION
# =============================================================================

SQL_SERVER    = "BI-SQL001"
SQL_DATABASE  = "CRPAF"
SQL_DRIVER    = "ODBC Driver 17 for SQL Server"
LOG_FILE      = r"C:\Logs\PunchlistQC.log"
AZURE_MODEL   = "gpt-4o-3"
LOOKBACK_DAYS = 180

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# DATABASE
# =============================================================================

def get_connection():
    return pyodbc.connect(
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )


def get_open_items(conn):
    """Fetch all open/in-progress/blocked items with full detail."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT PunchlistItemID, Project, ItemNumber, Title, Description,
               Status, Priority, BlockedBy, CreatedDate, LastModifiedDate
        FROM [dbo].[PMA_PunchlistItems]
        WHERE Status IN ('Open', 'In Progress', 'Blocked')
        ORDER BY Project, ItemNumber
    """)
    rows = cursor.fetchall()
    cursor.close()
    return [
        {
            'id':            r[0],
            'project':       r[1],
            'item_number':   r[2] or '(no #)',
            'title':         r[3],
            'description':   r[4] or '',
            'status':        r[5],
            'priority':      r[6] or 'Medium',
            'blocked_by':    r[7],
            'created_date':  r[8],
            'modified_date': r[9],
        }
        for r in rows
    ]


def get_completed_items(conn, lookback_days=LOOKBACK_DAYS):
    """Fetch completed items within the lookback window."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT PunchlistItemID, Project, ItemNumber, Title, Description,
               CompletedDate
        FROM [dbo].[PMA_PunchlistItems]
        WHERE Status = 'Completed'
          AND CompletedDate >= DATEADD(DAY, ?, GETDATE())
        ORDER BY Project, ItemNumber
    """, (-lookback_days,))
    rows = cursor.fetchall()
    cursor.close()
    return [
        {
            'id':             r[0],
            'project':        r[1],
            'item_number':    r[2] or '(no #)',
            'title':          r[3],
            'description':    r[4] or '',
            'completed_date': r[5],
        }
        for r in rows
    ]


# =============================================================================
# PROMPT BUILDING
# =============================================================================

def _fmt_open(item):
    created  = item['created_date'].strftime('%Y-%m-%d')  if item['created_date']  else '?'
    modified = item['modified_date'].strftime('%Y-%m-%d') if item['modified_date'] else '?'
    lines = [
        f"[ID:{item['id']}] {item['project']} | {item['item_number']} | {item['status']} | {item['priority']}",
        f"Created: {created}  Last modified: {modified}",
        f"Title: {item['title']}",
    ]
    if item['description'].strip():
        lines.append(f"Description: {item['description'].strip()[:600]}")
    if item['blocked_by']:
        lines.append(f"Blocked by: {item['blocked_by']}")
    return '\n'.join(lines)


def _fmt_completed(item):
    completed = item['completed_date'].strftime('%Y-%m-%d') if item['completed_date'] else '?'
    lines = [
        f"[ID:{item['id']}] {item['project']} | {item['item_number']} | Completed: {completed}",
        f"Title: {item['title']}",
    ]
    if item['description'].strip():
        lines.append(f"Description: {item['description'].strip()[:400]}")
    return '\n'.join(lines)


def build_qc_prompt(focus_items, other_open_items, completed_items):
    """
    Build a focused QC prompt for one project's items.

    focus_items      — the project being reviewed; AI only flags these.
    other_open_items — open items from all other projects, provided as cross-reference context.
    completed_items  — recently completed items (all projects) for already-done detection.
    """
    project = focus_items[0]['project'] if focus_items else '?'
    sep = '=' * 60
    schema = (
        '{\n'
        '  "findings": [\n'
        '    {\n'
        '      "type": "duplicate" | "already_done" | "stale",\n'
        '      "item_id": <int — FOCUS item being flagged>,\n'
        '      "item_number": "<string>",\n'
        '      "project": "<string>",\n'
        '      "title": "<string>",\n'
        '      "related_item_id": <int or null>,\n'
        '      "related_item_number": "<string or null>",\n'
        '      "related_project": "<string or null>",\n'
        '      "reason": "<concise, 1-2 sentences>"\n'
        '    }\n'
        '  ],\n'
        '  "summary": "<brief overall summary string>"\n'
        '}'
    )
    parts = [
        "You are a QC analyst reviewing open work items for CRP Industries, an automotive aftermarket parts company.\n\n",
        f"Review the FOCUS ITEMS from project {project} below. For each focus item, check:\n"
        "  1. Against other focus items — do any two request substantially the same thing? (within-project duplicate)\n"
        "  2. Against COMPARISON items — does any focus item overlap significantly with an item from another project? (cross-project duplicate)\n"
        "  3. Against COMPLETED items — is any focus item's goal already achieved by a recently completed item?\n"
        "  4. On its own — is any focus item stale (created 6+ months ago AND description is vague or unactionable)?\n\n"
        "Only flag FOCUS items in your output. Comparison and Completed items are reference context only.\n"
        "Flag anything that looks questionable — the human will make the final call.\n"
        "Lean toward surfacing potential issues rather than suppressing them.\n\n",
        "FINDING TYPES:\n"
        "  duplicate    - Two items requesting substantially the same thing.\n"
        "                 Set item_id = the FOCUS item, related_item_id = the item it duplicates.\n"
        "  already_done - A focus item whose goal is covered by a completed item.\n"
        "                 Set item_id = the focus item, related_item_id = the completed item.\n"
        "  stale        - A focus item that is old AND vague/unactionable.\n"
        "                 Set related_item_id / related_item_number / related_project to null.\n\n",
        f"JSON SCHEMA (return this structure only — no markdown, no extra keys):\n{schema}\n\n",
        f"\n{sep}\nFOCUS ITEMS — {project} ({len(focus_items)})\n{sep}\n\n",
    ]
    for item in focus_items:
        parts.append(_fmt_open(item) + '\n\n')

    if other_open_items:
        parts.append(
            f"\n{sep}\nCOMPARISON: OTHER PROJECTS' OPEN ITEMS ({len(other_open_items)})\n{sep}\n\n"
        )
        for item in other_open_items:
            parts.append(_fmt_open(item) + '\n\n')

    if completed_items:
        parts.append(
            f"\n{sep}\nCOMPLETED IN LAST {LOOKBACK_DAYS} DAYS ({len(completed_items)})\n{sep}\n\n"
        )
        for item in completed_items:
            parts.append(_fmt_completed(item) + '\n\n')

    return ''.join(parts)


# =============================================================================
# AZURE OPENAI
# =============================================================================

def _get_llm_client():
    api_key  = os.getenv("AZURE_OPENAI_API_KEY")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    if not api_key or not endpoint:
        raise RuntimeError(
            "Azure OpenAI credentials not found. "
            "Expected AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in .env"
        )
    return AzureOpenAI(api_key=api_key, api_version="2024-02-01", azure_endpoint=endpoint)


def run_analysis(focus_items, other_open_items, completed_items):
    """
    Call Azure OpenAI for one project's QC pass.  Returns a list of finding dicts.
    Each finding has: type, item_id, item_number, project, title,
    related_item_id, related_item_number, related_project, reason.
    """
    if not focus_items:
        return []

    project = focus_items[0]['project']
    client = _get_llm_client()
    prompt = build_qc_prompt(focus_items, other_open_items, completed_items)
    logger.info(f"[{project}] Sending {len(prompt):,} chars to Azure OpenAI…")

    response = client.chat.completions.create(
        model=AZURE_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a QC analyst for a software development team. "
                    "Return only valid JSON exactly matching the schema provided. "
                    "Do not include markdown or explanatory text outside the JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    raw = response.choices[0].message.content
    logger.info(f"[{project}] Received {len(raw):,} chars from Azure OpenAI.")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Azure OpenAI returned invalid JSON: {exc}\n\nFirst 500 chars:\n{raw[:500]}"
        )

    findings = data.get("findings", [])
    summary  = data.get("summary", "")
    logger.info(f"[{project}] {summary}")
    logger.info(f"[{project}] Findings: {len(findings)}")
    return findings


# =============================================================================
# WRITE-BACK
# =============================================================================

def apply_mark_completed(conn, item_id):
    """Mark an open item Completed with today's date."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE [dbo].[PMA_PunchlistItems]
        SET Status = 'Completed',
            CompletedDate = GETDATE(),
            LastModifiedDate = GETDATE()
        WHERE PunchlistItemID = ?
    """, (item_id,))
    conn.commit()
    cursor.close()
    logger.info(f"Item {item_id} marked Completed by QC.")


def apply_flag_duplicate(conn, item_id, related_ref):
    """
    Prepend a duplicate warning to the item's Description.
    related_ref should be a human-readable string, e.g. 'PLM-007 (PLM)'.
    Guards against double-stamping if the item is already flagged.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT Description FROM [dbo].[PMA_PunchlistItems] WHERE PunchlistItemID = ?",
        (item_id,)
    )
    row = cursor.fetchone()
    existing = (row[0] or '').strip() if row else ''

    note = f"⚠️ Possible duplicate of {related_ref} — review and close if confirmed.\n\n"
    if existing.startswith("⚠️ Possible duplicate"):
        new_desc = existing  # already flagged, don't double-stamp
    else:
        new_desc = note + existing

    cursor.execute("""
        UPDATE [dbo].[PMA_PunchlistItems]
        SET Description = ?, LastModifiedDate = GETDATE()
        WHERE PunchlistItemID = ?
    """, (new_desc, item_id))
    conn.commit()
    cursor.close()
    logger.info(f"Item {item_id} flagged as possible duplicate of {related_ref}.")


# =============================================================================
# ORCHESTRATION
# =============================================================================

def run_qc(lookback_days=LOOKBACK_DAYS):
    """
    Main entry point for external callers (e.g. PunchlistCommander).

    Runs one focused Azure OpenAI pass per project so the model has a clear
    target set rather than one giant undifferentiated list.  Cross-project
    duplicate findings are deduplicated across passes by item-pair.

    Returns:
        (findings, items_by_id)
        findings    — list of finding dicts, deduplicated across all passes
        items_by_id — dict mapping PunchlistItemID -> open item dict
    """
    conn = get_connection()
    try:
        all_open        = get_open_items(conn)
        completed_items = get_completed_items(conn, lookback_days)
    finally:
        conn.close()

    logger.info(
        f"Loaded {len(all_open)} open item(s), "
        f"{len(completed_items)} completed item(s) in last {lookback_days} days."
    )

    # Group open items by project for focused per-project passes
    by_project = {}
    for item in all_open:
        by_project.setdefault(item['project'], []).append(item)

    all_findings = []
    seen_pairs   = set()   # dedup duplicate findings found from both sides
    seen_singles = set()   # dedup already_done / stale by item_id

    for project, focus_items in sorted(by_project.items()):
        other_open = [i for i in all_open if i['project'] != project]
        findings   = run_analysis(focus_items, other_open, completed_items)

        for f in findings:
            ftype   = f.get('type')
            item_id = f.get('item_id')

            if ftype == 'duplicate':
                pair = frozenset({item_id, f.get('related_item_id')})
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
            else:
                if item_id in seen_singles:
                    continue
                seen_singles.add(item_id)

            all_findings.append(f)

    logger.info(f"Total findings after dedup: {len(all_findings)}")
    items_by_id = {item['id']: item for item in all_open}
    return all_findings, items_by_id


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Punchlist QC — detect duplicates, already-done items, and stale entries."
    )
    parser.add_argument(
        '--lookback', type=int, default=LOOKBACK_DAYS,
        help=f"Days of completed items to compare against (default: {LOOKBACK_DAYS})"
    )
    args = parser.parse_args()

    findings, _ = run_qc(lookback_days=args.lookback)

    if not findings:
        print("\n✅ No QC issues found.\n")
        return

    labels = {
        'duplicate':    'DUPLICATE',
        'already_done': 'ALREADY DONE',
        'stale':        'STALE',
    }
    print(f"\n{'=' * 60}")
    print(f"QC REPORT  —  {len(findings)} finding(s)")
    print(f"{'=' * 60}")
    for f in findings:
        label = labels.get(f.get('type', ''), f.get('type', '?').upper())
        print(f"\n[{label}]  {f.get('project')} | {f.get('item_number')}: {f.get('title')}")
        if f.get('related_item_number'):
            print(f"  Related:  {f.get('related_project')} | {f.get('related_item_number')}")
        print(f"  Reason:   {f.get('reason')}")
    print(f"\n{'=' * 60}\n")


if __name__ == "__main__":
    main()
