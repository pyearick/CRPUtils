"""
start_fix.py - Start Fix engine for Punchlist Commander
=========================================================

Assembles a context-rich brief for a single punchlist item and launches
Claude Code against the project folder to work the fix. Also handles the
companion "Refresh Synopsis" action and the work-performed log.

This module is the engine. PunchlistCommander.py imports it and wires the
buttons; it can also be run standalone from the command line for testing:

    # Assemble + launch Claude Code interactively for a punchlist item
    python start_fix.py --project PLM --item PLM-002

    # Assemble the brief and write it to disk, but do NOT launch
    python start_fix.py --project PLM --item PLM-002 --dry-run

    # Regenerate <project>_Synopsis.md via headless Claude Code
    python start_fix.py --project PLM --refresh-synopsis

What it reuses (all live alongside this file in the CRPUtils folder):
  - GPTProjectUploadGUI.create_project_document  -> fresh pdoc XML
  - ProjectAnalyzer.run_full_analysis            -> project inventory prompt

Pieces:
  1. build_pdoc            - refresh pdoc_<project>.xml
  2. get_roster_analysis   - run ProjectAnalyzer across the full roster (cached)
  3. assemble_start_fix_prompt - punchlist item + siblings + cross-refs
                                  + synopsis + inventory -> one brief
  4. launch_claude_code    - hand the brief to Claude Code (interactive or -p)
  5. refresh_synopsis      - headless Claude Code writes <project>_Synopsis.md
  6. PMA_PunchlistWorkLog  - one row per fix: started/completed + git delta

Lives in: CRPUtils folder
Database: BI-SQL001 / CRPAF

Author: Pat Yearick
Created: May 2026
"""

import os
import sys
import re
import glob
import shutil
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json, threading, time
from blast_radius import compute_blast_radius, render_blast_radius

import pyodbc

# =============================================================================
# CONFIGURATION
# =============================================================================

SQL_SERVER = "BI-SQL001"
SQL_DATABASE = "CRPAF"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"

# Log file - C:/Logs, no date suffix (CRPAF convention).
LOG_FILE = r"C:/Logs/StartFix.log"

# Transient files (assembled briefs, launch .bat files) live on a local,
# NON-OneDrive path so regenerating them every run does not thrash sync.
TEMP_DIR = r"C:/Temp/StartFix"

# The Claude Code CLI command. Pinned to the full path of the npm-installed
# claude.cmd shim so Start Fix does not depend on PATH (a self-updater once
# left the binary missing from PATH, which broke launches). If you reinstall
# elsewhere, update this - 'where claude' in cmd prints the current path.
CLAUDE_CMD = r"C:\Users\pyearick.CRP\AppData\Roaming\npm\claude.cmd"

# Path to wt.exe (Windows Terminal) - the preferred terminal for interactive
# Start Fix sessions (proper copy/paste, resizing, scrollback - unlike a raw
# cmd console). Leave as None to auto-detect. Set to a full path to override,
# e.g. a portable / unzipped build:
#   WT_CMD = r"C:\Tools\WindowsTerminal\terminal-1.24.11911.0\wt.exe"
WT_CMD = None

# Extra folders searched for a portable Windows Terminal build when WT_CMD is
# None and wt.exe is not on PATH. Each folder is checked directly and one level
# deep, so a versioned extraction folder (terminal-1.24.11911.0) is found
# without pinning the version number here.
WT_SEARCH_DIRS = [
    r"C:\Tools\WindowsTerminal",
    os.path.join(os.environ.get("USERPROFILE", ""), r".local\bin\WindowsTerminal"),
]

# Path to ConEmu64.exe - retained as a fallback for machines that still have
# ConEmu installed. Same override semantics as WT_CMD.
CONEMU_CMD = None

# Tools Claude Code is allowed to use in HEADLESS (Refresh Synopsis) runs.
# Interactive Start Fix sessions are not scoped here - you approve in person.
# Scoped to reading the project and writing the synopsis; no Bash.
CLAUDE_HEADLESS_TOOLS = "Read,Glob,Grep,Write"

# =============================================================================
# LOGGING
# =============================================================================

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# DATABASE
# =============================================================================

def get_connection():
    """Get a SQL Server connection using Windows auth."""
    return pyodbc.connect(
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )


def ensure_worklog_table():
    """
    Create PMA_PunchlistWorkLog if it does not exist. Idempotent.

    One row per fix attempt: FixStarted inserts it, Mark Fixed completes it.
    Doubles as an invoicing feed (StartedDate / CompletedDate) and a per-item
    work history (GitCommitSummary / WorkNote).

    Note: PunchlistItemID is a plain INT, NOT a foreign key. This is a
    durable audit/invoicing record - deleting a punchlist item must never
    block the delete or destroy the work history. Project + ItemNumber are
    stored denormalized so the row stays readable on its own.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PMA_PunchlistWorkLog')
        BEGIN
            CREATE TABLE [dbo].[PMA_PunchlistWorkLog] (
                [WorkLogID]         INT IDENTITY(1,1) PRIMARY KEY,
                [PunchlistItemID]   INT NOT NULL,
                [Project]           NVARCHAR(100) NOT NULL,
                [ItemNumber]        NVARCHAR(50) NULL,
                [StartedDate]       DATETIME NOT NULL DEFAULT GETDATE(),
                [CompletedDate]     DATETIME NULL,
                [PromptFile]        NVARCHAR(500) NULL,
                [GitCommitSummary]  NVARCHAR(MAX) NULL,
                [WorkNote]          NVARCHAR(MAX) NULL,
                [Outcome]           NVARCHAR(20) NULL
            )

            CREATE INDEX IX_WorkLog_Item
                ON [dbo].[PMA_PunchlistWorkLog](PunchlistItemID)

            CREATE INDEX IX_WorkLog_Started
                ON [dbo].[PMA_PunchlistWorkLog](StartedDate)

            PRINT 'Created PMA_PunchlistWorkLog'
        END
    """)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("PMA_PunchlistWorkLog verified / created")


def log_fix_started(item_id, project, item_number, prompt_file):
    """
    Insert a FixStarted row. Returns the new WorkLogID.
    Called when Start Fix launches Claude Code for an item.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO [dbo].[PMA_PunchlistWorkLog]
            (PunchlistItemID, Project, ItemNumber, StartedDate, PromptFile)
        OUTPUT INSERTED.WorkLogID
        VALUES (?, ?, ?, GETDATE(), ?)
    """, (item_id, project, item_number, prompt_file))
    work_log_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"WorkLog {work_log_id} started for item {item_id} ({item_number})")
    return work_log_id


def complete_latest_fix_log(item_id, git_summary=None, work_note=None,
                            outcome='Completed'):
    """
    Complete the most recent open (CompletedDate IS NULL) work-log row for an
    item. If no open row exists - e.g. the item was finished without a Start
    Fix launch - insert a completed row so the history is still captured.

    Returns the WorkLogID that was completed.
    Called by PunchlistCommander when an item is marked Fixed.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TOP 1 WorkLogID
        FROM [dbo].[PMA_PunchlistWorkLog]
        WHERE PunchlistItemID = ? AND CompletedDate IS NULL
        ORDER BY StartedDate DESC
    """, (item_id,))
    row = cursor.fetchone()

    if row:
        work_log_id = row[0]
        cursor.execute("""
            UPDATE [dbo].[PMA_PunchlistWorkLog]
            SET CompletedDate = GETDATE(),
                GitCommitSummary = ?,
                WorkNote = ?,
                Outcome = ?
            WHERE WorkLogID = ?
        """, (git_summary, work_note, outcome, work_log_id))
    else:
        # No Start Fix was logged - record a completed-only row.
        cursor.execute("""
            SELECT Project, ItemNumber FROM [dbo].[PMA_PunchlistItems]
            WHERE PunchlistItemID = ?
        """, (item_id,))
        item_row = cursor.fetchone()
        project = item_row[0] if item_row else '(unknown)'
        item_number = item_row[1] if item_row else None
        cursor.execute("""
            INSERT INTO [dbo].[PMA_PunchlistWorkLog]
                (PunchlistItemID, Project, ItemNumber, StartedDate,
                 CompletedDate, GitCommitSummary, WorkNote, Outcome)
            OUTPUT INSERTED.WorkLogID
            VALUES (?, ?, ?, GETDATE(), GETDATE(), ?, ?, ?)
        """, (item_id, project, item_number, git_summary, work_note, outcome))
        work_log_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"WorkLog {work_log_id} completed for item {item_id} ({outcome})")
    return work_log_id


# -----------------------------------------------------------------------------
# Data access - punchlist context
# -----------------------------------------------------------------------------

def _fetch_all_items():
    """Return every punchlist item as a list of dicts."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT PunchlistItemID, Project, ItemNumber, Title, Description,
               Status, Priority, BlockedBy, Unlocks
        FROM [dbo].[PMA_PunchlistItems]
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            'id': r[0], 'project': r[1], 'item_number': r[2] or '(no #)',
            'title': r[3] or '', 'description': r[4] or '',
            'status': r[5], 'priority': r[6] or 'Medium',
            'blocked_by': r[7] or '', 'unlocks': r[8] or '',
        }
        for r in rows
    ]


def resolve_item(project, item_ref):
    """
    Resolve a punchlist item from a project name plus an item reference,
    which may be an ItemNumber ('PLM-002') or a numeric PunchlistItemID.
    Returns the item dict, or None if not found.
    """
    all_items = _fetch_all_items()
    item_ref = str(item_ref).strip()

    # Numeric id?
    if item_ref.isdigit():
        target_id = int(item_ref)
        for it in all_items:
            if it['id'] == target_id:
                return it

    # Item number within the project (case-insensitive).
    for it in all_items:
        if it['project'] == project and \
                it['item_number'].lower() == item_ref.lower():
            return it
    return None


def gather_punchlist_context(item):
    """
    Build the punchlist neighborhood for an item:
      - the item itself
      - siblings: other open items in the same project
      - cross_refs: open items in OTHER projects that share a blocker with
        this item, or whose title/description references its ItemNumber

    This is the cross-project awareness - it surfaces work that may already
    be handled elsewhere, or that should be coordinated with this fix.
    """
    all_items = _fetch_all_items()

    siblings = [
        i for i in all_items
        if i['project'] == item['project']
        and i['id'] != item['id']
        and i['status'] != 'Completed'
    ]

    item_num = (item['item_number'] or '').lower()
    blocked_by = (item['blocked_by'] or '').lower()
    cross_refs = []
    for other in all_items:
        if other['id'] == item['id'] or other['project'] == item['project']:
            continue
        if other['status'] == 'Completed':
            continue
        other_blocked = (other['blocked_by'] or '').lower()
        other_text = f"{other['title']} {other['description']}".lower()
        shares_blocker = bool(blocked_by) and bool(other_blocked) and \
            (blocked_by in other_blocked or other_blocked in blocked_by)
        references_item = bool(item_num) and item_num != '(no #)' and \
            item_num in other_text
        if shares_blocker or references_item:
            cross_refs.append(other)

    return {'item': item, 'siblings': siblings, 'cross_refs': cross_refs}


# =============================================================================
# PROJECT FOLDER RESOLUTION
# =============================================================================

def get_pycharm_root():
    """
    The PycharmProjects folder - the parent of CRPUtils, where this file
    lives. Every project sits one level up from CRPUtils in its own folder.
    """
    return Path(__file__).resolve().parent.parent


def resolve_project_folder(project):
    """Return the Path to a project's folder, or None if it does not exist."""
    project_dir = get_pycharm_root() / project
    if project_dir.exists() and project_dir.is_dir():
        return project_dir
    return None


# =============================================================================
# STEP 1 - pdoc REBUILD
# =============================================================================

def build_pdoc(project_dir):
    """
    Refresh pdoc_<project>.xml for a project by calling
    GPTProjectUploadGUI.create_project_document (same function PunchlistGUI
    uses). Returns the output path, or None on failure.
    """
    try:
        from GPTProjectUploadGUI import create_project_document
    except ImportError as e:
        logger.error(f"Could not import GPTProjectUploadGUI: {e}")
        return None

    try:
        output_path = create_project_document(
            str(project_dir), prefix="", compress_output=False
        )
        logger.info(f"pdoc refreshed: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"pdoc build failed for {project_dir}: {e}")
        return None


# =============================================================================
# STEP 2 - PROJECT ANALYZER
# =============================================================================

def extract_inventory_sections(analyzer_prompt):
    """
    Strip the synopsis-generation scaffolding from ProjectAnalyzer's prompt,
    keeping only the factual inventory.

    ProjectAnalyzer's prompt has two halves: the inventory (script-by-script,
    cross-project connections, related SSMS) and the '## OUTPUT INSTRUCTIONS'
    section that tells Claude to write a synopsis in a fixed format. Start Fix
    wants the inventory - handing Claude Code the synopsis template would tell
    it to write a synopsis when its job is to fix a punchlist item.

    Keeps everything from '## SCRIPT INVENTORY' up to '## OUTPUT INSTRUCTIONS'.
    """
    if not analyzer_prompt:
        return ""

    start_marker = "## SCRIPT INVENTORY"
    end_marker = "## OUTPUT INSTRUCTIONS"

    start_idx = analyzer_prompt.find(start_marker)
    end_idx = analyzer_prompt.find(end_marker)

    if start_idx == -1:
        # Inventory header not found - fall back to dropping just the tail.
        body = analyzer_prompt
        if end_idx != -1:
            body = analyzer_prompt[:end_idx]
        return body.strip()

    if end_idx == -1 or end_idx < start_idx:
        return analyzer_prompt[start_idx:].strip()

    return analyzer_prompt[start_idx:end_idx].strip()


# =============================================================================
# SYNOPSIS
# =============================================================================

def _newest_code_mtime(project_dir):
    """Return the most recent modification time among .py files in a project."""
    newest = None
    for path in glob.glob(os.path.join(str(project_dir), "**", "*.py"),
                          recursive=True):
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
        except OSError:
            continue
        if newest is None or mtime > newest:
            newest = mtime
    return newest


def read_synopsis(project_dir, project):
    """
    Read <project>_Synopsis.md from the project's own folder.

    Returns a dict:
        {'text': str or None,
         'path': str,
         'exists': bool,
         'is_stale': bool,        # synopsis older than newest .py file
         'synopsis_mtime': datetime or None}
    """
    synopsis_path = os.path.join(str(project_dir), f"{project}_Synopsis.md")
    result = {
        'text': None, 'path': synopsis_path, 'exists': False,
        'is_stale': False, 'synopsis_mtime': None,
    }

    if not os.path.isfile(synopsis_path):
        return result

    result['exists'] = True
    try:
        with open(synopsis_path, 'r', encoding='utf-8', errors='replace') as f:
            result['text'] = f.read()
        result['synopsis_mtime'] = datetime.fromtimestamp(
            os.path.getmtime(synopsis_path))
    except Exception as e:
        logger.error(f"Could not read synopsis {synopsis_path}: {e}")
        return result

    newest_code = _newest_code_mtime(project_dir)
    if newest_code and result['synopsis_mtime'] and \
            newest_code > result['synopsis_mtime']:
        result['is_stale'] = True

    return result


# =============================================================================
# REFERENCE NOTES (loose project .md "bag of clues")
# =============================================================================

def gather_reference_docs(project_dir, item, top_k=6, head_lines=15):
    """
    Rank the loose *.md notes in a project's TOP-LEVEL folder by relevance to
    this punchlist item, and return an annotated menu of pointers.

    A project accumulates conclusion write-ups, specs, drafts and meeting briefs
    (PLM_080_*.md, *_Conclusions.md, *_Architecture.md, ...) that record where
    prior analyses LANDED and why - the intent and gotchas the code alone won't
    tell you. We do NOT embed their bodies (that would blow the brief and bury
    the signal); we hand Claude Code a curated, ranked menu and let it Read the
    ones that bear on the task.

    Scoring (deterministic, no LLM):
      +8  the item's number appears in the filename   (PLM-078 -> '078')
      +2  per item keyword found in the filename
      +1  per item keyword found in the first `head_lines` lines
      +3  the filename looks like a conclusion / spec / draft / findings note

    Keywords are product codes / acronyms / capitalized tokens pulled from the
    item's title + description. <project>_Synopsis.md is skipped (the brief
    references it separately).

    Returns [{'path', 'one_liner', 'score'}] sorted best-first, at most top_k.
    """
    num = re.sub(r'\D', '', item.get('item_number') or '')
    if len(num) < 2:
        num = ''  # too short to match reliably (avoid '2' matching everything)
    text = f"{item.get('title', '')} {item.get('description', '')}"
    keywords = {t.lower() for t in re.findall(r'\b[A-Z0-9]{3,}\b', text)}
    # Drop the project-prefix token (e.g. 'PLM') - it appears in nearly every
    # filename in the folder, so it adds noise instead of discriminating.
    prefix = re.match(r'[A-Za-z]+', item.get('item_number') or '')
    if prefix:
        keywords.discard(prefix.group(0).lower())
    spec_hints = ('_conclusion', '_brief', '_architecture', '_draft',
                  '_readiness', '_spec', '_findings', '_exhibit')

    try:
        candidates = sorted(Path(project_dir).glob('*.md'))
    except OSError:
        return []

    scored = []
    for path in candidates:
        name = path.name.lower()
        if name.endswith('_synopsis.md'):
            continue
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                head = ''.join(f.readline() for _ in range(head_lines))
        except OSError:
            continue
        head_lc = head.lower()

        score = 8 if (num and num in name) else 0
        score += sum(2 for k in keywords if k in name)
        score += sum(1 for k in keywords if k in head_lc)
        if any(h in name for h in spec_hints):
            score += 3
        if score <= 0:
            continue

        first_heading = next(
            (ln.lstrip('#').strip()
             for ln in head.splitlines() if ln.lstrip().startswith('#')),
            path.stem,
        )
        scored.append({
            'path': str(path),
            'one_liner': first_heading[:100],
            'score': score,
        })

    scored.sort(key=lambda d: d['score'], reverse=True)
    return scored[:top_k]


# =============================================================================
# STEP 3 - PROMPT ASSEMBLY
# =============================================================================

def condense_inventory(inventory_text, keep_tables=True):
    """
    Shrink ProjectAnalyzer's SCRIPT INVENTORY section to a purpose digest.

    The raw inventory emits ~7 lines per script (filename, line count, modified
    date, entry-point flag, argparse flag, purpose, table reads/writes, imports).
    For orienting a fix, the purpose line and the entry-point marker are what
    earn their tokens; the rest is either re-derivable from source or noise.

    Per script we KEEP:  the `### filename` heading, the **Entry point** marker,
                         the `- Purpose:` line (and any wrapped continuation),
                         and - if keep_tables - one compact reads/writes line.
    Per script we DROP:  Lines/Modified, the argparse flag, and per-script
                         `Imports from:` (cross-project imports live in the
                         CROSS-PROJECT CONNECTIONS section, which is preserved).

    Every section OTHER than SCRIPT INVENTORY (SQL files, documentation,
    cross-project connections, scheduled jobs, NSSM, SSMS) passes through
    untouched - those are short or carry the cross-project value.
    """
    if not inventory_text:
        return ""

    lines = inventory_text.splitlines()
    out = []

    in_script_section = False   # between '## SCRIPT INVENTORY' and the next '## '
    in_block = False            # inside a single '### filename' block
    keeping_continuation = False  # last kept bullet may wrap onto later lines
    pending_tables = {}         # collect reads/writes to merge into one line

    def flush_tables():
        """Emit the merged compact tables line for the block just closed."""
        if keep_tables and (pending_tables.get("reads") or pending_tables.get("writes")):
            parts = []
            if pending_tables.get("reads"):
                parts.append(f"reads {pending_tables['reads']}")
            if pending_tables.get("writes"):
                parts.append(f"writes {pending_tables['writes']}")
            out.append(f"- Data: {'; '.join(parts)}")
        pending_tables.clear()

    for line in lines:
        stripped = line.strip()

        # --- Section boundaries -------------------------------------------------
        if stripped.startswith("## "):
            if in_block:
                flush_tables()
                in_block = False
            in_script_section = (stripped == "## SCRIPT INVENTORY")
            keeping_continuation = False
            out.append(line)
            continue

        # Outside the script inventory, pass everything through verbatim.
        if not in_script_section:
            out.append(line)
            continue

        # --- Inside SCRIPT INVENTORY -------------------------------------------
        if stripped.startswith("### "):
            # New script block starts; close the previous one's tables line.
            if in_block:
                flush_tables()
            in_block = True
            keeping_continuation = False
            out.append(line)
            continue

        if not in_block:
            # The 'Total: N code files...' summary and blank lines before the
            # first script - keep as-is.
            out.append(line)
            continue

        # Within a script block, decide bullet by bullet.
        if stripped.startswith("- Purpose:"):
            out.append(line)
            keeping_continuation = True
            continue

        if stripped.startswith("- **Entry point**"):
            out.append(line)
            keeping_continuation = False
            continue

        if stripped.startswith("- Reads from:"):
            pending_tables["reads"] = stripped[len("- Reads from:"):].strip()
            keeping_continuation = False
            continue

        if stripped.startswith("- Writes to:"):
            pending_tables["writes"] = stripped[len("- Writes to:"):].strip()
            keeping_continuation = False
            continue

        if stripped.startswith("- "):
            # Lines/Modified, argparse, Imports from: - drop.
            keeping_continuation = False
            continue

        if stripped == "":
            # Blank line ends the block's bullets; emit the tables line, then
            # preserve the blank as a separator.
            if in_block:
                flush_tables()
            keeping_continuation = False
            out.append(line)
            continue

        # A non-bullet, non-blank line: continuation of a multi-line value.
        if keeping_continuation:
            out.append(line)

    if in_block:
        flush_tables()

    # Collapse any runs of 3+ blank lines the trimming may have left behind.
    cleaned = []
    blank_run = 0
    for line in out:
        if line.strip() == "":
            blank_run += 1
            if blank_run <= 1:
                cleaned.append(line)
        else:
            blank_run = 0
            cleaned.append(line)

    return "\n".join(cleaned).strip()


def assemble_start_fix_prompt(context, synopsis, inventory_text,
                              project, project_dir, pdoc_path,
                              blast_radius_text=None, xref_path=None,
                              ref_docs=None):
    """
    Build the Start Fix brief - punchlist neighborhood + condensed inventory,
    with the synopsis referenced on disk rather than embedded.

    context        : dict from gather_punchlist_context()
    synopsis       : dict from read_synopsis()
    inventory_text : str from extract_inventory_sections()
    xref_path      : optional path to the FULL-ROSTER ProjectAnalyzer
                     CrossReference workbook. The per-item Start Fix run is
                     single-project and therefore has NO shared tables, so this
                     must point at the canonical full-roster output (the one
                     that actually contains the "who uses what data" map), or
                     be left None to omit the pointer.
    blast_radius_text : rendered blast-radius markdown for `project` (from
                     blast_radius.render_blast_radius). Embedded inline - it
                     is small and central to the task. None omits the section.
    """
    item = context['item']
    lines = []

    lines.append(f"# Start Fix: {item['item_number']} - {item['title']}")
    lines.append("")
    lines.append(f"Project: **{project}**")
    lines.append(f"Project folder: `{project_dir}`")
    if pdoc_path:
        lines.append(f"Fresh code snapshot: `{pdoc_path}`")
    lines.append("")
    lines.append("You are working in this project's folder. Read the source "
                 "directly as needed.")
    lines.append("")

    # --- The task ---
    lines.append("## The Item To Fix")
    lines.append("")
    lines.append(f"**{item['item_number']}: {item['title']}**")
    lines.append("")
    lines.append(f"- Status: {item['status']}")
    lines.append(f"- Priority: {item['priority']}")
    if item['blocked_by']:
        lines.append(f"- Blocked by: {item['blocked_by']}")
    if item['unlocks']:
        lines.append(f"- Unlocks: {item['unlocks']}")
    lines.append("")
    lines.append("Description:")
    lines.append("")
    desc = (item['description'] or '').strip()
    lines.append(desc if desc else "(no description recorded)")
    lines.append("")

    # --- Siblings ---
    lines.append("## Other Open Items In This Project")
    lines.append("")
    if context['siblings']:
        lines.append("Be aware of these - avoid colliding with related work, "
                     "and flag any that should be handled alongside this one:")
        lines.append("")
        for s in context['siblings']:
            lines.append(f"- **{s['item_number']}** ({s['priority']}, "
                         f"{s['status']}): {s['title']}")
    else:
        lines.append("None - this is the only open item in the project.")
    lines.append("")

    # --- Cross-project references ---
    lines.append("## Related Items In Other Projects")
    lines.append("")
    if context['cross_refs']:
        lines.append("These items in other projects share a blocker with this "
                     "one, or reference it. Check whether any of this work has "
                     "already been done elsewhere before starting:")
        lines.append("")
        for c in context['cross_refs']:
            lines.append(f"- **[{c['project']}] {c['item_number']}** "
                         f"({c['status']}): {c['title']}")
    else:
        lines.append("None detected.")
    lines.append("")

    # --- Project background: synopsis (pointer, not full embed) ---
    lines.append("## Project Background (Synopsis)")
    lines.append("")
    if synopsis['exists'] and synopsis['text']:
        lines.append(f"A project synopsis is on disk at `{synopsis['path']}` "
                     "(in this folder). Read it for project history, "
                     "architecture, and data flow before proposing changes.")
        if synopsis['is_stale']:
            lines.append("")
            lines.append("> Note: this synopsis is older than the newest code "
                         "in the project and may be partially out of date - "
                         "weigh the actual source higher where they disagree.")
    else:
        lines.append("(No synopsis on file. Rely on the inventory below and "
                     "the source code.)")
    lines.append("")

    # --- Reference notes (curated menu of on-disk project .md, pointers) ---
    lines.append("## Reference Notes In This Project")
    lines.append("")
    if ref_docs:
        lines.append("These on-disk notes look relevant to this item - prior "
                     "conclusions, specs, drafts and analyses that record what "
                     "we decided and the gotchas the code alone won't tell you. "
                     "READ the ones that bear on the task before proposing "
                     "changes (they are ranked most-relevant first):")
        lines.append("")
        for d in ref_docs:
            lines.append(f"- `{d['path']}` - {d['one_liner']}")
    else:
        lines.append("(No project notes matched this item.)")
    lines.append("")

    # --- Blast radius (embedded: small, high-value, task-central) ---
    if blast_radius_text:
        lines.append(blast_radius_text.strip())
        lines.append("")

    # --- Cross-project data ownership (pointer to full-roster workbook) ---
    if xref_path:
        lines.append("## Cross-Project Data Ownership")
        lines.append("")
        lines.append("For which projects read/write each shared table - the "
                     "ripple-effect map you can't get from this folder alone - "
                     f"see the **Shared Tables** sheet in `{xref_path}`. "
                     "Consult it before changing any table this project writes.")
        lines.append("")

    # --- Project inventory (condensed purpose digest) ---
    lines.append("## Project Inventory")
    lines.append("")
    condensed = condense_inventory(inventory_text)
    if condensed:
        lines.append("Per-script purpose digest from ProjectAnalyzer. Open the "
                     "actual source for implementation detail; this is for "
                     "orientation - which file does what.")
        lines.append("")
        lines.append(condensed)
    else:
        lines.append("(Inventory unavailable.)")
    lines.append("")

    # --- Instructions ---
    lines.append("## How To Proceed")
    lines.append("")
    lines.append("1. Review the item, the related items, and the background "
                 "above.")
    lines.append("2. Before writing any code, discuss the approach: what "
                 "questions do you have, and how would you suggest we proceed?")
    lines.append("3. Once we agree, implement the fix - keeping class names as "
                 "they are, MS SQL Server 2019 syntax only.")
    lines.append("")

    return "\n".join(lines)


def write_brief_file(text, project, kind):
    """
    Write an assembled brief (or the synopsis prompt) to a transient file in
    TEMP_DIR. 'kind' is a short tag, e.g. 'StartFix' or 'Synopsis'.
    Returns the file path.
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = os.path.normpath(
        os.path.join(TEMP_DIR, f"{project}_{kind}_{timestamp}.md"))
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
    logger.info(f"Brief written: {path}")
    return path


# =============================================================================
# STEP 4 - CLAUDE CODE LAUNCH
# =============================================================================

def _pointer_prompt(brief_path):
    """
    The short instruction handed to the claude CLI. The full brief is too
    large for a Windows command line, so we point Claude Code at the file
    instead - it reads the file itself.
    """
    return (f"Read the task brief at {brief_path} and follow it. "
            f"Discuss your approach before writing any code.")


def claude_available():
    """
    Return the resolved path to the claude executable, or None if it cannot
    be found. shutil.which respects PATHEXT, so it finds claude.cmd / claude.exe
    the same way a shell would - unlike a bare subprocess call.
    """
    return shutil.which(CLAUDE_CMD)


def _find_wt():
    """
    Resolve the path to wt.exe (Windows Terminal), or None if it cannot
    be found.

    Checks the WT_CMD override first, then PATH, then WT_SEARCH_DIRS (both the
    folder itself and one level below it, to catch versioned portable
    extractions like terminal-1.24.11911.0).

    Note on portable installs: wt.exe only lands on PATH after the user logs
    off and back on, so the directory scan is what makes Start Fix work in the
    session where Windows Terminal was first unzipped.
    """
    if WT_CMD:
        return WT_CMD if os.path.isfile(WT_CMD) else None

    found = shutil.which("wt") or shutil.which("wt.exe")
    if found:
        return found

    for base in WT_SEARCH_DIRS:
        if not base or not os.path.isdir(base):
            continue
        direct = os.path.join(base, "wt.exe")
        if os.path.isfile(direct):
            return direct
        try:
            subdirs = sorted(
                (os.path.join(base, d) for d in os.listdir(base)
                 if os.path.isdir(os.path.join(base, d))),
                reverse=True,          # newest versioned folder wins
            )
        except OSError:
            continue
        for sub in subdirs:
            candidate = os.path.join(sub, "wt.exe")
            if os.path.isfile(candidate):
                return candidate
    return None


def _find_conemu():
    """
    Resolve the path to ConEmu64.exe, or None if it cannot be found.

    Checks the CONEMU_CMD override first, then PATH, then the usual install
    locations. ConEmu gives interactive sessions proper copy/paste, resizing
    and scrollback - much nicer than a raw cmd console.
    """
    if CONEMU_CMD:
        return CONEMU_CMD if os.path.isfile(CONEMU_CMD) else None
    found = shutil.which("ConEmu64") or shutil.which("ConEmu64.exe")
    if found:
        return found
    for candidate in (
        r"C:\Program Files\ConEmu\ConEmu64.exe",
        r"C:\Program Files (x86)\ConEmu\ConEmu64.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def launch_claude_code_interactive(project_dir, brief_path):
    """
    Open Claude Code in a new terminal window, in the project folder, seeded
    with a pointer to the brief. Interactive - you review and steer the fix.

    A tiny .bat is written to TEMP_DIR and launched, which avoids Windows
    nested-quoting problems with paths that contain spaces.

    Prefers Windows Terminal (good copy/paste, resizing, scrollback); falls
    back to ConEmu if it is still installed, then a classic cmd console.
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    pointer = _pointer_prompt(brief_path)
    bat_path = os.path.join(
        TEMP_DIR,
        f"launch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bat"
    )
    with open(bat_path, 'w', encoding='utf-8') as f:
        f.write("@echo off\r\n")
        f.write(f'cd /d "{project_dir}"\r\n')
        f.write(f'{CLAUDE_CMD} "{pointer}"\r\n')

    title = "Start Fix - Claude Code"

    # Windows Terminal splits its command line on ';' to start extra tabs, so
    # any semicolon in a path handed to it has to be escaped or the rest of the
    # command is swallowed. TEMP_DIR has none today; this keeps it safe if it
    # ever moves somewhere exotic.
    wt_bat = bat_path.replace(";", "\\;")

    wt = _find_wt()
    conemu = _find_conemu()

    if wt:
        # Windows Terminal: --title names the tab. cmd /k runs the bat and
        # keeps the pane open after Claude Code exits. Passed as an argument
        # list (no shell) so the title quotes correctly without nesting.
        launch = [wt, "--title", title, "cmd", "/k", wt_bat]
        use_shell = False
        terminal = "Windows Terminal"
    elif conemu:
        # ConEmu: -Title names the tab. -run must be LAST - ConEmu treats
        # everything after it as the command line. The bat lives in TEMP_DIR
        # (no spaces), so its path quotes cleanly here; any spaces in
        # project_dir are handled inside the bat by 'cd /d'.
        launch = [conemu, "-Title", title, "-run", "cmd", "/k", bat_path]
        use_shell = False
        terminal = "ConEmu"
    else:
        # Fallback: classic cmd console via 'start' (first quoted token = title).
        # This one needs the shell - 'start' is a cmd builtin, not an exe.
        launch = f'start "{title}" cmd /k "{bat_path}"'
        use_shell = True
        terminal = "cmd"

    # Note: wt.exe is a launcher, not the terminal itself - it hands off to the
    # real window and exits immediately. Do not wait() on this handle or read
    # its return code as a signal that the fix session finished.
    subprocess.Popen(launch, shell=use_shell)
    logger.info(f"Launched interactive Claude Code via {terminal} ({bat_path})")
    return bat_path


def launch_claude_code_headless(project_dir, brief_path):
    """
    Run Claude Code headless ('claude -p') in the project folder, pointed at
    the brief. Used by Refresh Synopsis - Claude Code reads the project and
    writes the synopsis file, then exits.

    On Windows the claude CLI is normally a .cmd shim; a bare subprocess call
    will not find it, so the call is routed through 'cmd /c', which resolves
    it the same way a shell does.

    Returns (returncode, stdout, stderr).
    """
    pointer = _pointer_prompt(brief_path)

    resolved = claude_available()
    if not resolved:
        msg = (f"'{CLAUDE_CMD}' not found on PATH. Install Claude Code, or set "
               f"CLAUDE_CMD at the top of this file to its full path.")
        logger.error(msg)
        return 1, "", msg

    base = [CLAUDE_CMD, "-p", pointer, "--allowedTools", CLAUDE_HEADLESS_TOOLS]
    cmd = ["cmd", "/c"] + base if os.name == "nt" else base

    logger.info(f"Headless Claude Code (resolved: {resolved}) cwd={project_dir}")
    try:
        result = subprocess.run(
            cmd, cwd=str(project_dir),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            timeout=900,        # 15 minutes
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except FileNotFoundError:
        msg = f"Could not launch '{CLAUDE_CMD}'."
        logger.error(msg)
        return 1, "", msg
    except subprocess.TimeoutExpired:
        msg = "Headless Claude Code timed out after 15 minutes."
        logger.error(msg)
        return 1, "", msg


# =============================================================================
# GIT LOG CAPTURE
# =============================================================================

def git_log_delta(project_dir, since_dt, until_dt):
    """
    Return the git commit log for a project folder between two timestamps -
    the work-performed record. Reads local history only (no push, no network,
    no credentials).

    Returns the formatted commit list as a string, or None if the folder is
    not a git repository or git is unavailable.
    """
    since = since_dt.strftime('%Y-%m-%d %H:%M:%S')
    until = until_dt.strftime('%Y-%m-%d %H:%M:%S')
    cmd = [
        "git", "-C", str(project_dir), "log",
        f"--since={since}", f"--until={until}",
        "--date=iso", "--pretty=format:%h  %ad  %an  %s",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            encoding='utf-8', errors='replace', timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.warning(f"git log unavailable for {project_dir}: {e}")
        return None

    if result.returncode != 0:
        # Most commonly: not a git repository.
        logger.info(f"git log returned {result.returncode} for {project_dir}")
        return None

    output = (result.stdout or "").strip()
    return output if output else "(no commits in this window)"


# =============================================================================
# TOP-LEVEL FLOWS
# =============================================================================

def start_fix(project, item_ref, dry_run=False):
    """
    Full Start Fix flow for one punchlist item:
      1. resolve the item and gather its punchlist neighborhood
      2. rebuild the pdoc
      3. run ProjectAnalyzer, strip to the inventory
      4. read the synopsis
      5. assemble the brief and write it to TEMP_DIR
      6. (unless dry_run) launch Claude Code interactively + log FixStarted

    Returns a result dict.
    """
    result = {'ok': False, 'brief_path': None, 'work_log_id': None,
              'message': ''}

    project_dir = resolve_project_folder(project)
    if not project_dir:
        result['message'] = f"Project folder not found for '{project}'."
        logger.error(result['message'])
        return result

    item = resolve_item(project, item_ref)
    if not item:
        result['message'] = (f"Punchlist item '{item_ref}' not found "
                              f"in project '{project}'.")
        logger.error(result['message'])
        return result

    logger.info(f"Start Fix: [{project}] {item['item_number']} - {item['title']}")

    context = gather_punchlist_context(item)
    pdoc_path = build_pdoc(project_dir)
    brief = prepare_start_fix_brief(context, project, project_dir, pdoc_path)
    brief_path = write_brief_file(brief, project, "StartFix")
    result['brief_path'] = brief_path

    if dry_run:
        result['ok'] = True
        result['message'] = f"Dry run - brief written to {brief_path}"
        logger.info(result['message'])
        return result

    # Pre-flight: confirm Claude Code is reachable before launching.
    if not claude_available():
        result['message'] = (
            f"Brief is ready at {brief_path}, but '{CLAUDE_CMD}' was not found "
            f"on PATH. Install Claude Code (or set CLAUDE_CMD), then re-run.")
        logger.error(result['message'])
        return result

    # Log the FixStarted row, then launch.
    try:
        ensure_worklog_table()
        work_log_id = log_fix_started(
            item['id'], project, item['item_number'], brief_path
        )
        result['work_log_id'] = work_log_id
    except Exception as e:
        # A logging failure should not block the actual work.
        logger.error(f"Could not write WorkLog row (non-fatal): {e}")

    launch_claude_code_interactive(project_dir, brief_path)
    result['ok'] = True
    result['message'] = f"Claude Code launched for {item['item_number']}."
    return result


def refresh_synopsis(project):
    """
    Refresh <project>_Synopsis.md:
      1. rebuild the pdoc
      2. run ProjectAnalyzer to get its full synopsis-generation prompt
      3. write that prompt to a brief file
      4. run Claude Code headless - it reads the project and writes
         <project>_Synopsis.md into the project's own folder

    Returns a result dict.
    """
    result = {'ok': False, 'message': '', 'returncode': None}

    project_dir = resolve_project_folder(project)
    if not project_dir:
        result['message'] = f"Project folder not found for '{project}'."
        logger.error(result['message'])
        return result

    logger.info(f"Refresh Synopsis: {project}")

    build_pdoc(project_dir)
    output_dir = get_roster_analysis(force_refresh=True)
    analyzer_prompt = (read_full_project_prompt(output_dir, project)
                       if output_dir else None)
    if not analyzer_prompt:
        result['message'] = "ProjectAnalyzer did not produce a prompt."
        logger.error(result['message'])
        return result

    # The analyzer prompt already instructs Claude to write the synopsis;
    # we hand it through unchanged.
    brief_path = write_brief_file(analyzer_prompt, project, "Synopsis")

    returncode, stdout, stderr = launch_claude_code_headless(
        project_dir, brief_path
    )
    result['returncode'] = returncode

    # Claude Code's -p mode prints its result, and many errors, to stdout
    # rather than stderr - log and surface both so failures are diagnosable.
    if stdout.strip():
        logger.info(f"Claude Code stdout:\n{stdout.strip()}")
    if stderr.strip():
        logger.info(f"Claude Code stderr:\n{stderr.strip()}")

    synopsis_path = os.path.join(str(project_dir), f"{project}_Synopsis.md")
    if returncode == 0 and os.path.isfile(synopsis_path):
        result['ok'] = True
        result['message'] = f"Synopsis written: {synopsis_path}"
    else:
        detail = (stdout.strip() or stderr.strip()
                  or "(no output from Claude Code)")
        result['message'] = (
            f"Headless run finished with code {returncode}. "
            f"Synopsis not confirmed at {synopsis_path}.\n"
            f"Claude Code said: {detail[:600]}")
    logger.info(result['message'])
    return result




WORKBOOK_NAME = "ProjectAnalyzer_CrossReference.xlsx"

# Session cache + lock. Holding the lock across the run means concurrent Start
# Fixes don't each kick off a roster scan and don't race on the output files -
# the first one runs it, the rest wait briefly and reuse the result.
_ROSTER_LOCK = threading.Lock()
_ROSTER_CACHE = {"output_dir": None, "ran_at": None}


def get_roster_analysis(force_refresh=False, max_age_seconds=None):
    """
    Run (or reuse) a full-roster ProjectAnalyzer pass and return its output_dir.

    force_refresh    : ignore the cache and run again.
    max_age_seconds  : if set, a cached run older than this is considered stale
                       and re-run. None means the session cache never expires
                       on its own (refresh on demand via force_refresh).
    """
    with _ROSTER_LOCK:
        cached = _ROSTER_CACHE["output_dir"]
        ran_at = _ROSTER_CACHE["ran_at"]
        fresh = (
                cached
                and os.path.isdir(cached)
                and not force_refresh
                and (max_age_seconds is None
                     or (ran_at and (time.time() - ran_at) < max_age_seconds))
        )
        if fresh:
            logger.info(f"Reusing cached roster analysis: {cached}")
            return cached

        output_dir = _run_full_roster()
        if output_dir:
            _ROSTER_CACHE["output_dir"] = output_dir
            _ROSTER_CACHE["ran_at"] = time.time()
        return output_dir


def _run_full_roster():
    """Run ProjectAnalyzer across every project, all SSMS, no scheduled jobs."""
    try:
        from ProjectAnalyzer import (run_full_analysis, load_config,
                                     discover_projects)
    except ImportError as e:
        logger.error(f"Could not import ProjectAnalyzer: {e}")
        return None

    try:
        config = load_config()
        projects = discover_projects(config["pycharm_root"])
        if not projects:
            logger.error("No projects discovered for roster analysis.")
            return None
        output_dir = run_full_analysis(
            project_folders=projects,
            pycharm_root=config["pycharm_root"],
            ssms_root=config.get("ssms_root"),
            selected_jobs=[],  # SSMS auto-associates by table overlap
            output_root=config.get("output_root"),
        )
        logger.info(f"Roster analysis complete ({len(projects)} projects): "
                    f"{output_dir}")
        return output_dir
    except Exception as e:
        logger.error(f"Roster analysis failed: {e}")
        return None


def _pick_prompt_file(output_dir, project):
    """Find this project's prompt within a full-roster run's Prompts folder."""
    prompts_dir = os.path.join(output_dir, "Prompts")
    if not os.path.isdir(prompts_dir):
        logger.error(f"No Prompts folder in roster output: {prompts_dir}")
        return None
    candidates = glob.glob(os.path.join(prompts_dir, "*.md"))
    for path in candidates:
        if project.lower() in os.path.basename(path).lower():
            return path
    logger.warning(f"No prompt file matched project '{project}' in {prompts_dir}")
    return None


def read_full_project_prompt(output_dir, project):
    """Read this project's UNSTRIPPED prompt (with OUTPUT INSTRUCTIONS) from
    a full-roster run - used by refresh_synopsis to drive synopsis generation.
    Returns the full text, or None."""
    path = _pick_prompt_file(output_dir, project)
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Could not read prompt {path}: {e}")
        return None


def _load_xref(output_dir):
    """Load the persisted cross-project graph. Returns (readers, writers, imports)."""
    path = os.path.join(output_dir, "xref.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (data.get("table_readers", {}),
                data.get("table_writers", {}),
                data.get("import_graph", {}))
    except FileNotFoundError:
        logger.warning(f"xref.json not found in {output_dir} - blast radius "
                       "unavailable (is run_full_analysis patched?).")
        return None
    except Exception as e:
        logger.error(f"Could not read xref.json: {e}")
        return None


def build_project_slice(output_dir, project):
    """
    Slice a full-roster run down to one project's brief inputs.

    Returns a dict:
        {'inventory_text': str,        # from this project's full-roster prompt
         'blast_radius_text': str|None,
         'xref_path': str|None}        # workbook pointer, if present
    """
    result = {"inventory_text": "", "blast_radius_text": None, "xref_path": None}
    if not output_dir or not os.path.isdir(output_dir):
        return result

    # Inventory: the per-project prompt from a full-roster run already has the
    # populated cross-project sections, so the same extractor as before works.
    prompt_path = _pick_prompt_file(output_dir, project)
    if prompt_path:
        try:
            with open(prompt_path, "r", encoding="utf-8", errors="replace") as f:
                prompt_text = f.read()
            result["inventory_text"] = extract_inventory_sections(prompt_text)  # noqa: F821
        except Exception as e:
            logger.error(f"Could not read prompt {prompt_path}: {e}")

    # Blast radius: computed here from the full graph the roster run persisted.
    graph = _load_xref(output_dir)
    if graph:
        readers, writers, imports = graph
        blast = compute_blast_radius(readers, writers, imports, project,
                                     max_hops=2)
        result["blast_radius_text"] = render_blast_radius(blast)

    # Workbook pointer for table-level drill-down.
    wb = os.path.join(output_dir, WORKBOOK_NAME)
    if os.path.isfile(wb):
        result["xref_path"] = wb

    return result


def prepare_start_fix_brief(context, project, project_dir, pdoc_path,
                            force_refresh=False):
    """
    End-to-end: ensure a fresh-enough roster analysis exists, slice this
    project out of it, and assemble the brief. Drop-in for the old sequence
    of run_project_analyzer -> extract -> assemble inside run_start_fix.
    """
    output_dir = get_roster_analysis(force_refresh=force_refresh)
    sl = build_project_slice(output_dir, project) if output_dir else \
        {"inventory_text": "", "blast_radius_text": None, "xref_path": None}

    synopsis = read_synopsis(project_dir, project)  # noqa: F821
    ref_docs = gather_reference_docs(project_dir, context['item'])

    return assemble_start_fix_prompt(  # noqa: F821
        context=context,
        synopsis=synopsis,
        inventory_text=sl["inventory_text"],
        project=project,
        project_dir=project_dir,
        pdoc_path=pdoc_path,
        blast_radius_text=sl["blast_radius_text"],
        xref_path=sl["xref_path"],
        ref_docs=ref_docs,
    )


# =============================================================================
# STANDALONE CLI (for testing without the Commander GUI)
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Start Fix engine - assemble a brief and launch Claude Code"
    )
    parser.add_argument('--project', required=True,
                        help="Project name (folder name under PycharmProjects)")
    parser.add_argument('--item',
                        help="Punchlist item: ItemNumber (e.g. PLM-002) "
                             "or numeric PunchlistItemID")
    parser.add_argument('--refresh-synopsis', action='store_true',
                        help="Regenerate <project>_Synopsis.md instead of "
                             "starting a fix")
    parser.add_argument('--dry-run', action='store_true',
                        help="Assemble and write the brief, but do not launch "
                             "Claude Code")
    args = parser.parse_args()

    if args.refresh_synopsis:
        res = refresh_synopsis(args.project)
        print(res['message'])
        return 0 if res['ok'] else 1

    if not args.item:
        parser.error("--item is required unless --refresh-synopsis is used")

    res = start_fix(args.project, args.item, dry_run=args.dry_run)
    print(res['message'])
    if res['brief_path']:
        print(f"Brief: {res['brief_path']}")
    return 0 if res['ok'] else 1


if __name__ == "__main__":
    sys.exit(main())