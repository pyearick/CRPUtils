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
  2. run_project_analyzer  - run ProjectAnalyzer for one project, get its prompt
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

# The Claude Code CLI command. Override here if 'claude' is not on PATH.
CLAUDE_CMD = "claude"

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

def run_project_analyzer(project):
    """
    Run ProjectAnalyzer for a single project and return its analysis prompt
    text (the script inventory + cross-project connections + related SSMS).

    ProjectAnalyzer is imported and called directly - it lives in CRPUtils
    alongside this file. Returns the prompt text, or None on failure.
    """
    try:
        from ProjectAnalyzer import run_full_analysis, load_config
    except ImportError as e:
        logger.error(f"Could not import ProjectAnalyzer: {e}")
        return None

    try:
        config = load_config()
        output_dir = run_full_analysis(
            project_folders=[project],
            pycharm_root=config['pycharm_root'],
            ssms_root=config.get('ssms_root'),
            selected_jobs=[],            # SSMS auto-associates; jobs left empty
            output_root=config.get('output_root'),
        )
    except Exception as e:
        logger.error(f"ProjectAnalyzer run failed for {project}: {e}")
        return None

    # The per-project prompt lands in <output_dir>/Prompts/.
    prompts_dir = os.path.join(output_dir, "Prompts")
    if not os.path.isdir(prompts_dir):
        logger.error(f"ProjectAnalyzer produced no Prompts folder: {prompts_dir}")
        return None

    candidates = glob.glob(os.path.join(prompts_dir, "*.md"))
    if not candidates:
        logger.error(f"No prompt file found in {prompts_dir}")
        return None

    # Prefer the file whose name contains the project name; else take the
    # only one (single-project run produces a single prompt).
    chosen = None
    for path in candidates:
        if project.lower() in os.path.basename(path).lower():
            chosen = path
            break
    if chosen is None:
        chosen = candidates[0]

    try:
        with open(chosen, 'r', encoding='utf-8', errors='replace') as f:
            text = f.read()
        logger.info(f"ProjectAnalyzer prompt read: {chosen}")
        return text
    except Exception as e:
        logger.error(f"Could not read prompt file {chosen}: {e}")
        return None


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
# STEP 3 - PROMPT ASSEMBLY
# =============================================================================

def assemble_start_fix_prompt(context, synopsis, inventory_text,
                              project, project_dir, pdoc_path):
    """
    Build the Start Fix brief - one markdown document combining the punchlist
    neighborhood, the project synopsis, and the ProjectAnalyzer inventory.

    context        : dict from gather_punchlist_context()
    synopsis       : dict from read_synopsis()
    inventory_text : str from extract_inventory_sections()
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

    # --- Project background: synopsis ---
    lines.append("## Project Background (Synopsis)")
    lines.append("")
    if synopsis['exists'] and synopsis['text']:
        if synopsis['is_stale']:
            lines.append("> Note: this synopsis is older than the newest code "
                         "in the project and may be partially out of date.")
            lines.append("")
        lines.append(synopsis['text'].strip())
    else:
        lines.append("(No synopsis on file. Rely on the inventory below and "
                     "the source code.)")
    lines.append("")

    # --- Project inventory ---
    lines.append("## Project Inventory")
    lines.append("")
    if inventory_text:
        lines.append("Freshly generated by ProjectAnalyzer. The cross-project "
                     "connections in particular are information that cannot be "
                     "derived from this project's folder alone.")
        lines.append("")
        lines.append(inventory_text.strip())
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


def launch_claude_code_interactive(project_dir, brief_path):
    """
    Open Claude Code in a new terminal window, in the project folder, seeded
    with a pointer to the brief. Interactive - you review and steer the fix.

    A tiny .bat is written to TEMP_DIR and launched, which avoids Windows
    nested-quoting problems with paths that contain spaces.
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

    # 'start' opens a new window; first quoted token is the window title.
    subprocess.Popen(
        f'start "Start Fix - Claude Code" cmd /k "{bat_path}"',
        shell=True
    )
    logger.info(f"Launched interactive Claude Code via {bat_path}")
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
    analyzer_prompt = run_project_analyzer(project)
    inventory_text = extract_inventory_sections(analyzer_prompt)
    synopsis = read_synopsis(project_dir, project)

    brief = assemble_start_fix_prompt(
        context, synopsis, inventory_text, project, project_dir, pdoc_path
    )
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
    analyzer_prompt = run_project_analyzer(project)
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