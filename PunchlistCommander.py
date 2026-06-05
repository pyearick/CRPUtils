"""
PunchlistCommander.py - Parallel Work Stream Orchestrator
==========================================================

Browse open punchlist items across all projects, select multiple to work on
in parallel, and launch them in Chrome at the correct Claude project.
Tracks chat URLs per item with full history (including follow-on chats
created when context fills up) and timestamps every open for invoicing
reconstruction.

Tables managed (created on first run if they don't exist):
  - PMA_ProjectClaudeMap          project -> Claude project URL
  - PMA_PunchlistChatLinks        item -> chat URLs, with IsCurrent flag
  - PMA_PunchlistChatAccessLog    timestamped Created/Opened/FollowOnAdded events

Reads from existing PMA_PunchlistItems table.

The Start Fix action is delegated to the start_fix module, which assembles
a context brief and launches Claude Code (and manages its own
PMA_PunchlistWorkLog table).

Lives in: CRPUtils folder
Database: BI-SQL001 / CRPAF

Author: Pat Yearick
Created: May 2026
"""

import os
import sys
import re
import logging
import threading
import webbrowser
import pyodbc
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, scrolledtext
from datetime import datetime

# Start Fix engine - pdoc rebuild, ProjectAnalyzer, Claude Code launch.
# Imported defensively: if it fails to load, the Start Fix button reports
# the problem instead of the whole Commander failing to start.
try:
    import start_fix
    _START_FIX_IMPORT_ERROR = None
except Exception as _e:                      # noqa: BLE001
    start_fix = None
    _START_FIX_IMPORT_ERROR = str(_e)

try:
    import PunchlistQC
    _QC_IMPORT_ERROR = None
except Exception as _e:                      # noqa: BLE001
    PunchlistQC = None
    _QC_IMPORT_ERROR = str(_e)

# =============================================================================
# CONFIGURATION
# =============================================================================

SQL_SERVER = "BI-SQL001"
SQL_DATABASE = "CRPAF"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
LOG_FILE = r"C:/Logs/PunchlistCommander.log"

# URL patterns for validation when capturing from clipboard
CHAT_URL_PATTERN = re.compile(
    r'^https://claude\.ai/chat/[a-f0-9\-]+', re.IGNORECASE
)
PROJECT_URL_PATTERN = re.compile(
    r'^https://claude\.ai/project/[a-f0-9\-]+', re.IGNORECASE
)

# Status values we surface in the tree (Completed is hidden by default)
ACTIVE_STATUSES = ('Open', 'In Progress', 'Blocked')

# Priority sort order
PRIORITY_ORDER = {'High': 0, 'Medium': 1, 'Low': 2}

# Color scheme - matches PunchlistGUI for visual consistency
COLORS = {
    'bg':            '#f5f5f5',
    'header_bg':     '#2c3e50',
    'header_fg':     '#ffffff',
    'btn_primary':   '#3498db',
    'btn_success':   '#27ae60',
    'btn_warning':   '#f39c12',
    'btn_danger':    '#e74c3c',
    'btn_fg':        '#ffffff',
    'high':          '#e74c3c',
    'medium':        '#f39c12',
    'low':           '#27ae60',
    'blocked':       '#9b59b6',
    'in_progress':   '#3498db',
    'open':          '#2c3e50',
    'tree_stripe':   '#ecf0f1',
    'link':          '#1565c0',
    'link_hover':    '#0d47a1',
}

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


def ensure_tables_exist():
    """
    Idempotent schema setup. Safe to call on every startup.
    Creates the three Commander-specific tables if missing.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Project -> Claude project URL
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PMA_ProjectClaudeMap')
        BEGIN
            CREATE TABLE [dbo].[PMA_ProjectClaudeMap] (
                [Project]            NVARCHAR(100) NOT NULL PRIMARY KEY,
                [ClaudeProjectURL]   NVARCHAR(500) NOT NULL,
                [CreatedDate]        DATETIME NOT NULL DEFAULT GETDATE(),
                [LastUsedDate]       DATETIME NOT NULL DEFAULT GETDATE()
            )
            PRINT 'Created PMA_ProjectClaudeMap'
        END
    """)

    # 2. Punchlist item -> chat URLs (history)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PMA_PunchlistChatLinks')
        BEGIN
            CREATE TABLE [dbo].[PMA_PunchlistChatLinks] (
                [ChatLinkID]         INT IDENTITY(1,1) PRIMARY KEY,
                [PunchlistItemID]    INT NOT NULL,
                [ChatURL]            NVARCHAR(500) NOT NULL,
                [ChatLabel]          NVARCHAR(200) NULL,
                [IsCurrent]          BIT NOT NULL DEFAULT 1,
                [CreatedDate]        DATETIME NOT NULL DEFAULT GETDATE(),
                [LastOpenedDate]     DATETIME NULL,
                [Notes]              NVARCHAR(MAX) NULL,
                CONSTRAINT FK_ChatLinks_Item FOREIGN KEY ([PunchlistItemID])
                    REFERENCES [dbo].[PMA_PunchlistItems]([PunchlistItemID])
                    ON DELETE CASCADE
            )

            CREATE INDEX IX_ChatLinks_Item
                ON [dbo].[PMA_PunchlistChatLinks](PunchlistItemID)

            -- Filtered unique index: only one IsCurrent=1 per item allowed
            CREATE UNIQUE INDEX UX_ChatLinks_OneCurrentPerItem
                ON [dbo].[PMA_PunchlistChatLinks](PunchlistItemID)
                WHERE IsCurrent = 1

            PRINT 'Created PMA_PunchlistChatLinks'
        END
    """)

    # 3. Access log (every open is a session marker for invoicing)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PMA_PunchlistChatAccessLog')
        BEGIN
            CREATE TABLE [dbo].[PMA_PunchlistChatAccessLog] (
                [AccessLogID]        INT IDENTITY(1,1) PRIMARY KEY,
                [ChatLinkID]         INT NOT NULL,
                [AccessedDate]       DATETIME NOT NULL DEFAULT GETDATE(),
                [AccessType]         NVARCHAR(20) NOT NULL,
                CONSTRAINT FK_AccessLog_ChatLink FOREIGN KEY ([ChatLinkID])
                    REFERENCES [dbo].[PMA_PunchlistChatLinks]([ChatLinkID])
                    ON DELETE CASCADE
            )

            CREATE INDEX IX_AccessLog_ChatLink
                ON [dbo].[PMA_PunchlistChatAccessLog](ChatLinkID)

            CREATE INDEX IX_AccessLog_Date
                ON [dbo].[PMA_PunchlistChatAccessLog](AccessedDate)

            PRINT 'Created PMA_PunchlistChatAccessLog'
        END
    """)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Schema verified / created")


# -----------------------------------------------------------------------------
# Data access - Punchlist items
# -----------------------------------------------------------------------------

def get_open_items_grouped():
    """
    Return open/in-progress/blocked items grouped by project then priority.

    Shape:
      {
        'BigDawgHunt': {
          'High':   [item_dict, ...],
          'Medium': [...],
          'Low':    [...],
        },
        ...
      }

    Each item_dict carries:
      id, project, item_number, title, description, status, priority,
      blocked_by, modified_date, current_chat_url, chat_count
    """
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        SELECT
            i.PunchlistItemID,
            i.Project,
            i.ItemNumber,
            i.Title,
            i.Description,
            i.Status,
            i.Priority,
            i.BlockedBy,
            i.LastModifiedDate,
            (
                SELECT TOP 1 cl.ChatURL
                FROM [dbo].[PMA_PunchlistChatLinks] cl
                WHERE cl.PunchlistItemID = i.PunchlistItemID
                  AND cl.IsCurrent = 1
            ) AS CurrentChatURL,
            (
                SELECT COUNT(*)
                FROM [dbo].[PMA_PunchlistChatLinks] cl
                WHERE cl.PunchlistItemID = i.PunchlistItemID
            ) AS ChatCount
        FROM [dbo].[PMA_PunchlistItems] i
        WHERE i.Status IN ('Open', 'In Progress', 'Blocked')
        ORDER BY i.Project, i.ItemNumber
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    grouped = {}
    for r in rows:
        item = {
            'id': r[0],
            'project': r[1],
            'item_number': r[2] or '(no #)',
            'title': r[3],
            'description': r[4] or '',
            'status': r[5],
            'priority': r[6] or 'Medium',
            'blocked_by': r[7],
            'modified_date': r[8],
            'current_chat_url': r[9],
            'chat_count': r[10] or 0,
        }
        proj = item['project']
        pri = item['priority'] if item['priority'] in PRIORITY_ORDER else 'Medium'
        grouped.setdefault(proj, {}).setdefault(pri, []).append(item)

    return grouped


def update_item_status(item_id, new_status):
    """Update the Status column for a punchlist item."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE [dbo].[PMA_PunchlistItems]
        SET Status = ?, LastModifiedDate = GETDATE()
        WHERE PunchlistItemID = ?
    """, (new_status, item_id))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Item {item_id} status -> {new_status}")


# -----------------------------------------------------------------------------
# Data access - Project Claude URLs
# -----------------------------------------------------------------------------

def get_project_claude_url(project):
    """Return saved Claude project URL for a project, or None."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ClaudeProjectURL FROM [dbo].[PMA_ProjectClaudeMap]
        WHERE Project = ?
    """, (project,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def save_project_claude_url(project, url):
    """Upsert a project -> Claude project URL mapping."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        IF EXISTS (SELECT 1 FROM [dbo].[PMA_ProjectClaudeMap] WHERE Project = ?)
            UPDATE [dbo].[PMA_ProjectClaudeMap]
            SET ClaudeProjectURL = ?, LastUsedDate = GETDATE()
            WHERE Project = ?
        ELSE
            INSERT INTO [dbo].[PMA_ProjectClaudeMap]
                (Project, ClaudeProjectURL, CreatedDate, LastUsedDate)
            VALUES (?, ?, GETDATE(), GETDATE())
    """, (project, url, project, project, url))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved Claude project URL for {project}")


def touch_project_url(project):
    """Bump LastUsedDate when a project URL is opened."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE [dbo].[PMA_ProjectClaudeMap]
        SET LastUsedDate = GETDATE()
        WHERE Project = ?
    """, (project,))
    conn.commit()
    cursor.close()
    conn.close()


# -----------------------------------------------------------------------------
# Data access - Chat links
# -----------------------------------------------------------------------------

def get_current_chat_link(item_id):
    """Return the current chat link row for an item, or None."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ChatLinkID, ChatURL, ChatLabel, CreatedDate, LastOpenedDate
        FROM [dbo].[PMA_PunchlistChatLinks]
        WHERE PunchlistItemID = ? AND IsCurrent = 1
    """, (item_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return None
    return {
        'id': row[0],
        'url': row[1],
        'label': row[2],
        'created': row[3],
        'last_opened': row[4],
    }


def get_chat_history(item_id):
    """
    Return all chat links for an item (most recent first), with access counts.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            cl.ChatLinkID,
            cl.ChatURL,
            cl.ChatLabel,
            cl.IsCurrent,
            cl.CreatedDate,
            cl.LastOpenedDate,
            (SELECT COUNT(*) FROM [dbo].[PMA_PunchlistChatAccessLog] al
             WHERE al.ChatLinkID = cl.ChatLinkID AND al.AccessType = 'Opened')
                AS OpenCount
        FROM [dbo].[PMA_PunchlistChatLinks] cl
        WHERE cl.PunchlistItemID = ?
        ORDER BY cl.IsCurrent DESC, cl.CreatedDate DESC
    """, (item_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            'id': r[0], 'url': r[1], 'label': r[2],
            'is_current': bool(r[3]), 'created': r[4],
            'last_opened': r[5], 'open_count': r[6] or 0,
        }
        for r in rows
    ]


def save_chat_link(item_id, url, label=None, is_followon=False):
    """
    Insert a new chat link. If is_followon=True, flip any existing current
    link for this item to IsCurrent=0 first (in a single transaction).
    Logs a 'Created' event in the access log.
    Returns the new ChatLinkID.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if is_followon:
            cursor.execute("""
                UPDATE [dbo].[PMA_PunchlistChatLinks]
                SET IsCurrent = 0
                WHERE PunchlistItemID = ? AND IsCurrent = 1
            """, (item_id,))

        # Insert as current
        cursor.execute("""
            INSERT INTO [dbo].[PMA_PunchlistChatLinks]
                (PunchlistItemID, ChatURL, ChatLabel, IsCurrent,
                 CreatedDate, LastOpenedDate)
            OUTPUT INSERTED.ChatLinkID
            VALUES (?, ?, ?, 1, GETDATE(), GETDATE())
        """, (item_id, url, label))
        new_id = cursor.fetchone()[0]

        # Log creation event
        access_type = 'FollowOnAdded' if is_followon else 'Created'
        cursor.execute("""
            INSERT INTO [dbo].[PMA_PunchlistChatAccessLog]
                (ChatLinkID, AccessedDate, AccessType)
            VALUES (?, GETDATE(), ?)
        """, (new_id, access_type))

        conn.commit()
        logger.info(f"Saved chat link {new_id} for item {item_id} (followon={is_followon})")
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def log_chat_access(chat_link_id, access_type='Opened'):
    """Record an access event and bump LastOpenedDate."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO [dbo].[PMA_PunchlistChatAccessLog]
            (ChatLinkID, AccessedDate, AccessType)
        VALUES (?, GETDATE(), ?)
    """, (chat_link_id, access_type))
    if access_type == 'Opened':
        cursor.execute("""
            UPDATE [dbo].[PMA_PunchlistChatLinks]
            SET LastOpenedDate = GETDATE()
            WHERE ChatLinkID = ?
        """, (chat_link_id,))
    conn.commit()
    cursor.close()
    conn.close()


def update_chat_link(chat_link_id, url=None, label=None):
    """Update URL or label on an existing chat link."""
    if url is None and label is None:
        return
    sets = []
    params = []
    if url is not None:
        sets.append("ChatURL = ?")
        params.append(url)
    if label is not None:
        sets.append("ChatLabel = ?")
        params.append(label)
    params.append(chat_link_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        UPDATE [dbo].[PMA_PunchlistChatLinks]
        SET {', '.join(sets)}
        WHERE ChatLinkID = ?
    """, params)
    conn.commit()
    cursor.close()
    conn.close()


def delete_chat_link(chat_link_id):
    """Hard delete a chat link (and its access log via cascade)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM [dbo].[PMA_PunchlistChatLinks]
        WHERE ChatLinkID = ?
    """, (chat_link_id,))
    conn.commit()
    cursor.close()
    conn.close()


# =============================================================================
# RESUME / START PROMPT BUILDER
# =============================================================================

def build_prompt_for_item(item, has_history=False, is_followon=False):
    """
    Build a prompt to paste into Claude for this item.

    - First chat (no history):     start prompt
    - Resume (has current chat):   resume prompt
    - Follow-on (after overflow):  continuation prompt
    """
    lines = []
    if is_followon:
        lines.append(
            f"This is a continuation of work on punchlist item "
            f"{item['item_number']}: {item['title']}."
        )
        lines.append(
            "The previous chat hit context limits. You won't have access to "
            "that history, so we're starting fresh in this chat."
        )
    elif has_history:
        lines.append(
            f"I'm resuming work on punchlist item "
            f"{item['item_number']}: {item['title']}."
        )
    else:
        lines.append(
            f"I'm starting work on punchlist item "
            f"{item['item_number']}: {item['title']}."
        )

    lines.append("")
    lines.append("Original description:")
    desc = (item.get('description') or '').strip()
    lines.append(desc if desc else "(no description recorded)")
    lines.append("")
    lines.append(f"Current status: {item['status']}")
    lines.append(f"Priority: {item['priority']}")
    if item.get('blocked_by'):
        lines.append(f"Currently blocked by: {item['blocked_by']}")
    lines.append("")

    if has_history and not is_followon:
        lines.append("Please review our prior conversation in this chat and tell me:")
        lines.append("1. What progress have we made toward closing this item?")
        lines.append("2. What's the next concrete step?")
        lines.append("3. Any new blockers or open questions you've identified?")
    elif is_followon:
        lines.append(
            "Help me pick up where we left off. Suggest the next concrete step "
            "based on the description above."
        )
    else:
        lines.append(
            "Before writing any code, let's discuss the approach. "
            "What questions do you have about the requirement, "
            "and how would you suggest we proceed?"
        )
    return "\n".join(lines)


# =============================================================================
# GUI HELPERS
# =============================================================================

class HyperlinkLabel(tk.Label):
    """A clickable label that opens a URL in the default browser."""

    def __init__(self, master, url, text=None, max_chars=70, **kwargs):
        display = text if text else url
        if len(display) > max_chars:
            display = display[:max_chars - 3] + '...'
        super().__init__(
            master,
            text=display,
            fg=COLORS['link'],
            cursor='hand2',
            font=('Segoe UI', 9, 'underline'),
            bg=kwargs.pop('bg', COLORS['bg']),
            **kwargs
        )
        self._url = url
        self.bind('<Button-1>', self._on_click)
        self.bind('<Enter>', lambda e: self.config(fg=COLORS['link_hover']))
        self.bind('<Leave>', lambda e: self.config(fg=COLORS['link']))

    def _on_click(self, _event):
        webbrowser.open(self._url)


# =============================================================================
# MAIN GUI
# =============================================================================

class PunchlistCommander(tk.Tk):
    """Main window for the parallel work stream orchestrator."""

    def __init__(self):
        super().__init__()
        self.title("Punchlist Commander")
        self.geometry("1400x800")
        self.configure(bg=COLORS['bg'])

        # State
        self.grouped_items = {}              # from get_open_items_grouped()
        self.checked_item_ids = set()        # set of int IDs currently checked
        self.tree_node_to_item = {}          # tree iid -> item dict
        self.current_detail_item = None      # item shown in detail pane
        self._start_fix_busy = False         # guards against concurrent runs
        self._btn_start_fix = None           # detail-pane Start Fix button ref

        # Build UI
        self._build_toolbar()
        self._build_main_pane()
        self._build_status_bar()

        # Initial load
        self.refresh_tree()

    # -------------------------------------------------------------------------
    # UI construction
    # -------------------------------------------------------------------------

    def _build_toolbar(self):
        bar = tk.Frame(self, bg=COLORS['header_bg'], height=50)
        bar.pack(side=tk.TOP, fill=tk.X)
        bar.pack_propagate(False)

        title = tk.Label(
            bar, text="  ⚓ Punchlist Commander",
            font=('Segoe UI', 14, 'bold'),
            bg=COLORS['header_bg'], fg=COLORS['header_fg']
        )
        title.pack(side=tk.LEFT, padx=10)

        # Right-side buttons
        btn_settings = tk.Button(
            bar, text="⚙ Project URLs", command=self.manage_project_urls,
            bg=COLORS['header_bg'], fg=COLORS['header_fg'],
            activebackground='#34495e', activeforeground=COLORS['header_fg'],
            bd=0, padx=10, font=('Segoe UI', 9)
        )
        btn_settings.pack(side=tk.RIGHT, padx=5, pady=8)

        btn_refresh = tk.Button(
            bar, text="🗘 Refresh", command=self.refresh_tree,
            bg=COLORS['btn_primary'], fg=COLORS['btn_fg'],
            activebackground='#2980b9', bd=0, padx=10, pady=4,
            font=('Segoe UI', 9, 'bold')
        )
        btn_refresh.pack(side=tk.RIGHT, padx=5, pady=8)

        btn_qc = tk.Button(
            bar, text="🔍 QC", command=self._run_qc_report,
            bg=COLORS['btn_warning'], fg=COLORS['btn_fg'],
            activebackground='#d68910', bd=0, padx=10, pady=4,
            font=('Segoe UI', 9, 'bold')
        )
        btn_qc.pack(side=tk.RIGHT, padx=5, pady=8)

        self.btn_launch = tk.Button(
            bar, text="🚀 Launch Selected (0)",
            command=self.launch_selected,
            bg=COLORS['btn_success'], fg=COLORS['btn_fg'],
            activebackground='#229954', bd=0, padx=15, pady=4,
            font=('Segoe UI', 10, 'bold'),
            state=tk.DISABLED
        )
        self.btn_launch.pack(side=tk.RIGHT, padx=5, pady=8)

    def _build_main_pane(self):
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Left: tree
        left = tk.Frame(paned, bg=COLORS['bg'])
        paned.add(left, weight=3)

        self.tree = ttk.Treeview(
            left,
            columns=('priority', 'status', 'chats', 'modified'),
            show='tree headings',
            selectmode='browse'    # single-row "active" select; checks are separate
        )
        self.tree.heading('#0', text='Project / Priority / Item')
        self.tree.heading('priority', text='Priority')
        self.tree.heading('status', text='Status')
        self.tree.heading('chats', text='Chats')
        self.tree.heading('modified', text='Last Modified')

        self.tree.column('#0', width=550, anchor='w')
        self.tree.column('priority', width=80, anchor='center')
        self.tree.column('status', width=100, anchor='center')
        self.tree.column('chats', width=60, anchor='center')
        self.tree.column('modified', width=140, anchor='center')

        # Tag styling
        self.tree.tag_configure('high', foreground=COLORS['high'])
        self.tree.tag_configure('medium', foreground=COLORS['medium'])
        self.tree.tag_configure('low', foreground=COLORS['low'])
        self.tree.tag_configure('blocked', foreground=COLORS['blocked'])
        self.tree.tag_configure('group', font=('Segoe UI', 10, 'bold'))

        sb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind('<Button-1>', self._on_tree_click)
        self.tree.bind('<<TreeviewSelect>>', self._on_tree_select)
        self.tree.bind('<Button-3>', self._on_tree_right_click)

        # Right: detail pane
        right = tk.Frame(paned, bg=COLORS['bg'])
        paned.add(right, weight=2)
        self._build_detail_pane(right)

        # Right-click context menu (built once, populated dynamically)
        self.context_menu = tk.Menu(self, tearoff=0)

    def _build_detail_pane(self, parent):
        """Right-side detail pane shown for the currently selected item."""
        # Container frame
        self.detail_frame = tk.Frame(parent, bg=COLORS['bg'])
        self.detail_frame.pack(fill=tk.BOTH, expand=True)

        self._render_empty_detail()

    def _render_empty_detail(self):
        for w in self.detail_frame.winfo_children():
            w.destroy()
        lbl = tk.Label(
            self.detail_frame,
            text="\n\n\nSelect an item to see details.\n\n"
                 "Tip: click the ☐/☑ in the tree to check items;\n"
                 "click anywhere else on a row to view it here.",
            bg=COLORS['bg'], fg='#888', font=('Segoe UI', 10),
            justify=tk.LEFT
        )
        lbl.pack(padx=20, pady=20, anchor='nw')

    def _render_detail(self, item):
        """Render the detail pane for a given item."""
        for w in self.detail_frame.winfo_children():
            w.destroy()

        # --- Header ---
        header = tk.Frame(self.detail_frame, bg=COLORS['bg'])
        header.pack(fill=tk.X, padx=12, pady=(12, 4))

        tk.Label(
            header,
            text=f"{item['item_number']}  |  {item['project']}",
            bg=COLORS['bg'], fg='#666', font=('Segoe UI', 9)
        ).pack(anchor='w')

        tk.Label(
            header, text=item['title'],
            bg=COLORS['bg'], font=('Segoe UI', 12, 'bold'),
            wraplength=500, justify=tk.LEFT
        ).pack(anchor='w', pady=(2, 0))

        # --- Metadata row ---
        meta = tk.Frame(self.detail_frame, bg=COLORS['bg'])
        meta.pack(fill=tk.X, padx=12, pady=4)
        pri_color = COLORS.get(item['priority'].lower(), COLORS['medium'])
        tk.Label(meta, text=f"Priority: {item['priority']}",
                 bg=COLORS['bg'], fg=pri_color,
                 font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT)
        tk.Label(meta, text=f"   Status: {item['status']}",
                 bg=COLORS['bg'], font=('Segoe UI', 9)).pack(side=tk.LEFT)
        if item.get('blocked_by'):
            tk.Label(meta, text=f"   ⛔ Blocked by: {item['blocked_by']}",
                     bg=COLORS['bg'], fg=COLORS['blocked'],
                     font=('Segoe UI', 9, 'italic')).pack(side=tk.LEFT)

        # --- Description ---
        desc_lbl = tk.Label(self.detail_frame, text="Description:",
                            bg=COLORS['bg'], font=('Segoe UI', 9, 'bold'))
        desc_lbl.pack(anchor='w', padx=12, pady=(8, 2))
        desc = scrolledtext.ScrolledText(
            self.detail_frame, height=8, wrap=tk.WORD,
            font=('Segoe UI', 9), bg='white'
        )
        desc.insert('1.0', item.get('description') or '(no description)')
        desc.config(state=tk.DISABLED)
        desc.pack(fill=tk.X, padx=12, pady=2)

        # --- Chat section ---
        chat_section = tk.LabelFrame(
            self.detail_frame, text="Claude Chat",
            bg=COLORS['bg'], font=('Segoe UI', 9, 'bold'), padx=8, pady=6
        )
        chat_section.pack(fill=tk.X, padx=12, pady=8)

        current = get_current_chat_link(item['id'])
        history = get_chat_history(item['id'])

        if current:
            tk.Label(chat_section, text="Current chat:",
                     bg=COLORS['bg'], font=('Segoe UI', 9)).pack(anchor='w')
            HyperlinkLabel(chat_section, url=current['url']).pack(anchor='w', pady=(0, 4))
            stats = (
                f"Created {current['created'].strftime('%Y-%m-%d %H:%M') if current['created'] else '—'}"
            )
            if current.get('last_opened'):
                stats += f"  •  Last opened {current['last_opened'].strftime('%Y-%m-%d %H:%M')}"
            stats += f"  •  {len(history)} chat(s) total"
            tk.Label(chat_section, text=stats, bg=COLORS['bg'],
                     fg='#666', font=('Segoe UI', 8)).pack(anchor='w')
        else:
            tk.Label(chat_section, text="No chat URL captured yet.",
                     bg=COLORS['bg'], fg='#888',
                     font=('Segoe UI', 9, 'italic')).pack(anchor='w', pady=4)

        # --- URL capture row ---
        cap_frame = tk.Frame(chat_section, bg=COLORS['bg'])
        cap_frame.pack(fill=tk.X, pady=(8, 0))

        self.detail_url_var = tk.StringVar()
        url_entry = tk.Entry(cap_frame, textvariable=self.detail_url_var,
                             font=('Consolas', 9), width=60)
        url_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))

        tk.Button(
            cap_frame, text="📋 From clipboard",
            command=lambda: self._fill_url_from_clipboard(self.detail_url_var),
            bg=COLORS['btn_primary'], fg=COLORS['btn_fg'], bd=0, padx=8,
            font=('Segoe UI', 8)
        ).pack(side=tk.LEFT, padx=2)

        save_text = "💾 Save URL" if not current else "💾 Replace URL"
        tk.Button(
            cap_frame, text=save_text,
            command=lambda: self._save_captured_url(item, is_followon=False),
            bg=COLORS['btn_success'], fg=COLORS['btn_fg'], bd=0, padx=8,
            font=('Segoe UI', 8)
        ).pack(side=tk.LEFT, padx=2)

        if current:
            tk.Button(
                cap_frame, text="➕ Follow-on",
                command=lambda: self._save_captured_url(item, is_followon=True),
                bg=COLORS['btn_warning'], fg=COLORS['btn_fg'], bd=0, padx=8,
                font=('Segoe UI', 8)
            ).pack(side=tk.LEFT, padx=2)

        # --- Action buttons ---
        actions = tk.Frame(self.detail_frame, bg=COLORS['bg'])
        actions.pack(fill=tk.X, padx=12, pady=(4, 8))

        # Start Fix - assemble a context brief and launch Claude Code.
        self._btn_start_fix = tk.Button(
            actions, text="🔧 Start Fix",
            command=lambda: self._start_fix(item),
            bg=COLORS['btn_success'], fg=COLORS['btn_fg'], bd=0,
            padx=10, pady=4, font=('Segoe UI', 9, 'bold')
        )
        self._btn_start_fix.pack(side=tk.LEFT, padx=2)
        if self._start_fix_busy:
            # A run is in flight (detail pane re-rendered mid-run) - keep
            # the button locked so it reflects the true state.
            self._btn_start_fix.config(state=tk.DISABLED, text="⏳ Start Fix…")

        tk.Button(
            actions, text="📋 Copy resume prompt",
            command=lambda: self._copy_prompt(item, is_followon=False),
            bg=COLORS['btn_primary'], fg=COLORS['btn_fg'], bd=0, padx=10, pady=4,
            font=('Segoe UI', 9)
        ).pack(side=tk.LEFT, padx=2)

        if current:
            tk.Button(
                actions, text="📋 Copy follow-on prompt",
                command=lambda: self._copy_prompt(item, is_followon=True),
                bg=COLORS['btn_primary'], fg=COLORS['btn_fg'], bd=0, padx=10, pady=4,
                font=('Segoe UI', 9)
            ).pack(side=tk.LEFT, padx=2)

        tk.Button(
            actions, text="📜 View history",
            command=lambda: self.view_chat_history(item),
            bg=COLORS['btn_primary'], fg=COLORS['btn_fg'], bd=0, padx=10, pady=4,
            font=('Segoe UI', 9)
        ).pack(side=tk.LEFT, padx=2)

        tk.Button(
            actions, text="✓ Mark completed",
            command=lambda: self._mark_completed(item),
            bg=COLORS['btn_warning'], fg=COLORS['btn_fg'], bd=0, padx=10, pady=4,
            font=('Segoe UI', 9)
        ).pack(side=tk.LEFT, padx=2)

    def _build_status_bar(self):
        self.status_var = tk.StringVar(value="Ready.")
        bar = tk.Label(
            self, textvariable=self.status_var, anchor='w',
            bg='#34495e', fg='white', font=('Segoe UI', 9), padx=8
        )
        bar.pack(side=tk.BOTTOM, fill=tk.X)

    # -------------------------------------------------------------------------
    # Tree population
    # -------------------------------------------------------------------------

    def refresh_tree(self):
        """Reload from SQL and rebuild the tree."""
        try:
            self.grouped_items = get_open_items_grouped()
        except Exception as e:
            logger.exception("Failed to load punchlist items")
            messagebox.showerror("Database error",
                                 f"Could not load items:\n{e}")
            return

        # Preserve check state across refresh by item_id
        existing_checked = set(self.checked_item_ids)

        # Clear
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self.tree_node_to_item.clear()

        total_items = 0
        for project in sorted(self.grouped_items.keys(), key=str.lower):
            pri_groups = self.grouped_items[project]
            project_count = sum(len(v) for v in pri_groups.values())
            total_items += project_count

            proj_node = self.tree.insert(
                '', 'end',
                text=f"📁 {project}  ({project_count})",
                values=('', '', '', ''),
                tags=('group',),
                open=True
            )

            for pri in ('High', 'Medium', 'Low'):
                items = pri_groups.get(pri, [])
                if not items:
                    continue
                pri_icon = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}[pri]
                pri_node = self.tree.insert(
                    proj_node, 'end',
                    text=f"{pri_icon} {pri}  ({len(items)})",
                    values=('', '', '', ''),
                    tags=('group', pri.lower()),
                    open=True
                )
                for item in items:
                    check = '☑' if item['id'] in existing_checked else '☐'
                    chat_marker = ''
                    if item['chat_count'] > 0:
                        chat_marker = f"💬{item['chat_count']}" if item['chat_count'] > 1 else "💬"
                    text = f"  {check}  {item['item_number']} — {item['title']}"
                    pri_tag = pri.lower()
                    status_tag = 'blocked' if item['status'] == 'Blocked' else pri_tag
                    iid = self.tree.insert(
                        pri_node, 'end',
                        text=text,
                        values=(
                            item['priority'],
                            item['status'],
                            chat_marker,
                            item['modified_date'].strftime('%Y-%m-%d %H:%M')
                                if item['modified_date'] else ''
                        ),
                        tags=(status_tag,)
                    )
                    self.tree_node_to_item[iid] = item

        # Sync checked set to only IDs still present
        present_ids = {it['id'] for it in self.tree_node_to_item.values()}
        self.checked_item_ids = {i for i in existing_checked if i in present_ids}

        self._update_launch_button()
        self.status_var.set(
            f"Loaded {total_items} active item(s) across "
            f"{len(self.grouped_items)} project(s)."
        )

    # -------------------------------------------------------------------------
    # Tree event handlers
    # -------------------------------------------------------------------------

    def _on_tree_click(self, event):
        """
        Click handling:
         - On the tree text column (#0), if click is in the leading checkbox
           glyph region, toggle the check state.
         - Anything else falls through to default selection.
        """
        region = self.tree.identify('region', event.x, event.y)
        if region != 'tree':
            return  # not in the tree column
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        item = self.tree_node_to_item.get(iid)
        if not item:
            return  # group row, no checkbox

        # We placed "  ☐  " or "  ☑  " at the very start. The first ~30px
        # is roughly where the glyph sits. We're permissive: any click in
        # the first 40px of the row counts as a toggle.
        bbox = self.tree.bbox(iid, '#0')
        if not bbox:
            return
        x_in_cell = event.x - bbox[0]
        if x_in_cell <= 40:
            self._toggle_item_check(iid, item)
            return 'break'  # prevent default selection on toggle clicks

    def _toggle_item_check(self, iid, item):
        if item['id'] in self.checked_item_ids:
            self.checked_item_ids.discard(item['id'])
            new_check = '☐'
        else:
            self.checked_item_ids.add(item['id'])
            new_check = '☑'
        # Rewrite the row text
        existing_text = self.tree.item(iid, 'text')
        # Replace the first occurrence of ☐ or ☑
        new_text = re.sub(r'[☐☑]', new_check, existing_text, count=1)
        self.tree.item(iid, text=new_text)
        self._update_launch_button()

    def _update_launch_button(self):
        n = len(self.checked_item_ids)
        self.btn_launch.config(
            text=f"🚀 Launch Selected ({n})",
            state=tk.NORMAL if n else tk.DISABLED
        )

    def _on_tree_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        item = self.tree_node_to_item.get(iid)
        if item:
            self.current_detail_item = item
            self._render_detail(item)

    def _on_tree_right_click(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        item = self.tree_node_to_item.get(iid)
        if not item:
            return
        self.tree.selection_set(iid)
        self.current_detail_item = item
        self._render_detail(item)

        # Build context menu fresh
        self.context_menu.delete(0, tk.END)
        self.context_menu.add_command(
            label="🚀 Launch this item",
            command=lambda: self._launch_one(item)
        )
        self.context_menu.add_command(
            label="🔧 Start Fix (Claude Code)",
            command=lambda: self._start_fix(item)
        )
        self.context_menu.add_separator()
        current = get_current_chat_link(item['id'])
        self.context_menu.add_command(
            label="📋 Copy resume prompt",
            command=lambda: self._copy_prompt(item, is_followon=False)
        )
        if current:
            self.context_menu.add_command(
                label="📋 Copy follow-on prompt",
                command=lambda: self._copy_prompt(item, is_followon=True)
            )
            self.context_menu.add_command(
                label="➕ Add follow-on chat (after context overflow)",
                command=lambda: self._save_captured_url(item, is_followon=True,
                                                       prompt_for_url=True)
            )
        self.context_menu.add_separator()
        self.context_menu.add_command(
            label="📜 View chat history",
            command=lambda: self.view_chat_history(item)
        )
        self.context_menu.add_separator()
        self.context_menu.add_command(
            label="✓ Mark completed",
            command=lambda: self._mark_completed(item)
        )
        self.context_menu.tk_popup(event.x_root, event.y_root)

    # -------------------------------------------------------------------------
    # Project URL setup
    # -------------------------------------------------------------------------

    def manage_project_urls(self):
        """Open a window listing all projects with their Claude URLs (editable)."""
        win = tk.Toplevel(self)
        win.title("Project Claude URL Mappings")
        win.geometry("800x500")
        win.configure(bg=COLORS['bg'])

        tk.Label(
            win, text="Claude project URLs per CRPAF project",
            bg=COLORS['bg'], font=('Segoe UI', 11, 'bold')
        ).pack(pady=(10, 4))
        tk.Label(
            win, text="Format: https://claude.ai/project/<uuid>",
            bg=COLORS['bg'], fg='#666', font=('Segoe UI', 9, 'italic')
        ).pack()

        frame = tk.Frame(win, bg=COLORS['bg'])
        frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)

        tree = ttk.Treeview(frame, columns=('url', 'last_used'),
                            show='tree headings')
        tree.heading('#0', text='Project')
        tree.heading('url', text='Claude project URL')
        tree.heading('last_used', text='Last used')
        tree.column('#0', width=180)
        tree.column('url', width=400)
        tree.column('last_used', width=140, anchor='center')
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        sb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate: every project in grouped_items + every saved mapping
        all_projects = set(self.grouped_items.keys())
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Project, ClaudeProjectURL, LastUsedDate
            FROM [dbo].[PMA_ProjectClaudeMap]
        """)
        saved = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        all_projects |= set(saved.keys())

        for proj in sorted(all_projects, key=str.lower):
            url, last = saved.get(proj, (None, None))
            tree.insert('', 'end', iid=proj, text=proj,
                        values=(url or '(not set)',
                                last.strftime('%Y-%m-%d %H:%M') if last else '—'))

        def on_double_click(_event):
            sel = tree.selection()
            if not sel:
                return
            proj = sel[0]
            current_url = saved.get(proj, (None, None))[0] or ''
            new_url = simpledialog.askstring(
                "Set Claude project URL",
                f"Claude project URL for {proj}:",
                initialvalue=current_url, parent=win
            )
            if new_url is None:
                return
            new_url = new_url.strip()
            if new_url and not PROJECT_URL_PATTERN.match(new_url):
                messagebox.showwarning(
                    "Invalid URL",
                    "Expected format: https://claude.ai/project/<uuid>",
                    parent=win
                )
                return
            if new_url:
                save_project_claude_url(proj, new_url)
                saved[proj] = (new_url, datetime.now())
                tree.item(proj, values=(new_url, datetime.now()
                                        .strftime('%Y-%m-%d %H:%M')))

        tree.bind('<Double-1>', on_double_click)

        tk.Label(
            win, text="Double-click a row to edit. "
                     "URL is required before launching items in that project.",
            bg=COLORS['bg'], fg='#666', font=('Segoe UI', 9)
        ).pack(pady=(0, 6))

        tk.Button(win, text="Close", command=win.destroy,
                  bg=COLORS['btn_primary'], fg=COLORS['btn_fg'],
                  bd=0, padx=20, pady=4).pack(pady=(0, 10))

    def _ensure_project_url(self, project):
        """
        Return saved Claude project URL; if missing, prompt the user.
        Returns None if user declines.
        """
        url = get_project_claude_url(project)
        if url:
            return url
        new_url = simpledialog.askstring(
            "Claude project URL needed",
            f"No Claude project URL is saved for {project}.\n\n"
            f"Paste the URL (https://claude.ai/project/<uuid>):",
            parent=self
        )
        if not new_url:
            return None
        new_url = new_url.strip()
        if not PROJECT_URL_PATTERN.match(new_url):
            messagebox.showwarning(
                "Invalid URL",
                "Expected format: https://claude.ai/project/<uuid>"
            )
            return None
        save_project_claude_url(project, new_url)
        return new_url

    # -------------------------------------------------------------------------
    # Launch flow
    # -------------------------------------------------------------------------

    def launch_selected(self):
        """Launch every checked item in Chrome."""
        if not self.checked_item_ids:
            return
        # Resolve item dicts
        items = [it for it in self.tree_node_to_item.values()
                 if it['id'] in self.checked_item_ids]
        if not items:
            return

        launched = 0
        skipped = 0
        for item in items:
            if self._launch_one(item, refresh_after=False):
                launched += 1
            else:
                skipped += 1

        self.refresh_tree()
        self.status_var.set(
            f"Launched {launched} item(s)"
            f"{f', {skipped} skipped' if skipped else ''}."
        )
        if launched:
            messagebox.showinfo(
                "Launched",
                f"Opened {launched} chat(s) in Chrome.\n\n"
                "For new chats, copy the URL from Chrome's address bar "
                "(Ctrl+L, Ctrl+C) and click 'From clipboard' in the detail pane "
                "to save it."
            )

    def _launch_one(self, item, refresh_after=True):
        """
        Launch a single item. Returns True on success, False if skipped.
        - If item has a current chat URL: open it, log Opened event.
        - Else: open the project's Claude URL (prompting for one if missing).
        """
        current = get_current_chat_link(item['id'])
        if current:
            webbrowser.open(current['url'])
            log_chat_access(current['id'], 'Opened')
            logger.info(f"Opened existing chat for item {item['id']}")
        else:
            project_url = self._ensure_project_url(item['project'])
            if not project_url:
                logger.warning(f"No project URL for {item['project']}; skipping")
                return False
            webbrowser.open(project_url)
            touch_project_url(item['project'])
            logger.info(f"Opened project URL for {item['project']} (item {item['id']})")

        if refresh_after:
            self.refresh_tree()
            # Re-render detail in case open count changed
            if self.current_detail_item and self.current_detail_item['id'] == item['id']:
                self._render_detail(item)
        return True

    # -------------------------------------------------------------------------
    # Start Fix
    # -------------------------------------------------------------------------

    def _start_fix(self, item):
        """
        Assemble a context brief for this item and launch Claude Code against
        the project folder. The slow work - pdoc rebuild and the ProjectAnalyzer
        scan - runs on a background thread so the GUI stays responsive.
        """
        if start_fix is None:
            messagebox.showerror(
                "Start Fix unavailable",
                "The start_fix module could not be loaded:\n\n"
                f"{_START_FIX_IMPORT_ERROR}\n\n"
                "Confirm start_fix.py is present in the CRPUtils folder."
            )
            return

        if self._start_fix_busy:
            messagebox.showinfo(
                "Start Fix in progress",
                "A Start Fix is already running. Let it finish before "
                "starting another one."
            )
            return

        if not messagebox.askyesno(
            "Start Fix?",
            f"Start a fix for {item['item_number']} — {item['title']}?\n\n"
            "This rebuilds the project's pdoc, runs ProjectAnalyzer, assembles "
            "a brief, and opens Claude Code in a new window to work the item."
        ):
            return

        # Lock out re-entry and give visual feedback.
        self._start_fix_busy = True
        try:
            self._btn_start_fix.config(state=tk.DISABLED, text="⏳ Start Fix…")
        except (AttributeError, tk.TclError):
            pass
        self.status_var.set(
            f"Start Fix: preparing brief for {item['item_number']} "
            "(rebuilding pdoc, running ProjectAnalyzer)…"
        )

        def worker():
            try:
                result = start_fix.start_fix(item['project'], item['id'])
            except Exception as e:                       # noqa: BLE001
                logger.exception("Start Fix worker failed")
                result = {'ok': False, 'brief_path': None,
                          'work_log_id': None,
                          'message': f"Start Fix failed: {e}"}
            # Marshal the result back onto the GUI thread.
            self.after(0, self._start_fix_done, item, result)

        threading.Thread(target=worker, daemon=True).start()

    def _start_fix_done(self, item, result):
        """Runs on the GUI thread after the Start Fix worker finishes."""
        self._start_fix_busy = False
        try:
            self._btn_start_fix.config(state=tk.NORMAL, text="🔧 Start Fix")
        except (AttributeError, tk.TclError):
            pass

        if result.get('ok'):
            # Starting a fix is a clear "work has begun" signal.
            if item.get('status') == 'Open':
                try:
                    update_item_status(item['id'], 'In Progress')
                    logger.info(f"Start Fix promoted item {item['id']} "
                                "to In Progress")
                except Exception:
                    logger.exception("Status flip failed (non-fatal)")
            self.status_var.set(
                f"Start Fix launched for {item['item_number']}."
            )
            messagebox.showinfo(
                "Start Fix launched",
                f"{result.get('message', '')}\n\n"
                "Claude Code is opening in its own window. Brief:\n"
                f"{result.get('brief_path') or '(not recorded)'}"
            )
            self.refresh_tree()
        else:
            self.status_var.set(
                f"Start Fix did not launch for {item['item_number']}."
            )
            messagebox.showwarning(
                "Start Fix did not launch",
                result.get('message', 'Unknown error.')
            )

    # -------------------------------------------------------------------------
    # URL capture / save
    # -------------------------------------------------------------------------

    def _fill_url_from_clipboard(self, var):
        try:
            content = self.clipboard_get().strip()
        except tk.TclError:
            messagebox.showwarning("Clipboard empty",
                                   "Nothing on the clipboard.")
            return
        var.set(content)

    def _save_captured_url(self, item, is_followon=False, prompt_for_url=False):
        """
        Save a chat URL for the item. Source:
         - the detail pane URL entry (if rendered for this item), OR
         - clipboard (if prompt_for_url=True; used by right-click follow-on)
        """
        if prompt_for_url:
            try:
                url = self.clipboard_get().strip()
            except tk.TclError:
                url = ''
            url = simpledialog.askstring(
                "Add follow-on chat URL",
                "Paste the new chat URL (or leave the clipboard value):",
                initialvalue=url, parent=self
            )
            if not url:
                return
            url = url.strip()
        else:
            url = self.detail_url_var.get().strip()
            if not url:
                messagebox.showwarning(
                    "No URL", "Paste or capture a URL first."
                )
                return

        if not CHAT_URL_PATTERN.match(url):
            if not messagebox.askyesno(
                "URL doesn't look right",
                f"This doesn't match the expected pattern\n"
                f"https://claude.ai/chat/<uuid>\n\n"
                f"Got: {url}\n\n"
                f"Save it anyway?"
            ):
                return

        # Save
        try:
            new_id = save_chat_link(
                item['id'], url, label=None, is_followon=is_followon
            )
        except Exception as e:
            logger.exception("Failed to save chat link")
            messagebox.showerror("Save failed", str(e))
            return

        # Auto-flip status: Open -> In Progress on first chat capture
        if not is_followon and item['status'] == 'Open':
            try:
                update_item_status(item['id'], 'In Progress')
                logger.info(f"Auto-promoted item {item['id']} to In Progress")
            except Exception:
                logger.exception("Status flip failed (non-fatal)")

        self.status_var.set(
            f"Saved {'follow-on ' if is_followon else ''}chat URL for "
            f"{item['item_number']}."
        )
        self.refresh_tree()
        # Re-render detail
        if self.current_detail_item and self.current_detail_item['id'] == item['id']:
            # Pull updated item from refreshed tree
            for it in self.tree_node_to_item.values():
                if it['id'] == item['id']:
                    self.current_detail_item = it
                    self._render_detail(it)
                    break

    # -------------------------------------------------------------------------
    # Prompt copy
    # -------------------------------------------------------------------------

    def _copy_prompt(self, item, is_followon=False):
        history = get_chat_history(item['id'])
        has_history = len(history) > 0
        prompt = build_prompt_for_item(
            item, has_history=has_history, is_followon=is_followon
        )
        self.clipboard_clear()
        self.clipboard_append(prompt)
        self.status_var.set(
            f"Copied {'follow-on ' if is_followon else ''}prompt "
            f"for {item['item_number']} to clipboard."
        )

    # -------------------------------------------------------------------------
    # Chat history viewer
    # -------------------------------------------------------------------------

    def view_chat_history(self, item):
        win = tk.Toplevel(self)
        win.title(f"Chat history — {item['item_number']}")
        win.geometry("900x500")
        win.configure(bg=COLORS['bg'])

        tk.Label(
            win, text=f"{item['item_number']}: {item['title']}",
            bg=COLORS['bg'], font=('Segoe UI', 11, 'bold'),
            wraplength=850, justify=tk.LEFT
        ).pack(anchor='w', padx=12, pady=(10, 4))

        history = get_chat_history(item['id'])
        if not history:
            tk.Label(
                win, text="No chat history.",
                bg=COLORS['bg'], fg='#888', font=('Segoe UI', 10, 'italic')
            ).pack(pady=20)
            tk.Button(win, text="Close", command=win.destroy,
                      bg=COLORS['btn_primary'], fg=COLORS['btn_fg'],
                      bd=0, padx=20).pack(pady=10)
            return

        frame = tk.Frame(win, bg=COLORS['bg'])
        frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)

        tree = ttk.Treeview(
            frame,
            columns=('current', 'created', 'last_opened', 'opens', 'label'),
            show='tree headings'
        )
        tree.heading('#0', text='URL')
        tree.heading('current', text='Current')
        tree.heading('created', text='Created')
        tree.heading('last_opened', text='Last opened')
        tree.heading('opens', text='Opens')
        tree.heading('label', text='Label')
        tree.column('#0', width=320)
        tree.column('current', width=70, anchor='center')
        tree.column('created', width=130, anchor='center')
        tree.column('last_opened', width=130, anchor='center')
        tree.column('opens', width=60, anchor='center')
        tree.column('label', width=140)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        sb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        for h in history:
            url_short = h['url']
            if len(url_short) > 60:
                url_short = url_short[:57] + '...'
            tree.insert(
                '', 'end',
                iid=str(h['id']),
                text=url_short,
                values=(
                    '✓' if h['is_current'] else '',
                    h['created'].strftime('%Y-%m-%d %H:%M') if h['created'] else '—',
                    h['last_opened'].strftime('%Y-%m-%d %H:%M') if h['last_opened'] else '—',
                    h['open_count'],
                    h['label'] or ''
                )
            )

        def open_selected():
            sel = tree.selection()
            if not sel:
                return
            link_id = int(sel[0])
            for h in history:
                if h['id'] == link_id:
                    webbrowser.open(h['url'])
                    log_chat_access(link_id, 'Opened')
                    break

        def make_current():
            sel = tree.selection()
            if not sel:
                return
            link_id = int(sel[0])
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE [dbo].[PMA_PunchlistChatLinks]
                SET IsCurrent = 0
                WHERE PunchlistItemID = ? AND IsCurrent = 1
            """, (item['id'],))
            cursor.execute("""
                UPDATE [dbo].[PMA_PunchlistChatLinks]
                SET IsCurrent = 1
                WHERE ChatLinkID = ?
            """, (link_id,))
            conn.commit()
            cursor.close()
            conn.close()
            messagebox.showinfo("Updated",
                                "That chat is now marked current.",
                                parent=win)
            win.destroy()
            self.refresh_tree()

        def edit_label():
            sel = tree.selection()
            if not sel:
                return
            link_id = int(sel[0])
            target = next(h for h in history if h['id'] == link_id)
            new_label = simpledialog.askstring(
                "Edit label",
                "Optional label for this chat (e.g., 'after context refresh'):",
                initialvalue=target['label'] or '',
                parent=win
            )
            if new_label is None:
                return
            update_chat_link(link_id, label=new_label or None)
            tree.set(sel[0], 'label', new_label or '')

        def delete_selected():
            sel = tree.selection()
            if not sel:
                return
            link_id = int(sel[0])
            if not messagebox.askyesno(
                "Delete chat link?",
                "This deletes the link record AND its access history. "
                "The chat itself in Claude is untouched.\n\nProceed?",
                parent=win
            ):
                return
            delete_chat_link(link_id)
            tree.delete(sel[0])

        btns = tk.Frame(win, bg=COLORS['bg'])
        btns.pack(fill=tk.X, padx=12, pady=8)
        for txt, cmd, color in [
            ("Open in browser", open_selected, COLORS['btn_primary']),
            ("Make current", make_current, COLORS['btn_success']),
            ("Edit label", edit_label, COLORS['btn_primary']),
            ("Delete", delete_selected, COLORS['btn_danger']),
            ("Close", win.destroy, '#7f8c8d'),
        ]:
            tk.Button(btns, text=txt, command=cmd, bg=color,
                      fg=COLORS['btn_fg'], bd=0, padx=12, pady=4,
                      font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=3)

    # -------------------------------------------------------------------------
    # QC report
    # -------------------------------------------------------------------------

    def _run_qc_report(self):
        """Launch the punchlist QC analysis in a background thread."""
        if _QC_IMPORT_ERROR:
            messagebox.showerror(
                "QC unavailable",
                f"PunchlistQC failed to load:\n{_QC_IMPORT_ERROR}"
            )
            return

        self.status_var.set("Running QC analysis… (one pass per project — may take 2–3 minutes)")

        def _worker():
            try:
                findings, items_by_id = PunchlistQC.run_qc()
            except Exception as exc:
                self.after(0, lambda: messagebox.showerror("QC Error", str(exc)))
                self.after(0, lambda: self.status_var.set("QC analysis failed."))
                return
            n = len(findings)
            self.after(0, lambda: self._show_qc_dialog(findings, items_by_id))
            self.after(0, lambda: self.status_var.set(
                f"QC complete — {n} finding(s)." if n else "QC complete — no issues found."
            ))

        threading.Thread(target=_worker, daemon=True).start()

    def _show_qc_dialog(self, findings, items_by_id):
        """Display QC findings in a scrollable dialog with per-finding action buttons."""
        win = tk.Toplevel(self)
        win.title("Punchlist QC Report")
        win.geometry("820x600")
        win.configure(bg=COLORS['bg'])
        win.grab_set()

        # Header
        tk.Label(
            win, text="🔍 Punchlist QC Report",
            bg=COLORS['header_bg'], fg=COLORS['header_fg'],
            font=('Segoe UI', 13, 'bold'), anchor='w', padx=12
        ).pack(fill=tk.X)

        if not findings:
            tk.Label(
                win, text="\n✅  No issues found — all open items look good.\n",
                bg=COLORS['bg'], font=('Segoe UI', 11), fg='#27ae60'
            ).pack(pady=40)
            tk.Button(
                win, text="Close", command=win.destroy,
                bg=COLORS['btn_primary'], fg=COLORS['btn_fg'],
                bd=0, padx=20, pady=6, font=('Segoe UI', 9)
            ).pack(pady=8)
            return

        # Scrollable findings area
        container = tk.Frame(win, bg=COLORS['bg'])
        container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        canvas = tk.Canvas(container, bg=COLORS['bg'], highlightthickness=0)
        sb = ttk.Scrollbar(container, orient=tk.VERTICAL, command=canvas.yview)
        inner = tk.Frame(canvas, bg=COLORS['bg'])

        inner_id = canvas.create_window((0, 0), window=inner, anchor='nw')
        canvas.configure(yscrollcommand=sb.set)

        def _on_inner_configure(_event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event):
            canvas.itemconfig(inner_id, width=event.width)

        inner.bind("<Configure>", _on_inner_configure)
        canvas.bind("<Configure>", _on_canvas_configure)
        _scroll_cb = lambda e: canvas.yview_scroll(-1 * (e.delta // 120), "units")
        win.bind("<MouseWheel>", _scroll_cb)
        win.protocol("WM_DELETE_WINDOW", lambda: (win.unbind("<MouseWheel>"), win.destroy()))

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        # Type display config: (badge_text, badge_fg, badge_bg)
        type_cfg = {
            'duplicate':    ("🔄 DUPLICATE",    'white', '#e67e22'),
            'already_done': ("✅ ALREADY DONE", 'white', '#2980b9'),
            'stale':        ("⏰ STALE",        'white', '#7f8c8d'),
        }

        active_findings = list(findings)   # mutable so cards can remove themselves

        def _dismiss_card(card_frame, finding):
            card_frame.destroy()
            active_findings.remove(finding)
            if not active_findings:
                _show_all_done()

        def _mark_completed_action(card_frame, finding):
            item_id  = finding['item_id']
            item_num = finding.get('item_number', str(item_id))
            if not messagebox.askyesno(
                "Mark completed?",
                f"Mark {item_num} as Completed?\n\n"
                "Sets Status=Completed and CompletedDate=now in PMA_PunchlistItems.",
                parent=win
            ):
                return
            conn = get_connection()
            try:
                PunchlistQC.apply_mark_completed(conn, item_id)
            finally:
                conn.close()
            _dismiss_card(card_frame, finding)
            self.refresh_tree()

        def _flag_duplicate_action(card_frame, finding):
            item_id  = finding['item_id']
            item_num = finding.get('item_number', str(item_id))
            related_num  = finding.get('related_item_number', '?')
            related_proj = finding.get('related_project', '?')
            related_ref  = f"{related_num} ({related_proj})"
            if not messagebox.askyesno(
                "Flag as duplicate?",
                f"Prepend a duplicate warning to {item_num}'s description?\n\n"
                f"Will note: possible duplicate of {related_ref}.",
                parent=win
            ):
                return
            conn = get_connection()
            try:
                PunchlistQC.apply_flag_duplicate(conn, item_id, related_ref)
            finally:
                conn.close()
            _dismiss_card(card_frame, finding)
            self.refresh_tree()

        def _show_all_done():
            for w in inner.winfo_children():
                w.destroy()
            tk.Label(
                inner, text="\n✅  All findings resolved.\n",
                bg=COLORS['bg'], font=('Segoe UI', 11), fg='#27ae60'
            ).pack(pady=30)

        # Build one card per finding
        for finding in findings:
            ftype = finding.get('type', '')
            badge_text, badge_fg, badge_bg = type_cfg.get(
                ftype, (ftype.upper(), 'white', '#555')
            )

            card = tk.LabelFrame(
                inner, bg=COLORS['bg'],
                relief=tk.GROOVE, bd=1, padx=10, pady=8
            )
            card.pack(fill=tk.X, padx=6, pady=5, ipadx=4, ipady=4)

            # Badge + item reference row
            top = tk.Frame(card, bg=COLORS['bg'])
            top.pack(fill=tk.X)

            tk.Label(
                top, text=badge_text,
                bg=badge_bg, fg=badge_fg,
                font=('Segoe UI', 8, 'bold'), padx=6, pady=2
            ).pack(side=tk.LEFT, padx=(0, 8))

            proj    = finding.get('project', '')
            num     = finding.get('item_number', '')
            title   = finding.get('title', '')
            ref_str = f"{proj} | {num}: {title}"
            tk.Label(
                top, text=ref_str,
                bg=COLORS['bg'], font=('Segoe UI', 9, 'bold'),
                anchor='w', wraplength=580, justify=tk.LEFT
            ).pack(side=tk.LEFT, fill=tk.X, expand=True)

            # Related item (for duplicate / already_done)
            if finding.get('related_item_number'):
                rel_text = (
                    f"Related: {finding.get('related_project')} | "
                    f"{finding.get('related_item_number')}"
                )
                tk.Label(
                    card, text=rel_text,
                    bg=COLORS['bg'], fg='#555', font=('Segoe UI', 8, 'italic'),
                    anchor='w'
                ).pack(fill=tk.X, pady=(2, 0))

            # Reason
            tk.Label(
                card, text=finding.get('reason', ''),
                bg=COLORS['bg'], font=('Segoe UI', 9),
                anchor='w', wraplength=720, justify=tk.LEFT
            ).pack(fill=tk.X, pady=(4, 6))

            # Action buttons
            btns = tk.Frame(card, bg=COLORS['bg'])
            btns.pack(anchor='w')

            if ftype == 'duplicate':
                older_num = finding.get('item_number', str(finding.get('item_id', '?')))
                tk.Button(
                    btns,
                    text=f"🔁 Flag {older_num} as Duplicate",
                    command=lambda c=card, f=finding: _flag_duplicate_action(c, f),
                    bg=COLORS['btn_warning'], fg=COLORS['btn_fg'],
                    bd=0, padx=8, pady=3, font=('Segoe UI', 8)
                ).pack(side=tk.LEFT, padx=(0, 4))

            elif ftype in ('already_done', 'stale'):
                item_num = finding.get('item_number', str(finding.get('item_id', '?')))
                tk.Button(
                    btns,
                    text=f"✓ Mark {item_num} Completed",
                    command=lambda c=card, f=finding: _mark_completed_action(c, f),
                    bg=COLORS['btn_success'], fg=COLORS['btn_fg'],
                    bd=0, padx=8, pady=3, font=('Segoe UI', 8)
                ).pack(side=tk.LEFT, padx=(0, 4))

            # Dismiss is available for all types
            tk.Button(
                btns, text="✕ Dismiss",
                command=lambda c=card, f=finding: _dismiss_card(c, f),
                bg='#bdc3c7', fg='#2c3e50',
                bd=0, padx=8, pady=3, font=('Segoe UI', 8)
            ).pack(side=tk.LEFT)

        # Footer close button
        tk.Button(
            win, text="Close",
            command=win.destroy,
            bg=COLORS['btn_primary'], fg=COLORS['btn_fg'],
            bd=0, padx=20, pady=6, font=('Segoe UI', 9)
        ).pack(pady=8)

    # -------------------------------------------------------------------------
    # Misc
    # -------------------------------------------------------------------------

    def _mark_completed(self, item):
        if not messagebox.askyesno(
            "Mark completed?",
            f"Mark {item['item_number']} as Completed?\n\n"
            f"This sets Status=Completed and CompletedDate=now in "
            f"PMA_PunchlistItems. Chat history is preserved."
        ):
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE [dbo].[PMA_PunchlistItems]
            SET Status = 'Completed',
                CompletedDate = GETDATE(),
                LastModifiedDate = GETDATE()
            WHERE PunchlistItemID = ?
        """, (item['id'],))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Marked item {item['id']} completed")
        self.refresh_tree()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    try:
        ensure_tables_exist()
    except Exception as e:
        logger.exception("Schema setup failed")
        # Show error in a pre-tk window since main app hasn't started
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Database setup failed",
            f"Could not create or verify Commander tables:\n\n{e}\n\n"
            f"Check that {SQL_SERVER}/{SQL_DATABASE} is reachable."
        )
        return 1

    app = PunchlistCommander()
    app.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())