"""
punchlist_gui.py - Punchlist Manager GUI
==========================================

Tkinter-based GUI for managing PMA_PunchlistItems in CRPAF.

Features:
  - Browse all items with filtering by Project, Priority, Status
  - Edit items inline (title, description, status, priority, blockers, etc.)
  - Add new items
  - Mark items completed
  - Run Export (SQL -> markdown) and Summary from buttons
  - Run Ingest (markdown -> SQL) to refresh from punchlist files

Lives in: CRPUtils folder
Table:    [CRPAF].[dbo].[PMA_PunchlistItems] on BI-SQL001

Author: Pat Yearick
Created: February 2026
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pyodbc
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

SQL_SERVER = "BI-SQL001"
SQL_DATABASE = "CRPAF"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"

VALID_STATUSES = ['Open', 'In Progress', 'Blocked', 'Completed']
VALID_PRIORITIES = ['High', 'Medium', 'Low']

# Color scheme
COLORS = {
    'bg': '#f5f5f5',
    'header_bg': '#2c3e50',
    'header_fg': '#ffffff',
    'btn_primary': '#3498db',
    'btn_success': '#27ae60',
    'btn_warning': '#f39c12',
    'btn_danger': '#e74c3c',
    'btn_fg': '#ffffff',
    'high': '#e74c3c',
    'medium': '#f39c12',
    'low': '#27ae60',
    'blocked': '#9b59b6',
    'completed': '#95a5a6',
    'in_progress': '#3498db',
    'open': '#2c3e50',
    'tree_stripe': '#ecf0f1',
}


# =============================================================================
# DATABASE
# =============================================================================

def get_connection():
    """Get database connection using Windows auth."""
    return pyodbc.connect(
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )


def fetch_all_items(project_filter=None, status_filter=None, priority_filter=None):
    """Fetch items from PMA_PunchlistItems with optional filters."""
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        SELECT PunchlistItemID, Project, ItemNumber, Title, Description,
               Status, Priority, Section, BlockedBy, Unlocks,
               CreatedDate, LastModifiedDate, CompletedDate
        FROM [dbo].[PMA_PunchlistItems]
        WHERE 1=1
    """
    params = []

    if project_filter and project_filter != '(All)':
        sql += " AND Project = ?"
        params.append(project_filter)
    if status_filter and status_filter != '(All)':
        sql += " AND Status = ?"
        params.append(status_filter)
    if priority_filter and priority_filter != '(All)':
        sql += " AND Priority = ?"
        params.append(priority_filter)

    sql += """
        ORDER BY
            CASE Status
                WHEN 'Blocked' THEN 1
                WHEN 'In Progress' THEN 2
                WHEN 'Open' THEN 3
                WHEN 'Completed' THEN 4
                ELSE 5
            END,
            CASE Priority
                WHEN 'High' THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'Low' THEN 3
                ELSE 4
            END,
            Project, ItemNumber
    """

    cursor.execute(sql, params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]


def fetch_distinct_projects():
    """Get list of distinct project names from the SQL table."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT Project FROM [dbo].[PMA_PunchlistItems] ORDER BY Project")
    projects = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return projects


# Folders to skip when scanning for project folders
SKIP_FOLDERS = {
    '.idea', '.git', '.venv', '__pycache__', 'node_modules',
    'CommitsGH', '.ipynb_checkpoints'
}


def fetch_all_project_folders():
    """
    Scan the PycharmProjects directory for ALL project folders,
    including ones that don't have a punchlist yet.
    Returns a sorted list of folder names.
    """
    try:
        base = Path(__file__).resolve().parent.parent  # Up from CRPUtils to PycharmProjects
        folders = []
        for child in sorted(base.iterdir()):
            if child.is_dir() and child.name not in SKIP_FOLDERS and not child.name.startswith('.'):
                folders.append(child.name)
        return folders
    except Exception:
        return []


def fetch_merged_project_list():
    """
    Merge SQL projects with filesystem folders so the user can
    add items to projects that don't have a punchlist yet.
    Existing SQL projects appear first, then new folders marked with (new).
    """
    sql_projects = set(fetch_distinct_projects())
    all_folders = fetch_all_project_folders()

    # Build merged list: SQL projects first (sorted), then new folders
    merged = sorted(sql_projects)
    new_folders = [f for f in all_folders if f not in sql_projects]
    if new_folders:
        merged.extend(new_folders)

    return merged, new_folders


def update_item(item_id, **fields):
    """Update specific fields on a punchlist item."""
    conn = get_connection()
    cursor = conn.cursor()

    set_clauses = []
    params = []
    for col, val in fields.items():
        set_clauses.append(f"{col} = ?")
        params.append(val)

    set_clauses.append("LastModifiedDate = GETDATE()")
    params.append(item_id)

    sql = f"""
        UPDATE [dbo].[PMA_PunchlistItems]
        SET {', '.join(set_clauses)}
        WHERE PunchlistItemID = ?
    """

    cursor.execute(sql, params)
    conn.commit()
    cursor.close()
    conn.close()


def insert_item(project, item_number, title, description, status, priority,
                section, blocked_by, unlocks):
    """Insert a new punchlist item."""
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO [dbo].[PMA_PunchlistItems]
            (Project, ItemNumber, Title, Description, Status, Priority,
             Section, BlockedBy, Unlocks, SourceFile, CreatedDate,
             LastModifiedDate, IngestedDate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'GUI', GETDATE(), GETDATE(), GETDATE())
    """

    cursor.execute(sql, (project, item_number, title, description, status,
                         priority, section, blocked_by, unlocks))
    conn.commit()
    cursor.close()
    conn.close()


def get_next_item_number(project):
    """Get next available item number for a project."""
    prefix_map = {
        'BigDawgHunt': 'BDH',
        'LostSales': 'LS',
        'PMAssistant': 'PMA',
    }
    prefix = prefix_map.get(project, project[:3].upper())

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(TRY_CAST(
            SUBSTRING(ItemNumber,
                CHARINDEX('-', ItemNumber) + 1,
                LEN(ItemNumber))
            AS INT))
        FROM [dbo].[PMA_PunchlistItems]
        WHERE Project = ? AND ItemNumber LIKE ?
    """, (project, f"{prefix}-%"))

    row = cursor.fetchone()
    next_num = (row[0] or 0) + 1
    cursor.close()
    conn.close()
    return f"{prefix}-{next_num:03d}"


def get_summary_stats():
    """Get summary statistics for the status bar."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(*) AS Total,
            SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenCount,
            SUM(CASE WHEN Status = 'In Progress' THEN 1 ELSE 0 END) AS InProgressCount,
            SUM(CASE WHEN Status = 'Blocked' THEN 1 ELSE 0 END) AS BlockedCount,
            SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) AS CompletedCount,
            SUM(CASE WHEN Priority = 'High' AND Status <> 'Completed' THEN 1 ELSE 0 END) AS HighPri
        FROM [dbo].[PMA_PunchlistItems]
    """)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return {
        'total': row[0], 'open': row[1], 'in_progress': row[2],
        'blocked': row[3], 'completed': row[4], 'high_pri': row[5]
    }


# =============================================================================
# MAIN APPLICATION
# =============================================================================

class PunchlistApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Punchlist Manager — PMA_PunchlistItems")
        self.root.geometry("1400x800")
        self.root.configure(bg=COLORS['bg'])

        # Try to set icon if available
        try:
            self.root.iconbitmap(default='')
        except Exception:
            pass

        self._build_header()
        self._build_filters()
        self._build_treeview()
        self._build_detail_panel()
        self._build_status_bar()

        # Initial load
        self.refresh_data()

    # -----------------------------------------------------------------
    # HEADER
    # -----------------------------------------------------------------
    def _build_header(self):
        header = tk.Frame(self.root, bg=COLORS['header_bg'], height=50)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(
            header, text="Punchlist Manager",
            font=('Segoe UI', 16, 'bold'),
            bg=COLORS['header_bg'], fg=COLORS['header_fg']
        ).pack(side=tk.LEFT, padx=15)

        # Action buttons on the right
        btn_frame = tk.Frame(header, bg=COLORS['header_bg'])
        btn_frame.pack(side=tk.RIGHT, padx=10)

        self._make_button(btn_frame, "Ingest", self.run_ingest,
                          COLORS['btn_primary']).pack(side=tk.LEFT, padx=3)
        self._make_button(btn_frame, "Export", self.run_export,
                          COLORS['btn_success']).pack(side=tk.LEFT, padx=3)
        self._make_button(btn_frame, "Summary", self.show_summary,
                          COLORS['btn_warning']).pack(side=tk.LEFT, padx=3)
        self._make_button(btn_frame, "+ New Item", self.add_new_item,
                          COLORS['btn_primary']).pack(side=tk.LEFT, padx=3)

    # -----------------------------------------------------------------
    # FILTER BAR
    # -----------------------------------------------------------------
    def _build_filters(self):
        filter_frame = tk.Frame(self.root, bg=COLORS['bg'], pady=8)
        filter_frame.pack(fill=tk.X, padx=10)

        # Project filter
        tk.Label(filter_frame, text="Project:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 3))
        self.project_var = tk.StringVar(value='(All)')
        self.project_combo = ttk.Combobox(
            filter_frame, textvariable=self.project_var,
            state='readonly', width=20
        )
        self.project_combo.pack(side=tk.LEFT, padx=(0, 12))
        self.project_combo.bind('<<ComboboxSelected>>', lambda e: self.refresh_data())

        # Status filter
        tk.Label(filter_frame, text="Status:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 3))
        self.status_var = tk.StringVar(value='(All)')
        self.status_combo = ttk.Combobox(
            filter_frame, textvariable=self.status_var,
            values=['(All)'] + VALID_STATUSES, state='readonly', width=14
        )
        self.status_combo.pack(side=tk.LEFT, padx=(0, 12))
        self.status_combo.bind('<<ComboboxSelected>>', lambda e: self.refresh_data())

        # Priority filter
        tk.Label(filter_frame, text="Priority:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 3))
        self.priority_var = tk.StringVar(value='(All)')
        self.priority_combo = ttk.Combobox(
            filter_frame, textvariable=self.priority_var,
            values=['(All)'] + VALID_PRIORITIES, state='readonly', width=10
        )
        self.priority_combo.pack(side=tk.LEFT, padx=(0, 12))
        self.priority_combo.bind('<<ComboboxSelected>>', lambda e: self.refresh_data())

        # Refresh button
        self._make_button(filter_frame, "Refresh", self.refresh_data,
                          COLORS['btn_primary']).pack(side=tk.LEFT, padx=5)

        # Item count label
        self.count_label = tk.Label(
            filter_frame, text="", bg=COLORS['bg'],
            font=('Segoe UI', 9, 'italic')
        )
        self.count_label.pack(side=tk.RIGHT, padx=10)

    # -----------------------------------------------------------------
    # TREEVIEW (item list)
    # -----------------------------------------------------------------
    def _build_treeview(self):
        tree_frame = tk.Frame(self.root, bg=COLORS['bg'])
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 5))

        columns = ('project', 'item_num', 'title', 'status', 'priority',
                   'section', 'blocked_by')
        self.tree = ttk.Treeview(
            tree_frame, columns=columns, show='headings',
            selectmode='browse', height=15
        )

        # Column config
        col_config = {
            'project': ('Project', 120),
            'item_num': ('Item #', 80),
            'title': ('Title', 420),
            'status': ('Status', 90),
            'priority': ('Priority', 75),
            'section': ('Section', 160),
            'blocked_by': ('Blocked By', 200),
        }
        for col, (heading, width) in col_config.items():
            self.tree.heading(col, text=heading,
                              command=lambda c=col: self._sort_column(c))
            self.tree.column(col, width=width, minwidth=50)

        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Style for row tags
        style = ttk.Style()
        style.configure('Treeview', font=('Segoe UI', 9), rowheight=24)
        style.configure('Treeview.Heading', font=('Segoe UI', 9, 'bold'))

        self.tree.tag_configure('high', foreground=COLORS['high'])
        self.tree.tag_configure('blocked', foreground=COLORS['blocked'])
        self.tree.tag_configure('completed', foreground=COLORS['completed'])
        self.tree.tag_configure('in_progress', foreground=COLORS['in_progress'])
        self.tree.tag_configure('stripe', background=COLORS['tree_stripe'])

        # Bind selection
        self.tree.bind('<<TreeviewSelect>>', self._on_item_select)

        # Store full item data
        self.item_data = {}

    # -----------------------------------------------------------------
    # DETAIL / EDIT PANEL
    # -----------------------------------------------------------------
    def _build_detail_panel(self):
        detail_outer = tk.LabelFrame(
            self.root, text="Item Detail / Edit",
            font=('Segoe UI', 10, 'bold'),
            bg=COLORS['bg'], padx=10, pady=5
        )
        detail_outer.pack(fill=tk.X, padx=10, pady=(0, 5))

        # Row 1: Project, Item#, Status, Priority
        row1 = tk.Frame(detail_outer, bg=COLORS['bg'])
        row1.pack(fill=tk.X, pady=2)

        tk.Label(row1, text="Project:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=0, sticky='e', padx=3)
        self.detail_project = ttk.Combobox(row1, state='readonly', width=18)
        self.detail_project.grid(row=0, column=1, padx=3)

        tk.Label(row1, text="Item #:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=2, sticky='e', padx=3)
        self.detail_item_num = tk.Entry(row1, width=12, font=('Segoe UI', 9))
        self.detail_item_num.grid(row=0, column=3, padx=3)

        tk.Label(row1, text="Status:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=4, sticky='e', padx=3)
        self.detail_status = ttk.Combobox(
            row1, values=VALID_STATUSES, state='readonly', width=12
        )
        self.detail_status.grid(row=0, column=5, padx=3)

        tk.Label(row1, text="Priority:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=6, sticky='e', padx=3)
        self.detail_priority = ttk.Combobox(
            row1, values=VALID_PRIORITIES, state='readonly', width=10
        )
        self.detail_priority.grid(row=0, column=7, padx=3)

        tk.Label(row1, text="Section:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=8, sticky='e', padx=3)
        self.detail_section = tk.Entry(row1, width=22, font=('Segoe UI', 9))
        self.detail_section.grid(row=0, column=9, padx=3)

        # Row 2: Title
        row2 = tk.Frame(detail_outer, bg=COLORS['bg'])
        row2.pack(fill=tk.X, pady=2)

        tk.Label(row2, text="Title:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=3)
        self.detail_title = tk.Entry(row2, font=('Segoe UI', 9))
        self.detail_title.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=3)

        # Row 3: Blocked By / Unlocks
        row3 = tk.Frame(detail_outer, bg=COLORS['bg'])
        row3.pack(fill=tk.X, pady=2)

        tk.Label(row3, text="Blocked By:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=3)
        self.detail_blocked = tk.Entry(row3, width=40, font=('Segoe UI', 9))
        self.detail_blocked.pack(side=tk.LEFT, padx=3)

        tk.Label(row3, text="Unlocks:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(15, 3))
        self.detail_unlocks = tk.Entry(row3, width=40, font=('Segoe UI', 9))
        self.detail_unlocks.pack(side=tk.LEFT, padx=3)

        # Row 4: Description (multiline)
        row4 = tk.Frame(detail_outer, bg=COLORS['bg'])
        row4.pack(fill=tk.X, pady=2)

        tk.Label(row4, text="Description:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).pack(anchor='nw', padx=3)
        self.detail_desc = scrolledtext.ScrolledText(
            row4, height=4, font=('Consolas', 9), wrap=tk.WORD
        )
        self.detail_desc.pack(fill=tk.X, padx=3, pady=2)

        # Row 5: Action buttons
        row5 = tk.Frame(detail_outer, bg=COLORS['bg'])
        row5.pack(fill=tk.X, pady=5)

        self._make_button(row5, "Save Changes", self.save_changes,
                          COLORS['btn_success']).pack(side=tk.LEFT, padx=3)
        self._make_button(row5, "Mark Completed", self.mark_completed,
                          COLORS['btn_warning']).pack(side=tk.LEFT, padx=3)
        self._make_button(row5, "Reopen Item", self.reopen_item,
                          COLORS['btn_primary']).pack(side=tk.LEFT, padx=3)
        self._make_button(row5, "Generate Prompt", self.generate_prompt,
                          '#8e44ad').pack(side=tk.LEFT, padx=12)
        self._make_button(row5, "Delete Item", self.delete_item,
                          COLORS['btn_danger']).pack(side=tk.RIGHT, padx=3)

        # Track currently selected item
        self.selected_item_id = None

    # -----------------------------------------------------------------
    # STATUS BAR
    # -----------------------------------------------------------------
    def _build_status_bar(self):
        self.status_bar = tk.Label(
            self.root, text="", bd=1, relief=tk.SUNKEN, anchor=tk.W,
            font=('Segoe UI', 9), bg='#dfe6e9', padx=10
        )
        self.status_bar.pack(fill=tk.X, side=tk.BOTTOM)

    # -----------------------------------------------------------------
    # HELPERS
    # -----------------------------------------------------------------
    def _make_button(self, parent, text, command, color):
        btn = tk.Button(
            parent, text=text, command=command,
            bg=color, fg=COLORS['btn_fg'],
            font=('Segoe UI', 9, 'bold'),
            relief=tk.FLAT, padx=10, pady=3,
            activebackground=color, activeforeground=COLORS['btn_fg'],
            cursor='hand2'
        )
        return btn

    def _set_status(self, msg):
        self.status_bar.config(text=msg)
        self.root.update_idletasks()

    def _clear_detail(self):
        self.selected_item_id = None
        self.detail_project.set('')
        self.detail_item_num.delete(0, tk.END)
        self.detail_title.delete(0, tk.END)
        self.detail_status.set('')
        self.detail_priority.set('')
        self.detail_section.delete(0, tk.END)
        self.detail_blocked.delete(0, tk.END)
        self.detail_unlocks.delete(0, tk.END)
        self.detail_desc.delete('1.0', tk.END)

    def _populate_detail(self, item):
        self._clear_detail()
        self.selected_item_id = item['PunchlistItemID']

        projects = fetch_distinct_projects()
        self.detail_project['values'] = projects
        self.detail_project.set(item['Project'] or '')

        self.detail_item_num.insert(0, item['ItemNumber'] or '')
        self.detail_title.insert(0, item['Title'] or '')
        self.detail_status.set(item['Status'] or 'Open')
        self.detail_priority.set(item['Priority'] or 'Medium')
        self.detail_section.insert(0, item['Section'] or '')
        self.detail_blocked.insert(0, item['BlockedBy'] or '')
        self.detail_unlocks.insert(0, item['Unlocks'] or '')
        self.detail_desc.insert('1.0', item['Description'] or '')

    def _sort_column(self, col):
        """Sort treeview by column (toggle asc/desc)."""
        items = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        items.sort(reverse=getattr(self, '_sort_reverse', False))
        for index, (_, k) in enumerate(items):
            self.tree.move(k, '', index)
        self._sort_reverse = not getattr(self, '_sort_reverse', False)

    # -----------------------------------------------------------------
    # DATA LOADING
    # -----------------------------------------------------------------
    def refresh_data(self):
        """Reload items from SQL and refresh the treeview."""
        self._set_status("Loading...")
        self._clear_detail()

        # Update project filter dropdown
        try:
            projects = fetch_distinct_projects()
            self.project_combo['values'] = ['(All)'] + projects
            self.detail_project['values'] = projects
        except Exception as e:
            messagebox.showerror("Database Error", f"Could not connect:\n{e}")
            return

        # Fetch filtered data
        items = fetch_all_items(
            project_filter=self.project_var.get(),
            status_filter=self.status_var.get(),
            priority_filter=self.priority_var.get()
        )

        # Clear tree
        for child in self.tree.get_children():
            self.tree.delete(child)
        self.item_data.clear()

        # Populate
        for i, item in enumerate(items):
            item_id = item['PunchlistItemID']
            self.item_data[str(item_id)] = item

            # Determine row tag
            tags = []
            if item['Status'] == 'Completed':
                tags.append('completed')
            elif item['Status'] == 'Blocked':
                tags.append('blocked')
            elif item['Status'] == 'In Progress':
                tags.append('in_progress')
            elif item['Priority'] == 'High':
                tags.append('high')

            if i % 2 == 1:
                tags.append('stripe')

            self.tree.insert('', tk.END, iid=str(item_id), values=(
                item['Project'],
                item['ItemNumber'],
                item['Title'][:80],
                item['Status'],
                item['Priority'],
                item['Section'] or '',
                item['BlockedBy'] or ''
            ), tags=tuple(tags))

        self.count_label.config(text=f"{len(items)} items shown")

        # Update status bar
        try:
            stats = get_summary_stats()
            self._set_status(
                f"Total: {stats['total']}  |  "
                f"Open: {stats['open']}  |  "
                f"In Progress: {stats['in_progress']}  |  "
                f"Blocked: {stats['blocked']}  |  "
                f"Completed: {stats['completed']}  |  "
                f"High Priority: {stats['high_pri']}"
            )
        except Exception:
            self._set_status(f"{len(items)} items loaded")

    # -----------------------------------------------------------------
    # TREEVIEW EVENTS
    # -----------------------------------------------------------------
    def _on_item_select(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        item_id = selection[0]
        item = self.item_data.get(item_id)
        if item:
            self._populate_detail(item)

    # -----------------------------------------------------------------
    # EDIT ACTIONS
    # -----------------------------------------------------------------
    def _refresh_and_reselect(self, item_id):
        """Refresh the tree, then re-select and reload the given item."""
        self.refresh_data()
        str_id = str(item_id)
        if str_id in self.item_data:
            try:
                self.tree.selection_set(str_id)
                self.tree.focus(str_id)
                self.tree.see(str_id)
                self._populate_detail(self.item_data[str_id])
            except tk.TclError:
                pass  # Item may have been filtered out

    def save_changes(self):
        if not self.selected_item_id:
            messagebox.showwarning("No Selection", "Select an item first.")
            return

        item_id = self.selected_item_id
        try:
            fields = {
                'Project': self.detail_project.get(),
                'ItemNumber': self.detail_item_num.get().strip(),
                'Title': self.detail_title.get().strip(),
                'Status': self.detail_status.get(),
                'Priority': self.detail_priority.get(),
                'Section': self.detail_section.get().strip() or None,
                'BlockedBy': self.detail_blocked.get().strip() or None,
                'Unlocks': self.detail_unlocks.get().strip() or None,
                'Description': self.detail_desc.get('1.0', tk.END).strip() or None,
            }

            # If status changed to Completed, set CompletedDate
            if fields['Status'] == 'Completed':
                fields['CompletedDate'] = datetime.now()

            update_item(item_id, **fields)
            self._set_status(f"Saved: {fields['ItemNumber']} - {fields['Title'][:50]}")
            self._refresh_and_reselect(item_id)

        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save:\n{e}")

    def mark_completed(self):
        if not self.selected_item_id:
            messagebox.showwarning("No Selection", "Select an item first.")
            return

        item_id = self.selected_item_id
        item = self.item_data.get(str(item_id))
        title = item['Title'][:60] if item else '?'

        if messagebox.askyesno("Confirm", f"Mark as completed?\n\n{title}"):
            try:
                update_item(item_id,
                            Status='Completed',
                            CompletedDate=datetime.now())
                self._set_status(f"Completed: {title}")
                self._refresh_and_reselect(item_id)
            except Exception as e:
                messagebox.showerror("Error", f"Could not update:\n{e}")

    def reopen_item(self):
        if not self.selected_item_id:
            messagebox.showwarning("No Selection", "Select an item first.")
            return

        item_id = self.selected_item_id
        item = self.item_data.get(str(item_id))
        title = item['Title'][:60] if item else '?'

        if messagebox.askyesno("Confirm", f"Reopen this item?\n\n{title}"):
            try:
                update_item(item_id,
                            Status='Open',
                            CompletedDate=None)
                self._set_status(f"Reopened: {title}")
                self._refresh_and_reselect(item_id)
            except Exception as e:
                messagebox.showerror("Error", f"Could not update:\n{e}")

    def delete_item(self):
        if not self.selected_item_id:
            messagebox.showwarning("No Selection", "Select an item first.")
            return

        item = self.item_data.get(str(self.selected_item_id))
        title = item['Title'][:60] if item else '?'

        if messagebox.askyesno(
            "Confirm Delete",
            f"Permanently delete this item?\n\n{title}\n\nThis cannot be undone.",
            icon='warning'
        ):
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM [dbo].[PMA_PunchlistItems] WHERE PunchlistItemID = ?",
                    (self.selected_item_id,)
                )
                conn.commit()
                cursor.close()
                conn.close()
                self._set_status(f"Deleted: {title}")
                self.refresh_data()
            except Exception as e:
                messagebox.showerror("Error", f"Could not delete:\n{e}")

    # -----------------------------------------------------------------
    # GENERATE PROMPT
    # -----------------------------------------------------------------
    def generate_prompt(self):
        """
        Build a context-rich starter prompt for the selected punchlist item.
        Includes: item detail, related items from same project, cross-project
        blockers/dependencies, and suggested next steps.
        Opens in a popup with Copy to Clipboard button.
        """
        if not self.selected_item_id:
            messagebox.showwarning("No Selection", "Select an item first.")
            return

        item = self.item_data.get(str(self.selected_item_id))
        if not item:
            return

        self._set_status("Generating prompt...")

        # Gather context
        try:
            all_items = fetch_all_items()
        except Exception:
            all_items = []

        project = item['Project']
        item_num = item['ItemNumber']

        # Same-project siblings (other open items)
        siblings = [i for i in all_items
                     if i['Project'] == project
                     and i['PunchlistItemID'] != item['PunchlistItemID']
                     and i['Status'] != 'Completed']

        # Cross-project items that reference this item or share blockers
        blocked_by = item.get('BlockedBy') or ''
        cross_refs = []
        for other in all_items:
            if other['PunchlistItemID'] == item['PunchlistItemID']:
                continue
            if other['Status'] == 'Completed':
                continue
            # Items that share the same blocker
            other_blocked = other.get('BlockedBy') or ''
            if blocked_by and other_blocked and blocked_by.lower() in other_blocked.lower():
                cross_refs.append(other)
                continue
            # Items whose title or description references this project or item
            other_desc = (other.get('Description') or '').lower()
            other_title = (other.get('Title') or '').lower()
            if project.lower() in other_desc or item_num.lower() in other_desc:
                cross_refs.append(other)
            elif project.lower() in other_title:
                cross_refs.append(other)

        # Build the prompt
        lines = []
        lines.append(f"PROJECT: {project}")
        lines.append(f"ITEM: {item_num} — {item['Title']}")
        lines.append(f"STATUS: {item['Status']}  |  PRIORITY: {item['Priority']}")
        if item.get('Section'):
            lines.append(f"SECTION: {item['Section']}")
        lines.append("")

        # Blockers
        if item.get('BlockedBy'):
            lines.append(f"BLOCKED BY: {item['BlockedBy']}")
            lines.append("")
        if item.get('Unlocks'):
            lines.append(f"UNLOCKS: {item['Unlocks']}")
            lines.append("")

        # Description / context
        desc = item.get('Description') or ''
        if desc:
            lines.append("CONTEXT:")
            lines.append(desc.strip())
            lines.append("")

        # Related items in the same project
        if siblings:
            lines.append(f"OTHER OPEN ITEMS IN {project} ({len(siblings)}):")
            for sib in siblings[:10]:
                pri_tag = f" [{sib['Priority']}]" if sib['Priority'] == 'High' else ""
                blocked_tag = f" (BLOCKED: {sib['BlockedBy']})" if sib.get('BlockedBy') else ""
                lines.append(f"  - {sib['ItemNumber']}: {sib['Title'][:70]}{pri_tag}{blocked_tag}")
            if len(siblings) > 10:
                lines.append(f"  ... and {len(siblings) - 10} more")
            lines.append("")

        # Cross-project connections
        if cross_refs:
            lines.append("CROSS-PROJECT CONNECTIONS:")
            seen = set()
            for ref in cross_refs:
                key = ref['PunchlistItemID']
                if key in seen:
                    continue
                seen.add(key)
                lines.append(f"  - [{ref['Project']}] {ref['ItemNumber']}: "
                             f"{ref['Title'][:60]}")
                if ref.get('BlockedBy'):
                    lines.append(f"    Shared blocker: {ref['BlockedBy']}")
            lines.append("")

        # Instructions for the AI
        lines.append("=" * 60)
        lines.append("INSTRUCTIONS:")
        lines.append("")
        lines.append(f"I'm working on {item_num} in the {project} project.")
        lines.append("Please review the context above and help me:")
        lines.append("")

        if item.get('BlockedBy'):
            lines.append(f"1. Assess options given the blocker: {item['BlockedBy']}")
            lines.append("   - Can we work around it? Partial progress possible?")
            lines.append("   - Should we escalate or follow up?")
            lines.append("2. Suggest concrete next steps I can take right now.")
            lines.append("3. If there are related items we should tackle together, flag them.")
        else:
            lines.append("1. Break this into concrete implementation steps.")
            lines.append("2. Identify any risks or dependencies I should be aware of.")
            lines.append("3. Suggest what to tackle first for maximum progress.")
            if siblings:
                lines.append("4. Flag any sibling items that should be worked alongside this one.")

        lines.append("")
        lines.append("Let's discuss approach before writing any code.")

        prompt_text = '\n'.join(lines)

        # Offer to update the pdoc (project code snapshot) before showing prompt
        self._offer_pdoc_update(project)

        # Show in popup
        self._show_prompt_popup(item, prompt_text)
        self._set_status(f"Prompt generated for {item_num}")

    def _resolve_project_folder(self, project_name):
        """Resolve the full filesystem path for a project folder."""
        base = Path(__file__).resolve().parent.parent  # PycharmProjects
        project_dir = base / project_name
        if project_dir.exists() and project_dir.is_dir():
            return project_dir
        return None

    def _offer_pdoc_update(self, project_name):
        """
        Ask the user if they want to refresh the pdoc XML for this project.
        If yes, calls GPTProjectUploadGUI.create_project_document() against
        the project folder so the code snapshot is current before starting
        a new Claude chat.
        """
        project_dir = self._resolve_project_folder(project_name)
        if not project_dir:
            return  # Folder doesn't exist, skip silently

        # Check if a pdoc file already exists and show its age
        pdoc_file = project_dir / f"pdoc_{project_name}.xml"
        age_msg = ""
        if pdoc_file.exists():
            mod_time = datetime.fromtimestamp(pdoc_file.stat().st_mtime)
            age = datetime.now() - mod_time
            if age.days > 0:
                age_msg = f"\n\nCurrent pdoc is {age.days} day(s) old ({mod_time.strftime('%Y-%m-%d %H:%M')})."
            else:
                hours = age.seconds // 3600
                age_msg = f"\n\nCurrent pdoc is {hours} hour(s) old ({mod_time.strftime('%Y-%m-%d %H:%M')})."
        else:
            age_msg = "\n\nNo pdoc file exists for this project yet."

        if not messagebox.askyesno(
            "Update Project Code Snapshot",
            f"Update pdoc_{project_name}.xml before starting?\n"
            f"This refreshes the code snapshot you upload to Claude.{age_msg}",
        ):
            return

        self._set_status(f"Generating pdoc for {project_name}...")
        self.root.update_idletasks()

        try:
            # Import the GPT project upload function
            # It lives in CRPUtils alongside this script
            from GPTProjectUploadGUI import create_project_document

            output_path = create_project_document(
                str(project_dir), prefix="", compress_output=False
            )
            self._set_status(f"pdoc updated: {output_path}")
            messagebox.showinfo(
                "pdoc Updated",
                f"Code snapshot refreshed:\n\n{output_path}"
            )
        except ImportError:
            messagebox.showwarning(
                "GPTProjectUploadGUI Not Found",
                "Could not import GPTProjectUploadGUI.py.\n"
                "Make sure it exists in the CRPUtils folder."
            )
        except Exception as e:
            messagebox.showerror(
                "pdoc Error",
                f"Failed to generate pdoc:\n\n{e}"
            )

    def _show_prompt_popup(self, item, prompt_text):
        """Display the generated prompt in a popup with copy button."""
        popup = tk.Toplevel(self.root)
        popup.title(f"Prompt — {item['ItemNumber']}: {item['Title'][:50]}")
        popup.geometry("800x600")
        popup.configure(bg=COLORS['bg'])
        popup.transient(self.root)

        # Header
        header_frame = tk.Frame(popup, bg=COLORS['header_bg'], height=40)
        header_frame.pack(fill=tk.X)
        header_frame.pack_propagate(False)

        tk.Label(
            header_frame,
            text=f"  Starter Prompt for {item['ItemNumber']}",
            font=('Segoe UI', 12, 'bold'),
            bg=COLORS['header_bg'], fg=COLORS['header_fg']
        ).pack(side=tk.LEFT, padx=10)

        tk.Label(
            header_frame,
            text=f"Copy this into a new chat in the {item['Project']} Claude project",
            font=('Segoe UI', 9, 'italic'),
            bg=COLORS['header_bg'], fg='#bdc3c7'
        ).pack(side=tk.RIGHT, padx=10)

        # Text area
        text_widget = scrolledtext.ScrolledText(
            popup, font=('Consolas', 10), wrap=tk.WORD, padx=10, pady=10
        )
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 5))
        text_widget.insert('1.0', prompt_text)

        # Button bar
        btn_bar = tk.Frame(popup, bg=COLORS['bg'])
        btn_bar.pack(fill=tk.X, padx=10, pady=8)

        def copy_to_clipboard():
            current_text = text_widget.get('1.0', tk.END).strip()
            popup.clipboard_clear()
            popup.clipboard_append(current_text)
            popup.update()
            copied_label.config(text="Copied!")
            popup.after(2000, lambda: copied_label.config(text=""))

        self._make_button(btn_bar, "Copy to Clipboard", copy_to_clipboard,
                          '#8e44ad').pack(side=tk.LEFT, padx=5)

        copied_label = tk.Label(btn_bar, text="", bg=COLORS['bg'],
                                fg=COLORS['btn_success'],
                                font=('Segoe UI', 9, 'bold'))
        copied_label.pack(side=tk.LEFT, padx=5)

        self._make_button(btn_bar, "Close", popup.destroy,
                          COLORS['btn_primary']).pack(side=tk.RIGHT, padx=5)

    # -----------------------------------------------------------------
    # ADD NEW ITEM
    # -----------------------------------------------------------------
    def add_new_item(self):
        """Open a dialog to add a new punchlist item."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add New Punchlist Item")
        dialog.geometry("600x520")
        dialog.configure(bg=COLORS['bg'])
        dialog.transient(self.root)
        dialog.grab_set()

        # Get merged project list (SQL + filesystem folders)
        merged_projects, new_folders = fetch_merged_project_list()
        new_set = set(new_folders)

        # Project
        tk.Label(dialog, text="Project:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=0, column=0, sticky='e', padx=8, pady=4)
        proj_combo = ttk.Combobox(dialog, values=merged_projects, width=25)
        proj_combo.grid(row=0, column=1, sticky='w', padx=8, pady=4)
        if merged_projects:
            proj_combo.set(merged_projects[0])

        # Label that shows "(new punchlist)" when a folder without one is selected
        new_label = tk.Label(dialog, text="", bg=COLORS['bg'],
                             font=('Segoe UI', 9, 'italic'), fg=COLORS['btn_success'])
        new_label.grid(row=0, column=2, sticky='w', padx=4, pady=4)

        # Auto-generate item number when project changes
        item_num_var = tk.StringVar()

        def update_item_num(*args):
            proj = proj_combo.get()
            if proj:
                try:
                    item_num_var.set(get_next_item_number(proj))
                except Exception:
                    item_num_var.set('')
                # Show/hide new punchlist indicator
                if proj in new_set:
                    new_label.config(text="(new punchlist)")
                else:
                    new_label.config(text="")

        proj_combo.bind('<<ComboboxSelected>>', update_item_num)
        update_item_num()

        # Item Number
        tk.Label(dialog, text="Item #:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=1, column=0, sticky='e', padx=8, pady=4)
        item_num_entry = tk.Entry(dialog, textvariable=item_num_var, width=15,
                                  font=('Segoe UI', 9))
        item_num_entry.grid(row=1, column=1, sticky='w', padx=8, pady=4)

        # Title
        tk.Label(dialog, text="Title:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=2, column=0, sticky='e', padx=8, pady=4)
        title_entry = tk.Entry(dialog, width=50, font=('Segoe UI', 9))
        title_entry.grid(row=2, column=1, sticky='w', padx=8, pady=4)

        # Status
        tk.Label(dialog, text="Status:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=3, column=0, sticky='e', padx=8, pady=4)
        status_combo = ttk.Combobox(dialog, values=VALID_STATUSES,
                                     state='readonly', width=14)
        status_combo.grid(row=3, column=1, sticky='w', padx=8, pady=4)
        status_combo.set('Open')

        # Priority
        tk.Label(dialog, text="Priority:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=4, column=0, sticky='e', padx=8, pady=4)
        priority_combo = ttk.Combobox(dialog, values=VALID_PRIORITIES,
                                       state='readonly', width=10)
        priority_combo.grid(row=4, column=1, sticky='w', padx=8, pady=4)
        priority_combo.set('Medium')

        # Section
        tk.Label(dialog, text="Section:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=5, column=0, sticky='e', padx=8, pady=4)
        section_entry = tk.Entry(dialog, width=30, font=('Segoe UI', 9))
        section_entry.grid(row=5, column=1, sticky='w', padx=8, pady=4)

        # Blocked By
        tk.Label(dialog, text="Blocked By:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=6, column=0, sticky='e', padx=8, pady=4)
        blocked_entry = tk.Entry(dialog, width=40, font=('Segoe UI', 9))
        blocked_entry.grid(row=6, column=1, sticky='w', padx=8, pady=4)

        # Unlocks
        tk.Label(dialog, text="Unlocks:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=7, column=0, sticky='e', padx=8, pady=4)
        unlocks_entry = tk.Entry(dialog, width=40, font=('Segoe UI', 9))
        unlocks_entry.grid(row=7, column=1, sticky='w', padx=8, pady=4)

        # Description
        tk.Label(dialog, text="Description:", bg=COLORS['bg'],
                 font=('Segoe UI', 9)).grid(row=8, column=0, sticky='ne', padx=8, pady=4)
        desc_text = scrolledtext.ScrolledText(
            dialog, height=6, width=50, font=('Consolas', 9), wrap=tk.WORD
        )
        desc_text.grid(row=8, column=1, padx=8, pady=4)

        # Save button
        def do_save():
            title = title_entry.get().strip()
            if not title:
                messagebox.showwarning("Required", "Title is required.")
                return

            try:
                insert_item(
                    project=proj_combo.get(),
                    item_number=item_num_var.get(),
                    title=title,
                    description=desc_text.get('1.0', tk.END).strip() or None,
                    status=status_combo.get(),
                    priority=priority_combo.get(),
                    section=section_entry.get().strip() or None,
                    blocked_by=blocked_entry.get().strip() or None,
                    unlocks=unlocks_entry.get().strip() or None
                )
                dialog.destroy()
                self._set_status(f"Added: {item_num_var.get()} - {title[:50]}")
                self.refresh_data()
            except Exception as e:
                messagebox.showerror("Error", f"Could not save:\n{e}")

        btn_frame = tk.Frame(dialog, bg=COLORS['bg'])
        btn_frame.grid(row=9, column=0, columnspan=2, pady=12)
        self._make_button(btn_frame, "Save", do_save,
                          COLORS['btn_success']).pack(side=tk.LEFT, padx=5)
        self._make_button(btn_frame, "Cancel", dialog.destroy,
                          COLORS['btn_danger']).pack(side=tk.LEFT, padx=5)

    # -----------------------------------------------------------------
    # TOOLBAR ACTIONS
    # -----------------------------------------------------------------
    def run_ingest(self):
        """Run the punchlist ingester (markdown -> SQL)."""
        if not messagebox.askyesno(
            "Run Ingest",
            "This will scan all *_punchlist.md files and update the SQL table.\n\n"
            "Existing items will be updated if changed.\nNew items will be inserted.\n\n"
            "Continue?"
        ):
            return

        self._set_status("Running ingest...")
        try:
            from Punchlist_manager import run_ingest
            result = run_ingest()
            if result:
                msg = (f"Ingest complete:\n\n"
                       f"  Inserted: {result['inserted']}\n"
                       f"  Updated: {result['updated']}\n"
                       f"  Unchanged: {result['unchanged']}\n"
                       f"  Errors: {result['errors']}")
                messagebox.showinfo("Ingest Complete", msg)
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Ingest Error", f"Failed:\n{e}")
            self._set_status("Ingest failed")

    def run_export(self):
        """Run the markdown exporter (SQL -> markdown files)."""
        if not messagebox.askyesno(
            "Run Export",
            "This will write normalized *_Punchlist.md files\n"
            "back to each project folder.\n\n"
            "Existing punchlist files will be overwritten.\n\nContinue?"
        ):
            return

        self._set_status("Running export...")
        try:
            from Punchlist_manager import run_export
            files = run_export()
            if files:
                file_list = '\n'.join(f"  {p}: {f}" for p, f in files.items())
                messagebox.showinfo("Export Complete",
                                    f"Exported {len(files)} file(s):\n\n{file_list}")
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed:\n{e}")
            self._set_status("Export failed")

    def show_summary(self):
        """Show a summary popup with stats from the table."""
        try:
            stats = get_summary_stats()
            items = fetch_all_items()

            # By project
            by_project = {}
            for item in items:
                proj = item['Project']
                by_project.setdefault(proj, {'total': 0, 'open': 0, 'high': 0, 'blocked': 0})
                by_project[proj]['total'] += 1
                if item['Status'] != 'Completed':
                    by_project[proj]['open'] += 1
                    if item['Priority'] == 'High':
                        by_project[proj]['high'] += 1
                    if item['BlockedBy']:
                        by_project[proj]['blocked'] += 1

            # Build summary text
            lines = []
            lines.append(f"{'='*50}")
            lines.append(f"PMA_PunchlistItems Summary")
            lines.append(f"As of: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            lines.append(f"{'='*50}")
            lines.append(f"")
            lines.append(f"Total items:    {stats['total']}")
            lines.append(f"  Open:         {stats['open']}")
            lines.append(f"  In Progress:  {stats['in_progress']}")
            lines.append(f"  Blocked:      {stats['blocked']}")
            lines.append(f"  Completed:    {stats['completed']}")
            lines.append(f"  High Priority:{stats['high_pri']}")
            lines.append(f"")
            lines.append(f"{'Project':<22} {'Total':>6} {'Open':>6} {'High':>6} {'Blocked':>8}")
            lines.append(f"{'-'*48}")
            for proj in sorted(by_project.keys()):
                p = by_project[proj]
                lines.append(f"{proj:<22} {p['total']:>6} {p['open']:>6} "
                             f"{p['high']:>6} {p['blocked']:>8}")

            # Blocked items
            blocked = [i for i in items if i['BlockedBy'] and i['Status'] != 'Completed']
            if blocked:
                lines.append(f"")
                lines.append(f"{'='*50}")
                lines.append(f"BLOCKED ITEMS")
                lines.append(f"{'='*50}")
                for item in blocked:
                    lines.append(f"  [{item['Project']}] {item['ItemNumber']}: "
                                 f"{item['Title'][:50]}")
                    lines.append(f"    Blocked by: {item['BlockedBy']}")

            # Show in popup
            summary_win = tk.Toplevel(self.root)
            summary_win.title("Punchlist Summary")
            summary_win.geometry("600x500")
            summary_win.configure(bg=COLORS['bg'])
            summary_win.transient(self.root)

            text_widget = scrolledtext.ScrolledText(
                summary_win, font=('Consolas', 10), wrap=tk.WORD
            )
            text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            text_widget.insert('1.0', '\n'.join(lines))
            text_widget.config(state=tk.DISABLED)

            self._make_button(summary_win, "Close", summary_win.destroy,
                              COLORS['btn_primary']).pack(pady=5)

        except Exception as e:
            messagebox.showerror("Error", f"Could not generate summary:\n{e}")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    root = tk.Tk()
    app = PunchlistApp(root)
    root.mainloop()