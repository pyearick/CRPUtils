"""
punchlist_manager.py - Punchlist SQL Table Manager
====================================================

Reads *_punchlist.md files from sibling project folders, normalizes them
into a canonical format, and stores them in PMA_PunchlistItems (CRPAF).

The SQL table becomes the source of truth. Clean markdown files can be
regenerated from the table back to each project folder at any time.

Phases:
  1. INGEST  - parse markdown -> SQL table (upsert)
  2. EXPORT  - SQL table -> clean markdown per project
  3. COLLATE - (future) collator reads SQL instead of markdown

Lives in: CRPUtils folder
Table:    [CRPAF].[dbo].[PMA_PunchlistItems] on BI-SQL001

Author: Pat Yearick
Created: February 2026
"""

import os
import sys
import re
import logging
import pyodbc
import hashlib
from pathlib import Path
from datetime import datetime

# Import the parsing logic from the existing collator
from PunchlistCollator import (
    find_punchlist_files,
    parse_punchlist_file,
    PunchlistItem
)

# =============================================================================
# CONFIGURATION
# =============================================================================

SQL_SERVER = "BI-SQL001"
SQL_DATABASE = "CRPAF"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
LOG_FILE = r"C:\Logs\punchlist_manager.log"

# Set up logging
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
# DATABASE UTILITIES
# =============================================================================

def get_connection():
    """Get database connection using Windows auth."""
    return pyodbc.connect(
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )


def ensure_table_exists():
    """
    Create PMA_PunchlistItems table if it doesn't exist.
    This is the source of truth for all punchlist items across projects.
    """
    conn = get_connection()
    cursor = conn.cursor()

    create_sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PMA_PunchlistItems')
    BEGIN
        CREATE TABLE [dbo].[PMA_PunchlistItems] (
            [PunchlistItemID]   INT IDENTITY(1,1) PRIMARY KEY,
            [Project]           NVARCHAR(100) NOT NULL,
            [ItemNumber]        NVARCHAR(50) NULL,
            [Title]             NVARCHAR(500) NOT NULL,
            [Description]       NVARCHAR(MAX) NULL,
            [Status]            NVARCHAR(20) NOT NULL DEFAULT 'Open',
            [Priority]          NVARCHAR(20) NOT NULL DEFAULT 'Medium',
            [Section]           NVARCHAR(200) NULL,
            [BlockedBy]         NVARCHAR(500) NULL,
            [Unlocks]           NVARCHAR(500) NULL,
            [SourceFile]        NVARCHAR(500) NULL,
            [ContentHash]       NVARCHAR(64) NULL,
            [CreatedDate]       DATETIME NOT NULL DEFAULT GETDATE(),
            [LastModifiedDate]  DATETIME NOT NULL DEFAULT GETDATE(),
            [CompletedDate]     DATETIME NULL,
            [IngestedDate]      DATETIME NOT NULL DEFAULT GETDATE()
        )

        CREATE INDEX IX_PunchlistItems_Project
            ON [dbo].[PMA_PunchlistItems](Project)

        CREATE INDEX IX_PunchlistItems_Status
            ON [dbo].[PMA_PunchlistItems](Status)

        CREATE INDEX IX_PunchlistItems_Priority
            ON [dbo].[PMA_PunchlistItems](Priority)

        CREATE UNIQUE INDEX UX_PunchlistItems_ProjectItem
            ON [dbo].[PMA_PunchlistItems](Project, ItemNumber)
            WHERE ItemNumber IS NOT NULL

        PRINT 'Created PMA_PunchlistItems table'
    END
    """

    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Ensured PMA_PunchlistItems table exists")


# =============================================================================
# INGESTION LOGIC
# =============================================================================

def compute_content_hash(item):
    """
    Generate a hash of the item's content so we can detect changes
    without relying solely on title matching.
    """
    content = f"{item.title}|{item.body}|{item.section}"
    return hashlib.sha256(content.encode('utf-8', errors='replace')).hexdigest()[:32]


def detect_blocked_by(body):
    """
    Scan item body text for blocker references.
    Looks for patterns like:
      - "Blocked by: ..."
      - "depends on ..."
      - "waiting on Byron"
      - "prerequisite for ..."
    Returns a comma-separated string of detected blockers, or None.
    """
    blockers = []

    # Explicit blocker patterns
    patterns = [
        r'[Bb]locked\s+by[:\s]+([^\n.;]+)',
        r'[Dd]epend(?:s|encies?)\s*(?:on)?[:\s]+([^\n.;]+)',
        r'[Ww]aiting\s+(?:on|for)\s+([^\n.;]+)',
        r'[Pp]rerequisite[:\s]+([^\n.;]+)',
        r'[Nn]eeded?\s+from\s+([^\n.;]+)',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, body)
        for match in matches:
            # Clean up markdown artifacts and trim
            cleaned = match.strip().lstrip('*').rstrip(',.*').strip()
            # Skip false positives
            if not cleaned or len(cleaned) > 200:
                continue
            lower = cleaned.lower()
            if lower.startswith('none') or lower.startswith('n/a'):
                continue
            if lower.startswith('for item') or lower.startswith('for ls'):
                # This is "prerequisite FOR something" — that's an unlock, not a blocker
                continue
            blockers.append(cleaned)

    return '; '.join(blockers) if blockers else None


def detect_unlocks(body):
    """
    Scan item body text for items this unlocks.
    Looks for patterns like:
      - "blocker for all kit ..."
      - "prerequisite for Item 1"
      - "unlocks ..."
      - "enables ..."
    """
    unlocks = []

    patterns = [
        r'[Bb]locker\s+for\s+([^\n.;]+)',
        r'[Uu]nlocks?\s+([^\n.;]+)',
        r'[Ee]nables?\s+([^\n.;]+)',
        r'[Rr]equired\s+(?:for|before)\s+([^\n.;]+)',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, body)
        for match in matches:
            cleaned = match.strip().rstrip(',.')
            if cleaned and len(cleaned) < 200:
                unlocks.append(cleaned)

    return '; '.join(unlocks) if unlocks else None


def normalize_priority(priority):
    """Ensure priority is one of High/Medium/Low. Default to Medium."""
    if priority and priority.capitalize() in ('High', 'Medium', 'Low'):
        return priority.capitalize()
    return 'Medium'


def normalize_status(status):
    """Ensure status is one of the canonical values. Default to Open."""
    if not status:
        return 'Open'
    status_lower = status.lower().strip()
    mapping = {
        'not started': 'Open',
        'open': 'Open',
        'in progress': 'In Progress',
        'started': 'In Progress',
        'blocked': 'Blocked',
        'completed': 'Completed',
        'done': 'Completed',
        'shipped': 'Completed',
    }
    return mapping.get(status_lower, 'Open')


def generate_item_number(project, index):
    """
    Generate a stable item number for items that don't have one.
    Uses project prefix + sequential number.
    """
    # Map project names to short prefixes
    prefix_map = {
        'BigDawgHunt': 'BDH',
        'LostSales': 'LS',
        'PMAssistant': 'PMA',
    }

    # Try known prefixes, fall back to first 3 chars uppercase
    prefix = prefix_map.get(project, project[:3].upper())
    return f"{prefix}-{index:03d}"


def upsert_items(items_by_project):
    """
    Insert or update items in PMA_PunchlistItems.

    Matching logic:
    - If item has an item_id (ITEM 1, PL-001, etc.), match on Project + ItemNumber
    - If no item_id, match on Project + Title (fuzzy via first 200 chars)
    - If no match found, INSERT as new
    - If match found and content changed (hash differs), UPDATE
    - If match found and content same, skip
    """
    conn = get_connection()
    cursor = conn.cursor()

    stats = {'inserted': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

    for project, items in items_by_project.items():

        # Assign item numbers to items that don't have them
        # First, get the max existing number for this project prefix
        prefix_map = {
            'BigDawgHunt': 'BDH',
            'LostSales': 'LS',
            'PMAssistant': 'PMA',
        }
        prefix = prefix_map.get(project, project[:3].upper())

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

        for item in items:
            try:
                content_hash = compute_content_hash(item)
                priority = normalize_priority(item.priority)
                status = normalize_status(item.status)
                blocked_by = detect_blocked_by(item.body)
                unlocks = detect_unlocks(item.body)
                description = item.body.strip() if item.body else None

                # Determine the item number
                item_number = item.item_id
                if not item_number:
                    # Check if this title already exists in the table
                    cursor.execute("""
                        SELECT ItemNumber FROM [dbo].[PMA_PunchlistItems]
                        WHERE Project = ? AND LEFT(Title, 200) = LEFT(?, 200)
                    """, (project, item.title))
                    existing = cursor.fetchone()
                    if existing:
                        item_number = existing[0]
                    else:
                        item_number = f"{prefix}-{next_num:03d}"
                        next_num += 1

                # Check if this item already exists
                cursor.execute("""
                    SELECT PunchlistItemID, ContentHash
                    FROM [dbo].[PMA_PunchlistItems]
                    WHERE Project = ? AND ItemNumber = ?
                """, (project, item_number))

                existing_row = cursor.fetchone()

                if existing_row:
                    existing_id, existing_hash = existing_row
                    if existing_hash == content_hash:
                        stats['unchanged'] += 1
                    else:
                        # Content changed — update
                        cursor.execute("""
                            UPDATE [dbo].[PMA_PunchlistItems]
                            SET Title = ?,
                                Description = ?,
                                Status = ?,
                                Priority = ?,
                                Section = ?,
                                BlockedBy = ?,
                                Unlocks = ?,
                                SourceFile = ?,
                                ContentHash = ?,
                                LastModifiedDate = GETDATE()
                            WHERE PunchlistItemID = ?
                        """, (
                            item.title[:500],
                            description,
                            status,
                            priority,
                            item.section,
                            blocked_by,
                            unlocks,
                            item.source_project,
                            content_hash,
                            existing_id
                        ))
                        stats['updated'] += 1
                        logger.info(f"  UPDATED: [{project}] {item_number}: {item.title[:60]}")
                else:
                    # New item — insert
                    cursor.execute("""
                        INSERT INTO [dbo].[PMA_PunchlistItems]
                            (Project, ItemNumber, Title, Description, Status,
                             Priority, Section, BlockedBy, Unlocks, SourceFile,
                             ContentHash, CreatedDate, LastModifiedDate, IngestedDate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), GETDATE())
                    """, (
                        project,
                        item_number,
                        item.title[:500],
                        description,
                        status,
                        priority,
                        item.section,
                        blocked_by,
                        unlocks,
                        item.source_project,
                        content_hash
                    ))
                    stats['inserted'] += 1
                    logger.info(f"  INSERTED: [{project}] {item_number}: {item.title[:60]}")

            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  ERROR on [{project}] {item.title[:60]}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    return stats


# =============================================================================
# MARKDOWN EXPORT - Generate clean punchlists from SQL
# =============================================================================

def export_markdown(output_dir=None):
    """
    Read all items from PMA_PunchlistItems and generate clean, normalized
    markdown files — one per project — back to the project folders.

    If output_dir is None, writes to sibling folders of CRPUtils
    (same structure the scanner reads from).
    """
    if output_dir is None:
        output_dir = Path(__file__).resolve().parent.parent

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT Project, ItemNumber, Title, Description, Status, Priority,
               Section, BlockedBy, Unlocks, CompletedDate
        FROM [dbo].[PMA_PunchlistItems]
        ORDER BY Project,
            CASE Priority
                WHEN 'High' THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'Low' THEN 3
                ELSE 4
            END,
            Section,
            ItemNumber
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    if not rows:
        logger.warning("No items in PMA_PunchlistItems — nothing to export.")
        return {}

    # Group by project
    by_project = {}
    for row in rows:
        item = dict(zip(columns, row))
        by_project.setdefault(item['Project'], []).append(item)

    # Map project names to filename prefixes
    prefix_map = {
        'BigDawgHunt': 'BDH',
        'LostSales': 'LS',
        'PMAssistant': 'PMA',
    }

    files_written = {}

    for project, items in by_project.items():
        prefix = prefix_map.get(project, project[:3].upper())
        filename = f"{prefix}_Punchlist.md"

        # Separate open vs completed
        open_items = [i for i in items if i['Status'] != 'Completed']
        completed_items = [i for i in items if i['Status'] == 'Completed']

        lines = []
        lines.append(f"# {project} Punch List\n")
        lines.append(f"_Auto-generated from PMA_PunchlistItems — "
                      f"{datetime.now().strftime('%Y-%m-%d %H:%M')}_\n")
        lines.append(f"---\n")

        # Open items grouped by section
        current_section = None
        for item in open_items:
            section = item['Section'] or 'General'
            if section != current_section:
                current_section = section
                lines.append(f"\n## {current_section}\n")

            # Item header
            status_icon = {
                'Open': '🔲',
                'In Progress': '🔄',
                'Blocked': '🚫',
            }.get(item['Status'], '🔲')

            lines.append(f"### {status_icon} {item['ItemNumber']}: {item['Title']}")
            lines.append(f"- **Status:** {item['Status']}")
            lines.append(f"- **Priority:** {item['Priority']}")

            if item['BlockedBy']:
                lines.append(f"- **Blocked By:** {item['BlockedBy']}")
            if item['Unlocks']:
                lines.append(f"- **Unlocks:** {item['Unlocks']}")

            if item['Description']:
                lines.append(f"\n{item['Description']}")

            lines.append("")  # blank line between items

        # Completed section
        if completed_items:
            lines.append(f"\n---\n")
            lines.append(f"## ✅ COMPLETED\n")
            for item in completed_items:
                completed_str = ""
                if item['CompletedDate']:
                    completed_str = f" ({item['CompletedDate'].strftime('%Y-%m-%d')})"
                lines.append(f"- ~~{item['ItemNumber']}: {item['Title']}~~{completed_str}")

        lines.append(f"\n---\n")
        lines.append(f"_Last updated: {datetime.now().strftime('%Y-%m-%d')}_\n")

        # Write the file
        # Try to write to the project's folder
        project_dir = output_dir / project
        if project_dir.exists():
            filepath = project_dir / filename
        else:
            # Fall back to a subfolder of the output dir
            filepath = output_dir / filename
            logger.warning(f"Project folder {project_dir} not found, "
                           f"writing {filename} to {output_dir}")

        filepath.write_text('\n'.join(lines), encoding='utf-8')
        files_written[project] = str(filepath)
        logger.info(f"Exported: {filepath} ({len(open_items)} open, "
                     f"{len(completed_items)} completed)")

    return files_written


# =============================================================================
# REPORTING - Console summary of what's in the table
# =============================================================================

def print_table_summary():
    """Print a summary of what's in PMA_PunchlistItems."""
    conn = get_connection()
    cursor = conn.cursor()

    # Overall counts
    cursor.execute("""
        SELECT
            COUNT(*) AS Total,
            SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenCount,
            SUM(CASE WHEN Status = 'In Progress' THEN 1 ELSE 0 END) AS InProgressCount,
            SUM(CASE WHEN Status = 'Blocked' THEN 1 ELSE 0 END) AS BlockedCount,
            SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) AS CompletedCount
        FROM [dbo].[PMA_PunchlistItems]
    """)
    totals = cursor.fetchone()

    # By project
    cursor.execute("""
        SELECT Project,
            COUNT(*) AS Total,
            SUM(CASE WHEN Status <> 'Completed' THEN 1 ELSE 0 END) AS OpenItems,
            SUM(CASE WHEN Priority = 'High' AND Status <> 'Completed' THEN 1 ELSE 0 END) AS HighPri,
            SUM(CASE WHEN BlockedBy IS NOT NULL AND Status <> 'Completed' THEN 1 ELSE 0 END) AS Blocked
        FROM [dbo].[PMA_PunchlistItems]
        GROUP BY Project
        ORDER BY SUM(CASE WHEN Status <> 'Completed' THEN 1 ELSE 0 END) DESC
    """)
    project_rows = cursor.fetchall()

    # Items with blockers
    cursor.execute("""
        SELECT Project, ItemNumber, Title, BlockedBy
        FROM [dbo].[PMA_PunchlistItems]
        WHERE BlockedBy IS NOT NULL AND Status <> 'Completed'
        ORDER BY Project, ItemNumber
    """)
    blocked_rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Print report
    print("\n" + "=" * 70)
    print("PMA_PunchlistItems - TABLE SUMMARY")
    print(f"As of: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    print(f"\nTotal items:    {totals[0]}")
    print(f"  Open:         {totals[1]}")
    print(f"  In Progress:  {totals[2]}")
    print(f"  Blocked:      {totals[3]}")
    print(f"  Completed:    {totals[4]}")

    print(f"\n{'Project':<25} {'Total':>6} {'Open':>6} {'High':>6} {'Blocked':>8}")
    print("-" * 55)
    for row in project_rows:
        print(f"{row[0]:<25} {row[1]:>6} {row[2]:>6} {row[3]:>6} {row[4]:>8}")

    if blocked_rows:
        print(f"\n{'='*70}")
        print("BLOCKED ITEMS")
        print(f"{'='*70}")
        for row in blocked_rows:
            print(f"  [{row[0]}] {row[1]}: {row[2][:60]}")
            print(f"    Blocked by: {row[3]}")


# =============================================================================
# MAIN
# =============================================================================

def run_ingest(base_dir=None):
    """
    Main ingestion entry point.
    1. Ensure table exists
    2. Find and parse all punchlist markdown files
    3. Upsert into PMA_PunchlistItems
    4. Print summary
    """
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent

    logger.info("=" * 60)
    logger.info("PUNCHLIST MANAGER - Ingestion Starting")
    logger.info(f"Base directory: {base_dir}")
    logger.info("=" * 60)

    # Step 1: Ensure table
    ensure_table_exists()

    # Step 2: Find and parse
    punchlist_files = find_punchlist_files(base_dir)
    if not punchlist_files:
        logger.warning("No punchlist files found.")
        return

    items_by_project = {}
    total_items = 0
    for pf in punchlist_files:
        items = parse_punchlist_file(pf)
        if items:
            project = items[0].source_project
            items_by_project[project] = items
            total_items += len(items)

    logger.info(f"Parsed {total_items} items from {len(punchlist_files)} files")

    # Step 3: Upsert
    stats = upsert_items(items_by_project)

    logger.info(f"Ingestion complete: "
                f"{stats['inserted']} inserted, "
                f"{stats['updated']} updated, "
                f"{stats['unchanged']} unchanged, "
                f"{stats['errors']} errors")

    # Step 4: Summary
    print_table_summary()

    return stats


def run_export(output_dir=None):
    """
    Export entry point.
    Reads PMA_PunchlistItems and writes clean markdown files.
    """
    logger.info("=" * 60)
    logger.info("PUNCHLIST MANAGER - Export Starting")
    logger.info("=" * 60)

    files = export_markdown(output_dir)

    if files:
        print(f"\nExported {len(files)} punchlist file(s):")
        for project, filepath in files.items():
            print(f"  {project}: {filepath}")
    else:
        print("\nNo items to export.")

    return files


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Force UTF-8 output
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

    # Parse command line args
    mode = 'ingest'  # default
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()

    try:
        print("\n" + "=" * 60)
        print("PUNCHLIST MANAGER - SQL-Backed Punchlist System")
        print("=" * 60)

        if mode == 'ingest':
            print("Mode: INGEST (markdown -> SQL)\n")
            result = run_ingest()
            if result:
                print(f"\nIngestion: {result['inserted']} new, "
                      f"{result['updated']} updated, "
                      f"{result['unchanged']} unchanged, "
                      f"{result['errors']} errors")

        elif mode == 'export':
            print("Mode: EXPORT (SQL -> markdown)\n")
            run_export()

        elif mode == 'summary':
            print("Mode: SUMMARY\n")
            ensure_table_exists()
            print_table_summary()

        elif mode == 'both':
            print("Mode: INGEST + EXPORT\n")
            run_ingest()
            print("\n" + "-" * 60)
            print("Now exporting clean markdown files...\n")
            run_export()

        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python punchlist_manager.py [ingest|export|summary|both]")
            sys.exit(1)

        print(f"\nLog: {LOG_FILE}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Punchlist manager failed: {e}", exc_info=True)
        sys.exit(1)