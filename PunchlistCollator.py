"""
punchlist_collator.py - Cross-Project Punchlist Intelligence
=============================================================

Scans sibling project folders for *_punchlist.md files, parses open items,
and uses Azure OpenAI to generate an intelligent work plan with dependency
analysis, sequencing recommendations, and blocker identification.

Lives in: CRPUtils folder
Scans:    Sibling folders at the same level

Output:   Console + C:\\Logs\\punchlist_collator.log

Azure OpenAI is used for the "smart" analysis layer so inference costs
run through CRP's Azure subscription, not personal API keys.

Author: Pat Yearick
Created: February 2026
"""

import os
import sys
import re
import glob
import logging
from pathlib import Path
from datetime import datetime
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load environment variables (expects .env in project root or CRPUtils)
load_dotenv()
# Also try parent-level .env files (PMAssistant, etc.)
for env_candidate in [
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent.parent / "PMAssistant" / ".env",
]:
    if env_candidate.exists():
        load_dotenv(env_candidate)
        break

# =============================================================================
# CONFIGURATION
# =============================================================================

LOG_FILE = r"C:\Logs\punchlist_collator.log"
AZURE_MODEL = "gpt-4o-3"  # Azure deployment name - matches SynthesisAgent

# Set up logging - console + file, no date suffix on log name
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
# MARKDOWN PARSING
# =============================================================================

class PunchlistItem:
    """Represents a single open work item parsed from a punchlist."""

    def __init__(self, source_project, section, title, body="",
                 priority=None, status=None, item_id=None, depends_on=None):
        self.source_project = source_project
        self.section = section
        self.title = title
        self.body = body
        self.priority = priority
        self.status = status
        self.item_id = item_id
        self.depends_on = depends_on

    def __repr__(self):
        pri = f" [{self.priority}]" if self.priority else ""
        return f"[{self.source_project}] {self.section} > {self.title}{pri}"

    def to_summary(self):
        """Compact text representation for LLM prompt."""
        lines = [f"- **{self.title}**"]
        if self.priority:
            lines[0] += f"  (Priority: {self.priority})"
        if self.status:
            lines[0] += f"  (Status: {self.status})"
        if self.depends_on:
            lines.append(f"  Depends on: {self.depends_on}")
        if self.body:
            # Trim body to keep prompt size reasonable
            trimmed = self.body.strip()
            if len(trimmed) > 500:
                trimmed = trimmed[:500] + "..."
            lines.append(f"  {trimmed}")
        return "\n".join(lines)


def find_punchlist_files(base_dir):
    """
    From base_dir (CRPUtils location), go up one level and scan each
    sibling folder for *_punchlist.md files (case-insensitive).
    """
    parent = Path(base_dir).resolve().parent
    found = []

    logger.info(f"Scanning for punchlist files under: {parent}")

    for child in sorted(parent.iterdir()):
        if not child.is_dir():
            # Also check top-level files (punchlist might be at parent level)
            if child.is_file() and child.name.lower().endswith('_punchlist.md'):
                found.append(child)
            continue
        # Skip hidden folders and common non-project dirs
        if child.name.startswith('.') or child.name in ('__pycache__', '.venv', 'node_modules', 'PunchlistReview'):
            continue
        # Glob for *_punchlist.md (case-insensitive via manual check)
        for f in child.iterdir():
            if f.is_file() and re.match(r'.+_punchlist\.md$', f.name, re.IGNORECASE):
                found.append(f)

    logger.info(f"Found {len(found)} punchlist file(s): {[f.name for f in found]}")
    return found


def extract_priority(text):
    """Pull priority from text like '**Priority:** High'."""
    match = re.search(r'\*\*Priority:\*\*\s*(High|Medium|Low)', text, re.IGNORECASE)
    return match.group(1).capitalize() if match else None


def extract_status(text):
    """Pull status from text like '**Status:** Not Started'."""
    match = re.search(r'\*\*Status:\*\*\s*([^\n*]+)', text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def extract_depends_on(text):
    """
    Pull dependency from text like '**Depends on:** RIG-002' or
    '* Depends on: RIG-002'.
    Handles both bold-markdown and plain bullet formats.
    """
    # Bold markdown: **Depends on:** value
    match = re.search(r'\*\*Depends\s+on:\*\*\s*([^\n]+)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Plain bullet: * Depends on: value  or  - Depends on: value
    match = re.search(r'^[*\-]\s*Depends\s+on:\s*([^\n]+)', text, re.IGNORECASE | re.MULTILINE)
    if match:
        return match.group(1).strip()
    return None


def strip_metadata_from_body(body):
    """
    Remove metadata lines (Status, Priority, Depends on) from the body text
    so they don't duplicate what's already captured as structured fields.
    """
    lines = body.split('\n')
    cleaned = []
    for line in lines:
        stripped = line.strip()
        # Skip lines that are metadata bullets
        if re.match(r'^[*\-]\s*\*\*(Status|Priority|Depends\s+on|Added):\*\*', stripped, re.IGNORECASE):
            continue
        if re.match(r'^[*\-]\s*(Status|Priority|Depends\s+on|Added):\s', stripped, re.IGNORECASE):
            continue
        cleaned.append(line)
    return '\n'.join(cleaned).strip()


def is_completed_section(header_text):
    """Check if a section header indicates completed items."""
    lower = header_text.lower()
    return any(marker in lower for marker in ['completed', 'shipped', 'done', '✅'])


def parse_structured_items(content, source_project):
    """
    Parse punchlist files that use ## ITEM N: Title format (LS, PMA style).
    Each item is a ## or ### headed block with body text below.
    """
    items = []
    # Split on ## headers (level 2 or 3)
    sections = re.split(r'^(#{2,3})\s+', content, flags=re.MULTILINE)

    current_section = "General"
    in_completed = False
    i = 1  # sections[0] is text before first header

    while i < len(sections):
        header_marker = sections[i]
        if i + 1 < len(sections):
            header_and_body = sections[i + 1]
        else:
            header_and_body = ""
        i += 2

        # Split header line from body
        lines = header_and_body.split('\n', 1)
        header_text = lines[0].strip()
        body = lines[1].strip() if len(lines) > 1 else ""

        # Check if entering/leaving completed section
        if is_completed_section(header_text):
            in_completed = True
            continue

        # A new top-level section resets completed flag
        if header_marker == '##' and not is_completed_section(header_text):
            # Check if this is a category header or an item
            item_match = re.match(r'[🔲⬜]\s*ITEM\s*(\d+):\s*(.+)', header_text)
            if item_match:
                if not in_completed:
                    item_id = item_match.group(1)
                    title = item_match.group(2).strip()
                    priority = extract_priority(body)
                    status = extract_status(body)
                    depends_on = extract_depends_on(body)
                    clean_body = strip_metadata_from_body(body)
                    items.append(PunchlistItem(
                        source_project=source_project,
                        section=current_section,
                        title=title,
                        body=clean_body,
                        priority=priority,
                        status=status,
                        item_id=f"ITEM {item_id}",
                        depends_on=depends_on
                    ))
            else:
                # It's a section header
                current_section = header_text.strip()
                in_completed = is_completed_section(current_section)

    return items


def parse_bullet_items(content, source_project):
    """
    Parse punchlist files that use ### Section > - bullet format (BDH style).
    Only used for files that don't have PL-NNN or ITEM N blocks.
    """
    items = []
    current_section = "General"
    in_completed = False

    for line in content.split('\n'):
        # Detect section headers
        header_match = re.match(r'^(#{2,3})\s+(.+)', line)
        if header_match:
            section_text = header_match.group(2).strip()
            if is_completed_section(section_text):
                in_completed = True
                continue
            else:
                current_section = section_text
                in_completed = False
                continue

        if in_completed:
            continue

        # Detect bullet items (- at start, not indented)
        bullet_match = re.match(r'^-\s+(.+)', line)
        if bullet_match:
            bullet_text = bullet_match.group(1).strip()
            # Skip metadata bullets (Status, Priority, Added, Depends on)
            if re.match(r'\*\*(Status|Priority|Added|Depends\s+on):\*\*', bullet_text):
                continue
            if re.match(r'(Status|Priority|Added|Depends\s+on):\s', bullet_text, re.IGNORECASE):
                continue

            priority = extract_priority(bullet_text)
            items.append(PunchlistItem(
                source_project=source_project,
                section=current_section,
                title=bullet_text,
                body="",
                priority=priority
            ))
        elif line.startswith('  -') or line.startswith('    -'):
            # Sub-bullet: append to last item's body
            if items:
                sub_text = line.strip().lstrip('- ')
                items[-1].body += f"\n  - {sub_text}"

    return items


def parse_pl_blocks(content, source_project):
    """
    Parse PREFIX-NNN style blocks (PMA, CH, LS, BDH, etc.).
    Each ## PREFIX-NNN: Title (with optional emoji) is one item.
    Everything until the next header or --- separator is the item's body.
    Also captures free-form items separated by --- that lack headers.
    """
    items = []

    # Match any PREFIX-NNN: header (## or ###, optional emoji)
    pl_pattern = r'^#{2,3}\s+(?:[🔲⬜🔄🚫]\s*)?([A-Z]+-\d+):\s*(.+)'
    separator = r'^---\s*$'

    blocks = []
    current_block = {'id': None, 'title': None, 'lines': []}

    for line in content.split('\n'):
        pl_match = re.match(pl_pattern, line)
        sep_match = re.match(separator, line)

        if pl_match:
            # Save previous block if it has content
            if current_block['title'] or current_block['lines']:
                blocks.append(current_block)
            current_block = {
                'id': pl_match.group(1),
                'title': pl_match.group(2).strip(),
                'lines': []
            }
        elif sep_match:
            # Save previous block, start a new unnamed block
            if current_block['title'] or current_block['lines']:
                blocks.append(current_block)
            current_block = {'id': None, 'title': None, 'lines': []}
        else:
            # Skip the top-level # header
            if re.match(r'^#\s+', line) and not re.match(r'^##', line):
                continue
            current_block['lines'].append(line)

    # Don't forget the last block
    if current_block['title'] or current_block['lines']:
        blocks.append(current_block)

    for block in blocks:
        body = '\n'.join(block['lines']).strip()
        if not body and not block['title']:
            continue

        # For unnamed blocks (after ---), derive title from first meaningful line
        title = block['title']
        if not title:
            for bline in block['lines']:
                bline = bline.strip()
                if bline and not bline.startswith('#'):
                    title = bline[:100]
                    break
        if not title:
            continue

        if is_completed_section(title):
            continue

        priority = extract_priority(body)
        status = extract_status(body)
        depends_on = extract_depends_on(body)
        clean_body = strip_metadata_from_body(body)

        items.append(PunchlistItem(
            source_project=source_project,
            section="Punch List",
            title=title,
            body=clean_body,
            priority=priority,
            status=status,
            item_id=block['id'],
            depends_on=depends_on
        ))

    return items


def parse_punchlist_file(filepath):
    """
    Parse a punchlist markdown file and return open items.
    Auto-detects format:
      - ITEM N: blocks (LS style with emoji checkboxes)
      - PL-NNN: blocks (PMA style)
      - ### Section > - bullet (BDH style)
    """
    filepath = Path(filepath)
    source_project = filepath.parent.name
    # If file is at the root level (no parent folder context), use filename prefix
    if source_project == filepath.parent.parent.name:
        source_project = filepath.stem.split('_')[0]

    logger.info(f"Parsing: {filepath.name} (project: {source_project})")

    try:
        content = filepath.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = filepath.read_text(encoding='utf-8', errors='replace')

    # Detect format
    has_item_blocks = bool(re.search(r'##\s*[🔲⬜]\s*ITEM\s*\d+:', content))
    has_pl_blocks = bool(re.search(r'#{2,3}\s+(?:[🔲⬜]\s*)?[A-Z]+-\d+:', content))

    if has_item_blocks:
        items = parse_structured_items(content, source_project)
    elif has_pl_blocks:
        items = parse_pl_blocks(content, source_project)
    else:
        items = parse_bullet_items(content, source_project)

    logger.info(f"  Found {len(items)} open item(s)")
    return items


# =============================================================================
# AZURE OPENAI - SMART ANALYSIS
# =============================================================================

def sanitize_for_api(text):
    """Remove characters that can't be encoded as UTF-8 (surrogate pairs, etc.)."""
    return text.encode('utf-8', errors='replace').decode('utf-8')


def _get_llm_client():
    """Get Azure OpenAI client instance - same pattern as PMA_SynthesisAgent."""
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")

    if not api_key or not endpoint:
        logger.error("Azure OpenAI credentials not found in environment.")
        logger.error("Expected: AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in .env")
        return None

    return AzureOpenAI(
        api_key=api_key,
        api_version="2024-02-01",
        azure_endpoint=endpoint,
    )


def build_analysis_prompt(all_items):
    """
    Build the prompt that asks the LLM to analyze the open work items
    and produce an intelligent work plan.
    """
    # Group items by project
    by_project = {}
    for item in all_items:
        by_project.setdefault(item.source_project, []).append(item)

    separator = "=" * 60

    prompt_parts = [
        "You are a strategic project analyst for CRP Industries, an automotive "
        "aftermarket parts company. Below are all open work items across multiple "
        "active projects. Each project has its own punchlist.\n",
        "YOUR TASK:\n"
        "1. DEPENDENCY MAP: Identify items that depend on or unlock other items, "
        "both within and across projects. Be specific about which items connect.\n"
        "2. BLOCKERS: Flag items that are blocked (waiting on people, data, or "
        "other items to complete first). Distinguish 'hard blocked' (cannot start) "
        "from 'soft blocked' (can partially start).\n"
        "3. QUICK WINS: Items that can start immediately with no dependencies — "
        "low effort, high value.\n"
        "4. RECOMMENDED SEQUENCE: Suggest a practical work order. Group related "
        "items that should be tackled together. Consider that this is a small team "
        "and context-switching has a real cost.\n"
        "5. STRATEGIC OBSERVATIONS: Patterns you see across projects — shared "
        "blockers, recurring themes, opportunities to consolidate effort.\n"
        "6. RISK CALL-OUTS: Items that have been sitting or have cascading impact "
        "if they slip further.\n\n"
        "Be direct and actionable. No fluff. Use the item titles and project names "
        "as references so we can trace back to the source.\n",
        f"\n{separator}\nOPEN WORK ITEMS BY PROJECT\n{separator}\n"
    ]

    for project, items in sorted(by_project.items()):
        prompt_parts.append(f"\n--- PROJECT: {project} ({len(items)} open items) ---\n")
        current_section = None
        for item in items:
            if item.section != current_section:
                current_section = item.section
                prompt_parts.append(f"\n  [{current_section}]\n")
            prompt_parts.append(item.to_summary() + "\n")

    prompt_parts.append(f"\n{'=' * 60}\n")
    prompt_parts.append(f"Total: {len(all_items)} open items across "
                        f"{len(by_project)} projects\n")
    prompt_parts.append(f"Analysis date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    return "\n".join(prompt_parts)


def run_llm_analysis(prompt):
    """Call Azure OpenAI to generate the intelligent analysis."""
    client = _get_llm_client()
    if client is None:
        return None

    prompt = sanitize_for_api(prompt)
    logger.info(f"Sending {len(prompt)} characters to Azure OpenAI for analysis...")

    try:
        response = client.chat.completions.create(
            model=AZURE_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a strategic project analyst helping a small R&D "
                        "and data analytics team at an automotive aftermarket company "
                        "prioritize their work backlog. Be concise, direct, and "
                        "actionable. Reference specific item titles and project names. "
                        "Do not use excessive markdown formatting."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            max_tokens=3000,
            temperature=0.3
        )

        analysis = response.choices[0].message.content
        logger.info(f"Analysis generated: {len(analysis)} characters")
        return analysis

    except Exception as e:
        logger.error(f"LLM analysis failed: {e}")
        return None


# =============================================================================
# REPORT GENERATION
# =============================================================================

def build_inventory_report(all_items):
    """
    Build the structured inventory section of the report (non-AI).
    This always runs regardless of whether Azure OpenAI is available.
    """
    lines = []
    lines.append("=" * 70)
    lines.append("PUNCHLIST COLLATOR - OPEN ITEMS INVENTORY")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 70)

    # Group by project
    by_project = {}
    for item in all_items:
        by_project.setdefault(item.source_project, []).append(item)

    # Summary counts
    lines.append(f"\nTotal open items: {len(all_items)}")
    lines.append(f"Projects scanned: {len(by_project)}")

    # Priority breakdown
    priorities = {}
    for item in all_items:
        pri = item.priority or "Unspecified"
        priorities[pri] = priorities.get(pri, 0) + 1
    lines.append("\nBy Priority:")
    for pri in ["High", "Medium", "Low", "Unspecified"]:
        if pri in priorities:
            lines.append(f"  {pri}: {priorities[pri]}")

    lines.append("\nBy Project:")
    for project, items in sorted(by_project.items()):
        lines.append(f"  {project}: {len(items)} items")

    # Detailed listing
    lines.append("\n" + "-" * 70)
    lines.append("DETAILED ITEM LISTING")
    lines.append("-" * 70)

    for project, items in sorted(by_project.items()):
        lines.append(f"\n  [{project}] ({len(items)} items)")
        lines.append(f"  {'~' * 40}")
        current_section = None
        for item in items:
            if item.section != current_section:
                current_section = item.section
                lines.append(f"\n    {current_section}:")
            prefix = f"[{item.priority}] " if item.priority else ""
            id_prefix = f"{item.item_id}: " if item.item_id else ""
            lines.append(f"    - {prefix}{id_prefix}{item.title}")

    return "\n".join(lines)


def build_full_report(all_items, ai_analysis=None):
    """Combine inventory report with AI analysis into final output."""
    report_parts = [build_inventory_report(all_items)]

    if ai_analysis:
        report_parts.append("\n\n" + "=" * 70)
        report_parts.append("AI ANALYSIS - WORK PLAN & RECOMMENDATIONS")
        report_parts.append("=" * 70)
        report_parts.append(ai_analysis)
    else:
        report_parts.append("\n\n" + "=" * 70)
        report_parts.append("AI ANALYSIS UNAVAILABLE")
        report_parts.append("=" * 70)
        report_parts.append("Azure OpenAI credentials not configured or call failed.")
        report_parts.append("Set AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in .env")
        report_parts.append("The inventory above is still valid for manual review.")

    report_parts.append("\n\n" + "=" * 70)
    report_parts.append("END OF REPORT")
    report_parts.append("=" * 70)

    return "\n".join(report_parts)


# =============================================================================
# MAIN
# =============================================================================

def run_collation(base_dir=None):
    """
    Main entry point.
    base_dir: Path to CRPUtils folder. If None, uses this script's location.
    """
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent

    logger.info("=" * 60)
    logger.info("PUNCHLIST COLLATOR Starting")
    logger.info(f"Base directory: {base_dir}")
    logger.info("=" * 60)

    # Step 1: Find all punchlist files
    punchlist_files = find_punchlist_files(base_dir)

    if not punchlist_files:
        logger.warning("No punchlist files found. Nothing to collate.")
        print("\nNo *_punchlist.md files found in sibling folders.")
        return

    # Step 2: Parse all files
    all_items = []
    for pf in punchlist_files:
        items = parse_punchlist_file(pf)
        all_items.extend(items)

    logger.info(f"Total open items parsed: {len(all_items)}")

    if not all_items:
        logger.info("All punchlist items are completed. Nothing open.")
        print("\nAll punchlist items are marked complete. Nice work!")
        return

    # Step 3: Build LLM prompt and run analysis
    prompt = build_analysis_prompt(all_items)
    logger.info(f"Analysis prompt: {len(prompt)} characters")

    ai_analysis = run_llm_analysis(prompt)

    # Step 4: Build and output report
    report = build_full_report(all_items, ai_analysis)

    # Console output
    print("\n" + report)

    # Log file output (the logger already writes to file, but we want the
    # full report as a clean block too)
    try:
        log_path = Path(LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"Report written to {LOG_FILE}")
    except Exception as e:
        logger.error(f"Could not write report to {LOG_FILE}: {e}")

    logger.info("=" * 60)
    logger.info("PUNCHLIST COLLATOR Complete")
    logger.info("=" * 60)

    return {
        'status': 'SUCCESS',
        'items_found': len(all_items),
        'files_scanned': len(punchlist_files),
        'ai_analysis': ai_analysis,
        'report': report
    }


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Force UTF-8 output for console
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

    try:
        print("\n" + "=" * 60)
        print("PUNCHLIST COLLATOR - Cross-Project Work Intelligence")
        print("=" * 60 + "\n")

        result = run_collation()

        if result and result['status'] == 'SUCCESS':
            print(f"\nScanned {result['files_scanned']} files, "
                  f"found {result['items_found']} open items.")
            if result['ai_analysis']:
                print("AI analysis included in report.")
            else:
                print("AI analysis was not available (check Azure credentials).")
            print(f"Full report: {LOG_FILE}")

        sys.exit(0)

    except Exception as e:
        logger.error(f"Punchlist collator failed: {e}", exc_info=True)
        sys.exit(1)