"""
SynopsisAuditor.py - Cross-Project Synopsis Intelligence
=========================================================

Reads *_Synopsis.md files from a folder, extracts structured claims
(table references, blocker/dependency claims, stale references),
cross-references them to find disconnects, and generates per-project
correction notes for injection into ProjectAnalyzer prompts.

No AI/LLM calls — pure pattern matching.  Fast, free, deterministic.

Can run standalone (prints audit report) or be imported by
ProjectAnalyzer to enrich per-project prompts with a
"## CROSS-PROJECT INTELLIGENCE" section.

Lives in: CRPUtils folder
Input:    Folder containing *_Synopsis.md files
Output:   Dict of {project_name: [correction_note_strings]}

Author: Pat Yearick
Created: April 2026
"""

import os
import re
import sys
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================

LOG_FILE = r"C:\Logs\SynopsisAuditor.log"

# Known external dependency owners — expand as needed
DEPENDENCY_OWNERS = ['Byron', 'IT', 'DBA', 'ERP']

# Pattern for dated table names  (e.g., PartsVoice_2024_10_01)
DATED_TABLE_PATTERN = re.compile(
    r'`?([A-Za-z]\w*_\d{4}(?:_\d{2}(?:_\d{2})?)?)`?'
)

# Blocker / pending language
BLOCKER_PATTERNS = [
    re.compile(r'(?:blocked|waiting)\s+(?:on|for|pending)\s+(.{10,120})',
               re.IGNORECASE),
    re.compile(r'pending\s+(?:confirmation|availability|data|table|'
               r'access|enablement|deployment|review)\s+(.{5,100})',
               re.IGNORECASE),
    re.compile(r'not\s+yet\s+(?:available|enabled|deployed|implemented|'
               r'applied|integrated|built)\b(.{0,100})',
               re.IGNORECASE),
    re.compile(r'blocked\s+pending\s+(.{10,120})', re.IGNORECASE),
]

# Table usage language
TABLE_READ_PATTERNS = [
    re.compile(r'reads?\s+(?:from\s+)?`([^`]+)`', re.IGNORECASE),
    re.compile(r'queries?\s+`([^`]+)`', re.IGNORECASE),
    re.compile(r'consumes?\s+`([^`]+)`', re.IGNORECASE),
    re.compile(r'from\s+`([^`]+)`', re.IGNORECASE),
]

TABLE_WRITE_PATTERNS = [
    re.compile(r'writes?\s+(?:to\s+)?`([^`]+)`', re.IGNORECASE),
    re.compile(r'populates?\s+`([^`]+)`', re.IGNORECASE),
    re.compile(r'inserts?\s+(?:into\s+)?`([^`]+)`', re.IGNORECASE),
    re.compile(r'updates?\s+`([^`]+)`', re.IGNORECASE),
]

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
# DATA CLASSES
# =============================================================================

@dataclass
class TableClaim:
    """A project's claim about a table — reads, writes, or pending."""
    project: str
    table_name: str
    claim_type: str        # 'reads', 'writes', 'blocked_pending'
    context: str           # surrounding sentence for audit trail
    section: str = ''      # which synopsis section it appeared in


@dataclass
class BlockerClaim:
    """A stated blocker or pending dependency."""
    project: str
    raw_text: str          # the matched text
    owner: str             # 'Byron', 'IT', or '' if unattributed
    section: str = ''


@dataclass
class DatedReference:
    """A hardcoded date-stamped table or resource name."""
    project: str
    reference: str         # e.g., 'PartsVoice_2024_10_01'
    context: str


@dataclass
class Disconnect:
    """A detected cross-project inconsistency."""
    disconnect_type: str   # 'table_exists', 'stale_reference',
                           # 'blocker_consolidation', 'blocker_resolved'
    projects: List[str]
    summary: str           # human-readable description
    correction_note: str   # text to inject into the prompt


@dataclass
class SynopsisClaims:
    """All extracted claims from one synopsis."""
    project: str
    filepath: str
    tables_read: List[TableClaim] = field(default_factory=list)
    tables_written: List[TableClaim] = field(default_factory=list)
    tables_pending: List[TableClaim] = field(default_factory=list)
    blockers: List[BlockerClaim] = field(default_factory=list)
    dated_refs: List[DatedReference] = field(default_factory=list)
    raw_text: str = ''


# =============================================================================
# SYNOPSIS READER
# =============================================================================

def find_synopsis_files(folder: str) -> List[Path]:
    """Find all *_Synopsis.md files in the given folder."""
    folder_path = Path(folder)
    if not folder_path.exists():
        logger.warning(f"Synopsis folder does not exist: {folder}")
        return []

    files = sorted(folder_path.glob('*_Synopsis.md'))
    logger.info(f"Found {len(files)} synopsis file(s) in {folder}")
    return files


def _extract_project_name(filepath: Path) -> str:
    """Derive project name from filename like 'BigDawgHunt_Synopsis.md'."""
    name = filepath.stem                       # 'BigDawgHunt_Synopsis'
    return name.replace('_Synopsis', '')


def _get_section(text: str, start_heading: str) -> str:
    """
    Extract the text under a specific ## heading, up to the next ## heading.
    Case-insensitive match on the heading text.
    """
    pattern = re.compile(
        r'^##\s+\d*\.?\s*' + re.escape(start_heading) + r'.*?\n'
        r'(.*?)(?=^##\s|\Z)',
        re.MULTILINE | re.DOTALL | re.IGNORECASE
    )
    match = pattern.search(text)
    return match.group(1).strip() if match else ''


def _get_sections_by_prefix(text: str) -> Dict[str, str]:
    """Split the synopsis into named sections keyed by heading text."""
    sections = {}
    current_heading = None
    current_lines = []

    for line in text.split('\n'):
        heading_match = re.match(r'^##\s+\d*\.?\s*(.*)', line)
        if heading_match:
            if current_heading is not None:
                sections[current_heading] = '\n'.join(current_lines)
            current_heading = heading_match.group(1).strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_heading is not None:
        sections[current_heading] = '\n'.join(current_lines)

    return sections


# =============================================================================
# CLAIM EXTRACTION
# =============================================================================

def _extract_backtick_names(text: str) -> Set[str]:
    """Pull all backtick-quoted names that look like table/view names."""
    candidates = re.findall(r'`([A-Za-z]\w{2,})`', text)
    # Filter out obvious non-tables: script names, file extensions, keywords
    noise = {
        'True', 'False', 'None', 'NULL', 'GETDATE', 'ROW_NUMBER',
        'MERGE', 'INSERT', 'UPDATE', 'DELETE', 'SELECT', 'WHERE',
        'FROM', 'JOIN', 'INTO', 'VALUES', 'CREATE', 'ALTER',
    }
    return {c for c in candidates
            if not c.endswith('.py') and not c.endswith('.bat')
            and not c.endswith('.sql') and not c.endswith('.md')
            and not c.endswith('.xlsx') and not c.endswith('.csv')
            and not c.endswith('.xml') and not c.endswith('.json')
            and c not in noise}


def _is_negated(text: str, match_start: int) -> bool:
    """
    Check if a regex match is preceded by negation words.
    Looks at the 40 characters before the match for words like
    'never', 'not', 'doesn't', 'don't', 'no', 'without'.
    """
    lookback = text[max(0, match_start - 40):match_start].lower()
    negation_words = ['never ', 'not ', "doesn't ", "doesn\\'t ",
                      "don't ", "don\\'t ", ' no ', 'without ',
                      'cannot ', "can't "]
    return any(neg in lookback for neg in negation_words)


def _find_sentence_around(text: str, target: str, chars: int = 200) -> str:
    """Get a snippet of text surrounding a match for context."""
    idx = text.find(target)
    if idx < 0:
        return ''
    start = max(0, idx - chars // 2)
    end = min(len(text), idx + len(target) + chars // 2)
    snippet = text[start:end].replace('\n', ' ').strip()
    return snippet


def _extract_table_claims_from_section(
    project: str, section_text: str, section_name: str
) -> Tuple[List[TableClaim], List[TableClaim]]:
    """
    Extract table read/write claims from a synopsis section.
    Parses markdown tables (| TableName | ...) and inline patterns.
    """
    reads = []
    writes = []

    # Strategy 1: Parse markdown tables with | delimiters
    # Look for rows like: | `TableName` | description |
    # or                  | TableName | description |
    for line in section_text.split('\n'):
        if '|' not in line or line.strip().startswith('|--'):
            continue
        cells = [c.strip().strip('`') for c in line.split('|')]
        cells = [c for c in cells if c]
        if not cells:
            continue
        # First cell is often the table name in Data Flow sections
        candidate = cells[0]
        if (re.match(r'^[A-Za-z]\w{2,}$', candidate)
                and candidate not in ('Table', 'Source', 'Script',
                                      'Agent', 'Job', 'Resource',
                                      'Report', 'Destination')):
            claim = TableClaim(
                project=project, table_name=candidate,
                claim_type='reads', context=line.strip(),
                section=section_name
            )
            # If in a "Write" or "Destination" section, mark as write
            if any(w in section_name.lower()
                   for w in ('write', 'destination', 'output')):
                claim.claim_type = 'writes'
                writes.append(claim)
            else:
                reads.append(claim)

    # Strategy 2: Inline pattern matching
    for pattern in TABLE_READ_PATTERNS:
        for match in pattern.finditer(section_text):
            if _is_negated(section_text, match.start()):
                continue
            table = match.group(1)
            if re.match(r'^[A-Za-z]\w{2,}$', table):
                ctx = _find_sentence_around(section_text, match.group(0))
                reads.append(TableClaim(
                    project=project, table_name=table,
                    claim_type='reads', context=ctx,
                    section=section_name
                ))

    for pattern in TABLE_WRITE_PATTERNS:
        for match in pattern.finditer(section_text):
            if _is_negated(section_text, match.start()):
                continue
            table = match.group(1)
            if re.match(r'^[A-Za-z]\w{2,}$', table):
                ctx = _find_sentence_around(section_text, match.group(0))
                writes.append(TableClaim(
                    project=project, table_name=table,
                    claim_type='writes', context=ctx,
                    section=section_name
                ))

    return reads, writes


def _extract_blockers(
    project: str, text: str, section_name: str
) -> List[BlockerClaim]:
    """Find blocker and pending-dependency claims."""
    blockers = []

    for pattern in BLOCKER_PATTERNS:
        for match in pattern.finditer(text):
            raw = match.group(0).strip().rstrip('.,;')
            # Determine owner
            owner = ''
            for dep_owner in DEPENDENCY_OWNERS:
                if dep_owner.lower() in raw.lower():
                    owner = dep_owner
                    break

            blockers.append(BlockerClaim(
                project=project, raw_text=raw,
                owner=owner, section=section_name
            ))

    return blockers


def _extract_dated_references(
    project: str, text: str
) -> List[DatedReference]:
    """Find hardcoded date-stamped table/resource names."""
    refs = []
    seen = set()

    for match in DATED_TABLE_PATTERN.finditer(text):
        ref = match.group(1)
        # Must actually contain a 4-digit year in 2020s
        if not re.search(r'_20[2-3]\d', ref):
            continue
        # Skip date-formatted filenames (like 20250723_TaskSchedulerCheck)
        if ref[0].isdigit():
            continue
        # Skip very short prefixes before the date (likely noise)
        prefix = re.split(r'_20\d{2}', ref)[0]
        if len(prefix) < 4:
            continue
        # Skip common false positive patterns
        skip_words = {'notfound', 'exclude', 'filter', 'test',
                      'temp', 'backup', 'old', 'archive', 'example'}
        if any(sw in prefix.lower() for sw in skip_words):
            continue
        if ref in seen:
            continue
        seen.add(ref)

        ctx = _find_sentence_around(text, ref)
        refs.append(DatedReference(
            project=project, reference=ref, context=ctx
        ))

    return refs


def _extract_pending_tables(
    project: str, text: str, section_name: str
) -> List[TableClaim]:
    """
    Find tables that are claimed as not-yet-available or blocked.
    Looks for patterns like 'pending BOM table', 'blocked pending X table'.
    """
    pending = []
    # Look for "blocked pending <table>" or "<table> not yet available"
    patterns = [
        re.compile(r'(?:blocked|pending)\s+.*?`([A-Za-z]\w+)`', re.IGNORECASE),
        re.compile(r'`([A-Za-z]\w+)`\s+.*?(?:not yet|pending|unavailable)',
                   re.IGNORECASE),
        re.compile(r'pending\s+(\w+)\s+table', re.IGNORECASE),
        re.compile(r'(\w+)\s+table\s+.*?(?:pending|requested|promised)',
                   re.IGNORECASE),
    ]

    for pattern in patterns:
        for match in pattern.finditer(text):
            table = match.group(1)
            if len(table) < 3:
                continue
            ctx = _find_sentence_around(text, match.group(0))
            pending.append(TableClaim(
                project=project, table_name=table,
                claim_type='blocked_pending', context=ctx,
                section=section_name
            ))

    return pending


# =============================================================================
# MAIN PARSING
# =============================================================================

def parse_synopsis(filepath: Path) -> SynopsisClaims:
    """Parse a single synopsis file and extract all claims."""
    project = _extract_project_name(filepath)

    try:
        text = filepath.read_text(encoding='utf-8')
    except Exception as e:
        logger.error(f"Could not read {filepath}: {e}")
        return SynopsisClaims(project=project, filepath=str(filepath))

    claims = SynopsisClaims(
        project=project, filepath=str(filepath), raw_text=text
    )

    sections = _get_sections_by_prefix(text)

    for section_name, section_text in sections.items():
        # Table reads/writes (primarily from Data Flow sections)
        reads, writes = _extract_table_claims_from_section(
            project, section_text, section_name
        )
        claims.tables_read.extend(reads)
        claims.tables_written.extend(writes)

        # Blockers (primarily from Known Issues, but scan everything)
        blockers = _extract_blockers(project, section_text, section_name)
        claims.blockers.extend(blockers)

        # Pending tables (from Known Issues and blockers sections)
        pending = _extract_pending_tables(
            project, section_text, section_name
        )
        claims.tables_pending.extend(pending)

    # Dated references (scan full text)
    claims.dated_refs = _extract_dated_references(project, text)

    logger.info(
        f"  {project}: {len(claims.tables_read)} reads, "
        f"{len(claims.tables_written)} writes, "
        f"{len(claims.blockers)} blockers, "
        f"{len(claims.dated_refs)} dated refs"
    )

    return claims


def parse_all_synopses(folder: str) -> Dict[str, SynopsisClaims]:
    """Parse all synopsis files in a folder. Returns {project: claims}."""
    files = find_synopsis_files(folder)
    all_claims = {}
    for f in files:
        claims = parse_synopsis(f)
        all_claims[claims.project] = claims
    return all_claims


# =============================================================================
# CROSS-REFERENCE ANALYSIS
# =============================================================================

def _find_table_existence_contradictions(
    all_claims: Dict[str, SynopsisClaims]
) -> List[Disconnect]:
    """
    Find cases where Project A says a table is pending/blocked
    but Project B already reads from or writes to it.
    """
    disconnects = []

    # Build a set of tables that are actively used (read or written)
    active_tables = defaultdict(list)  # table_name -> [(project, claim_type)]
    for project, claims in all_claims.items():
        for tc in claims.tables_read:
            active_tables[tc.table_name].append((project, 'reads'))
        for tc in claims.tables_written:
            active_tables[tc.table_name].append((project, 'writes'))

    # Check pending claims against active tables
    seen_pairs = set()  # (project, table) to prevent duplicates
    for project, claims in all_claims.items():
        for pending in claims.tables_pending:
            table = pending.table_name
            # Skip single/two-letter names
            if len(table) < 3:
                continue

            dedup_key = (project, table)
            if dedup_key in seen_pairs:
                continue
            seen_pairs.add(dedup_key)

            matches = []
            for active_name, users in active_tables.items():
                # Require meaningful match
                is_match = False
                if table.lower() == active_name.lower():
                    # Exact match (case-insensitive)
                    is_match = True
                elif (len(table) >= 3
                      and active_name.lower().startswith(
                          table.lower())):
                    # Prefix match: BOM matches BOMMaster
                    is_match = True
                elif (len(table) >= 6
                      and table.lower() in active_name.lower()):
                    # Substring match for longer names
                    is_match = True

                if not is_match:
                    continue

                other_users = [(p, ct) for p, ct in users
                               if p != project]
                if other_users:
                    # Deduplicate users for this active table
                    seen_users = set()
                    for p, ct in other_users:
                        if p not in seen_users:
                            seen_users.add(p)
                            matches.append(
                                (active_name, p, ct)
                            )

            if matches:
                # Build a readable summary
                examples = []
                for active_name, other_proj, claim_type in matches[:3]:
                    examples.append(
                        f"{other_proj} {claim_type} `{active_name}`"
                    )
                example_str = '; '.join(examples)

                disconnects.append(Disconnect(
                    disconnect_type='table_exists',
                    projects=[project]
                             + [m[1] for m in matches[:3]],
                    summary=(
                        f"{project} claims `{table}` is pending/blocked, "
                        f"but: {example_str}"
                    ),
                    correction_note=(
                        f"Your previous synopsis indicated `{table}` is "
                        f"pending or blocked. However, {example_str}. "
                        f"Please verify whether this blocker is still "
                        f"active and update accordingly."
                    )
                ))

    return disconnects


def _find_shared_stale_references(
    all_claims: Dict[str, SynopsisClaims]
) -> List[Disconnect]:
    """
    Find dated table names referenced by multiple projects.
    These are candidates for a shared technical debt note.
    """
    disconnects = []

    # Group dated references by the reference name
    ref_projects = defaultdict(list)  # reference -> [project, ...]
    for project, claims in all_claims.items():
        for dr in claims.dated_refs:
            ref_projects[dr.reference].append(project)

    for ref, projects in ref_projects.items():
        if len(projects) >= 2:
            proj_list = ', '.join(sorted(projects))
            disconnects.append(Disconnect(
                disconnect_type='stale_reference',
                projects=sorted(projects),
                summary=(
                    f"Dated reference `{ref}` appears in "
                    f"{len(projects)} projects: {proj_list}"
                ),
                correction_note=(
                    f"Your synopsis references `{ref}`, a date-stamped "
                    f"table name. This same reference also appears in: "
                    f"{proj_list}. Consider noting this as a shared "
                    f"technical debt item — all projects would benefit "
                    f"from a dynamic table name resolution pattern."
                )
            ))

    return disconnects


def _consolidate_external_dependencies(
    all_claims: Dict[str, SynopsisClaims]
) -> List[Disconnect]:
    """
    Gather all Byron / IT / DBA dependencies across projects
    so each project's prompt knows about the full picture.
    """
    disconnects = []

    # Group by owner
    by_owner = defaultdict(list)   # owner -> [(project, raw_text)]
    for project, claims in all_claims.items():
        for blocker in claims.blockers:
            if blocker.owner:
                by_owner[blocker.owner].append(
                    (project, blocker.raw_text)
                )

    for owner, items in by_owner.items():
        projects_involved = sorted(set(p for p, _ in items))
        # Only consolidate if 2+ distinct projects share this dependency
        if len(projects_involved) < 2:
            continue

        # Build a concise summary of all items for this owner
        item_lines = []
        for proj, text in items:
            # Truncate long blocker text
            short = text[:100] + ('...' if len(text) > 100 else '')
            item_lines.append(f"  - [{proj}] {short}")
        items_str = '\n'.join(item_lines)

        disconnects.append(Disconnect(
            disconnect_type='blocker_consolidation',
            projects=projects_involved,
            summary=(
                f"{owner} dependencies span {len(projects_involved)} "
                f"projects: {', '.join(projects_involved)}"
            ),
            correction_note=(
                f"Your synopsis includes a dependency on {owner}. "
                f"For context, {owner} dependencies exist across "
                f"{len(projects_involved)} projects:\n{items_str}\n"
                f"If coordinating with {owner}, these could be "
                f"raised together."
            )
        ))

    return disconnects


def _find_shared_table_inconsistencies(
    all_claims: Dict[str, SynopsisClaims]
) -> List[Disconnect]:
    """
    Find tables that appear in multiple projects with potentially
    inconsistent descriptions (one writes, another doesn't know).
    """
    disconnects = []

    # Build writer map: table -> [writing projects]
    writers = defaultdict(set)
    for project, claims in all_claims.items():
        for tc in claims.tables_written:
            writers[tc.table_name].add(project)

    # Build reader map: table -> [reading projects]
    readers = defaultdict(set)
    for project, claims in all_claims.items():
        for tc in claims.tables_read:
            readers[tc.table_name].add(project)

    # Find tables where the writer changed but readers may not know
    # (This is informational, not necessarily an error)
    for table, writing_projects in writers.items():
        reading_projects = readers.get(table, set())
        if writing_projects and reading_projects:
            # Skip if same project reads and writes
            cross = reading_projects - writing_projects
            if cross and len(writing_projects) == 1:
                writer = list(writing_projects)[0]
                reader_list = ', '.join(sorted(cross))
                # Only flag if 3+ readers — otherwise too noisy
                if len(cross) >= 3:
                    disconnects.append(Disconnect(
                        disconnect_type='shared_table_info',
                        projects=[writer] + sorted(cross),
                        summary=(
                            f"`{table}` is written by {writer} and "
                            f"read by {len(cross)} others: {reader_list}"
                        ),
                        correction_note=(
                            f"For reference: `{table}` is written by "
                            f"{writer} and read by: {reader_list}. "
                            f"Changes to this table's schema or refresh "
                            f"schedule would affect all of these projects."
                        )
                    ))

    return disconnects


def run_audit(
    all_claims: Dict[str, SynopsisClaims]
) -> List[Disconnect]:
    """
    Run all cross-reference checks and return a combined list
    of disconnects.
    """
    disconnects = []

    logger.info("Running table existence contradiction check...")
    disconnects.extend(
        _find_table_existence_contradictions(all_claims)
    )

    logger.info("Running shared stale reference check...")
    disconnects.extend(
        _find_shared_stale_references(all_claims)
    )

    logger.info("Running external dependency consolidation...")
    disconnects.extend(
        _consolidate_external_dependencies(all_claims)
    )

    logger.info("Running shared table inconsistency check...")
    disconnects.extend(
        _find_shared_table_inconsistencies(all_claims)
    )

    logger.info(f"Audit complete: {len(disconnects)} disconnect(s) found")
    return disconnects


# =============================================================================
# PER-PROJECT NOTE GENERATION
# =============================================================================

def build_project_notes(
    all_claims: Dict[str, SynopsisClaims],
    disconnects: List[Disconnect]
) -> Dict[str, List[str]]:
    """
    Build per-project correction notes from the disconnects list.
    Returns {project_name: [note_string, note_string, ...]}.
    """
    notes = defaultdict(list)

    for disc in disconnects:
        for project in disc.projects:
            # Only add the note if it's relevant to this project
            if project in disc.correction_note:
                notes[project].append(disc.correction_note)

    # Deduplicate notes per project
    for project in notes:
        seen = set()
        unique = []
        for note in notes[project]:
            key = note[:80]   # rough dedup on first 80 chars
            if key not in seen:
                seen.add(key)
                unique.append(note)
        notes[project] = unique

    return dict(notes)


def format_prompt_section(notes: List[str]) -> str:
    """
    Format a list of correction notes into a prompt section string
    that can be appended to a ProjectAnalyzer prompt.
    """
    if not notes:
        return ''

    lines = []
    lines.append("## CROSS-PROJECT INTELLIGENCE")
    lines.append("")
    lines.append(
        "The following notes were generated by comparing your previous "
        "synopsis against all other project synopses. Please address "
        "each item when writing the updated synopsis."
    )
    lines.append("")

    for i, note in enumerate(notes, 1):
        lines.append(f"**Note {i}:** {note}")
        lines.append("")

    return '\n'.join(lines)


# =============================================================================
# STANDALONE REPORT
# =============================================================================

def print_audit_report(
    all_claims: Dict[str, SynopsisClaims],
    disconnects: List[Disconnect]
):
    """Print a human-readable audit report to console."""
    print()
    print("=" * 70)
    print("CROSS-PROJECT SYNOPSIS AUDIT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Synopses analyzed: {len(all_claims)}")
    print("=" * 70)

    # Summary by type
    by_type = defaultdict(list)
    for d in disconnects:
        by_type[d.disconnect_type].append(d)

    type_labels = {
        'table_exists': 'Table Existence Contradictions',
        'stale_reference': 'Shared Stale References',
        'blocker_consolidation': 'External Dependency Consolidation',
        'shared_table_info': 'Shared Table Awareness',
        'blocker_resolved': 'Potentially Resolved Blockers',
    }

    print(f"\nDisconnects found: {len(disconnects)}")
    for dtype, items in by_type.items():
        label = type_labels.get(dtype, dtype)
        print(f"  {label}: {len(items)}")

    for dtype, items in by_type.items():
        label = type_labels.get(dtype, dtype)
        print(f"\n{'─' * 70}")
        print(f"  {label.upper()}")
        print(f"{'─' * 70}")
        for item in items:
            print(f"\n  {item.summary}")
            projects_str = ', '.join(item.projects)
            print(f"  Projects: {projects_str}")

    # Extraction stats
    print(f"\n{'─' * 70}")
    print("  EXTRACTION SUMMARY")
    print(f"{'─' * 70}")
    for project, claims in sorted(all_claims.items()):
        print(f"\n  {project}:")
        print(f"    Tables read:    {len(claims.tables_read)}")
        print(f"    Tables written: {len(claims.tables_written)}")
        print(f"    Pending tables: {len(claims.tables_pending)}")
        print(f"    Blockers:       {len(claims.blockers)}")
        print(f"    Dated refs:     {len(claims.dated_refs)}")
        if claims.dated_refs:
            for dr in claims.dated_refs:
                print(f"      - {dr.reference}")

    # Per-project notes preview
    notes = build_project_notes(all_claims, disconnects)
    if notes:
        print(f"\n{'=' * 70}")
        print("PER-PROJECT PROMPT NOTES PREVIEW")
        print(f"{'=' * 70}")
        for project, project_notes in sorted(notes.items()):
            print(f"\n  [{project}] — {len(project_notes)} note(s)")
            for note in project_notes:
                # Truncate for console readability
                short = note[:150] + ('...' if len(note) > 150 else '')
                print(f"    • {short}")

    print(f"\n{'=' * 70}")
    print("END OF AUDIT")
    print(f"{'=' * 70}")


# =============================================================================
# PUBLIC API  (for import by ProjectAnalyzer)
# =============================================================================

def audit_synopses(synopsis_folder: str) -> Dict[str, List[str]]:
    """
    Main entry point for ProjectAnalyzer integration.

    Reads all *_Synopsis.md from the given folder, runs the full
    cross-reference audit, and returns per-project correction notes.

    Returns:
        Dict mapping project_name -> list of correction note strings.
        Use format_prompt_section(notes) to convert to prompt text.
    """
    all_claims = parse_all_synopses(synopsis_folder)
    if not all_claims:
        logger.warning("No synopses found — no cross-project intel.")
        return {}

    disconnects = run_audit(all_claims)
    notes = build_project_notes(all_claims, disconnects)

    logger.info(
        f"Synopsis audit: {len(disconnects)} disconnects, "
        f"{len(notes)} projects with notes"
    )
    return notes


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Cross-Project Synopsis Auditor'
    )
    parser.add_argument(
        'folder', nargs='?',
        default=str(
            Path(__file__).resolve().parent.parent / 'Results'
        ),
        help='Folder containing *_Synopsis.md files '
             '(default: ../Results)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Show detailed extraction output'
    )

    args = parser.parse_args()

    print(f"Scanning: {args.folder}")
    all_claims = parse_all_synopses(args.folder)

    if not all_claims:
        print("No synopsis files found.")
        sys.exit(1)

    disconnects = run_audit(all_claims)
    print_audit_report(all_claims, disconnects)