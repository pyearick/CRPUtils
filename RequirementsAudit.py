"""
RequirementsAudit.py
====================
Walks PycharmProjects, compares each project's .venv against its
requirements.txt, reports drift, and optionally regenerates.

Why this exists
---------------
- Onboarding: a new developer (e.g., Sai) needs a reliable requirements.txt
  per project so they can recreate each .venv with `pip install -r`.
- Service safety: NSSM-managed services (PMA_Orchestrator, VoiceSQLServer,
  BDH_Streamlit) all run from project .venv folders. Drift between what's
  installed and what's documented is operationally risky.
- Multi-developer parity: prevents "works on my machine" by keeping
  requirements.txt as the single source of truth in version control.

Usage
-----
    # Audit all projects (read-only, no changes)
    python RequirementsAudit.py

    # Audit + regenerate requirements.txt where drift/missing
    # Existing requirements.txt files are backed up with a timestamp suffix.
    python RequirementsAudit.py --update

    # Audit a single project
    python RequirementsAudit.py --project PMAssistant

    # Use a non-default root
    python RequirementsAudit.py --root "C:\\some\\other\\path"

Output
------
- Console: per-project status table + drift breakdown + summary counts.
- Log:     C:\\Logs\\RequirementsAudit.log (line-buffered, no date suffix).
- Files (only when --update is passed):
    <project>\\requirements.txt              (regenerated from pip freeze)
    <project>\\requirements.txt.bak.YYYYMMDD_HHMMSS  (backup if one existed)
    <project>\\python_version.txt            (interpreter version, e.g. 3.12.5)

Conventions
-----------
- Project = a direct child of root that has a .venv\\Scripts\\python.exe.
- Skip folders match the rest of CRPUtils tooling.
- Package name normalization: lowercase + underscore-to-dash (good enough
  for the comparison; not full PEP 503 normalization).
- Only standard `name==version` pins are compared. Editable installs (-e),
  VCS installs, and other non-pin lines are ignored on both sides.
"""

import argparse
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from logging_utils import setup_logging, ScriptTimer

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_NAME = 'RequirementsAudit'
logger = setup_logging(SCRIPT_NAME)

# Default root is the parent of the CRPUtils folder, i.e. PycharmProjects.
# Same approach Punchlist_manager and ProjectAnalyzer use.
DEFAULT_ROOT = Path(__file__).resolve().parent.parent

# Folders to skip when scanning for project folders. Matches the convention
# used elsewhere in CRPUtils (WorkLogGUI, Punchlist_manager, ProjectAnalyzer).
SKIP_FOLDERS = {
    '.idea', '.git', '.venv', '__pycache__', 'node_modules',
    'CommitsGH', 'PunchlistReview', '.ipynb_checkpoints'
}

# Subprocess timeouts (seconds)
TIMEOUT_PYTHON_VERSION = 10
TIMEOUT_PIP_FREEZE = 120


# =============================================================================
# DATA CLASSES
# =============================================================================

class AuditStatus(Enum):
    IN_SYNC = "InSync"
    DRIFT = "Drift"
    MISSING_REQUIREMENTS = "MissingRequirements"
    NO_VENV = "NoVenv"
    NO_PYTHON = "NoPython"
    PIP_FREEZE_FAILED = "PipFreezeFailed"


@dataclass
class PackageDiff:
    """Differences between venv-installed packages and requirements.txt pins."""
    added: List[Tuple[str, str]] = field(default_factory=list)        # (name, venv_ver)  - in venv, not in reqs
    removed: List[Tuple[str, str]] = field(default_factory=list)      # (name, reqs_ver)  - in reqs, not in venv
    changed: List[Tuple[str, str, str]] = field(default_factory=list) # (name, reqs_ver, venv_ver)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    @property
    def total_changes(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed)


@dataclass
class ProjectAuditResult:
    """Result of auditing a single project."""
    project_name: str
    project_path: Path
    status: AuditStatus
    python_version: Optional[str] = None
    venv_packages: Dict[str, str] = field(default_factory=dict)
    requirements_packages: Dict[str, str] = field(default_factory=dict)
    diff: Optional[PackageDiff] = None
    error_message: Optional[str] = None
    requirements_updated: bool = False
    backup_path: Optional[Path] = None


# =============================================================================
# DISCOVERY
# =============================================================================

def discover_projects(root: Path, single_project: Optional[str] = None) -> List[Path]:
    """Return immediate child folders under root that look like project folders.

    A "project folder" is any direct child that isn't in SKIP_FOLDERS and
    doesn't start with a dot. The caller decides whether each one actually
    has a .venv (handled later in audit_project).
    """
    if not root.exists():
        logger.error(f"Root directory does not exist: {root}")
        return []

    projects = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if child.name in SKIP_FOLDERS or child.name.startswith('.'):
            continue
        if single_project and child.name != single_project:
            continue
        projects.append(child)

    return projects


# =============================================================================
# VENV INSPECTION
# =============================================================================

def get_venv_python(project_path: Path) -> Optional[Path]:
    """Return the path to .venv\\Scripts\\python.exe if it exists, else None."""
    candidate = project_path / '.venv' / 'Scripts' / 'python.exe'
    return candidate if candidate.exists() else None


def get_python_version(python_exe: Path) -> Optional[str]:
    """Return the Python version reported by the venv interpreter, e.g. '3.12.5'."""
    try:
        result = subprocess.run(
            [str(python_exe), '--version'],
            capture_output=True, text=True, timeout=TIMEOUT_PYTHON_VERSION,
        )
        # `python --version` writes to stdout in 3.4+, but be defensive
        version = (result.stdout or result.stderr or '').strip()
        if version.startswith('Python '):
            return version[len('Python '):]
        return version or None
    except Exception as e:
        logger.warning(f"  Failed to get Python version from {python_exe}: {e}")
        return None


def run_pip_freeze(python_exe: Path) -> Optional[str]:
    """Run `pip freeze` using the venv's interpreter; return stdout or None on failure."""
    try:
        result = subprocess.run(
            [str(python_exe), '-m', 'pip', 'freeze'],
            capture_output=True, text=True, timeout=TIMEOUT_PIP_FREEZE,
        )
        if result.returncode != 0:
            logger.warning(f"  pip freeze exited {result.returncode}: {result.stderr.strip()}")
            return None
        return result.stdout
    except Exception as e:
        logger.warning(f"  Failed to run pip freeze: {e}")
        return None


# =============================================================================
# PARSING
# =============================================================================

# Matches a simple pinned spec like "package-name==1.2.3" (no extras, no markers
# beyond the version). Names allow letters, digits, underscore, dash, dot.
_PIN_RE = re.compile(r'^([A-Za-z0-9_\-.]+)\s*==\s*([^\s;]+)')


def _normalize_name(name: str) -> str:
    """Loose name normalization for comparison. Not full PEP 503 but consistent."""
    return name.lower().replace('_', '-')


def parse_pip_freeze(output: str) -> Dict[str, str]:
    """Parse pip freeze output into {normalized_name: version}. Skips non-pin lines."""
    packages: Dict[str, str] = {}
    for raw in output.splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or line.startswith('-'):
            continue
        m = _PIN_RE.match(line)
        if m:
            packages[_normalize_name(m.group(1))] = m.group(2)
    return packages


def _read_text_with_fallback(path: Path) -> Optional[str]:
    """Read a text file trying common encodings in turn.

    Some requirements.txt files in the wild are saved as UTF-16 (the default
    output of PowerShell's `pip freeze > requirements.txt` redirect, which
    pipes through Out-File with a UTF-16-LE BOM). A few may also be Latin-1
    or other 8-bit encodings.

    Order: utf-8-sig (handles plain UTF-8 and UTF-8 with BOM), utf-16 (LE/BE
    auto-detected via BOM), then latin-1 as a last resort (always succeeds
    on any byte sequence; loses information for non-ASCII but at least lets
    us see the pinned versions which are pure ASCII).

    Returns the file contents as a string, or None if the file couldn't be
    opened at all (e.g. locked, permission error). A warning is logged when
    a fallback encoding was used so the user knows the file should be
    rewritten as UTF-8.
    """
    encodings = [
        ('utf-8-sig', None),
        ('utf-16', 'UTF-16'),
        ('latin-1', 'latin-1'),
    ]
    last_err: Optional[Exception] = None
    for enc, warn_label in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                content = f.read()
            if warn_label:
                logger.warning(
                    f"  {path.name} read using {warn_label} fallback "
                    f"(file is not UTF-8). Will be rewritten as UTF-8 if --update is used."
                )
            return content
        except UnicodeError as e:
            last_err = e
            continue
        except Exception as e:
            logger.warning(f"  Error reading {path}: {e}")
            return None
    logger.warning(
        f"  Could not decode {path} with any encoding tried (last error: {last_err})"
    )
    return None


def parse_requirements_file(path: Path) -> Dict[str, str]:
    """Parse requirements.txt into {normalized_name: version}. Skips comments,
    blank lines, and non-pin entries (-e, -r, --hash, VCS URLs, etc.).

    Handles UTF-8, UTF-8-with-BOM, UTF-16 (PowerShell-redirect output), and
    Latin-1 source files via _read_text_with_fallback.
    """
    packages: Dict[str, str] = {}
    if not path.exists():
        return packages

    content = _read_text_with_fallback(path)
    if content is None:
        return packages

    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or line.startswith('-'):
            continue
        m = _PIN_RE.match(line)
        if m:
            packages[_normalize_name(m.group(1))] = m.group(2)
    return packages


# =============================================================================
# DIFFING
# =============================================================================

def diff_packages(venv: Dict[str, str], reqs: Dict[str, str]) -> PackageDiff:
    """Compute additions, removals, and version changes between venv and reqs."""
    diff = PackageDiff()
    venv_keys = set(venv.keys())
    reqs_keys = set(reqs.keys())

    for name in sorted(venv_keys - reqs_keys):
        diff.added.append((name, venv[name]))
    for name in sorted(reqs_keys - venv_keys):
        diff.removed.append((name, reqs[name]))
    for name in sorted(venv_keys & reqs_keys):
        if venv[name] != reqs[name]:
            diff.changed.append((name, reqs[name], venv[name]))

    return diff


# =============================================================================
# WRITING
# =============================================================================

def write_requirements(
    project_path: Path, freeze_output: str, python_version: Optional[str]
) -> Tuple[Optional[Path], Path]:
    """Write requirements.txt (with timestamped backup if one existed) and
    python_version.txt. Returns (backup_path or None, requirements_path)."""
    req_path = project_path / 'requirements.txt'
    backup_path: Optional[Path] = None

    if req_path.exists():
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = project_path / f'requirements.txt.bak.{ts}'
        shutil.copy2(req_path, backup_path)
        logger.info(f"  Backed up existing requirements.txt -> {backup_path.name}")

    with open(req_path, 'w', encoding='utf-8') as f:
        f.write(freeze_output)

    if python_version:
        version_path = project_path / 'python_version.txt'
        with open(version_path, 'w', encoding='utf-8') as f:
            f.write(python_version + '\n')

    return backup_path, req_path


# =============================================================================
# AUDIT (per project)
# =============================================================================

def audit_project(project_path: Path, update: bool = False) -> ProjectAuditResult:
    """Audit one project and (optionally) regenerate requirements.txt."""
    name = project_path.name
    result = ProjectAuditResult(
        project_name=name,
        project_path=project_path,
        status=AuditStatus.NO_VENV,
    )

    # 1. Locate the venv interpreter
    python_exe = get_venv_python(project_path)
    if python_exe is None:
        return result  # NO_VENV

    # 2. Get its Python version
    result.python_version = get_python_version(python_exe)
    if result.python_version is None:
        result.status = AuditStatus.NO_PYTHON
        result.error_message = "Could not determine Python version"
        return result

    # 3. Run pip freeze
    freeze_output = run_pip_freeze(python_exe)
    if freeze_output is None:
        result.status = AuditStatus.PIP_FREEZE_FAILED
        result.error_message = "pip freeze failed"
        return result

    result.venv_packages = parse_pip_freeze(freeze_output)

    # 4. Read requirements.txt (or note its absence)
    req_path = project_path / 'requirements.txt'
    if not req_path.exists():
        result.status = AuditStatus.MISSING_REQUIREMENTS
        if update:
            backup, _ = write_requirements(project_path, freeze_output, result.python_version)
            result.backup_path = backup
            result.requirements_updated = True
        return result

    result.requirements_packages = parse_requirements_file(req_path)

    # 5. Diff
    result.diff = diff_packages(result.venv_packages, result.requirements_packages)
    if result.diff.has_drift:
        result.status = AuditStatus.DRIFT
        if update:
            backup, _ = write_requirements(project_path, freeze_output, result.python_version)
            result.backup_path = backup
            result.requirements_updated = True
    else:
        result.status = AuditStatus.IN_SYNC

    return result


# =============================================================================
# REPORTING
# =============================================================================

def format_summary_line(result: ProjectAuditResult) -> str:
    """Produce one console-aligned summary line for a project."""
    name_col = result.project_name[:28].ljust(28)
    status_col = result.status.value.ljust(20)

    detail = ""
    if result.status == AuditStatus.IN_SYNC:
        detail = f"{len(result.venv_packages)} pkgs"
    elif result.status == AuditStatus.DRIFT and result.diff:
        d = result.diff
        parts = []
        if d.added:
            parts.append(f"+{len(d.added)}")
        if d.removed:
            parts.append(f"-{len(d.removed)}")
        if d.changed:
            parts.append(f"~{len(d.changed)}")
        detail = " ".join(parts)
        if result.requirements_updated:
            detail += "  (UPDATED)"
    elif result.status == AuditStatus.MISSING_REQUIREMENTS:
        detail = f"{len(result.venv_packages)} pkgs in venv"
        if result.requirements_updated:
            detail += "  (CREATED)"
    elif result.status == AuditStatus.NO_VENV:
        detail = "skipped (no .venv)"
    elif result.error_message:
        detail = result.error_message

    return f"  {name_col} {status_col} {detail}"


def print_drift_detail(result: ProjectAuditResult) -> None:
    """Print the per-package drift breakdown for one project."""
    if not result.diff or not result.diff.has_drift:
        return
    d = result.diff
    print(f"\n  {result.project_name}:")
    if d.added:
        print(f"    Added (in .venv, not in requirements.txt):")
        for n, v in d.added:
            print(f"      + {n}=={v}")
    if d.removed:
        print(f"    Removed (in requirements.txt, not in .venv):")
        for n, v in d.removed:
            print(f"      - {n}=={v}")
    if d.changed:
        print(f"    Version changed:")
        for n, rv, vv in d.changed:
            print(f"      ~ {n}: {rv} -> {vv}")


def print_report(results: List[ProjectAuditResult]) -> None:
    """Print the full console report."""
    print()
    print("=" * 78)
    print(f"  Requirements Audit  -  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 78)
    print()

    print(f"  {'Project'.ljust(28)} {'Status'.ljust(20)} Detail")
    print(f"  {'-' * 28} {'-' * 20} {'-' * 24}")
    for r in results:
        print(format_summary_line(r))

    # Summary by status
    status_counts: Dict[AuditStatus, int] = {}
    for r in results:
        status_counts[r.status] = status_counts.get(r.status, 0) + 1

    print()
    print("  Summary:")
    print(f"    Total projects scanned: {len(results)}")
    for status in AuditStatus:
        count = status_counts.get(status, 0)
        if count:
            print(f"    {status.value}: {count}")

    # Drift detail (the actionable part)
    drift_results = [r for r in results if r.status == AuditStatus.DRIFT]
    if drift_results:
        print()
        print("  Drift detail:")
        for r in drift_results:
            print_drift_detail(r)

    print()


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit each project's .venv against its requirements.txt.",
    )
    parser.add_argument(
        '--root', type=Path, default=DEFAULT_ROOT,
        help=f"Root directory to scan (default: {DEFAULT_ROOT})",
    )
    parser.add_argument(
        '--update', action='store_true',
        help="Regenerate requirements.txt where drift or missing "
             "(creates timestamped backups of any existing files).",
    )
    parser.add_argument(
        '--project', type=str, default=None,
        help="Operate on a single project by folder name (e.g. PMAssistant).",
    )
    args = parser.parse_args()

    root = args.root.resolve()

    with ScriptTimer(SCRIPT_NAME, logger):
        logger.info(f"Root directory: {root}")
        if args.project:
            logger.info(f"Single-project mode: {args.project}")
        if args.update:
            logger.info("UPDATE mode enabled - drift and missing requirements.txt will be regenerated")
        else:
            logger.info("Audit-only mode (no files will be written)")

        projects = discover_projects(root, args.project)
        if not projects:
            logger.warning("No projects discovered.")
            return

        logger.info(f"Discovered {len(projects)} project(s) to audit")

        results: List[ProjectAuditResult] = []
        for project_path in projects:
            logger.info(f"Auditing {project_path.name}...")
            try:
                result = audit_project(project_path, update=args.update)
            except Exception as e:
                logger.error(f"  Unexpected error auditing {project_path.name}: {e}")
                result = ProjectAuditResult(
                    project_name=project_path.name,
                    project_path=project_path,
                    status=AuditStatus.PIP_FREEZE_FAILED,
                    error_message=str(e),
                )
            results.append(result)
            logger.info(f"  -> {result.status.value}")

        print_report(results)
        logger.info(f"Audit complete: {len(results)} project(s) processed")


if __name__ == '__main__':
    main()