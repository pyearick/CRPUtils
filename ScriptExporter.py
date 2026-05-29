"""
ScriptExporter.py
=================
Exports all curated PycharmProjects source files to a single zip archive
for sharing with other developers (e.g. Sai).

Collects .py, .ps1, .bat, .md, and requirements.txt from every project
except the explicitly skipped experimental/research folders.

Usage
-----
    # Export to default destination (Results\\ScriptExports\\)
    python ScriptExporter.py

    # Export to a custom location
    python ScriptExporter.py --output C:\\temp

    # Use a non-default root
    python ScriptExporter.py --root "C:\\some\\other\\PycharmProjects"

Output
------
- Zip file: CRP_Scripts_YYYYMMDD_HHMMSS.zip
- Default destination: <CRPAF>\\Results\\ScriptExports\\
- Console: per-project file counts + summary
- Log: C:\\Logs\\ScriptExporter.log
"""

import argparse
import zipfile
from datetime import datetime
from pathlib import Path

from logging_utils import setup_logging, ScriptTimer

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_NAME = 'ScriptExporter'
logger = setup_logging(SCRIPT_NAME)

# PycharmProjects root — parent of the CRPUtils folder this script lives in
DEFAULT_ROOT = Path(__file__).resolve().parent.parent

# Default output folder — Results/ScriptExports at the CRPAF level
DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent.parent / 'Results' / 'ScriptExports'

# Projects to skip (experimental, research, backups, or otherwise not for Sai)
# Edit this list to add or remove projects from the export.
SKIP_PROJECTS = {
    'AST-Neo4J', 'LangChain', 'CRPML', 'SynapseML',
    'ProdDev(tf Keras)', 'SQLAlchemy', 'SQLAlchemyTest',
    'DeepResearch', 'ProjectBackups', 'Requirements-txt_Update', 'PunchlistReview',
}

# Directories to skip while walking inside a project folder
SKIP_DIRS = {
    '.venv', 'venv',  # both naming conventions in use across projects
    '__pycache__', '.git', '.idea', 'build', 'dist',
    'node_modules', '.mypy_cache', '.ipynb_checkpoints',
}

# File extensions to include
INCLUDE_EXTENSIONS = {'.py', '.ps1', '.bat', '.md'}

# Exact filenames to include regardless of extension
INCLUDE_FILENAMES = {'requirements.txt'}


# =============================================================================
# CORE LOGIC
# =============================================================================

def should_include(path: Path) -> bool:
    return path.suffix.lower() in INCLUDE_EXTENSIONS or path.name.lower() in INCLUDE_FILENAMES


def collect_project_files(project_dir: Path) -> list:
    """Walk a project folder without entering skipped directories."""
    files = []
    queue = [project_dir]
    while queue:
        current = queue.pop()
        try:
            entries = list(current.iterdir())
        except PermissionError:
            logger.warning(f"Permission denied: {current}")
            continue
        for entry in entries:
            if entry.is_dir():
                if entry.name not in SKIP_DIRS:
                    queue.append(entry)
            elif entry.is_file() and should_include(entry):
                files.append(entry)
    return files


def build_zip(root: Path, output_dir: Path):
    """Collect files from all curated projects and write a zip to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_path = output_dir / f'CRP_Scripts_{timestamp}.zip'

    total_files = 0
    project_summary = []

    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for project_dir in sorted(root.iterdir()):
            if not project_dir.is_dir():
                continue
            if project_dir.name in SKIP_PROJECTS:
                logger.debug(f"Skipping {project_dir.name}")
                continue

            files = collect_project_files(project_dir)
            if not files:
                continue

            for f in files:
                arcname = f.relative_to(root)
                zf.write(f, arcname)

            project_summary.append((project_dir.name, len(files)))
            total_files += len(files)
            logger.info(f"  {project_dir.name}: {len(files)} file(s)")

    return zip_path, project_summary, total_files


def print_summary(zip_path: Path, project_summary: list, total_files: int):
    width = max((len(name) for name, _ in project_summary), default=20)
    print(f"\n{'Project':<{width}}  Files")
    print('-' * (width + 8))
    for name, count in project_summary:
        print(f"{name:<{width}}  {count}")
    print('-' * (width + 8))
    print(f"{'TOTAL':<{width}}  {total_files}")
    size_kb = zip_path.stat().st_size / 1024
    print(f"\nZip:  {zip_path}")
    print(f"Size: {size_kb:,.0f} KB\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Export curated CRP PycharmProjects scripts to a zip archive.'
    )
    parser.add_argument(
        '--root',
        type=Path,
        default=DEFAULT_ROOT,
        help=f'PycharmProjects root folder (default: {DEFAULT_ROOT})',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f'Destination folder for the zip file (default: {DEFAULT_OUTPUT})',
    )
    args = parser.parse_args()

    if not args.root.is_dir():
        logger.error(f"Root folder not found: {args.root}")
        raise SystemExit(1)

    logger.info(f"Scanning: {args.root}")
    logger.info(f"Output:   {args.output}")

    with ScriptTimer(SCRIPT_NAME):
        zip_path, project_summary, total_files = build_zip(args.root, args.output)

    print_summary(zip_path, project_summary, total_files)
    logger.info(f"Done. {total_files} files -> {zip_path}")


if __name__ == '__main__':
    main()
