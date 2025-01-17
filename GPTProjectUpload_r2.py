import os
import tkinter as tk
from tkinter import filedialog
import zipfile
import datetime
import argparse
import json

LAST_DIRECTORY_FILE = os.path.expanduser("~/.last_directory.json")


def save_last_directory(directory):
    """Save the last visited directory to a file."""
    with open(LAST_DIRECTORY_FILE, 'w') as f:
        json.dump({"last_directory": directory}, f)


def load_last_directory():
    """Load the last visited directory from a file, if it exists."""
    if os.path.exists(LAST_DIRECTORY_FILE):
        try:
            with open(LAST_DIRECTORY_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_directory", None)
        except Exception:
            return None
    return None


def select_directory():
    """
    Opens a tkinter dialog to select a directory.
    Returns the selected directory path or None if cancelled.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window

    initial_dir = load_last_directory() or "."

    directory_path = filedialog.askdirectory(
        title="Select Project Directory",
        initialdir=initial_dir
    )

    if directory_path:
        save_last_directory(directory_path)

    return directory_path


def extract_imports(file_content):
    """
    Extracts the import statements from a Python file.
    """
    imports = []
    for line in file_content.splitlines():
        line = line.strip()
        if line.startswith("import") or line.startswith("from"):
            imports.append(line)
    return imports


def create_project_document(directory_path, compress_output=False):
    """
    Creates a Claude-formatted document containing all Python files in the specified directory,
    excluding specific folders like .venv, including metadata and optional compression.
    """
    excluded_dirs = {".venv", "__pycache__", ".idea", ".git"}
    output = "<documents>"
    log = []  # Track errors and processing information

    # Find all Python files recursively, excluding specific directories
    python_files = []
    for root, dirs, files in os.walk(directory_path):
        # Exclude directories dynamically, case-insensitive
        dirs[:] = [d for d in dirs if d.lower() not in {e.lower() for e in excluded_dirs}]

        for file in files:
            if file.endswith(".py"):
                python_files.append(os.path.join(root, file))

    python_files.sort()  # Sort files alphabetically

    # Process each file
    for index, file_path in enumerate(python_files, 1):
        relative_path = os.path.relpath(file_path, directory_path)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            imports = extract_imports(content)
            metadata = {
                "size": os.path.getsize(file_path),
                "created": datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                "modified": datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
            }

            # Create the document entry
            output += f"""<document index=\"{index}\">
<source>{relative_path}</source>
<metadata>
    <size>{metadata['size']}</size>
    <created>{metadata['created']}</created>
    <modified>{metadata['modified']}</modified>
</metadata>
<imports>
    {', '.join(imports) if imports else 'None'}
</imports>
<document_content>{content}</document_content>
</document>"""

        except Exception as e:
            log.append(f"Error processing {relative_path}: {e}")

    output += "</documents>"

    # Write to output file
    output_path = os.path.join(directory_path, "project_document.xml")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)

    # Optionally compress the output
    if compress_output:
        zip_path = output_path.replace(".xml", ".zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(output_path, arcname="project_document.xml")
        os.remove(output_path)  # Remove uncompressed file
        output_path = zip_path

    # Write log file
    log_path = os.path.join(directory_path, "processing_log.txt")
    with open(log_path, 'w', encoding='utf-8') as log_file:
        log_file.write("\n".join(log))

    print(f"\nProcessed {len(python_files)} Python files:")
    for file in python_files:
        print(f"- {os.path.relpath(file, directory_path)}")
    print(f"\nOutput saved to: {output_path}")
    if log:
        print(f"\nErrors logged to: {log_path}")


def main():
    while True:
        parser = argparse.ArgumentParser(description="Generate a project document for Python files.")
        parser.add_argument("--directory", type=str, help="Path to the project directory.", default=None)
        parser.add_argument("--compress", action="store_true", help="Compress the output XML file into a ZIP archive.")

        args = parser.parse_args()

        if not args.directory:
            print("Please select your project directory...")
            args.directory = select_directory()

        if args.directory:
            print(f"Selected directory: {args.directory}")
            create_project_document(args.directory, compress_output=args.compress)
        else:
            print("No directory selected. Exiting.")
            break

        repeat = input("Do you want to process another directory? (y/n): ").strip().lower()
        if repeat != 'y':
            break


if __name__ == "__main__":
    main()
