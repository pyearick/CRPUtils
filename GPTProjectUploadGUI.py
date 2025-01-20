import os
import datetime
import json
import zipfile
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox

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

def extract_imports(file_content):
    """Extracts the import statements from a Python file."""
    imports = []
    for line in file_content.splitlines():
        line = line.strip()
        if line.startswith("import") or line.startswith("from"):
            imports.append(line)
    return imports

def extract_table_references(file_content):
    """Extracts potential table references from a Python file."""
    table_references = []
    for line in file_content.splitlines():
        if any(keyword in line for keyword in ["SELECT", "FROM", "JOIN"]):
            table_references.append(line.strip())
    return table_references

def create_project_document(directory_path, compress_output=False):
    """Creates an XML document summarizing Python files in the specified directory."""
    excluded_dirs = {".venv", "venv", "__pycache__", ".idea", ".git", ".venvBISQL001", "Archive"}

    output = "<documents>"

    # Find all Python files recursively, excluding specific directories
    python_files = []
    for root, dirs, files in os.walk(directory_path):
        dirs[:] = [d for d in dirs if d.lower() not in {e.lower() for e in excluded_dirs}]
        for file in files:
            if file.endswith(".py"):
                python_files.append(os.path.join(root, file))

    python_files.sort()  # Sort files alphabetically

    for index, file_path in enumerate(python_files, 1):
        relative_path = os.path.relpath(file_path, directory_path)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            imports = extract_imports(content)
            tables = extract_table_references(content)
            metadata = {
                "size": os.path.getsize(file_path),
                "created": datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                "modified": datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
            }

            output += f"""
<document index=\"{index}\">
    <source>{relative_path}</source>
    <metadata>
        <size>{metadata['size']}</size>
        <created>{metadata['created']}</created>
        <modified>{metadata['modified']}</modified>
    </metadata>
    <references>
        <imports>{', '.join(imports) if imports else 'None'}</imports>
        <tables>{', '.join(tables) if tables else 'None'}</tables>
    </references>
    <summary>
        Number of lines: {len(content.splitlines())}
        Number of imports: {len(imports)}
        Number of table references: {len(tables)}
    </summary>
</document>"""

        except Exception as e:
            output += f"<error>{relative_path}: {e}</error>"

    output += "</documents>"

    output_path = os.path.join(directory_path, "project_document.xml")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output)
    except Exception as e:
        raise IOError(f"Failed to write file {output_path}: {e}")

    if compress_output:
        zip_path = output_path.replace(".xml", ".zip")
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(output_path, arcname="project_document.xml")
            os.remove(output_path)
            output_path = zip_path
        except Exception as e:
            raise IOError(f"Failed to compress file {zip_path}: {e}")

    return output_path

def select_directory():
    """Open a Tkinter file dialog to select a directory."""
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    initial_dir = load_last_directory() or os.getcwd()
    selected_dir = filedialog.askdirectory(initialdir=initial_dir, title="Select Directory")
    if selected_dir:
        save_last_directory(selected_dir)
    return selected_dir

# Example Usage
def main():
    directory = select_directory()
    if directory:
        output_file = create_project_document(directory, compress_output=False)
        print(f"XML document created at: {output_file}")
    else:
        print("No directory selected.")

if __name__ == "__main__":
    main()
