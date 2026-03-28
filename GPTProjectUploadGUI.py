import os
import datetime
import json
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET
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

def escape_xml_content(content):
    """Escape special characters for valid XML."""
    return (
        content.replace("&", "&amp;")
               .replace("<", "&lt;")
               .replace(">", "&gt;")
               .replace('"', "&quot;")
               .replace("'", "&apos;")
    )

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

def create_project_document(directory_path, prefix="", compress_output=False):
    """Creates an XML document summarizing script files in the specified directory,
    supporting optional prefix filtering."""
    excluded_dirs = {".venv", "venv", "__pycache__", ".idea", ".git", ".venvBISQL001"}

    output = "<documents>"

    # Find all supported files recursively, excluding specific directories
    script_files = []
    for root, dirs, files in os.walk(directory_path):
        dirs[:] = [d for d in dirs if d.lower() not in {e.lower() for e in excluded_dirs}]
        for file in files:
            if file.startswith(prefix) and file.lower().endswith(('.py', '.ps1', '.bat')):
                script_files.append(os.path.join(root, file))

    script_files.sort()  # Sort files alphabetically

    for index, file_path in enumerate(script_files, 1):
        relative_path = os.path.relpath(file_path, directory_path)
        extension = os.path.splitext(file_path)[1].lower()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            if extension == '.py':
                imports = extract_imports(content)
                tables = extract_table_references(content)
            else:
                imports = []
                tables = []

            # Check if file is in an Archive subdirectory
            is_archived = "Archive" in Path(relative_path).parts

            metadata = {
                "size": os.path.getsize(file_path),
                "created": datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                "modified": datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
                "archived": is_archived,
            }

            output += f"""
<document index="{index}">
    <source>{escape_xml_content(relative_path)}</source>
    <metadata>
        <size>{metadata['size']}</size>
        <created>{metadata['created']}</created>
        <modified>{metadata['modified']}</modified>
        <archived>{str(metadata['archived']).lower()}</archived>
    </metadata>
    <references>
        <imports>
{"".join(f"            <import>{escape_xml_content(imp)}</import>\n" for imp in imports)}
        </imports>
        <tables>
{"".join(f"            <query>{escape_xml_content(table)}</query>\n" for table in tables)}
        </tables>
    </references>
    <sourceCode>
{escape_xml_content(content)}
    </sourceCode>
    <summary>
        Number of lines: {len(content.splitlines())}
        Number of imports: {len(imports)}
        Number of table references: {len(tables)}
    </summary>
</document>"""

        except Exception as e:
            output += f"<error>{escape_xml_content(relative_path)}: {escape_xml_content(str(e))}</error>"

    output += "</documents>"

    # Get folder name for output filename
    folder_name = os.path.basename(directory_path)
    output_filename = f"pdoc_{folder_name}.xml"
    output_path = os.path.join(directory_path, output_filename)
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output)
    except Exception as e:
        raise IOError(f"Failed to write file {output_path}: {e}")

    if compress_output:
        zip_path = output_path.replace(".xml", ".zip")
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(output_path, arcname=output_filename)
            os.remove(output_path)
            output_path = zip_path
        except Exception as e:
            raise IOError(f"Failed to compress file {zip_path}: {e}")

    # Validate the generated XML
    try:
        ET.parse(output_path)
        print("XML validation successful.")
    except ET.ParseError as e:
        print(f"XML validation failed: {e}")

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

def ask_for_prefix():
    """Prompt the user to enter a prefix for filtering files."""
    root = tk.Tk()
    root.withdraw()  # Hide the main window

    prefix = tk.simpledialog.askstring(
        "File Prefix Filter",
        "Enter a prefix to filter files (leave blank for no filtering):"
    )
    if prefix is None:
        prefix = ""  # Treat cancel as empty prefix (include everything)
    return prefix.strip()

def main():
    directory = select_directory()
    if directory:
        prefix = ask_for_prefix()
        output_file = create_project_document(directory, prefix=prefix, compress_output=False)
        print(f"XML document created at: {output_file}")
        
        # Open the folder containing the output file in Windows Explorer
        os.startfile(directory)
    else:
        print("No directory selected.")

if __name__ == "__main__":
    main()