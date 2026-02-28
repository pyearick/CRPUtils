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


def extract_python_imports(file_content):
    """Extracts the import statements from a Python file."""
    imports = []
    for line in file_content.splitlines():
        line = line.strip()
        if line.startswith("import") or line.startswith("from"):
            imports.append(line)
    return imports


def extract_sql_dependencies(file_content):
    """Extracts dependencies from a T-SQL file."""
    dependencies = []
    for line in file_content.splitlines():
        line = line.strip().upper()
        if line.startswith("USE ") or line.startswith("EXEC ") or line.startswith("EXECUTE "):
            dependencies.append(line)
        # Check for linked servers
        elif "SERVER" in line and any(keyword in line for keyword in ["OPENQUERY", "OPENDATASOURCE"]):
            dependencies.append(line)
    return dependencies


def extract_table_references(file_content, is_sql=False):
    """Extracts table references from a file."""
    table_references = []
    keywords = ["SELECT", "FROM", "JOIN"] if not is_sql else [
        "SELECT", "FROM", "JOIN", "INSERT INTO", "UPDATE", "DELETE FROM",
        "MERGE INTO", "TRUNCATE TABLE", "ALTER TABLE", "DROP TABLE"
    ]

    for line in file_content.splitlines():
        line = line.strip()
        sql_line = line.upper() if is_sql else line
        if any(keyword in sql_line for keyword in keywords):
            table_references.append(line)
    return table_references


def extract_schema_objects(file_content):
    """Extracts schema objects from a T-SQL file."""
    schema_objects = []
    keywords = [
        "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE FUNCTION",
        "CREATE TRIGGER", "CREATE INDEX", "CREATE SCHEMA", "CREATE TYPE",
        "ALTER TABLE", "ALTER VIEW", "ALTER PROCEDURE", "ALTER FUNCTION"
    ]

    for line in file_content.splitlines():
        line = line.strip().upper()
        if any(keyword in line for keyword in keywords):
            schema_objects.append(line)
    return schema_objects


def create_project_document(directory_path, compress_output=False):
    """Creates an XML document summarizing Python and SQL files in the specified directory."""
    excluded_dirs = {".venv", "venv", "__pycache__", ".idea", ".git", ".venvBISQL001", "Archive"}

    output = "<documents>"

    # Find all Python and SQL files recursively, excluding specific directories
    code_files = []
    for root, dirs, files in os.walk(directory_path):
        dirs[:] = [d for d in dirs if d.lower() not in {e.lower() for e in excluded_dirs}]
        for file in files:
            if file.endswith((".py", ".sql")):
                code_files.append(os.path.join(root, file))

    code_files.sort()  # Sort files alphabetically

    for index, file_path in enumerate(code_files, 1):
        relative_path = os.path.relpath(file_path, directory_path)
        is_sql = file_path.endswith('.sql')

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extract appropriate information based on file type
            if is_sql:
                dependencies = extract_sql_dependencies(content)
                tables = extract_table_references(content, is_sql=True)
                schemas = extract_schema_objects(content)
            else:
                dependencies = extract_python_imports(content)
                tables = extract_table_references(content, is_sql=False)
                schemas = []

            metadata = {
                "size": os.path.getsize(file_path),
                "created": datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                "modified": datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
                "type": "SQL" if is_sql else "Python"
            }

            output += f"""
<document index=\"{index}\">
    <source>{relative_path}</source>
    <metadata>
        <size>{metadata['size']}</size>
        <created>{metadata['created']}</created>
        <modified>{metadata['modified']}</modified>
        <type>{metadata['type']}</type>
    </metadata>
    <references>
        <dependencies>
{"".join(f"            <dependency>{escape_xml_content(dep)}</dependency>\n" for dep in dependencies)}
        </dependencies>
        <tables>
{"".join(f"            <query>{escape_xml_content(table)}</query>\n" for table in tables)}
        </tables>"""

            if is_sql:
                output += f"""
        <schemas>
{"".join(f"            <object>{escape_xml_content(schema)}</object>\n" for schema in schemas)}
        </schemas>"""

            output += f"""
    </references>
    <summary>
        Number of lines: {len(content.splitlines())}
        Number of dependencies: {len(dependencies)}
        Number of table references: {len(tables)}
        {f'Number of schema objects: {len(schemas)}' if is_sql else ''}
    </summary>
</document>"""

        except Exception as e:
            output += f"<e>{relative_path}: {e}</e>"

    output += "</documents>"

    # Get folder name for output filename
    folder_name = os.path.basename(directory_path)
    output_filename = f"pdoc_{folder_name}_sql.xml"
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


def main():
    directory = select_directory()
    if directory:
        output_file = create_project_document(directory, compress_output=False)
        print(f"XML document created at: {output_file}")
    else:
        print("No directory selected.")


if __name__ == "__main__":
    main()
