import os
import tkinter as tk
from tkinter import filedialog, messagebox
import zipfile
import datetime
import json
from tkinter import scrolledtext
import shutil
import pyperclip

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

def create_project_document(directory_path, compress_output=False):
    """Creates an XML document containing all Python files in the specified directory."""
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

    return output_path, log

def copy_file():
    """Copies the generated XML file path to the clipboard."""
    source_path = os.path.join(dir_path.get(), "project_document.xml")
    if not os.path.exists(source_path):
        messagebox.showerror("Error", "No generated XML file to copy.")
        return

    try:
        pyperclip.copy(source_path)
        messagebox.showinfo("Success", f"File path copied to clipboard: {source_path}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to copy file path: {e}")

def generate_xml():
    """Handles the XML generation process when the button is clicked."""
    directory = dir_path.get()
    if not os.path.exists(directory):
        messagebox.showerror("Error", "Please select a valid directory.")
        return

    try:
        output_path, log = create_project_document(directory)
        with open(output_path, 'r', encoding='utf-8') as f:
            content = f.read()
            text_area.delete("1.0", tk.END)
            text_area.insert(tk.END, content)
            status_label.config(text=f"Generated: {output_path}")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

def select_directory():
    """Opens a dialog to select a directory."""
    initial_dir = load_last_directory() or "."
    selected_dir = filedialog.askdirectory(initialdir=initial_dir)
    if selected_dir:
        save_last_directory(selected_dir)
        dir_path.set(selected_dir)

# Set up the main Tkinter window
root = tk.Tk()
root.title("Project Document Generator")
root.geometry("800x600")

dir_path = tk.StringVar()

# Directory selection frame
frame = tk.Frame(root)
frame.pack(pady=10)

dir_label = tk.Label(frame, text="Select Directory:")
dir_label.pack(side=tk.LEFT, padx=5)

dir_entry = tk.Entry(frame, textvariable=dir_path, width=50)
dir_entry.pack(side=tk.LEFT, padx=5)

browse_button = tk.Button(frame, text="Browse", command=select_directory)
browse_button.pack(side=tk.LEFT, padx=5)

# Buttons for generate and copy
button_frame = tk.Frame(root)
button_frame.pack(pady=10)

generate_button = tk.Button(button_frame, text="Generate XML", command=generate_xml)
generate_button.pack(side=tk.LEFT, padx=10)

copy_button = tk.Button(button_frame, text="Copy File", command=copy_file)
copy_button.pack(side=tk.LEFT, padx=10)

# Text area to display output
text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=90, height=25)
text_area.pack(pady=10)

# Status label
status_label = tk.Label(root, text="", anchor="w")
status_label.pack(fill=tk.X, padx=5, pady=5)

root.mainloop()
