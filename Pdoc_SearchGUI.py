"""
PdocSearchGUI.py
Search tool for pdoc_*.xml project snapshot files.
Opens the XML, lets you search all scripts for a string,
and displays the full script with matches highlighted.
"""

import os
import json
import xml.etree.ElementTree as ET
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import html
import re

SETTINGS_FILE = os.path.expanduser("~/.pdoc_search_settings.json")
LOG_DIR = r"C:\Logs"
LOG_FILE = os.path.join(LOG_DIR, "PdocSearchGUI.log")


def log_message(msg):
    """Append a message to the log file."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{msg}\n")
    except Exception:
        pass


def save_settings(data):
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def unescape_xml_content(content):
    """Unescape XML-escaped content back to readable code."""
    return html.unescape(content)


def parse_pdoc_xml(filepath):
    """
    Parse a pdoc_*.xml file and return a list of dicts:
    [{ 'source': relative_path, 'content': full_script_text }, ...]
    """
    documents = []
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        for doc_elem in root.findall("document"):
            source_elem = doc_elem.find("source")
            content_elem = doc_elem.find("sourceCode")
            if source_elem is not None and content_elem is not None:
                source = source_elem.text or ""
                raw_content = content_elem.text or ""
                content = unescape_xml_content(raw_content)
                documents.append({"source": source.strip(), "content": content})
    except ET.ParseError as e:
        log_message(f"XML parse error: {e}")
        raise
    except Exception as e:
        log_message(f"Error reading {filepath}: {e}")
        raise
    return documents


class PdocSearchApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Pdoc Search")
        self.root.geometry("1100x750")
        self.root.minsize(800, 500)

        self.documents = []       # parsed documents from XML
        self.search_results = []  # list of (doc_index, line_number, line_text)
        self.loaded_file = None

        settings = load_settings()
        self.last_dir = settings.get("last_dir", os.path.expanduser("~"))
        self.last_search = settings.get("last_search", "")

        self._build_ui()
        log_message("PdocSearchGUI started")

    def _build_ui(self):
        # --- Top bar: file selection ---
        file_frame = tk.Frame(self.root)
        file_frame.pack(fill=tk.X, padx=6, pady=(6, 2))

        tk.Button(file_frame, text="Open pdoc XML...", command=self._open_file).pack(side=tk.LEFT)
        self.file_label = tk.Label(file_frame, text="No file loaded", anchor="w", fg="gray")
        self.file_label.pack(side=tk.LEFT, padx=8, fill=tk.X, expand=True)

        # --- Search bar ---
        search_frame = tk.Frame(self.root)
        search_frame.pack(fill=tk.X, padx=6, pady=2)

        tk.Label(search_frame, text="Search:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar(value=self.last_search)
        self.search_entry = tk.Entry(search_frame, textvariable=self.search_var, width=40)
        self.search_entry.pack(side=tk.LEFT, padx=4)
        self.search_entry.bind("<Return>", lambda e: self._do_search())

        self.case_var = tk.BooleanVar(value=False)
        tk.Checkbutton(search_frame, text="Case sensitive", variable=self.case_var).pack(side=tk.LEFT, padx=4)

        tk.Button(search_frame, text="Search", command=self._do_search).pack(side=tk.LEFT, padx=4)

        self.result_count_label = tk.Label(search_frame, text="", fg="blue")
        self.result_count_label.pack(side=tk.LEFT, padx=8)

        # --- Main paned area: results tree (left) + script viewer (right) ---
        paned = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashwidth=6)
        paned.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)

        # Left: results tree
        left_frame = tk.Frame(paned)
        paned.add(left_frame, width=420, minsize=250)

        tree_label = tk.Label(left_frame, text="Search Results", font=("Segoe UI", 9, "bold"), anchor="w")
        tree_label.pack(fill=tk.X)

        tree_scroll_y = tk.Scrollbar(left_frame, orient=tk.VERTICAL)
        tree_scroll_x = tk.Scrollbar(left_frame, orient=tk.HORIZONTAL)
        self.tree = ttk.Treeview(
            left_frame,
            columns=("line", "text"),
            show="tree headings",
            yscrollcommand=tree_scroll_y.set,
            xscrollcommand=tree_scroll_x.set,
        )
        self.tree.heading("#0", text="Script", anchor="w")
        self.tree.heading("line", text="Line", anchor="w")
        self.tree.heading("text", text="Match", anchor="w")
        self.tree.column("#0", width=200, minwidth=120)
        self.tree.column("line", width=45, minwidth=40, stretch=False)
        self.tree.column("text", width=300, minwidth=100)

        tree_scroll_y.config(command=self.tree.yview)
        tree_scroll_x.config(command=self.tree.xview)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(fill=tk.BOTH, expand=True)

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        # Right: script viewer
        right_frame = tk.Frame(paned)
        paned.add(right_frame, minsize=300)

        self.viewer_label = tk.Label(right_frame, text="Script Viewer", font=("Segoe UI", 9, "bold"), anchor="w")
        self.viewer_label.pack(fill=tk.X)

        viewer_scroll_y = tk.Scrollbar(right_frame, orient=tk.VERTICAL)
        viewer_scroll_x = tk.Scrollbar(right_frame, orient=tk.HORIZONTAL)
        self.viewer = tk.Text(
            right_frame,
            wrap=tk.NONE,
            font=("Consolas", 10),
            state=tk.DISABLED,
            yscrollcommand=viewer_scroll_y.set,
            xscrollcommand=viewer_scroll_x.set,
        )
        viewer_scroll_y.config(command=self.viewer.yview)
        viewer_scroll_x.config(command=self.viewer.xview)
        viewer_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        viewer_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.viewer.pack(fill=tk.BOTH, expand=True)

        # Tag for highlighting matches
        self.viewer.tag_configure("highlight", background="#FFFF00", foreground="#000000")
        self.viewer.tag_configure("current_line", background="#FFFFAA")

        # --- Status bar ---
        self.status_var = tk.StringVar(value="Ready")
        status_bar = tk.Label(self.root, textvariable=self.status_var, anchor="w", relief=tk.SUNKEN, bd=1)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)

    # ----- File handling -----

    def _open_file(self):
        filepath = filedialog.askopenfilename(
            initialdir=self.last_dir,
            title="Select pdoc XML file",
            filetypes=[("XML files", "*.xml"), ("All files", "*.*")],
        )
        if not filepath:
            return
        self.last_dir = os.path.dirname(filepath)
        save_settings({"last_dir": self.last_dir, "last_search": self.search_var.get()})

        try:
            self.documents = parse_pdoc_xml(filepath)
            self.loaded_file = filepath
            fname = os.path.basename(filepath)
            self.file_label.config(text=f"{fname}  ({len(self.documents)} scripts)", fg="black")
            self.status_var.set(f"Loaded {fname} — {len(self.documents)} scripts")
            log_message(f"Loaded {filepath} with {len(self.documents)} documents")
            # Clear previous results
            self._clear_results()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to parse XML:\n{e}")
            log_message(f"Failed to parse {filepath}: {e}")

    # ----- Search -----

    def _clear_results(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.search_results = []
        self.result_count_label.config(text="")
        self.viewer.config(state=tk.NORMAL)
        self.viewer.delete("1.0", tk.END)
        self.viewer.config(state=tk.DISABLED)
        self.viewer_label.config(text="Script Viewer")

    def _do_search(self):
        query = self.search_var.get().strip()
        if not query:
            messagebox.showinfo("Search", "Please enter a search term.")
            return
        if not self.documents:
            messagebox.showinfo("Search", "No XML file loaded. Open a pdoc file first.")
            return

        save_settings({"last_dir": self.last_dir, "last_search": query})

        case_sensitive = self.case_var.get()
        self._clear_results()

        total_matches = 0
        files_with_matches = 0

        for doc_idx, doc in enumerate(self.documents):
            source = doc["source"]
            lines = doc["content"].splitlines()

            matches_in_file = []
            for line_num, line in enumerate(lines, start=1):
                if case_sensitive:
                    if query in line:
                        matches_in_file.append((line_num, line.strip()))
                else:
                    if query.lower() in line.lower():
                        matches_in_file.append((line_num, line.strip()))

            if matches_in_file:
                files_with_matches += 1
                # Get just the filename for the tree display
                display_name = os.path.basename(source)
                parent_id = self.tree.insert(
                    "",
                    tk.END,
                    text=f"{display_name} ({len(matches_in_file)})",
                    values=("", source),
                    open=True,
                )
                # Store doc_idx on the parent for lookup
                self.tree.set(parent_id, "line", "")
                # Tag parent with doc index
                for line_num, line_text in matches_in_file:
                    truncated = line_text[:120] + ("..." if len(line_text) > 120 else "")
                    child_id = self.tree.insert(
                        parent_id,
                        tk.END,
                        text="",
                        values=(str(line_num), truncated),
                    )
                    # Store lookup info
                    self.search_results.append({
                        "tree_id": child_id,
                        "doc_idx": doc_idx,
                        "line_num": line_num,
                    })
                total_matches += len(matches_in_file)

        self.result_count_label.config(
            text=f"{total_matches} match{'es' if total_matches != 1 else ''} in {files_with_matches} file{'s' if files_with_matches != 1 else ''}"
        )
        self.status_var.set(f"Search complete: '{query}' — {total_matches} matches in {files_with_matches} files")
        log_message(f"Search '{query}' — {total_matches} matches in {files_with_matches} files")

    # ----- Tree selection → Script viewer -----

    def _on_tree_select(self, event):
        selected = self.tree.selection()
        if not selected:
            return
        item_id = selected[0]

        # Determine which document and line to show
        doc_idx = None
        target_line = None

        # Check if it's a child (match line) or parent (file)
        parent_id = self.tree.parent(item_id)
        if parent_id:
            # It's a child match row — find the result entry
            for res in self.search_results:
                if res["tree_id"] == item_id:
                    doc_idx = res["doc_idx"]
                    target_line = res["line_num"]
                    break
        else:
            # It's a parent file row — find doc_idx from the first child
            children = self.tree.get_children(item_id)
            if children:
                for res in self.search_results:
                    if res["tree_id"] == children[0]:
                        doc_idx = res["doc_idx"]
                        target_line = res["line_num"]
                        break

        if doc_idx is None:
            return

        doc = self.documents[doc_idx]
        self._display_script(doc, target_line)

    def _display_script(self, doc, target_line=None):
        """Load a script into the viewer with line numbers, highlight matches."""
        query = self.search_var.get().strip()
        case_sensitive = self.case_var.get()

        self.viewer_label.config(text=doc["source"])
        self.viewer.config(state=tk.NORMAL)
        self.viewer.delete("1.0", tk.END)

        lines = doc["content"].splitlines()
        # Calculate width needed for line numbers
        num_width = len(str(len(lines)))

        for i, line in enumerate(lines, start=1):
            line_number_str = f"{i:>{num_width}}  "
            self.viewer.insert(tk.END, line_number_str + line + "\n")

        # Highlight all matches
        if query:
            search_flags = "" if case_sensitive else "-nocase"
            start_pos = "1.0"
            while True:
                if case_sensitive:
                    pos = self.viewer.search(query, start_pos, stopindex=tk.END)
                else:
                    pos = self.viewer.search(query, start_pos, stopindex=tk.END, nocase=True)
                if not pos:
                    break
                end_pos = f"{pos}+{len(query)}c"
                self.viewer.tag_add("highlight", pos, end_pos)
                start_pos = end_pos

        # Scroll to the target line
        if target_line and target_line <= len(lines):
            line_index = f"{target_line}.0"
            self.viewer.see(line_index)
            # Highlight the full line background
            line_end = f"{target_line}.end"
            self.viewer.tag_add("current_line", line_index, line_end)

        self.viewer.config(state=tk.DISABLED)


def main():
    root = tk.Tk()
    app = PdocSearchApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()