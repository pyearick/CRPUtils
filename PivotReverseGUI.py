"""
PivotReverseGUI.py
Point this at an Excel workbook that already contains a PivotTable and it
reverse-engineers the CRPUtils.excel_pivot.build_pivot_workbook(...) call that
recreates it. Build the pivot by hand in Excel (the easy, visual part), save,
then Browse to it here and copy the generated Python.

Reading uses openpyxl (no Excel required), so this runs anywhere.
"""

import os
import json
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from excel_pivot import describe_pivots, pivot_to_code

SETTINGS_FILE = os.path.expanduser("~/.pivot_reverse_settings.json")
LOG_DIR = r"C:\Logs"
LOG_FILE = os.path.join(LOG_DIR, "PivotReverseGUI.log")


def log_message(msg):
    """Append a message to the log file (best effort)."""
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


class PivotReverseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Pivot Reverse Engineer - excel_pivot")
        self.root.geometry("900x650")
        self.settings = load_settings()
        self.current_path = None

        # --- Top bar: file picker ---
        top = ttk.Frame(root, padding=10)
        top.pack(fill=tk.X)

        ttk.Label(top, text="Workbook:").pack(side=tk.LEFT)
        self.path_var = tk.StringVar()
        self.path_entry = ttk.Entry(top, textvariable=self.path_var)
        self.path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(top, text="Browse...", command=self.browse).pack(side=tk.LEFT)
        ttk.Button(top, text="Analyze", command=self.analyze).pack(side=tk.LEFT, padx=(6, 0))

        # --- Options ---
        opts = ttk.Frame(root, padding=(10, 0))
        opts.pack(fill=tk.X)
        ttk.Label(opts, text="DataFrame variable:").pack(side=tk.LEFT)
        self.df_var = tk.StringVar(value="df")
        ttk.Entry(opts, textvariable=self.df_var, width=12).pack(side=tk.LEFT, padx=(4, 16))
        ttk.Label(opts, text="output_file:").pack(side=tk.LEFT)
        self.out_var = tk.StringVar(value="output.xlsx")
        ttk.Entry(opts, textvariable=self.out_var, width=30).pack(side=tk.LEFT, padx=4)

        # --- Pivot list ---
        mid = ttk.Frame(root, padding=10)
        mid.pack(fill=tk.X)
        ttk.Label(mid, text="Pivots found:").pack(anchor=tk.W)
        self.pivot_list = tk.Listbox(mid, height=4)
        self.pivot_list.pack(fill=tk.X)

        # --- Generated code ---
        codeframe = ttk.Frame(root, padding=10)
        codeframe.pack(fill=tk.BOTH, expand=True)
        ttk.Label(codeframe, text="Generated Python:").pack(anchor=tk.W)
        self.code_text = tk.Text(codeframe, wrap=tk.NONE, font=("Consolas", 10))
        self.code_text.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        yscroll = ttk.Scrollbar(codeframe, orient=tk.VERTICAL, command=self.code_text.yview)
        yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.code_text.configure(yscrollcommand=yscroll.set)

        # --- Bottom bar ---
        bottom = ttk.Frame(root, padding=10)
        bottom.pack(fill=tk.X)
        self.status = tk.StringVar(value="Browse to an .xlsx that contains a PivotTable.")
        ttk.Label(bottom, textvariable=self.status).pack(side=tk.LEFT)
        ttk.Button(bottom, text="Copy to Clipboard", command=self.copy_code).pack(side=tk.RIGHT)

        last = self.settings.get("last_path")
        if last and os.path.exists(last):
            self.path_var.set(last)

    def browse(self):
        initial = self.settings.get("last_dir") or os.path.expanduser("~")
        path = filedialog.askopenfilename(
            title="Select an Excel workbook with a PivotTable",
            initialdir=initial,
            filetypes=[("Excel workbook", "*.xlsx"), ("All files", "*.*")],
        )
        if path:
            self.path_var.set(path)
            self.analyze()

    def analyze(self):
        path = self.path_var.get().strip().strip('"')
        if not path:
            messagebox.showwarning("No file", "Choose a workbook first.")
            return
        if not os.path.exists(path):
            messagebox.showerror("Not found", f"File does not exist:\n{path}")
            return

        self.pivot_list.delete(0, tk.END)
        self.code_text.delete("1.0", tk.END)
        try:
            pivots = describe_pivots(path)
        except PermissionError:
            messagebox.showerror(
                "Locked",
                "Could not open the file - it may be open in Excel. "
                "Close it and try again.",
            )
            return
        except Exception as exc:
            messagebox.showerror("Error", f"Could not read the workbook:\n{exc}")
            log_message(f"ERROR reading {path}: {exc}")
            return

        if not pivots:
            self.status.set("No PivotTables found in this workbook.")
            self.pivot_list.insert(tk.END, "(none found)")
            return

        for p in pivots:
            self.pivot_list.insert(
                tk.END,
                f"{p['name']}  (sheet {p['sheet']}) - "
                f"{len(p['rows'])} row / {len(p['columns'])} col / "
                f"{len(p['filters'])} filter / {len(p['values'])} value field(s)",
            )

        code = pivot_to_code(
            path,
            df_var=self.df_var.get().strip() or "df",
            output_file=self.out_var.get().strip() or "output.xlsx",
        )
        self.code_text.insert("1.0", code)
        self.status.set(f"Found {len(pivots)} pivot(s). Review / copy the code.")

        self.settings["last_path"] = path
        self.settings["last_dir"] = os.path.dirname(path)
        save_settings(self.settings)
        log_message(f"Analyzed {path}: {len(pivots)} pivot(s)")

    def copy_code(self):
        code = self.code_text.get("1.0", tk.END).strip()
        if not code:
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(code)
        self.status.set("Copied to clipboard.")


def main():
    root = tk.Tk()
    PivotReverseGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
