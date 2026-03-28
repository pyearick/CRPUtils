import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pyodbc
from pathlib import Path
import pandas as pd
from datetime import datetime
import os
import re
import subprocess  # For opening Excel files


class SupplierExclusionGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Supplier File Exclusion Manager")
        self.root.geometry("1000x600")

        self.db_conn = "DRIVER={SQL Server};SERVER=BI-SQL001;DATABASE=CRPAF;Trusted_Connection=yes;"
        self.base_path = Path(r"\\crpfiles\Dept_Files\Automotive R and D\Supplier catalogs and files")
        self.current_file = None  # Initialize current_file attribute

        self.setup_ui()
        self.setup_sort_bindings()  # Add sorting capability
        self.load_excluded_files()

    def setup_ui(self):
        select_frame = ttk.Frame(self.root, padding="10")
        select_frame.pack(fill=tk.X)

        ttk.Button(select_frame, text="Select File", command=self.select_file).pack(side=tk.LEFT, padx=5)
        self.open_excel_button = ttk.Button(select_frame, text="Open in Excel", command=self.open_in_excel,
                                            state=tk.DISABLED)
        self.open_excel_button.pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Check Clipboard OEANs", command=self.check_clipboard_oeans).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Quit", command=self.root.quit).pack(side=tk.RIGHT)

        # Sheet selection
        sheet_frame = ttk.Frame(self.root, padding="10")
        sheet_frame.pack(fill=tk.X)
        ttk.Label(sheet_frame, text="Sheet Name:").pack(side=tk.LEFT)
        self.sheet_combo = ttk.Combobox(sheet_frame, width=30)
        self.sheet_combo.pack(side=tk.LEFT, padx=5)

        # File path display
        path_frame = ttk.Frame(self.root, padding="10")
        path_frame.pack(fill=tk.X)
        ttk.Label(path_frame, text="File Path:").pack(side=tk.LEFT)
        self.path_var = tk.StringVar()
        ttk.Label(path_frame, textvariable=self.path_var, wraplength=900).pack(side=tk.LEFT, padx=5)

        # Reason entry
        reason_frame = ttk.Frame(self.root, padding="10")
        reason_frame.pack(fill=tk.X)
        ttk.Label(reason_frame, text="Exclusion Reason:").pack(side=tk.LEFT)
        self.reason_entry = ttk.Entry(reason_frame, width=50)
        self.reason_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # Set default reason with today's date
        today_date = datetime.now().strftime("%Y-%m-%d")
        default_reason = f"No OEANs found ({today_date})"
        self.reason_entry.insert(0, default_reason)

        ttk.Button(reason_frame, text="Add Exclusion", command=self.add_exclusion).pack(side=tk.LEFT)

        # Excluded files list
        list_frame = ttk.Frame(self.root, padding="10")
        list_frame.pack(fill=tk.BOTH, expand=True)

        columns = ('FilePath', 'SheetName', 'ExclusionReason', 'ExcludedDate', 'ExcludedBy')
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings', selectmode='browse')

        self.tree.column('FilePath', width=400, minwidth=200)
        self.tree.column('SheetName', width=100, minwidth=80)
        self.tree.column('ExclusionReason', width=200, minwidth=100)
        self.tree.column('ExcludedDate', width=150, minwidth=100)
        self.tree.column('ExcludedBy', width=100, minwidth=80)

        self.tree.heading('FilePath', text='File Path', anchor=tk.W)
        self.tree.heading('SheetName', text='Sheet', anchor=tk.W)
        self.tree.heading('ExclusionReason', text='Exclusion Reason', anchor=tk.W)
        self.tree.heading('ExcludedDate', text='Excluded Date', anchor=tk.W)
        self.tree.heading('ExcludedBy', text='Excluded By', anchor=tk.W)

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        h_scrollbar = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(xscrollcommand=h_scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        ttk.Button(self.root, text="Delete Selected", command=self.delete_exclusion, padding="10").pack(pady=10)

    def setup_sort_bindings(self):
        # Track sorting state (column, direction)
        self.sort_column = None
        self.sort_direction = {}

        # Bind click events on column headers
        for col in ('FilePath', 'SheetName', 'ExclusionReason', 'ExcludedDate', 'ExcludedBy'):
            self.tree.heading(col, command=lambda c=col: self.sort_by_column(c))
            self.sort_direction[col] = 'asc'  # Default sort direction

    def sort_by_column(self, column):
        # Get all items with their values
        items = [(self.tree.item(item, 'values'), item) for item in self.tree.get_children('')]

        # Determine the sort key and reverse flag based on column type
        if column == 'ExcludedDate':
            # Parse dates for proper chronological sorting
            items.sort(key=lambda x: datetime.strptime(x[0][3], '%Y-%m-%d %H:%M:%S.%f')
            if x[0][3] else datetime.min,
                       reverse=(self.sort_direction[column] == 'desc'))
        else:
            # For text columns, use case-insensitive string comparison
            col_idx = {'FilePath': 0, 'SheetName': 1, 'ExclusionReason': 2, 'ExcludedBy': 4}[column]
            items.sort(key=lambda x: x[0][col_idx].lower() if x[0][col_idx] else '',
                       reverse=(self.sort_direction[column] == 'desc'))

        # Toggle sort direction for next click
        self.sort_direction[column] = 'asc' if self.sort_direction[column] == 'desc' else 'desc'

        # Remove all items
        for item in self.tree.get_children(''):
            self.tree.detach(item)

        # Reinsert in sorted order
        for values, item in items:
            self.tree.move(item, '', 'end')

        # Update headers to show sort direction
        for col in self.sort_direction:
            if col == column:
                direction = ' ↑' if self.sort_direction[col] == 'asc' else ' ↓'
                self.tree.heading(col, text=f"{col}{direction}")
            else:
                self.tree.heading(col, text=col)

    def select_file(self):
        file_path = filedialog.askopenfilename(
            initialdir=self.base_path,
            title="Select Supplier File",
            filetypes=(("Excel files", "*.xlsx *.xls"), ("All files", "*.*"))
        )
        if file_path:
            self.current_file = file_path
            self.path_var.set(file_path)  # Display the selected file path
            self.load_sheets(file_path)
            self.open_excel_button.config(state=tk.NORMAL)  # Enable the Open in Excel button

            # Reset reason with today's date when a new file is selected
            today_date = datetime.now().strftime("%Y-%m-%d")
            default_reason = f"No OEANs found ({today_date})"
            self.reason_entry.delete(0, tk.END)
            self.reason_entry.insert(0, default_reason)

    def open_in_excel(self):
        """Open the currently selected file in Excel"""
        if not self.current_file:
            messagebox.showwarning("Warning", "Please select a file first")
            return

        try:
            # Normalize path to ensure proper backslash format for Windows
            normalized_path = str(self.current_file).replace('/', '\\')

            # Use the os.startfile function on Windows to open with the default application
            if os.name == 'nt':  # Windows
                os.startfile(normalized_path)
            else:  # macOS or Linux
                subprocess.call(['open', self.current_file])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open file in Excel: {str(e)}")

    def load_sheets(self, file_path):
        try:
            xl = pd.ExcelFile(file_path)
            self.sheet_combo['values'] = xl.sheet_names
            if xl.sheet_names:
                self.sheet_combo.set(xl.sheet_names[0])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load sheets: {str(e)}")

    def load_excluded_files(self):
        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.execute("""
                    SELECT RTRIM(LTRIM(FilePath)) as FilePath,
                           RTRIM(LTRIM(SheetName)) as SheetName,
                           RTRIM(LTRIM(ExclusionReason)) as ExclusionReason,
                           ExcludedDate,
                           RTRIM(LTRIM(ExcludedBy)) as ExcludedBy 
                    FROM SupplierExcludedFiles
                    ORDER BY FilePath, SheetName
                """)

                for item in self.tree.get_children():
                    self.tree.delete(item)

                for row in cursor:
                    cleaned_values = (
                        str(row.FilePath).strip(),
                        str(row.SheetName).strip(),
                        str(row.ExclusionReason).strip(),
                        row.ExcludedDate,
                        str(row.ExcludedBy).strip()
                    )
                    self.tree.insert('', 'end', values=cleaned_values)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load excluded files: {str(e)}")

    def add_exclusion(self):
        if not self.current_file:
            messagebox.showwarning("Warning", "Please select a file first")
            return

        sheet_name = self.sheet_combo.get()
        if not sheet_name:
            messagebox.showwarning("Warning", "Please select a sheet")
            return

        reason = self.reason_entry.get().strip()
        if not reason:
            messagebox.showwarning("Warning", "Please enter an exclusion reason")
            return

        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()
                cleaned_path = str(self.current_file).strip().replace('/', '\\')
                current_date = datetime.now()
                cursor.execute("""
                    INSERT INTO SupplierExcludedFiles 
                    (FilePath, SheetName, ExclusionReason, ExcludedBy, ExcludedDate)
                    VALUES (?, ?, ?, ?, ?)
                """, cleaned_path, sheet_name, reason, 'pyearick', current_date)
                conn.commit()

            messagebox.showinfo("Success", "File/Sheet added to exclusion list")
            # Reset fields for next entry but keep the file selected
            sheet_index = self.sheet_combo.current() + 1
            if sheet_index < len(self.sheet_combo['values']):
                self.sheet_combo.current(sheet_index)  # Move to next sheet if available

            # Reset reason with today's date
            today_date = datetime.now().strftime("%Y-%m-%d")
            default_reason = f"No OEANs found ({today_date})"
            self.reason_entry.delete(0, tk.END)
            self.reason_entry.insert(0, default_reason)

            self.load_excluded_files()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to add exclusion: {str(e)}")

    def delete_exclusion(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("Warning", "Please select an item to delete")
            return

        values = self.tree.item(selected_item)['values']
        file_path = values[0]
        sheet_name = values[1]

        if messagebox.askyesno("Confirm Delete", f"Remove this exclusion?\nFile: {file_path}\nSheet: {sheet_name}"):
            try:
                with pyodbc.connect(self.db_conn) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        DELETE FROM SupplierExcludedFiles 
                        WHERE FilePath = ? AND SheetName = ?
                    """, file_path, sheet_name)
                    conn.commit()

                self.load_excluded_files()
                messagebox.showinfo("Success", "Exclusion removed")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete exclusion: {str(e)}")

    # ---- OEAN normalization (mirrors csvCleaner.py / pipeline logic) ----

    @staticmethod
    def normalize_oean(oean):
        """Normalize an OEAN for Motors lookup — strip, remove hyphens and spaces."""
        return oean.strip().replace('-', '').replace(' ', '')

    @staticmethod
    def split_oeans(text):
        """Split raw text into individual OEANs — same delimiters as csvCleaner.py."""
        return re.split(r'[;,\\\s]+', text)

    def check_clipboard_oeans(self):
        """Read OEANs from clipboard, normalize, and check against Motor_OE_PartTracking."""
        # --- Get clipboard text ---
        try:
            raw_text = self.root.clipboard_get()
        except tk.TclError:
            messagebox.showwarning("Clipboard Empty", "Nothing on the clipboard to check.")
            return

        if not raw_text or not raw_text.strip():
            messagebox.showwarning("Clipboard Empty", "Clipboard text is empty.")
            return

        # --- Split and normalize ---
        raw_tokens = self.split_oeans(raw_text)
        # Dedupe while preserving original form for display
        seen = {}  # normalized -> raw
        for token in raw_tokens:
            token = token.strip()
            if not token or token.lower() == 'nan':
                continue
            normalized = self.normalize_oean(token)
            if normalized and normalized not in seen:
                seen[normalized] = token

        if not seen:
            messagebox.showinfo("No OEANs", "No valid OEANs found after parsing clipboard text.")
            return

        # --- Query Motor_OE_PartTracking ---
        try:
            conn = pyodbc.connect(self.db_conn)
            cursor = conn.cursor()

            # Build parameterized IN clause (batch if needed for large lists)
            normalized_list = list(seen.keys())
            matched = {}  # normalized -> (PartNumber, CleanPartNumber, Make, CurrentDescription)
            batch_size = 500

            for i in range(0, len(normalized_list), batch_size):
                batch = normalized_list[i:i + batch_size]
                placeholders = ','.join(['?'] * len(batch))
                query = f"""
                    SELECT PartNumber, CleanPartNumber, Make, CurrentDescription, IsActive
                    FROM [CRPAF].[dbo].[Motor_OE_PartTracking]
                    WHERE CleanPartNumber IN ({placeholders})
                """
                cursor.execute(query, batch)
                for row in cursor.fetchall():
                    matched[row.CleanPartNumber] = {
                        'PartNumber': row.PartNumber,
                        'CleanPartNumber': row.CleanPartNumber,
                        'Make': row.Make or '',
                        'Description': row.CurrentDescription or '',
                        'IsActive': row.IsActive
                    }

            conn.close()
        except Exception as e:
            messagebox.showerror("Database Error", f"Failed to query Motor_OE_PartTracking:\n{e}")
            return

        # --- Classify results ---
        matched_pairs = []   # (raw, normalized, db_info)
        unmatched_pairs = [] # (raw, normalized)

        for normalized, raw in seen.items():
            if normalized in matched:
                matched_pairs.append((raw, normalized, matched[normalized]))
            else:
                unmatched_pairs.append((raw, normalized))

        # --- Show results in popup ---
        self._show_oean_results(seen, matched_pairs, unmatched_pairs)

    def _show_oean_results(self, all_oeans, matched_pairs, unmatched_pairs):
        """Display OEAN check results in a popup window."""
        win = tk.Toplevel(self.root)
        win.title("OEAN Check Results — Motor_OE_PartTracking")
        win.geometry("950x550")

        total = len(all_oeans)
        hit_count = len(matched_pairs)
        miss_count = len(unmatched_pairs)

        # --- Summary bar ---
        summary_frame = ttk.Frame(win, padding="10")
        summary_frame.pack(fill=tk.X)

        verdict_color = "dark green" if hit_count > 0 else "red"
        verdict_text = (f"Checked {total} unique OEANs  —  "
                        f"{hit_count} matched Motors OE, {miss_count} did not")

        verdict_label = tk.Label(summary_frame, text=verdict_text,
                                 font=("Segoe UI", 11, "bold"), fg=verdict_color)
        verdict_label.pack(anchor=tk.W)

        if hit_count == 0:
            safe_label = tk.Label(summary_frame,
                                  text="✓  Zero Motors matches — safe to exclude (no OEs would flow to SQL)",
                                  font=("Segoe UI", 10), fg="gray")
            safe_label.pack(anchor=tk.W, pady=(2, 0))
        else:
            warn_label = tk.Label(summary_frame,
                                  text="⚠  Some OEANs match Motors — excluding this file will drop them from SQL",
                                  font=("Segoe UI", 10), fg="dark orange")
            warn_label.pack(anchor=tk.W, pady=(2, 0))

        # --- Notebook with tabs ---
        nb = ttk.Notebook(win)
        nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))

        # -- Matched tab --
        match_frame = ttk.Frame(nb)
        nb.add(match_frame, text=f"  Matched ({hit_count})  ")

        match_cols = ('Raw', 'Normalized', 'PartNumber', 'Make', 'Description', 'Active')
        match_tree = ttk.Treeview(match_frame, columns=match_cols, show='headings', height=15)
        match_tree.column('Raw', width=120)
        match_tree.column('Normalized', width=120)
        match_tree.column('PartNumber', width=120)
        match_tree.column('Make', width=100)
        match_tree.column('Description', width=300)
        match_tree.column('Active', width=55)
        for col in match_cols:
            match_tree.heading(col, text=col, anchor=tk.W)

        match_scroll = ttk.Scrollbar(match_frame, orient=tk.VERTICAL, command=match_tree.yview)
        match_tree.configure(yscrollcommand=match_scroll.set)
        match_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        match_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        for raw, normalized, info in matched_pairs:
            match_tree.insert('', 'end', values=(
                raw, normalized, info['PartNumber'], info['Make'],
                info['Description'], 'Yes' if info['IsActive'] else 'No'
            ))

        # -- Unmatched tab --
        nomatch_frame = ttk.Frame(nb)
        nb.add(nomatch_frame, text=f"  Not in Motors ({miss_count})  ")

        nomatch_cols = ('Raw', 'Normalized')
        nomatch_tree = ttk.Treeview(nomatch_frame, columns=nomatch_cols, show='headings', height=15)
        nomatch_tree.column('Raw', width=250)
        nomatch_tree.column('Normalized', width=250)
        for col in nomatch_cols:
            nomatch_tree.heading(col, text=col, anchor=tk.W)

        nomatch_scroll = ttk.Scrollbar(nomatch_frame, orient=tk.VERTICAL, command=nomatch_tree.yview)
        nomatch_tree.configure(yscrollcommand=nomatch_scroll.set)
        nomatch_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        nomatch_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        for raw, normalized in unmatched_pairs:
            nomatch_tree.insert('', 'end', values=(raw, normalized))

        # -- Close button --
        ttk.Button(win, text="Close", command=win.destroy).pack(pady=5)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = SupplierExclusionGUI()
    app.run()