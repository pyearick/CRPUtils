import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pyodbc
from pathlib import Path
import pandas as pd
from datetime import datetime
import os
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
        self.load_excluded_files()

    def setup_ui(self):
        select_frame = ttk.Frame(self.root, padding="10")
        select_frame.pack(fill=tk.X)

        ttk.Button(select_frame, text="Select File", command=self.select_file).pack(side=tk.LEFT, padx=5)
        self.open_excel_button = ttk.Button(select_frame, text="Open in Excel", command=self.open_in_excel,
                                            state=tk.DISABLED)
        self.open_excel_button.pack(side=tk.LEFT, padx=5)
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

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = SupplierExclusionGUI()
    app.run()