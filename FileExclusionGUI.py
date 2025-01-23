import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pyodbc
from pathlib import Path


class SupplierExclusionGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Supplier File Exclusion Manager")
        self.root.geometry("800x600")

        self.db_conn = "DRIVER={SQL Server};SERVER=BI-SQL001;DATABASE=CRPAF;Trusted_Connection=yes;"
        self.base_path = Path(r"\\crpfiles\Dept_Files\Automotive R and D\Supplier catalogs and files")

        self.setup_ui()
        self.load_excluded_files()

    def setup_ui(self):
        # File selection
        select_frame = ttk.Frame(self.root, padding="10")
        select_frame.pack(fill=tk.X)

        ttk.Button(select_frame, text="Select File", command=self.select_file).pack(side=tk.LEFT)
        ttk.Button(select_frame, text="Quit", command=self.root.quit).pack(side=tk.RIGHT)

        # Rest of the setup_ui method remains the same
        reason_frame = ttk.Frame(self.root, padding="10")
        reason_frame.pack(fill=tk.X)

        ttk.Label(reason_frame, text="Exclusion Reason:").pack(side=tk.LEFT)
        self.reason_entry = ttk.Entry(reason_frame, width=50)
        self.reason_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        ttk.Button(reason_frame, text="Add Exclusion", command=self.add_exclusion).pack(side=tk.LEFT)

        # Excluded files list
        list_frame = ttk.Frame(self.root, padding="10")
        list_frame.pack(fill=tk.BOTH, expand=True)

        columns = ('FilePath', 'ExclusionReason', 'ExcludedDate', 'ExcludedBy')
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings', selectmode='browse')

        # Set column widths proportionally
        self.tree.column('FilePath', width=400, minwidth=200)
        self.tree.column('ExclusionReason', width=200, minwidth=100)
        self.tree.column('ExcludedDate', width=150, minwidth=100)
        self.tree.column('ExcludedBy', width=100, minwidth=80)

        # Set column headings
        self.tree.heading('FilePath', text='File Path', anchor=tk.W)
        self.tree.heading('ExclusionReason', text='Exclusion Reason', anchor=tk.W)
        self.tree.heading('ExcludedDate', text='Excluded Date', anchor=tk.W)
        self.tree.heading('ExcludedBy', text='Excluded By', anchor=tk.W)

        # Add horizontal scrollbar
        h_scrollbar = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(xscrollcommand=h_scrollbar.set)

        # Add vertical scrollbar
        v_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=v_scrollbar.set)

        # Pack scrollbars and tree
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # Delete button
        button_frame = ttk.Frame(self.root, padding="10")
        button_frame.pack(fill=tk.X)
        ttk.Button(button_frame, text="Delete Selected", command=self.delete_exclusion).pack(pady=10)

    def select_file(self):
        file_path = filedialog.askopenfilename(
            initialdir=self.base_path,
            title="Select Supplier File",
            filetypes=(("Excel files", "*.xlsx *.xls"), ("All files", "*.*"))
        )
        if file_path:
            self.current_file = file_path
            self.reason_entry.focus()

    def load_excluded_files(self):
        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.execute("""
                    SELECT 
                        RTRIM(LTRIM(FilePath)) as FilePath, 
                        RTRIM(LTRIM(ExclusionReason)) as ExclusionReason,
                        ExcludedDate,
                        RTRIM(LTRIM(ExcludedBy)) as ExcludedBy 
                    FROM SupplierExcludedFiles
                """)

                # Clear existing items
                for item in self.tree.get_children():
                    self.tree.delete(item)

                # Add new items with cleaned data
                for row in cursor:
                    # Create a cleaned tuple of values
                    cleaned_values = (
                        str(row.FilePath).strip(),
                        str(row.ExclusionReason).strip(),
                        row.ExcludedDate,
                        str(row.ExcludedBy).strip()
                    )
                    self.tree.insert('', 'end', values=cleaned_values)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load excluded files: {str(e)}")

    def add_exclusion(self):
        if not hasattr(self, 'current_file'):
            messagebox.showwarning("Warning", "Please select a file first")
            return

        reason = self.reason_entry.get().strip()
        if not reason:
            messagebox.showwarning("Warning", "Please enter an exclusion reason")
            return

        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()
                # Clean the file path before inserting
                cleaned_path = str(self.current_file).strip().replace('/', '\\')
                cursor.execute("""
                    INSERT INTO SupplierExcludedFiles (FilePath, ExclusionReason, ExcludedBy)
                    VALUES (?, ?, ?)
                """, cleaned_path, reason, 'pyearick')
                conn.commit()

            messagebox.showinfo("Success", "File added to exclusion list")
            self.reason_entry.delete(0, tk.END)
            delattr(self, 'current_file')
            self.load_excluded_files()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to add exclusion: {str(e)}")

    def delete_exclusion(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("Warning", "Please select a file to delete")
            return

        file_path = self.tree.item(selected_item)['values'][0]

        if messagebox.askyesno("Confirm Delete", f"Remove this file from exclusions?\n{file_path}"):
            try:
                with pyodbc.connect(self.db_conn) as conn:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM SupplierExcludedFiles WHERE FilePath = ?", file_path)
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