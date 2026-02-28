import tkinter as tk
from tkinter import ttk, messagebox
import pyodbc
from datetime import datetime
import os
import sys


class TableExclusionGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Table Exclusion Manager for Data Freshness")
        self.root.geometry("900x600")

        self.db_conn = "DRIVER={SQL Server};SERVER=BI-SQL001;DATABASE=CRPAF;Trusted_Connection=yes;"
        self.setup_ui()
        self.load_tables()
        self.load_excluded_tables()

    def setup_ui(self):
        # Table selection frame
        select_frame = ttk.Frame(self.root, padding="10")
        select_frame.pack(fill=tk.X)

        ttk.Label(select_frame, text="Available Tables:").pack(side=tk.LEFT)
        self.table_combo = ttk.Combobox(self.root, width=50)
        self.table_combo.pack(pady=5)

        # Filter tables
        filter_frame = ttk.Frame(self.root, padding="10")
        filter_frame.pack(fill=tk.X)

        ttk.Label(filter_frame, text="Filter:").pack(side=tk.LEFT)
        self.filter_var = tk.StringVar()
        self.filter_var.trace("w", self.filter_tables)
        filter_entry = ttk.Entry(filter_frame, textvariable=self.filter_var, width=30)
        filter_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # Refresh button
        ttk.Button(filter_frame, text="Refresh Tables", command=self.load_tables).pack(side=tk.RIGHT)

        # Reason entry
        reason_frame = ttk.Frame(self.root, padding="10")
        reason_frame.pack(fill=tk.X)
        ttk.Label(reason_frame, text="Exclusion Reason:").pack(side=tk.LEFT)
        self.reason_entry = ttk.Entry(reason_frame, width=50)
        self.reason_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # Set default reason with today's date
        today_date = datetime.now().strftime("%Y-%m-%d")
        default_reason = f"Static reference table ({today_date})"
        self.reason_entry.insert(0, default_reason)

        ttk.Button(reason_frame, text="Add Exclusion", command=self.add_exclusion).pack(side=tk.LEFT)

        # Excluded tables list
        list_frame = ttk.Frame(self.root, padding="10")
        list_frame.pack(fill=tk.BOTH, expand=True)

        columns = ('TableName', 'ExclusionReason', 'DateAdded')
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings', selectmode='browse')

        self.tree.column('TableName', width=300, minwidth=200)
        self.tree.column('ExclusionReason', width=400, minwidth=200)
        self.tree.column('DateAdded', width=150, minwidth=100)

        self.tree.heading('TableName', text='Table Name', anchor=tk.W)
        self.tree.heading('ExclusionReason', text='Exclusion Reason', anchor=tk.W)
        self.tree.heading('DateAdded', text='Date Added', anchor=tk.W)

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        h_scrollbar = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(xscrollcommand=h_scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # Action buttons
        button_frame = ttk.Frame(self.root, padding="10")
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="Delete Selected", command=self.delete_exclusion).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Run Freshness Check", command=self.run_freshness_check).pack(side=tk.LEFT,
                                                                                                    padx=5)
        ttk.Button(button_frame, text="Quit", command=self.root.quit).pack(side=tk.RIGHT)

    def ensure_exclusion_table_exists(self):
        """Make sure the TableFreshnessExclusions table exists"""
        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()

                # Check if table exists
                table_exists_query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'TableFreshnessExclusions'
                """
                cursor.execute(table_exists_query)
                exists = cursor.fetchone()[0] > 0

                # Create the table if it doesn't exist
                if not exists:
                    create_table_query = """
                    CREATE TABLE TableFreshnessExclusions (
                        TableName nvarchar(128) PRIMARY KEY,
                        ExclusionReason nvarchar(500) NULL,
                        DateAdded datetime DEFAULT GETDATE()
                    )
                    """
                    cursor.execute(create_table_query)
                    conn.commit()
                    return True

                return True

        except Exception as e:
            messagebox.showerror("Error", f"Failed to create exclusion table: {str(e)}")
            return False

    def load_tables(self):
        """Load available tables from the database"""
        try:
            if not self.ensure_exclusion_table_exists():
                return

            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()

                # Get all tables
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """)

                self.all_tables = [row.TABLE_NAME for row in cursor.fetchall()]
                self.table_combo['values'] = self.all_tables

                if self.all_tables:
                    self.table_combo.current(0)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load tables: {str(e)}")

    def filter_tables(self, *args):
        """Filter tables based on user input"""
        filter_text = self.filter_var.get().lower()
        if filter_text:
            filtered_tables = [table for table in self.all_tables if filter_text in table.lower()]
            self.table_combo['values'] = filtered_tables
            if filtered_tables:
                self.table_combo.current(0)
        else:
            self.table_combo['values'] = self.all_tables

    def load_excluded_tables(self):
        """Load excluded tables from the database"""
        try:
            if not self.ensure_exclusion_table_exists():
                return

            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()

                # Clear existing items in tree
                for item in self.tree.get_children():
                    self.tree.delete(item)

                # Get excluded tables
                cursor.execute("""
                    SELECT TableName, ExclusionReason, DateAdded
                    FROM TableFreshnessExclusions
                    ORDER BY DateAdded DESC
                """)

                for row in cursor:
                    self.tree.insert('', 'end', values=(
                        row.TableName,
                        row.ExclusionReason,
                        row.DateAdded
                    ))

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load excluded tables: {str(e)}")

    def add_exclusion(self):
        """Add a table to the exclusion list"""
        table_name = self.table_combo.get()
        if not table_name:
            messagebox.showwarning("Warning", "Please select a table")
            return

        reason = self.reason_entry.get().strip()
        if not reason:
            messagebox.showwarning("Warning", "Please enter an exclusion reason")
            return

        try:
            with pyodbc.connect(self.db_conn) as conn:
                cursor = conn.cursor()

                # Check if already excluded
                cursor.execute("""
                    SELECT COUNT(*) FROM TableFreshnessExclusions
                    WHERE TableName = ?
                """, table_name)

                if cursor.fetchone()[0] > 0:
                    if not messagebox.askyesno("Warning",
                                               f"Table '{table_name}' is already excluded. Update the reason?"):
                        return

                    # Update existing exclusion
                    cursor.execute("""
                        UPDATE TableFreshnessExclusions
                        SET ExclusionReason = ?, DateAdded = GETDATE()
                        WHERE TableName = ?
                    """, reason, table_name)
                else:
                    # Add new exclusion
                    cursor.execute("""
                        INSERT INTO TableFreshnessExclusions (TableName, ExclusionReason)
                        VALUES (?, ?)
                    """, table_name, reason)

                conn.commit()

                messagebox.showinfo("Success", f"Table '{table_name}' added to exclusions")

                # Reset fields and refresh
                self.load_excluded_tables()

                # Reset reason with today's date
                today_date = datetime.now().strftime("%Y-%m-%d")
                default_reason = f"Static reference table ({today_date})"
                self.reason_entry.delete(0, tk.END)
                self.reason_entry.insert(0, default_reason)

                # Move to next table in combo if possible
                table_index = self.table_combo.current()
                if table_index < len(self.table_combo['values']) - 1:
                    self.table_combo.current(table_index + 1)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to add exclusion: {str(e)}")

    def delete_exclusion(self):
        """Remove a table from the exclusion list"""
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("Warning", "Please select an item to delete")
            return

        values = self.tree.item(selected_item)['values']
        table_name = values[0]

        if messagebox.askyesno("Confirm Delete", f"Remove table '{table_name}' from exclusions?"):
            try:
                with pyodbc.connect(self.db_conn) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        DELETE FROM TableFreshnessExclusions
                        WHERE TableName = ?
                    """, table_name)
                    conn.commit()

                self.load_excluded_tables()
                messagebox.showinfo("Success", f"Table '{table_name}' removed from exclusions")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete exclusion: {str(e)}")

    def run_freshness_check(self):
        """Run the data freshness check script"""
        try:
            # Get the directory of the current script
            current_dir = os.path.dirname(os.path.abspath(__file__))
            data_freshness_path = os.path.join(current_dir, "data_freshness.py")

            if not os.path.exists(data_freshness_path):
                messagebox.showwarning("Warning",
                                       "data_freshness.py not found in the same directory. Please run it manually.")
                return

            messagebox.showinfo("Running Check",
                                "Running data freshness check... Results will be displayed when finished.")

            # Run the data_freshness.py script
            import subprocess
            result = subprocess.run([sys.executable, data_freshness_path],
                                    capture_output=True, text=True)

            # Show results
            results_window = tk.Toplevel(self.root)
            results_window.title("Data Freshness Check Results")
            results_window.geometry("800x600")

            results_text = tk.Text(results_window, wrap="word")
            results_text.pack(expand=True, fill="both", padx=10, pady=10)

            # Add scrollbar
            scrollbar = ttk.Scrollbar(results_text, command=results_text.yview)
            scrollbar.pack(side="right", fill="y")
            results_text.config(yscrollcommand=scrollbar.set)

            # Insert results
            if result.stdout:
                results_text.insert("1.0", result.stdout)
            if result.stderr:
                results_text.insert("end", "\n\nErrors:\n" + result.stderr)

            # Add a close button
            ttk.Button(results_window, text="Close",
                       command=results_window.destroy).pack(pady=10)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to run freshness check: {str(e)}")

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = TableExclusionGUI()
    app.run()