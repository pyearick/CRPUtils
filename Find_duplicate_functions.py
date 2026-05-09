#!/usr/bin/env python3
"""
Python Function Analyzer
A tkinter application that analyzes Python files for function definitions and duplicates.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import re
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple


class FunctionAnalyzer:
    def __init__(self, root):
        self.root = root
        self.root.title("Python Function Analyzer")
        self.root.geometry("900x700")
        self.root.minsize(700, 500)
        
        self.file_path = None
        self.functions = {}  # function_name: [(line_number, full_definition), ...]
        
        self.create_widgets()
    
    def create_widgets(self):
        """Create the GUI layout"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # File selection frame
        file_frame = ttk.LabelFrame(main_frame, text="File Selection", padding="5")
        file_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)
        
        ttk.Button(file_frame, text="Browse File", command=self.browse_file).grid(row=0, column=0, padx=(0, 5))
        
        self.file_label = ttk.Label(file_frame, text="No file selected", foreground="gray")
        self.file_label.grid(row=0, column=1, sticky=(tk.W, tk.E))
        
        ttk.Button(file_frame, text="Analyze", command=self.analyze_file).grid(row=0, column=2, padx=(5, 0))
        
        # Statistics frame
        stats_frame = ttk.LabelFrame(main_frame, text="Analysis Summary", padding="5")
        stats_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.stats_label = ttk.Label(stats_frame, text="Select a file to analyze")
        self.stats_label.grid(row=0, column=0, sticky=tk.W)
        
        # Results frame
        results_frame = ttk.LabelFrame(main_frame, text="Analysis Results", padding="5")
        results_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(results_frame)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # All Functions tab
        self.all_functions_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.all_functions_frame, text="All Functions")
        self.create_all_functions_tab()
        
        # Duplicates tab
        self.duplicates_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.duplicates_frame, text="Duplicates")
        self.create_duplicates_tab()
        
        # Raw Output tab
        self.raw_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.raw_frame, text="Raw Output")
        self.create_raw_tab()
    
    def create_all_functions_tab(self):
        """Create the All Functions tab"""
        self.all_functions_frame.columnconfigure(0, weight=1)
        self.all_functions_frame.rowconfigure(0, weight=1)
        
        # Treeview for all functions
        columns = ("function", "line", "type")
        self.all_tree = ttk.Treeview(self.all_functions_frame, columns=columns, show="headings", height=15)
        
        self.all_tree.heading("function", text="Function Name")
        self.all_tree.heading("line", text="Line Number")
        self.all_tree.heading("type", text="Type")
        
        self.all_tree.column("function", width=300)
        self.all_tree.column("line", width=100)
        self.all_tree.column("type", width=100)
        
        # Scrollbars for all functions
        all_scrollbar_v = ttk.Scrollbar(self.all_functions_frame, orient=tk.VERTICAL, command=self.all_tree.yview)
        all_scrollbar_h = ttk.Scrollbar(self.all_functions_frame, orient=tk.HORIZONTAL, command=self.all_tree.xview)
        self.all_tree.configure(yscrollcommand=all_scrollbar_v.set, xscrollcommand=all_scrollbar_h.set)
        
        self.all_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        all_scrollbar_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        all_scrollbar_h.grid(row=1, column=0, sticky=(tk.W, tk.E))
    
    def create_duplicates_tab(self):
        """Create the Duplicates tab"""
        self.duplicates_frame.columnconfigure(0, weight=1)
        self.duplicates_frame.rowconfigure(0, weight=1)
        
        # Treeview for duplicates
        dup_columns = ("function", "occurrences", "lines")
        self.dup_tree = ttk.Treeview(self.duplicates_frame, columns=dup_columns, show="headings", height=15)
        
        self.dup_tree.heading("function", text="Function Name")
        self.dup_tree.heading("occurrences", text="Count")
        self.dup_tree.heading("lines", text="Line Numbers")
        
        self.dup_tree.column("function", width=300)
        self.dup_tree.column("occurrences", width=80)
        self.dup_tree.column("lines", width=200)
        
        # Scrollbars for duplicates
        dup_scrollbar_v = ttk.Scrollbar(self.duplicates_frame, orient=tk.VERTICAL, command=self.dup_tree.yview)
        dup_scrollbar_h = ttk.Scrollbar(self.duplicates_frame, orient=tk.HORIZONTAL, command=self.dup_tree.xview)
        self.dup_tree.configure(yscrollcommand=dup_scrollbar_v.set, xscrollcommand=dup_scrollbar_h.set)
        
        self.dup_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        dup_scrollbar_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        dup_scrollbar_h.grid(row=1, column=0, sticky=(tk.W, tk.E))
    
    def create_raw_tab(self):
        """Create the Raw Output tab"""
        self.raw_frame.columnconfigure(0, weight=1)
        self.raw_frame.rowconfigure(0, weight=1)
        
        self.raw_text = scrolledtext.ScrolledText(
            self.raw_frame,
            wrap=tk.WORD,
            font=('Consolas', 9),
            state=tk.DISABLED
        )
        self.raw_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
    
    def browse_file(self):
        """Browse for a Python file"""
        file_path = filedialog.askopenfilename(
            title="Select Python File",
            filetypes=[
                ("Python files", "*.py"),
                ("All files", "*.*")
            ]
        )
        
        if file_path:
            self.file_path = file_path
            self.file_label.config(text=os.path.basename(file_path), foreground="black")
            self.stats_label.config(text="File selected. Click 'Analyze' to process.")
    
    def analyze_file(self):
        """Analyze the selected file"""
        if not self.file_path:
            messagebox.showwarning("No File", "Please select a Python file first.")
            return
        
        if not os.path.exists(self.file_path):
            messagebox.showerror("File Error", "Selected file does not exist.")
            return
        
        try:
            self.stats_label.config(text="Analyzing...")
            self.root.update()
            
            # Parse the file
            self.functions = self.parse_python_file(self.file_path)
            
            # Update displays
            self.update_all_functions_display()
            self.update_duplicates_display()
            self.update_raw_display()
            self.update_stats_display()
            
        except Exception as e:
            messagebox.showerror("Analysis Error", f"Error analyzing file:\n{str(e)}")
            self.stats_label.config(text="Analysis failed")
    
    def parse_python_file(self, file_path: str) -> Dict[str, List[Tuple[int, str]]]:
        """Parse Python file and extract function definitions"""
        functions = defaultdict(list)
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
        
        # Patterns for different types of function definitions
        patterns = [
            (r'^\s*def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', 'function'),
            (r'^\s*async\s+def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', 'async function'),
            (r'^\s*class\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'class'),
        ]
        
        for line_num, line in enumerate(lines, 1):
            for pattern, func_type in patterns:
                match = re.match(pattern, line)
                if match:
                    func_name = match.group(1)
                    # Get the full line (trimmed)
                    full_def = line.strip()
                    functions[func_name].append((line_num, full_def, func_type))
        
        return functions
    
    def update_all_functions_display(self):
        """Update the All Functions tab"""
        # Clear existing items
        for item in self.all_tree.get_children():
            self.all_tree.delete(item)
        
        # Add all functions
        all_funcs = []
        for func_name, occurrences in self.functions.items():
            for line_num, full_def, func_type in occurrences:
                all_funcs.append((func_name, line_num, func_type))
        
        # Sort by line number
        all_funcs.sort(key=lambda x: x[1])
        
        for func_name, line_num, func_type in all_funcs:
            # Color duplicates differently
            tag = "duplicate" if len(self.functions[func_name]) > 1 else "single"
            item = self.all_tree.insert("", tk.END, values=(func_name, line_num, func_type), tags=(tag,))
        
        # Configure tags
        self.all_tree.tag_configure("duplicate", background="#ffeeee")
        self.all_tree.tag_configure("single", background="white")
    
    def update_duplicates_display(self):
        """Update the Duplicates tab"""
        # Clear existing items
        for item in self.dup_tree.get_children():
            self.dup_tree.delete(item)
        
        # Add only duplicates
        duplicates = {name: occurrences for name, occurrences in self.functions.items() 
                     if len(occurrences) > 1}
        
        for func_name, occurrences in duplicates.items():
            count = len(occurrences)
            line_numbers = [str(line_num) for line_num, _, _ in occurrences]
            lines_str = ", ".join(line_numbers)
            
            self.dup_tree.insert("", tk.END, values=(func_name, count, lines_str))
    
    def update_raw_display(self):
        """Update the Raw Output tab"""
        self.raw_text.config(state=tk.NORMAL)
        self.raw_text.delete(1.0, tk.END)
        
        output = f"Analysis of: {self.file_path}\n"
        output += "=" * 80 + "\n\n"
        
        # All functions section
        output += "ALL FUNCTIONS AND CLASSES:\n"
        output += "-" * 40 + "\n"
        
        all_funcs = []
        for func_name, occurrences in self.functions.items():
            for line_num, full_def, func_type in occurrences:
                all_funcs.append((line_num, func_name, func_type, full_def))
        
        all_funcs.sort()  # Sort by line number
        
        for line_num, func_name, func_type, full_def in all_funcs:
            output += f"Line {line_num:4d}: {func_type:15s} {func_name}\n"
            output += f"           {full_def}\n\n"
        
        # Duplicates section
        duplicates = {name: occurrences for name, occurrences in self.functions.items() 
                     if len(occurrences) > 1}
        
        if duplicates:
            output += "\n" + "=" * 80 + "\n"
            output += "DUPLICATE FUNCTIONS/CLASSES:\n"
            output += "-" * 40 + "\n"
            
            for func_name, occurrences in duplicates.items():
                output += f"\n'{func_name}' appears {len(occurrences)} times:\n"
                for line_num, full_def, func_type in occurrences:
                    output += f"  Line {line_num:4d}: {full_def}\n"
        else:
            output += "\n" + "=" * 80 + "\n"
            output += "No duplicate functions found! ✅\n"
        
        # Statistics
        output += "\n" + "=" * 80 + "\n"
        output += "STATISTICS:\n"
        output += "-" * 40 + "\n"
        output += f"Total functions/classes: {sum(len(occurrences) for occurrences in self.functions.values())}\n"
        output += f"Unique functions/classes: {len(self.functions)}\n"
        output += f"Duplicate functions/classes: {len(duplicates)}\n"
        
        if duplicates:
            output += f"Total duplicate occurrences: {sum(len(occurrences) for occurrences in duplicates.values())}\n"
        
        self.raw_text.insert(tk.END, output)
        self.raw_text.config(state=tk.DISABLED)
    
    def update_stats_display(self):
        """Update the statistics display"""
        total_functions = sum(len(occurrences) for occurrences in self.functions.values())
        unique_functions = len(self.functions)
        duplicates = {name: occurrences for name, occurrences in self.functions.items() 
                     if len(occurrences) > 1}
        
        stats_text = f"📊 Total: {total_functions} | Unique: {unique_functions} | Duplicates: {len(duplicates)}"
        
        if duplicates:
            stats_text += f" | ⚠️ {len(duplicates)} functions have duplicates"
        else:
            stats_text += " | ✅ No duplicates found"
        
        self.stats_label.config(text=stats_text)


def main():
    """Main entry point"""
    root = tk.Tk()
    app = FunctionAnalyzer(root)
    
    # Center the window
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")
    
    root.mainloop()


if __name__ == "__main__":
    main()