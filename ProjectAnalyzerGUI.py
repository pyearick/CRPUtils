r"""
ProjectAnalyzerGUI.py - CRP Project Intelligence GUI
======================================================

Tkinter front-end for ProjectAnalyzer.py.

Features:
  - Project folder checklist (auto-discovered from PycharmProjects)
  - SSMS folder toggle
  - Task Scheduler job selector with checkboxes
  - Scan button with progress bar
  - Results viewer with per-project prompt copy buttons
  - Opens output folder when complete

Lives in: CRPUtils folder

Author: Pat Yearick
Created: April 2026
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
from pathlib import Path
from datetime import datetime

# Import the engine
from ProjectAnalyzer import (
    discover_projects, scan_scheduled_tasks, load_config, save_config,
    run_full_analysis, ScheduledJob,
    DEFAULT_PYCHARM_ROOT, DEFAULT_SSMS_ROOT, DEFAULT_OUTPUT_ROOT,
)

# =============================================================================
# STYLE CONSTANTS — consistent with PunchlistGUI
# =============================================================================

COLORS = {
    'bg': '#F5F5F5',
    'header_bg': '#1B3A5C',
    'header_fg': '#FFFFFF',
    'card_bg': '#FFFFFF',
    'border': '#D0D8E0',
    'btn_primary': '#1B3A5C',
    'btn_primary_fg': '#FFFFFF',
    'btn_success': '#2E7D32',
    'btn_success_fg': '#FFFFFF',
    'btn_warning': '#E65100',
    'accent': '#E8913A',
    'text': '#1A1A1A',
    'text_secondary': '#6B7280',
    'stripe': '#F9FAFB',
    'selected': '#E3F2FD',
}

FONT_HEADER = ('Segoe UI', 14, 'bold')
FONT_SUBHEADER = ('Segoe UI', 11, 'bold')
FONT_NORMAL = ('Segoe UI', 9)
FONT_SMALL = ('Segoe UI', 8)
FONT_MONO = ('Consolas', 9)


# =============================================================================
# MAIN GUI CLASS
# =============================================================================

class ProjectAnalyzerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("CRP Project Analyzer")
        self.root.geometry("1100x750")
        self.root.configure(bg=COLORS['bg'])
        self.root.minsize(900, 600)

        # State
        self.config = load_config()
        self.project_vars = {}       # {name: BooleanVar}
        self.job_vars = {}           # {task_name: BooleanVar}
        self.all_jobs = []           # List[ScheduledJob]
        self.output_dir = None
        self.scanning = False

        self._build_ui()
        self._load_projects()

    # -----------------------------------------------------------------
    # UI CONSTRUCTION
    # -----------------------------------------------------------------
    def _build_ui(self):
        """Build the main window layout."""
        # Header bar
        header = tk.Frame(self.root, bg=COLORS['header_bg'], height=50)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text="CRP Project Analyzer",
                 font=FONT_HEADER, bg=COLORS['header_bg'],
                 fg=COLORS['header_fg']).pack(side=tk.LEFT, padx=15, pady=10)

        # Main content area — left panel + right panel
        content = tk.Frame(self.root, bg=COLORS['bg'])
        content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # --- Left Panel: Project & Job Selection ---
        left = tk.Frame(content, bg=COLORS['bg'], width=400)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        left.pack_propagate(False)

        # Project selection section
        proj_frame = tk.LabelFrame(left, text="  Projects  ", font=FONT_SUBHEADER,
                                   bg=COLORS['card_bg'], fg=COLORS['text'],
                                   padx=8, pady=5)
        proj_frame.pack(fill=tk.BOTH, expand=True, padx=(0, 5), pady=(0, 5))

        # Select all / none buttons
        btn_row = tk.Frame(proj_frame, bg=COLORS['card_bg'])
        btn_row.pack(fill=tk.X, pady=(0, 5))
        tk.Button(btn_row, text="Select All", command=self._select_all_projects,
                  font=FONT_SMALL, relief='flat', bg=COLORS['btn_primary'],
                  fg=COLORS['btn_primary_fg'], padx=8, pady=2
                  ).pack(side=tk.LEFT, padx=(0, 5))
        tk.Button(btn_row, text="Select None", command=self._select_no_projects,
                  font=FONT_SMALL, relief='flat', bg=COLORS['border'],
                  fg=COLORS['text'], padx=8, pady=2
                  ).pack(side=tk.LEFT)

        # Scrollable project checklist
        self.proj_canvas = tk.Canvas(proj_frame, bg=COLORS['card_bg'],
                                highlightthickness=0)
        proj_scrollbar = ttk.Scrollbar(proj_frame, orient=tk.VERTICAL,
                                       command=self.proj_canvas.yview)
        self.proj_list_frame = tk.Frame(self.proj_canvas, bg=COLORS['card_bg'])
        self.proj_list_frame.bind(
            "<Configure>",
            lambda e: self.proj_canvas.configure(scrollregion=self.proj_canvas.bbox("all"))
        )
        self.proj_canvas.create_window((0, 0), window=self.proj_list_frame, anchor="nw")
        self.proj_canvas.configure(yscrollcommand=proj_scrollbar.set)
        self.proj_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        proj_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Context-aware mousewheel — scrolls whichever list the mouse is over
        self._active_canvas = None

        def _on_mousewheel(event):
            if self._active_canvas:
                self._active_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _bind_canvas(canvas):
            canvas.bind("<Enter>", lambda e: setattr(self, '_active_canvas', canvas))
            canvas.bind("<Leave>", lambda e: setattr(self, '_active_canvas', None))

        self.root.bind_all("<MouseWheel>", _on_mousewheel)
        _bind_canvas(self.proj_canvas)

        # SSMS toggle
        ssms_frame = tk.LabelFrame(left, text="  SSMS Scripts  ", font=FONT_SUBHEADER,
                                   bg=COLORS['card_bg'], fg=COLORS['text'],
                                   padx=8, pady=5)
        ssms_frame.pack(fill=tk.X, padx=(0, 5), pady=(0, 5))

        self.ssms_var = tk.BooleanVar(value=self.config.get('include_ssms', True))
        tk.Checkbutton(ssms_frame, text="Include SSMS SQL scripts (historical)",
                       variable=self.ssms_var, bg=COLORS['card_bg'],
                       font=FONT_NORMAL).pack(anchor='w')
        self.ssms_path_label = tk.Label(ssms_frame, text=self.config.get('ssms_root', DEFAULT_SSMS_ROOT),
                                        font=FONT_SMALL, bg=COLORS['card_bg'],
                                        fg=COLORS['text_secondary'], wraplength=380)
        self.ssms_path_label.pack(anchor='w', padx=(20, 0))

        # Task Scheduler section
        job_frame = tk.LabelFrame(left, text="  Scheduled Tasks  ", font=FONT_SUBHEADER,
                                  bg=COLORS['card_bg'], fg=COLORS['text'],
                                  padx=8, pady=5)
        job_frame.pack(fill=tk.BOTH, expand=True, padx=(0, 5))

        job_btn_row = tk.Frame(job_frame, bg=COLORS['card_bg'])
        job_btn_row.pack(fill=tk.X, pady=(0, 5))
        tk.Button(job_btn_row, text="Load Tasks", command=self._load_scheduled_tasks,
                  font=FONT_SMALL, relief='flat', bg=COLORS['accent'],
                  fg='white', padx=8, pady=2
                  ).pack(side=tk.LEFT, padx=(0, 5))
        self.job_count_label = tk.Label(job_btn_row, text="Not loaded yet",
                                        font=FONT_SMALL, bg=COLORS['card_bg'],
                                        fg=COLORS['text_secondary'])
        self.job_count_label.pack(side=tk.LEFT)

        # Scrollable job checklist
        self.job_canvas = tk.Canvas(job_frame, bg=COLORS['card_bg'],
                               highlightthickness=0)
        job_scrollbar = ttk.Scrollbar(job_frame, orient=tk.VERTICAL,
                                      command=self.job_canvas.yview)
        self.job_list_frame = tk.Frame(self.job_canvas, bg=COLORS['card_bg'])
        self.job_list_frame.bind(
            "<Configure>",
            lambda e: self.job_canvas.configure(scrollregion=self.job_canvas.bbox("all"))
        )
        self.job_canvas.create_window((0, 0), window=self.job_list_frame, anchor="nw")
        self.job_canvas.configure(yscrollcommand=job_scrollbar.set)
        self.job_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        job_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Enable mousewheel scrolling for this canvas too
        _bind_canvas(self.job_canvas)

        # --- Right Panel: Action + Results ---
        right = tk.Frame(content, bg=COLORS['bg'])
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # Action bar
        action_frame = tk.Frame(right, bg=COLORS['card_bg'], relief='solid',
                                borderwidth=1, padx=10, pady=10)
        action_frame.pack(fill=tk.X, pady=(0, 10))

        self.scan_btn = tk.Button(
            action_frame, text="Run Analysis",
            command=self._start_scan,
            font=('Segoe UI', 12, 'bold'), relief='flat',
            bg=COLORS['btn_success'], fg=COLORS['btn_success_fg'],
            padx=20, pady=8, cursor='hand2'
        )
        self.scan_btn.pack(side=tk.LEFT)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(action_frame, variable=self.progress_var,
                                             maximum=100, length=300)
        self.progress_bar.pack(side=tk.LEFT, padx=15, fill=tk.X, expand=True)

        self.status_label = tk.Label(action_frame, text="Ready",
                                     font=FONT_NORMAL, bg=COLORS['card_bg'],
                                     fg=COLORS['text_secondary'])
        self.status_label.pack(side=tk.RIGHT)

        # Results area
        results_frame = tk.LabelFrame(right, text="  Results  ", font=FONT_SUBHEADER,
                                      bg=COLORS['card_bg'], fg=COLORS['text'],
                                      padx=8, pady=5)
        results_frame.pack(fill=tk.BOTH, expand=True)

        # Prompt list (treeview of generated prompts)
        self.results_tree = ttk.Treeview(results_frame,
                                          columns=('project', 'scripts', 'lines', 'status'),
                                          show='headings', height=10)
        self.results_tree.heading('project', text='Project')
        self.results_tree.heading('scripts', text='Scripts')
        self.results_tree.heading('lines', text='Lines')
        self.results_tree.heading('status', text='Prompt')
        self.results_tree.column('project', width=200)
        self.results_tree.column('scripts', width=80, anchor='center')
        self.results_tree.column('lines', width=80, anchor='center')
        self.results_tree.column('status', width=100, anchor='center')
        self.results_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 5))

        # Buttons below results
        result_btn_row = tk.Frame(results_frame, bg=COLORS['card_bg'])
        result_btn_row.pack(fill=tk.X)

        tk.Button(result_btn_row, text="Copy Selected Prompt",
                  command=self._copy_selected_prompt,
                  font=FONT_NORMAL, relief='flat',
                  bg=COLORS['btn_primary'], fg=COLORS['btn_primary_fg'],
                  padx=10, pady=4
                  ).pack(side=tk.LEFT, padx=(0, 5))

        tk.Button(result_btn_row, text="Open Output Folder",
                  command=self._open_output_folder,
                  font=FONT_NORMAL, relief='flat',
                  bg=COLORS['btn_primary'], fg=COLORS['btn_primary_fg'],
                  padx=10, pady=4
                  ).pack(side=tk.LEFT, padx=(0, 5))

        tk.Button(result_btn_row, text="View Prompt",
                  command=self._view_selected_prompt,
                  font=FONT_NORMAL, relief='flat',
                  bg=COLORS['accent'], fg='white',
                  padx=10, pady=4
                  ).pack(side=tk.LEFT)

    # -----------------------------------------------------------------
    # PROJECT LOADING
    # -----------------------------------------------------------------
    def _load_projects(self):
        """Discover projects and populate the checklist."""
        pycharm_root = self.config.get('pycharm_root', DEFAULT_PYCHARM_ROOT)
        projects = discover_projects(pycharm_root)
        previously_selected = set(self.config.get('selected_projects', []))

        for widget in self.proj_list_frame.winfo_children():
            widget.destroy()
        self.project_vars.clear()

        for i, name in enumerate(projects):
            var = tk.BooleanVar(
                value=(name in previously_selected) if previously_selected else True
            )
            self.project_vars[name] = var

            bg = COLORS['card_bg'] if i % 2 == 0 else COLORS['stripe']
            cb = tk.Checkbutton(self.proj_list_frame, text=name,
                                variable=var, bg=bg, font=FONT_NORMAL,
                                anchor='w', padx=5, pady=1)
            cb.pack(fill=tk.X)

    def _select_all_projects(self):
        for var in self.project_vars.values():
            var.set(True)

    def _select_no_projects(self):
        for var in self.project_vars.values():
            var.set(False)

    # -----------------------------------------------------------------
    # TASK SCHEDULER
    # -----------------------------------------------------------------
    def _load_scheduled_tasks(self):
        """Load tasks from Windows Task Scheduler."""
        self.job_count_label.config(text="Loading...")
        self.root.update_idletasks()

        # Run in thread to keep GUI responsive
        def _load():
            jobs = scan_scheduled_tasks()
            self.root.after(0, lambda: self._populate_jobs(jobs))

        threading.Thread(target=_load, daemon=True).start()

    def _populate_jobs(self, jobs):
        """Populate the job checklist with results."""
        self.all_jobs = jobs
        previously_selected = set(self.config.get('selected_jobs', []))

        for widget in self.job_list_frame.winfo_children():
            widget.destroy()
        self.job_vars.clear()

        # Sort by task name for readability
        jobs_sorted = sorted(jobs, key=lambda j: j.task_name.lower())

        for i, job in enumerate(jobs_sorted):
            # Default to selected if CRP-filtered (first run) or previously selected
            default_on = (job.task_name in previously_selected) if previously_selected else True
            var = tk.BooleanVar(value=default_on)
            self.job_vars[job.task_name] = var

            bg = COLORS['card_bg'] if i % 2 == 0 else COLORS['stripe']

            # Build display: TaskName (command) [State]
            display = f"{job.task_name}"
            if job.action_command and job.action_command != 'N/A':
                cmd_short = os.path.basename(job.action_command)
                display += f"  ({cmd_short})"

            row = tk.Frame(self.job_list_frame, bg=bg)
            row.pack(fill=tk.X)

            cb = tk.Checkbutton(row, text=display,
                                variable=var, bg=bg, font=FONT_SMALL,
                                anchor='w', padx=5, pady=1)
            cb.pack(side=tk.LEFT, fill=tk.X, expand=True)

            # State badge on the right
            state_text = job.state
            state_fg = COLORS['btn_success'] if job.state == 'Ready' else COLORS['text_secondary']
            tk.Label(row, text=state_text, font=FONT_SMALL,
                     bg=bg, fg=state_fg).pack(side=tk.RIGHT, padx=5)

        self.job_count_label.config(text=f"{len(jobs)} CRP tasks found")

    # -----------------------------------------------------------------
    # SCAN EXECUTION
    # -----------------------------------------------------------------
    def _start_scan(self):
        """Kick off the full analysis in a background thread."""
        if self.scanning:
            return

        # Gather selections
        selected_projects = [name for name, var in self.project_vars.items() if var.get()]
        if not selected_projects:
            messagebox.showwarning("No Projects", "Please select at least one project.")
            return

        selected_job_names = {name for name, var in self.job_vars.items() if var.get()}
        selected_jobs = [j for j in self.all_jobs if j.task_name in selected_job_names]

        ssms_root = self.config.get('ssms_root', DEFAULT_SSMS_ROOT) if self.ssms_var.get() else None

        # Save selections to config
        self.config['selected_projects'] = selected_projects
        self.config['selected_jobs'] = list(selected_job_names)
        self.config['include_ssms'] = self.ssms_var.get()
        save_config(self.config)

        # Clear previous results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)

        self.scanning = True
        self.scan_btn.config(state=tk.DISABLED, text="Scanning...")

        def _run():
            try:
                output_dir = run_full_analysis(
                    project_folders=selected_projects,
                    pycharm_root=self.config.get('pycharm_root', DEFAULT_PYCHARM_ROOT),
                    ssms_root=ssms_root,
                    selected_jobs=selected_jobs,
                    output_root=self.config.get('output_root', DEFAULT_OUTPUT_ROOT),
                    progress_callback=self._on_progress,
                )
                self.root.after(0, lambda: self._on_scan_complete(output_dir))
            except Exception as e:
                self.root.after(0, lambda: self._on_scan_error(str(e)))

        threading.Thread(target=_run, daemon=True).start()

    def _on_progress(self, message, percent):
        """Callback from engine — update GUI from main thread."""
        self.root.after(0, lambda: self._update_progress(message, percent))

    def _update_progress(self, message, percent):
        self.status_label.config(text=message)
        self.progress_var.set(percent)
        self.root.update_idletasks()

    def _on_scan_complete(self, output_dir):
        """Handle successful scan completion."""
        self.scanning = False
        self.output_dir = output_dir
        self.scan_btn.config(state=tk.NORMAL, text="Run Analysis")
        self.status_label.config(text="Complete!")
        self.progress_var.set(100)

        # Populate results tree from the prompts directory
        prompts_dir = os.path.join(output_dir, "Prompts")
        if os.path.exists(prompts_dir):
            for filename in sorted(os.listdir(prompts_dir)):
                if filename.endswith("_Prompt.md"):
                    proj_name = filename.replace("_Prompt.md", "")
                    # Read the prompt to get stats from the first few lines
                    prompt_path = os.path.join(prompts_dir, filename)
                    scripts_count = "—"
                    lines_count = "—"
                    try:
                        with open(prompt_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        # Parse "Total: X code files, Y SQL files" from the prompt
                        import re
                        m = re.search(r'Total:\s+(\d+)\s+code files.*?(\d+)\s+total lines', content)
                        if m:
                            scripts_count = m.group(1)
                            lines_count = m.group(2)
                    except Exception:
                        pass

                    self.results_tree.insert('', tk.END, iid=proj_name,
                                             values=(proj_name, scripts_count,
                                                     lines_count, 'Ready'))

        messagebox.showinfo("Analysis Complete",
                            f"Output saved to:\n{output_dir}\n\n"
                            f"Per-project prompts are in the Prompts subfolder.\n"
                            f"Select a project and click 'Copy Prompt' to paste\n"
                            f"into that project's Claude chat.")

    def _on_scan_error(self, error_msg):
        """Handle scan failure."""
        self.scanning = False
        self.scan_btn.config(state=tk.NORMAL, text="Run Analysis")
        self.status_label.config(text="Error!")
        messagebox.showerror("Analysis Failed", f"Error during scan:\n\n{error_msg}")

    # -----------------------------------------------------------------
    # RESULT ACTIONS
    # -----------------------------------------------------------------
    def _get_selected_prompt_path(self):
        """Get the file path of the currently selected prompt."""
        selected = self.results_tree.selection()
        if not selected or not self.output_dir:
            return None
        proj_name = selected[0]
        return os.path.join(self.output_dir, "Prompts", f"{proj_name}_Prompt.md")

    def _copy_selected_prompt(self):
        """Copy the selected project's prompt to clipboard."""
        prompt_path = self._get_selected_prompt_path()
        if not prompt_path:
            messagebox.showwarning("No Selection", "Select a project from the results list.")
            return

        try:
            with open(prompt_path, 'r', encoding='utf-8') as f:
                content = f.read()
            self.root.clipboard_clear()
            self.root.clipboard_append(content)
            proj_name = os.path.basename(prompt_path).replace("_Prompt.md", "")
            self.status_label.config(text=f"Copied {proj_name} prompt!")
        except Exception as e:
            messagebox.showerror("Error", f"Could not read prompt:\n{e}")

    def _view_selected_prompt(self):
        """Open the selected prompt in a scrollable popup."""
        prompt_path = self._get_selected_prompt_path()
        if not prompt_path:
            messagebox.showwarning("No Selection", "Select a project from the results list.")
            return

        try:
            with open(prompt_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            messagebox.showerror("Error", f"Could not read prompt:\n{e}")
            return

        proj_name = os.path.basename(prompt_path).replace("_Prompt.md", "")

        popup = tk.Toplevel(self.root)
        popup.title(f"Prompt — {proj_name}")
        popup.geometry("900x700")
        popup.configure(bg=COLORS['bg'])
        popup.transient(self.root)

        # Header
        header = tk.Frame(popup, bg=COLORS['header_bg'], height=40)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text=f"{proj_name} — Analysis Prompt",
                 font=FONT_SUBHEADER, bg=COLORS['header_bg'],
                 fg=COLORS['header_fg']).pack(side=tk.LEFT, padx=10, pady=8)

        # Text area
        text = scrolledtext.ScrolledText(popup, wrap=tk.WORD, font=FONT_MONO,
                                          bg=COLORS['card_bg'], padx=10, pady=10)
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 5))
        text.insert('1.0', content)

        # Buttons
        btn_row = tk.Frame(popup, bg=COLORS['bg'])
        btn_row.pack(fill=tk.X, padx=10, pady=(0, 10))

        def copy_and_close():
            # Copy from text widget so edits are captured
            self.root.clipboard_clear()
            self.root.clipboard_append(text.get('1.0', tk.END).strip())
            popup.destroy()
            self.status_label.config(text=f"Copied {proj_name} prompt!")

        tk.Button(btn_row, text="Copy to Clipboard & Close",
                  command=copy_and_close,
                  font=FONT_NORMAL, relief='flat',
                  bg=COLORS['btn_success'], fg=COLORS['btn_success_fg'],
                  padx=12, pady=4).pack(side=tk.LEFT, padx=(0, 10))

        tk.Button(btn_row, text="Close",
                  command=popup.destroy,
                  font=FONT_NORMAL, relief='flat',
                  bg=COLORS['border'], fg=COLORS['text'],
                  padx=12, pady=4).pack(side=tk.LEFT)

    def _open_output_folder(self):
        """Open the output folder in Windows Explorer."""
        if self.output_dir and os.path.exists(self.output_dir):
            os.startfile(self.output_dir)
        else:
            messagebox.showinfo("No Output", "Run an analysis first.")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    root = tk.Tk()
    app = ProjectAnalyzerGUI(root)

    # Center the window
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")

    root.mainloop()


if __name__ == '__main__':
    main()
