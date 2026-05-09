"""
NssmManagerGUI.py - NSSM Service Manager GUI
===============================================

Tkinter-based GUI for managing NSSM-installed Windows services.

Features:
  - Auto-discovers NSSM-managed services by scanning ImagePath
  - Shows service status (Running, Stopped, Paused, etc.)
  - Start / Stop / Restart / Edit / Remove controls
  - Detail panel showing key NSSM configuration properties

Lives in: CRPUtils folder
Requires: NSSM 2.24 at the configured path below

Author: Pat Yearick
Created: April 2026
"""

import os
import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import logging
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

NSSM_PATH = (
    "C:/Users/pyearick.CRP/OneDrive - CRP Industries Inc/CRPAF"
    "/PycharmProjects/nlp-sql-in-a-box/nssm-2.24/win64/nssm.exe"
)

LOG_PATH = "C:/Logs/NssmManagerGUI.log"

# Properties to display in the detail panel
NSSM_PROPERTIES = [
    "Application",
    "AppDirectory",
    "AppParameters",
    "Start",
    "ObjectName",
    "DisplayName",
    "Description",
    "AppStdout",
    "AppStderr",
    "AppRotateFiles",
    "AppRotateBytes",
    "AppEnvironmentExtra",
]

# Color scheme (consistent with other CRPUtils GUIs)
COLORS = {
    "bg": "#f5f5f5",
    "header_bg": "#2c3e50",
    "header_fg": "#ffffff",
    "btn_primary": "#3498db",
    "btn_success": "#27ae60",
    "btn_warning": "#f39c12",
    "btn_danger": "#e74c3c",
    "btn_fg": "#ffffff",
    "running": "#27ae60",
    "stopped": "#e74c3c",
    "paused": "#f39c12",
    "unknown": "#95a5a6",
    "tree_stripe": "#ecf0f1",
}

# =============================================================================
# LOGGING
# =============================================================================

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# NSSM HELPERS
# =============================================================================

def run_nssm(*args, timeout=15):
    """Run an NSSM command and return (returncode, stdout, stderr)."""
    cmd = [NSSM_PATH] + list(args)
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout,
        )
        # NSSM outputs UTF-16-LE; fall back to utf-8 if that fails
        for enc in ("utf-16-le", "utf-8"):
            try:
                out = result.stdout.decode(enc).strip().replace("\x00", "")
                err = result.stderr.decode(enc).strip().replace("\x00", "")
                break
            except UnicodeDecodeError:
                continue
        else:
            out = result.stdout.decode("utf-8", errors="replace").strip()
            err = result.stderr.decode("utf-8", errors="replace").strip()
        return result.returncode, out, err
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout running: {' '.join(cmd)}")
        return -1, "", "Command timed out"
    except Exception as e:
        logger.error(f"Error running NSSM: {e}")
        return -1, "", str(e)


def discover_nssm_services():
    """
    Find all Windows services whose ImagePath contains nssm.exe.
    Returns a list of dicts: {name, display_name, state, start_type}.
    """
    services = []
    try:
        # Use WMIC to find services with nssm in the path
        result = subprocess.run(
            [
                "wmic", "service", "where",
                "PathName like '%nssm%'",
                "get", "Name,DisplayName,State,StartMode,PathName",
                "/format:csv",
            ],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="replace",
        )
        lines = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
        if len(lines) < 2:
            return services

        # CSV header line
        headers = [h.strip().lower() for h in lines[0].split(",")]
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < len(headers):
                continue
            row = dict(zip(headers, cols))
            services.append({
                "name": row.get("name", "").strip(),
                "display_name": row.get("displayname", "").strip(),
                "state": row.get("state", "").strip(),
                "start_type": row.get("startmode", "").strip(),
            })
    except Exception as e:
        logger.error(f"Service discovery failed: {e}")
    return services


def get_service_property(service_name, prop):
    """Get a single NSSM property value."""
    rc, out, err = run_nssm("get", service_name, prop)
    if rc == 0:
        return out
    return ""


# =============================================================================
# GUI
# =============================================================================

class NssmManagerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("NSSM Service Manager")
        self.root.geometry("1050x650")
        self.root.minsize(850, 500)
        self.root.configure(bg=COLORS["bg"])

        self.services = []  # cached list from last refresh
        self._build_ui()
        self.refresh_services()

    # -----------------------------------------------------------------
    # UI CONSTRUCTION
    # -----------------------------------------------------------------

    def _build_ui(self):
        # Header
        hdr = tk.Frame(self.root, bg=COLORS["header_bg"], height=48)
        hdr.pack(fill=tk.X)
        hdr.pack_propagate(False)
        tk.Label(
            hdr, text="NSSM Service Manager", font=("Segoe UI", 14, "bold"),
            bg=COLORS["header_bg"], fg=COLORS["header_fg"],
        ).pack(side=tk.LEFT, padx=12)

        # Toolbar
        tb = tk.Frame(self.root, bg=COLORS["bg"], pady=6)
        tb.pack(fill=tk.X, padx=10)

        self._make_btn(tb, "Refresh", COLORS["btn_primary"], self.refresh_services).pack(side=tk.LEFT, padx=(0, 8))
        self._make_btn(tb, "Start", COLORS["btn_success"], self.start_service).pack(side=tk.LEFT, padx=(0, 4))
        self._make_btn(tb, "Stop", COLORS["btn_danger"], self.stop_service).pack(side=tk.LEFT, padx=(0, 4))
        self._make_btn(tb, "Restart", COLORS["btn_warning"], self.restart_service).pack(side=tk.LEFT, padx=(0, 4))
        self._make_btn(tb, "Edit (NSSM GUI)", COLORS["btn_primary"], self.edit_service).pack(side=tk.LEFT, padx=(0, 4))
        self._make_btn(tb, "Remove", COLORS["btn_danger"], self.remove_service).pack(side=tk.LEFT, padx=(0, 4))
        self._make_btn(tb, "Install New…", COLORS["btn_success"], self.install_service).pack(side=tk.RIGHT)

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(tb, textvariable=self.status_var, bg=COLORS["bg"],
                 font=("Segoe UI", 9, "italic"), anchor=tk.E).pack(side=tk.RIGHT, padx=10)

        # Paned: tree on left, detail on right
        pw = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        pw.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        # --- Service list (left) ---
        left = tk.Frame(pw, bg=COLORS["bg"])
        pw.add(left, weight=3)

        cols = ("name", "display_name", "state", "start_type")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", selectmode="browse")
        self.tree.heading("name", text="Service Name", anchor=tk.W)
        self.tree.heading("display_name", text="Display Name", anchor=tk.W)
        self.tree.heading("state", text="Status", anchor=tk.W)
        self.tree.heading("start_type", text="Startup", anchor=tk.W)
        self.tree.column("name", width=160, minwidth=100)
        self.tree.column("display_name", width=200, minwidth=120)
        self.tree.column("state", width=90, minwidth=70)
        self.tree.column("start_type", width=80, minwidth=60)

        vsb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # Tag colours for status
        self.tree.tag_configure("running", foreground=COLORS["running"])
        self.tree.tag_configure("stopped", foreground=COLORS["stopped"])
        self.tree.tag_configure("paused", foreground=COLORS["paused"])
        self.tree.tag_configure("unknown", foreground=COLORS["unknown"])

        # --- Detail panel (right) ---
        right = tk.Frame(pw, bg=COLORS["bg"])
        pw.add(right, weight=2)

        tk.Label(right, text="Service Properties", font=("Segoe UI", 11, "bold"),
                 bg=COLORS["bg"]).pack(anchor=tk.W, padx=4, pady=(4, 2))

        self.detail_text = scrolledtext.ScrolledText(
            right, wrap=tk.WORD, font=("Consolas", 9),
            state=tk.DISABLED, bg="#ffffff", relief=tk.FLAT,
        )
        self.detail_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=(0, 4))

    def _make_btn(self, parent, text, bg, command):
        return tk.Button(
            parent, text=text, bg=bg, fg=COLORS["btn_fg"],
            font=("Segoe UI", 9, "bold"), relief=tk.FLAT,
            activebackground=bg, cursor="hand2", padx=10, pady=3,
            command=command,
        )

    # -----------------------------------------------------------------
    # ACTIONS
    # -----------------------------------------------------------------

    def refresh_services(self):
        """Re-scan for NSSM services and repopulate the tree."""
        self.status_var.set("Scanning services…")
        self.root.update_idletasks()

        self.services = discover_nssm_services()

        # Clear tree
        for item in self.tree.get_children():
            self.tree.delete(item)

        for svc in self.services:
            tag = svc["state"].lower()
            if tag not in ("running", "stopped", "paused"):
                tag = "unknown"
            self.tree.insert(
                "", tk.END,
                iid=svc["name"],
                values=(svc["name"], svc["display_name"], svc["state"], svc["start_type"]),
                tags=(tag,),
            )

        count = len(self.services)
        self.status_var.set(f"{count} NSSM service{'s' if count != 1 else ''} found")
        self._clear_detail()
        logger.info(f"Refreshed — {count} services found")

    def _selected_service(self):
        """Return the name of the currently-selected service, or None."""
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("No Selection", "Please select a service first.")
            return None
        return sel[0]

    def start_service(self):
        name = self._selected_service()
        if not name:
            return
        self.status_var.set(f"Starting {name}…")
        self.root.update_idletasks()
        rc, out, err = run_nssm("start", name)
        if rc == 0:
            logger.info(f"Started {name}")
        else:
            messagebox.showerror("Start Failed", err or out or f"Return code {rc}")
            logger.error(f"Start {name} failed: {err}")
        self.refresh_services()
        self.tree.selection_set(name)

    def stop_service(self):
        name = self._selected_service()
        if not name:
            return
        self.status_var.set(f"Stopping {name}…")
        self.root.update_idletasks()
        rc, out, err = run_nssm("stop", name)
        if rc == 0:
            logger.info(f"Stopped {name}")
        else:
            messagebox.showerror("Stop Failed", err or out or f"Return code {rc}")
            logger.error(f"Stop {name} failed: {err}")
        self.refresh_services()
        self.tree.selection_set(name)

    def restart_service(self):
        name = self._selected_service()
        if not name:
            return
        self.status_var.set(f"Restarting {name}…")
        self.root.update_idletasks()
        rc, out, err = run_nssm("restart", name)
        if rc == 0:
            logger.info(f"Restarted {name}")
        else:
            messagebox.showerror("Restart Failed", err or out or f"Return code {rc}")
            logger.error(f"Restart {name} failed: {err}")
        self.refresh_services()
        self.tree.selection_set(name)

    def edit_service(self):
        """Open NSSM's own edit GUI for the selected service."""
        name = self._selected_service()
        if not name:
            return
        logger.info(f"Opening NSSM edit GUI for {name}")
        try:
            subprocess.Popen([NSSM_PATH, "edit", name])
        except Exception as e:
            messagebox.showerror("Error", f"Could not open NSSM editor:\n{e}")

    def remove_service(self):
        name = self._selected_service()
        if not name:
            return
        if not messagebox.askyesno(
            "Confirm Removal",
            f"Remove service '{name}'?\n\nThis will unregister it from Windows.",
        ):
            return
        self.status_var.set(f"Removing {name}…")
        self.root.update_idletasks()
        rc, out, err = run_nssm("remove", name, "confirm")
        if rc == 0:
            logger.info(f"Removed {name}")
        else:
            messagebox.showerror("Remove Failed", err or out or f"Return code {rc}")
            logger.error(f"Remove {name} failed: {err}")
        self.refresh_services()

    def install_service(self):
        """Open NSSM's install GUI for creating a new service."""
        logger.info("Opening NSSM install GUI")
        try:
            subprocess.Popen([NSSM_PATH, "install"])
        except Exception as e:
            messagebox.showerror("Error", f"Could not open NSSM installer:\n{e}")

    # -----------------------------------------------------------------
    # DETAIL PANEL
    # -----------------------------------------------------------------

    def _on_select(self, _event=None):
        """When a service is selected, load its NSSM properties."""
        name = self._selected_service()
        if not name:
            return
        self.status_var.set(f"Loading properties for {name}…")
        self.root.update_idletasks()

        lines = []
        for prop in NSSM_PROPERTIES:
            val = get_service_property(name, prop)
            lines.append(f"{prop:.<28s} {val}")

        self.detail_text.configure(state=tk.NORMAL)
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert(tk.END, "\n".join(lines))
        self.detail_text.configure(state=tk.DISABLED)

        self.status_var.set(f"Showing properties for {name}")

    def _clear_detail(self):
        self.detail_text.configure(state=tk.NORMAL)
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.configure(state=tk.DISABLED)

    # -----------------------------------------------------------------
    # RUN
    # -----------------------------------------------------------------

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    if not os.path.isfile(NSSM_PATH):
        print(f"ERROR: NSSM not found at:\n  {NSSM_PATH}")
        sys.exit(1)
    app = NssmManagerGUI()
    app.run()