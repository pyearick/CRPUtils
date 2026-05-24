"""
CRPUtils/logging_utils.py - Standard Logging & Timing Utilities for BDH Scripts
================================================================================

Provides:
1. setup_logging()  - Consistent logging with line-buffered file output
2. ScriptTimer      - Context manager for timing script execution

Usage:
------
    from CRPUtils.logging_utils import setup_logging, ScriptTimer

    # Basic usage
    logger = setup_logging('BDH_06_ShowMeTheParts')
    logger.info("Starting process...")

    # With script timer
    with ScriptTimer('BDH_06_ShowMeTheParts') as timer:
        # ... your script code ...
        pass
    # Automatically logs duration at end

    # Or manual timer control
    timer = ScriptTimer('MyScript')
    timer.start()
    # ... work ...
    timer.stop()  # Logs duration
    print(f"Took {timer.duration}")
"""

import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Optional
from contextlib import contextmanager

# =============================================================================
# CONFIGURATION
# =============================================================================
LOG_DIR = r"C:\Logs"
DEFAULT_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(
    log_name: str,
    log_dir: str = LOG_DIR,
    level: int = logging.INFO,
    line_buffered: bool = True,
    include_console: bool = True,
    format_string: str = DEFAULT_FORMAT,
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Logger:
    """
    Set up standardized logging for BDH scripts.

    Creates a logger that writes to both file and console (optional).
    File output is line-buffered by default so logs appear immediately.
    Log files rotate at max_bytes with backup_count old files retained.

    Args:
        log_name: Name for the log file (without .log extension)
                  e.g., 'BDH_06_ShowMeTheParts' -> C:\\Logs\\BDH_06_ShowMeTheParts.log
        log_dir: Directory for log files (default: C:\\Logs)
        level: Logging level (default: logging.INFO)
        line_buffered: If True, flush after each line (default: True)
        include_console: If True, also log to console (default: True)
        format_string: Log message format
        max_bytes: Rotate log file at this size in bytes (default: 50 MB)
        backup_count: Number of rotated log files to keep (default: 3)

    Returns:
        Configured logger instance

    Example:
        logger = setup_logging('BDH_07_MarketValidation')
        logger.info("Starting validation...")
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Build log file path (no date suffix per Pat's preference)
    log_path = os.path.join(log_dir, f"{log_name}.log")

    # Create formatter
    formatter = logging.Formatter(format_string)

    # Create rotating file handler
    file_handler = RotatingFileHandler(
        log_path, mode='a', encoding='utf-8',
        maxBytes=max_bytes, backupCount=backup_count,
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Enable line buffering if requested (writes immediately to disk)
    if line_buffered:
        # Close default stream and reopen with line buffering
        file_handler.stream.close()
        file_handler.stream = open(log_path, 'a', encoding='utf-8', buffering=1)
    
    # Build handlers list
    handlers = [file_handler]
    
    if include_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)
    
    # Clear any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Configure logging
    logging.basicConfig(
        level=level,
        handlers=handlers,
    )
    
    logger = logging.getLogger(log_name)
    logger.info(f"Logging to: {log_path}")
    
    return logger


# =============================================================================
# SCRIPT TIMER
# =============================================================================

class ScriptTimer:
    """
    Timer for measuring and logging script execution duration.
    
    Can be used as a context manager or manually started/stopped.
    
    Usage as context manager:
        with ScriptTimer('BDH_06_Scoring') as timer:
            # ... your code ...
            pass
        # Automatically logs duration when exiting
    
    Usage as manual timer:
        timer = ScriptTimer('BDH_06_Scoring')
        timer.start()
        # ... your code ...
        timer.stop()
        print(f"Duration: {timer.duration}")
        print(f"Duration (seconds): {timer.duration_seconds}")
    """
    
    def __init__(self, script_name: str, logger: Optional[logging.Logger] = None):
        """
        Initialize timer.
        
        Args:
            script_name: Name of the script (for logging)
            logger: Optional logger instance. If None, uses module logger.
        """
        self.script_name = script_name
        self.logger = logger or logging.getLogger(script_name)
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self._running = False
    
    def start(self) -> 'ScriptTimer':
        """Start the timer."""
        self.start_time = datetime.now()
        self._running = True
        self.logger.info(f"{'=' * 70}")
        self.logger.info(f"🚀 {self.script_name} STARTED")
        self.logger.info(f"   Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"{'=' * 70}")
        return self
    
    def stop(self) -> 'ScriptTimer':
        """Stop the timer and log duration."""
        if not self._running:
            self.logger.warning("Timer was not running")
            return self
            
        self.end_time = datetime.now()
        self._running = False
        
        self.logger.info(f"{'=' * 70}")
        self.logger.info(f"🏁 {self.script_name} COMPLETED")
        self.logger.info(f"   End time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"   Duration: {self.duration_formatted}")
        self.logger.info(f"{'=' * 70}")
        return self
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get duration as timedelta."""
        if self.start_time is None:
            return None
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        d = self.duration
        return d.total_seconds() if d else 0.0
    
    @property
    def duration_formatted(self) -> str:
        """Get human-readable duration string."""
        d = self.duration
        if d is None:
            return "Not started"
        
        total_seconds = int(d.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def __enter__(self) -> 'ScriptTimer':
        """Context manager entry - starts timer."""
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - stops timer and logs."""
        if exc_type is not None:
            # An exception occurred
            self.end_time = datetime.now()
            self._running = False
            self.logger.error(f"{'=' * 70}")
            self.logger.error(f"❌ {self.script_name} FAILED")
            self.logger.error(f"   Error: {exc_type.__name__}: {exc_val}")
            self.logger.error(f"   Duration before failure: {self.duration_formatted}")
            self.logger.error(f"{'=' * 70}")
            return False  # Don't suppress the exception
        else:
            self.stop()
            return False


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def setup_logging_with_timer(
    script_name: str,
    **logging_kwargs
) -> tuple:
    """
    Set up logging and create a timer in one call.
    
    Args:
        script_name: Name for both logging and timer
        **logging_kwargs: Additional arguments for setup_logging()
        
    Returns:
        Tuple of (logger, timer)
        
    Example:
        logger, timer = setup_logging_with_timer('BDH_06_Scoring')
        timer.start()
        logger.info("Processing...")
        timer.stop()
    """
    logger = setup_logging(script_name, **logging_kwargs)
    timer = ScriptTimer(script_name, logger)
    return logger, timer


# =============================================================================
# STEP TIMER (for timing individual steps within a script)
# =============================================================================

class StepTimer:
    """
    Timer for measuring individual steps within a script.
    
    Usage:
        step = StepTimer("Loading data", logger)
        step.start()
        # ... load data ...
        step.stop()  # Logs: "✅ Loading data completed in 2m 34s"
    """
    
    def __init__(self, step_name: str, logger: Optional[logging.Logger] = None):
        self.step_name = step_name
        self.logger = logger or logging.getLogger(__name__)
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def start(self) -> 'StepTimer':
        self.start_time = datetime.now()
        self.logger.info(f"⏱️  Starting: {self.step_name}...")
        return self
    
    def stop(self) -> 'StepTimer':
        self.end_time = datetime.now()
        duration = self.end_time - self.start_time
        self.logger.info(f"✅ {self.step_name} completed in {self._format_duration(duration)}")
        return self
    
    def _format_duration(self, d: timedelta) -> str:
        total_seconds = int(d.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{d.total_seconds():.1f}s"
    
    def __enter__(self) -> 'StepTimer':
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self.stop()
        else:
            self.end_time = datetime.now()
            duration = self.end_time - self.start_time
            self.logger.error(f"❌ {self.step_name} failed after {self._format_duration(duration)}")
        return False


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Demo the utilities
    print("CRPUtils Logging Utilities Demo")
    print("=" * 50)
    
    # Setup logging
    logger = setup_logging('logging_utils_demo')
    
    # Use script timer as context manager
    with ScriptTimer('Demo Script', logger) as timer:
        logger.info("Doing some work...")
        
        # Use step timer for individual steps
        with StepTimer("Step 1: Simulated processing", logger):
            import time
            time.sleep(1)
        
        with StepTimer("Step 2: More processing", logger):
            time.sleep(0.5)
        
        logger.info("Work complete!")
    
    print(f"\nTotal duration: {timer.duration_formatted}")
    print(f"Log file: C:\\Logs\\logging_utils_demo.log")