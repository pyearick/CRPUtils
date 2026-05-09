"""
CRPUtils/webdriver_utils.py - Chrome WebDriver Utilities
=========================================================

Provides automatic ChromeDriver management so you never have to manually
update ChromeDriver when Chrome auto-updates.

Usage:
------
    from CRPUtils.webdriver_utils import get_chrome_driver

    # Basic usage - headless by default
    driver = get_chrome_driver()
    
    # Visible browser for debugging
    driver = get_chrome_driver(headless=False)
    
    # With custom options
    driver = get_chrome_driver(headless=True, disable_images=True)
    
    # Use undetected-chromedriver for sites with bot detection
    driver = get_chrome_driver(use_undetected=True)
    
    # Don't forget to quit when done!
    driver.quit()

Features:
---------
- Automatic ChromeDriver version management (matches your Chrome version)
- Optional undetected-chromedriver for bypassing bot detection
- Sensible defaults for scraping (headless, common anti-detection options)
- Optional image/CSS disabling for faster scraping
- Logging integration

Requirements:
-------------
    pip install selenium webdriver-manager
    pip install undetected-chromedriver  # Optional, for bot-protected sites
"""

import logging
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# Try to import undetected-chromedriver for bot-protected sites
try:
    import undetected_chromedriver as uc
    HAS_UNDETECTED = True
except ImportError:
    HAS_UNDETECTED = False
    
logger = logging.getLogger(__name__)

def _detect_chrome_version() -> Optional[int]:
    """Detect installed Chrome major version from Windows registry."""
    try:
        import winreg
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                key = winreg.OpenKey(hive, r"SOFTWARE\Google\Chrome\BLBeacon")
                version_str, _ = winreg.QueryValueEx(key, "version")
                winreg.CloseKey(key)
                major = int(version_str.split(".")[0])
                logger.info(f"Detected Chrome {version_str} (major: {major})")
                return major
            except (FileNotFoundError, OSError):
                continue
    except ImportError:
        pass
    return None
    
def get_chrome_driver(
    headless: bool = True,
    use_undetected: bool = False,
    disable_images: bool = False,
    disable_css: bool = False,
    disable_javascript: bool = False,
    window_size: tuple = (1920, 1080),
    user_agent: Optional[str] = None,
    download_dir: Optional[str] = None,
    additional_options: Optional[list] = None,
    log_level: int = 3,  # 0=ALL, 1=DEBUG, 2=INFO, 3=WARNING (default to suppress noise)
    page_load_strategy: str = 'normal',  # 'normal', 'eager', or 'none'
) -> webdriver.Chrome:
    """
    Get a Chrome WebDriver with automatic driver management.
    
    Args:
        headless: Run browser without visible window (default: True)
        use_undetected: Use undetected-chromedriver for bot-protected sites (default: False)
        disable_images: Don't load images - faster scraping (default: False)
        disable_css: Don't load CSS - faster scraping (default: False)  
        disable_javascript: Disable JS - use carefully (default: False)
        window_size: Browser window size as (width, height)
        user_agent: Custom user agent string (None = Chrome default)
        download_dir: Directory for file downloads (None = default)
        additional_options: List of additional Chrome arguments
        log_level: Chrome log level 0-3 (default: 3 to suppress noise)
        page_load_strategy: 'normal' (wait for all), 'eager' (DOM ready), 'none' (don't wait)
        
    Returns:
        selenium.webdriver.Chrome instance (or undetected_chromedriver.Chrome)
        
    Example:
        driver = get_chrome_driver(headless=False)  # Visible for debugging
        driver.get("https://example.com")
        # ... scrape ...
        driver.quit()
        
        # For bot-protected sites like ECS Tuning or O'Reilly:
        driver = get_chrome_driver(use_undetected=True, headless=False)
    """
    
    # Use undetected-chromedriver if requested and available
    if use_undetected:
        if not HAS_UNDETECTED:
            logger.warning("undetected-chromedriver not installed, falling back to regular Chrome")
            logger.warning("Install with: pip install undetected-chromedriver")
        else:
            return _get_undetected_driver(headless, window_size, user_agent, additional_options)
    
    # Regular Chrome with webdriver-manager
    if not HAS_WEBDRIVER_MANAGER:
        raise ImportError(
            "webdriver-manager is required. Install with: pip install webdriver-manager"
        )
    
    options = Options()
    
    # Headless mode
    if headless:
        options.add_argument("--headless=new")  # New headless mode (Chrome 109+)
    
    # Window size (important even in headless for consistent rendering)
    options.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
    
    # Common anti-detection / stability options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    
    # Reduce detection as automated browser
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # Suppress logging noise
    options.add_argument(f"--log-level={log_level}")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    # Page load strategy
    options.page_load_strategy = page_load_strategy
    
    # Custom user agent
    if user_agent:
        options.add_argument(f"--user-agent={user_agent}")
    
    # Performance options - disable images/CSS
    prefs = {}
    
    if disable_images:
        prefs["profile.managed_default_content_settings.images"] = 2
        
    if disable_css:
        # Disable CSS via preferences
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
        
    if disable_javascript:
        prefs["profile.managed_default_content_settings.javascript"] = 2
        
    if download_dir:
        prefs["download.default_directory"] = download_dir
        prefs["download.prompt_for_download"] = False
        prefs["download.directory_upgrade"] = True
        
    if prefs:
        options.add_experimental_option("prefs", prefs)
    
    # Additional custom options
    if additional_options:
        for opt in additional_options:
            options.add_argument(opt)
    
    # Get the driver with automatic ChromeDriver management
    try:
        logger.info("Initializing Chrome WebDriver with automatic driver management...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("Chrome WebDriver initialized successfully")
        
        # Additional anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
        
    except Exception as e:
        logger.error(f"Failed to initialize Chrome WebDriver: {e}")
        raise


def _get_undetected_driver(
    headless: bool,
    window_size: tuple,
    user_agent: Optional[str],
    additional_options: Optional[list],
) -> 'uc.Chrome':
    """
    Get an undetected-chromedriver instance for bot-protected sites.
    
    Note: undetected-chromedriver handles its own driver management.
    """
    options = uc.ChromeOptions()
    
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
    options.add_argument("--disable-gpu")
    
    if user_agent:
        options.add_argument(f"--user-agent={user_agent}")
    
    if additional_options:
        for opt in additional_options:
            options.add_argument(opt)
    
    logger.info("Initializing undetected-chromedriver...")
    driver = uc.Chrome(options=options, version_main=_detect_chrome_version())
    logger.info("undetected-chromedriver initialized successfully")
    
    return driver


def get_chrome_driver_visible(**kwargs) -> webdriver.Chrome:
    """
    Convenience function for visible browser (for debugging).
    
    Same as get_chrome_driver(headless=False, **kwargs)
    """
    return get_chrome_driver(headless=False, **kwargs)


def get_chrome_driver_fast(**kwargs) -> webdriver.Chrome:
    """
    Convenience function for fast scraping (no images/CSS).
    
    Same as get_chrome_driver(disable_images=True, disable_css=True, **kwargs)
    """
    return get_chrome_driver(disable_images=True, disable_css=True, **kwargs)


def get_chrome_driver_undetected(headless: bool = False, **kwargs):
    """
    Convenience function for bot-protected sites.
    
    Uses undetected-chromedriver to bypass Cloudflare, etc.
    Note: headless=False is recommended for best results with bot detection.
    
    Same as get_chrome_driver(use_undetected=True, headless=False, **kwargs)
    """
    return get_chrome_driver(use_undetected=True, headless=headless, **kwargs)


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    print("CRPUtils WebDriver Utilities Demo")
    print("=" * 50)
    
    # Test that it works
    print("\nTesting Chrome WebDriver initialization...")
    
    try:
        driver = get_chrome_driver(headless=True)
        print("✅ WebDriver initialized successfully!")
        
        # Quick test
        driver.get("https://www.google.com")
        print(f"✅ Loaded page: {driver.title}")
        
        driver.quit()
        print("✅ WebDriver closed successfully!")
        
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("   Run: pip install selenium webdriver-manager")
        
    except Exception as e:
        print(f"❌ Error: {e}")