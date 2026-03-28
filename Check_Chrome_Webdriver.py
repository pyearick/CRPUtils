"""
Check_Chrome_WebDriver.py
==========================

Check Chrome and ChromeDriver versions to ensure compatibility

Usage:
    python Check_Chrome_WebDriver.py
"""

import subprocess
import sys
import os
import re


def check_chrome_version():
    """Check installed Chrome version"""
    try:
        # Try Windows Chrome location
        chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

        if not os.path.exists(chrome_path):
            chrome_path = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

        if os.path.exists(chrome_path):
            result = subprocess.run(
                [chrome_path, '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )

            version_text = result.stdout.strip()
            version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_text)

            if version_match:
                full_version = version_match.group(1)
                major_version = full_version.split('.')[0]

                print(f"✓ Chrome installed: {version_text}")
                print(f"  Full version: {full_version}")
                print(f"  Major version: {major_version}")
                return major_version
            else:
                print(f"⚠️ Chrome found but version unclear: {version_text}")
                return None
        else:
            print("❌ Chrome not found at standard locations")
            return None

    except Exception as e:
        print(f"❌ Error checking Chrome: {e}")
        return None


def check_chromedriver_version():
    """Check installed ChromeDriver version"""
    try:
        result = subprocess.run(
            ['chromedriver', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )

        version_text = result.stdout.strip()
        version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_text)

        if version_match:
            full_version = version_match.group(1)
            major_version = full_version.split('.')[0]

            print(f"✓ ChromeDriver installed: {version_text}")
            print(f"  Full version: {full_version}")
            print(f"  Major version: {major_version}")
            return major_version
        else:
            print(f"⚠️ ChromeDriver found but version unclear: {version_text}")
            return None

    except FileNotFoundError:
        print("❌ ChromeDriver not found in PATH")
        print("   Download from: https://chromedriver.chromium.org/downloads")
        return None
    except Exception as e:
        print(f"❌ Error checking ChromeDriver: {e}")
        return None


def check_selenium():
    """Check if Selenium is installed"""
    try:
        import selenium
        print(f"✓ Selenium installed: version {selenium.__version__}")
        return True
    except ImportError:
        print("❌ Selenium not installed")
        print("   Install with: pip install selenium")
        return False


def main():
    print("=" * 80)
    print("CHROME & CHROMEDRIVER VERSION CHECK")
    print("=" * 80)
    print()

    # Check Selenium
    print("1. Checking Selenium...")
    selenium_ok = check_selenium()
    print()

    # Check Chrome
    print("2. Checking Chrome...")
    chrome_version = check_chrome_version()
    print()

    # Check ChromeDriver
    print("3. Checking ChromeDriver...")
    chromedriver_version = check_chromedriver_version()
    print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if not selenium_ok:
        print("❌ Selenium not installed - install with: pip install selenium")
    elif not chrome_version:
        print("❌ Chrome not found - install Chrome browser")
    elif not chromedriver_version:
        print("❌ ChromeDriver not found - download from:")
        print("   https://chromedriver.chromium.org/downloads")
        if chrome_version:
            print(f"   You need ChromeDriver version {chrome_version}.x.x to match Chrome {chrome_version}")
    elif chrome_version != chromedriver_version:
        print("⚠️ VERSION MISMATCH!")
        print(f"   Chrome major version: {chrome_version}")
        print(f"   ChromeDriver major version: {chromedriver_version}")
        print()
        print(f"   Download ChromeDriver {chrome_version}.x.x from:")
        print("   https://chromedriver.chromium.org/downloads")
    else:
        print("✓ All good! Chrome and ChromeDriver versions match")
        print(f"  Version: {chrome_version}")
        print()
        print("You're ready to run:")
        print("  python BDH_06_ShowMeTheParts_Selenium.py --oe 0064664501")

    print()


if __name__ == "__main__":
    main()