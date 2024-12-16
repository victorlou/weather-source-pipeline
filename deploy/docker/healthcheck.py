#!/usr/bin/env python3
"""
Healthcheck script for the Weather Source ETL pipeline container.
Verifies that the essential components are working correctly.
"""

import os
import sys
from pathlib import Path

def check_environment():
    """Verify required environment variables are set."""
    required_vars = ['WEATHER_SOURCE_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def check_filesystem():
    """Verify access to required directories."""
    required_paths = [
        Path('/app/data'),  # Data directory
        Path('/app/src'),   # Source code
    ]
    
    for path in required_paths:
        if not path.exists():
            print(f"Required path does not exist: {path}")
            return False
        # Check write permission for data directory
        if path == Path('/app/data'):
            try:
                test_file = path / '.healthcheck_test'
                test_file.touch()
                test_file.unlink()
            except Exception as e:
                print(f"Cannot write to data directory: {e}")
                return False
    return True

def check_python_imports():
    """Verify critical Python packages are importable."""
    required_packages = [
        'pandas',
        'numpy',
        'requests',
        'boto3',
        'pyarrow'
    ]
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError as e:
            print(f"Failed to import {package}: {e}")
            return False
    return True

def main():
    """Run all health checks."""
    checks = [
        ('Environment Variables', check_environment),
        ('File System', check_filesystem),
        ('Python Imports', check_python_imports)
    ]
    
    all_passed = True
    for name, check in checks:
        try:
            if not check():
                print(f"❌ {name} check failed")
                all_passed = False
            else:
                print(f"✓ {name} check passed")
        except Exception as e:
            print(f"❌ {name} check error: {e}")
            all_passed = False
    
    sys.exit(0 if all_passed else 1)

if __name__ == '__main__':
    main() 