#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

# List of finalized testable scripts (add to this list over time)
TEST_SCRIPTS = [
    {"name": "check_birth_inconsistencies.py", "args": []},
    {"name": "find_missing_media_files.py", "args": []},
    {"name": "find_multiple_unique_facts.py", "args": []},
    # {"name": "nickname_cleanup.py", "args": ["--dry-run"]},  # Future updater script example
]

def run_test(script_info):
    script_path = Path(script_info["name"])
    if not script_path.exists():
        print(f"❌ Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path)] + script_info.get("args", [])
    print(f"▶ Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_path}:")
        print(e.stderr.strip())
        return False

def main():
    all_passed = True
    for script_info in TEST_SCRIPTS:
        if not run_test(script_info):
            all_passed = False

    print("\n✅ All tests passed!" if all_passed else "\n❌ Some tests failed.")
    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
