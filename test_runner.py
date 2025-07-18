#!/usr/bin/env python3
import sys
from pathlib import Path
import runpy
import subprocess

# List of finalized testable scripts
TEST_SCRIPTS = [
    {"name": "check_birth_inconsistencies.py", "label": "birth inconsistencies"},
    {"name": "find_missing_media_files.py", "label": "missing files"},
    {"name": "find_multiple_unique_facts.py", "label": "duplicate facts"},
]

def run_test(script_info):
    script_path = Path(script_info["name"])
    if not script_path.exists():
        print(f"❌ Script not found: {script_path}")
        return False

    try:
        # result = runpy.run_path(str(script_path))

        if script_path.name == "find_multiple_unique_facts.py":
            result = subprocess.run([sys.executable, str(script_path), "--summary"], capture_output=True, text=True)
        else:
            result = runpy.run_path(str(script_path))
            returned = result.get("main")() if "main" in result else None
            count = len(returned) if returned else 0
            print(f"[{script_path.name}] {count} {script_info['label']}")
            return True

        # returned = result.get("main")() if "main" in result else None

        if result.returncode == 0:
            print(result.stdout.strip())
            return True
        else:
            print(f"❌ Error in {script_path.name}:\n{result.stderr.strip()}")
            return False



        count = len(returned) if returned else 0
        print(f"[{script_path.name}] {count} {script_info['label']}")
        return True
    except Exception as e:
        print(f"❌ Error running {script_path.name}: {e}")
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
