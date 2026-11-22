"""Print one section of CHANGELOG.md.

Used as the message of the annotated tag and as the body of the GitHub
Release, so both carry the text that ships with the repository. Usage:

    python .github/scripts/changelog_section.py v3.6.0-beta
"""

import pathlib
import re
import sys

version = sys.argv[1]
changelog = pathlib.Path(__file__).resolve().parents[2] / "CHANGELOG.md"
text = changelog.read_text(encoding="utf-8")

pattern = r"^## \[{}\].*?$(.*?)(?=^## \[|^\[.*\]: )"
match = re.search(pattern.format(re.escape(version)), text, re.MULTILINE | re.DOTALL)
if not match:
    sys.exit(f"no section for {version} in CHANGELOG.md")

print(match.group(1).strip())