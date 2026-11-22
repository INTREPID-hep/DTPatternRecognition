"""Add the version selector to every page of the documentation site.

The site keeps one build per tag under <root>/<tag>/ plus <root>/latest/ for
the main branch. The same menu is injected into every page after the builds
are done, so versions whose conf.py predates this script get the menu too,
and the list is identical everywhere.
"""

import pathlib
import sys

root = pathlib.Path(sys.argv[1])
versions = [version.strip() for version in sys.argv[2].split(",") if version.strip()]
base = sys.argv[3].rstrip("/")

SEARCH_BOX = '<div class="wy-side-nav-search"'


def label(version: str) -> str:
    return "latest (main)" if version == "latest" else version


def menu(current: str) -> str:
    options = []
    for version in versions:
        selected = " selected" if version == current else ""
        options.append(
            f'<option value="{base}/{version}/"{selected}>{label(version)}</option>'
        )
    return (
        '<div class="dtpr-versions" style="margin: 0.6em 0; text-align: center;">'
        '<label for="dtpr-versions"><small>Version</small></label><br>'
        '<select id="dtpr-versions" style="width: 100%;" '
        'onchange="if (this.value) window.location.href = this.value;">'
        + "".join(options)
        + "</select></div>"
    )


def inject(html: str, current: str) -> str:
    if "dtpr-versions" in html:
        return html
    start = html.find(SEARCH_BOX)
    if start != -1:
        end = html.find(">", start) + 1
        return html[:end] + menu(current) + html[end:]
    end = html.rfind("</body>")
    if end == -1:
        return html
    return html[:end] + menu(current) + html[end:]


for directory in sorted(root.iterdir()):
    if not directory.is_dir() or directory.name not in versions:
        continue
    for page in directory.rglob("*.html"):
        text = page.read_text(encoding="utf-8")
        patched = inject(text, directory.name)
        if patched != text:
            page.write_text(patched, encoding="utf-8")