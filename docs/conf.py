import os
import pathlib
import re

project = 'DTPatternRecognition'
copyright = '2025, Daniel Estrada, Universidad de Oviedo'
author = 'Daniel Estrada'


def _release() -> str:
    """Version shown by the manual.

    The tag being built is passed in SPHINX_RELEASE by the documentation
    workflow, so every version of the site reports its own tag. Otherwise the
    version comes from pyproject.toml, which is the only place it is written
    down.
    """
    override = os.environ.get("SPHINX_RELEASE")
    if override:
        return override
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    try:
        import tomllib

        return tomllib.loads(text)["project"]["version"]
    except Exception:
        found = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        return found.group(1) if found else "unknown"


release = version = _release()

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.autosectionlabel",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]
suppress_warnings = ['autosectionlabel.*']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_rtd_theme'
html_baseurl = 'https://intrepid-hep.github.io/DTPatternRecognition/'
html_static_path = ['_static']
html_show_sourcelink=False
rst_prolog = """
:github_url: https://github.com/INTREPID-hep/DTPatternRecognition
"""