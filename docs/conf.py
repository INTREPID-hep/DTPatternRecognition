project = 'DTPatternRecognition'
copyright = '2025, Daniel Estrada, Universidad de Oviedo'
author = 'Daniel Estrada'
release = '3.0.0'

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
html_static_path = ['_static']
html_show_sourcelink=False
rst_prolog = """
:github_url: https://github.com/INTREPID-hep/DTPatternRecognition
"""