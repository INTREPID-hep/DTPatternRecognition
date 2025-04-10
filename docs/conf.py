
project = 'DTPatternRecognition'
copyright = '2025, Daniel Estrada, Universidad de Oviedo'
author = 'Daniel Estrada'
release = '1.0.0'

extensions = ["sphinx.ext.autodoc", "sphinx.ext.viewcode", "sphinx.ext.todo", "sphinx.ext.autosectionlabel", "sphinx_copybutton"]
suppress_warnings = ['autosectionlabel.*']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_show_sourcelink=False
rst_prolog = """
:github_url: https://github.com/DanielEstrada971102/DTPatternRecognition
"""