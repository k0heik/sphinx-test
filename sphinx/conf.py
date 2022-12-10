# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'SophiaAI'
copyright = '2022, SophiaAI'
author = 'SophiaAI'
release = ''

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",

]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
# html_theme = 'sphinx_rtd_theme'
# html_theme = 'sphinx_book_theme'
html_theme = 'furo'
html_static_path = ['_static']
html_logo = "logo.png"
html_title = "SophiaAI"
html_sidebars = {
    "**": [
        "sidebar/scroll-start.html",
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/ethical-ads.html",
        "sidebar/scroll-end.html",
    ]
}
html_theme_options = {
    "sidebar_hide_name": True,
    "top_of_page_button": "edit",
    "navigation_with_keys": True,
    "source_repository": "https://github.com/k0heik/sphinx-test/",
    "source_branch": "main",
    "source_directory": "sphinx/",
}

pygments_dark_style = "monokai"

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    # "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

import os
import sys
sys.path.insert(0, os.path.abspath("../"))
sys.path.insert(0, os.path.abspath("../sophia-ai"))
sys.path.insert(0, os.path.abspath("../cpc_prediction"))
sys.path.insert(0, os.path.abspath("../cvr_prediction"))
sys.path.insert(0, os.path.abspath("../spa_prediction"))
sys.path.insert(0, os.path.abspath("../main"))
