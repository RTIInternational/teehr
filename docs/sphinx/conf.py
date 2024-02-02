"""Configuration file for the Sphinx documentation builder."""
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.abspath('../../src/teehr'))

# -- Project information -----------------------------------------------------
project = 'TEEHR: Tools for Exploratory Evaluation in Hydrologic Research'
copyright = f'{datetime.now().year} RTI International'
author = 'RTI International, Matthew Denno <mdenno@rti.org>, Katie van Werkhoven <kvanwerkhoven@rti.org>, Sam Lamont <slamont@rti.org>'

# The full version, including alpha/beta/rc tags
release = '0.3.2'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    # "sphinx.ext.napoleon",
    "numpydoc",
    # "sphinx.ext.coverage",
    # "sphinx.ext.linkcode",
    "sphinx_design",  # gives us grids and other design elements
    "autoapi.extension",
    "sphinx.ext.autosummary",
    # "sphinx_click",
    # "myst_parser",
    "sphinx.ext.viewcode",  # links to source code
    # "nbsphinx"  # for rendering jupyter notebooks  ?? or myst-nb ??
    # "myst_nb"
]

# -- Options for autodoc ----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
# autodoc_typehints = "description"

# Don't show class signature with the class' name.
# autodoc_class_signature = "separated"

# autosummary_generate = True

# autoapi extension configuration
autoapi_dirs = ['../../src/teehr']
autoapi_add_toctree_entry = True
autoapi_template_dir = '_templates/autoapi'
# autoapi_options = {'show-module-summary': True}


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The main toctree document.
master_doc = 'index'


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'pydata_sphinx_theme'
html_theme_options = {
  "show_toc_level": 2,
  "github_url": "https://github.com/RTIInternational/teehr",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_logo = "../images/teehr.png"
html_css_files = [
    # "css/getting_started.css",
    "css/teehr.css",
]

# numpydoc
# numpydoc_show_class_members = False
# numpydoc_show_inherited_class_members = False
# numpydoc_attributes_as_param_list = False


# # Napoleon settings
# napoleon_google_docstring = False
# napoleon_numpy_docstring = True
# napoleon_include_private_with_doc = False
# napoleon_include_special_with_doc = True
# napoleon_use_admonition_for_examples = False
# napoleon_use_admonition_for_notes = False
# napoleon_use_admonition_for_references = False
# napoleon_use_ivar = False
# napoleon_use_param = False
# napoleon_use_rtype = False
