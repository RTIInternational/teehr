"""Configuration file for the Sphinx documentation builder."""
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
from datetime import datetime
import tomli
import os
import sys

# sys.path.insert(0, os.path.abspath('../../src'))
sys.path.insert(0, os.path.abspath("../../src/teehr"))

# -- Project information -----------------------------------------------------
project = 'TEEHR: Tools for Exploratory Evaluation in Hydrologic Research'
copyright = f'{datetime.now().year} RTI International'


def _get_project_meta():
    with open('../../pyproject.toml', mode='rb') as pyproject:
        return tomli.load(pyproject)['tool']['poetry']


# Get the authors and latest version from the pyproject.toml file.
pkg_meta = _get_project_meta()
author = ", ".join(pkg_meta['authors'])
version = str(pkg_meta['version'])
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_design",  # gives us grids and other design elements
    "sphinx.ext.viewcode",  # links to source code
    "sphinx.ext.githubpages",
    "myst_nb",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The main toctree document.
master_doc = 'index'

# -- Options for autodoc -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
# autodoc_typehints = "description"

# Don't show class signature with the class' name.
# autodoc_class_signature = "separated"

autosummary_generate = True

# -- Napolean options --------------------------------------------------------
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_param = False
napoleon_use_rtype = False
napoleon_preprocess_types = True


# -- MyST-NB options ---------------------------------------------------------
nb_execution_mode = "auto"
nb_execution_timeout = 60 * 10
nb_output_stderr = "remove-warn"
# myst_heading_anchors = 3
myst_enable_extensions = [
    "html_image",
    "colon_fence",
]

# -- Options for HTML output -------------------------------------------------
# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_theme_options = {
  "footer_start": ["copyright", "version"],
  "show_toc_level": 2,
  "github_url": "https://github.com/RTIInternational/teehr",
  "footer_center": ["footer_center.html"],
  "logo": {
      "image_light": "../images/teehr.png",
      "image_dark": "../images/TEEHR_Icon_DarkMode.png",
   }
}
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
# html_logo = "../images/teehr.png"
html_css_files = [
    # "css/getting_started.css",
    "css/teehr.css",
]
html_favicon = '_static/favicon.png'

