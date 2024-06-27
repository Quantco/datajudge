# Configuration file for the Sphinx documentation builder.

# -- Project information
project = "datajudge"
copyright = "(C) 2022 QuantCo Inc."
author = "QuantCo Inc."

release = "1.0"
version = "1.0.0"

extensions = [
    "numpydoc",
    "sphinxcontrib.apidoc",
    "sphinx.ext.autodoc",
]


apidoc_module_dir = "../../src/datajudge"
apidoc_output_dir = "api"
apidoc_separate_modules = True
apidoc_excluded_paths = [
    "../../src/datajudge/db_access.py",
    "../../src/datajudge/constraints",
    # Requirements should be part of the exposed API documentation.
    # Yet, they are already exposed via the top-level module.
    "../../src/datajudge/requirements.py",
]
apidoc_extra_args = ["--implicit-namespaces"]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "inherited-members": True,
    "undoc-members": True,
}
autodoc_typehints = "description"

# Copied from https://stackoverflow.com/questions/65198998/sphinx-warning-autosummary-stub-file-not-found-for-the-methods-of-the-class-c/
# Also tested numpydoc_class_members_toctree = False but it does still create a TOC
numpydoc_show_class_members = False
