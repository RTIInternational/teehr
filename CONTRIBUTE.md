# Contributing Guidelines
These contributing guidelines will be updated as we progress. They are pretty
slim to start.

TEEHR has multiple parts, one is a library of reusable code that can be imported
as a dependency to another project, another is examples and dashboards which are
more use case specific (e.g., a dashboard to conduct post event analysis). The
guidelines for contributing may be a bit different.
## Library Code
- Use [PEP 8](https://peps.python.org/pep-0008/)
- Use LFS for large files
- Write tests - you are going to test your code, why not write an actual test
[pytest](https://docs.pytest.org/en/7.3.x/).
- Use the Numpy doc string format
[numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html)

## Git LFS
Use git lfs for large files.  Even better keep large files out of the repo.

## Notebooks
- Do not commit notebook output to the repo.  Use can install and use `nbstripout`
to strip output.  After cloning, you must run `nbstripout --install`.

`nbstripoutput` is configured to strip output from notebooks to keep the size down
and make diffing files easier. See https://github.com/kynan/nbstripout.
The configuration is stored in the `.gitattributes` file, but the tool must be
installed per repo. You may need to install the Python package first with
`conda install nbstripout` or similar depending on your environment.


## Local Development
The most common way to use TEEHR is by installing it in a Python virtual
environment.  The document covers using a conda virtual environment, but
there is no hard requirement to do so.  In this case the packages are not
installed, so you need to make sure you add `src/` to your Python path.
There are two way to do this below, but depending on your development
environment, your milage may vary.

After you install conda (or miniconda3)
```bash
# You may need to run the following
# if you did not make conda default when installing
$ eval "$(/home/[username]/miniconda3/bin/conda shell.bash hook)"

# Create conda env.
$ conda config --append channels conda-forge
$ conda create --name teehr --file package-list.txt
$ conda activate teehr


# If the conda env already exists and needs to be updated:
$ conda env update --name evaluation --file package-list.txt --prune

# Add src/ to PYTHONPATH
$ conda env config vars set PYTHONPATH=${PWD}/src
$ echo PYTHONPATH=src > .env # for VSCode
```

It seems that editing the conda env config and setting the `.env` do
not impact the Jupyter environment, so you may still need to:

```python
import sys
sys.path.insert(0, "../../src") # or similar
```

If you add any packages, run the following to update the package list to
commit to the repo:

`conda list -e > package-list.txt`
