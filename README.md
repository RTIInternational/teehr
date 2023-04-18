![alt text](docs/images/teehr.png)
# TEEHR - Tools for Exploratory Evaluation in Hydrologic Research
TEEHR (pronounced "tier") is a python tool set for loading, storing,
processing and visualizing hydrlogic data, particularly National Water
Model data, for the purpose of exploring and evaluating the datasets to
assess their skill and performance.

NOTE: THIS PROJECT IS UNDER DEVELOPMENT - EXPECT TO FIND BROKEN AND INCOMPLETE CODE.

Project structure:

- `/docs`: This directory contains documentation
- `/examples`: This directory contains
- `/src`: This directory contains
- `/references`: This directory contains
- `/tests`: This directory contains
- `/teehr`: This directory contains the library source code.
- `/playground`: This directory contains
- `/dashboards`: This directory contains

## Examples
For examples of how to use TEEHR, see the [examples](examples)

## How to Install TEEHR
Install with `pip`.

```bash
# Create and activate python environment, requires python >= 3.10
$ python3 -m venv .venv
$ source .venv/bin/activate
$ python3 -m pip install --upgrade pip

# Build and install from source
$ python3 -m pip install --upgrade build
$ python -m build
$ python -m pip install dist/teehr-0.0.1.tar.gz

# Or install from GitHub
$ pip install 'teehr @ git+https://[PAT]@github.com/RTIInternational/teehr@[BRANCH]'
```

Use Docker (not working yet)...
```bash
$ docker build -t localbuild/teehr:latest .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 localbuild/teehr:latest jupyter lab --ip 0.0.0.0 $HOME
```


## Local Development
The most common way to use TEEHR is by installing it in a Python virtual
environment.  The document covers using a conda virtual environment, but
there is no hard requirement to do so.  In this case the packages are not
installed, so you need to make sure you ass `src/` to your Python path.
There are two way to do this below, but depending on your development
environment, your milage may vary.

Install conda (miniconda3).  You may need to
`eval "$(/home/[username]/miniconda3/bin/conda shell.bash hook)"`
if you did not make it default when installing.

`conda config --append channels conda-forge`
`conda create --name teehr --file package-list.txt`
`conda activate teehr`

If the conda env already exists and needs to be updated:

`conda env update --name evaluation --file package-list.txt --prune`

If you add any packages, run the following to update the package list to
commit to the repo:

`conda list -e > package-list.txt`

To add the `src` path to the `PYTHONPATH`, from within the repo root run:

`conda env config vars set PYTHONPATH=${PWD}/src`

If developing in VSCode, it also helps to create a `.env` file with the
PYTHONPATH in it.  You can do so by running the following from within the
repo root:

`echo PYTHONPATH=src > .env`
