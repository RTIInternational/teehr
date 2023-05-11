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

Use Docker
```bash
$ docker build -t teehr:latest .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:latest jupyter lab --ip 0.0.0.0 $HOME
```
