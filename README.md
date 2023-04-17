![alt text]("docs/images/teeher.png")
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
Example usage...

## How to Use TEEHR
Install...

`pip ...`

Use Docker...

`docker build -t localbuild/teehr:latest .`
`docker run -it --rm --volume $HOME:$HOME -p 8888:8888 localbuild/teehr:latest jupyter lab --ip 0.0.0.0 $HOME`

## Local Development
The most common way to use TEEHR is by installing it in a Python virtual 
environment.  The document covers using a conda virtual environment, but 
there is no hard requirement to do so.

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

# Cloning 

## GIT LFS
Git LFS is used for large files (*.csv, *.nc, etc.)

## NB Strip Output
`nbstripoutput` is configured to strip output from notebooks to keep the size down and make diffing files easier.  
See https://github.com/kynan/nbstripout.
Note, after cloning, you must run `nbstripout --install` in the repo to install `nbstripoutput`.
The configuraion is stored in the `.gitattributes` file, but the tool must be installed per repo.
You may need to install it with `conda install nbstripout` or similar depending on your environment.

 