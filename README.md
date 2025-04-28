![alt text](https://github.com/RTIInternational/teehr/blob/main/docs/images/teehr.png)

| | |
| --- | --- |
| ![alt text](https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png) | Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (NA22NWS4320003). |


# TEEHR - Tools for Exploratory Evaluation in Hydrologic Research
TEEHR (pronounced "tier") is a python tool set for loading, storing,
processing and visualizing hydrologic data, particularly National Water
Model data, for the purpose of exploring and evaluating the datasets to
assess their skill and performance.

NOTE: THIS PROJECT IS UNDER DEVELOPMENT - EXPECT TO FIND BROKEN AND INCOMPLETE CODE.

## Documentation
[TEEHR Documentation](https://rtiinternational.github.io/teehr/)

## How to Install TEEHR
We do not currently push TEEHR to PyPI, so the easiest way to install it is directly from GitHub.
If using `pip` to install TEEHR, we recommend installing TEEHR in a virtual environment.
The code below should create a new virtual environment and install TEEHR in it.

```bash
# Create directory for your code and create a new virtual environment.
mkdir teehr_examples
cd teehr_examples
python3 -m venv .venv
source .venv/bin/activate

# Install using pip.
# Starting with version 0.4.1 TEEHR is available in PyPI
pip install teehr

# Download the required JAR files for Spark to interact with AWS S3.
python -m teehr.utils.install_spark_jars
```
Use Docker
```bash
$ docker build -t teehr:v0.4.10 .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.4.10 jupyter lab --ip 0.0.0.0 $HOME
```

## Notes for Windows users:
Currently, TEEHR dependencies require users install on Linux or macOS. To use TEEHR on Windows, we recommend Windows Subsystem for Linux (WSL).

#### 1. Install Linux on Windows via WSL

- Detailed instructions here: [How to install Linux on Windows with WSL](https://learn.microsoft.com/en-us/windows/wsl/install)
- Summary:
  - Run `wsl --install` in PowerShell or Windows Terminal. This will install necessary features to run Windows subsystem for Linux (WSL) and install the default Ubuntu Linux distribution.
  - Restart your machine.
- Validate install:
  - Check what Ubuntu version you have installed: `wsl -l -v`
  - If you have just "Ubuntu" installed; install a newer, full version.
  - Check what versions are available to install: `wsl --list --online`
  - We recommend you install Ubuntu-22.04: `wsl --install -d Ubuntu-22.04`
  - Set Ubuntu-22.04 as default: `wsl -s Ubuntu-22.04`
    - NOTE: If you are in the Linux system from the step above, you need to "exit" then run this back in the Windows prompt.

#### 2. Launch Ubuntu.
- You can launch your default WSL installation from the terminal using: `wsl`
- You can also launch Ubuntu directly from the start menu by searching for 'Ubuntu'

#### 3. Set-up Python on Linux (within WSL terminal)
- Update and upgrade Ubuntu:
```
sudo apt update && sudo apt upgrade
sudo apt-get install wget ca-certificates
```

- Install some key default development packages:
```
sudo apt install -y build-essential git curl libexpat1-dev libssl-dev zlib1g-dev libncurses5-dev libbz2-dev liblzma-dev libsqlite3-dev libffi-dev tcl-dev linux-headers-generic libgdbm-dev libreadline-dev tk tk-dev
```

- Install pyenv to manage Python versions:
```
curl https://pyenv.run | bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
# Reload bashrc
source ~/.bashrc
# Confirm installation
pyenv --version
```

- Install the required Python version (>= 3.10.12):
```
pyenv install 3.10.12
pyenv rehash
# To set your global python version
pyenv global 3.10.12
# To set your local python version
pyenv local 3.10.12
```

#### 4. Set-up Linux dependencies
- Install Java
  - `sudo apt install openjdk-17-jre-headless`

#### 5. Set-up VSCode in WSL
- Follow the official VSCode Instructions here: [VSCode for WSL](https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode)

#### 6. Set-up TEEHR
- Create a directory to store the environment:
  - `mkdir teehr_example`
- Navigate to the directory in the terminal and set up a new virtual environment:
```
cd teehr_example
python3 -m venv .venv
source .venv/bin/activate
pip install teehr
python -m teehr.utils.install_spark_jars
```

## Examples
For examples of how to use TEEHR, see the [examples](examples).  We will maintain a basic set of example Jupyter Notebooks demonstrating how to use the TEEHR tools.


## Resources
In May of 2023 we put on a workshop at the CIROH 1st Annual Training and Developers Conference.  The workshop materials and presentation are available in the workshop GitHub repository: [teehr-may-2023-workshop](https://github.com/RTIInternational/teehr-may-2023-workshop).  This workshop was based on version 0.1.0.

## Versioning
The TEEHR project follows semantic versioning as described here: [https://semver.org/](https://semver.org/).
Note, per the specification, "Major version zero (0.y.z) is for initial development. Anything MAY change at any time. The public API SHOULD NOT be considered stable.".  We are solidly in "major version zero" territory, and trying to move fast, so expect breaking changes often.
