.. _getting_started:

===============
Getting started
===============


Installation Guide for macOS & Linux
------------------------------------
TEEHR requires the following dependencies:

* Python 3.10 or later

* Java 17 or later for Spark

* Poetry v2 or later


The easiest way to install TEEHR is from PyPI using `pip`.
If using `pip` to install TEEHR, we recommend installing TEEHR in a virtual environment.
The code below creates a new virtual environment and installs TEEHR in it.

.. code-block:: python

   # Create directory for your code and create a new virtual environment.
   mkdir teehr_examples
   cd teehr_examples
   python3 -m venv .venv
   source .venv/bin/activate

   # Install using pip.
   # Starting with version 0.4.1 TEEHR is available in PyPI
   pip install teehr


Installation Guide for Windows
------------------------------
Currently, TEEHR dependencies require users install on Linux or macOS. To use TEEHR on Windows, we recommend Windows Subsystem for Linux (WSL). `Learn more about WSL <https://learn.microsoft.com/en-us/windows/wsl/about>`_.

1. Install Linux on Windows via WSL (in Windows terminal)

   * Detailed instructions can be found here: `How to Install Linux on Windows with WSL <https://learn.microsoft.com/en-us/windows/wsl/install>`_
   * Summary:

      * Run the following command in PowerShell or Windows Terminal. This will install necessary features to run Windows subsystem for Linux (WSL) and install the Ubuntu-22.04 Linux distribution.

      .. code-block:: bash

         wsl --install -d Ubuntu-22.04

      * Restart your machine.

   * Validate install:

      * Check what Ubuntu version you have installed using the following command:

      .. code-block:: bash

         wsl -l -v

2. Launch Ubuntu (in Windows terminal)

   * You can launch your default WSL installation from the terminal using the following command:

   .. code-block:: bash

      wsl

   * Upon entering the command, you should notice your terminal prompt/CWD update to display your current windows directory relative to your Linux file system (i.e. '/mnt/c/Users/{your_username}$'. To access your home directory in Linux, enter the following command:

   .. code-block:: bash

      cd ~/

3. Set-up Python on Linux (within WSL terminal)

   * Update and upgrade Ubuntu:

   .. code-block:: bash

      sudo apt update && sudo apt upgrade
      sudo apt-get install wget ca-certificates

   * Install some key default development packages (within WSL terminal):

   .. code-block:: bash

      sudo apt install -y build-essential git curl libexpat1-dev libssl-dev zlib1g-dev libncurses5-dev libbz2-dev liblzma-dev libsqlite3-dev libffi-dev tcl-dev linux-headers-generic libgdbm-dev libreadline-dev tk tk-dev

   * Install pyenv to manage Python versions (within WSL terminal):

   .. code-block:: bash

      curl https://pyenv.run | bash
      echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
      echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
      echo 'eval "$(pyenv init -)"' >> ~/.bashrc
      # Reload bashrc
      source ~/.bashrc
      # Confirm installation
      pyenv --version

   * Install the required Python version [>=3.10.12] (within WSL terminal):

   .. code-block:: bash

      pyenv install 3.10.12
      pyenv rehash
      # Set the global Python version to 3.10.12
      pyenv global 3.10.12
      # Confirm installation
      python --version

4. Set-up Linux dependencies (within WSL terminal)

   * Install Java 17 (within WSL terminal):

   .. code-block:: bash

      sudo apt install openjdk-17-jre-headless
      # Confirm installation
      java -version

5. Set-up VSCode in WSL

   * Follow the official VSCode Instructions here: `VSCode for WSL <https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode>`_.

      * NOTE: An installation of VSCode on Windows is required to utilize VSCode in your WSL distribution. The provided link details the full set-up process -- including installation on Windows.

6. Set-up TEEHR (within WSL terminal)

   * Create a new virtual environment:

   .. code-block:: bash

      mkdir teehr_examples
      cd teehr_examples
      python3 -m venv .venv
      source .venv/bin/activate
      pip install teehr


Set-up Guide for Docker
-----------------------
If you do not want to install TEEHR in your own virtual environment, you can use Docker. A Dockerfile
is provided in a separate repository at: https://github.com/RTIInternational/teehr-hub

Clone the repository, build and run the Docker image. The following commands will build and run the Docker image,
mount your home directory to the Docker container, and start a Jupyter Lab server.

When building the Docker image, specify the version of TEEHR you want to use by passing it in as a build
argument. Pass "dev" to use the latest development version.

.. code-block:: bash

   git clone https://github.com/RTIInternational/teehr-hub.git
   cd teehr-hub
   docker build --build-arg IMAGE_TAG="dev" -t teehr:dev .
   docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:dev jupyter lab --ip 0.0.0.0 $HOME

Project Objectives
------------------

* Easy integration into research workflows

* Use of modern and efficient data structures and computing platforms

* Scalable for rapid execution of large-domain/large-sample evaluations

* Simplified exploration of performance trends and potential drivers (e.g., climate, time-period, regulation, and basin characteristics)

* Inclusion of common and emergent evaluation methods (e.g., error statistics, skill scores, categorical metrics, hydrologic signatures, uncertainty quantification, and graphical methods)

* Open source and community-extensible development


Why TEEHR?
----------
TEEHR is a python package that provides a framework for the evaluation of hydrologic model performance.
It is designed to enable iterative and exploratory analysis of hydrologic data, and facilitates this through:

* Scalability - TEEHR's computational engine is built on PySpark, allowing it to take advantage of your available compute resources.

* Data Integrity - TEEHR's internal data model (:doc:`teehr_framework`) makes it easier to work with and validate the various data making up your evaluation, such as model outputs, observations, location attributes, and more.

* Flexibility - TEEHR is designed to be flexible and extensible, allowing you to easily customize metrics, add bootstrapping, and group and filter your data in a variety of ways.


TEEHR Evaluation Example
------------------------
The following is an example of initializing a TEEHR Evaluation, cloning a dataset from the TEEHR S3 bucket,
and calculating two versions of KGE (one with bootstrap uncertainty and one without).

.. code-block:: python

   import teehr
   from pathlib import Path

   # Initialize an Evaluation object
   ev = teehr.Evaluation(
      dir_path=Path(Path().home(), "temp", "quick_start_example"),
      create_dir=True
   )

   # Clone the example data from S3
   ev.clone_from_s3("e0_2_location_example")

   # Define a bootstrapper with custom parameters.
   boot = teehr.Bootstrappers.CircularBlock(
      seed=50,
      reps=500,
      block_size=10,
      quantiles=[0.05, 0.95]
   )
   kge = teehr.DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot)
   kge.output_field_name = "BS_KGE"

   include_metrics = [kge, teehr.DeterministicMetrics.KlingGuptaEfficiency()]

   # Get the currently available fields to use in the query.
   flds = ev.joined_timeseries.field_enum()

   metrics_df = ev.metrics.query(
      include_metrics=include_metrics,
      group_by=[flds.primary_location_id],
      order_by=[flds.primary_location_id]
   ).to_pandas()

   metrics_df


For a full list of metrics currently available in TEEHR, see the :doc:`/user_guide/metrics/metrics` documentation.

.. toctree::
    :maxdepth: 2
    :hidden:

    TEEHR Framework <teehr_framework>