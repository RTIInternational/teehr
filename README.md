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

# Install using pip. The version can be changed to install a different version.
pip install 'teehr @ git+https://github.com/RTIInternational/teehr@v0.4.1'

# Download the required JAR files for Spark to interact with AWS S3.
python -m teehr.utils.install_spark_jars
```
Use Docker
```bash
$ docker build -t teehr:v0.4.1 .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.4.1 jupyter lab --ip 0.0.0.0 $HOME
```

## Examples
For examples of how to use TEEHR, see the [examples](examples).  We will maintain a basic set of example Jupyter Notebooks demonstrating how to use the TEEHR tools.


## Resources
In May of 2023 we put on a workshop at the CIROH 1st Annual Training and Developers Conference.  The workshop materials and presentation are available in the workshop GitHub repository: [teehr-may-2023-workshop](https://github.com/RTIInternational/teehr-may-2023-workshop).  This workshop was based on version 0.1.0.

## Versioning
The TEEHR project follows semantic versioning as described here: [https://semver.org/](https://semver.org/).
Note, per the specification, "Major version zero (0.y.z) is for initial development. Anything MAY change at any time. The public API SHOULD NOT be considered stable.".  We are solidly in "major version zero" territory, and trying to move fast, so expect breaking changes often.
