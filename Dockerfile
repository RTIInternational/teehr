# Build TEEHR
FROM python:3.12 AS builder

WORKDIR /teehr

COPY . /teehr

RUN TEEHR_VERSION=$(cat /teehr/version.txt) && \
    pip install --upgrade pip build && \
    python -m build && \
    python -m pip install dist/teehr-${TEEHR_VERSION}.tar.gz && \
    mv dist/teehr-${TEEHR_VERSION}.tar.gz dist/teehr-build.tar.gz

# Install TEEHR in the Pangeo Image
FROM pangeo/base-notebook:2025.01.24

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=${NB_PYTHON_PREFIX}/bin:$PATH

# Needed for apt-key to work -- Is this part needed?
RUN apt-get update -qq --yes > /dev/null && \
    apt-get install --yes -qq gnupg2 > /dev/null && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update \
 && apt-get install -y wget curl bzip2 libxtst6 libgtk-3-0 libx11-xcb-dev libdbus-glib-1-2 libxt6 libpci-dev libasound2 firefox openjdk-17-jdk

# RUN conda install -y -c conda-forge nodejs
RUN mamba install -n ${CONDA_ENV} -y -c conda-forge nodejs selenium geckodriver pyspark awscli htop

# Set up the environment for pyspark
ENV SPARK_HOME=${NB_PYTHON_PREFIX}/lib/python3.12/site-packages/pyspark
RUN curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -Lo ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar
RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -Lo ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar

RUN mkdir -p ${SPARK_HOME}/conf
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

WORKDIR /teehr

RUN pip install duckdb spatialpandas easydev colormap colorcet hydrotools datashader

COPY --from=builder /teehr/dist/teehr-build.tar.gz /teehr/dist/teehr-build.tar.gz

RUN python -m pip install dist/teehr-build.tar.gz

USER ${NB_USER}

WORKDIR /home/jovyan
