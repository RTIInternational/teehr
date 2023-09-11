# Build TEEHR
FROM python:3.10 AS builder

WORKDIR /teehr

COPY . /teehr

RUN TEEHR_VERSION=$(cat /teehr/version.txt) && \
    pip install --upgrade pip build && \
    python -m build && \
    python -m pip install dist/teehr-${TEEHR_VERSION}.tar.gz && \
    mv dist/teehr-${TEEHR_VERSION}.tar.gz dist/teehr-build.tar.gz

# Install TEEHR in the Pangeo Image
# https://hub.docker.com/r/pangeo/pangeo-notebook/tags
FROM pangeo/pangeo-notebook:2023.07.05

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH ${NB_PYTHON_PREFIX}/bin:$PATH

# Needed for apt-key to work
RUN apt-get update -qq --yes > /dev/null && \
    apt-get install --yes -qq gnupg2 > /dev/null && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update \
 && apt-get install -y wget bzip2 libxtst6 libgtk-3-0 libx11-xcb-dev libdbus-glib-1-2 libxt6 libpci-dev libasound2 firefox

# RUN conda install -y -c conda-forge nodejs
RUN mamba install -n ${CONDA_ENV} -y -c conda-forge nodejs selenium geckodriver

USER ${NB_USER}

WORKDIR /teehr

RUN pip install duckdb spatialpandas easydev colormap colorcet hydrotools

COPY --from=builder /teehr/dist/teehr-build.tar.gz /teehr/dist/teehr-build.tar.gz

RUN python -m pip install dist/teehr-build.tar.gz

WORKDIR /home/jovyan
