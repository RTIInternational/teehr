# Build TEEHR
FROM python:3.10 AS builder

ARG TEEHR_VERSION=0.1.0

WORKDIR /teehr

COPY . /teehr

RUN echo ${TEEHR_VERSION} && echo ${TEEHR_VERSION} > /teehr/version.txt

RUN pip install --upgrade pip build && \
    python -m build && \
    python -m pip install dist/teehr-${TEEHR_VERSION}.tar.gz

# Install TEEHR in the Pangeo Image
# https://hub.docker.com/r/pangeo/pangeo-notebook/tags
FROM pangeo/pangeo-notebook:2023.06.07

WORKDIR /teehr

RUN pip install duckdb spatialpandas easydev colormap colorcet hydrotools

COPY --from=builder /teehr/dist/teehr-${TEEHR_VERSION}.tar.gz /teehr/dist/teehr-${TEEHR_VERSION}.tar.gz

RUN python -m pip install dist/teehr-${TEEHR_VERSION}.tar.gz

WORKDIR /home/jovyan