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

RUN conda install -c conda-forge nodejs

WORKDIR /teehr

RUN pip install duckdb spatialpandas easydev colormap colorcet hydrotools

COPY --from=builder /teehr/dist/teehr-build.tar.gz /teehr/dist/teehr-build.tar.gz

RUN python -m pip install dist/teehr-build.tar.gz

WORKDIR /home/jovyan