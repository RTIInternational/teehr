# Build TEEHR
FROM python:3.10 AS builder

WORKDIR /teehr

COPY . /teehr

RUN pip install --upgrade pip build && \
    python -m build && \
    python -m pip install dist/teehr-0.1.0.tar.gz

# Install TEEHR in the Pangeo Image
FROM pangeo/pangeo-notebook:latest

WORKDIR /teehr

RUN pip install duckdb spatialpandas easydev colormap colorcet hydrotools

COPY --from=builder /teehr/dist/teehr-0.1.0.tar.gz /teehr/dist/teehr-0.1.0.tar.gz

RUN python -m pip install dist/teehr-0.1.0.tar.gz
