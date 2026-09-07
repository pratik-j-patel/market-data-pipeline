# =============================================================================
# The Airflow image, extended with this pipeline's Python.
#
# Airflow's own image ships Airflow's dependencies and nothing else. It has
# never heard of requests, boto3 or dbt, so a task that runs fetch_tickers.py
# fails on the first import until this file exists.
#
# WHY A SECOND VIRTUALENV RATHER THAN INSTALLING ALONGSIDE AIRFLOW
# Airflow and dbt both pin large, overlapping sets of libraries. Installing this
# project's requirements into Airflow's own environment asks pip to satisfy two
# sets of constraints at once, and when it cannot, it does not fail -- it
# resolves to versions neither side asked for. This project's dbt pins are the
# most fragile thing in it (requirements.txt explains why in detail), so they
# get their own environment where nothing else has an opinion. The exact pins
# verified on a laptop are the ones that run here.
#
# The cost is one extra path in every DAG command, which the three ENV values
# below turn into a name.
#
# NOTHING HERE IS BUILT FROM SOURCE. Every package in requirements.txt publishes
# a wheel for this image's platform. That was checked against PyPI before this
# file was written, because the last time pip fell back to a source build in
# this project it downloaded from GitHub at build time and died on a certificate
# error that read like a network fault and was not one.
# =============================================================================

FROM apache/airflow:3.3.1

# Root only to create the directory with the right owner; the install itself
# runs as airflow, so nothing in the venv ends up owned by root.
USER root
RUN install -d -o airflow -g 0 /opt/pipeline
USER airflow

# The build context is the repository root (see docker-compose.yaml), so this
# is the same requirements.txt the laptop uses. One file, one set of pins.
COPY --chown=airflow:0 requirements.txt /opt/pipeline/requirements.txt

RUN python -m venv /opt/pipeline/venv \
 && /opt/pipeline/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline/venv/bin/pip install --no-cache-dir -r /opt/pipeline/requirements.txt \
 # Prove the environment works while the build can still fail loudly. An image
 # that builds and then cannot import boto3 wastes the next hour inside a
 # scheduler log instead of here, where the error is three lines from its cause.
 && /opt/pipeline/venv/bin/python -c "import requests, boto3, dotenv, snowflake.connector, yaml, cryptography" \
 && /opt/pipeline/venv/bin/dbt --version

# Named so the DAG reads as commands rather than as paths.
ENV PIPELINE_PYTHON=/opt/pipeline/venv/bin/python \
    PIPELINE_DBT=/opt/pipeline/venv/bin/dbt \
    PIPELINE_REPO=/opt/repo
