"""Nox configuration file for running tests with pytest."""
import nox_poetry


@nox_poetry.session()
def all_tests(session):
    """Run the test suite using pytest."""
    session.install("pytest", ".")  # for coverage report include: "pytest-cov"
    session.run(
        "pytest",
        # "--cov=teehr",
        # "--ctrf=report.json",
        # "--cov-report=term",
    )


@nox_poetry.session(reuse_venv=True)
def evaluations(session):
    """Run tests related to evaluations."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/evaluations"
    )


@nox_poetry.session(reuse_venv=True)
def fetch(session):
    """Run tests related to fetching."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/fetch"
    )


@nox_poetry.session(reuse_venv=True)
def generate(session):
    """Run tests related to generating timeseries."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/generate"
    )


@nox_poetry.session(reuse_venv=True)
def load(session):
    """Run tests related to loading."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/load"
    )


@nox_poetry.session(reuse_venv=True)
def query(session):
    """Run tests related to querying."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/query"
    )


@nox_poetry.session(reuse_venv=True)
def s3_utils(session):
    """Run tests related to S3 utilities."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/s3_utils"
    )


@nox_poetry.session(reuse_venv=True)
def visualization(session):
    """Run tests related to visualization."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/visualization"
    )


@nox_poetry.session(reuse_venv=True)
def iceberg(session):
    """Run tests related to iceberg."""
    session.install("pytest", ".")
    session.run(
        "pytest",
        "tests/iceberg"
    )
