"""Nox configuration file for running tests with pytest."""
import nox_poetry
import nox


@nox_poetry.session()  # for local testing, ex: python=["3.12", "3.13"]
def all_tests(session):
    """Run the test suite using pytest."""
    session.install("pytest", ".")  # for coverage report include: "pytest-cov"
    session.run(
        "pytest",
        # "--cov=teehr",
        # "--ctrf=report.json",
        # "--cov-report=term",
    )


@nox.session()  # python=["3.12", "3.13"]
def single_test(session):
    """Run a single test using pytest."""
    session.install("pytest", "poetry")
    session.run("poetry", "install", "--no-interaction")
    session.run(
        "pytest",
        "tests/test_clone_from_s3.py"
    )
