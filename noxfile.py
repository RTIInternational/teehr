"""Nox configuration file for running tests with pytest."""
import nox_poetry


@nox_poetry.session()  # local test, ex: python=["3.11", "3.12"]
def tests(session):
    """Run the test suite using pytest."""
    session.install(".")
    session.run(
        "pytest",
        "--cov=teehr",
        "--ctrf=report.json",
        "--cov-report=term"
    )
