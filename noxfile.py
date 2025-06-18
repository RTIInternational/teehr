"""Nox configuration file for running tests with pytest."""
import nox_poetry


@nox_poetry.session()  # python=["3.11", "3.12"]
def tests(session):
    session.install(".")
    session.run("pytest", "tests/test_add_udfs.py")
