# Contributing Guidelines
These contributing guidelines will be updated as we progress. They are pretty 
slim to start.

TEEHR has multiple parts, one is a library of reusable code that can be imported 
as a dependency to another project, another is examples and dashboards which are 
more use case specific (e.g., a dashboard to conduct post event analysis). The
guidelines for contributing may be a bit different.
## Library Code
- Use [PEP 8](https://peps.python.org/pep-0008/)
- Use LFS for large files
- Write tests - you are going to test your code, why not write an actual test.
- Use the Numpy doc string format [numpydoc]
(https://numpydoc.readthedocs.io/en/latest/format.html)

## Notebooks
- Do not commit notebook output to the repo.  Use can install and use `nbstripout`
to 