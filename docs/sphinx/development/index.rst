.. _development:

===============
Developer Guide
===============

How to Contribute
-----------------
* bug reports
* issues
* pull requests
* documentation


Creating a Local Development Environment
----------------------------------------
* use poetry


Contributing to the Documentation
---------------------------------
* description
* docstring approach (numpy)
* pre-commit validation
* building and pushing docs


Contributing to the Code
------------------------
* guidelines
* testing
* documentation
* release process

Release Process
^^^^^^^^^^^^^^^
This document describes the release process which has some manual steps to complete.

Create branch with the following updated to the new version (find and replace version number):
- `version.txt`
- `README.md`
- `pyproject.toml`

Update the `CHANGELOG.md` to reflect the changes included in the release.

If also pushing changes to TEEHR-HUB, also update tags in `teehr-hub/helm-chart/config.yaml`.

Make a PR to main.  After PR has been reviewed and merged, checkout `main` pull changes and tag the commit.

.. code-block:: bash

   git checkout main
   git pull
   git tag -a v0.x.x -m "version 0.x.x"
   git push origin v0.x.x

Tagging will trigger a docker container build and push to the AWS registry for deployment to TEEHR-HUB.
Deployment to TEEHR-HUB is a manual process that requires the correct credentials.
