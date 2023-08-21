# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.2.1] - 2023-08-21

### Added

- Nothing

### Changed

- Updated TEEHR version in TEEHR-HUB to v0.2.1
- Converts nwm feature id's to numpy array in loading

## [0.2.0] - 2023-08-17

### Added

- This changelog

### Changed

- Loading directory refactor changed import paths to loading modules
- Changed directory of `generate_weights.py` utility
- Replaced NWM config parameter dictionary with pydantic models
- NWM reference time  used by TEEHR is now taken directly from the file name rather than the "reference time" embedded in the file
- Use of the term `run` updated to `configuration` for NWM

## [0.1.3] - 2023-06-17

### Added

- Initial release