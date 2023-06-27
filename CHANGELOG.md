# Changelog

## [0.1.3] - 27.06.2023

### Added

- Added support for pydantic models and similar type-safety frameworks

### Changed

- Performance improvements
- Thread safety improvements

## [0.1.2] - 2023-05-26

### Fixed

- Switched from Tornado 6.2 to 6.3.2 due to a vulnerability.

## [0.1.1] - 2023-04-17

### Added

- Added PyODBCache for inter-process caching functionality.
- Added extended documentation

### Changed

- Many performance improvements
- Reaching max recursion depth no longer causes an error
- Types beyond max recursion depth are now pickled and saved as blobs

### Fixed

- Stabilized Multi-Threading and Multi-Processing
  (still causes Problems under specific circumstances)


## [0.1.0] - 2023-03-31

### Added

- First fully working Version
- Implemented PyODB Main Functionalities
