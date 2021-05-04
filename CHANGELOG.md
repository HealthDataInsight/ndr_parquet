# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added:
- Adopted the ActiveModel type register - supporting binary, date, integer and unsigned integer Arrow column types and casting.
- Added support for decimal Arrow column types

### Changed:
- "klasses" that are used across table now work as expected. The "klass" parquet contains data from all relevant tables, not just the last.
- `generator.output_files` now includes the total row count as well as the generated file path

## [0.1.0] - 2021-03-29

- Initial release
