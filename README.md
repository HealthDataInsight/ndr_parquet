# NdrParquet [![Build Status](https://github.com/timgentry/ndr_parquet/workflows/Test/badge.svg)](https://github.com/timgentry/ndr_parquet/actions?query=workflow%3Atest)

Based on [Ollie Tulloch](https://github.com/ollietulloch)'s boilerplate MongoDB example, this generates parquet file(s) using [NdrImport](https://github.com/PublicHealthEngland/ndr_import) and [Apache Arrow](https://arrow.apache.org).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ndr_parquet', git: 'https://github.com/timgentry/ndr_parquet', branch: 'main'
```

And then execute:

    $ bundle install

## Usage

Below is an example that extracts data from a spreadsheet and transforms it into to a `raw` and `mapped` parquet files, defined by their `Hash` "klass":

```ruby
require 'ndr_parquet'

source_file = SafePath.new(...).join('ABC_Collection-June-2020_03.xlsm')
table_mappings = SafePath.new(...).join('national_collection.yml')
generator = NdrParquet::Generator.new(source_file, table_mappings)
generator.load
```

See `test/ndr_parquet_test.rb` for a more complete working example.

More information on the workings of the mapper are available in the [wiki](https://github.com/PublicHealthEngland/ndr_import/wiki).

## Type definitions

Columns are always cast as strings in `raw` parquet files. To specify a column type in the `mapped` parquet files, add `arrow_data_type` to field mappings. For example:

```yaml
  - column: TotalNum
    mappings:
    - field: TOTAL_NUMBER
      arrow_data_type: :int32
```

defines an integer `TOTAL_NUMBER` column in the `mapped` parquet files, with values cast appropriately for that type.

Currently supported Arrow column types are:

* :binary
* :boolean
* :date32
* :decimal128, :decimal256
* :int8, :int16, :int32, :int64 and :integer
* :string [default]
* :uint8, :uint16, :uint32 and :uint64

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/timgentry/ndr_parquet. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/timgentry/ndr_parquet/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the NdrParquet::Generator project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/timgentry/ndr_parquet/blob/main/CODE_OF_CONDUCT.md).
