# frozen_string_literal: true

require 'fileutils'
require 'test_helper'

class GeneratorTest < Minitest::Test
  def setup
    @permanent_test_files = SafePath.new('permanent_test_files')
  end

  def test_the_output_schemas
    FileUtils.rm 'ABC_Collection-June-2020_03.hash.mapped.parquet', force: true
    FileUtils.rm 'ABC_Collection-June-2020_03.hash.raw.parquet', force: true

    source_file = @permanent_test_files.join('ABC_Collection-June-2020_03.xlsm')
    table_mappings = @permanent_test_files.join('national_collection.yml')
    generator = NdrParquet::Generator.new(source_file, table_mappings)
    generator.load

    table = Arrow::Table.load('ABC_Collection-June-2020_03.hash.mapped.parquet')
    expected_schema = [
      %w[providercode utf8],
      %w[SQU03_5_3_1 int32],
      %w[SQU03_5_3_2 int32],
      %w[SQU03_6_2_1 int32],
      %w[SQU03_6_2_2 list],
      %w[K1N bool],
      %w[K1M utf8],
      %w[K150 utf8],
      %w[K190 utf8],
      %w[F1N utf8],
      %w[F1T utf8],
      %w[F1M utf8],
      %w[F190 utf8],
      %w[P1B utf8],
      %w[P1N utf8]
    ]
    actual_schema = table.schema.fields.map { |f| [f.name, f.data_type.name] }
    assert_equal expected_schema, actual_schema

    raw_table = Arrow::Table.load('ABC_Collection-June-2020_03.hash.raw.parquet')
    expected_schema = [
      %w[filename utf8],
      ['squ03_5_3_1:n', 'utf8'],
      ['squ03_5_3_2:n', 'utf8'],
      ['squ03_6_2_1:n', 'utf8'],
      ['squ03_6_2_2:n', 'utf8'],
      ['k1n:n', 'utf8'],
      ['k1m:n', 'utf8'],
      ['k150:n', 'utf8'],
      ['k190:n', 'utf8'],
      ['f1n:n', 'utf8'],
      ['f1t:n', 'utf8'],
      ['f1m:n', 'utf8'],
      ['f190:n', 'utf8'],
      ['p1b:n', 'utf8'],
      ['p1n:n', 'utf8']
    ]
    actual_schema = raw_table.schema.fields.map { |f| [f.name, f.data_type.name] }
    assert_equal expected_schema, actual_schema
  end

  def test_a_tmpdir_output_path
    assert_equal Dir.tmpdir, SafePath.new('tmpdir').to_s

    Dir.mktmpdir do |dir|
      source_file = @permanent_test_files.join('ABC_Collection-June-2020_03.xlsm')
      table_mappings = @permanent_test_files.join('national_collection.yml')
      output_path = Pathname.new(dir)

      generator = NdrParquet::Generator.new(source_file, table_mappings, output_path)
      generator.load

      assert_equal [
        output_path.join('ABC_Collection-June-2020_03.hash.mapped.parquet'),
        output_path.join('ABC_Collection-June-2020_03.hash.raw.parquet')
      ], generator.output_files
    end
  end
end
