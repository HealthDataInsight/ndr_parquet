# frozen_string_literal: true

require 'fileutils'
require 'test_helper'

class GeneratorTest < Minitest::Test
  def setup
    @permanent_test_files = SafePath.new('permanent_test_files')
  end

  def teardown
    FileUtils.rm 'ABC_Collection-June-2020_03.hash.mapped.parquet', force: true
    FileUtils.rm 'ABC_Collection-June-2020_03.hash.raw.parquet', force: true
  end

  def test_the_output_schemas
    generate_parquet('ABC_Collection-June-2020_03.xlsm', 'national_collection.yml')

    table = Arrow::Table.load('ABC_Collection-June-2020_03.hash.mapped.parquet')
    expected_schema = [
      %w[providercode utf8],
      %w[SQU03_5_3_1 int32],
      %w[SQU03_5_3_2 int32],
      %w[SQU03_6_2_1 decimal],
      %w[SQU03_6_2_2 list],
      %w[K1N bool],
      %w[K1M date32],
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
      ['k1m:d', 'utf8'],
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
      generator.process

      assert_equal [
        {
          path: output_path.join('ABC_Collection-June-2020_03.hash.mapped.parquet'),
          total_rows: 1
        },
        {
          path: output_path.join('ABC_Collection-June-2020_03.hash.raw.parquet'),
          total_rows: 1
        }
      ], generator.output_files
    end
  end

  def test_complex_data_types
    generate_parquet('ABC_Collection-June-2020_03.xlsm', 'national_collection.yml')

    table = Arrow::Table.load('ABC_Collection-June-2020_03.hash.mapped.parquet')
    raw_table = Arrow::Table.load('ABC_Collection-June-2020_03.hash.raw.parquet')

    # :decimal256 data type
    assert_kind_of BigDecimal, table.find_column('SQU03_6_2_1').first
    assert_equal '13.2134', raw_table.find_column('squ03_6_2_1:n').first

    # :list data type
    assert_equal %w[14a 14b 14c], table.find_column('SQU03_6_2_2').first.to_a
    assert_equal '14a,14b,14c', raw_table.find_column('squ03_6_2_2:n').first
  end

  def test_cross_worksheet_klass
    generate_parquet('cross_worksheet_spreadsheet.xlsx', 'cross_worksheet_mapping.yml')

    table = Arrow::Table.load('cross_worksheet_spreadsheet.hash.mapped.parquet')
    expected_schema = [
      %w[COMMON1 utf8],
      %w[COMMON2 utf8],
      %w[FIRST utf8],
      %w[SECOND utf8],
      %w[THIRD utf8]
    ]
    actual_schema = table.schema.fields.map { |f| [f.name, f.data_type.name] }
    assert_equal expected_schema, actual_schema
    assert_equal 7, table.columns.first.length

    raw_table = Arrow::Table.load('cross_worksheet_spreadsheet.hash.raw.parquet')
    expected_schema = [
      %w[common1 utf8],
      %w[common2 utf8],
      %w[sheet1_first utf8],
      %w[sheet1_second utf8],
      %w[sheet2_third utf8]
    ]
    actual_schema = raw_table.schema.fields.map { |f| [f.name, f.data_type.name] }
    assert_equal expected_schema, actual_schema
    assert_equal %w[Sheet1_2A_Common Sheet1_2A_Common Sheet1_3A_Common Sheet1_3A_Common
                    Sheet2_2A_Common Sheet2_3A_Common Sheet2_4A_Common],
                 raw_table.columns.first.data.to_a
    assert_equal [nil, nil, nil, nil, 'Sheet2_2C_Third', 'Sheet2_3C_Third', 'Sheet2_4C_Third'],
                 raw_table.columns.last.data.to_a
  end

  private

    def generate_parquet(source_file, table_mappings)
      generator = NdrParquet::Generator.new(@permanent_test_files.join(source_file),
                                            @permanent_test_files.join(table_mappings))
      generator.process
    end
end
