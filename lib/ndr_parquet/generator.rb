# frozen_string_literal: true

require 'ndr_import'
require 'ndr_import/universal_importer_helper'
require 'ndr_parquet/type_casting'
require 'ndr_parquet/generator/parquet_file_helper'
require 'parquet'
require 'pathname'

module NdrParquet
  # Reads file using NdrImport ETL logic and creates parquet file(s)
  class Generator
    include NdrImport::UniversalImporterHelper
    include NdrParquet::Generator::ParquetFileHelper

    def initialize(filename, table_mappings, output_path = '')
      @filename = filename
      @table_mappings = YAML.load_file table_mappings
      @output_path = Pathname.new(output_path)

      ensure_all_mappings_are_tables
    end

    def load
      record_count = 0
      extract(@filename).each do |table, rows|
        arrow_fields = arrow_field_types(table)
        rawtext_column_names = rawtext_names(table)
        output_rows = {}
        rawtext_rows = {}

        table.transform(rows).each_slice(50) do |records|
          records.each do |(instance, fields, _index)|
            klass = instance.split('#').first

            # Convert the fields to an Arrow table "row", with appropriate casting.
            # Unfortunately, Arrow can't do it implicitly.
            output_rows[klass] ||= []
            row = arrow_fields[klass].map do |fieldname, type|
              value = fields[fieldname]
              TypeCasting.cast_to_arrow_datatype(value, type)
            end
            output_rows[klass] << row

            rawtext_rows[klass] ||= []
            rawtext_row = rawtext_column_names[klass].map do |rawtext_column_name|
              fields[:rawtext][rawtext_column_name]
            end
            rawtext_rows[klass] << rawtext_row
          end
          record_count += records.count
        end

        save_mapped_parquet_files(output_rows, table)
        save_raw_parquet_files(rawtext_rows, rawtext_column_names)
      end
      # puts "Inserted #{record_count} records in total"
    end

    private

      def ensure_all_mappings_are_tables
        return if @table_mappings.all? { |table| table.is_a?(NdrImport::Table) }

        raise 'Mappings must be inherit from NdrImport::Table'
      end

      def unzip_path
        @unzip_path ||= SafePath.new('unzip_path')
      end

      def get_notifier(_value); end

      def arrow_field_types(table)
        field_types = {}

        masked_mappings = table.send(:masked_mappings)
        masked_mappings.each do |instance, columns|
          klass = instance.split('#').first
          field_types[klass] ||= {}

          columns.each do |column|
            next if column['mappings'].nil? || column['mappings'] == []

            column['mappings'].each do |mapping|
              field = mapping['field']
              arrow_data_type = mapping['arrow_data_type'] || :string
              if arrow_data_type == :list
                field_types[klass][field] = mapping.fetch('arrow_list_field').symbolize_keys
              else
                field_types[klass][field] = arrow_data_type
              end
            end
          end
        end

        field_types
      end

      def rawtext_names(table)
        names = {}

        masked_mappings = table.send(:masked_mappings)
        masked_mappings.each do |instance, columns|
          klass = instance.split('#').first

          names[klass] ||= []
          columns.each do |column|
            rawtext_column_name = column[NdrImport::Mapper::Strings::RAWTEXT_NAME] ||
                                  column[NdrImport::Mapper::Strings::COLUMN]

            next if rawtext_column_name.nil?

            names[klass] << rawtext_column_name.downcase
          end
        end

        names
      end
  end
end
