module NdrParquet
  class Generator
    # Generator mixin to create and save Arrow tables as parquet files
    module ParquetFileHelper
      def self.included(base)
        base.class_eval do
          attr_reader :output_files
        end
      end

      private

        def parquet_filename(klass, type)
          basename = File.basename(@filename, File.extname(@filename))

          @output_path.join("#{basename}.#{klass.underscore}.#{type}.parquet")
        end

        def arrow_schemas(table)
          schemas = {}

          arrow_field_types(table).each do |klass, field_type_hash|
            field_array = field_type_hash.map do |fieldname, type|
              if TypeCasting.list_data_type?(type)
                Arrow::Field.new(name: fieldname, type: :list, field: type.except(:split))
              elsif TypeCasting.decimal_data_type?(type)
                Arrow::Field.new(name: fieldname, type: "decimal#{type[:bits]}".to_sym, **type)
              else
                Arrow::Field.new(fieldname, type)
              end
            end
            schemas[klass] = Arrow::Schema.new(field_array)
          end

          schemas
        end

        def save_mapped_parquet_files(output_rows, table)
          schemas = arrow_schemas(table)

          output_rows.each do |klass, records|
            # Save the mapped parquet file
            arrow_table = Arrow::Table.new(schemas[klass], records)

            output_filename = parquet_filename(klass, :mapped)
            arrow_table.save(output_filename)
            @output_files ||= []
            @output_files << output_filename
          end
        end

        def save_raw_parquet_files(rawtext_rows, rawtext_column_names)
          rawtext_rows.each do |klass, _records|
            # Save the rawtext parquet file
            raw_schema = Arrow::Schema.new(
              rawtext_column_names[klass].map do |fieldname|
                Arrow::Field.new(fieldname, :string)
              end
            )
            raw_arrow_table = Arrow::Table.new(raw_schema, rawtext_rows[klass])

            output_filename = parquet_filename(klass, :raw)
            raw_arrow_table.save(output_filename)
            @output_files ||= []
            @output_files << output_filename
          end
        end
    end
  end
end
