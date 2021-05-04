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
                list_field_type = type.except(:data_type, :split)
                Arrow::Field.new(name: fieldname, type: :list, field: list_field_type)
              elsif TypeCasting.decimal_data_type?(type)
                Arrow::Field.new(name: fieldname, type: type[:data_type], **type.except(:data_type))
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

        def save_raw_parquet_files(rawtexts)
          rawtexts.each do |klass, rawtext_hashes|
            # Save the rawtext parquet file
            schema = Arrow::Schema.new(
              @rawtext_column_names[klass].to_a.map do |fieldname|
                Arrow::Field.new(fieldname, :string)
              end
            )
            rows = rawtext_hashes.map do |rawtext_hash|
              @rawtext_column_names[klass].to_a.map { |fieldname| rawtext_hash[fieldname] }
            end

            raw_arrow_table = Arrow::Table.new(schema, rows)
            output_filename = parquet_filename(klass, :raw)
            raw_arrow_table.save(output_filename)

            @output_files ||= []
            @output_files << output_filename
          end
        end
    end
  end
end
