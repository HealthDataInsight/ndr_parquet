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

        def parquet_filename(klass, mode)
          basename = File.basename(@filename, File.extname(@filename))

          @output_path.join("#{basename}.#{klass.underscore}.#{mode}.parquet")
        end

        def mapped_arrow_schema(klass)
          Arrow::Schema.new(
            @arrow_column_types[klass].to_a.map do |fieldname, definition|
              type = definition[:type]
              options = definition[:options]

              case type
              when :list
                list_field_type = options.except(:split)
                Arrow::Field.new(name: fieldname, type: :list, field: list_field_type)
              when :decimal128, :decimal256
                Arrow::Field.new(name: fieldname, type: type, **options)
              else
                Arrow::Field.new(fieldname, type)
              end
            end
          )
        end

        def save_mapped_parquet_files(klass_mapped_hashes)
          klass_mapped_hashes.each do |klass, mapped_hashes|
            # Save the mapped parquet file
            schema = mapped_arrow_schema(klass)
            rows = mapped_hashes.map do |mapped_hash|
              @arrow_column_types[klass].to_a.map do |fieldname, definition|
                # Convert the fields to an Arrow table "row", with appropriate casting.
                # Unfortunately, Arrow can't do it implicitly.
                value = mapped_hash[fieldname]
                TypeCasting.cast_to_arrow_datatype(value, definition[:type], definition[:options])
              end
            end

            save_and_log_parquet_file(klass, schema, rows, :mapped)
          end
        end

        def save_raw_parquet_files(klass_rawtext_hashes)
          klass_rawtext_hashes.each do |klass, rawtext_hashes|
            # Save the rawtext parquet file
            schema = Arrow::Schema.new(
              @rawtext_column_names[klass].to_a.map do |fieldname|
                Arrow::Field.new(fieldname, :string)
              end
            )
            rows = rawtext_hashes.map do |rawtext_hash|
              @rawtext_column_names[klass].to_a.map { |fieldname| rawtext_hash[fieldname] }
            end

            save_and_log_parquet_file(klass, schema, rows, :raw)
          end
        end

        def save_and_log_parquet_file(klass, schema, rows, mode)
          arrow_table = Arrow::Table.new(schema, rows)
          output_filename = parquet_filename(klass, mode)
          arrow_table.save(output_filename)

          @output_files ||= []
          @output_files << {
            path: output_filename,
            total_rows: rows.length
          }
        end
    end
  end
end
