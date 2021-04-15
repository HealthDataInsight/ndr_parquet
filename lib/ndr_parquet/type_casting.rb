module NdrParquet
  # This mixin casts values to Apache Arrow field types
  class TypeCasting
    def self.cast_to_arrow_datatype(value, type)
      return nil if value.nil?

      # puts "value: " + value.inspect
      # puts "type: " + type.inspect
      # puts
      case type
      when :int32
        Integer(value)
      when :boolean
        ActiveRecord::Type::Boolean.new.cast(value)
      when :string
        value.to_s
      when Hash
        value.to_s.split(type[:split]) if list_data_type?(type)
      else
        raise "Unrecognised type: #{type.inspect}"
      end
    end

    def self.list_data_type?(type)
      type.is_a?(Hash) && type[:split].present?
    end
  end
end

# ActiveModel::Type::BigInteger
# ActiveModel::Type::Binary
# ActiveModel::Type::Boolean
# Type::Date
# Type::DateTime
# ActiveModel::Type::Decimal
# ActiveModel::Type::Float
# ActiveModel::Type::Integer
# ActiveModel::Type::ImmutableString
# ActiveRecord::Type::Json
# ActiveModel::Type::String
# Type::Time
# # ActiveModel::Type::Value
