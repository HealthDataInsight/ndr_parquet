require 'active_model/type'
require 'active_record/type/unsigned_integer'

# See the README for supported Arrow column types.

# Unsupported/untested types:
# date64
# decimal
# dense_union
# dictionary
# double
# extension
# fixed_size_binary
# fixed_width
# float
# floating_point
# large_binary
# large_list
# large_string
# map
# null
# numeric
# sparse_union
# struct
# time
# time32
# time64
# timestamp
# union

ActiveModel::Type.register(:int8) { ActiveModel::Type::Integer.new(limit: 1) }
ActiveModel::Type.register(:int16) { ActiveModel::Type::Integer.new(limit: 2) }
ActiveModel::Type.register(:int32) { ActiveModel::Type::Integer.new(limit: 4) }
ActiveModel::Type.register(:int64) { ActiveModel::Type::Integer.new(limit: 8) }

ActiveModel::Type.register(:uint8) { ActiveRecord::Type::UnsignedInteger.new(limit: 1) }
ActiveModel::Type.register(:uint16) { ActiveRecord::Type::UnsignedInteger.new(limit: 2) }
ActiveModel::Type.register(:uint32) { ActiveRecord::Type::UnsignedInteger.new(limit: 4) }
ActiveModel::Type.register(:uint64) { ActiveRecord::Type::UnsignedInteger.new(limit: 8) }

ActiveModel::Type.register(:date32, ActiveModel::Type::Date)

module NdrParquet
  # This mixin casts values to Apache Arrow field types
  class TypeCasting
    SUPPORTED_DATA_TYPES = %i[binary boolean date32 decimal128 decimal256 int8 int16 int32
                              int64 integer list string uint8 uint16 uint32 uint64].freeze

    def self.cast_to_arrow_datatype(value, type, options = {})
      raise ArgumentError, "Unsupported data type: #{type}" if SUPPORTED_DATA_TYPES.exclude?(type)

      return nil if value.nil?

      case type
      when :decimal128, :decimal256
        ActiveRecord::Type::Decimal.new(**options).cast(value)
      when :list
        value.to_s.split(options.fetch(:split))
      else
        ActiveModel::Type.lookup(type).cast(value)
      end
    end

    def self.decimal_data_type?(type)
      type.is_a?(Hash) && type[:precision].present? && type[:scale].present?
    end
  end
end

# BigInteger = ActiveModel::Type::BigInteger
# Decimal = ActiveModel::Type::Decimal
# Float = ActiveModel::Type::Float
# String = ActiveModel::Type::String
# Value = ActiveModel::Type::Value

# register(:big_integer, Type::BigInteger, override: false)
# register(:datetime, Type::DateTime, override: false)
# register(:decimal, Type::Decimal, override: false)
# register(:float, Type::Float, override: false)
# register(:integer, Type::Integer, override: false)
# register(:json, Type::Json, override: false)
# register(:string, Type::String, override: false)
# register(:text, Type::Text, override: false)
# register(:time, Type::Time, override: false)

# ActiveModel::Type::BigInteger
# Type::DateTime
# ActiveModel::Type::Decimal
# ActiveModel::Type::Float
# ActiveModel::Type::ImmutableString
# ActiveRecord::Type::Json
# ActiveModel::Type::String
# Type::Time
# # ActiveModel::Type::Value
