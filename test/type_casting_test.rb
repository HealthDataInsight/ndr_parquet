# frozen_string_literal: true

require 'test_helper'

class TypeCastingTest < Minitest::Test
  def test_casting_to_int32
    assert_equal 12, NdrParquet::TypeCasting.cast_to_arrow_datatype('12', :int32)
    assert_equal 13, NdrParquet::TypeCasting.cast_to_arrow_datatype(13, :int32)

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :int32)
  end

  def test_casting_to_boolean
    ['1', 'true', 1, true].each do |value|
      assert NdrParquet::TypeCasting.cast_to_arrow_datatype(value, :boolean)
    end

    ['0', 'false', 0, false].each do |value|
      refute NdrParquet::TypeCasting.cast_to_arrow_datatype(value, :boolean)
    end

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :boolean)
  end

  def test_casting_to_date32
    assert_equal Date.new(2021, 4, 28),
                 NdrParquet::TypeCasting.cast_to_arrow_datatype('2021-04-28', :date32)
    assert_equal Date.new(1970, 1, 1),
                 NdrParquet::TypeCasting.cast_to_arrow_datatype('01/01/1970', :date32)
    assert_equal Date.new(1959, 12, 31),
                 NdrParquet::TypeCasting.cast_to_arrow_datatype('1959-12-31', :date32)

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :date32)
  end

  def test_casting_to_string
    assert_equal '34', NdrParquet::TypeCasting.cast_to_arrow_datatype('34', :string)
    assert_equal '35', NdrParquet::TypeCasting.cast_to_arrow_datatype(35, :string)

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :string)
  end

  def test_casting_to_list
    list_options = { split: ';' }
    assert_equal %w[1 2 3], NdrParquet::TypeCasting.cast_to_arrow_datatype('1;2;3', :list, list_options)
    assert_empty NdrParquet::TypeCasting.cast_to_arrow_datatype('', :list, list_options)
    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :list, list_options)
  end

  def test_casting_to_decimal
    decimal_options = { precision: 3, scale: 1 }
    assert_kind_of BigDecimal, NdrParquet::TypeCasting.cast_to_arrow_datatype('110.2', :decimal256, decimal_options)
    assert_nil  NdrParquet::TypeCasting.cast_to_arrow_datatype('', :decimal256, decimal_options)
    assert_nil  NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :decimal256, decimal_options)
  end

  def test_casting_to_unknown_type
    exception = assert_raises ArgumentError do
      NdrParquet::TypeCasting.cast_to_arrow_datatype('12', :unknown_type)
    end
    assert_equal 'Unsupported data type: unknown_type', exception.message

    exception = assert_raises ArgumentError do
      NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :unknown_type)
    end
    assert_equal 'Unsupported data type: unknown_type', exception.message

    unknown_data_type_options = { precision: 3, scale: 1, data_type: :unknown_type }
    exception = assert_raises ArgumentError do
      NdrParquet::TypeCasting.cast_to_arrow_datatype('12', unknown_data_type_options)
    end
    assert_equal "Unsupported data type: #{unknown_data_type_options}", exception.message
  end
end
