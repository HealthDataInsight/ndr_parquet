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

  def test_casting_to_string
    assert_equal '34', NdrParquet::TypeCasting.cast_to_arrow_datatype('34', :string)
    assert_equal '35', NdrParquet::TypeCasting.cast_to_arrow_datatype(35, :string)

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :string)
  end

  def test_casting_to_unknown_type
    assert_raises StandardError do
      NdrParquet::TypeCasting.cast_to_arrow_datatype('12', :unknown_type)
    end

    assert_nil NdrParquet::TypeCasting.cast_to_arrow_datatype(nil, :unknown_type)
  end
end
