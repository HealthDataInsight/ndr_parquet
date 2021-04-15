# frozen_string_literal: true

require 'ndr_parquet/generator'
require 'ndr_parquet/version'

module NdrParquet
  def self.root
    ::File.expand_path('../..', __FILE__)
  end
end
