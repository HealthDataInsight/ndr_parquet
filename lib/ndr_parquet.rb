# frozen_string_literal: true

require 'ndr_parquet/generator'
require 'ndr_parquet/version'

# This exposes the root folder for filesystem paths
module NdrParquet
  def self.root
    ::File.expand_path('..', __dir__)
  end
end
