# frozen_string_literal: true

require 'ndr_parquet/generator'
require 'ndr_parquet/version'

begin
  # Include NdrParquet::S3Wrapper if Aws::S3::Client available
  require 'aws-sdk-s3'

  require 'ndr_parquet/s3_wrapper'
rescue LoadError
  # do nothing if gem unavailable
end

# This exposes the root folder for filesystem paths
module NdrParquet
  def self.root
    ::File.expand_path('..', __dir__)
  end
end
