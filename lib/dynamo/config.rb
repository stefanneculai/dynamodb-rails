# encoding: utf-8
require "uri"
require "dynamo/config/options"

module Dynamo

  # Contains all the basic configuration information required for Dynamoid: both sensible defaults and required fields.
  module Config
    extend self
    extend Options
    include ActiveModel::Observing

    # All the default options.
    option :namespace, :default => defined?(Rails) ? "dynamo_#{Rails.application.class.parent_name}_#{Rails.env}" : "dynamo"
    option :logger, :default => defined?(Rails)
    option :access_key, :default => ''
    option :secret_key, :default => ''
    option :read_capacity, :default => 1
    option :write_capacity, :default => 1
    option :endpoint, :default => 'dynamodb.us-east-1.amazonaws.com'
    option :use_ssl, :default => true
    option :port, :default => '443'
    option :included_models, :default => []
    option :identity_map, :default => false
  end
end
