require "delegate"
require "time"
require "securerandom"
require "active_support/core_ext"
require 'active_support/json'
require "active_support/inflector"
require "active_support/lazy_load_hooks"
require "active_support/time_with_zone"
require "active_model"
require 'pp'

require 'dynamodb-rails/errors'
require 'dynamodb-rails/fields'
require 'dynamodb-rails/persistence'
require 'dynamodb-rails/dirty'
require 'dynamodb-rails/validations'
require 'dynamodb-rails/criteria'
require 'dynamodb-rails/finders'
require 'dynamodb-rails/identity_map'
require 'dynamodb-rails/config'
require 'dynamodb-rails/components'
require 'dynamodb-rails/model'
require 'dynamodb-rails/client'
require 'dynamodb-rails/version'

require 'dynamodb-rails/middleware/identity_map'

module Dynamo
  extend self

  MAX_ITEM_SIZE = 65_536

  def configure
    block_given? ? yield(Dynamo::Config) : Dynamo::Config
    Dynamo::Client.connect!
  end
  alias :config :configure

  def logger
    Dynamo::Config.logger
  end

  def included_models
    @included_models ||= []
  end

end
