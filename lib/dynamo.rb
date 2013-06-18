require "delegate"
require "time"
require "securerandom"
require "active_support/core_ext"
require 'active_support/json'
require "active_support/inflector"
require "active_support/lazy_load_hooks"
require "active_support/time_with_zone"
require "active_model"

require 'dynamo/errors'
require 'dynamo/fields'
require 'dynamo/persistence'
require 'dynamo/dirty'
require 'dynamo/validations'
require 'dynamo/criteria'
require 'dynamo/finders'
require 'dynamo/identity_map'
require 'dynamo/config'
require 'dynamo/components'
require 'dynamo/model'
require 'dynamo/client'
require 'dynamo/version'

require 'dynamo/middleware/identity_map'

module Dynamo
  extend self

  MAX_ITEM_SIZE = 65_536

  def configure
    block_given? ? yield(Dynamo::Config) : Dynamo::Config
    Dynamo::Client.connect!
  end
  alias :config :configure

  def included_models
    @included_models ||= []
  end

end
