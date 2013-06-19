# encoding: utf-8
module Dynamo

  # All modules that a Document is composed of are defined in this
  # module, to keep the document class from getting too cluttered.
  module Components
    extend ActiveSupport::Concern

    included do
      extend ActiveModel::Translation
      extend ActiveModel::Callbacks

      define_model_callbacks :create, :save, :destroy, :initialize, :update

      before_create :set_created_at
      before_save :set_updated_at
    end

    include ActiveModel::AttributeMethods
    include ActiveModel::Conversion
    include ActiveModel::MassAssignmentSecurity
    include ActiveModel::Naming
    include ActiveModel::Observing
    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml
    include Dynamo::Fields
    include Dynamo::Persistence
    include Dynamo::Finders
    include Dynamo::Criteria
    include Dynamo::Validations
    include Dynamo::IdentityMap
    include Dynamo::Dirty
  end
end