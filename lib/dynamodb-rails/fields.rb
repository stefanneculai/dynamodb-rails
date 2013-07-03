# encoding: utf-8
module Dynamo #:nodoc:

  # All fields on a Dynamo::Document must be explicitly defined -- if you have fields in the database that are not 
  # specified with field, then they will be ignored.
  module Fields
    extend ActiveSupport::Concern

    # Initialize the attributes we know the class has, in addition to our magic attributes: id, created_at, and updated_at.
    included do
      class_attribute :attributes

      self.attributes = {}
      field :created_at, :D
      field :updated_at, :D
    end
    
    module ClassMethods
      
      # Specify a field for a document. Its type determines how it is coerced when read in and out of the datastore: 
      # default is string, but you can also specify :integer, :float, :set, :array, :datetime, and :serialized.
      #
      # @param [Symbol] name the name of the field
      # @param [Hash] options any additional options for the field
      #
      # @since 0.2.0
      def field(name, type = 'S', options={})
        named = name.to_s
        self.attributes[name] = options.merge({:type => type})

        define_method(named) { read_attribute(named) }
        define_method("#{named}?") { !read_attribute(named).nil? }
        define_method("#{named}=") {|value| write_attribute(named, value) }
      end
    end
    
    # You can access the attributes of an object directly on its attributes method, which is by default an empty hash.
    attr_accessor :attributes
    alias :raw_attributes :attributes

    # Write an attribute on the object. Also marks the previous value as dirty.
    #
    # @param [Symbol] name the name of the field
    # @param [Object] value the value to assign to that field
    #
    # @since 0.2.0
    def write_attribute(name, value)
      if (size = value.to_s.size) > MAX_ITEM_SIZE
        Dynamo.logger.warn "DynamoDB can't store items larger than #{MAX_ITEM_SIZE} and the #{name} field has a length of #{size}."
      end

      attributes[name.to_sym] = value
    end
    alias :[]= :write_attribute

    # Read an attribute from an object.
    #
    # @param [Symbol] name the name of the field
    #
    # @since 0.2.0
    def read_attribute(name)
      attributes[name.to_sym]
    end
    alias :[] :read_attribute

    # Updates multiple attibutes at once, saving the object once the updates are complete.
    #
    # @param [Hash] attributes a hash of attributes to update
    #
    # @since 0.2.0
    def update_attributes(attributes)
      attributes.each {|attribute, value| self.write_attribute(attribute, value)} unless attributes.nil? || attributes.empty?
      save
    end

    # Update a single attribute, saving the object afterwards.
    #
    # @param [Symbol] attribute the attribute to update
    # @param [Object] value the value to assign it
    #
    # @since 0.2.0
    def update_attribute(attribute, value)
      write_attribute(attribute, value)
      save
    end

    def assign_attributes(new_attributes, options =   {}))
      new_attributes.each do |k, v|
        self.send("#{k}=", v)
      end
    end
    
    private
    
    # Automatically called during the created callback to set the created_at time.
    #
    # @since 0.2.0
    def set_created_at
      self.created_at = Time.now
    end

    # Automatically called during the save callback to set the updated_at time.
    #
    # @since 0.2.0    
    def set_updated_at
      self.updated_at = Time.now
    end
    
  end
  
end
