require 'orm_adapter'

# encoding: utf-8
module Dynamo #:nodoc:

  # This is the base module for all domain objects that need to be persisted to
  # the database as documents.
  module Model
    extend ActiveSupport::Concern
    include Dynamo::Components

    included do
      class_attribute :options, :read_only_attributes
      self.options = {}
      self.options[:table_name] = self.name.split('::').last.downcase.pluralize
      self.options[:throughput] = {
        :read_capacity_units => Dynamo::Config.read_capacity,
        :write_capacity_units => Dynamo::Config.write_capacity
      }
      self.options[:indexes] = []
      self.options[:keys] = {:hash => {:name => :id, :type => :S}}

      self.read_only_attributes = []

      Dynamo::Config.included_models << self
    end

    module ClassMethods
      include OrmAdapter::ToAdapter

      # Set up table options, including naming it whatever you want, setting the id key, and manually overriding read and
      # write capacity.
      #
      # @param [Hash] options options to pass for this table
      # @option options [Symbol] :name the name for the table; this still gets namespaced
      #
      # @since 0.4.0
      def table(name)
        self.options[:table_name] = name
      end

      def key(key_type,name, type)
        self.options[:keys][key_type] = {:name => name, :type => type}
      end

      def hash_key
        self.options[:keys][:hash][:name] unless self.options[:keys][:hash].nil?
      end

      def range_key
        self.options[:keys][:range][:name] unless self.options[:keys][:range].nil?
      end

      def attr_readonly(*read_only_attributes)
        self.read_only_attributes.concat read_only_attributes.map(&:to_s)
      end

      def provisioned_throughput (read_units, write_units)
        self.options[:throughput][:read_capacity_units] = read_units
        self.options[:throughput][:write_capacity_units] = write_units
      end

      # Returns the read_capacity for this table.
      #
      # @since 0.4.0
      def read_capacity
        self.options[:throughput][:read_capacity_units] || Dynamo::Config.read_capacity
      end

      # Returns the write_capacity for this table.
      #
      # @since 0.4.0
      def write_capacity
        self.options[:throughput][:read_capacity_units] || Dynamo::Config.write_capacity
      end

      # secondary_index :atrribute, :type,
      def index(name, type, projection=:ALL, non_key=[])
        self.options[:indexes].push({:field => name, :type=> type, :projection => projection, :non_key => non_key})
      end

      # Initialize a new object and immediately save it to the database.
      #
      # @param [Hash] attrs Attributes with which to create the object.
      #
      # @return [Dynamo::Document] the saved document
      #
      # @since 0.2.0
      def create(attrs = {})
        new(attrs).tap(&:save)
      end

      # Initialize a new object and immediately save it to the database. Raise an exception if persistence failed.
      #
      # @param [Hash] attrs Attributes with which to create the object.
      #
      # @return [Dynamo::Document] the saved document
      #
      # @since 0.2.0
      def create!(attrs = {})
        new(attrs).tap(&:save!)
      end

      # Initialize a new object.
      #
      # @param [Hash] attrs Attributes with which to create the object.
      #
      # @return [Dynamo::Document] the new document
      #
      # @since 0.2.0
      def build(attrs = {})
        new(attrs)
      end

      # Does this object exist?
      #
      # @param [Mixed] id_or_conditions the id of the object or a hash with the options to filter from.
      #
      # @return [Boolean] true/false
      #
      # @since 0.2.0
      def exists?(id_or_conditions = {})
        case id_or_conditions
          when Hash then ! where(id_or_conditions).all.empty?
          else !! find(id_or_conditions)
        end
      end
    end

    # Initialize a new object.
    #
    # @param [Hash] attrs Attributes with which to create the object.
    #
    # @return [Dynamo::Document] the new document
    #
    # @since 0.2.0
    def initialize(attrs = {})
      run_callbacks :initialize do
        #self.class.send(:field, self.class.hash_key) unless self.respond_to?(self.class.hash_key)
        #self.class.send(:field, self.class.range_key) unless self.respond_to?(self.class.range_key)

        @new_record = true
        @attributes ||= {}

        load(attrs)
      end
    end

    def load(attrs)
      self.class.undump(attrs).each {|key, value| send "#{key}=", value }
    end

    # An object is equal to another object if their ids are equal.
    #
    # @since 0.2.0
    def ==(other)
      if self.class.identity_map_on?
        super
      else
        return false if other.nil?
        other.is_a?(Dynamo::Model) && self.hash_key == other.hash_key && self.range_key == other.range_key
      end
    end

    def eql?(other)
      self == other
    end

    # Reload an object from the database -- if you suspect the object has changed in the datastore and you need those
    # changes to be reflected immediately, you would call this method.
    #
    # @return [Dynamo::Document] the document this method was called on
    #
    # @since 0.2.0
    def reload
      #range_key_value = range_value ? dumped_range_value : nil
      #self.attributes = self.class.find(hash_key, :range_key => range_key_value).attributes
      #@associations.values.each(&:reset)
      self
    end

    def hash_key
      self.options[:keys][:hash][:name] unless self.options[:keys][:hash].nil?
    end

    def range_key
      self.options[:keys][:range][:name] unless self.options[:keys][:range].nil?
    end

    # Assign an object's hash key, regardless of what it might be called to the object.
    #
    # @since 0.4.0
    def hash_key=(value)
      @attributes[self.class.hash_key] = value
    end

    def hash_key_value
      @attributes[self.class.hash_key]
    end

    def range_key_value
      @attributes[self.class.range_key]
    end

    class OrmAdapter < ::OrmAdapter::Base
      # get a list of column names for a given class
      def column_names
        klass.attributes.keys
      end

      # @see OrmAdapter::Base#get!
      def get!(id)
        klass.find_by_id(wrap_key(id)) || raise(Dynamo::Model::ModelNotFound)
      end

      # @see OrmAdapter::Base#get
      def get(id)
        klass.where(klass.hash_key => wrap_key(id)).first
      end

      # @see OrmAdapter::Base#find_first
      def find_first(options = {})
        conditions, order = extract_conditions!(options)
#        klass.limit(1).where(conditions_to_fields(conditions)).order_by(order).first
        klass.where(conditions_to_fields(conditions)).limit(1).first
      end

      # @see OrmAdapter::Base#find_all
      def find_all(options = {})
        conditions, order, limit, offset = extract_conditions!(options)
#        klass.where(conditions_to_fields(conditions)).order_by(order).limit(limit).offset(offset)
        klass.where(conditions_to_fields(conditions)).limit(limit)
      end

      # @see OrmAdapter::Base#create!
      def create!(attributes = {})
        klass.create!(attributes)
      end

      # @see OrmAdapter::Base#destroy
      def destroy(object)
        object.destroy if valid_object?(object)
      end

      protected

      def conditions_to_fields(conditions)
        conditions.inject({}) do |fields, (key, value)|
          if value.is_a?(Dynamo::Model) && assoc_key = association_key(key)
            fields.merge(assoc_key => Set[value.id])
          else
            fields.merge(key => value)
          end
        end
      end

      def association_key(key)
        k = "#{key}_ids"
        column_names.find{|c| c == k || c == k.to_sym}
      end
    end

  end
end
