require 'securerandom'

# encoding: utf-8
module Dynamo

  # Persistence is responsible for dumping objects to and marshalling objects from the datastore. It tries to reserialize
  # values to be of the same type as when they were passed in, based on the fields in the class.
  module Persistence
    extend ActiveSupport::Concern

    attr_accessor :new_record
    alias :new_record? :new_record

    module ClassMethods

      def table_name
        return options[:table_name]
      end

      def options
        return options
      end

      # Creates a table.
      #
      # @param [Hash] options options to pass for table creation
      # @option options [Symbol] :id the id field for the table
      # @option options [Symbol] :table_name the actual name for the table
      # @option options [Integer] :read_capacity set the read capacity for the table; does not work on existing tables
      # @option options [Integer] :write_capacity set the write capacity for the table; does not work on existing tables
      # @option options [Hash] {range_key => :type} a hash of the name of the range key and a symbol of its type
      #
      # @since 0.4.0
      def create_table
        return true if table_exists?(table_name)

        Dynamo::Client.tables << table_name if Dynamo::Client.create_table(options)
      end

      # Does a table with this name exist?
      #
      # @since 0.2.0
      def table_exists?(table_name)
        Dynamo::Client.tables ? Dynamo::Client.tables.include?(table_name.to_s) : false
      end

      def from_database(attrs = {})
        new(attrs).tap { |r| r.new_record = false }
      end

      # Undump an object into a hash, converting each type from a string representation of itself into the type specified by the field.
      #
      # @since 0.2.0
      def undump(incoming = nil)
        incoming = (incoming || {}).symbolize_keys
        Hash.new.tap do |hash|
          self.attributes.each do |attribute, options|
            hash[attribute] = undump_field(incoming[attribute], options)
          end
          incoming.each {|attribute, value| hash[attribute] = value unless hash.has_key? attribute }
        end
      end

      # Undump a value for a given type. Given a string, it'll determine (based on the type provided) whether to turn it into a
      # string, integer, float, set, array, datetime, or serialized return value.
      #
      # @since 0.2.0
      def undump_field(value, options)
        if value.nil? && (default_value = options[:default])
          value = default_value.respond_to?(:call) ? default_value.call : default_value
        else
          return if value.nil? || (value.respond_to?(:empty?) && value.empty?)
        end

        if options[:type]
          case options[:type]
            when :string
              value.to_s
            when :integer
              value.to_i
            when :float
              value.to_f
            when :set, :array
              if value.is_a?(Set) || value.is_a?(Array)
                value
              else
                Set[value]
              end
            when :datetime
              if value.is_a?(Date) || value.is_a?(DateTime) || value.is_a?(Time)
                value
              else
                Time.at(value).to_datetime
              end
            when :serialized
              if value.is_a?(String)
                options[:serializer] ? options[:serializer].load(value) : YAML.load(value)
              else
                value
              end
            when :boolean
              # persisted as 't', but because undump is called during initialize it can come in as true
              if value == 't' || value == true
                true
              elsif value == 'f' || value == false
                false
              else
                raise ArgumentError, "Boolean column neither true nor false"
              end
            else
              raise ArgumentError, "Unknown type #{options[:type]}"
          end
        end
      end
    end

    # Set updated_at and any passed in field to current DateTime. Useful for things like last_login_at, etc.
    #
    def touch(name = nil)
      now = DateTime.now
      self.updated_at = now.to_f
      attributes[name.to_sym] = now if name
      save
    end

    # Is this object persisted in the datastore? Required for some ActiveModel integration stuff.
    #
    # @since 0.2.0
    def persisted?
      !new_record?
    end

    # Run the callbacks and then persist this object in the datastore.
    #
    # @since 0.2.0
    def save(options = {})
      self.class.create_table
      
      if new_record?
        conditions = { self.options[:keys][:hash][:name] => {:exists => false} }
        conditions[self.options[:keys][:range][:name]] = {:exists => false} if self.options[:keys][:range]

        run_callbacks(:create) { persist(conditions) }
      else
        persist
      end

      self
    end

    #
    # update!() will increment the lock_version if the table has the column, but will not check it. Thus, a concurrent save will 
    # never cause an update! to fail, but an update! may cause a concurrent save to fail. 
    #
    #
    def update!(conditions = {}, &block)
      run_callbacks(:update) do
        if self.changed_attributes.has_key?(self.hash_key.to_s)
          conditions[self.hash_key] = {:value => self.changed_attributes[self.hash_key.to_s]}
        else
          conditions[self.range_key] = {:value => self.hash_key_value}
        end

        if self.range_key
          if self.changed_attributes.has_key?(self.range_key.to_s)
            conditions[self.range_key] = {:value => self.changed_attributes[self.range_key.to_s]}
          else
            conditions[self.range_key] = {:value => self.range_key_value}
          end
        end

        Dynamo::Client.update_item(options, self.dump, conditions) # do |t|
        #  yield t
        #end
        #load(new_attrs)
      end
    end

    def update(conditions = {}, &block)
      update!(conditions, &block)
      true
    rescue Dynamo::Errors::ConditionalCheckFailedException
      false
    end

    # Delete this object, but only after running callbacks for it.
    #
    # @since 0.2.0
    def destroy
      run_callbacks(:destroy) do
    #    self.delete
      end
      self
    end

    # Delete this object from the datastore and all indexes.
    #
    # @since 0.2.0
    def delete
      # options = range_key ? {:range_key => dump_field(self.read_attribute(range_key), self.class.attributes[range_key])} : {}
      # Dynamo::Adapter.delete(self.class.table_name, self.hash_key, options)
    end

    # Determine how to dump this field. Given a value, it'll determine how to turn it into a value that can be
    # persisted into the datastore.
    #
    # @since 0.2.0
    def dump_field(value, options)
      return if value.nil? || (value.respond_to?(:empty?) && value.empty?)

      if options[:type]
        case options[:type]
          when :string
            value.to_s
          when :integer
            value.to_i
          when :float
            value.to_f
          when :set, :array
            if value.is_a?(Set) || value.is_a?(Array)
              value
            else
              Set[value]
            end
          when :datetime
            value.to_time.to_f
          when :serialized
            options[:serializer] ? options[:serializer].dump(value) : value.to_yaml
          when :boolean
            value.to_s[0]
          else
            value
        end
      else
        value
      end
    end

    def dump
      Hash.new.tap do |hash|
        self.class.attributes.each do |attribute, options|
          if new_record?
            hash[attribute] = dump_field(self.read_attribute(attribute), options)
          else
            hash[attribute] = dump_field(self.read_attribute(attribute), options) if self.changed_attributes.has_key?(attribute.to_sym)
          end
        end
      end
    end

    private
    
    # Persist the object into the datastore. Assign it an id first if it doesn't have one; then afterwards,
    # save its indexes.
    #
    # @since 0.2.0
    def persist(conditions = nil)
      run_callbacks(:save) do

        self.changed_attributes.symbolize_keys!

        self.hash_key = SecureRandom.uuid if self.hash_key_value.nil? || self.hash_key_value.blank?

        conditions ||= {}

        if(new_record?)
          response = Dynamo::Client.put_item(options, self.dump, conditions)
        else
          if self.changed_attributes.has_key?(self.hash_key)
            conditions[self.hash_key] = {:value => self.changed_attributes[self.hash_key]}
          else
            conditions[self.hash_key] = {:value => self.hash_key_value}
          end

          if self.range_key
            if self.changed_attributes.has_key?(self.range_key)
              conditions[self.range_key] = {:value => self.changed_attributes[self.range_key]}
            else
              conditions[self.range_key] = {:value => self.range_key_value}
            end
          end

          response = Dynamo::Client.update_item(options, self.dump, conditions)
        end

        @new_record = false
        response
      end
    end
  end

end
