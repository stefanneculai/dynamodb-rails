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
            hash[attribute] = incoming[attribute]
          end
          incoming.each {|attribute, value| hash[attribute.to_sym] = value unless hash.has_key? attribute }
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

    def dump
      Hash.new.tap do |hash|
        self.class.attributes.each do |attribute, options|
          if new_record?
            hash[attribute] = self.read_attribute(attribute)
          else
            hash[attribute] = self.read_attribute(attribute) if self.changed_attributes.has_key?(attribute.to_sym)
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
