require 'aws-sdk'

# encoding: utf-8
module Dynamo

  # Adapter provides a generic, write-through class that abstracts variations in the underlying connections to provide a uniform response
  # to Dynamo.
  module Client
    extend self

    attr_accessor :tables

    @@client = nil

    # Establishes a connection to the underyling adapter and caches all its tables for speedier future lookups. Issued when the adapter is first called.
    #
    # @since 0.2.0
    def connect!
      if @@client.nil?
        @@client = AWS::DynamoDB::ClientV2.new(
          :access_key_id => Dynamo::Config.access_key || AWS.config::access_key_id,
          :secret_access_key => Dynamo::Config.secret_key || AWS.config::secret_access_key
        )

        self.tables = @@client.list_tables()[:table_names]
        return @@client
      end
    end

    # Find out the type of the field.
    def type_indicator(value)
      case
        when value.kind_of?(AWS::DynamoDB::Binary) then "B"
        when value.respond_to?(:to_str) then "S"
        when value.kind_of?(Numeric) then "N"
        when value.respond_to?(:each)
          indicator = nil
          value.each do |v|
            member_indicator = type_indicator(v)
            raise_error("nested collections") if
              member_indicator.to_s.size > 1
            raise_error("mixed types") if
              indicator and member_indicator != indicator
            indicator = member_indicator
          end
          indicator ||= "S"
          :"#{indicator}S"
        when value == :empty_number_set
          "NS"
        else
          raise "unsupported attribute type #{value.class}"
      end
    end

    # Convert value from response to the right type.
    def value_from_response(hash, options = {})
      (type, value) = hash.to_a.first
      type = type.to_s.upcase

      case type
        when "S"
          value
        when "SS"
          Set[*value]
        when "N"
          value.to_f
        when "NS"
          Set[*value.map {|v| v.to_f }]
        when "B"
          AWS::DynamoDB::Binary.new(value)
        when "BS"
          Set[*value.map{|v| AWS::DynamoDB::Binary.new(v) }]
      end
    end

    # Get comparison type.
    def field_comparison(key)
      options = ['EQ', 'NE', 'LE', 'LT', 'GE', 'GT', 'NOT_NULL', 'NULL', 'CONTAINS', 'NOT_CONTAINS', 'BEGINS_WITH', 'IN', 'BETWEEN']
      c_key = key.to_s.upcase.split('.').last

      return c_key if options.include?(c_key)
      'EQ'
    end

    # Get field that is compared.
    def field_from_comparison(key)
      options = ['EQ', 'NE', 'LE', 'LT', 'GE', 'GT', 'NOT_NULL', 'NULL', 'CONTAINS', 'NOT_CONTAINS', 'BEGINS_WITH', 'IN', 'BETWEEN']
      c_key = key.to_s.upcase.split('.').last

      return key.split('.')[0..-2].join('.') if options.include?(c_key)
      key
    end

    # Create table.
    def create_table(opts)
      r = {}

      # Table name.
      r[:table_name] = opts[:table_name].to_s

      # Attribute definitions.
      attribute_definitions = []
      opts[:keys].merge(opts[:indexes]).each do |key, value|
        attribute_definitions.push({:attribute_name => value[:name].to_s, :attribute_type=> value[:type].to_s.upcase})
      end
      r[:attribute_definitions] = attribute_definitions

      # Keys.
      key_schema = []
      opts[:keys].each do |key, value|
        key_schema.push({:attribute_name => value[:name].to_s, :key_type=> key.to_s.upcase})
      end
      r[:key_schema] = key_schema

      # Local secondary indexes
      local_secondary_indexes = []
      opts[:indexes].each do |index|
        secondary_index = {}
        secondary_index[:index_name] = "#{opts[:table_name].to_s}_#{index[:name].to_s}_index"
        secondary_index[:key_schema] = []
        secondary_index[:key_schema].push({:attribute_name => opts[:keys][:hash][:name].to_s, :key_type => "HASH"})
        secondary_index[:key_schema].push({:attribute_name => index[:name].to_s, :key_type => "RANGE"})
        secondary_index[:projection] = {}
        secondary_index[:projection][:projection_type] = index[:projection].to_s.upcase
        index[:projection][:non_key_attributes] = index[:non_key].map{|v| v.to_s} if index[:projection].to_s.upcase == 'INCLUDE'

        local_secondary_indexes.push(secondary_index)
      end
      r[:local_secondary_indexes] = local_secondary_indexes unless local_secondary_indexes.empty?

      # Provisioned throughput
      r[:provisioned_throughput] = opts[:throughput]

      # Do request
      @@client.create_table(r)
    end

    # Describe table.
    def describe_table (table_name)
      @@client.describe_table({:table_name => table_name.to_s})
    end

    # List tables.
    def list_tables(exclusive_start_table_name = nil, limit = nil)
      r = {}
      r[:exclusive_start_table_name] = exclusive_start_table_name unless exclusive_start_table_name.nil?
      r[:limit] = limit unless limit.nil?

      @@client.list_tables(r)
    end

    # Put new item.
    def put_item(opts, obj, conditions)
      r = {}

      # Table name.
      r[:table_name] = opts[:table_name].to_s

      # Item attributes.
      r[:item] = {}
      obj.each do |key, value|
        r[:item][key.to_s] = {type_indicator(value) => "#{value}"} unless value.nil?
      end

      # Set expectations.
      r[:expected] = {}
      conditions.each do |field, cond|
        r[:expected][field.to_s] = {}
        r[:expected][field.to_s][:exists] = cond[:exists] unless cond[:exists].nil?
        r[:expected][field.to_s][:value] = {type_indicator(cond[:value]) => "#{cond[:value]}"} unless cond[:value].nil?
      end

      # Save item.
      @@client.put_item(r)
    end

    # Update item.
    def update_item(opts, obj, conditions)

      puts conditions

      r = {}

      # Table name.
      r[:table_name] = opts[:table_name].to_s

      # Keys.
      r[:key] = {}
      opts[:keys].each do |key, value|
        r[:key][value[:name].to_s] = {type_indicator(conditions[value[:name]][:value]) => "#{conditions[value[:name]][:value].to_s}"}
      end

      # Set attributes.
      r[:attribute_updates] = {}
      obj.each do |key, value|
        r[:attribute_updates][key.to_s] = {}
        r[:attribute_updates][key.to_s][:value] = {type_indicator(value) => "#{value}"} unless value.nil?
        r[:attribute_updates][key.to_s][:action] = value.nil? ? 'DELETE' : 'PUT' # Nil values are deleted
      end

      # Set expectations.
      r[:expected] = {}
      conditions.each do |field, cond|
        r[:expected][field.to_s] = {}
        r[:expected][field.to_s][:exists] = cond[:exists] unless cond[:exists].nil?
        r[:expected][field.to_s][:value] = {type_indicator(cond[:value]) => "#{cond[:value]}"} unless cond[:value].nil?
      end

      # Return new values
      r[:return_values] = 'ALL_NEW'

      # Do request.
      response = @@client.update_item(r)

      return [] if response[:attributes].nil?
      response[:attributes].inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}
    end

    def delete_item(table_name, key)
      r = {}

      # Table name.
      r[:table_name] = table_name.to_s

      # Set keys
      r[:key] = {}
      key.each do |k, v|
        r[:key][k.to_s] = {type_indicator(v[:value]) => "#{v[:value]}"}
      end

      @@client.delete_item(r)
    end

    # Get item.
    def get_item(table_name, key, options)
      r = {}

      # Table name.
      r[:table_name] = table_name.to_s

      # Consistent read TODO add to all requests.
      r[:consistent_read] = true if options[:consistent_read] == true

      # Set keys
      r[:key] = {}
      key.each do |k, v|
        r[:key][k.to_s] = {type_indicator(v) => "#{v}"}
      end

      # Get item.
      response = @@client.get_item(r)

      return false if response[:item].nil?
      response[:item].inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}
    end

    # Batch get item.
    def batch_get_item(table_name, keys, options)
      r = {}

      # Requested items.
      r[:request_items] = {}
      r[:request_items][table_name.to_s] = {}
      r[:request_items][table_name.to_s][:consistent_read] = true if options[:consistent_read] == true

      # Set keys for each table.
      r[:request_items][table_name.to_s][:keys] = [] unless keys.empty?
      keys.each do |key|
        l_key = {}
        key.each do |k, v|
          l_key[k.to_s] = {type_indicator(v) => "#{v}"}
        end

        r[:request_items][table_name.to_s][:keys].push(l_key)
      end

      # TODO check unprocessed keys
      response = @@client.batch_get_item(r)

      return false if response[:responses][table_name.to_s].empty?
      response[:responses][table_name.to_s].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}
    end

    # Scan table for certain conditions.
    def scan(table_name, query, opts, total = 0)

      # Limit is fake. No request to be done.
      if !opts[:limit].nil? && opts[:limit].to_i <= 0
        return []
      end

      # Count the number of items that match the current conditions. If count is 0 then return [].
      count = scan(table_name, query, opts.merge({:count => true}).reject{|k, v| v if k.to_s == 'limit'}) if opts[:count].nil?
      return [] if count == 0 && !count.nil?

      r = {}

      # Table name.
      r[:table_name] = table_name.to_s

      # Ask for as many items as we care about. TODO reconsider
      # r[:limit] = opts[:limit] - total if opts[:limit]

      # Set exclusive start key if there is one.
      r[:exclusive_start_key] = opts[:last_evaluated_key] unless opts[:last_evaluated_key].nil?
      if opts[:next_token]
        r[:exclusive_start_key] = {}
        opts[:next_token].each do |k, v|
          r[:exclusive_start_key][k.to_s] = {type_indicator(v) => "#{v}"}
        end

        opts.delete(:next_token)
      end

      # Selected attrs.
      r[:attributes_to_get] = opts[:select] if opts[:select] and opts[:count].nil?

      # Set scan filter
      r[:scan_filter] = {} unless query.empty?
      query.each do |key, value|
        attr = {}
        attr[:attribute_value_list] = []
        if value.respond_to?(:each)
          value.each do |v|
            attr[:attribute_value_list].push({type_indicator(v) => "#{v}"})
          end
        else
          attr[:attribute_value_list].push({type_indicator(value) => "#{value}"})
        end
        attr[:comparison_operator] = field_comparison(key.to_s)

        r[:scan_filter][field_from_comparison(key.to_s)] = attr
      end

      r[:select] = "COUNT" if opts[:count]

      # Do scan request.
      response = @@client.scan(r)

      # Return the number of items if there was a COUNT request.
      return response[:count] if opts[:count]

      # Check if we have enough items so far
      if !opts[:limit].nil? && total + response[:count] >= opts[:limit]
        return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}[0..(opts[:limit] - total - 1)]
      else
        if response[:last_evaluated_key]
          more_results = scan(table_name, query, opts.merge({:last_evaluated_key => response[:last_evaluated_key]}), total + response[:count])
        else
          more_results = []
        end

        if opts[:limit].nil?
          return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}.concat(more_results)
        else
          return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}.concat(more_results[0..(opts[:limit] - total - 1)])
        end
      end
    end

    def query(table_name, query, opts, total = 0)
      if !opts[:limit].nil? && opts[:limit].to_i <= 0
        return []
      end

      # Count the number of items that match the current conditions. If count is 0 then return [].
      count = query(table_name, query, opts.merge({:count => true}).reject{|k, v| v if k.to_s == 'limit'}) if opts[:count].nil?
      return [] if count == 0 && !count.nil?

      r = {}
      r[:table_name] = table_name.to_s

      # Ask for as many items as we care about.
      # r[:limit] = opts[:limit] - total if opts[:limit]

      # Set exclusive start key if there is one.
      r[:exclusive_start_key] = opts[:last_evaluated_key] unless opts[:last_evaluated_key].nil?
      if opts[:next_token]
        r[:exclusive_start_key] = {}
        opts[:next_token].each do |k, v|
          r[:exclusive_start_key][k.to_s] = {type_indicator(v) => "#{v}"}
        end

        opts.delete(:next_token)
      end

      # Set scan index forward
      r[:scan_index_forward] = opts[:scan_index_forward]

      # Set index
      r[:index_name] = opts[:index_name] if opts[:index_name]

      # Selected attrs.
      r[:attributes_to_get] = opts[:select] if opts[:select] and opts[:count].nil?

      # Set query filter
      r[:key_conditions] = {} unless query.empty?
      query.each do |key, value|
        attr = {}
        attr[:attribute_value_list] = []
        if value.respond_to?(:each)
          value.each do |v|
            attr[:attribute_value_list].push({type_indicator(v) => "#{v}"})
          end
        else
          attr[:attribute_value_list].push({type_indicator(value) => "#{value}"})
        end
        attr[:comparison_operator] = field_comparison(key.to_s)

        r[:key_conditions][field_from_comparison(key.to_s)] = attr
      end

      r[:select] = "COUNT" if opts[:count]

      # Do query request.
      response = @@client.query(r)

      # Return the number of items if there was a COUNT request.
      return response[:count] if opts[:count]

      # Check if we have enough items so far
      if !opts[:limit].nil? && total + response[:count] >= opts[:limit]
        return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}[0..(opts[:limit] - total - 1)]
      else
        if response[:last_evaluated_key]
          more_results = query(table_name, query, opts.merge({:last_evaluated_key => response[:last_evaluated_key]}), total + response[:count])
        else
          more_results = []
        end

        if opts[:limit].nil?
          return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}.concat(more_results)
        else
          return response[:member].map{|item| item.inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}}.concat(more_results[0..(opts[:limit] - total - 1)])
        end
      end
    end
  end
end