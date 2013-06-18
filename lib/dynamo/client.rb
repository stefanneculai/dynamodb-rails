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
          :access_key_id => Dynamo::Config.access_key,
          :secret_access_key => Dynamo::Config.secret_key,
        )

        self.tables = @@client.list_tables()[:table_names]
        return @@client
      end
    end

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

    def create_table(opts)
      r = {}
      attribute_definitions = []
      opts[:keys].each do |key, value|
        attribute_definitions.push({:attribute_name => value[:name].to_s, :attribute_type=> value[:type].to_s.upcase})
      end
      opts[:indexes].each do |index|
        attribute_definitions.push({:attribute_name => index[:field].to_s, :attribute_type=> index[:type].to_s.upcase})
      end
      r[:attribute_definitions] = attribute_definitions

      r[:table_name] = opts[:table_name].to_s

      key_schema = []
      opts[:keys].each do |key, value|
        key_schema.push({:attribute_name => value[:name].to_s, :key_type=> key.to_s.upcase})
      end
      r[:key_schema] = key_schema

      local_secondary_indexes = []
      opts[:indexes].each do |index|
        secondary_index = {}
        secondary_index[:index_name] = "#{opts[:table_name.to_s]}_#{index[:field].to_s}_index"
        secondary_index[:key_schema] = []
        secondary_index[:key_schema].push({:attribute_name => opts[:keys][:hash][:name].to_s, :key_type => "HASH"})
        secondary_index[:key_schema].push({:attribute_name => index[:field].to_s, :key_type => "RANGE"})
        secondary_index[:projection] = {}
        secondary_index[:projection][:projection_type] = index[:projection].to_s.upcase
        index[:projection][:non_key_attributes] = index[:non_key].map{|v| v.to_s} if index[:projection].to_s.upcase == 'INCLUDE'

        local_secondary_indexes.push(secondary_index)
      end
      r[:local_secondary_indexes] = local_secondary_indexes unless local_secondary_indexes.empty?

      r[:provisioned_throughput] = opts[:throughput]

      @@client.create_table(r)
    end

    def describe_table (table_name)
      @@client.describe_table({:table_name => table_name.to_s})
    end

    def list_tables(exclusive_start_table_name = nil, limit = nil)
      r = {}
      r[:exclusive_start_table_name] = exclusive_start_table_name unless exclusive_start_table_name.nil?
      r[:limit] = limit unless limit.nil?

      @@client.list_tables(r)
    end

    def put_item(opts, obj, conditions)
      r = {}
      r[:table_name] = opts[:table_name].to_s

      r[:item] = {}
      obj.each do |key, value|
        r[:item][key.to_s] = {type_indicator(value) => "#{value}"} unless value.nil?
      end

      r[:expected] = {}
      conditions.each do |field, cond|
        r[:expected][field.to_s] = {}
        r[:expected][field.to_s][:exists] = cond[:exists] unless cond[:exists].nil?
        r[:expected][field.to_s][:value] = {type_indicator(cond[:value]) => "#{cond[:value]}"} unless cond[:value].nil?
      end

      @@client.put_item(r)
    end

    def update_item(opts, obj, conditions)
      r = {}
      r[:table_name] = opts[:table_name].to_s

      r[:key] = {}
      opts[:keys].each do |key, value|
        r[:key][value[:name].to_s] = {type_indicator(conditions[value[:name]][:value]) => "#{conditions[value[:name]][:value].to_s}"}
      end

      r[:attribute_updates] = {}
      obj.each do |key, value|
        r[:attribute_updates][key.to_s] = {}
        r[:attribute_updates][key.to_s][:value] = {type_indicator(value) => "#{value}"} unless value.nil?
        r[:attribute_updates][key.to_s][:action] = 'PUT'
      end

      r[:expected] = {}
      conditions.each do |field, cond|
        r[:expected][field.to_s] = {}
        r[:expected][field.to_s][:exists] = cond[:exists] unless cond[:exists].nil?
        r[:expected][field.to_s][:value] = {type_indicator(cond[:value]) => "#{cond[:value]}"} unless cond[:value].nil?
      end

      r[:return_values] = 'ALL_NEW'

      @@client.update_item(r)
    end

    def get_item(table_name, key, options)
      r = {}
      r[:table_name] = table_name.to_s
      r[:consistent_read] = true if options[:consistent_read] == true

      r[:key] = {}
      key.each do |k, v|
        r[:key][k.to_s] = {type_indicator(v) => "#{v}"}
      end

      r[:return_consumed_capacity] = "TOTAL"

      response = @@client.get_item(r)

      return false if response[:item].nil?
      response[:item].each do |k, v|
        puts "#{k}: #{v}"
      end

      response[:item].inject({}){|e,(k, v)| e[k.to_sym] = value_from_response(v); e}
    end
  end
end