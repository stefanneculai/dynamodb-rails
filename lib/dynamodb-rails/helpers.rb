# encoding: utf-8
module Dynamo

  # Adapter provides a generic, write-through class that abstracts variations in the underlying connections to provide a uniform response
  # to Dynamo.
  module Helpers
    extend self

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

      case options[:type].to_s.upcase
        when "D"
          Time.at(value)
        when "DS"
          Set[*value.map {|v| Time.at(v) }]
        else
          value
      end
    end

    # Determine how to dump this field. Given a value, it'll determine how to turn it into a value that can be
    # persisted into the datastore.
    #
    # @since 0.2.0
    def dump_field(value, options)
      return if value.nil? || (value.respond_to?(:empty?) && value.empty?)

      case options[:type].to_s.upcase
        when "S"
          value
        when "SS"
          value.flatten
        when "N", "D"
          "#{value.to_f}"
        when "NS", "DS"
          Set[*value.map {|v| "#{v.to_f}" }]
        when "B"
          AWS::DynamoDB::Binary.new(value)
        when "BS"
          Set[*value.map{|v| AWS::DynamoDB::Binary.new(v) }]
      end
    end

    def key_type_dump(key)
      return :N if key.to_s.upcase == "D"
      return :NS if key.to_s.upcase == "DS"

      # TODO fix hack. Another method for converting Sets.
      # raise "Wrong key type #{key}" unless ["N", "S", "B"].include?(key.to_s.upcase)

      key.to_s.upcase
    end
  end
end