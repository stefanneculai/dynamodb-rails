# encoding: utf-8
module Dynamo

  # This module defines the finder methods that hang off the document at the
  # class level, like find, find_by_id, and the method_missing style finders.
  module Finders
    extend ActiveSupport::Concern

    module ClassMethods

      # Find one or many objects, specified by one id or an array of ids.
      #
      # @param [Array/String] *id an array of ids or one single id
      #
      # @return [Dynamoid::Document] one object or an array of objects, depending on whether the input was an array or not
      #
      # @since 0.2.0
      def find(*ids)

        options = if ids.last.is_a? Hash
                    ids.slice!(-1)
                  else
                    {}
                  end

        # TODO verify for [[RAMGE, HASH], [RANGE, HASH]]
        ids = Array(ids.flatten.uniq)
        if ids.count == 1 || (!self.range_key.nil? and ids.count == 2 and !ids.first.respond_to?(:each))
          self.find_by_id(ids.first) if self.range_key.nil?
          self.find_by_id(ids) if !self.range_key.nil?
        else
          find_all(ids)
        end
      end

      def get_key(id)
        key = {}

        # HASH KEY
        if id.respond_to?(:to_str)
          unless self.range_key.nil?
            raise 'Key is expected to be [HASH, RANGE].'
          end
          v = Dynamo::Helpers.dump_field(id, self.attributes[self.hash_key])
          t = Dynamo::Helpers.key_type_dump(self.attributes[self.hash_key][:type])
          key = {self.hash_key => {:value => v, :type => t}}

          # RANGE KEY
        elsif id.respond_to?(:each)
          if self.range_key.nil?
            raise 'Key is expected to be [HASH].'
          end

          if id.length != 2
            raise 'Key is expected to be [HASH,RANGE].'
          end
          v = Dynamo::Helpers.dump_field(id.first, self.attributes[self.hash_key])
          t = Dynamo::Helpers.key_type_dump(self.attributes[self.hash_key][:type])
          key[self.hash_key] = {:value => v, :type => t}

          v = Dynamo::Helpers.dump_field(id.second, self.attributes[self.range_key])
          t = Dynamo::Helpers.key_type_dump(self.attributes[self.range_key][:type])
          key[self.range_key] = {:value => v, :type => t}
        end

        return key
      end

      # Return objects found by the given array of ids, either hash keys, or hash/range key combinations using BatchGet.
      # Returns empty array if no results found.
      #
      # @param [Array<ID>] ids
      # @param [Hash] options: Passed to the underlying query.
      #
      # @example
      #   find all the user with hash key
      #   User.find_all(['1', '2', '3'])
      #
      #   find all the tweets using hash key and range key with consistent read
      #   Tweet.find_all([['1', 'red'], ['1', 'green']], :consistent_read => true)
      def find_all(ids, options = {})
        keys = []

        ids.each do |id|
          keys.push(get_key(id))
        end

        return [] if keys.empty?

        items = Dynamo::Client.batch_get_item(self.table_name, keys, options)
        items ? items.map{|i| from_database(i)} : []
      end

      # Find one object directly by id.
      #
      # @param [String] id the id of the object to find
      #
      # @return [Dynamoid::Document] the found object, or nil if nothing was found
      #
      # @since 0.2.0
      def find_by_id(id, options={})
        item = Dynamo::Client.get_item(self.table_name, get_key(id), options)
        if item
          from_database(item)
        else
          nil
        end
      end

      # Find using exciting method_missing finders attributes. Uses criteria chains under the hood to accomplish this neatness.
      #
      # @example find a user by a first name
      #   User.find_by_first_name('Josh')
      #
      # @example find all users by first and last name
      #   User.find_all_by_first_name_and_last_name('Josh', 'Symonds')
      #
      # @return [Dynamoid::Document/Array] the found object, or an array of found objects if all was somewhere in the method
      #
      # @since 0.2.0
      def method_missing(method, *args)
        if method =~ /find/
          finder = method.to_s.split('_by_').first
          attributes = method.to_s.split('_by_').last.split('_and_')

          chain = Dynamo::Criteria::Chain.new(self)
          chain.where(Hash.new.tap {|h| attributes.each_with_index {|attr, index| h[attr.to_sym] = args[index]}})

          if finder =~ /all/
            return chain.all
          else
            return chain.first
          end
        else
          super
        end
      end
    end
  end

end
