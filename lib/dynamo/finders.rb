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

        ids = Array(ids.flatten.uniq)
        if ids.count == 1
          self.find_by_id(ids.first)
        else
          find_all(ids)
        end
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
        items = Dynamo::Client.batch_get_item(self.table_name, ids, options)
        #puts items
        items ? items[self.table_name].map{|i| from_database(i)} : []
      end

      # Find one object directly by id.
      #
      # @param [String] id the id of the object to find
      #
      # @return [Dynamoid::Document] the found object, or nil if nothing was found
      #
      # @since 0.2.0
      def find_by_id(id, options={})
        key = {}

        # HASH KEY
        if id.respond_to?(:to_str)
          key = {self.hash_key => id}

        # RANGE KEY
        elsif id.respond_to?(:each)
          key = id
        end

        item = Dynamo::Client.get_item(self.table_name, key, options)
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
          chain.query = Hash.new.tap {|h| attributes.each_with_index {|attr, index| h[attr.to_sym] = args[index]}}

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
