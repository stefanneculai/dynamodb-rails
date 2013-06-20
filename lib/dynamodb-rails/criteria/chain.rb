# encoding: utf-8
module Dynamo #:nodoc:
  module Criteria

    # The criteria chain is equivalent to an ActiveRecord relation (and realistically I should change the name from
    # chain to relation). It is a chainable object that builds up a query and eventually executes it either on an index
    # or by a full table scan.
    class Chain
      attr_accessor :query, :source, :values, :limit, :start, :consistent_read
      include Enumerable

      # Create a new criteria chain.
      #
      # @param [Class] source the class upon which the ultimate query will be performed.
      def initialize(source)
        @query = {}
        @source = source
        @consistent_read = false
        @scan_index_forward = false
      end

      # The workhorse method of the criteria chain. Each key in the passed in hash will become another criteria that the
      # ultimate query must match. A key can either be a symbol or a string, and should be an attribute name or
      # an attribute name with a range operator.
      #
      # @example A simple criteria
      #   where(:name => 'Josh')
      #
      # @example A more complicated criteria
      #   where(:name => 'Josh', 'created_at.gt' => DateTime.now - 1.day)
      def where(args)
        args.each do |k, v|
          mk = k.to_s.split('.').first.to_sym
          if @source.attributes.has_key? mk
            vx = Dynamo::Helpers.dump_field(v, @source.attributes[mk])
            tx = Dynamo::Helpers.key_type_dump(@source.attributes[mk][:type])
            query[k.to_sym] = {:value => vx, :type => tx}
          end
        end

        self
      end

      def consistent
        @consistent_read = true
        self
      end

      # Returns all the records matching the criteria.
      #
      # @since 0.2.0
      def all(opts = {})
        batch opts[:batch_size] if opts.has_key? :batch_size
        records
      end
      
      # Destroys all the records matching the criteria.
      def destroy_all
        ids = []
        
        if range?
          ranges = []
         # Dynamo::Client.query(source.table_name, range_query).collect do |hash|
          #  ids << hash[source.hash_key.to_sym]
          #  ranges << hash[source.range_key.to_sym]
          #end
          
          source.destroy(source.table_name, ids,{:range_key => ranges})
        else
          Dynamo::Client.scan(source.table_name, query, scan_opts).collect do |hash| 
            ids << hash[source.hash_key.to_sym]
          end
          
          source.destroy(source.table_name, ids)
        end   
      end

      # Destroys all the records matching the criteria.
      def delete_all
        ids = []

        if range?
          ranges = []
          # Dynamo::Client.query(source.table_name, range_query).collect do |hash|
          #  ids << hash[source.hash_key.to_sym]
          #  ranges << hash[source.range_key.to_sym]
          #end

          source.destroy(source.table_name, ids,{:range_key => ranges})
        else
          Dynamo::Client.scan(source.table_name, query, scan_opts).collect do |hash|
            ids << hash[source.hash_key.to_sym]
          end

          source.destroy(source.table_name, ids)
        end
      end

      # Returns the first record matching the criteria.
      #
      # @since 0.2.0
      def first
        limit(1).first
      end

      def limit(limit)
        @limit = limit
        records
      end

      def batch(batch_size)
        raise 'Cannot batch calls when using partitioning' if Dynamo::Config.partitioning?
        @batch_size = batch_size
        self
      end

      def start(start)
        @start = start
        self
      end

      def scan_index_forward(scan_index_forward)
        @scan_index_forward = scan_index_forward
        self
      end

      def select(select)
        @select = select
        self
      end

      # Allows you to use the results of a search as an enumerable over the results found.
      #
      # @since 0.2.0
      def each(&block)
        records.each(&block)
      end

      def consistent_opts
        { :consistent_read => consistent_read }
      end

      private

      # The actual records referenced by the association.
      def records
        results = if range?
          records_with_range
        else
          records_without_index
        end
        @batch_size ? results : Array(results)
      end

      # Get records from a range request.
      def records_with_range
        Enumerator.new do |yielder|
          Dynamo::Client.query(source.table_name, query, query_opts).each do |hash|
            yielder.yield source.from_database(hash)
          end
        end
      end

      # If the query does not match an index, we'll manually scan the associated table to find results.
      def records_without_index
        if Dynamo::Config.warn_on_scan
          Dynamo.logger.warn 'Queries without an index are forced to use scan and are generally much slower than indexed queries!'
          Dynamo.logger.warn "You can index this query by adding this to #{source.to_s.downcase}.rb: index [#{source.attributes.sort.collect{|attr| ":#{attr}"}.join(', ')}]"
        end

        if @consistent_read
          raise Dynamo::Errors::InvalidQuery, 'Consistent read is not supported by SCAN operation'
        end

        Enumerator.new do |yielder|
          Dynamo::Client.scan(source.table_name, query, scan_opts).each do |hash|
            yielder.yield source.from_database(hash)
          end
        end
      end

      # Get the query keys. Sometimes it may be created_at.gt.
      def query_keys
        query.keys.collect{|k| k.to_s.split('.').first}
      end

      # Use range query only when [hash_key] or [hash_key, range_key] is specified in query keys.
      def range?
        # Query has hash_key or range_key
        hr = (query_keys.include?(source.hash_key.to_s) or query_keys.include?(source.range_key.to_s))

        # Query has hash_key or index_key
        query_index = nil
        source.options[:indexes].each do |index|
          if query_keys.include?(index[:name].to_s)
            query_index = index[:name]
            @secondary_index = "#{source.table_name}_#{query_index.to_s}_index"
          end
        end

        # Table has key [HASH,RANGE]
        return false unless (hr or !query_index.nil?) and !source.range_key.nil?

        # Query is formed only with [KEY] or [KEY, HASH]
        only_hash = query_keys == [source.hash_key.to_s]
        only_hash_range = (query_keys.to_set == [source.hash_key.to_s, source.range_key.to_s].to_set)
        only_hash_index = (query_keys.to_set == [source.hash_key.to_s, query_index.to_s].to_set)

        only_hash || only_hash_range || only_hash_index
      end

      # Options for query.
      def query_opts
        opts = {}
        opts[:index_name] = @secondary_index if @secondary_index
        opts[:limit] = @limit if @limit
        opts[:next_token] = source.get_key(@start) if @start
        opts[:scan_index_forward] = @scan_index_forward
        opts[:select] = @select if @select
        opts
      end

      # Options for scan.
      def scan_opts
        opts = {}
        opts[:limit] = @limit if @limit
        opts[:next_token] = source.get_key(@start) if @start
        opts[:batch_size] = @batch_size if @batch_size
        opts[:select] = @select if @select
        opts
      end
    end

  end

end
