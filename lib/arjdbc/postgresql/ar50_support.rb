module PG
  module TextEncoder
    class Array
      def initialize(options)
        @name = options.fetch(:name)
        @delimiter = options.fetch(:delimiter)
      end

      def encode(value)
        quote(value)
      end

      private
      
      def quote(value)
        case value
        when ::Array
          "{#{value.map { |x| quote(x) }.join(@delimiter)}}"
        when nil
          'NULL'
        else
          quote_string(value.to_s)
        end
      end

      def quote_string(string)
        "\"#{string.gsub(/[\\\"]/, "\"" => "\\\"", "\\" => "\\\\")}\""
      end
    end
  end

  module TextDecoder
    class Array
      # Loads pg_array_parser if available. String parsing can be
      # performed quicker by a native extension, which will not create
      # a large amount of Ruby objects that will need to be garbage
      # collected. pg_array_parser has a C and Java extension
      begin
        require 'pg_array_parser'
        include PgArrayParser
      rescue LoadError
        require 'arjdbc/postgresql/base/array_parser'
        include ActiveRecord::ConnectionAdapters::PostgreSQL::ArrayParser
      end

      def initialize(options)
        @name = options.fetch(:name)
        @delimiter = options.fetch(:delimiter)
      end

      def decode(value)
        parse_pg_array(value)
      end
    end
  end
end

module PGconn
  def self.quote_ident(name)
    %("#{name.to_s.gsub("\"", "\"\"")}")
  end
end

module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter
      def extract_limit(sql_type) # :nodoc:
        case sql_type
        when /^bigint/i, /^int8/i
          8
        when /^smallint/i
          2
        else
          super
        end
      end

      # Extracts the value from a PostgreSQL column default definition.
      def extract_value_from_default(default) # :nodoc:
        case default
          # Quoted types
          when /\A[\(B]?'(.*)'.*::"?([\w. ]+)"?(?:\[\])?\z/m
            # The default 'now'::date is CURRENT_DATE
            if $1 == "now".freeze && $2 == "date".freeze
              nil
            else
              $1.gsub("''".freeze, "'".freeze)
            end
          # Boolean types
          when 'true'.freeze, 'false'.freeze
            default
          # Numeric types
          when /\A\(?(-?\d+(\.\d*)?)\)?(::bigint)?\z/
            $1
          # Object identifier types
          when /\A-?\d+\z/
            $1
          else
            # Anything else is blank, some user type, or some function
            # and we can't know the value of that, so return nil.
            nil
        end
      end

      def extract_default_function(default_value, default) # :nodoc:
        default if has_default_function?(default_value, default)
      end

      def has_default_function?(default_value, default) # :nodoc:
        !default_value && (%r{\w+\(.*\)|\(.*\)::\w+} === default)
      end
    end

    module PostgreSQL
      module Quoting
        def escape_bytea(string) #:nodoc:
          super
        end

        # Unescapes bytea output from a database to the binary string it represents.
        # NOTE: This is NOT an inverse of escape_bytea! This is only to be used
        # on escaped binary output from database drive.
        def unescape_bytea(value)
          if value
            String.from_java_bytes(Java::OrgPostgresqlUtil::PGbytea.toBytes(value.to_java_bytes))
          end
        end

        def quote_string(string) #:nodoc:
          super
        end
      end
    end
  end
end
