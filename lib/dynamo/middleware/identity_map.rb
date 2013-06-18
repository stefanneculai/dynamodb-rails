module Dynamo
  module Middleware
    class IdentityMap
      def initialize(app)
        @app = app
      end

      def call(env)
        Dynamo::IdentityMap.clear
        @app.call(env)
      ensure
        Dynamo::IdentityMap.clear
      end
    end
  end
end
