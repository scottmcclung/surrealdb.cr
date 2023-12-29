require "http/client"
require "http"

module SurrealDB
  class HTTP < Client
    @client : ::HTTP::Client
    @headers : ::HTTP::Headers

    def initialize(url : String)
      @client = ::HTTP::Client.new(URI.parse(url))
      # @headers = ::HTTP::Headers{"Content-Type" => "application/json", "Accept" => "application/json"}
      @headers = ::HTTP::Headers{"Content-Type" => "application/json"}
    end

    def authenticate(user : String, pass : String)
      self.signin user, pass
    end

    def signup(user : String, pass : String)
      self.signin user, pass
    end

    def signin(user : String, pass : String) : Void
      @headers["Authorization"] = "Basic #{Base64.strict_encode("#{user}:#{pass}")}"
    end

    def use(ns : String, db : String) : Void
      @headers["NS"] = ns
      @headers["DB"] = db
    end

    def info
    end

    def invalidate
    end

    def let(key : String, value : String)
    end

    def query(query : String, params : Data)
      self.request "POST", "/sql", query
    end

    def update(table : String, params : Data)
      self.send "PUT", table, params
    end

    def change(table : String, params : Data)
      self.update table, params
    end

    def modify(table : String, params : Data)
      self.send "PATCH", table, params
    end

    def create(table : String, params : Data)
      self.send "POST", table, params
    end

    def select(data : String)
      self.send "GET", data
    end

    def delete(data : String)
      self.send "DELETE", data
    end

    def kill(data : String)
    end

    def live(data : String)
    end

    private def send(method : String, table : String, query : Data)
      self.request method, "/key/#{table.sub(":", "/")}", query.to_json
    end

    private def send(method : String, table : String)
      self.request method, "/key/#{table.sub(":", "/")}", nil
    end

    private def request(method : String, endpoint : String, query : String?)
      response = @client.exec method, endpoint, headers: @headers, body: query
      process_request(response)
    end

    private def process_request(response : ::HTTP::Client::Response)
      case response.status_code
      when 200..299
        return Array(Response).from_json response.body
      else
        raise Client::ServerError.new("#{response.status_code}: #{response.status_message} - #{ErrorResponse.from_json(response.body).description}")
      end
    end
  end

  class Client::ServerError < Exception
  end
end
