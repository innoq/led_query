require "json"
require "net/http"
require "led_query"

module LEDQuery::SPARQL

  # returns HTTP response if the request failed
  def self.query(endpoint, query, infer=false)
    params = { "query" => query }
    params[:infer] = false unless infer
    res = http_request("POST", endpoint, params,
        { "Accept" => "application/sparql-results+json" })
    return res.code == "200" ? JSON.load(res.body) : res
  end

  def self.http_request(method, uri, body=nil, headers={})
    uri = URI.parse(uri)

    req = Net::HTTP.const_get(method.to_s.downcase.capitalize).new(uri.to_s)
    headers.each { |key, value| req[key] = value }
    if body.is_a? Hash
      req.set_form_data(body)
    elsif body
      req.body = body
    end

    return Net::HTTP.new(uri.host, uri.port).request(req)
  end

end
