require "json"
require "net/http"
require "led_query"

module LEDQuery::SPARQL

  # returns HTTP response if the request failed
  def self.query(endpoint, query, infer=false, logger=nil)
    params = { "query" => query }
    params[:infer] = false unless infer
    logger.debug ["==== SPARQL (infer: #{infer})", endpoint, "", query, "----"].
        join("\n") if logger
    res = http_request("POST", endpoint, params,
        { "Accept" => "application/sparql-results+json" })
    return res.code == "200" ? JSON.load(res.body) : res
  end

  def self.make_query(template, data={})
    templates_dir = File.expand_path(File.join("..", "templates"), __FILE__)
    render = lambda do |template, data| # required for partials -- XXX: hacky!
      data[:render] = render
      path = File.join(templates_dir, "#{template}.sparql.erb")
      res = File.read(path)
      return Erubis::Eruby.new(res).result(data)
    end
    query = render.call(template, data)
    infer = query[0..18] == "#META infer: true\n\n" # XXX: too strict?
    return query, infer
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
