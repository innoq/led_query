require "rest_client"

# NB: HTTP API reverse engineered based on web UI
class LEDQuery::SesameStore

  def initialize(host)
    @host = host.chomp("/")
  end

  def create_repo(id, type, ruleset, desc=nil)
    desc ||= id

    uri = "#{repo_uri("NONE")}/create"
    return send_request(uri, "Repository ID" => id, :type => type,
          "Rule-set" => ruleset, "Repository title" => desc)
  end

  def delete_repo(id)
    uri = "#{repo_uri(id)}/delete"
    return send_request(uri, :id => id)
  end

  def add_triples(repo_id, format, rdf)
    uri = "#{repo_uri(repo_id)}/add"
    return send_request(uri, :multipart => true, :source => "contents",
          "Content-Type" => format, :content => rdf)
  end

  def repo_uri(id)
    id = URI.encode_www_form_component(id)
    return "#{@host}/repositories/#{id}"
  end

  def send_request(uri, params)
    begin
      RestClient.post(uri, params)
      return false
    rescue RestClient::Found
      return true
    end
  end

end
