require "minitest/autorun"
require "led_query/database"
require "led_query/sesame_store"

class DefaultTest < Minitest::Test

  # XXX: relying on remote server
  HOST = "http://store.led.innoq.com:8080/openrdf-sesame/repositories/"

  def setup
    @repo = "ledtest"
    @DB = database_connection(@repo)
    @store = make_repo(@repo)

    @led = "http://data.uba.de/led/"
    @common = File.expand_path("../fixtures/common.ttl", __FILE__)
  end

  def teardown
    @store.delete_repo @repo
  end

  def database_connection(repo_name)
    url = HOST + repo_name
    return LEDQuery::Database.new(url)
  end

  def make_repo(name)
    host = HOST.sub(URI.parse(HOST).path, "")
    store = LEDQuery::SesameStore.new("#{host}/openrdf-workbench")
    store.create_repo name, "owlim-lite", "owl-max-optimized"
    return store
  end

end
