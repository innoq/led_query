# encoding: UTF-8

$:.push File.expand_path("../lib", __FILE__)
require "led_query/version"

Gem::Specification.new do |s|
  s.name        = "led_query"
  s.version     = LEDQuery::VERSION
  s.authors     = ["FND"]
  s.summary     = "API for accessing the Linked Environment Data triplestore"
  s.description = <<-EOS.strip
    provides common abstractions for querying the LED triplestore via SPARQL
  EOS

  s.rubyforge_project = "led_query"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- test/*.rb`.split("\n")
  s.require_paths = ["lib"]

  s.add_development_dependency "rake"
  s.add_development_dependency "minitest"
  s.add_development_dependency "rest-client"
end
