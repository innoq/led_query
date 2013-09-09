require File.expand_path("../test_helper.rb", __FILE__)
require 'yaml'

class HierarchyTest < DefaultTest

  def test_resolve_hierarchy
    data = [
      ["brd", "brd", "nrw"],
      ["brd", "brd", "sl"],
      ["brd", "brd", "rp"],
      ["brd", "nrw", "cologne"],
      ["brd", "sl", "saarbruecken"],
      ["brd", "cologne", "portz"],
      # reversed order
      ["brd", "by", "munich"],
      ["brd", "brd", "by"],
      # arbitrary depth
      ["root", "dos", "lorem"],
      ["root", "bravo", "uno"],
      ["root", "bravo", "dos"],
      ["root", "bar", "alpha"],
      ["root", "bar", "bravo"],
      ["root", "root", "foo"],
      ["root", "root", "bar"],
      ["root", "dos", "ipsum"]
    ]
    expected = YAML.load <<-EOS
brd:
  nrw:
    cologne:
      portz: {}
  rp: {}
  sl:
    saarbruecken: {}
  by:
    munich: {}
root:
  foo: {}
  bar:
    alpha: {}
    bravo:
      uno: {}
      dos:
        lorem: {}
        ipsum: {}
    EOS
    assert_equal LEDQuery::Database.resolve_hierarchy(data), expected
  end

  def test_inferences
    rdf = <<-EOS.strip
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix led: <http://data.uba.de/led/> .

led:brd a skos:Concept;
    skos:narrower led:nrw;
    skos:prefLabel "Bundesrepublik Deutschland"@de.

led:nrw a skos:Concept;
    skos:prefLabel "Nordrhein-Westfalen"@de.

led:sl a skos:Concept;
    skos:broader led:brd;
    skos:prefLabel "Saarland"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    query = <<-EOS
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX led:<http://data.uba.de/led/>

SELECT DISTINCT ?child WHERE {
    ?parent skos:narrower ?child
}
    EOS

    # without SKOS vocabulary awareness
    [false, true].each do |infer|
      res = @db.sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["child"]["value"] }
      assert_equal concepts, ["#{@led}nrw"]
    end

    skos = File.expand_path("../fixtures/skos.ttl", __FILE__)
    @store.add_triples @repo, "text/turtle", File.read(skos)

    # with SKOS vocabulary awareness
    [false, true].each do |infer|
      res = @db.sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["child"]["value"] }

      expected = ["#{@led}nrw"]
      expected << "#{@led}sl" if infer

      assert_equal concepts, expected
    end
  end

  def test_transitivity
    rdf = <<-EOS.strip
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix led: <http://data.uba.de/led/> .

led:brd a skos:Concept;
    skos:prefLabel "Bundesrepublik Deutschland"@de.

led:nrw a skos:Concept;
    skos:broader led:brd;
    skos:prefLabel "Nordrhein-Westfalen"@de.

led:sl a skos:Concept;
    skos:broader led:brd;
    skos:prefLabel "Saarland"@de.

led:cologne a skos:Concept;
    skos:broader led:nrw;
    skos:prefLabel "Köln"@de.

led:saarbruecken a skos:Concept;
    skos:broader led:sl;
    skos:prefLabel "Saarbrücken"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf
    skos = File.expand_path("../fixtures/skos.ttl", __FILE__)
    @store.add_triples @repo, "text/turtle", File.read(skos)

    query = <<-EOS
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX led:<http://data.uba.de/led/>

SELECT DISTINCT ?desc WHERE {
    led:brd skos:narrowerTransitive ?desc
}
    EOS

    [false, true].each do |infer|
      res = @db.sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["desc"]["value"] }.sort

      expected = infer == false ? [] : ["#{@led}nrw", "#{@led}sl",
          "#{@led}cologne", "#{@led}saarbruecken"].sort

      assert_equal concepts, expected
    end
  end

end
