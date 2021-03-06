require File.expand_path("../test_helper.rb", __FILE__)
require 'yaml'

class HierarchyTest < DefaultTest

  def test_concept_hierarchy
    rdf = File.read(@common) + <<-EOS
led:berlin a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Berlin"@de.
led:hamburg a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Hamburg"@de.
led:munich a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:bavaria;
    skos:prefLabel "München"@de.
led:bavaria a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Bayern"@de.
led:germany a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Bundesrepublik Deutschland"@de.

led:ammonium rdf:type skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Ammonium"@de.
led:phosphorus a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Phosphor"@de.

led:obs123 a qb:Observation;
    led:analyte led:ammonium;
    led:location led:berlin.
led:obs321 a qb:Observation;
    led:analyte led:ammonium;
    led:location led:berlin.
led:obs456 a qb:Observation;
    led:analyte led:phosphorus;
    led:location led:hamburg.
led:obs789 a qb:Observation;
    led:analyte led:phosphorus;
    led:location led:munich.
    EOS
    @store.add_triples @repo, "text/turtle", rdf
    @store.add_triples @repo, "text/turtle", File.read(@skos)

    concepts, obs_count, hierarchy = @db.determine_concepts("#{@led}location",
        {}, :include_observations_count => true, :include_hierarchy => true)
    assert_equal obs_count, 4
    assert_equal concepts, {
      "#{@led}location" => {
        "#{@led}berlin" => { "de" => "Berlin" },
        "#{@led}hamburg" => { "de" => "Hamburg" },
        "#{@led}munich" => { "de" => "München" },
        "#{@led}bavaria" => { "de" => "Bayern" },
        "#{@led}germany" => { "de" => "Bundesrepublik Deutschland" }
      }
    }
    assert_equal hierarchy, YAML.load(<<-EOS)
#{@led}germany:
  #{@led}berlin: {}
  #{@led}hamburg: {}
  #{@led}bavaria:
    #{@led}munich: {}
    EOS

    selected_concepts = { "#{@led}location" => ["#{@led}bavaria"] }

    concepts, obs_count = @db.determine_concepts("#{@led}analyte",
        selected_concepts, :include_observations_count => true,
        :include_hierarchy => false, :include_descendants => false)
    assert_equal obs_count, 0
    assert_equal concepts, {}

    concepts, obs_count = @db.determine_concepts("#{@led}analyte",
        selected_concepts, :include_observations_count => true,
        :include_hierarchy => false, :include_descendants => true)
    assert_equal obs_count, 1
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}phosphorus" => { "de" => "Phosphor" }
      }
    }
  end

  def test_observations_via_concept_descendants
    prefixes = "@prefix dct: <http://purl.org/dc/terms/>."
    rdf = prefixes + File.read(@common) + <<-EOS.strip
led:berlin a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Berlin"@de.
led:hamburg a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Hamburg"@de.
led:munich a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:bavaria;
    skos:prefLabel "München"@de.
led:bavaria a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:broader led:germany;
    skos:prefLabel "Bayern"@de.
led:germany a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Bundesrepublik Deutschland"@de.

led:fluvialWater a skos:Concept;
    skos:inScheme led:observedMediaScheme;
    skos:prefLabel "Flusswasser"@de.

led:ammonium rdf:type skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Ammonium"@de.
led:phosphorus a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Phosphor"@de.

led:obs123 a qb:Observation;
    led:source led:places;
    led:observedMedia led:fluvialWater;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 2000; dct:end 2000 ].
led:obs321 a qb:Observation;
    led:source led:places;
    led:observedMedia led:fluvialWater;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 2000; dct:end 2000 ].
led:obs456 a qb:Observation;
    led:source led:places;
    led:observedMedia led:fluvialWater;
    led:analyte led:phosphorus;
    led:location led:hamburg;
    led:temporal [ dct:start 2000; dct:end 2000 ].
led:obs789 a qb:Observation;
    led:source led:places;
    led:observedMedia led:fluvialWater;
    led:analyte led:phosphorus;
    led:location led:munich;
    led:temporal [ dct:start 2000; dct:end 2000 ].

led:places a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme.
    EOS
    @store.add_triples @repo, "text/turtle", rdf
    @store.add_triples @repo, "text/turtle", File.read(@skos)

    selected_concepts = { "#{@led}location" => ["#{@led}germany"] }

    counts = @db.observations_count(selected_concepts)
    observations = @db.determine_observations(selected_concepts)
    assert_equal counts, { nil => { "count" => 0, "label" => nil } }
    assert_equal observations.length, counts[nil]["count"]

    counts = @db.observations_count(selected_concepts, true)
    observations = @db.determine_observations(selected_concepts, true)
    assert_equal counts, { "#{@led}places" => { "count" => 4, "label" => nil } }
    assert_equal observations.length, counts["#{@led}places"]["count"]
    results = observations.map { |uri, obs| uri }
    assert_equal results.to_set, ["#{@led}obs123", "#{@led}obs321",
        "#{@led}obs456", "#{@led}obs789"].to_set
  end

  def test_resolve_hierarchy
    @store.add_triples @repo, "text/turtle", File.read(@skos)

    @store.add_triples @repo, "text/turtle", <<-EOS.strip
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix : <http://data.uba.de/led/> .

:root a skos:Concept.
:foo a skos:Concept;
    skos:broader :root.
:bar a skos:Concept;
    skos:broader :root.
:alpha a skos:Concept;
    skos:broader :bar.
:bravo a skos:Concept;
    skos:broader :bar.
:uno a skos:Concept;
    skos:broader :bravo.
:dos a skos:Concept;
    skos:broader :bravo.
:lorem a skos:Concept;
    skos:broader :dos.
:ipsum a skos:Concept;
    skos:broader :dos.

:o1 a qb:Observation;
    :location :alpha.
:o2 a qb:Observation;
    :location :lorem.
    EOS

    query = <<-EOS
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX qb:<http://purl.org/linked-data/cube#>
PREFIX led:<http://data.uba.de/led/>

SELECT DISTINCT ?grancestor ?ancestor ?parent ?concept WHERE {
    ?obs a qb:Observation .
    ?obs led:location ?concept .
    OPTIONAL {
        ?parent skos:narrower ?concept .
        ?ancestor skos:narrowerTransitive ?concept .
        OPTIONAL { ?grancestor skos:narrower ?ancestor }
    }
}
    EOS
    res = @db._sparql(query, true)
    data = res["results"]["bindings"].map do |result|
      ["grancestor", "ancestor", "parent", "concept"].map do |var|
        result[var]["value"].sub(@led, "") rescue nil
      end
    end
    assert_equal data.to_set, [
      ["root", "bar", "bar", "alpha"],
      [nil, "root", "bar", "alpha"],
      ["bravo", "dos", "dos", "lorem"],
      ["bar", "bravo", "dos", "lorem"],
      ["root", "bar", "dos", "lorem"],
      [nil, "root", "dos", "lorem"]
    ].to_set

    expected = YAML.load <<-EOS
root:
  bar:
    alpha: {}
    bravo:
      dos:
        lorem: {}
    EOS
    assert_equal expected, LEDQuery::Database.resolve_hierarchy(data)

    data = [
      [nil, "de", "de", "berlin"],
      [nil, "de", "de", "hamburg"],
      [nil, "de", "sl", "saarbruecken"] # unprocessable due to missing entry for "sl"
    ]
    expected = YAML.load <<-EOS
de:
  berlin: {}
  hamburg: {}
    EOS
    assert_equal expected, LEDQuery::Database.resolve_hierarchy(data)
  end

  def disabled_test_inferences # disabled as this merely tests the database
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
      res = @db._sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["child"]["value"] }
      assert_equal concepts, ["#{@led}nrw"]
    end

    @store.add_triples @repo, "text/turtle", File.read(@skos)

    # with SKOS vocabulary awareness
    [false, true].each do |infer|
      res = @db._sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["child"]["value"] }

      expected = ["#{@led}nrw"]
      expected << "#{@led}sl" if infer

      assert_equal concepts, expected
    end
  end

  def disabled_test_transitivity # disabled as this merely tests the database
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
    @store.add_triples @repo, "text/turtle", File.read(@skos)

    query = <<-EOS
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX led:<http://data.uba.de/led/>

SELECT DISTINCT ?desc WHERE {
    led:brd skos:narrowerTransitive ?desc
}
    EOS

    [false, true].each do |infer|
      res = @db._sparql(query, infer)
      concepts = res["results"]["bindings"].map { |r| r["desc"]["value"] }.sort

      expected = infer == false ? [] : ["#{@led}nrw", "#{@led}sl",
          "#{@led}cologne", "#{@led}saarbruecken"].sort

      assert_equal concepts, expected
    end
  end

end
