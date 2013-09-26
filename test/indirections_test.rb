require File.expand_path("../test_helper.rb", __FILE__)

class IndirectionsTest < DefaultTest

  def test_concept_mappings
    rdf = File.read(@common) + <<-EOS
led:samples a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme.

led:koeln a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Köln"@de.

led:ammonium rdf:type skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Ammonium"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf
    @store.add_triples @repo, "text/turtle", File.read(@skos)

    # ensure that concepts without matches are not ignored

    rdf = <<-EOS
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix led: <http://data.uba.de/led/> .

led:obs123 a qb:Observation;
    led:source led:samples;
    led:analyte led:ammonium;
    led:location led:koeln.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    selected_concepts = { "#{@led}location" => ["#{@led}koeln"] }
    count = @db.observations_count(selected_concepts)
    observations = @db.determine_observations(selected_concepts)
    assert_equal count, 1
    assert_equal observations.length, count

    # concept mappings

    rdf = <<-EOS
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix led: <http://data.uba.de/led/> .

led:obs321 a qb:Observation;
    led:source led:samples;
    led:analyte led:ammonium;
    led:location led:cologne.

led:cologne skos:exactMatch led:koeln.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    ["koeln", "cologne"].each do |location|
      selected_concepts = { "#{@led}location" => ["#{@led}#{location}"] }
      count = @db.observations_count(selected_concepts)
      observations = @db.determine_observations(selected_concepts)
      assert_equal count, 2, location
      assert_equal observations.length, count, location
    end

    concepts, obs_count = @db.determine_concepts("#{@led}location", {},
        :include_observations_count => true, :infer => true)
    assert_equal obs_count, 2
    assert_equal concepts, {
      "#{@led}location" => {
        "#{@led}cologne" => { "de" => "Köln" },
      }
    }
  end

end
