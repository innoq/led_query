require File.expand_path("../test_helper.rb", __FILE__)

class DatabaseTest < DefaultTest

  def test_dimensions
    @store.add_triples @repo, "text/turtle", File.read(@common)

    dimensions = @DB.determine_dimensions
    assert_equal dimensions.keys.sort, ["#{@led}analyte", "#{@led}location",
        "#{@led}observedMedia", "#{@led}source"]
    assert_equal dimensions.values.map(&:inspect).sort, ['{"de"=>"Analyt"}',
        '{"de"=>"Datenquelle"}', '{"de"=>"Raumbezug"}',
        '{"de"=>"Untersuchungsmedium"}']
  end

  def test_concepts
    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"], {}, true)
    assert_equal obs_count, 0
    assert_equal concepts, {}

    rdf = File.read(@common) + <<-EOS
led:berlin a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Berlin"@de.
led:hamburg a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Hamburg"@de.

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
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"], {}, true)
    assert_equal obs_count, 3
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}ammonium" => { "de" => "Ammonium" },
        "#{@led}phosphorus" => { "de" => "Phosphor" }
      }
    }

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"],
        { "#{@led}location" => ["#{@led}berlin"] }, true)
    assert_equal obs_count, 2
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}ammonium" => { "de" => "Ammonium" }
      }
    }

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"],
        { "#{@led}location" => ["#{@led}hamburg"] }, true)
    assert_equal obs_count, 1
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}phosphorus" => { "de" => "Phosphor" }
      }
    }

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"],
        { "#{@led}location" => ["#{@led}berlin", "#{@led}hamburg"] }, true)
    assert_equal obs_count, 3
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}ammonium" => { "de" => "Ammonium" },
        "#{@led}phosphorus" => { "de" => "Phosphor" }
      }
    }

    @store.add_triples @repo, "text/turtle", <<-EOS
@prefix skos: <http://www.w3.org/2004/02/skos/core#>.
@prefix qb: <http://purl.org/linked-data/cube#>.
@prefix led: <http://data.uba.de/led/>.

led:eea a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "European Environment Agency"@en;
    skos:prefLabel "Europäische Umweltagentur"@de.

led:nitrogen a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Stickstoff"@de.

led:obs789 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:nitrogen;
    led:location led:berlin.
    EOS

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"],
        { "#{@led}location" => ["#{@led}berlin"] }, true)
    assert_equal obs_count, 3
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}ammonium" => { "de" => "Ammonium" },
        "#{@led}nitrogen" => { "de" => "Stickstoff" }
      }
    }

    concepts, obs_count = @DB.determine_concepts(["#{@led}analyte"], {
      "#{@led}location" => ["#{@led}berlin"],
      "#{@led}source" => ["#{@led}eea"]
    }, true)
    assert_equal obs_count, 1
    assert_equal concepts, {
      "#{@led}analyte" => {
        "#{@led}nitrogen" => { "de" => "Stickstoff" }
      }
    }
  end

  def test_observations
    prefixes = "@prefix dct: <http://purl.org/dc/terms/>."
    rdf = prefixes + File.read(@common) + <<-EOS
led:eea a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Europäische Umweltagentur"@de.
led:upb a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Umweltprobenbank"@de.

led:berlin a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Berlin"@de.
led:hamburg a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Hamburg"@de.

led:ammonium rdf:type skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Ammonium"@de.
led:phosphorus a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Phosphor"@de.
led:nitrogen a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Stickstoff"@de.

led:obs123 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 2001; dct:end 2001 ];
    led:mean 1.23;
    led:uom "mg/l N".
led:obs321 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 1996; dct:end 1996 ];
    led:mean 3.21;
    led:uom "mg/l N".
led:obs456 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:phosphorus;
    led:location led:hamburg;
    led:temporal [ dct:start 2007; dct:end 2007 ];
    led:mean 4.56;
    led:uom "mg/l P".
led:obs789 a qb:Observation;
    qb:dataSet led:upb;
    led:analyte led:nitrogen;
    led:location led:berlin;
    led:temporal [ dct:start 2011; dct:end 2011 ];
    led:mean 7.89;
    led:uom "mg/l N".
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    count = @DB.observations_count
    assert_equal count, 4

    selected_concepts = { "#{@led}location" => ["#{@led}berlin"] }
    count = @DB.observations_count(selected_concepts)
    observations = @DB.determine_observations(selected_concepts)
    assert_equal count, 3
    assert_equal observations.length, count
    results = observations.map do |obs|
      ["obs", "mean", "uom", "time", "analyte", "location", "source"].
          map { |key| obs[key].to_s }.join(" | ")
    end.sort.join("\n")
    assert_equal results, <<-EOS.strip
#{@led}obs123 | 1.23 | mg/l N | [2001, 2001] | "Ammonium"<#{@led}ammonium> | "Berlin"<#{@led}berlin> | "Europäische Umweltagentur"<#{@led}eea>
#{@led}obs321 | 3.21 | mg/l N | [1996, 1996] | "Ammonium"<#{@led}ammonium> | "Berlin"<#{@led}berlin> | "Europäische Umweltagentur"<#{@led}eea>
#{@led}obs789 | 7.89 | mg/l N | [2011, 2011] | "Stickstoff"<#{@led}nitrogen> | "Berlin"<#{@led}berlin> | "Umweltprobenbank"<#{@led}upb>
    EOS

    count = @DB.observations_count({
      "#{@led}location" => ["#{@led}berlin"],
      "#{@led}source" => ["#{@led}eea"]
    })
    assert_equal count, 2

    count = @DB.observations_count({
      "#{@led}location" => ["#{@led}berlin"],
      "#{@led}source" => ["#{@led}upb"]
    })
    assert_equal count, 1
  end

  def test_time_handling
    prefixes = "@prefix dct: <http://purl.org/dc/terms/>."
    rdf = prefixes + File.read(@common) + <<-EOS
led:eea a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Europäische Umweltagentur"@de.

led:berlin a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Berlin"@de.
led:hamburg a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Hamburg"@de.

led:ammonium rdf:type skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Ammonium"@de.
led:phosphorus a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Phosphor"@de.
led:nitrogen a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Stickstoff"@de.

led:obs123 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 2001; dct:end 2001 ];
    led:mean 1.23;
    led:uom "mg/l N".
led:obs321 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:nitrogen;
    led:location led:berlin;
    led:temporal [ dct:start 2001; dct:end 2001 ];
    led:mean 3.21;
    led:uom "mg/l N".
led:obs456 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:phosphorus;
    led:location led:hamburg;
    led:temporal [ dct:start 2007; dct:end 2007 ];
    led:mean 4.56;
    led:uom "mg/l P".
led:obs789 a qb:Observation;
    qb:dataSet led:eea;
    led:analyte led:ammonium;
    led:location led:berlin;
    led:temporal [ dct:start 2011; dct:end 2011 ];
    led:mean 7.89;
    led:uom "mg/l N".
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    count = @DB.observations_count
    assert_equal count, 4

    selected_concepts = { "#{@led}temporal" => ["2001"] }
    count = @DB.observations_count(selected_concepts)
    observations = @DB.determine_observations(selected_concepts)
    assert_equal count, 2
    assert_equal observations.length, count
    results = observations.map do |obs|
      ["obs", "mean", "uom", "time", "analyte", "location", "source"].
          map { |key| obs[key].to_s }.join(" | ")
    end.sort.join("\n")
    assert_equal results, <<-EOS.strip
#{@led}obs123 | 1.23 | mg/l N | [2001, 2001] | "Ammonium"<#{@led}ammonium> | "Berlin"<#{@led}berlin> | "Europäische Umweltagentur"<#{@led}eea>
#{@led}obs321 | 3.21 | mg/l N | [2001, 2001] | "Stickstoff"<#{@led}nitrogen> | "Berlin"<#{@led}berlin> | "Europäische Umweltagentur"<#{@led}eea>
    EOS

    selected_concepts = {
      "#{@led}temporal" => ["2001"],
      "#{@led}analyte" => ["#{@led}nitrogen"]
    }
    count = @DB.observations_count(selected_concepts)
    observations = @DB.determine_observations(selected_concepts)
    assert_equal count, 1
    assert_equal observations.length, count
    results = observations.map do |obs|
      ["obs", "mean", "uom", "time", "analyte", "location", "source"].
          map { |key| obs[key].to_s }.join(" | ")
    end.sort.join("\n")
    assert_equal results, <<-EOS.strip
#{@led}obs321 | 3.21 | mg/l N | [2001, 2001] | "Stickstoff"<#{@led}nitrogen> | "Berlin"<#{@led}berlin> | "Europäische Umweltagentur"<#{@led}eea>
    EOS
  end

end
