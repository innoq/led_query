require File.expand_path("../test_helper.rb", __FILE__)

class ObservationsTest < DefaultTest

  def test_extra_attributes
    prefixes = "@prefix dct: <http://purl.org/dc/terms/>."
    rdf = prefixes + File.read(@common) + <<-EOS
led:obs123 a qb:Observation;
    led:source led:upb;
    led:observedMedia led:fluvialWater;
    led:analyte led:lead;
    led:location led:trier;
    led:temporal [ dct:start 2001; dct:end 2001 ];
    led:mean 1.23;
    led:uom "mg/l N".

led:upb a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Umweltprobenbank"@de.

led:fluvialWater a skos:Concept;
    skos:inScheme led:observedMediaScheme;
    skos:prefLabel "Flusswasser"@de.

led:lead a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Blei"@de.

led:trier a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Trier"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    observations = @db.determine_observations({})
    assert_equal observations.length, 1
    assert observations["#{@led}obs123"].extras.empty?

    rdf = <<-EOS
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix led: <http://data.uba.de/led/> .
@prefix upb: <led://data.uba.de/upb/> .

led:obs123 upb:extractionMethod upb:kw .

upb:kw a upb:extractionMethod;
    skos:prefLabel "Königswasser"@de .
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    observations = @db.determine_observations({})
    assert observations["#{@led}obs123"].extras.empty?

    rdf = <<-EOS
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix upb: <led://data.uba.de/upb/> .

upb:extractionMethod a qb:DimensionProperty;
    skos:prefLabel "Extraktionsmethode"@de .
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    observations = @db.determine_observations({})
    obs = observations["#{@led}obs123"]
    extras = obs.extras.map do |key, value|
      "#{key}: #{value.map(&:to_s).join(", ")}"
    end
    upb = "led://data.uba.de/upb/"
    assert_equal extras.join("\n"), <<-EOS.strip
"Extraktionsmethode"@de<#{upb}extractionMethod>: "Königswasser"@de<#{upb}kw>
    EOS

    rdf = <<-EOS
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix led: <http://data.uba.de/led/> .
@prefix upb: <led://data.uba.de/upb/> .

led:obs123 upb:gender "weiblich";
    upb:weight 6.93;
    upb:total 7 .

upb:gender a qb:AttributeProperty;
    skos:prefLabel "Geschlecht"@de .
upb:weight a qb:AttributeProperty;
    skos:prefLabel "Gewicht"@de .
upb:total a qb:AttributeProperty;
    skos:prefLabel "Anzahl"@de .
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    observations = @db.determine_observations({})
    obs = observations["#{@led}obs123"]
    extras = obs.extras.map do |key, value|
      "#{key}: #{value.map(&:to_s).join(", ")}"
    end
    upb = "led://data.uba.de/upb/"
    assert_equal extras.sort.join("\n"), <<-EOS.strip
"Anzahl"@de<#{upb}total>: 7
"Extraktionsmethode"@de<#{upb}extractionMethod>: "Königswasser"@de<#{upb}kw>
"Geschlecht"@de<#{upb}gender>: weiblich
"Gewicht"@de<#{upb}weight>: 6.93
    EOS
  end

end
