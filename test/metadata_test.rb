require File.expand_path("../test_helper.rb", __FILE__)

class MetadataTest < DefaultTest

  def test_observations
    prefixes = ["@prefix dcat: <http://www.w3.org/ns/dcat#>.",
        "@prefix dct: <http://purl.org/dc/terms/>."].join("\n")
    rdf = prefixes + File.read(@common) + <<-EOS.strip
led:soil123 a dcat:Dataset, qb:Observation;
    led:source led:bodenportal;
    led:location led:westerzgebirge;
    led:observedMedia led:groundwater;
    led:analyte led:sand;
    led:temporal [ dct:start 1996; dct:end 1996 ];
    dct:title "Hello World"@de;
    dct:description "lorem ipsum dolor sit amet"@de.

led:bodenportal a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Bodenportal"@de;
    dcat:landingPage <http://bodendaten.de>.

led:westerzgebirge a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Westerzgebirge"@de.

led:groundwater a skos:Concept;
    skos:inScheme led:observedMediaScheme;
    skos:prefLabel "Grundwasser"@de.

led:sand a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Sand"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    selected_concepts = { "#{@led}location" => ["#{@led}westerzgebirge"] }
    counts = @db.observations_count(selected_concepts)
    observations = @db.determine_observations(selected_concepts)
    assert_equal counts, {
      "#{@led}bodenportal" => { "count" => 1, "label" => "Bodenportal" }
    }
    assert_equal observations.length, counts["#{@led}bodenportal"]["count"]
    results = observations.map do |uri, obs|
      ["mean", "uom", "title", "desc", "time", "analyte", "location", "source"].
          map { |attr| obs[attr].map(&:to_s).join(", ") }.unshift(uri).
          join(" | ")
    end.sort.join("\n")
    assert_equal results, <<-EOS.strip
#{@led}soil123 |  |  | Hello World | lorem ipsum dolor sit amet | [1996, 1996] | "Sand"@de<#{@led}sand> | "Westerzgebirge"@de<#{@led}westerzgebirge> | "Bodenportal"@de<#{@led}bodenportal>
    EOS
  end

end
